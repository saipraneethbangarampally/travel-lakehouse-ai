"""Bronze: land source data as-is (all strings), append-only, with lineage columns.

* Relational sources are pulled incrementally with a per-table high-watermark
  (``WHERE watermark >= last``). ``>=`` gives at-least-once delivery so a row committed in
  the same second as the last extract is never missed; silver de-duplicates.
* File sources are tracked by a manifest so every file is processed exactly once.
* Watermarks are only advanced after the bronze write succeeds.

In Fabric this module maps to a Data Factory pipeline (Copy activity per table driven by
the same sources.yaml metadata) or a notebook using JDBC through the on-prem data gateway.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from . import lake, settings


def bronze_name(partner: str, source_table: str) -> str:
    return f"{partner.lower()}__{source_table.lower()}"


def _extract(db_path, table: str, wm_col: str, last_wm: str | None) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        if last_wm is None:
            sql, params = f"SELECT * FROM {table}", ()
        else:
            sql, params = f"SELECT * FROM {table} WHERE {wm_col} >= ?", (last_wm,)
        return pd.read_sql_query(sql, conn, params=params)


def _to_spark_strings(pdf: pd.DataFrame):
    """Schema-on-read: bronze keeps every column as string so upstream type drift never
    breaks ingestion. Typing happens in silver under a contract."""
    schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    rows = [tuple(None if pd.isna(v) else str(v) for v in r)
            for r in pdf.itertuples(index=False, name=None)]
    return lake.spark().createDataFrame(rows, schema)


def ingest_relational(batch_id: int) -> list[dict]:
    cfg = settings.sources_cfg()
    watermarks = settings.read_state("watermarks", {})
    log = []
    now = datetime.now(timezone.utc).isoformat()
    for partner, p in cfg["partners"].items():
        db = settings.SOURCES / p["database"]
        for t in p["tables"]:
            key = f"{partner}.{t['source_table']}"
            last = watermarks.get(key)
            pdf = _extract(db, t["source_table"], t["watermark_column"], last)
            entry = {"batch_id": batch_id, "source": key, "rows": len(pdf),
                     "from_watermark": last, "to_watermark": last}
            if len(pdf):
                df = (_to_spark_strings(pdf)
                      .withColumn("_partner_code", F.lit(partner))
                      .withColumn("_source_system", F.lit(key))
                      .withColumn("_batch_id", F.lit(batch_id))
                      .withColumn("_ingested_at", F.lit(now).cast("timestamp")))
                lake.write(df, "bronze", bronze_name(partner, t["source_table"]), mode="append")
                new_wm = str(pdf[t["watermark_column"]].max())
                watermarks[key] = new_wm  # advance only after a successful write
                entry["to_watermark"] = new_wm
            log.append(entry)
    settings.write_state("watermarks", watermarks)
    return log


def ingest_files(batch_id: int) -> list[dict]:
    cfg = settings.sources_cfg()["file_sources"]
    manifest = settings.read_state("file_manifest", {})
    log = []
    for name, fs in cfg.items():
        landing = settings.SOURCES / fs["landing"]
        seen = set(manifest.get(name, []))
        new_files = sorted(str(f) for f in landing.glob("*") if f.is_file() and str(f) not in seen)
        rows = 0
        if new_files:
            df = (lake.spark().read.json(new_files)
                  .withColumn("_source_file", F.input_file_name())
                  .withColumn("_batch_id", F.lit(batch_id))
                  .withColumn("_ingested_at", F.current_timestamp()))
            df = df.select([F.col(c).cast("string") if not c.startswith("_") or c == "_source_file"
                            else F.col(c) for c in df.columns])
            lake.write(df, "bronze", name, mode="append")
            rows = df.count()
            manifest[name] = sorted(seen | set(new_files))
        log.append({"batch_id": batch_id, "source": name, "files": len(new_files), "rows": rows})
    settings.write_state("file_manifest", manifest)
    return log


def run(batch_id: int) -> list[dict]:
    log = ingest_relational(batch_id) + ingest_files(batch_id)
    history = settings.read_state("ingestion_log", [])
    settings.write_state("ingestion_log", history + log)
    return log
