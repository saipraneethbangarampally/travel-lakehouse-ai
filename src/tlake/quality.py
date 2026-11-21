"""Declarative data-quality gate (rules live in config/quality.yaml).

Returns (passed, quarantined) and records every rule result so failures are visible
*before* data reaches reporting, which is the point of a DQ gate.
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import settings


class DataQualityError(RuntimeError):
    """Raised when an `error`-severity rule fails: the run stops, nothing is published."""


def apply(df: DataFrame, entity: str, batch_id: int) -> tuple[DataFrame, DataFrame, list[dict]]:
    rules = settings.quality_cfg()[entity]
    checked = df
    for r in rules:
        checked = checked.withColumn(f"_ok_{r['name']}", F.coalesce(F.expr(r["expr"]), F.lit(False)))

    agg = checked.agg(F.count(F.lit(1)).alias("_rows"),
                      *[F.sum(F.when(~F.col(f"_ok_{r['name']}"), 1).otherwise(0)).alias(r["name"])
                        for r in rules]).collect()[0]
    total = agg["_rows"]
    now = datetime.now(timezone.utc).isoformat()
    results = [{"batch_id": batch_id, "entity": entity, "rule": r["name"], "severity": r["severity"],
                "rows_checked": total, "failed": int(agg[r["name"]] or 0), "checked_at": now}
               for r in rules]
    _log(results)

    errors = [x for x in results if x["severity"] == "error" and x["failed"]]
    if errors:
        raise DataQualityError(f"{entity}: blocking rule(s) failed: {errors}")

    q_rules = [r["name"] for r in rules if r["severity"] == "quarantine"]
    failed_list = F.filter(
        F.array(*[F.when(~F.col(f"_ok_{n}"), F.lit(n)) for n in q_rules]) if q_rules else F.array(),
        lambda x: x.isNotNull())
    checked = checked.withColumn("_failed_rules", failed_list)
    ok_cols = [c for c in checked.columns if c.startswith("_ok_")]
    passed = checked.filter(F.size("_failed_rules") == 0).drop(*ok_cols, "_failed_rules")
    quarantined = checked.filter(F.size("_failed_rules") > 0).drop(*ok_cols)
    return passed, quarantined, results


def volume_check(source: str, rows: int, history: list[dict]) -> dict | None:
    """Flag a batch whose size is far off the running average for that source."""
    ratio_limit = settings.quality_cfg()["volume"]["max_ratio_vs_average"]
    prev = [h["rows"] for h in history if h.get("source") == source and h.get("rows")]
    if len(prev) < 2 or rows == 0:
        return None
    avg = sum(prev) / len(prev)
    if rows > avg * ratio_limit:
        return {"source": source, "rows": rows, "running_avg": round(avg, 1), "alert": "volume_spike"}
    return None


def _log(results: list[dict]) -> None:
    settings.write_state("dq_results", settings.read_state("dq_results", []) + results)
