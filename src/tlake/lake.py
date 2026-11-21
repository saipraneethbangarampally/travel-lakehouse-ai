"""Spark session + table I/O, written once so the rest of the code is storage-agnostic.

Locally tables are Parquet folders. Set TLAKE_TABLE_FORMAT=delta (Fabric, Databricks, or
a laptop where delta-spark can download its jars) and the same code writes Delta tables.
"""
from __future__ import annotations

import shutil
import uuid
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import settings

_spark: SparkSession | None = None


def spark() -> SparkSession:
    global _spark
    if _spark is not None:
        return _spark
    builder = (
        SparkSession.builder.appName("tlake")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if settings.TABLE_FORMAT == "delta":
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        builder = configure_spark_with_delta_pip(builder)
    _spark = builder.getOrCreate()
    _spark.sparkContext.setLogLevel("ERROR")
    _start_callback_server(_spark)
    return _spark


def _start_callback_server(session: SparkSession) -> None:
    """Start py4j's callback server (needed by foreachBatch) before PySpark does.

    PySpark reads the port with socket.getsockname() right after start(), while py4j's
    listener thread is still calling listen(). On WSL1 that race fails with
    `OSError: [Errno 22]`, so do the same setup but use the port py4j recorded at bind time.
    """
    from py4j.java_gateway import JavaObject

    gw = session.sparkContext._gateway
    if gw.__dict__.get("_callback_server") is not None:
        return
    p = gw.callback_server_parameters
    p.eager_load, p.daemonize, p.daemonize_connections, p.port = True, True, True, 0
    gw.start_callback_server(p)
    port = gw._callback_server.get_listening_port()
    gw._callback_server.port = port
    gw._python_proxy_port = port
    jgws = JavaObject("GATEWAY_SERVER", gw._gateway_client)
    jgws.resetCallbackClient(jgws.getCallbackClient().getAddress(), port)


def path(layer: str, name: str) -> str:
    return str(settings.LAKE / layer / name)


def exists(layer: str, name: str) -> bool:
    p = settings.LAKE / layer / name
    return p.exists() and any(p.iterdir())


def read(layer: str, name: str) -> DataFrame:
    return spark().read.format(settings.TABLE_FORMAT).load(path(layer, name))


def read_or_empty(layer: str, name: str, schema) -> DataFrame:
    if exists(layer, name):
        return read(layer, name)
    return spark().createDataFrame([], schema)


def write(df: DataFrame, layer: str, name: str, mode: str = "overwrite",
          partition_by: list[str] | None = None) -> int:
    """Write a table and return its row count.

    Overwrites are staged to a temp folder and swapped in, so a job may read a table and
    overwrite it in the same run (Spark reads lazily). Delta handles this natively.
    """
    fmt = settings.TABLE_FORMAT
    target = settings.LAKE / layer / name
    target.parent.mkdir(parents=True, exist_ok=True)

    def _writer(frame: DataFrame, dest: str, m: str):
        w = frame.write.format(fmt).mode(m)
        if fmt == "delta":
            w = w.option("overwriteSchema", "true")
            if m == "overwrite":
                # Delta rejects overwriteSchema with the session-wide dynamic partition
                # overwrite; a full replace (like the Parquet staging path) is intended here.
                w = w.option("partitionOverwriteMode", "static")
        if partition_by:
            w = w.partitionBy(*partition_by)
        w.save(dest)

    if mode == "append" or fmt == "delta":
        _writer(df, str(target), mode)
    else:
        staging = target.parent / f".{name}_{uuid.uuid4().hex[:8]}"
        _writer(df, str(staging), "overwrite")
        if target.exists():
            shutil.rmtree(target)
        staging.rename(target)
    return read(layer, name).count()


def latest_per_key(df: DataFrame, keys: list[str], order_cols: list) -> DataFrame:
    """Keep one row per key: the one that sorts first by order_cols (pass desc() columns)."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def upsert(layer: str, name: str, incoming: DataFrame, keys: list[str],
           order_cols: list) -> int:
    """Idempotent upsert (MERGE semantics): newest version per key wins.

    Equivalent Fabric/Delta SQL at scale:
        MERGE INTO t USING s ON t.k = s.k
        WHEN MATCHED AND s.modified_at >= t.modified_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    if exists(layer, name):
        current = read(layer, name)
        combined = current.unionByName(incoming, allowMissingColumns=True)
    else:
        combined = incoming
    return write(latest_per_key(combined, keys, order_cols), layer, name)


def union_all(frames: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


def hash_email(col):
    """Normalize then SHA-256 an email. Silver/gold join on this, never on raw email."""
    return F.sha2(F.lower(F.trim(col)), 256)
