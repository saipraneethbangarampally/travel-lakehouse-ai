"""Consent: a Spark Structured Streaming job that keeps marketing permissions current.

Why streaming: an opt-out must take effect before the next campaign send, not after the
nightly batch. In Fabric this is Eventstream (from Event Hubs / the preference-center app)
into a Lakehouse table; here a landing folder stands in for the event hub and
``trigger(availableNow=True)`` processes whatever has arrived, then stops (same code runs
continuously with ``processingTime``).

Correctness rules (tested in tests/test_consent.py):
  * the LATEST event by event time wins, not the latest to arrive (late CRM backfills
    can't resurrect an opt-out)
  * on an exact timestamp tie the more restrictive choice (opt_out) wins
  * replaying the stream is idempotent (dedupe on event_id; checkpointed offsets)
  * raw email is hashed inside the stream and never persisted
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from . import lake, settings

RAW_SCHEMA = StructType([StructField(c, StringType()) for c in
                         ["event_id", "partner_code", "email", "channel", "consent", "source", "event_ts"]])
CONSENT_COLS = ["event_id", "partner_code", "email_hash", "channel", "consent", "source", "event_ts"]


def apply_consent_events(events: DataFrame, batch_id: int) -> int:
    """Append to the consent audit log and upsert the latest state per member+channel."""
    events = (events.select(*CONSENT_COLS)
              .filter(F.col("consent").isin("opt_in", "opt_out") & F.col("email_hash").isNotNull())
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_priority", F.when(F.col("consent") == "opt_out", 0).otherwise(1)))
    if events.isEmpty():
        return 0
    lake.upsert("silver", "consent_events", events, ["event_id"], [F.col("_batch_id").desc()])
    order = [F.col("event_ts").desc()]
    if settings.consent_cfg().get("restrictive_first", True):
        order.append(F.col("_priority").asc())
    order.append(F.col("event_id").asc())  # fully deterministic
    return lake.upsert("silver", "consent_latest", events, ["partner_code", "email_hash", "channel"], order)


def run_stream(batch_id: int) -> dict:
    landing = settings.SOURCES / settings.sources_cfg()["streams"]["consent_events"]["landing"]
    landing.mkdir(parents=True, exist_ok=True)
    spark = lake.spark()
    stats = {"micro_batches": 0, "events": 0}

    stream = (spark.readStream.schema(RAW_SCHEMA).option("maxFilesPerTrigger", 1).json(str(landing))
              .select("event_id", "partner_code", lake.hash_email(F.col("email")).alias("email_hash"),
                      F.lower("channel").alias("channel"), F.lower("consent").alias("consent"), "source",
                      F.to_timestamp("event_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_ts")))

    def _sink(df: DataFrame, micro_batch_id: int):
        n = df.count()
        if n:
            apply_consent_events(df, batch_id)
            stats["micro_batches"] += 1
            stats["events"] += n

    q = (stream.writeStream.foreachBatch(_sink)
         .option("checkpointLocation", str(settings.CHECKPOINTS / "consent_stream"))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return stats
