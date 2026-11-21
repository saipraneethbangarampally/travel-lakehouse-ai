"""Consent stream semantics: event time wins, restrictive tie-break, idempotent replay."""
from datetime import datetime

from pyspark.sql import functions as F


def _events(spark, rows):
    return spark.createDataFrame(rows, ["event_id", "partner_code", "email_hash", "channel", "consent",
                                        "source", "event_ts"])


def _state(email_hash):
    from tlake import lake
    return lake.read("silver", "consent_latest").filter(F.col("email_hash") == email_hash).collect()


def test_late_backfill_cannot_resurrect_opt_out(spark):
    from tlake.consent import apply_consent_events
    apply_consent_events(_events(spark, [("t1-out", "SAM", "hash-late", "email", "opt_out", "pref",
                                          datetime(2026, 11, 1, 12))]), 900)
    apply_consent_events(_events(spark, [("t1-in", "SAM", "hash-late", "email", "opt_in", "crm_backfill",
                                          datetime(2025, 1, 1))]), 901)
    (row,) = _state("hash-late")
    assert row["consent"] == "opt_out"


def test_same_timestamp_conflict_resolves_to_opt_out(spark):
    from tlake.consent import apply_consent_events
    ts = datetime(2026, 11, 1, 9)
    apply_consent_events(_events(spark, [("t2-in", "SAM", "hash-tie", "email", "opt_in", "form", ts),
                                         ("t2-out", "SAM", "hash-tie", "email", "opt_out", "call", ts)]), 902)
    (row,) = _state("hash-tie")
    assert row["consent"] == "opt_out"


def test_replay_is_idempotent(spark):
    from tlake import lake
    from tlake.consent import apply_consent_events
    ev = _events(spark, [("t3", "BHB", "hash-replay", "email", "opt_in", "form", datetime(2026, 10, 1))])
    apply_consent_events(ev, 903)
    n1 = lake.read("silver", "consent_latest").count()
    e1 = lake.read("silver", "consent_events").count()
    apply_consent_events(ev, 904)
    assert lake.read("silver", "consent_latest").count() == n1
    assert lake.read("silver", "consent_events").count() == e1


def test_generated_stream_edge_cases(platform, spark):
    """Day 2 data contains a 400-day-old backfilled opt_in and a same-second conflict."""
    import sqlite3

    from tlake import settings
    conn = sqlite3.connect(settings.SOURCES / "onprem/bhb.sqlite")
    (late_email,), (tie_email,) = conn.execute(
        "SELECT EmailAddress FROM Members WHERE MemberID IN (1, 2) ORDER BY MemberID").fetchall()
    import hashlib
    for e in (late_email, tie_email):
        (row,) = _state(hashlib.sha256(e.strip().lower().encode()).hexdigest())
        assert row["consent"] == "opt_out"


def test_unsubscribes_from_marketing_tool_become_opt_outs(platform, spark):
    from tlake import lake
    n = lake.read("silver", "consent_latest").filter("source = 'marketing_unsubscribe'").count()
    assert n > 0
