"""Unit tests for the SCD2 merge as a pure function on tiny frames."""
from datetime import datetime

from pyspark.sql import Row


def _row(key, tier, ts, phone="111"):
    return Row(member_key=key, partner_code="BHB", member_natural_id=key, tier=tier, country="US",
               state="AZ", email_hash="h", first_name="A", last_name="B", email="a@b.com", phone=phone,
               email_is_valid=True, created_at=datetime(2020, 1, 1), modified_at=ts)


def _df(spark, rows):
    return spark.createDataFrame(rows)


def test_new_tier_creates_version_type1_does_not(spark):
    from tlake.silver import scd2_merge
    v1 = scd2_merge(None, _df(spark, [_row("k", "BASIC", datetime(2026, 1, 1))])).localCheckpoint()
    assert v1.count() == 1 and str(v1.first()["valid_from"]).startswith("1900")

    # phone correction only -> still one version, phone updated
    v2 = scd2_merge(v1, _df(spark, [_row("k", "BASIC", datetime(2026, 2, 1), phone="999")])).localCheckpoint()
    assert v2.count() == 1 and v2.first()["phone"] == "999"

    # tier change -> two versions, old one closed at the change time
    v3 = scd2_merge(v2, _df(spark, [_row("k", "GOLD", datetime(2026, 3, 1), phone="999")])).localCheckpoint()
    rows = sorted(v3.collect(), key=lambda r: r["valid_from"])
    assert [r["tier"] for r in rows] == ["BASIC", "GOLD"]
    assert rows[0]["valid_to"] == datetime(2026, 3, 1) and not rows[0]["is_current"]
    assert rows[1]["is_current"] and rows[1]["valid_to"] is None


def test_redelivered_row_is_a_noop(spark):
    from tlake.silver import scd2_merge
    first = scd2_merge(None, _df(spark, [_row("k", "BASIC", datetime(2026, 1, 1))])).localCheckpoint()
    again = scd2_merge(first, _df(spark, [_row("k", "BASIC", datetime(2026, 1, 1))])).localCheckpoint()
    assert again.count() == 1
    assert again.first()["member_sk"] == first.first()["member_sk"]


def test_multiple_versions_in_one_batch(spark):
    from tlake.silver import scd2_merge
    out = scd2_merge(None, _df(spark, [_row("k", "BASIC", datetime(2026, 1, 1)),
                                       _row("k", "SILVER", datetime(2026, 2, 1)),
                                       _row("k", "SILVER", datetime(2026, 2, 5)),   # no tracked change
                                       _row("k", "GOLD", datetime(2026, 3, 1))]))
    assert [r["tier"] for r in sorted(out.collect(), key=lambda r: r["valid_from"])] == ["BASIC", "SILVER", "GOLD"]
