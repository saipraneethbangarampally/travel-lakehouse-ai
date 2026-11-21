"""End-to-end correctness of the batch pipeline (bronze -> silver -> gold)."""
from pyspark.sql import functions as F


def test_both_runs_succeed(platform):
    assert platform["day1"]["status"] == "succeeded"
    assert platform["day2"]["status"] == "succeeded"


def test_incremental_day2_only_pulls_changes(platform):
    day1 = {e["source"]: e["rows"] for e in platform["day1"]["bronze"]}
    day2 = {e["source"]: e["rows"] for e in platform["day2"]["bronze"]}
    for src in ("BHB.Members", "SAM.customer", "NWCU.mbr"):
        assert 0 < day2[src] < day1[src] * 0.2, src


def test_scd2_exactly_one_current_version_and_no_gaps(spark):
    from tlake import lake
    m = lake.read("silver", "members")
    per_key = m.groupBy("member_key").agg(F.sum(F.col("is_current").cast("int")).alias("cur"))
    assert per_key.filter("cur <> 1").count() == 0
    # each closed version ends exactly where the next one starts
    from pyspark.sql import Window
    w = Window.partitionBy("member_key").orderBy("valid_from")
    gaps = m.withColumn("next_from", F.lead("valid_from").over(w)).filter(
        F.col("next_from").isNotNull() & (F.col("valid_to") != F.col("next_from")))
    assert gaps.count() == 0
    assert m.groupBy("member_key").count().filter("count > 1").count() > 100  # day-2 tier changes


def test_point_in_time_tier_on_bookings(spark):
    from tlake import lake
    dim = lake.read("gold", "dim_member")
    changed = dim.filter(~F.col("is_current")).select("member_key", F.col("tier").alias("old_tier"),
                                                     F.col("valid_to").alias("changed_at"))
    fb = lake.read("gold", "fact_booking").join(changed, "member_key")
    before = fb.filter(F.col("booking_date").cast("timestamp") < F.col("changed_at"))
    assert before.count() > 0
    assert before.filter(F.col("tier_at_booking") != F.col("old_tier")).count() == 0


def test_late_arriving_members_resolve_orphans(spark):
    from tlake import lake
    q = lake.read("silver", "bookings_quarantine")
    orphans = q.filter(F.array_contains("_failed_rules", "member_exists"))
    # only bookings of the two members held for an unknown country remain orphaned
    nwcu_held = lake.read("silver", "members_quarantine").select("member_key").distinct()
    assert orphans.join(nwcu_held, "member_key", "left_anti").count() == 0
    assert platform_orphans_day1(spark) > orphans.count()


def platform_orphans_day1(spark):
    from tlake import settings
    return [r for r in settings.read_state("run_log", []) if r["batch_id"] == 1][0]["silver"][1]["orphans_now"]


def test_source_fix_leaves_quarantine(spark):
    from tlake import lake
    q = lake.read("silver", "bookings_quarantine")
    assert q.filter(F.array_contains("_failed_rules", "amount_non_negative")).count() == 2  # 3 bad, 1 fixed
    assert lake.read("silver", "bookings").filter("amount_local < 0").count() == 0


def test_amounts_converted_to_usd(spark):
    from tlake import lake
    b = lake.read("silver", "bookings")
    eur = b.filter("currency = 'EUR'").limit(1).collect()
    if eur:
        assert abs(float(eur[0]["amount_usd"]) - float(eur[0]["amount_local"]) * 1.08) < 0.02
    assert b.filter("amount_usd IS NULL").count() == 0


def test_raw_email_only_lives_in_dim_member(spark):
    from tlake import lake
    for layer, name in [("silver", "email_engagement"), ("silver", "consent_latest"),
                        ("silver", "consent_events"), ("gold", "fact_email_engagement"),
                        ("gold", "member_360"), ("gold", "member_contactability")]:
        assert "email" not in lake.read(layer, name).columns, name


def test_rerun_without_new_data_is_idempotent(platform, spark):
    from tlake import lake, pipeline
    before = {(layer, n): lake.read(layer, n).count() for layer, n in
              [("silver", "members"), ("silver", "bookings"), ("silver", "consent_latest"), ("gold", "fact_booking")]}
    pipeline.run()  # >= watermark re-delivers boundary rows; silver must not change
    after = {k: lake.read(*k).count() for k in before}
    assert before == after
