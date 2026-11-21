"""Audience builder: criteria DSL safety, Spark/pandas parity, consent at build AND export."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F


def test_criteria_rejects_unknown_and_badly_typed():
    from tlake.audience import compile_criteria
    with pytest.raises(ValueError):
        compile_criteria({"sql": "1=1"})
    with pytest.raises(ValueError):
        compile_criteria({"min_cruise_affinity": "0.3 OR 1=1"})
    with pytest.raises(ValueError):
        compile_criteria({"countries": "US"})
    assert compile_criteria({"exclude_upcoming_trips": False}) == []


@pytest.mark.parametrize("criteria", [
    {"min_cruise_affinity": 0.3, "value_segments": ["MID", "HIGH"]},
    {"tiers": ["GOLD"], "max_days_since_last_booking": 365, "exclude_upcoming_trips": True},
    {"countries": ["DE", "FR"], "min_email_opens_90d": 1},
])
def test_spark_and_pandas_backends_agree(spark, criteria):
    from tlake import audience, lake, serving
    triples = audience.compile_criteria(criteria)
    n_spark = lake.read("gold", "member_360").filter(audience.spark_condition(triples)).count()
    pdf = serving.load("member_360")
    assert n_spark == int(audience.pandas_mask(pdf, triples).sum())


def test_audience_is_single_partner_and_marketable(platform, spark):
    from tlake import lake
    aud = lake.read("gold", "audience__sam_crz_carib_27q1")
    assert {r["partner_code"] for r in aud.select("partner_code").distinct().collect()} == {"SAM"}
    contact = lake.read("gold", "member_contactability")
    assert aud.join(contact, "member_key").filter(~F.col("is_email_marketable")).count() == 0
    s = platform["audience"]
    assert s["final_audience"] + sum(s["suppressed"].values()) == s["matched_criteria"]


def test_export_rechecks_consent_at_activation(platform, spark, tmp_path):
    """An opt-out that streams in AFTER the audience was built must still be honoured."""
    from tlake import audience, gold, lake
    from tlake.consent import apply_consent_events
    victim = lake.read("gold", "audience__sam_crz_carib_27q1").first()["member_key"]
    email = lake.read("gold", "dim_member").filter(F.col("member_key") == victim).filter("is_current").first()["email"]
    ev = spark.createDataFrame([("late-opt-out", "SAM", email, "email", "opt_out", "preference_center",
                                 datetime(2026, 11, 2, 1, 59))],
                               ["event_id", "partner_code", "email", "channel", "consent", "source", "event_ts"])
    apply_consent_events(ev.withColumn("email_hash", F.sha2(F.lower(F.trim("email")), 256)), 950)
    gold.build()  # contactability refresh (in prod: the stream updates it continuously)

    res = audience.export("SAM-CRZ-CARIB-27Q1", "user:mops.sam", out_dir=tmp_path)
    assert res["removed_since_build"] >= 1 and res["removed_reasons"].get("opted_out", 0) >= 1
    assert email not in (tmp_path / "SAM-CRZ-CARIB-27Q1.csv").read_text()


@pytest.mark.parametrize("who", ["user:analyst.bhb", "agent:data-copilot"])
def test_export_requires_marketing_ops_role(platform, who, tmp_path):
    from tlake import audience, governance
    with pytest.raises(governance.AccessDenied):
        audience.export("SAM-CRZ-CARIB-27Q1", who, out_dir=tmp_path)
