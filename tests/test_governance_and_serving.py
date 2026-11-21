"""Column/row security and the governed metric API that AI agents use."""
import pytest


def P(pid):
    from tlake import governance
    return governance.principal(pid)


def test_ai_agent_never_sees_pii_or_confidential_columns():
    from tlake import governance
    cols = governance.visible_columns("member_360", P("agent:data-copilot"))
    assert "member_key" not in cols
    assert governance.can_read_dataset("dim_member", P("agent:data-copilot")) is False


def test_analyst_sees_masked_pii_for_own_partner_only(platform):
    from tlake import governance, serving
    df = governance.secure_frame(serving.load("dim_member"), "dim_member", P("user:analyst.bhb"))
    assert set(df["partner_code"]) == {"BHB"}
    assert df["email"].str.contains(r"\*\*\*@").all()
    assert not df["first_name"].str.len().gt(4).any()


def test_unknown_principal_denied():
    from tlake import governance
    with pytest.raises(governance.AccessDenied):
        governance.principal("agent:made-up")


def test_metric_matches_spark_ground_truth(platform, spark):
    from pyspark.sql import functions as F

    from tlake import lake, serving
    res = serving.query_metric("gross_booking_value_usd", P("agent:data-copilot"), ["partner_code"])
    api = {r["partner_code"]: r["value"] for r in res["rows"]}
    truth = {r["partner_code"]: round(float(r["v"]), 2) for r in lake.read("gold", "fact_booking")
             .filter(~F.col("is_cancelled")).groupBy("partner_code").agg(F.sum("amount_usd").alias("v")).collect()}
    assert api == pytest.approx(truth, abs=0.05)


def test_partner_scoped_agent_cannot_see_other_partners(platform):
    from tlake import governance, serving
    res = serving.query_metric("bookings", P("agent:sam-marketing-copilot"), ["partner_code"])
    assert {r["partner_code"] for r in res["rows"]} == {"SAM"}
    with pytest.raises(governance.AccessDenied):
        serving.estimate_audience("BHB", {"min_cruise_affinity": 0.2}, P("agent:sam-marketing-copilot"))


def test_small_groups_are_suppressed(platform):
    from tlake import serving
    res = serving.query_metric("bookings", P("agent:data-copilot"),
                               ["destination", "tier_at_booking", "booking_month"])
    assert res["suppressed_groups"] > 0
    assert all(r["value"] is None for r in res["rows"] if r.get("suppressed"))


@pytest.mark.parametrize("bad", [
    dict(metric="DROP TABLE"),
    dict(metric="bookings", group_by=["member_key"]),           # not an allowed dimension
    dict(metric="bookings", group_by=["a", "b", "c", "d"]),
    dict(metric="bookings", filters={"email": ["x"]}),
])
def test_invalid_requests_rejected(platform, bad):
    from tlake import serving
    with pytest.raises(serving.InvalidRequest):
        serving.query_metric(who=P("agent:data-copilot"), **bad)


def test_filter_values_are_literals_not_code(platform):
    from tlake import serving
    res = serving.query_metric("bookings", P("agent:data-copilot"),
                               filters={"product_type": ["CRUISE' OR '1'='1"]})
    assert res["rows"][0]["value"] in (0, None)


def test_fabric_sql_generated_from_semantic_model():
    from tlake.governance import render_fabric_sql
    sql = render_fabric_sql()
    assert "ALTER TABLE gold.dim_member ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()')" in sql
    assert "CREATE SECURITY POLICY security.partner_isolation" in sql
