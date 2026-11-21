"""Gold: the ONE curated layer that serves Power BI *and* AI agents.

Star schema (dims + facts) for reporting, plus two wide, documented tables built for
consumers that are not humans in Power BI: ``member_360`` (features) and
``member_contactability`` (consent decision per member). Column meanings, PII tags and
metric definitions live in config/semantic_model.yaml, so the same definitions drive the
semantic model, the governance views and the MCP tools. No second "AI layer" to rebuild.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from . import lake, settings
from .generate import as_of

PRODUCT_FROM_CAMPAIGN = {"CRZ": "CRUISE", "HTL": "HOTEL", "CAR": "CAR", "FLT": "FLIGHT"}


def build() -> dict:
    spark = lake.spark()
    cfg = settings.sources_cfg()
    ref = as_of()
    counts = {}

    # dim_partner ---------------------------------------------------------------------
    dim_partner = spark.createDataFrame([(k, v["name"]) for k, v in cfg["partners"].items()],
                                        ["partner_code", "partner_name"])
    counts["dim_partner"] = lake.write(dim_partner, "gold", "dim_partner")

    # dim_member (SCD2) ---------------------------------------------------------------
    members = lake.read("silver", "members")
    dim_member = members.select("member_sk", "member_key", "partner_code", "member_natural_id",
                                "first_name", "last_name", "email", "phone", "email_is_valid",
                                "tier", "country", "state", "valid_from", "valid_to", "is_current")
    counts["dim_member"] = lake.write(dim_member, "gold", "dim_member")
    dim_member = lake.read("gold", "dim_member")
    current = dim_member.filter("is_current")

    # fact_booking with point-in-time member attributes -------------------------------
    b = lake.read("silver", "bookings").alias("b")
    m = dim_member.alias("m")
    pit = (F.col("b.member_key") == F.col("m.member_key")) & \
          (F.col("b.booking_date").cast("timestamp") >= F.col("m.valid_from")) & \
          (F.col("m.valid_to").isNull() | (F.col("b.booking_date").cast("timestamp") < F.col("m.valid_to")))
    fact_booking = b.join(m, pit, "left").select(
        "b.booking_key", "b.booking_natural_id", "b.partner_code", "b.member_key", "m.member_sk",
        F.col("m.tier").alias("tier_at_booking"), "b.product_type", "b.destination", "b.booking_date",
        "b.travel_date", "b.amount_usd", "b.status",
        (F.col("b.status") == "CANCELLED").alias("is_cancelled"))
    counts["fact_booking"] = lake.write(fact_booking, "gold", "fact_booking", partition_by=["partner_code"])

    # fact_email_engagement -----------------------------------------------------------
    prod_map = F.create_map(*[x for k, v in PRODUCT_FROM_CAMPAIGN.items() for x in (F.lit(k), F.lit(v))])
    member_by_email = current.filter("email_is_valid").select(
        "partner_code", F.sha2(F.col("email"), 256).alias("email_hash"), "member_key")
    eng = (lake.read("silver", "email_engagement")
           .join(member_by_email, ["partner_code", "email_hash"], "left")
           .select("event_id", "partner_code", "member_key", "campaign_id",
                   prod_map[F.element_at(F.split("campaign_id", "-"), 2)].alias("campaign_product"),
                   "activity_type", "activity_ts", F.to_date("activity_ts").alias("activity_date"),
                   F.col("member_key").isNotNull().alias("is_known_member")))
    counts["fact_email_engagement"] = lake.write(eng, "gold", "fact_email_engagement")

    # member_contactability -----------------------------------------------------------
    pol = settings.consent_cfg()
    juris = F.create_map(*[x for k, v in pol["jurisdictions"].items() for x in (F.lit(k), F.lit(v))])
    consent = (lake.read("silver", "consent_latest").filter("channel = 'email'")
               .select("partner_code", "email_hash", F.col("consent").alias("email_consent"),
                       F.col("event_ts").alias("consent_ts"), F.col("source").alias("consent_source")))
    contact = (current.withColumn("email_hash", F.sha2(F.col("email"), 256))
               .join(consent, ["partner_code", "email_hash"], "left")
               .withColumn("consent_model", F.coalesce(juris[F.col("country")], F.lit(pol["default_model"])))
               .withColumn("email_consent", F.coalesce("email_consent", F.lit("unknown")))
               .withColumn("suppression_reason", F.expr("""
                    CASE WHEN NOT email_is_valid OR email IS NULL THEN 'no_valid_email'
                         WHEN email_consent = 'opt_out' THEN 'opted_out'
                         WHEN consent_model = 'opt_in' AND email_consent <> 'opt_in' THEN 'no_opt_in_required_by_jurisdiction'
                    END"""))
               .withColumn("is_email_marketable", F.col("suppression_reason").isNull())
               .select("member_key", "partner_code", "country", "consent_model", "email_consent",
                       "consent_ts", "consent_source", "is_email_marketable", "suppression_reason"))
    counts["member_contactability"] = lake.write(contact, "gold", "member_contactability")
    contact = lake.read("gold", "member_contactability")

    # member_360 ----------------------------------------------------------------------
    ref_lit = F.lit(ref.isoformat()).cast("date")
    fb = lake.read("gold", "fact_booking").filter(~F.col("is_cancelled"))
    bk = fb.groupBy("member_key").agg(
        F.count("*").alias("total_bookings"),
        F.sum("amount_usd").cast("decimal(14,2)").alias("lifetime_value_usd"),
        F.sum(F.when(F.col("product_type") == "CRUISE", 1).otherwise(0)).alias("cruise_bookings"),
        F.sum(F.when(F.col("product_type") == "HOTEL", 1).otherwise(0)).alias("hotel_bookings"),
        F.max("booking_date").alias("last_booking_date"),
        F.sum(F.when((F.col("travel_date") >= ref_lit) & (F.col("status") == "CONFIRMED"), 1)
              .otherwise(0)).alias("upcoming_trips"))
    fe = lake.read("gold", "fact_email_engagement").filter(F.col("is_known_member"))
    days_ago = F.datediff(ref_lit, F.col("activity_date"))
    en = fe.groupBy("member_key").agg(
        F.sum(F.when((F.col("activity_type") == "open") & (days_ago <= 90), 1).otherwise(0)).alias("email_opens_90d"),
        F.sum(F.when((F.col("activity_type") == "click") & (days_ago <= 90), 1).otherwise(0)).alias("email_clicks_90d"),
        F.sum(F.when((F.col("activity_type") == "click") & (F.col("campaign_product") == "CRUISE") & (days_ago <= 180), 1)
              .otherwise(0)).alias("cruise_clicks_180d"))
    m360 = (current.select("member_key", "partner_code", "tier", "country", "state")
            .join(bk, "member_key", "left").join(en, "member_key", "left")
            .fillna(0, ["total_bookings", "cruise_bookings", "hotel_bookings", "upcoming_trips",
                        "email_opens_90d", "email_clicks_90d", "cruise_clicks_180d"])
            .withColumn("lifetime_value_usd", F.coalesce("lifetime_value_usd", F.lit(0).cast("decimal(14,2)")))
            .withColumn("days_since_last_booking", F.datediff(ref_lit, "last_booking_date"))
            .withColumn("cruise_affinity", F.round(
                0.5 * F.least(F.col("cruise_bookings") / 2.0, F.lit(1.0))
                + 0.3 * (F.col("cruise_clicks_180d") > 0).cast("double")
                + 0.2 * F.coalesce((F.col("days_since_last_booking") <= 365).cast("double"), F.lit(0.0)), 2))
            .withColumn("value_segment", F.expr(
                "CASE WHEN lifetime_value_usd >= 8000 THEN 'HIGH' WHEN lifetime_value_usd >= 2500 THEN 'MID' ELSE 'LOW' END"))
            .join(contact.select("member_key", "is_email_marketable"), "member_key", "left")
            .withColumn("as_of_date", ref_lit))
    counts["member_360"] = lake.write(m360, "gold", "member_360")
    return counts
