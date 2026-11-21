"""Silver: one canonical, typed, de-duplicated, quality-checked model across all partners.

* members   -> SCD Type 2 history (tier/country/state/email changes create new versions;
               name/phone are Type 1 corrections on the current version)
* bookings  -> latest version per booking (cancellations update in place), amounts in USD,
               orphans (booking before its member arrived) quarantined and RETRIED each run
* engagement-> Marketo-style activity, email hashed on the way in (raw email stops at bronze)
* web events-> Tealium EventStream; email hashed, cookie consent kept as can_personalize,
               preference-center updates become consent events

Every step only processes bronze batches it has not seen yet (incremental), and every
write is idempotent, so re-running a batch changes nothing.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import lake, quality, settings
from .bronze import bronze_name

TRACKED = ["tier", "country", "state", "email_hash"]       # SCD2: history matters
TYPE1 = ["first_name", "last_name", "email", "phone", "email_is_valid", "created_at"]
BEGINNING_OF_TIME = "1900-01-01 00:00:00"
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def _new_canonical(entity: str, since_batch: int) -> DataFrame | None:
    """Map every partner's new bronze rows for an entity to the canonical contract."""
    frames = []
    for partner, p in settings.sources_cfg()["partners"].items():
        for t in p["tables"]:
            if t["entity"] != entity or not lake.exists("bronze", bronze_name(partner, t["source_table"])):
                continue
            raw = lake.read("bronze", bronze_name(partner, t["source_table"])).filter(
                F.col("_batch_id") > since_batch)
            exprs = [F.expr(e).alias(c) for c, e in t["canonical"].items()]
            frames.append(raw.select(*exprs, F.col("_partner_code").alias("partner_code"),
                                     "_batch_id", "_ingested_at"))
    return lake.union_all(frames) if frames else None


def _progress() -> dict:
    return settings.read_state("silver_progress", {})


def _save_progress(entity: str, batch_id: int):
    prog = _progress()
    prog[entity] = batch_id
    settings.write_state("silver_progress", prog)


# ------------------------------------------------------------------------- members ----

def scd2_merge(existing: DataFrame | None, incoming: DataFrame) -> DataFrame:
    """Pure function: (current SCD2 table, new source rows) -> new SCD2 table."""
    inc = incoming.withColumn("version_ts", F.col("modified_at"))
    keep_cols = ["member_key", "partner_code", "member_natural_id", *TRACKED, *TYPE1, "version_ts"]

    if existing is not None:
        closed = existing.filter(~F.col("is_current"))
        current = existing.filter(F.col("is_current"))
        # ignore re-delivered rows (>= watermark overlap) that are not newer than current
        inc = (inc.join(current.select("member_key", F.col("source_modified_at").alias("_cur")),
                        "member_key", "left")
               .filter(F.col("_cur").isNull() | (F.col("version_ts") > F.col("_cur"))).drop("_cur"))
        cur_versions = current.withColumn("version_ts", F.col("source_modified_at")) \
                              .withColumn("_first", F.col("valid_from") == F.lit(BEGINNING_OF_TIME).cast("timestamp"))
        chain = cur_versions.select(*keep_cols, "_first").unionByName(
            inc.select(*keep_cols).withColumn("_first", F.lit(False)))
    else:
        closed = None
        chain = inc.select(*keep_cols).withColumn("_first", F.lit(False))

    by_key = Window.partitionBy("member_key").orderBy("version_ts")
    chain = lake.latest_per_key(chain, ["member_key", "version_ts"], [F.col("_first").desc()])
    chain = chain.withColumn("attr_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~"))
                                                                   for c in TRACKED]), 256))
    chain = (chain.withColumn("_prev", F.lag("attr_hash").over(by_key))
                  .filter(F.col("_prev").isNull() | (F.col("_prev") != F.col("attr_hash"))).drop("_prev"))
    # an existing chain's first version keeps "beginning of time"; brand-new keys get it too
    chain = chain.withColumn("_is_first", F.row_number().over(by_key) == 1)
    has_history = (existing.select("member_key").distinct().withColumn("_hist", F.lit(True))
                   if existing is not None else None)
    if has_history is not None:
        chain = chain.join(has_history, "member_key", "left")
    else:
        chain = chain.withColumn("_hist", F.lit(None).cast("boolean"))
    chain = chain.withColumn(
        "valid_from",
        F.when(F.col("_first") | (F.col("_is_first") & F.col("_hist").isNull()),
               F.lit(BEGINNING_OF_TIME).cast("timestamp")).otherwise(F.col("version_ts")))
    chain = (chain.withColumn("valid_to", F.lead("valid_from").over(Window.partitionBy("member_key").orderBy("valid_from")))
                  .withColumn("is_current", F.col("valid_to").isNull())
                  .withColumnRenamed("version_ts", "source_modified_at"))

    # Type 1: latest non-tracked values overwrite the current version
    latest_t1 = lake.latest_per_key(
        incoming.select("member_key", "modified_at", *TYPE1), ["member_key"], [F.col("modified_at").desc()]
    ).drop("modified_at")
    t1 = [F.coalesce(F.col(f"_n.{c}"), F.col(f"c.{c}")).alias(c) if c != "created_at"
          else F.col(f"c.{c}") for c in TYPE1]
    cur_rows = chain.filter("is_current").alias("c").join(latest_t1.alias("_n"), "member_key", "left").select(
        *[F.col(f"c.{x}") for x in chain.columns if x not in TYPE1], *t1)
    chain = chain.filter(~F.col("is_current")).unionByName(cur_rows)

    chain = chain.withColumn("member_sk", F.sha2(F.concat_ws("|", "member_key",
                                                             F.col("valid_from").cast("string")), 256))
    cols = ["member_sk", "member_key", "partner_code", "member_natural_id", *TRACKED, *TYPE1,
            "valid_from", "valid_to", "is_current", "source_modified_at", "attr_hash"]
    chain = chain.select(*cols)
    return closed.select(*cols).unionByName(chain) if closed is not None else chain


def build_members(batch_id: int) -> dict:
    since = _progress().get("members", 0)
    inc = _new_canonical("members", since)
    if inc is None or inc.isEmpty():
        return {"entity": "members", "incoming": 0}
    inc = (inc.withColumn("member_key", F.sha2(F.concat_ws("|", "partner_code", "member_natural_id"), 256))
              .withColumn("email", F.lower(F.trim("email")))
              .withColumn("email_is_valid", F.coalesce(F.col("email").rlike(EMAIL_RE), F.lit(False)))
              .withColumn("email_hash", F.when(F.col("email").isNotNull(), lake.hash_email(F.col("email")))))
    inc = lake.latest_per_key(inc, ["member_key", "modified_at"], [F.col("_batch_id").desc()])
    passed, quarantined, _ = quality.apply(inc, "members", batch_id)
    _quarantine("members", quarantined, ["member_key"])

    existing = lake.read("silver", "members") if lake.exists("silver", "members") else None
    if existing is not None:
        existing = existing.localCheckpoint()  # we overwrite the table we read from
    n = lake.write(scd2_merge(existing, passed), "silver", "members")
    _save_progress("members", batch_id)
    return {"entity": "members", "incoming": inc.count(), "quarantined": quarantined.count(),
            "silver_rows": n}


# ------------------------------------------------------------------------- bookings ---

def build_bookings(batch_id: int) -> dict:
    since = _progress().get("bookings", 0)
    inc = _new_canonical("bookings", since)
    fx = settings.sources_cfg()["fx_to_usd"]
    fx_map = F.create_map(*[x for k, v in fx.items() for x in (F.lit(k), F.lit(v))])

    if inc is not None:
        inc = (inc.withColumn("booking_key", F.sha2(F.concat_ws("|", "partner_code", "booking_natural_id"), 256))
                  .withColumn("member_key", F.sha2(F.concat_ws("|", "partner_code", "member_natural_id"), 256)))
    # retry orphans held from previous runs (late-arriving members)
    if lake.exists("silver", "bookings_quarantine"):
        retry = lake.read("silver", "bookings_quarantine").filter(
            F.array_contains("_failed_rules", "member_exists")).drop("_failed_rules", "amount_usd")
        inc = retry if inc is None else inc.unionByName(retry, allowMissingColumns=True)
    if inc is None or inc.isEmpty():
        return {"entity": "bookings", "incoming": 0}

    inc = lake.latest_per_key(inc, ["booking_key"], [F.col("modified_at").desc(), F.col("_batch_id").desc()])
    # materialise: `inc` reads the quarantine table, which is rewritten below
    inc = inc.localCheckpoint()
    processed = inc.count()
    inc = inc.withColumn("amount_usd", F.round(F.col("amount_local") * fx_map[F.col("currency")], 2)
                         .cast("decimal(12,2)"))
    passed, quarantined, _ = quality.apply(inc, "bookings", batch_id)

    members = lake.read("silver", "members").select("member_key").distinct()
    orphans = passed.join(members, "member_key", "left_anti").withColumn(
        "_failed_rules", F.array(F.lit("member_exists")))
    passed = passed.join(members, "member_key", "left_semi")

    n = lake.upsert("silver", "bookings", passed.drop("_ingested_at"), ["booking_key"],
                    [F.col("modified_at").desc(), F.col("_batch_id").desc()])
    held = quarantined.unionByName(orphans)
    _quarantine("bookings", held, ["booking_key"], resolved=passed.select("booking_key"))
    _save_progress("bookings", batch_id)
    return {"entity": "bookings", "processed": processed, "quarantined_now": held.count(),
            "orphans_now": orphans.count(), "silver_rows": n}


def _quarantine(entity: str, rows: DataFrame, keys: list[str], resolved: DataFrame | None = None):
    """Quarantine = latest failing version per key, minus keys that have since passed."""
    name = f"{entity}_quarantine"
    rows = rows.drop("_ingested_at")
    if lake.exists("silver", name):
        prev = lake.read("silver", name)
        if resolved is not None:
            prev = prev.join(resolved, keys, "left_anti")
        # retried orphans are re-evaluated above, so drop their old copies
        prev = prev.join(rows.select(*keys), keys, "left_anti")
        rows = prev.unionByName(rows, allowMissingColumns=True)
    lake.write(rows, "silver", name)


# ------------------------------------------------------------------------- engagement -

def build_engagement(batch_id: int) -> dict:
    since = _progress().get("engagement", 0)
    if not lake.exists("bronze", "marketing_engagement"):
        return {"entity": "engagement", "incoming": 0}
    raw = lake.read("bronze", "marketing_engagement").filter(F.col("_batch_id") > since)
    if raw.isEmpty():
        return {"entity": "engagement", "incoming": 0}
    ev = raw.select(
        F.col("activityId").alias("event_id"),
        F.col("partnerCode").alias("partner_code"),
        F.col("campaignId").alias("campaign_id"),
        F.lower("activityType").alias("activity_type"),
        F.to_timestamp("activityDate", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("activity_ts"),
        lake.hash_email(F.col("leadEmail")).alias("email_hash"),   # raw email dropped here
        "_batch_id",
    )
    n = lake.upsert("silver", "email_engagement", ev, ["event_id"], [F.col("_batch_id").desc()])

    # unsubscribes in the marketing tool are consent events too
    from .consent import apply_consent_events
    unsubs = (ev.filter("activity_type = 'unsubscribe'")
                .select(F.concat(F.lit("UNSUB-"), "event_id").alias("event_id"), "partner_code", "email_hash",
                        F.lit("email").alias("channel"), F.lit("opt_out").alias("consent"),
                        F.lit("marketing_unsubscribe").alias("source"), F.col("activity_ts").alias("event_ts")))
    apply_consent_events(unsubs, batch_id)
    _save_progress("engagement", batch_id)
    return {"entity": "engagement", "incoming": ev.count(), "silver_rows": n}


# ------------------------------------------------------------------------- web events -

def _col_or_null(df: DataFrame, name: str):
    return F.col(name) if name in df.columns else F.lit(None).cast("string")


def build_web_events(batch_id: int) -> dict:
    """Tealium web events -> silver.web_events (+ preference-center consent events)."""
    since = _progress().get("web_events", 0)
    if not lake.exists("bronze", "tealium_events"):
        return {"entity": "web_events", "incoming": 0}
    raw = lake.read("bronze", "tealium_events").filter(F.col("_batch_id") > since)
    if raw.isEmpty():
        return {"entity": "web_events", "incoming": 0}
    profiles = settings.integrations_cfg()["tealium"]["profiles"]
    to_partner = F.create_map(*[x for code, prof in profiles.items() for x in (F.lit(prof), F.lit(code))])
    ev = raw.select(
        F.col("tealium_event_id").alias("event_id"),
        to_partner[F.col("tealium_profile")].alias("partner_code"),
        F.col("tealium_visitor_id").alias("visitor_id"),
        lake.hash_email(_col_or_null(raw, "customer_email")).alias("email_hash"),   # raw email dropped here
        F.lower("tealium_event").alias("event_name"),
        F.upper(_col_or_null(raw, "product_category")).alias("product_type"),
        (F.col("tealium_timestamp_epoch").cast("long") / 1000).cast("timestamp").alias("event_ts"),
        F.array_contains(F.split(F.coalesce(F.col("consent_categories"), F.lit("")), ","), "personalization")
         .alias("can_personalize"),
        F.lower(_col_or_null(raw, "email_marketing_opt_in")).alias("_email_opt_in"),
        "_batch_id",
    )
    unknown = ev.filter(F.col("partner_code").isNull()).count()   # profile not mapped to a partner
    ev = ev.filter(F.col("partner_code").isNotNull())
    n = lake.upsert("silver", "web_events", ev.drop("_email_opt_in"), ["event_id"], [F.col("_batch_id").desc()])

    # an email-preference change made on the website is a consent event like any other
    from .consent import apply_consent_events
    prefs = (ev.filter("event_name = 'email_preferences_update' AND email_hash IS NOT NULL "
                       "AND _email_opt_in IN ('true', 'false')")
               .select(F.concat(F.lit("TEAL-"), "event_id").alias("event_id"), "partner_code", "email_hash",
                       F.lit("email").alias("channel"),
                       F.when(F.col("_email_opt_in") == "true", "opt_in").otherwise("opt_out").alias("consent"),
                       F.lit("tealium_preference_center").alias("source"), "event_ts"))
    apply_consent_events(prefs, batch_id)
    _save_progress("web_events", batch_id)
    return {"entity": "web_events", "incoming": ev.count(), "silver_rows": n,
            "unmapped_profile_rows": unknown, "consent_updates": prefs.count()}


def run(batch_id: int) -> list[dict]:
    return [build_members(batch_id), build_bookings(batch_id), build_engagement(batch_id),
            build_web_events(batch_id)]
