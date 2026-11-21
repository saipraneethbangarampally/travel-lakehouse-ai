"""Consent-aware audience builder: the business use case from the interview.

Marketing describes WHO in a small YAML spec (config/campaigns/*.yaml); the platform
decides who may legally be contacted. The output is a suppression funnel marketing can
trust, plus an export that re-checks consent at activation time (an opt-out that streams
in after the audience was built still wins).

The criteria language is tiny on purpose: a fixed set of keys, each compiled to a
(column, operator, value) triple, with a Spark backend (batch build) and a pandas backend
(the MCP `estimate_audience` tool). tests/test_audience.py checks both agree.
"""
from __future__ import annotations

import argparse
import csv
import json
import operator
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from . import governance, settings

CRITERIA = {  # key -> (column, op, expected python type)
    "min_cruise_affinity": ("cruise_affinity", ">=", float),
    "min_lifetime_value_usd": ("lifetime_value_usd", ">=", float),
    "max_days_since_last_booking": ("days_since_last_booking", "<=", int),
    "min_email_opens_90d": ("email_opens_90d", ">=", int),
    "value_segments": ("value_segment", "in", list),
    "tiers": ("tier", "in", list),
    "countries": ("country", "in", list),
    "exclude_upcoming_trips": ("upcoming_trips", "==0", bool),
    "min_web_cruise_views_30d": ("web_cruise_views_30d", ">=", int),
}
OPS = {">=": operator.ge, "<=": operator.le}


def compile_criteria(criteria: dict) -> list[tuple[str, str, object]]:
    out = []
    for key, value in (criteria or {}).items():
        if key not in CRITERIA:
            raise ValueError(f"unsupported criterion {key!r}; allowed: {sorted(CRITERIA)}")
        col, op, typ = CRITERIA[key]
        if typ is list:
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise ValueError(f"{key} must be a list of strings")
        elif typ is bool:
            if not isinstance(value, bool):
                raise ValueError(f"{key} must be true/false")
            if not value:
                continue
        else:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{key} must be a number")
        out.append((col, op, value))
    return out


def spark_condition(triples):
    from pyspark.sql import functions as F
    cond = F.lit(True)
    for col, op, v in triples:
        c = F.col(col)
        expr = c.isin(v) if op == "in" else (c == 0) if op == "==0" else OPS[op](c, F.lit(v))
        cond = cond & F.coalesce(expr, F.lit(False))
    return cond


def pandas_mask(df: pd.DataFrame, triples) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for col, op, v in triples:
        s = df[col]
        if op == "in":
            m = s.isin(v)
        elif op == "==0":
            m = s == 0
        else:
            m = OPS[op](pd.to_numeric(s, errors="coerce"), v)
        mask &= m.fillna(False).astype(bool)
    return mask


def load_spec(path: str | Path) -> dict:
    spec = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    for k in ("campaign_id", "partner_code", "channel"):
        if k not in spec:
            raise ValueError(f"campaign spec missing {k}")
    if spec["channel"] != "email":
        raise ValueError("only the email channel is modelled in this demo")
    compile_criteria(spec.get("criteria"))
    return spec


def _table_name(campaign_id: str) -> str:
    return "audience__" + campaign_id.lower().replace("-", "_")


def build(spec_path: str | Path) -> dict:
    from pyspark.sql import functions as F

    from . import lake

    spec = load_spec(spec_path)
    triples = compile_criteria(spec.get("criteria"))
    m360 = lake.read("gold", "member_360").filter(F.col("partner_code") == spec["partner_code"])
    contact = lake.read("gold", "member_contactability").select("member_key", "suppression_reason")
    eligible = m360.filter(spark_condition(triples)).join(contact, "member_key", "left")

    funnel = {r["suppression_reason"] or "marketable": r["count"]
              for r in eligible.groupBy("suppression_reason").count().collect()}
    final = (eligible.filter(F.col("is_email_marketable"))
             .select("member_key", "partner_code", "cruise_affinity", "value_segment", "tier", "country")
             .withColumn("campaign_id", F.lit(spec["campaign_id"]))
             .withColumn("built_at", F.current_timestamp()))
    n = lake.write(final, "gold", _table_name(spec["campaign_id"]))
    summary = {
        "campaign_id": spec["campaign_id"], "partner_code": spec["partner_code"],
        "criteria": spec.get("criteria"),
        "partner_members": m360.count(),
        "matched_criteria": sum(funnel.values()),
        "suppressed": {k: v for k, v in funnel.items() if k != "marketable"},
        "final_audience": n,
        "built_at": datetime.now(timezone.utc).isoformat(),
    }
    report = settings.LAKE / "_reports" / f"audience_{spec['campaign_id']}.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def export(campaign_id: str, principal_id: str, out_dir: str | Path | None = None) -> dict:
    """Activation export (email + first name). Only marketing_ops, only own partners,
    and consent is RE-CHECKED against the latest contactability at export time."""
    from . import serving

    who = governance.principal(principal_id)
    if who.role != "marketing_ops":
        raise governance.AccessDenied(f"{principal_id} ({who.role}) cannot export contact lists")
    aud = serving.load(_table_name(campaign_id))
    partner = aud["partner_code"].iloc[0] if len(aud) else None
    if partner and not who.can_see_partner(partner):
        raise governance.AccessDenied(f"{principal_id} cannot export {partner} data")

    contact = serving.load("member_contactability")[["member_key", "is_email_marketable", "suppression_reason"]]
    members = serving.load("dim_member")
    members = members[members["is_current"]][["member_key", "email", "first_name"]]
    joined = aud.merge(contact, on="member_key", how="left")
    late = joined[~joined["is_email_marketable"].fillna(False).astype(bool)]
    ok = joined[joined["is_email_marketable"].fillna(False).astype(bool)].merge(members, on="member_key")

    out_dir = Path(out_dir or settings.EXPORTS)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{campaign_id}.csv"
    ok[["email", "first_name", "campaign_id"]].to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)
    entry = {"campaign_id": campaign_id, "principal": principal_id, "rows": len(ok),
             "removed_since_build": int(len(late)),
             "removed_reasons": late["suppression_reason"].value_counts().to_dict(),
             "file": _display(path), "exported_at": datetime.now(timezone.utc).isoformat()}
    settings.write_state("export_audit", settings.read_state("export_audit", []) + [entry])
    return entry


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(settings.HOME))
    except ValueError:
        return str(path)


def main():
    ap = argparse.ArgumentParser(description="Build or export a consent-aware audience")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("spec")
    e = sub.add_parser("export")
    e.add_argument("campaign_id")
    e.add_argument("--principal", default="user:mops.sam")
    a = ap.parse_args()
    res = build(a.spec) if a.cmd == "build" else export(a.campaign_id, a.principal)
    print(json.dumps(res, indent=2, default=str))


if __name__ == "__main__":
    main()
