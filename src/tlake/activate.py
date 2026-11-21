"""Activation automation: one approved campaign -> Marketo list + Tealium audience, in sync.

    python -m tlake.activate SAM-CRZ-CARIB-27Q1 --principal user:mops.sam          # dry run
    python -m tlake.activate SAM-CRZ-CARIB-27Q1 --principal user:mops.sam --live   # real APIs

Steps, in this order on purpose:
  1. audience.export: only marketing_ops, only their own partner, consent RE-CHECKED now
  2. Marketo: push every opt-out as unsubscribed=true (so no Marketo program can email them)
  3. Marketo: sync the static list to exactly the consented audience (adds AND removes)
  4. Tealium: send each member (hashed email only) as an audience event for the website
  5. log counts (never emails) to lakehouse/_state/activation_log.json

Run it on a schedule after every pipeline run and the systems stay in step with consent.
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import audience, governance, serving, settings
from .integrations import marketo, tealium


def _opted_out_emails(partner: str) -> list[str]:
    contact = serving.load("member_contactability")
    out = contact[(contact["partner_code"] == partner) & (contact["suppression_reason"] == "opted_out")]
    members = serving.load("dim_member")
    members = members[members["is_current"] & members["email_is_valid"].fillna(False).astype(bool)]
    return sorted(out.merge(members[["member_key", "email"]], on="member_key")["email"].dropna().unique().tolist())


def activate(campaign_id: str, principal_id: str, *, live: bool = False,
             marketo_client: Any = None, tealium_client: Any = None) -> dict[str, Any]:
    cfg = settings.integrations_cfg()
    exp = audience.export(campaign_id, principal_id)          # raises AccessDenied for anyone else
    path = Path(exp["file"]) if Path(exp["file"]).is_absolute() else settings.HOME / exp["file"]
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    partner = serving.load(audience._table_name(campaign_id))["partner_code"].iloc[0]
    for r in rows:
        r["partner_code"] = partner

    list_id = cfg["marketo"]["lists"].get(campaign_id)
    if list_id is None:
        raise ValueError(f"no Marketo list configured for {campaign_id} in config/integrations.yaml")

    mk = marketo_client or (marketo.MarketoClient.from_env(batch_size=cfg["marketo"]["batch_size"]) if live
                            else marketo.DryRunMarketo(cfg["marketo"]["batch_size"]))
    tl = tealium_client or (tealium.TealiumCollect.from_env() if live else tealium.DryRunTealium())

    result: dict[str, Any] = {"campaign_id": campaign_id, "partner_code": partner, "principal": principal_id,
                              "mode": getattr(mk, "mode", "live"), "consented_rows": len(rows),
                              "removed_at_export": exp["removed_since_build"]}
    if cfg["marketo"].get("sync_unsubscribes", True):
        result["marketo_unsubscribes"] = marketo.push_unsubscribes(mk, _opted_out_emails(partner))
    result["marketo_list"] = marketo.sync_list(mk, list_id, rows)
    result["tealium"] = tealium.send_audience(tl, rows, campaign_id, partner)
    if hasattr(mk, "save"):
        result["marketo_requests_file"] = audience._display(Path(mk.save(campaign_id)))
    if hasattr(tl, "save"):
        result["tealium_requests_file"] = audience._display(Path(tl.save(campaign_id)))
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    settings.write_state("activation_log", settings.read_state("activation_log", []) + [result])
    return result


def recent(who: governance.Principal, n: int = 10) -> list[dict]:
    """Activation history this principal may see (counts only, never contact data)."""
    keep = ("campaign_id", "partner_code", "mode", "consented_rows", "removed_at_export",
            "marketo_unsubscribes", "marketo_list", "tealium", "finished_at")
    rows = [r for r in settings.read_state("activation_log", []) if who.can_see_partner(r["partner_code"])]
    return [{k: r.get(k) for k in keep} for r in rows[-n:]][::-1]


def main():
    ap = argparse.ArgumentParser(description="Sync a consented audience to Marketo and Tealium")
    ap.add_argument("campaign_id")
    ap.add_argument("--principal", required=True, help="a marketing_ops principal, e.g. user:mops.sam")
    ap.add_argument("--live", action="store_true", help="call the real APIs (needs credentials in env vars)")
    a = ap.parse_args()
    print(json.dumps(activate(a.campaign_id, a.principal, live=a.live), indent=2, default=str))


if __name__ == "__main__":
    main()
