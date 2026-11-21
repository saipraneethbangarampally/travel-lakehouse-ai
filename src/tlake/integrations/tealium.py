"""Tealium outbound connector: tell Tealium which visitors qualified for an audience.

Inbound Tealium data (EventStream web events, consent-manager choices, preference-center
updates) is handled by the pipeline: bronze.ingest_files -> silver.build_web_events ->
gold.fact_web_event. This module covers the other direction: after the lakehouse builds a
consented audience, each member is sent to Tealium's HTTP API (Collect) as an event, so
AudienceStream can personalise the website for the same people the email campaign targets.

Only a SHA-256 hash of the normalised email leaves the lakehouse, never the email itself.
Tealium matches it to a visitor through a visitor-ID attribute configured on the hashed
email, so the website and the email channel agree on who is in the audience.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from .. import settings


def email_hash(email: str) -> str:
    """Same normalisation as lake.hash_email: lower(trim(email)), then SHA-256."""
    return hashlib.sha256(email.strip().lower().encode()).hexdigest()


def audience_events(rows: list[dict], campaign_id: str, partner: str) -> list[dict]:
    cfg = settings.integrations_cfg()["tealium"]
    return [{"tealium_account": cfg["account"], "tealium_profile": cfg["profiles"][partner],
             "tealium_event": cfg["audience_event"], "audience_id": campaign_id,
             "customer_email_hash": email_hash(r["email"])} for r in rows if r.get("email")]


class TealiumCollect:
    """Live client for the Tealium Collect HTTP API."""

    mode = "live"

    def __init__(self, url: str, datasource: str, *, transport: httpx.BaseTransport | None = None):
        self.url, self.datasource = url, datasource
        self._http = httpx.Client(transport=transport, timeout=15)

    @classmethod
    def from_env(cls, **kw) -> "TealiumCollect":
        key = os.environ.get("TEALIUM_DATASOURCE_KEY")
        if not key:
            raise RuntimeError("live mode needs environment variable TEALIUM_DATASOURCE_KEY")
        return cls(settings.integrations_cfg()["tealium"]["collect_url"], key, **kw)

    def send(self, events: list[dict]) -> dict[str, int]:
        sent = failed = 0
        for e in events:
            r = self._http.post(self.url, json={**e, "tealium_datasource": self.datasource})
            if r.status_code < 300:
                sent += 1
            else:
                failed += 1
        return {"events_sent": sent, "events_failed": failed}


class DryRunTealium:
    mode = "dry-run"

    def __init__(self):
        self.requests: list[dict] = []

    def send(self, events: list[dict]) -> dict[str, int]:
        self.requests += [{"method": "POST", "url": settings.integrations_cfg()["tealium"]["collect_url"],
                           "body": {**e, "tealium_datasource": "<TEALIUM_DATASOURCE_KEY>"}} for e in events]
        return {"events_sent": len(events), "events_failed": 0}

    def save(self, label: str) -> str:
        out = Path(settings.EXPORTS) / "integrations"
        out.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        path = out / f"tealium_{label}_{ts}.json"
        path.write_text(json.dumps(self.requests, indent=2), encoding="utf-8")
        return str(path)


def send_audience(client: Any, rows: list[dict], campaign_id: str, partner: str) -> dict[str, Any]:
    return client.send(audience_events(rows, campaign_id, partner))
