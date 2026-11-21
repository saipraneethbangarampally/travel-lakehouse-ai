"""Adobe Marketo Engage REST connector: keep a static list in sync with a consented audience.

Endpoints used (Marketo REST API v1):
  GET    /identity/oauth/token?grant_type=client_credentials   access token (expires ~1h)
  POST   /rest/v1/leads.json                                   createOrUpdate leads (<=300)
  GET    /rest/v1/lists/{listId}/leads.json                    current list members (paged)
  POST   /rest/v1/lists/{listId}/leads.json                    add leads to the list (<=300)
  DELETE /rest/v1/lists/{listId}/leads.json                    remove leads from the list (<=300)

Behaviour that matters in production:
  * the token is sent as an Authorization header (not a query parameter) and refreshed on
    601/602 (invalid / expired)
  * 606 (rate limit), 615 (concurrency limit), 604 (timeout) and HTTP 5xx are retried with
    exponential backoff; anything else fails loudly
  * per-lead results are checked: "skipped" leads are reported, never silently dropped
  * SYNC, not append: members who left the audience (for example they opted out since the
    last run) are removed from the list, so suppressions reach Marketo too
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx

from .. import settings

RETRY_CODES = {"604", "606", "615"}
TOKEN_CODES = {"601", "602"}


class MarketoError(RuntimeError):
    pass


def _chunks(items: list, n: int):
    for i in range(0, len(items), n):
        yield items[i:i + n]


class MarketoClient:
    """Live client. Credentials come from the environment, never from config or arguments
    a caller might log."""

    def __init__(self, base_url: str, client_id: str, client_secret: str, *,
                 transport: httpx.BaseTransport | None = None, max_retries: int = 4,
                 sleep: Callable[[float], None] = time.sleep, batch_size: int = 300):
        self.base_url = base_url.rstrip("/")
        self._client_id, self._client_secret = client_id, client_secret
        self._http = httpx.Client(base_url=self.base_url, transport=transport, timeout=30)
        self._token: str | None = None
        self._token_expires = 0.0
        self.max_retries, self.sleep, self.batch_size = max_retries, sleep, batch_size
        self.calls = 0

    @classmethod
    def from_env(cls, **kw) -> "MarketoClient":
        missing = [v for v in ("MARKETO_BASE_URL", "MARKETO_CLIENT_ID", "MARKETO_CLIENT_SECRET") if not os.environ.get(v)]
        if missing:
            raise MarketoError(f"live mode needs environment variables: {', '.join(missing)}")
        return cls(os.environ["MARKETO_BASE_URL"], os.environ["MARKETO_CLIENT_ID"], os.environ["MARKETO_CLIENT_SECRET"], **kw)

    # ---------------------------------------------------------------- plumbing --------
    def _get_token(self, force: bool = False) -> str:
        if self._token and not force and time.time() < self._token_expires - 60:
            return self._token
        r = self._http.get("/identity/oauth/token", params={
            "grant_type": "client_credentials", "client_id": self._client_id, "client_secret": self._client_secret})
        r.raise_for_status()
        body = r.json()
        if "access_token" not in body:
            raise MarketoError(f"token request failed: {body.get('error_description') or body}")
        self._token = body["access_token"]
        self._token_expires = time.time() + int(body.get("expires_in", 3600))
        return self._token

    def _call(self, method: str, path: str, *, params: dict | None = None, body: dict | None = None) -> dict:
        force = False
        for attempt in range(self.max_retries + 1):
            headers = {"Authorization": f"Bearer {self._get_token(force)}"}
            force = False
            self.calls += 1
            r = self._http.request(method, path, params=params, json=body, headers=headers)
            if r.status_code >= 500:
                code = f"http{r.status_code}"
            else:
                r.raise_for_status()
                data = r.json()
                if data.get("success"):
                    return data
                errors = data.get("errors") or [{}]
                code = str(errors[0].get("code"))
                if code in TOKEN_CODES:
                    force = True
                    continue
                if code not in RETRY_CODES:
                    raise MarketoError(f"{method} {path} failed: {errors}")
            if attempt == self.max_retries:
                raise MarketoError(f"{method} {path} still failing after {attempt + 1} attempts ({code})")
            self.sleep(min(2 ** attempt, 20))
        raise MarketoError(f"{method} {path}: token kept being rejected")

    # ---------------------------------------------------------------- API -------------
    def upsert_leads(self, leads: list[dict], lookup_field: str = "email") -> list[dict]:
        out = []
        for batch in _chunks(leads, self.batch_size):
            data = self._call("POST", "/rest/v1/leads.json",
                              body={"action": "createOrUpdate", "lookupField": lookup_field, "input": batch})
            out += data.get("result", [])
        return out

    def list_members(self, list_id: int) -> list[dict]:
        members, token = [], None
        while True:
            params = {"batchSize": self.batch_size, "fields": "id,email"}
            if token:
                params["nextPageToken"] = token
            data = self._call("GET", f"/rest/v1/lists/{list_id}/leads.json", params=params)
            members += data.get("result", [])
            token = data.get("nextPageToken")
            if not token or not data.get("result"):
                return members

    def add_to_list(self, list_id: int, lead_ids: list[int]) -> int:
        for batch in _chunks(lead_ids, self.batch_size):
            self._call("POST", f"/rest/v1/lists/{list_id}/leads.json", body={"input": [{"id": i} for i in batch]})
        return len(lead_ids)

    def remove_from_list(self, list_id: int, lead_ids: list[int]) -> int:
        for batch in _chunks(lead_ids, self.batch_size):
            self._call("DELETE", f"/rest/v1/lists/{list_id}/leads.json", body={"input": [{"id": i} for i in batch]})
        return len(lead_ids)


class DryRunMarketo:
    """Same interface; records every request and keeps list membership in a local state file
    so repeated dry runs show additions AND removals."""

    mode = "dry-run"

    def __init__(self, batch_size: int = 300):
        self.batch_size = batch_size
        self.requests: list[dict] = []
        self._state = settings.read_state("marketo_dryrun_lists", {})

    @staticmethod
    def _lead_id(email: str) -> int:
        return int(hashlib.sha256(email.strip().lower().encode()).hexdigest()[:7], 16)

    def _record(self, method, path, body=None, params=None):
        self.requests.append({"method": method, "path": path, **({"params": params} if params else {}),
                              **({"body": body} if body is not None else {})})

    def upsert_leads(self, leads, lookup_field="email"):
        for batch in _chunks(leads, self.batch_size):
            self._record("POST", "/rest/v1/leads.json", {"action": "createOrUpdate", "lookupField": lookup_field, "input": batch})
        return [{"id": self._lead_id(lead[lookup_field]), "status": "updated"} for lead in leads]

    def list_members(self, list_id):
        self._record("GET", f"/rest/v1/lists/{list_id}/leads.json", params={"batchSize": self.batch_size, "fields": "id,email"})
        return [{"id": i} for i in self._state.get(str(list_id), [])]

    def add_to_list(self, list_id, lead_ids):
        for batch in _chunks(lead_ids, self.batch_size):
            self._record("POST", f"/rest/v1/lists/{list_id}/leads.json", {"input": [{"id": i} for i in batch]})
        cur = set(self._state.get(str(list_id), []))
        self._state[str(list_id)] = sorted(cur | set(lead_ids))
        return len(lead_ids)

    def remove_from_list(self, list_id, lead_ids):
        for batch in _chunks(lead_ids, self.batch_size):
            self._record("DELETE", f"/rest/v1/lists/{list_id}/leads.json", {"input": [{"id": i} for i in batch]})
        cur = set(self._state.get(str(list_id), []))
        self._state[str(list_id)] = sorted(cur - set(lead_ids))
        return len(lead_ids)

    def save(self, label: str) -> str:
        settings.write_state("marketo_dryrun_lists", self._state)
        out = Path(settings.EXPORTS) / "integrations"
        out.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        path = out / f"marketo_{label}_{ts}.json"
        path.write_text(json.dumps(self.requests, indent=2), encoding="utf-8")
        return str(path)


# -------------------------------------------------------------------- sync logic ------

def _leads(rows: list[dict], fields: dict[str, str]) -> list[dict]:
    return [{mk: r[col] for col, mk in fields.items() if r.get(col) not in (None, "")} for r in rows]


def sync_list(client: Any, list_id: int, rows: list[dict]) -> dict[str, Any]:
    """Make the Marketo list contain exactly `rows` (already consent-checked)."""
    cfg = settings.integrations_cfg()["marketo"]
    results = client.upsert_leads(_leads(rows, cfg["lead_fields"]), cfg["lookup_field"])
    ok_ids = [r["id"] for r in results if r.get("status") in ("created", "updated") and "id" in r]
    skipped = [r for r in results if r.get("status") not in ("created", "updated")]
    current = {m["id"] for m in client.list_members(list_id)}
    target = set(ok_ids)
    to_add = sorted(target - current)
    to_remove = sorted(current - target)
    if to_add:
        client.add_to_list(list_id, to_add)
    if to_remove:
        client.remove_from_list(list_id, to_remove)
    return {"list_id": list_id, "leads_upserted": len(ok_ids), "leads_skipped": len(skipped),
            "skip_reasons": sorted({str(r.get("reasons")) for r in skipped})[:5],
            "added_to_list": len(to_add), "removed_from_list": len(to_remove),
            "list_size_after": len(target)}


def push_unsubscribes(client: Any, emails: list[str]) -> dict[str, int]:
    """Opt-outs recorded in the lakehouse (preference center, Tealium, call center...) are
    written to Marketo's own `unsubscribed` flag so no Marketo program can email them."""
    if not emails:
        return {"unsubscribes_pushed": 0}
    res = client.upsert_leads([{"email": e, "unsubscribed": True} for e in emails], "email")
    return {"unsubscribes_pushed": sum(r.get("status") in ("created", "updated") for r in res)}
