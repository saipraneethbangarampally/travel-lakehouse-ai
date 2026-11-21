"""REST API + visual dashboard over the governed serving layer.

The same rules as the MCP server apply, because both call `serving` and `retrieve`:
metrics only from the semantic model, partner row scope, no personal data, small groups
suppressed, every call audit-logged. Nothing here reads raw tables or runs free-form SQL.

Identity: the caller picks a configured principal with the `X-Principal` header (default
`agent:data-copilot`). That is a local-demo stand-in for real authentication; in production
the principal would come from a verified token (Entra ID), never from a header the client sets.

Run:   python -m tlake.api                 -> http://127.0.0.1:8000  (dashboard)
                                           -> http://127.0.0.1:8000/docs  (try the API)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import activate, audience, governance, serving, settings
from .rag import retrieve

WEB = Path(__file__).parent / "web"
DEFAULT_PRINCIPAL = "agent:data-copilot"

app = FastAPI(
    title="Travel Lakehouse API",
    version="1.0.0",
    description=(
        "Governed, read-only access to the travel marketing lakehouse. Pick who you are with the "
        "`X-Principal` header (e.g. `agent:sam-marketing-copilot`) and watch the results change: "
        "partner scope, suppression of small groups and consent rules are enforced server-side."
    ),
)


# ------------------------------------------------------------------ plumbing --------

def current_principal(
    x_principal: Annotated[str, Header(description="Configured principal id")] = DEFAULT_PRINCIPAL,
) -> governance.Principal:
    try:
        return governance.principal(x_principal)
    except governance.AccessDenied as e:
        raise HTTPException(status_code=403, detail=str(e)) from None


Who = Annotated[governance.Principal, Depends(current_principal)]


def _audit(who: governance.Principal, endpoint: str, args: dict, ok: bool, error: str | None = None) -> None:
    settings.STATE.mkdir(parents=True, exist_ok=True)
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "channel": "api", "principal": who.id,
             "endpoint": endpoint, "args": args, "ok": ok, **({"error": error} if error else {})}
    with open(settings.STATE / "api_audit.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def _run(who: governance.Principal, endpoint: str, args: dict, fn) -> Any:
    try:
        out = fn()
    except governance.AccessDenied as e:
        _audit(who, endpoint, args, False, f"AccessDenied: {e}")
        raise HTTPException(status_code=403, detail=str(e)) from None
    except FileNotFoundError as e:
        _audit(who, endpoint, args, False, f"NotBuilt: {e}")
        raise HTTPException(status_code=503, detail=f"{e}. Run `make demo` first.") from None
    except (serving.InvalidRequest, ValueError, KeyError) as e:
        _audit(who, endpoint, args, False, f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from None
    _audit(who, endpoint, args, True)
    return out


# ------------------------------------------------------------------ models ----------

class MetricQuery(BaseModel):
    metric: str = Field(examples=["gross_booking_value_usd"])
    group_by: list[str] = Field(default_factory=list, examples=[["partner_code", "product_type"]])
    filters: dict[str, list[str]] = Field(default_factory=dict, examples=[{"booking_year": ["2026"]}])


class AudienceEstimate(BaseModel):
    partner_code: str = Field(examples=["SAM"])
    criteria: dict[str, Any] = Field(default_factory=dict,
                                     examples=[{"min_cruise_affinity": 0.3, "value_segments": ["MID", "HIGH"]}])


# ------------------------------------------------------------------ endpoints -------

@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(WEB / "index.html")


@app.get("/api/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok", "table_format": settings.TABLE_FORMAT}


@app.get("/api/principals", tags=["meta"])
def principals() -> list[dict]:
    """The identities this demo can act as, with their role and partner scope."""
    return [{"id": pid, "role": p["role"], "partners": p["partners"]}
            for pid, p in settings.semantic_cfg()["principals"].items()]


@app.get("/api/me", tags=["meta"])
def me(who: Who) -> dict:
    """Who the API thinks you are, and what that role is allowed to do."""
    return {"id": who.id, "role": who.role, "partners": list(who.partners), "policy": who.policy}


@app.get("/api/freshness", tags=["data"])
def freshness(who: Who) -> dict:
    """As-of date, last successful pipeline run and open data-quality issues."""
    return _run(who, "freshness", {}, serving.data_freshness)


@app.get("/api/metrics", tags=["data"])
def metrics() -> list[dict]:
    """All governed metrics, with definitions and the dimensions you can group by."""
    dims = settings.semantic_cfg()["dimensions"]
    return [{**m, "dimensions": dims.get(m["dataset"], [])} for m in serving.list_metrics()]


@app.get("/api/datasets", tags=["data"])
def datasets(who: Who) -> list[dict]:
    """Curated gold datasets this principal may use."""
    return _run(who, "datasets", {}, lambda: serving.list_datasets(who))


@app.get("/api/datasets/{name}", tags=["data"])
def dataset(name: str, who: Who) -> dict:
    """Columns this principal can see (PII and confidential columns are withheld for agents)."""
    return _run(who, "dataset", {"name": name}, lambda: serving.describe_dataset(name, who))


@app.post("/api/query", tags=["data"])
def query(q: MetricQuery, who: Who) -> dict:
    """Compute a governed metric, optionally grouped (max 3 dimensions) and filtered."""
    return _run(who, "query", q.model_dump(),
                lambda: serving.query_metric(q.metric, who, q.group_by or None, q.filters or None))


@app.get("/api/audience/criteria", tags=["audience"])
def audience_criteria() -> list[str]:
    """Criteria keys the audience builder understands (no SQL from users)."""
    return sorted(audience.CRITERIA)


@app.post("/api/audience/estimate", tags=["audience"])
def audience_estimate(a: AudienceEstimate, who: Who) -> dict:
    """Size a consent-aware email audience for ONE partner. Counts only, never a member list."""
    return _run(who, "audience_estimate", a.model_dump(),
                lambda: serving.estimate_audience(a.partner_code, a.criteria, who))


@app.get("/api/knowledge/search", tags=["knowledge"])
def knowledge_search(q: Annotated[str, Query(min_length=1, max_length=1000, examples=["Can we email members in Germany?"])],
                     who: Who) -> dict:
    """Search brand guidelines and policies (only documents this principal may see)."""
    def run():
        res = retrieve.search(q, who)
        return {"question": q,
                "answer_found": bool(res["results"]),
                "passages": [{"citation": r["chunk_id"], "source": f"{r['title']} / {r['section']}",
                              "score": r["score"], "untrusted_text": r["text"]} for r in res["results"]],
                "quarantined_passages_withheld": res["quarantined_chunks_skipped"]}
    return _run(who, "knowledge_search", {"q": q}, run)


@app.get("/api/integrations", tags=["automations"])
def integrations(who: Who) -> dict:
    """Marketo and Tealium connection mode, and recent activations (counts only)."""
    cfg = settings.integrations_cfg()
    mk_live = all(os.environ.get(v) for v in ("MARKETO_BASE_URL", "MARKETO_CLIENT_ID", "MARKETO_CLIENT_SECRET"))
    tl_live = bool(os.environ.get("TEALIUM_DATASOURCE_KEY"))
    lists = {c: i for c, i in cfg["marketo"]["lists"].items() if who.can_see_partner(c.split("-")[0])}
    profiles = {p: v for p, v in cfg["tealium"]["profiles"].items() if who.can_see_partner(p)}
    return _run(who, "integrations", {}, lambda: {
        "marketo": {"mode": "live" if mk_live else "dry-run", "lists": lists},
        "tealium": {"mode": "live" if tl_live else "dry-run", "account": cfg["tealium"]["account"], "profiles": profiles},
        "recent_activations": activate.recent(who)})


def main() -> None:
    import uvicorn
    host = os.environ.get("TLAKE_API_HOST", "127.0.0.1")
    port = int(os.environ.get("TLAKE_API_PORT", "8000"))
    print(f"Dashboard: http://{host}:{port}/   API docs: http://{host}:{port}/docs")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
