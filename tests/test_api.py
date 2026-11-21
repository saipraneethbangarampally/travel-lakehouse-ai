"""REST API: same governance as the MCP tools, exposed over HTTP with a dashboard."""
import pytest


@pytest.fixture(scope="module")
def client(platform):
    from fastapi.testclient import TestClient

    from tlake.api import app
    return TestClient(app)


def H(pid):
    return {"X-Principal": pid}


def test_dashboard_and_docs_are_served(client):
    assert "Travel Lakehouse" in client.get("/").text
    assert client.get("/openapi.json").status_code == 200


def test_unknown_principal_is_forbidden(client):
    assert client.get("/api/me", headers=H("agent:made-up")).status_code == 403


def test_query_is_partner_scoped(client):
    body = {"metric": "gross_booking_value_usd", "group_by": ["partner_code"]}
    everyone = client.post("/api/query", json=body, headers=H("agent:data-copilot")).json()
    sam_only = client.post("/api/query", json=body, headers=H("agent:sam-marketing-copilot")).json()
    assert {r["partner_code"] for r in everyone["rows"]} == {"BHB", "SAM", "NWCU"}
    assert {r["partner_code"] for r in sam_only["rows"]} == {"SAM"}


def test_bad_requests_are_rejected_not_executed(client):
    r = client.post("/api/query", json={"metric": "drop table members"}, headers=H("agent:data-copilot"))
    assert r.status_code == 400
    r = client.post("/api/query", json={"metric": "bookings", "group_by": ["email"]}, headers=H("agent:data-copilot"))
    assert r.status_code == 400


def test_audience_estimate_counts_only_and_respects_scope(client):
    body = {"partner_code": "SAM", "criteria": {"min_cruise_affinity": 0.3}}
    r = client.post("/api/audience/estimate", json=body, headers=H("agent:sam-marketing-copilot"))
    assert r.status_code == 200
    out = r.json()
    assert {"matched_criteria", "marketable", "suppressed"} <= set(out)
    assert not any(isinstance(v, list) for v in out.values())  # counts only, never a member list
    denied = client.post("/api/audience/estimate", json={**body, "partner_code": "BHB"},
                         headers=H("agent:sam-marketing-copilot"))
    assert denied.status_code == 403


def test_agents_never_see_pii_or_confidential_columns(client):
    r = client.get("/api/datasets/member_360", headers=H("agent:data-copilot")).json()
    assert "member_key" not in r["columns"] and r["withheld_columns"] > 0
    assert client.get("/api/datasets/dim_member", headers=H("agent:data-copilot")).status_code == 403


def test_knowledge_search_is_acl_filtered(client):
    q = {"q": "BlueHarbor brand guidelines tone of voice"}
    sam = client.get("/api/knowledge/search", params=q, headers=H("agent:sam-marketing-copilot")).json()
    assert all("BlueHarbor" not in p["source"] and "bhb" not in p["citation"].lower() for p in sam["passages"])
