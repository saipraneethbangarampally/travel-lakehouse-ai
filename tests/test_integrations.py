"""Tealium (inbound web events + consent, outbound audience) and Marketo (list sync)."""
import json

import httpx
import pytest

# ------------------------------------------------------------------ fake Marketo -------

class FakeMarketo:
    """Just enough of the Marketo REST API to exercise the client: tokens, errors, paging."""

    def __init__(self, members=(), fail_first_with=None):
        self.members = set(members)
        self.leads = {}
        self.tokens = 0
        self.fail_queue = list(fail_first_with or [])
        self.seen_auth = []
        self.batches = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/identity/oauth/token":
            self.tokens += 1
            return httpx.Response(200, json={"access_token": f"tok{self.tokens}", "expires_in": 3600})
        self.seen_auth.append(request.headers.get("Authorization"))
        assert "access_token" not in str(request.url), "token must not travel in the URL"
        if self.fail_queue:
            code = self.fail_queue.pop(0)
            return httpx.Response(200, json={"success": False, "errors": [{"code": code, "message": "x"}]})
        body = json.loads(request.content) if request.content else {}
        if path == "/rest/v1/leads.json":
            self.batches.append(len(body["input"]))
            out = []
            for lead in body["input"]:
                if "@" not in lead.get("email", ""):
                    out.append({"status": "skipped", "reasons": [{"code": "1003", "message": "bad email"}]})
                    continue
                lid = self.leads.setdefault(lead["email"], len(self.leads) + 1)
                out.append({"id": lid, "status": "updated"})
            return httpx.Response(200, json={"success": True, "result": out})
        if path.endswith("/leads.json") and request.method == "GET":
            ids = sorted(self.members)
            return httpx.Response(200, json={"success": True, "result": [{"id": i} for i in ids]})
        if path.endswith("/leads.json") and request.method == "POST":
            self.members |= {x["id"] for x in body["input"]}
            return httpx.Response(200, json={"success": True, "result": []})
        if path.endswith("/leads.json") and request.method == "DELETE":
            self.members -= {x["id"] for x in body["input"]}
            return httpx.Response(200, json={"success": True, "result": []})
        return httpx.Response(404)


def _client(fake, **kw):
    from tlake.integrations.marketo import MarketoClient
    return MarketoClient("https://123-ABC-456.mktorest.com", "id", "secret",
                         transport=httpx.MockTransport(fake), sleep=lambda s: None, **kw)


def test_marketo_refreshes_expired_token_and_retries_rate_limits():
    fake = FakeMarketo(fail_first_with=["602", "606", "615"])
    c = _client(fake)
    res = c.upsert_leads([{"email": "a@example.com"}])
    assert res[0]["status"] == "updated"
    assert fake.tokens == 2                                   # refreshed after 602
    assert all(a and a.startswith("Bearer ") for a in fake.seen_auth)


def test_marketo_gives_up_on_hard_errors():
    from tlake.integrations.marketo import MarketoError
    c = _client(FakeMarketo(fail_first_with=["1003"]))
    with pytest.raises(MarketoError):
        c.upsert_leads([{"email": "a@example.com"}])


def test_marketo_batches_at_300():
    fake = FakeMarketo()
    _client(fake).upsert_leads([{"email": f"p{i}@example.com"} for i in range(650)])
    assert fake.batches == [300, 300, 50]


def test_sync_list_adds_new_members_and_removes_people_who_left():
    from tlake.integrations import marketo
    fake = FakeMarketo(members={999})                         # 999 opted out since the last sync
    out = marketo.sync_list(_client(fake), 1001, [
        {"email": "a@example.com", "first_name": "A", "partner_code": "SAM"},
        {"email": "b@example.com", "first_name": "B", "partner_code": "SAM"},
        {"email": "not-an-email", "first_name": "C", "partner_code": "SAM"}])
    assert out["added_to_list"] == 2 and out["removed_from_list"] == 1 and out["leads_skipped"] == 1
    assert fake.members == {1, 2}


def test_tealium_audience_events_carry_only_a_hash():
    from tlake.integrations import tealium
    ev = tealium.audience_events([{"email": " Ava.Smith@Example.com "}], "SAM-CRZ-CARIB-27Q1", "SAM")
    assert ev[0]["customer_email_hash"] == tealium.email_hash("ava.smith@example.com")
    assert "@" not in json.dumps(ev) and ev[0]["tealium_profile"] == "sam-web"


def test_tealium_collect_posts_with_datasource():
    from tlake.integrations import tealium
    got = []

    def handler(request):
        got.append(json.loads(request.content))
        return httpx.Response(200)

    c = tealium.TealiumCollect("https://collect.tealiumiq.com/event", "ds123", transport=httpx.MockTransport(handler))
    assert c.send([{"tealium_event": "x"}]) == {"events_sent": 1, "events_failed": 0}
    assert got[0]["tealium_datasource"] == "ds123"


# ------------------------------------------------------------------ pipeline ----------

def test_tealium_events_land_without_raw_email(spark):
    from tlake import lake
    w = lake.read("silver", "web_events")
    assert w.count() > 1000 and "customer_email" not in w.columns and "email" not in w.columns
    assert w.filter("partner_code IS NULL").count() == 0
    assert "email" not in lake.read("gold", "fact_web_event").columns


def test_visitors_are_stitched_to_members(spark):
    from tlake import lake
    fw = lake.read("gold", "fact_web_event")
    known = fw.filter("is_known_member")
    # more events are known than carried an email: logged-out visits are stitched by visitor id
    with_email = lake.read("silver", "web_events").filter("email_hash IS NOT NULL").count()
    assert known.count() > with_email
    assert fw.filter("visitor_id LIKE '%anon%' AND is_known_member").count() == 0


def test_declined_personalization_never_feeds_member_features(spark):
    from pyspark.sql import functions as F

    from tlake import lake
    fw = lake.read("gold", "fact_web_event")
    declined = (fw.filter("is_known_member AND NOT can_personalize").select("member_key").distinct()
                  .join(fw.filter("can_personalize").select("member_key").distinct(), "member_key", "left_anti"))
    m = lake.read("gold", "member_360").join(declined, "member_key")
    assert m.count() > 0
    assert m.agg(F.max("web_cruise_views_30d")).first()[0] == 0


def test_tealium_preference_center_feeds_consent(spark):
    from tlake import lake
    src = {r["source"] for r in lake.read("silver", "consent_events").select("source").distinct().collect()}
    assert "tealium_preference_center" in src


def test_activation_dry_run_syncs_and_is_role_checked(platform):
    from tlake import activate, governance
    first = activate.activate("SAM-CRZ-CARIB-27Q1", "user:mops.sam")
    assert first["mode"] == "dry-run"
    assert first["marketo_list"]["list_size_after"] == first["consented_rows"] > 0
    assert first["tealium"]["events_sent"] == first["consented_rows"]
    again = activate.activate("SAM-CRZ-CARIB-27Q1", "user:mops.sam")
    assert again["marketo_list"]["added_to_list"] == 0 and again["marketo_list"]["removed_from_list"] == 0
    with pytest.raises(governance.AccessDenied):
        activate.activate("SAM-CRZ-CARIB-27Q1", "user:analyst.bhb")
    hist = activate.recent(governance.principal("agent:sam-marketing-copilot"))
    assert hist and all(h["partner_code"] == "SAM" for h in hist) and "@" not in json.dumps(hist)
