"""v7 security & integrity regressions (Wave A). One section per audit finding; each test exercises
behaviour that the pre-v7 code did not have (see docs/audits and ux7 notes-A)."""

from __future__ import annotations

import io
import json
import threading
import zipfile
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from starlette.requests import Request

from icp_api.entitlements import PLANS
from icp_api.errors import ApiError
from icp_api.security import (
    AuthFailureAudit,
    FailureThrottle,
    Principal,
    RateLimiter,
    client_key,
    member_org_id,
    member_user_id,
)
from icp_api.services.reports import cell_safe

API = "/api/v1"
PW = "Keamanan-v7-Uji-2026!"
ROOT = Path(__file__).resolve().parents[2]


# -- helpers ---------------------------------------------------------------------------------------
def _register(client: Any, slug: str, email: str | None = None) -> dict[str, Any]:
    email = email or f"owner@{slug}.v7.test"
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": f"PT V7 {slug}",
            "email": email,
            "name": f"Owner {slug}",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return {
        "h": {"Authorization": f"Bearer {body['token']}"},
        "org": body["organization"]["id"],
        "user": body["user"]["id"],
        "email": email,
    }


def _admin(client: Any) -> dict[str, str]:
    return {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}


def _member(client: Any, owner_h: dict[str, str], email: str, role: str) -> dict[str, Any]:
    inv = client.post(
        f"{API}/org/members", json={"email": email, "name": role, "role": role}, headers=owner_h
    )
    assert inv.status_code == 201, inv.text
    temp = inv.json()["temporary_password"]
    tok = client.post(f"{API}/auth/login", json={"email": email, "password": temp}).json()["token"]
    client.post(
        f"{API}/auth/password",
        json={"current_password": temp, "new_password": PW},
        headers={"Authorization": f"Bearer {tok}"},
    )
    tok = client.post(f"{API}/auth/login", json={"email": email, "password": PW}).json()["token"]
    return {"h": {"Authorization": f"Bearer {tok}"}, "id": inv.json()["member"]["id"], "email": email}


def _set_plan(client: Any, org: str, code: str) -> None:
    r = client.post(
        f"{API}/admin/organizations/{org}/plan",
        json={"plan": code, "reason": "rahasia-internal-v7"},
        headers=_admin(client),
    )
    assert r.status_code == 200, r.text


def _gw(client: Any, ip: str, **extra: str) -> dict[str, str]:
    return {
        "x-gateway-secret": client.app.state.container.settings.gateway_shared_secret,
        **extra,
        "x-icp-client-ip": ip,
    }


@pytest.fixture
def limits(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Fresh in-memory limiters; returns a setter for production-like settings."""
    monkeypatch.setattr(client.app.state, "rate_limiter", RateLimiter())
    monkeypatch.setattr(client.app.state, "login_failures", FailureThrottle())
    monkeypatch.setattr(client.app.state, "password_failures", FailureThrottle())
    monkeypatch.setattr(client.app.state, "auth_failure_audit", AuthFailureAudit())
    c = client.app.state.container

    def set_(**changes: Any) -> None:
        monkeypatch.setattr(c, "settings", replace(c.settings, **changes))

    return set_


def _request(headers: dict[str, str], via_gateway: bool, host: str = "10.9.9.9") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
        "client": (host, 1234),
        "state": {"via_gateway": via_gateway},
    }
    return Request(scope)


# -- SEC-01 client IP ------------------------------------------------------------------------------
def test_sec01_client_key_prefers_dedicated_header_only_behind_the_gateway():
    worker_egress = "2a06:98c0:3600::103"
    both = {"x-icp-client-ip": "203.0.113.4", "cf-connecting-ip": worker_egress}
    assert client_key(_request(both, via_gateway=True)) == "203.0.113.4"
    assert client_key(_request(both, via_gateway=False)) == "10.9.9.9"  # forged without the secret
    # older gateways: CF-Connecting-IP / X-Forwarded-For remain a fallback
    assert client_key(_request({"cf-connecting-ip": "198.51.100.2"}, True)) == "198.51.100.2"
    assert client_key(_request({"x-forwarded-for": "198.51.100.3, 10.0.0.1"}, True)) == "198.51.100.3"
    # garbage is never used as a bucket key
    assert client_key(_request({"x-icp-client-ip": "not-an-ip", "cf-connecting-ip": "x"}, True)) == "10.9.9.9"


def test_sec01_sign_in_throttle_is_per_visitor_even_when_cf_connecting_ip_is_the_worker(client, limits):
    limits(login_failures_max=3, rate_limit_auth_per_minute=1000)
    _register(client, "sec01")
    bad = {"email": "owner@sec01.v7.test", "password": "salah-salah-salah"}
    egress = {"cf-connecting-ip": "2a06:98c0:3600::103"}
    attacker = [
        client.post(f"{API}/auth/login", json=bad, headers=_gw(client, "203.0.113.50", **egress)).status_code
        for _ in range(4)
    ]
    assert attacker == [401, 401, 401, 429]
    owner = client.post(
        f"{API}/auth/login", json={**bad, "password": PW}, headers=_gw(client, "198.51.100.77", **egress)
    )
    assert owner.status_code == 200  # the owner elsewhere is not locked out by the attacker


# -- SEC-02 junk tokens ----------------------------------------------------------------------------
def _audit_count(client: Any, action: str) -> int:
    rows = client.app.state.container.repo.db.query(
        "SELECT COUNT(*) AS n FROM audit_events WHERE action = ?", (action,)
    )
    return int(rows[0]["n"])


def test_sec02_junk_tokens_write_one_audit_row_per_client_minute_and_are_rate_limited(client, limits):
    limits(rate_limit_default_per_minute=6)
    before = _audit_count(client, "auth.failed")
    codes = [
        client.get(
            f"{API}/org", headers={**_gw(client, "203.0.113.90"), "Authorization": f"Bearer junk{i}"}
        ).status_code
        for i in range(8)
    ]
    assert codes[:6] == [401] * 6 and codes[6:] == [429, 429]
    assert _audit_count(client, "auth.failed") - before == 1
    # another client gets its own (single) row
    client.get(f"{API}/audit", headers={**_gw(client, "203.0.113.91"), "Authorization": "Bearer junk"})
    assert _audit_count(client, "auth.failed") - before == 2


def test_sec02_account_and_operator_routes_have_a_per_client_budget(client, limits):
    o = _register(client, "sec02")
    limits(rate_limit_default_per_minute=2)
    h = {**o["h"], **_gw(client, "203.0.113.92")}
    assert [client.get(f"{API}/org", headers=h).status_code for _ in range(3)] == [200, 200, 429]
    jobs = {**_admin(client), **_gw(client, "203.0.113.93")}
    assert [client.get(f"{API}/jobs", headers=jobs).status_code for _ in range(3)][-1] == 429


# -- SEC-03 admin vs admin -------------------------------------------------------------------------
def test_sec03_only_the_owner_manages_admins(client):
    o = _register(client, "sec03")
    _set_plan(client, o["org"], "professional")
    a1 = _member(client, o["h"], "a1@sec03.v7.test", "admin")
    a2 = _member(client, o["h"], "a2@sec03.v7.test", "admin")
    an = _member(client, o["h"], "an@sec03.v7.test", "analyst")
    for path, body in [
        (f"/org/members/{a2['id']}/reset-password", None),
        (f"/org/members/{a2['id']}", {"role": "viewer"}),
        (f"/org/members/{a2['id']}", {"status": "disabled"}),
    ]:
        r = client.post(f"{API}{path}", json=body, headers=a1["h"])
        assert r.status_code == 403 and r.json()["error"]["code"] == "FORBIDDEN", (path, r.text)
    # admins still manage analysts/viewers; the owner manages admins; self-reset stays allowed
    assert client.post(f"{API}/org/members/{an['id']}/reset-password", headers=a1["h"]).status_code == 200
    assert client.post(f"{API}/org/members/{a1['id']}/reset-password", headers=a1["h"]).status_code == 200
    assert (
        client.post(f"{API}/org/members/{a2['id']}", json={"role": "viewer"}, headers=o["h"]).status_code
        == 200
    )
    assert client.post(f"{API}/org/members/{a2['id']}/reset-password", headers=o["h"]).status_code == 200


# -- SEC-04 enumeration ----------------------------------------------------------------------------
def test_sec04_taken_email_costs_the_same_scrypt_work_as_a_registration(client, monkeypatch):
    import icp_api.services.accounts as acc

    calls = {"n": 0}
    real = acc.hashlib.scrypt

    def counting(*a: Any, **k: Any) -> bytes:
        calls["n"] += 1
        return real(*a, **k)

    monkeypatch.setattr(acc.hashlib, "scrypt", counting)
    _register(client, "sec04")
    fresh = calls["n"]
    calls["n"] = 0
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT X",
            "email": "owner@sec04.v7.test",
            "name": "X",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 409
    assert calls["n"] == fresh == 2


def test_sec04_invitation_answer_does_not_reveal_other_tenants(client):
    a, b = _register(client, "sec04a"), _register(client, "sec04b")
    _member(client, a["h"], "same@sec04a.v7.test", "viewer")
    other = client.post(
        f"{API}/org/members", json={"email": b["email"], "name": "x", "role": "viewer"}, headers=a["h"]
    )
    own = client.post(
        f"{API}/org/members",
        json={"email": "same@sec04a.v7.test", "name": "x", "role": "viewer"},
        headers=a["h"],
    )
    assert other.status_code == own.status_code == 409
    assert other.json()["error"]["code"] == own.json()["error"]["code"] == "EMAIL_UNAVAILABLE"
    assert other.json()["error"]["message"] == own.json()["error"]["message"]


# -- SEC-05 deletion withdraw ----------------------------------------------------------------------
def test_sec05_only_the_requester_withdraws_a_deletion_request(client):
    o = _register(client, "sec05")
    m = _member(client, o["h"], "m@sec05.v7.test", "viewer")
    rid = client.post(f"{API}/org/deletion-requests", json={"scope": "account"}, headers=m["h"]).json()["id"]
    assert client.post(f"{API}/org/deletion-requests/{rid}/withdraw", headers=o["h"]).status_code == 404
    seen = client.get(f"{API}/org/deletion-requests", headers=o["h"]).json()["items"]
    assert any(r["id"] == rid and r["status"] == "pending" for r in seen)  # the owner still sees it
    w = client.post(f"{API}/org/deletion-requests/{rid}/withdraw", headers=m["h"])
    assert w.status_code == 200 and w.json()["status"] == "withdrawn"


# -- SEC-07 quota ----------------------------------------------------------------------------------
def test_sec07_refused_api_key_requests_do_not_consume_quota(client):
    o = _register(client, "sec07")
    _set_plan(client, o["org"], "professional")
    key = client.post(f"{API}/org/api-keys", json={"name": "k"}, headers=o["h"]).json()["key"]
    kh = {"Authorization": f"Bearer {key}"}
    first = int(client.get(f"{API}/data/summary", headers=kh).headers["x-quota-remaining"])
    for _ in range(3):
        assert client.get(f"{API}/org", headers=kh).status_code == 403
    after = int(client.get(f"{API}/data/summary", headers=kh).headers["x-quota-remaining"])
    assert after == first - 1
    usage = client.get(f"{API}/org", headers=o["h"]).json()["usage_this_month"]
    assert usage["api.request"] == 2


def test_sec07_quota_reservation_is_atomic_under_concurrency(client):
    o = _register(client, "sec07b")
    repo = client.app.state.container.repo
    results: list[str | None] = []
    lock = threading.Lock()

    def worker() -> None:
        rid = repo.reserve_usage(o["org"], None, "v7.concurrency", "2000-01-01T00:00:00.000Z", 5)
        with lock:
            results.append(rid)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(1 for r in results if r) == 5
    assert repo.count_usage(o["org"], "v7.concurrency", "2000-01-01T00:00:00.000Z") == 5


# -- SEC-11 plan features on every path ------------------------------------------------------------
def test_sec11_scenario_and_alert_write_paths_recheck_the_plan(client, monkeypatch):
    from icp_api import entitlements

    o = _register(client, "sec11")
    sid = client.post(f"{API}/scenarios", json={"name": "S", "horizon": 1}, headers=o["h"]).json()["id"]
    rule = client.post(
        f"{API}/alerts/rules", json={"kind": "icp_published", "params": {}}, headers=o["h"]
    ).json()["id"]
    monkeypatch.setitem(entitlements.PLANS, "trial", replace(PLANS["trial"], scenarios=False, alerts=False))
    calls = [
        ("POST", f"/scenarios/{sid}", {"name": "T"}),
        ("POST", f"/scenarios/{sid}/archive", None),
        ("POST", f"/scenarios/{sid}/run", None),
        ("POST", "/scenarios/compare", {"scenario_ids": [sid]}),
        ("POST", f"/alerts/rules/{rule}", {"enabled": False}),
        ("POST", f"/alerts/rules/{rule}/delete", None),
        ("POST", "/alerts/evaluate", None),
        ("POST", "/alerts/events/read", None),
        (
            "POST",
            "/reports",
            {"kind": "scenario_comparison", "params": {"scenario_ids": [sid]}, "formats": ["csv"]},
        ),
    ]
    for method, path, body in calls:
        r = client.request(method, f"{API}{path}", json=body, headers=o["h"])
        assert r.status_code == 403 and r.json()["error"]["code"] == "FEATURE_NOT_IN_PLAN", (path, r.text)


# -- SEC-12 export contents ------------------------------------------------------------------------
def test_sec12_export_hides_operator_identity_notes_and_other_members_devices(client):
    o = _register(client, "sec12")
    _set_plan(client, o["org"], "professional")  # operator action with an internal reason
    m = _member(client, o["h"], "m@sec12.v7.test", "viewer")
    pr = client.post(
        f"{API}/org/plan-requests", json={"requested_plan": "enterprise"}, headers=o["h"]
    ).json()["id"]
    client.post(
        f"{API}/admin/plan-requests/{pr}/resolve",
        json={"decision": "decline", "note": "Hubungi kami"},
        headers=_admin(client),
    )
    r = client.get(f"{API}/org/export", headers={**o["h"], "User-Agent": "OwnerBrowser/1"})
    z = zipfile.ZipFile(io.BytesIO(r.content))
    blob = b"".join(z.read(n) for n in z.namelist()).decode("utf-8")
    assert "tester" not in blob  # operator name from ADMIN_API_TOKENS
    assert "rahasia-internal-v7" not in blob  # operator's internal reason
    sessions = json.loads(z.read("sessions.json"))
    assert sessions and {s["user_id"] for s in sessions} == {o["user"]}
    assert m["id"] not in {s["user_id"] for s in sessions}
    audit = json.loads(z.read("audit_events.json"))
    assert any(e["actor"] == "platform-operator" and e["action"] == "org.plan.set" for e in audit)
    plans = json.loads(z.read("plan_requests.json"))
    assert plans[0]["resolved_by"] == "platform-operator" and plans[0]["resolution_note"] == "Hubungi kami"
    # the customer API shows the same redaction; operators still see the name
    mine = client.get(f"{API}/org/plan-requests", headers=o["h"]).json()["items"][0]
    assert mine["resolved_by"] == "platform-operator"
    ops = client.get(f"{API}/admin/plan-requests", headers=_admin(client)).json()["items"]
    assert any(p["id"] == pr and p["resolved_by"] == "tester" for p in ops)


# -- SEC-13 report params --------------------------------------------------------------------------
@pytest.mark.parametrize("kind", ["market_monthly", "data_provenance"])
def test_sec13_reports_without_parameters_refuse_unknown_params(client, customer, kind):
    r = client.post(
        f"{API}/reports",
        json={"kind": kind, "formats": ["csv"], "params": {"junk": "A" * 5000}},
        headers=customer,
    )
    assert r.status_code == 422 and r.json()["error"]["code"] == "VALIDATION_ERROR"


# -- SEC-14 spreadsheet cells ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "value",
    ["\u200b=1+1", "\ufeff=1+1", "\x00=1+1", "\u2060@SUM(A1)", "\u200d\u200c+cmd", "\x0b=1", " \u200b-x"],
)
def test_sec14_invisible_prefixes_do_not_hide_formulas(value):
    assert cell_safe(value) == "'" + value


def test_sec14_plain_values_unchanged():
    for v in ["\u200b-1", "+1%", "-", "Brent", "2026-09"]:
        assert cell_safe(v) == v


# -- SEC-15 asserts / request ids ------------------------------------------------------------------
def test_sec15_member_helpers_raise_instead_of_asserting():
    op = Principal("ops", "operator", "operator")
    key = Principal("apikey:k", "analyst", "user", org_id="org_x", api_key_id="k")
    with pytest.raises(ApiError):
        member_org_id(op)
    with pytest.raises(ApiError):
        member_user_id(key)
    assert member_org_id(key) == "org_x"
    routers = ROOT / "backend" / "icp_api" / "routers"
    for name in ("accounts.py", "product.py"):
        assert "\n    assert " not in (routers / name).read_text(encoding="utf-8").replace("\r\n", "\n")


def test_sec15_unsafe_request_ids_are_replaced(client):
    for bad in ["x" * 300, "abc def ghi", "<script>alert(1)</script>", "short"]:
        rid = client.get(f"{API}/plans", headers={"x-request-id": bad}).headers["x-request-id"]
        assert rid != bad and rid.startswith("req_")


# -- AUTH-02 e-mail normalisation ------------------------------------------------------------------
def test_auth02_full_width_and_case_variants_are_the_same_account(client):
    _register(client, "auth02", email="owner@auth02.v7.test")
    dup = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Dup",
            "email": "OWNER@auth02.v7.\uff54est",
            "name": "D",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert dup.status_code == 409, dup.text
    ok = client.post(
        f"{API}/auth/login", json={"email": "Owner@AUTH02.v7.\uff54\uff45\uff53\uff54", "password": PW}
    )
    assert ok.status_code == 200 and ok.json()["user"]["email"] == "owner@auth02.v7.test"


# -- AUTH-03 registration race ---------------------------------------------------------------------
def test_auth03_lost_registration_race_answers_409_and_leaves_no_ownerless_org(client, monkeypatch):
    _register(client, "auth03", email="race@auth03.v7.test")
    repo = client.app.state.container.repo
    monkeypatch.setattr(
        repo, "get_user_by_email", lambda email: None
    )  # the check ran before the winner's insert
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Race Loser",
            "email": "race@auth03.v7.test",
            "name": "L",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 409 and r.json()["error"]["code"] == "EMAIL_TAKEN"
    orphans = repo.db.query(
        "SELECT COUNT(*) AS n FROM organizations o WHERE o.name = ? "
        "AND NOT EXISTS (SELECT 1 FROM users u WHERE u.org_id = o.id)",
        ("PT Race Loser",),
    )
    assert orphans[0]["n"] == 0


# -- AUTH-05 password change -----------------------------------------------------------------------
def test_auth05_wrong_current_passwords_are_throttled(client, limits):
    limits(login_failures_max=3)
    o = _register(client, "auth05")
    codes = [
        client.post(
            f"{API}/auth/password",
            json={"current_password": f"tebakan-{i}-xx", "new_password": "Baru-v7-2026!!"},
            headers=o["h"],
        ).status_code
        for i in range(4)
    ]
    assert codes == [401, 401, 401, 429]
    right = client.post(
        f"{API}/auth/password",
        json={"current_password": PW, "new_password": "Baru-v7-2026!!"},
        headers=o["h"],
    )
    assert right.status_code == 429


# -- D1 write amplification ------------------------------------------------------------------------
def test_session_and_api_key_touch_at_most_every_few_minutes(client, monkeypatch):
    o = _register(client, "touch")
    _set_plan(client, o["org"], "professional")
    key = client.post(f"{API}/org/api-keys", json={"name": "k"}, headers=o["h"]).json()["key"]
    repo = client.app.state.container.repo
    counts = {"s": 0, "k": 0}
    real_s, real_k = repo.touch_session, repo.touch_api_key
    monkeypatch.setattr(
        repo, "touch_session", lambda sid: (counts.__setitem__("s", counts["s"] + 1), real_s(sid))
    )
    monkeypatch.setattr(
        repo, "touch_api_key", lambda kid: (counts.__setitem__("k", counts["k"] + 1), real_k(kid))
    )
    for _ in range(5):
        client.get(f"{API}/auth/whoami", headers=o["h"])
        client.get(f"{API}/data/summary", headers={"Authorization": f"Bearer {key}"})
    assert counts["s"] == 0  # touched at sign-in moments ago
    assert counts["k"] == 1  # first use only


# -- SQL-01 ----------------------------------------------------------------------------------------
def test_sql01_no_nosec_and_constant_update_statements(client):
    for path in (ROOT / "backend" / "icp_api").rglob("*.py"):
        assert "nosec" not in path.read_text(encoding="utf-8"), path
    repo = client.app.state.container.repo
    with pytest.raises(ValueError):
        repo.update_user("usr_x", password_hash="x", is_superuser=1)
    o = _register(client, "sql01")
    sid = client.post(f"{API}/scenarios", json={"name": "S", "horizon": 1}, headers=o["h"]).json()["id"]
    repo.update_scenario(o["org"], sid, archived_at="2026-10-20T00:00:00.000Z")
    repo.update_scenario(o["org"], sid, archived_at=None, name="S2")  # explicit NULL is written
    row = repo.get_scenario(o["org"], sid)
    assert row["archived_at"] is None and row["name"] == "S2" and row["horizon"] == 1


# -- settings --------------------------------------------------------------------------------------
def test_settings_require_explicit_bind_and_strong_rotation_secrets(monkeypatch):
    from icp_api.settings import ConfigError, load_settings

    base = {
        "APP_ENV": "staging",
        "METADATA_BACKEND": "d1_gateway",
        "OBJECT_STORE_BACKEND": "gateway",
        "MODEL_SOURCE": "bundle",
        "GATEWAY_INTERNAL_URL": "https://gw.example.workers.dev",
        "GATEWAY_SHARED_SECRET": "z" * 40,
        "REQUIRE_GATEWAY_SECRET": "true",
        "ADMIN_API_TOKENS": "ops:" + "t" * 40,
        "CORS_ALLOWED_ORIGINS": "https://gw.example.workers.dev",
        "METRICS_TOKEN": "m" * 40,
        "RENDER": "true",
    }
    for k, v in base.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("API_HOST", raising=False)
    with pytest.raises(ConfigError, match="API_HOST"):
        load_settings()
    monkeypatch.setenv("API_HOST", "0.0.0.0")
    monkeypatch.setenv("GATEWAY_ACCEPTED_SECRETS", "short")
    with pytest.raises(ConfigError, match="GATEWAY_ACCEPTED_SECRETS"):
        load_settings()
    monkeypatch.setenv("GATEWAY_ACCEPTED_SECRETS", "p" * 40)
    s = load_settings()
    assert s.gateway_accepted_secrets == ("p" * 40,) and "p" * 40 not in repr(s)


def test_previous_gateway_secret_is_accepted_during_rotation(client, monkeypatch):
    c = client.app.state.container
    old = "previous-gateway-secret-0123456789abcdef"
    from icp_api.middleware import gateway_secret_matches

    assert not gateway_secret_matches(old, c.settings)
    assert gateway_secret_matches(old, replace(c.settings, gateway_accepted_secrets=(old,)))
    assert gateway_secret_matches(c.settings.gateway_shared_secret, c.settings)
    assert not gateway_secret_matches("", replace(c.settings, gateway_shared_secret=""))
