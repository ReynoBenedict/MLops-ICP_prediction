"""Security hardening v6 (Agent S): API-level adversarial tests.

Login throttling vs. account denial, public health minimalism, sessions, security headers, operator
read/write limits, authorization sweep over every route, IDOR on v5/v6 routes, error leakage.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from icp_api.security import FailureThrottle, RateLimiter

API = "/api/v1"
PW = "Keamanan-Uji-v6-2026!"


def _register(client: Any, slug: str) -> dict[str, Any]:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": f"PT Aman {slug}",
            "email": f"owner@{slug}.aman.test",
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
        "email": f"owner@{slug}.aman.test",
    }


def _ip(ip: str) -> dict[str, str]:
    """Client address as the verified gateway reports it (proxy headers are trusted only with the secret)."""
    import os

    return {"x-gateway-secret": os.environ["GATEWAY_SHARED_SECRET"], "cf-connecting-ip": ip}


@pytest.fixture
def throttled(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    c = client.app.state.container
    monkeypatch.setattr(
        c,
        "settings",
        replace(
            c.settings,
            rate_limit_auth_per_minute=1000,
            login_failures_max=3,
            login_account_failures_max=8,
            login_failure_window_seconds=900,
        ),
    )
    monkeypatch.setattr(client.app.state, "rate_limiter", RateLimiter())
    monkeypatch.setattr(client.app.state, "login_failures", FailureThrottle())
    return client


# -- login throttling ----------------------------------------------------------------------------
def test_attacker_ip_is_throttled_but_owner_from_another_ip_still_signs_in(throttled):
    owner = _register(throttled, "throttle-a")
    bad = {"email": owner["email"], "password": "tebakan-salah-123"}
    good = {"email": owner["email"], "password": PW}
    attacker = _ip("203.0.113.7")
    codes = [throttled.post(f"{API}/auth/login", json=bad, headers=attacker).status_code for _ in range(4)]
    assert codes == [401, 401, 401, 429]
    blocked = throttled.post(f"{API}/auth/login", json=good, headers=attacker)
    assert blocked.status_code == 429 and blocked.json()["error"]["details"]["scope"] == "client"
    assert int(blocked.headers["retry-after"]) > 0
    # the owner, from their own address, is not locked out by the attacker
    ok = throttled.post(f"{API}/auth/login", json=good, headers=_ip("198.51.100.20"))
    assert ok.status_code == 200, ok.text


def test_owner_sign_in_elsewhere_does_not_reset_the_attackers_counters(throttled):
    """A success clears only the (account, succeeding client) pair: the attacker's pair counter and
    the account-wide window keep counting, so frequent owner sign-ins cannot refill the attacker's
    guesses or defeat the account ceiling."""
    owner = _register(throttled, "throttle-reset")
    bad = {"email": owner["email"], "password": "tebakan-salah-123"}
    good = {"email": owner["email"], "password": PW}
    attacker = _ip("203.0.113.40")
    assert [
        throttled.post(f"{API}/auth/login", json=bad, headers=attacker).status_code for _ in range(4)
    ] == [
        401,
        401,
        401,
        429,
    ]
    assert throttled.post(f"{API}/auth/login", json=good, headers=_ip("198.51.100.23")).status_code == 200
    again = throttled.post(f"{API}/auth/login", json=bad, headers=attacker)
    assert again.status_code == 429 and again.json()["error"]["details"]["scope"] == "client"
    # the account window keeps the attacker's failures too: 3 so far + 5 from fresh addresses hits 8
    codes = [
        throttled.post(f"{API}/auth/login", json=bad, headers=_ip(f"192.0.2.{50 + i}")).status_code
        for i in range(6)
    ]
    assert codes[:5] == [401] * 5 and codes[5] == 429


def test_distributed_guessing_hits_the_account_ceiling(throttled):
    owner = _register(throttled, "throttle-b")
    bad = {"email": owner["email"], "password": "tebakan-salah-123"}
    codes = []
    for i in range(12):  # 2 guesses from each of 6 addresses: no single IP reaches its limit (3)
        codes.append(
            throttled.post(f"{API}/auth/login", json=bad, headers=_ip(f"192.0.2.{i // 2}")).status_code
        )
    assert codes[:8] == [401] * 8 and set(codes[8:]) == {429}
    r = throttled.post(f"{API}/auth/login", json=bad, headers=_ip("192.0.2.200"))
    assert r.status_code == 429 and r.json()["error"]["details"]["scope"] == "account"


def test_unknown_email_throttles_like_a_known_one(throttled):
    unknown = {"email": "ghost@throttle-c.aman.test", "password": "x"}
    codes = [
        throttled.post(f"{API}/auth/login", json=unknown, headers=_ip("203.0.113.9")).status_code
        for _ in range(4)
    ]
    assert codes == [401, 401, 401, 429]


def test_success_clears_the_pair_counter(throttled):
    owner = _register(throttled, "throttle-d")
    bad = {"email": owner["email"], "password": "tebakan-salah-123"}
    good = {"email": owner["email"], "password": PW}
    h = _ip("203.0.113.30")
    for _ in range(2):
        assert throttled.post(f"{API}/auth/login", json=bad, headers=h).status_code == 401
    assert throttled.post(f"{API}/auth/login", json=good, headers=h).status_code == 200
    for _ in range(2):
        assert throttled.post(f"{API}/auth/login", json=bad, headers=h).status_code == 401


# -- public health -------------------------------------------------------------------------------
def test_public_health_reveals_only_status(client, admin):
    root = client.get("/")
    assert root.status_code == 200 and root.json() == {"status": "ok"}
    public = client.get("/health/ready")
    body = public.json()
    assert body["status"] in ("ready", "degraded", "not_ready")
    assert body.get("environment") is None and body.get("version") is None and body.get("checks") is None
    for leak in ("sqlite", "metadata_store", "schema", "test", "mlflow"):
        assert leak not in public.text.lower().replace('"status"', ""), leak
    # a customer session is not an operator either
    operator = client.get("/health/ready", headers=admin).json()
    assert operator["environment"] and operator["version"] and operator["checks"]
    assert any(c["detail"] for c in operator["checks"])


# -- security headers ----------------------------------------------------------------------------
def test_api_responses_carry_security_headers(client, customer):
    for r in (
        client.get(f"{API}/plans"),
        client.get(f"{API}/data/summary", headers=customer),
        client.get(f"{API}/reports/rpt_missing", headers=customer),  # error responses too
        client.get("/health/ready"),
    ):
        h = r.headers
        assert h["x-content-type-options"] == "nosniff"
        assert h["x-frame-options"] == "DENY"
        assert h["referrer-policy"] == "no-referrer"
        assert h["cross-origin-resource-policy"] == "same-origin"
        assert h["content-security-policy"] == "default-src 'none'; frame-ancestors 'none'"
        assert "strict-transport-security" not in h  # only when deployed (HTTPS terminates upstream)
    docs = client.get("/docs")
    if docs.status_code == 200:  # Swagger UI needs its own scripts: no API CSP there
        assert "default-src 'none'" not in docs.headers.get("content-security-policy", "")


# -- operator rate limits: reads vs writes -------------------------------------------------------
def test_operator_console_reads_do_not_exhaust_the_write_budget(client, admin, monkeypatch):
    c = client.app.state.container
    monkeypatch.setattr(
        c,
        "settings",
        replace(c.settings, rate_limit_admin_per_minute=5, rate_limit_admin_read_per_minute=40),
    )
    monkeypatch.setattr(client.app.state, "rate_limiter", RateLimiter())
    # ~8 console reloads x 4 operator reads each (status + page calls) stay under the read budget
    pages = (f"{API}/system/status", f"{API}/jobs", f"{API}/audit", f"{API}/admin/organizations")
    codes = [client.get(p, headers=admin).status_code for _ in range(8) for p in pages]
    assert 429 not in codes, codes
    # writes keep their own, stricter budget
    writes = [
        client.post(
            f"{API}/admin/organizations/org_missing/status",
            json={"status": "active", "reason": "uji status"},
            headers=admin,
        ).status_code
        for _ in range(7)
    ]
    assert writes[-1] == 429 and 429 not in writes[:5], writes
    # and reads beyond their budget are still limited
    more = [client.get(f"{API}/jobs", headers=admin).status_code for _ in range(12)]
    assert more[-1] == 429


def test_operator_status_change_on_unknown_org_is_404_and_not_audited_as_success(client, admin):
    r = client.post(
        f"{API}/admin/organizations/org_does_not_exist/status",
        json={"status": "suspended", "reason": "uji status"},
        headers=admin,
    )
    assert r.status_code == 404
    audit = client.get(f"{API}/audit", params={"limit": 100}, headers=admin).json()["items"]
    assert not [a for a in audit if a.get("target") == "org_does_not_exist" and a["outcome"] == "success"]


# -- sessions & API keys lifecycle ---------------------------------------------------------------
def _login(client: Any, email: str, password: str = PW) -> dict[str, str]:
    r = client.post(f"{API}/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def test_session_lifecycle(client, admin):
    """Documented lifecycle: 30-day expiry, logout, password change, member disable, org suspension."""
    from datetime import datetime, timedelta, timezone

    c = client.app.state.container
    org = _register(client, "sesi-a")
    r = client.post(f"{API}/auth/login", json={"email": org["email"], "password": PW})
    body = r.json()
    expires = datetime.strptime(body["expires_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    assert timedelta(days=29, hours=23) < expires - datetime.now(timezone.utc) <= timedelta(days=30)
    # an expired session is refused
    second = {"Authorization": f"Bearer {body['token']}"}
    sid = c.repo.get_session_by_hash(__import__("hashlib").sha256(body["token"].encode()).hexdigest())["id"]
    c.repo.db.execute("UPDATE sessions SET expires_at = ? WHERE id = ?", ("2001-01-01T00:00:00Z", sid))
    assert client.get(f"{API}/auth/whoami", headers=second).status_code == 401
    # password change revokes every session of that user (the UI signs out afterwards)
    other_device = _login(client, org["email"])
    new_pw = PW + "x"
    assert (
        client.post(
            f"{API}/auth/password", json={"current_password": PW, "new_password": new_pw}, headers=org["h"]
        ).status_code
        == 204
    )
    for h in (org["h"], other_device):
        assert client.get(f"{API}/auth/whoami", headers=h).status_code == 401
    owner = _login(client, org["email"], new_pw)
    # a disabled member loses every session immediately
    inv = client.post(
        f"{API}/org/members",
        json={"email": "anggota@sesi-a.aman.test", "name": "A", "role": "viewer"},
        headers=owner,
    ).json()
    member = _login(client, "anggota@sesi-a.aman.test", inv["temporary_password"])
    assert client.get(f"{API}/auth/whoami", headers=member).status_code == 200
    client.post(f"{API}/org/members/{inv['member']['id']}", json={"status": "disabled"}, headers=owner)
    assert client.get(f"{API}/auth/whoami", headers=member).status_code == 401
    # a suspended organisation is locked out (sessions and API keys)
    client.post(
        f"{API}/admin/organizations/{org['org']}/plan",
        json={"plan": "professional", "reason": "uji sesi"},
        headers=admin,
    )
    key = client.post(f"{API}/org/api-keys", json={"name": "uji"}, headers=owner).json()
    kh = {"Authorization": f"Bearer {key['key']}"}
    assert client.get(f"{API}/data/summary", headers=kh).status_code == 200
    client.post(
        f"{API}/admin/organizations/{org['org']}/status",
        json={"status": "suspended", "reason": "uji sesi"},
        headers=admin,
    )
    assert client.get(f"{API}/auth/whoami", headers=owner).status_code == 401
    assert client.get(f"{API}/data/summary", headers=kh).status_code == 401
    client.post(
        f"{API}/admin/organizations/{org['org']}/status",
        json={"status": "active", "reason": "uji sesi"},
        headers=admin,
    )
    # API-key revocation is immediate; logout revokes only that session
    assert client.get(f"{API}/data/summary", headers=kh).status_code == 200
    assert client.post(f"{API}/org/api-keys/{key['id']}/revoke", headers=owner).status_code == 204
    assert client.get(f"{API}/data/summary", headers=kh).status_code == 401
    spare = _login(client, org["email"], new_pw)
    assert client.post(f"{API}/auth/logout", headers=owner).status_code == 204
    assert client.get(f"{API}/auth/whoami", headers=owner).status_code == 401
    assert client.get(f"{API}/auth/whoami", headers=spare).status_code == 200
    # the session listing never exposes token hashes
    me = client.get(f"{API}/auth/me", headers=spare).json()
    assert me["sessions"] and all("token_hash" not in s for s in me["sessions"])


# -- authorization sweep over every route ----------------------------------------------------------
PUBLIC_ROUTES = {
    ("GET", "/"),
    ("GET", "/health/live"),
    ("GET", "/health/ready"),
    ("GET", "/metrics"),  # protected by METRICS_TOKEN, which deployed settings require
    ("GET", "/api/v1/plans"),
    ("GET", "/api/v1/public/market-snapshot"),
    ("GET", "/api/v1/auth/whoami"),
    ("POST", "/api/v1/auth/register"),
    ("POST", "/api/v1/auth/login"),
    # e-mail token flows (v7 Wave B): authenticated by the single-use token in the body; 503 while
    # EMAIL_ENABLED=false (test_v7_identity.py)
    ("POST", "/api/v1/auth/password-reset/request"),
    ("POST", "/api/v1/auth/password-reset/confirm"),
    ("POST", "/api/v1/auth/email/verify/confirm"),
    ("POST", "/api/v1/auth/invite/accept"),
    (
        "POST",
        "/api/v1/billing/webhooks/midtrans",
    ),  # Wave E: signature-verified; 503 while BILLING_ENABLED=false
}


def _api_routes(app: Any) -> list[Any]:
    """Every APIRoute, also inside included routers (FastAPI >= 0.140 wraps them)."""
    from fastapi.routing import APIRoute

    found: list[Any] = []

    def walk(routes: Any) -> None:
        for r in routes:
            if isinstance(r, APIRoute):
                found.append(r)
            elif hasattr(r, "original_router"):
                walk(r.original_router.routes)
            elif hasattr(r, "routes"):
                walk(r.routes)

    walk(app.routes)
    return found


def _routes(app: Any) -> list[tuple[str, str]]:
    out = [(m, r.path) for r in _api_routes(app) for m in sorted(r.methods - {"HEAD", "OPTIONS"})]
    assert len(out) > 60, "route discovery broke"
    return out


def test_every_route_requires_authentication_unless_public(client):
    """New routes are protected by default: a guest gets 401 everywhere except the public list."""
    import re

    unexpected = []
    for method, path in _routes(client.app):
        if (method, path) in PUBLIC_ROUTES:
            continue
        url = re.sub(r"\{[^}]+\}", "x_probe", path)
        r = client.request(method, url, json={})
        if r.status_code != 401:
            unexpected.append((method, path, r.status_code))
    assert unexpected == []


def test_customers_get_403_on_every_operator_route(client, customer):
    """Every route guarded by require_admin refuses a signed-in customer owner."""
    import re

    from icp_api.security import require_admin

    def guarded(route: Any) -> bool:
        return any(d.call is require_admin for d in route.dependant.dependencies) or any(
            getattr(d, "call", None) is require_admin for d in getattr(route, "dependencies", [])
        )

    checked, wrong = 0, []
    for route in _api_routes(client.app):
        if not guarded(route):
            continue
        for m in route.methods - {"HEAD", "OPTIONS"}:
            r = client.request(m, re.sub(r"\{[^}]+\}", "x_probe", route.path), json={}, headers=customer)
            checked += 1
            if r.status_code != 403:
                wrong.append((m, route.path, r.status_code))
    assert checked >= 20 and wrong == []


@pytest.fixture(scope="module")
def tenants(client: Any) -> dict[str, Any]:
    admin = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    a, b = _register(client, "idor-a"), _register(client, "idor-b")
    client.post(
        f"{API}/admin/organizations/{a['org']}/plan",
        json={"plan": "professional", "reason": "uji idor"},
        headers=admin,
    )
    inv = client.post(
        f"{API}/org/members",
        json={"email": "analis@idor-a.aman.test", "name": "A", "role": "analyst"},
        headers=a["h"],
    )
    a["member"] = inv.json()["member"]["id"]
    a["key"] = client.post(f"{API}/org/api-keys", json={"name": "k"}, headers=a["h"]).json()["id"]
    a["plan_request"] = client.post(
        f"{API}/org/plan-requests", json={"requested_plan": "enterprise"}, headers=a["h"]
    ).json()["id"]
    rule = client.post(f"{API}/alerts/rules", json={"kind": "icp_published"}, headers=a["h"]).json()["id"]
    from icp_api.storage.repository import new_id

    a["event"] = new_id("ale")
    client.app.state.container.repo.insert_alert_event(
        {
            "id": a["event"],
            "rule_id": rule,
            "org_id": a["org"],
            "dedupe_key": "idor",
            "severity": "info",
            "title": "t",
            "body": "b",
        }
    )
    return {"a": a, "b": b, "admin": admin}


def test_cross_tenant_ids_on_member_key_plan_and_alert_routes(client, tenants):
    a, bh = tenants["a"], tenants["b"]["h"]
    attempts = [
        ("POST", f"/org/members/{a['member']}", {"role": "viewer"}),
        ("POST", f"/org/members/{a['member']}/reset-password", {}),
        ("POST", f"/org/api-keys/{a['key']}/revoke", {}),
        ("POST", f"/org/plan-requests/{a['plan_request']}/withdraw", {}),
    ]
    for method, path, body in attempts:
        r = client.request(method, f"{API}{path}", json=body, headers=bh)
        assert r.status_code == 404, (path, r.status_code, r.text)
    # marking another tenant's alert as read changes nothing
    r = client.post(f"{API}/alerts/events/read", params={"event_id": a["event"]}, headers=bh)
    assert r.status_code in (403, 404) or r.json()["marked"] == 0
    events = client.get(f"{API}/alerts/events", params={"unread_only": True}, headers=a["h"]).json()["items"]
    assert a["event"] in {e["id"] for e in events}
    # A's records are intact
    keys = client.get(f"{API}/org/api-keys", headers=a["h"]).json()
    assert next(k for k in keys if k["id"] == a["key"])["revoked_at"] is None
    pr = client.get(f"{API}/org/plan-requests", headers=a["h"]).json()["items"]
    assert next(p for p in pr if p["id"] == a["plan_request"])["status"] == "pending"


def test_org_id_parameters_are_ignored(client, tenants):
    """Tenant scope always comes from the credential, never from a query/body org_id."""
    a, b = tenants["a"], tenants["b"]
    for path in ("/org", "/org/members", "/reports", "/scenarios", "/alerts/rules", "/org/plan-requests"):
        r = client.get(f"{API}{path}", params={"org_id": a["org"]}, headers=b["h"])
        assert a["org"] not in r.text, path
    r = client.post(f"{API}/org", json={"name": "Dibajak", "org_id": a["org"]}, headers=b["h"])
    assert r.status_code in (200, 422)
    assert client.get(f"{API}/org", headers=a["h"]).json()["name"] != "Dibajak"


# -- no exception text, credentials or full e-mails leak -------------------------------------------
def test_unhandled_errors_and_validation_errors_do_not_leak(client, customer, monkeypatch):
    from fastapi.testclient import TestClient

    secret = "C:/srv/secret/path at 0xdeadbeef password=hunter2"

    def boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError(secret)

    monkeypatch.setattr(client.app.state.container.accounts, "organization", boom)
    raw = TestClient(client.app, raise_server_exceptions=False)
    r = raw.get(f"{API}/org", headers=customer)
    assert r.status_code == 500 and r.json()["error"]["code"] == "INTERNAL_ERROR"
    assert "secret" not in r.text and "RuntimeError" not in r.text and "0xdead" not in r.text
    assert r.json()["error"]["request_id"] == r.headers["x-request-id"]  # quotable id (was null)
    assert r.headers["x-frame-options"] == "DENY"
    v = client.post(f"{API}/auth/login", json={"email": "x@y.test", "password": 12345, "extra": secret})
    assert v.status_code == 422 and "hunter2" not in v.text


def test_logs_and_audit_hold_no_credentials_or_full_emails(client, caplog):
    import json
    import logging

    caplog.set_level(logging.DEBUG)
    org = _register(client, "log-a")
    h = org["h"]
    token = h["Authorization"].split()[1]
    client.post(f"{API}/auth/login", json={"email": org["email"], "password": "salah-salah-salah"})
    inv = client.post(
        f"{API}/org/members",
        json={"email": "rahasia.anggota@log-a.aman.test", "name": "R", "role": "viewer"},
        headers=h,
    ).json()
    client.post(
        f"{API}/auth/password", json={"current_password": PW, "new_password": PW + "-baru"}, headers=h
    )
    secrets_ = [PW, PW + "-baru", token, inv["temporary_password"], org["email"], "rahasia.anggota@log-a"]
    logged = "\n".join(
        f"{rec.getMessage()} {json.dumps({k: str(v) for k, v in rec.__dict__.items()})}"
        for rec in caplog.records
    )
    assert any(rec.name == "icp_api.access" for rec in caplog.records)  # the access log was captured
    rows = client.app.state.container.repo.db.query("SELECT * FROM audit_events")
    audit = json.dumps([dict(r) for r in rows])
    for s in secrets_:
        assert s not in logged, s[:6]
        assert s not in audit, s[:6]
