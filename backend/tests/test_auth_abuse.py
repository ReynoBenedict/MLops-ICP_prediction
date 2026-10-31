"""Sign-in abuse controls: per-IP auth rate limit, per-account failure throttle, email validation."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from icp_api.security import FailureThrottle, RateLimiter

API = "/api/v1"
PW = "Masuk-Aman-2026!"


@pytest.fixture
def tight(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Fresh limiters and production-like auth limits for one test."""
    c = client.app.state.container
    monkeypatch.setattr(
        c,
        "settings",
        replace(
            c.settings, rate_limit_auth_per_minute=4, login_failures_max=3, login_failure_window_seconds=900
        ),
    )
    monkeypatch.setattr(client.app.state, "rate_limiter", RateLimiter())
    monkeypatch.setattr(client.app.state, "login_failures", FailureThrottle())
    return client


def _register(client: Any, email: str) -> None:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Abuse Uji",
            "email": email,
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text


def test_auth_endpoints_are_limited_per_ip(tight):
    codes = [
        tight.post(f"{API}/auth/login", json={"email": "nobody@ip-limit.test", "password": "x"}).status_code
        for _ in range(6)
    ]
    # 4 per minute from one client: the remaining attempts are refused before any password check
    assert codes[:3] == [401, 401, 401] and codes[-1] == 429


def test_failed_logins_are_throttled_per_account_and_success_resets(client, monkeypatch):
    _register(client, "owner@throttle.akun.test")
    c = client.app.state.container
    monkeypatch.setattr(c, "settings", replace(c.settings, login_failures_max=3))
    monkeypatch.setattr(client.app.state, "login_failures", FailureThrottle())
    good = {"email": "owner@throttle.akun.test", "password": PW}
    bad = {"email": "OWNER@throttle.akun.test ", "password": "salah-salah-salah"}
    # one success clears earlier failures
    assert [client.post(f"{API}/auth/login", json=bad).status_code for _ in range(2)] == [401, 401]
    assert client.post(f"{API}/auth/login", json=good).status_code == 200
    assert [client.post(f"{API}/auth/login", json=bad).status_code for _ in range(3)] == [401, 401, 401]
    blocked = client.post(f"{API}/auth/login", json=good)  # even the right password waits
    assert blocked.status_code == 429 and int(blocked.headers["retry-after"]) > 0
    # unknown accounts behave the same (no enumeration through the throttle)
    unknown = {"email": "ghost@throttle.akun.test", "password": "x"}
    assert [client.post(f"{API}/auth/login", json=unknown).status_code for _ in range(4)] == [
        401,
        401,
        401,
        429,
    ]


@pytest.mark.parametrize("email", ["tanpa-at.test", "dua@@x.test", "spasi @x.test", "a@tanpatitik"])
def test_register_rejects_malformed_email(client, email):
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Email Uji",
            "email": email,
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 422 and r.json()["error"]["code"] == "VALIDATION_ERROR"


def test_invite_rejects_malformed_email(client):
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Undang Uji",
            "email": "owner@undang.akun.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    h = {"Authorization": f"Bearer {r.json()['token']}"}
    bad = client.post(
        f"{API}/org/members", json={"email": "bukan-email", "name": "X", "role": "viewer"}, headers=h
    )
    assert bad.status_code == 422
