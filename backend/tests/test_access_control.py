"""Access policy v4 regression tests (guest / customer / operator, tenant isolation, plan limits).

Runs before the model lifecycle tests in test_api.py (alphabetical order), so it never promotes a
model: forecast records needed for the isolation checks are inserted directly through the
repository, and forecast-producing calls that pass every access check end in 503
MODEL_UNAVAILABLE (proving the request got past authorization) rather than a forecast.
"""

from __future__ import annotations

from typing import Any

import pytest

from icp_api.storage.repository import new_id

PW = "Akses-Kebijakan-2026!"
API = "/api/v1"
SENTINEL_PREDICTION = 987.654321  # never appears in any guest response


def _register(client: Any, slug: str) -> dict[str, Any]:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": f"PT Akses {slug}",
            "email": f"owner@{slug}.akses.test",
            "name": f"Owner {slug}",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return {"h": {"Authorization": f"Bearer {body['token']}"}, "org": body["organization"]["id"]}


def _member(client: Any, owner_h: dict[str, str], email: str, role: str) -> dict[str, str]:
    """Invite, change the temporary password and sign in again (a fully activated member)."""
    inv = client.post(
        f"{API}/org/members", json={"email": email, "name": role, "role": role}, headers=owner_h
    )
    assert inv.status_code == 201, inv.text
    temp = inv.json()["temporary_password"]
    tok = client.post(f"{API}/auth/login", json={"email": email, "password": temp}).json()["token"]
    h = {"Authorization": f"Bearer {tok}"}
    r = client.post(f"{API}/auth/password", json={"current_password": temp, "new_password": PW}, headers=h)
    assert r.status_code == 204, r.text
    tok = client.post(f"{API}/auth/login", json={"email": email, "password": PW}).json()["token"]
    return {"Authorization": f"Bearer {tok}"}


def _forecast_row(client: Any, org_id: str | None, workflow: str) -> str:
    """Insert a succeeded forecast record with a CSV export (no model needed)."""
    c = client.app.state.container
    fid = new_id("fc")
    key = f"exports/forecasts/test/{fid}.csv"
    c.objects.put(key, f"forecast_id,prediction_usd\n{fid},{SENTINEL_PREDICTION}\n".encode(), "text/csv")
    c.repo.insert_forecast(
        {
            "id": fid,
            # far in the past so these rows never become "the latest" system forecast elsewhere
            "created_at": "2001-01-01T00:00:00.000000Z",
            "requested_by": "test",
            "status": "succeeded",
            "horizon": 1,
            "params": {"horizon": 1, "scenario": {}},
            "result": {"points": []},
            "export_object_key": key,
            "issue_date": "2001-01-01",
            "forecast_origin": "2000-12",
            "last_observed_period": "2000-12",
            "workflow": workflow,
            "org_id": org_id,
        }
    )
    return fid


@pytest.fixture(scope="module")
def admin(client: Any) -> dict[str, str]:
    """Module-scoped operator header (the conftest fixture is function-scoped)."""
    return {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}


@pytest.fixture(scope="module")
def world(client: Any, admin: dict[str, str]) -> dict[str, Any]:
    a = _register(client, "alfa")
    b = _register(client, "bravo")
    # org A on Professional: room for a viewer and an analyst, API keys, unlimited history
    up = client.post(
        f"{API}/admin/organizations/{a['org']}/plan",
        json={"plan": "professional", "reason": "access policy tests"},
        headers=admin,
    )
    assert up.status_code == 200, up.text
    a["viewer"] = _member(client, a["h"], "viewer@alfa.akses.test", "viewer")
    a["analyst"] = _member(client, a["h"], "analyst@alfa.akses.test", "analyst")
    a["forecast"] = _forecast_row(client, a["org"], "api")
    a["scenario"] = client.post(
        f"{API}/scenarios", json={"name": "Skenario A", "horizon": 2}, headers=a["h"]
    ).json()["id"]
    rep = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["csv"]}, headers=a["h"])
    assert rep.status_code == 201, rep.text
    a["report"] = rep.json()["id"]
    rule = client.post(f"{API}/alerts/rules", json={"kind": "icp_published"}, headers=a["h"])
    assert rule.status_code == 201, rule.text
    a["rule"] = rule.json()["id"]
    return {
        "a": a,
        "b": b,
        "system_forecast": _forecast_row(client, None, "system"),
        "legacy_forecast": _forecast_row(client, None, "api"),  # pre-v4 anonymous run
    }


GUEST_DENIED = [
    ("GET", "/system/status"),
    ("GET", "/data/summary"),
    ("GET", "/data/history"),
    ("GET", "/data/quality"),
    ("GET", "/data/evidence"),
    ("GET", "/datasets/active"),
    ("GET", "/datasets/versions"),
    ("GET", "/datasets/ingestions"),
    ("GET", "/datasets/versions/dsv_x"),
    ("GET", "/datasets/versions/dsv_x/download"),
    ("GET", "/models"),
    ("GET", "/models/versions/1"),
    ("GET", "/models/compare?versions=1,2"),
    ("GET", "/models/experiments/runs"),
    ("GET", "/models/promotions"),
    ("GET", "/models/drift"),
    ("GET", "/forecasts"),
    ("GET", "/forecasts/latest"),
    ("GET", "/forecasts/fc_x"),
    ("GET", "/forecasts/fc_x/export.csv"),
    ("POST", "/forecasts"),
    ("GET", "/insights"),
    ("GET", "/scenarios"),
    ("POST", "/scenarios/compare"),
    ("GET", "/alerts/kinds"),
    ("GET", "/alerts/rules"),
    ("GET", "/alerts/events"),
    ("GET", "/reports"),
    ("POST", "/reports"),
    ("GET", "/reports/rpt_x/download"),
    ("GET", "/org"),
    ("GET", "/org/members"),
    ("GET", "/org/api-keys"),
    ("GET", "/auth/me"),
    ("POST", "/auth/logout"),
    ("GET", "/jobs"),
    ("GET", "/audit"),
    ("GET", "/admin/organizations"),
]

OPERATOR_ONLY = [
    "/system/status",
    "/data/quality",
    "/datasets/versions",
    "/datasets/versions/dsv_x",
    "/datasets/ingestions",
    "/models/compare?versions=1,2",
    "/models/experiments/runs",
    "/models/promotions",
    "/models/drift",
    "/jobs",
    "/audit",
    "/admin/organizations",
]


def _call(client: Any, method: str, path: str, headers: dict[str, str] | None = None) -> Any:
    if method == "POST":
        return client.post(f"{API}{path}", json={}, headers=headers or {})
    return client.get(f"{API}{path}", headers=headers or {})


@pytest.mark.parametrize(("method", "path"), GUEST_DENIED)
def test_guest_is_refused_with_401_and_challenge(client, method, path):
    r = _call(client, method, path)
    assert r.status_code == 401, (path, r.status_code, r.text)
    assert r.json()["error"]["code"] == "AUTH_REQUIRED"
    assert r.headers.get("www-authenticate") == "Bearer"


def test_guest_allowed_endpoints_and_snapshot_has_no_forecast_values(client, world):
    for path in ("/plans", "/public/market-snapshot", "/auth/whoami"):
        assert client.get(f"{API}{path}").status_code == 200, path
    assert client.get("/health/live").status_code == 200
    snap = client.get(f"{API}/public/market-snapshot")
    teaser = snap.json()["latest_forecast"]
    assert teaser is not None and set(teaser) == {"available", "issue_date", "forecast_origin", "horizon"}
    assert teaser["available"] is True
    for word in ("prediction", "interval", "points", "method"):
        assert word not in str(teaser), word
    assert str(SENTINEL_PREDICTION) not in snap.text
    assert snap.json()["icp_usd"] and snap.json()["source"] and snap.json()["freshness"]


def test_customer_workspace_is_open_to_members(client, world):
    a = world["a"]
    for who in ("viewer", "analyst"):
        h = a[who]
        for path in (
            "/data/summary",
            "/data/history",
            "/data/evidence",
            "/datasets/active",
            "/forecasts",
            "/insights",
            "/alerts/kinds",
            "/scenarios",
            "/alerts/rules",
            "/alerts/events",
            "/reports",
            "/org",
            "/org/members",
            "/auth/me",
        ):
            r = client.get(f"{API}{path}", headers=h)
            assert r.status_code == 200, (who, path, r.status_code, r.text)
        # no model is promoted yet in this part of the session: past authorization means 503 here
        assert client.get(f"{API}/models", headers=h).status_code not in (401, 403)
        assert client.get(f"{API}/forecasts/latest", headers=h).status_code in (200, 404)
    created = client.post(f"{API}/forecasts", json={"horizon": 1}, headers=a["viewer"])
    assert created.status_code == 503 and created.json()["error"]["code"] == "MODEL_UNAVAILABLE"


@pytest.mark.parametrize("path", OPERATOR_ONLY)
def test_members_get_403_forbidden_on_operator_endpoints(client, world, path):
    for who in ("viewer", "analyst"):
        r = client.get(f"{API}{path}", headers=world["a"][who])
        assert r.status_code == 403, (who, path, r.text)
        assert r.json()["error"]["code"] == "FORBIDDEN"
        assert "www-authenticate" not in r.headers


def test_operator_reads_internal_and_tenant_data(client, world, admin):
    for path in ("/system/status", "/data/quality", "/datasets/versions", "/datasets/ingestions"):
        assert client.get(f"{API}{path}", headers=admin).status_code == 200, path
    assert client.get(f"{API}/forecasts", headers=admin).status_code == 200
    assert client.get(f"{API}/forecasts/{world['legacy_forecast']}", headers=admin).status_code == 200
    assert client.get(f"{API}/forecasts/{world['a']['forecast']}", headers=admin).status_code == 200
    assert client.get(f"{API}/reports/{world['a']['report']}", headers=admin).status_code == 200


def test_operator_is_blocked_from_customer_org_endpoints(client, world, admin):
    org = {"org_id": world["a"]["org"]}
    for method, path in (
        ("GET", "/scenarios"),
        ("POST", "/scenarios/compare"),
        ("GET", f"/scenarios/{world['a']['scenario']}"),
        ("GET", "/alerts/rules"),
        ("GET", "/alerts/events"),
        ("POST", "/alerts/evaluate"),
        ("POST", "/alerts/events/read"),
        ("GET", "/org"),
        ("GET", "/org/members"),
        ("POST", "/org/members"),
        ("GET", "/org/api-keys"),
        ("POST", "/org/api-keys"),
        ("GET", "/auth/me"),
    ):
        if method == "POST":
            r = client.post(f"{API}{path}", params=org, json={}, headers=admin)
        else:
            r = client.get(f"{API}{path}", params=org, headers=admin)
        assert r.status_code == 403, (path, r.status_code, r.text)
        assert r.json()["error"]["code"] == "FORBIDDEN"


def test_cross_tenant_ids_are_404(client, world):
    a, bh = world["a"], world["b"]["h"]
    for path in (
        f"/forecasts/{a['forecast']}",
        f"/forecasts/{a['forecast']}/export.csv",
        f"/scenarios/{a['scenario']}",
        f"/reports/{a['report']}",
        f"/reports/{a['report']}/download?format=csv",
    ):
        r = client.get(f"{API}{path}", headers=bh)
        assert r.status_code == 404 and r.json()["error"]["code"] == "NOT_FOUND", (path, r.text)
    for r in (
        client.post(f"{API}/alerts/rules/{a['rule']}", json={"enabled": False}, headers=bh),
        client.post(f"{API}/alerts/rules/{a['rule']}/delete", headers=bh),
        client.post(f"{API}/scenarios/{a['scenario']}/run", headers=bh),
        client.post(
            f"{API}/reports",
            json={"kind": "forecast", "params": {"forecast_id": a["forecast"]}, "formats": ["csv"]},
            headers=bh,
        ),
    ):
        assert r.status_code == 404, r.text
    assert a["forecast"] not in {f["id"] for f in client.get(f"{API}/forecasts", headers=bh).json()["items"]}
    # the owning organisation still reads its own records, export marked private
    own = client.get(f"{API}/forecasts/{a['forecast']}/export.csv", headers=a["viewer"])
    assert own.status_code == 200 and own.headers["cache-control"] == "private, no-store"
    assert (
        client.get(f"{API}/reports/{a['report']}/download?format=csv", headers=a["viewer"]).status_code == 200
    )


def test_system_forecasts_are_shared_and_legacy_anonymous_rows_are_not(client, world):
    for h in (world["a"]["viewer"], world["b"]["h"]):
        assert client.get(f"{API}/forecasts/{world['system_forecast']}", headers=h).status_code == 200
        assert (
            client.get(f"{API}/forecasts/{world['system_forecast']}/export.csv", headers=h).status_code == 200
        )
        assert client.get(f"{API}/forecasts/{world['legacy_forecast']}", headers=h).status_code == 404
        assert (
            client.get(f"{API}/forecasts/{world['legacy_forecast']}/export.csv", headers=h).status_code == 404
        )
    listed = client.get(f"{API}/forecasts", params={"limit": 100}, headers=world["b"]["h"]).json()["items"]
    assert world["system_forecast"] in {f["id"] for f in listed}
    assert world["legacy_forecast"] not in {f["id"] for f in listed}


def test_platform_reports_are_not_visible_to_customers(client, world, admin):
    op = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["csv"]}, headers=admin)
    assert op.status_code == 201 and op.json()["org_id"] is None
    rid = op.json()["id"]
    h = world["a"]["h"]
    assert rid not in {r["id"] for r in client.get(f"{API}/reports", headers=h).json()["items"]}
    assert client.get(f"{API}/reports/{rid}", headers=h).status_code == 404
    assert client.get(f"{API}/reports/{rid}/download?format=csv", headers=h).status_code == 404
    assert client.get(f"{API}/reports/{rid}", headers=admin).status_code == 200


def test_logout_revokes_the_token(client):
    org = _register(client, "charlie")
    assert client.post(f"{API}/auth/logout", headers=org["h"]).status_code == 204
    for path in ("/auth/whoami", "/data/summary", "/scenarios"):
        r = client.get(f"{API}{path}", headers=org["h"])
        assert r.status_code == 401 and r.json()["error"]["code"] == "AUTH_INVALID", path


def test_api_key_cannot_administer_the_organisation(client, world):
    a = world["a"]
    key = client.post(f"{API}/org/api-keys", json={"name": "akses"}, headers=a["h"])
    assert key.status_code == 201, key.text
    kh = {"Authorization": f"Bearer {key.json()['key']}"}
    assert client.get(f"{API}/data/summary", headers=kh).status_code == 200
    for method, path in (
        ("GET", "/org/members"),
        ("GET", "/org/api-keys"),
        ("POST", "/org/members"),
        ("POST", "/org/api-keys"),
        ("GET", "/auth/me"),
        ("GET", "/jobs"),
    ):
        r = _call(client, method, path, kh)
        assert r.status_code == 403 and r.json()["error"]["code"] == "FORBIDDEN", (path, r.text)
    client.post(f"{API}/org/api-keys/{key.json()['id']}/revoke", headers=a["h"])


def test_plan_horizon_on_every_forecast_producing_path(client, world):
    b = world["b"]  # trial: horizon max 3
    h = b["h"]

    def limited(r: Any) -> None:
        assert r.status_code == 403 and r.json()["error"]["code"] == "PLAN_LIMIT", r.text
        assert r.json()["error"]["details"]["limit"] == 3

    limited(client.post(f"{API}/forecasts", json={"horizon": 4}, headers=h))
    limited(client.post(f"{API}/scenarios", json={"name": "Jauh", "horizon": 6}, headers=h))
    sid = client.post(f"{API}/scenarios", json={"name": "Dekat", "horizon": 3}, headers=h).json()["id"]
    limited(client.post(f"{API}/scenarios/{sid}", json={"horizon": 6}, headers=h))
    limited(client.post(f"{API}/scenarios/compare", json={"scenario_ids": [sid], "horizon": 6}, headers=h))
    # a saved scenario longer than the current plan (e.g. after a downgrade) cannot be run or compared
    c = client.app.state.container
    long_id = new_id("scn")
    c.repo.insert_scenario(
        {"id": long_id, "org_id": b["org"], "created_by": "test", "name": "Lama", "horizon": 6, "inputs": {}}
    )
    limited(client.post(f"{API}/scenarios/{long_id}/run", headers=h))
    limited(client.post(f"{API}/scenarios/compare", json={"scenario_ids": [long_id]}, headers=h))
    limited(client.post(f"{API}/reports", json={"kind": "forecast", "params": {"horizon": 6}}, headers=h))
    limited(
        client.post(
            f"{API}/reports",
            json={"kind": "scenario_comparison", "params": {"scenario_ids": [sid], "horizon": 6}},
            headers=h,
        )
    )
    limited(
        client.post(
            f"{API}/reports",
            json={"kind": "scenario_comparison", "params": {"scenario_ids": [long_id]}},
            headers=h,
        )
    )


def test_report_inputs_and_roles_match_the_direct_actions(client, world):
    a = world["a"]
    for params in (
        {"horizon": 0},
        {"horizon": 7},
        {"scenario": {"brent": {"2026-13": 50}}},
        {"scenario": {"gold": {"2026-04": 1}}},
        {"bogus": 1},
        {"forecast_id": 123},
    ):
        r = client.post(f"{API}/reports", json={"kind": "forecast", "params": params}, headers=a["h"])
        assert r.status_code == 422 and r.json()["error"]["code"] == "VALIDATION_ERROR", (params, r.text)
    bad_cmp = client.post(
        f"{API}/reports", json={"kind": "scenario_comparison", "params": {"scenario_ids": []}}, headers=a["h"]
    )
    assert bad_cmp.status_code == 422
    v = a["viewer"]
    for body in (
        {"kind": "scenario_comparison", "params": {"scenario_ids": [a["scenario"]]}},
        {"kind": "forecast", "params": {"horizon": 1, "scenario": {"brent": {"2026-10": 70}}}},
    ):
        r = client.post(f"{API}/reports", json=body, headers=v)
        assert r.status_code == 403 and r.json()["error"]["code"] == "FORBIDDEN", r.text
    # scenario assumptions on a direct forecast also need analyst+
    r = client.post(
        f"{API}/forecasts", json={"horizon": 1, "scenario": {"brent": {"2026-10": 70}}}, headers=v
    )
    assert r.status_code == 403 and r.json()["error"]["code"] == "FORBIDDEN"
    ok = client.post(
        f"{API}/forecasts", json={"horizon": 1, "scenario": {"brent": {"2026-10": 70}}}, headers=a["analyst"]
    )
    assert ok.status_code == 503  # authorised; no production model yet in this part of the session


def test_history_is_clipped_to_the_plan(client, world):
    trial = client.get(f"{API}/data/history", headers=world["b"]["h"]).json()
    floor = trial["earliest_period"]
    assert trial["history_years"] == 3 and floor
    assert trial["points"][0]["period"] == floor and all(p["period"] >= floor for p in trial["points"])
    icp_months = [p for p in trial["points"] if p["icp_price"] is not None]
    assert len({p["period"] for p in trial["points"] if p["period"] <= icp_months[-1]["period"]}) == 36
    early = client.get(f"{API}/data/history", params={"start": "2019-01"}, headers=world["b"]["h"]).json()
    assert early["points"][0]["period"] == floor
    none = client.get(f"{API}/data/history", params={"end": "2019-06"}, headers=world["b"]["h"]).json()
    assert none["points"] == []
    full = client.get(f"{API}/data/history", headers=world["a"]["viewer"]).json()  # professional
    assert full["history_years"] is None and full["earliest_period"] is None
    assert full["points"][0]["period"] == "2019-01"


def test_temporary_password_must_be_changed_first(client, world):
    b = world["b"]
    inv = client.post(
        f"{API}/org/members",
        json={"email": "baru@bravo.akses.test", "name": "Baru", "role": "viewer"},
        headers=b["h"],
    )
    assert inv.status_code == 201, inv.text
    temp = inv.json()["temporary_password"]
    tok = client.post(f"{API}/auth/login", json={"email": "baru@bravo.akses.test", "password": temp}).json()[
        "token"
    ]
    h = {"Authorization": f"Bearer {tok}"}
    for method, path in (
        ("GET", "/data/summary"),
        ("GET", "/forecasts"),
        ("POST", "/forecasts"),
        ("GET", "/org"),
        ("GET", "/reports"),
        ("POST", "/auth/me"),
    ):
        r = _call(client, method, path, h)
        assert r.status_code == 403 and r.json()["error"]["code"] == "PASSWORD_CHANGE_REQUIRED", (
            path,
            r.text,
        )
    who = client.get(f"{API}/auth/whoami", headers=h).json()
    assert who["authenticated"] and who["must_change_password"] is True
    assert client.get(f"{API}/auth/me", headers=h).status_code == 200
    assert client.get(f"{API}/plans", headers=h).status_code == 200
    assert client.get(f"{API}/public/market-snapshot", headers=h).status_code == 200
    r = client.post(f"{API}/auth/password", json={"current_password": temp, "new_password": PW}, headers=h)
    assert r.status_code == 204
    tok = client.post(f"{API}/auth/login", json={"email": "baru@bravo.akses.test", "password": PW}).json()[
        "token"
    ]
    assert client.get(f"{API}/data/summary", headers={"Authorization": f"Bearer {tok}"}).status_code == 200


def test_readiness_hides_dependency_details_from_non_operators(client, admin):
    public = client.get("/health/ready").json()
    assert public["checks"] and all(c["detail"] is None and c["latency_ms"] is None for c in public["checks"])
    operator = client.get("/health/ready", headers=admin).json()
    assert any(c["detail"] for c in operator["checks"])


def test_invalid_credentials_carry_the_bearer_challenge(client):
    for header in ("Bearer not-a-real-token", "Basic abc"):
        r = client.get(f"{API}/data/summary", headers={"Authorization": header})
        assert r.status_code == 401 and r.json()["error"]["code"] == "AUTH_INVALID"
        assert r.headers.get("www-authenticate") == "Bearer"


def test_report_downloads_are_not_cached(client, world):
    a = world["a"]
    r = client.get(f"{API}/reports/{a['report']}/download", params={"format": "csv"}, headers=a["h"])
    assert r.status_code == 200 and r.headers["cache-control"] == "private, no-store"
