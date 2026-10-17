"""Accounts, entitlements and the business product surface (insights, scenarios, alerts, reports).

Runs against the same session app as test_api.py; the model lifecycle fixture (``trained_version``)
is reused so forecasts, scenarios and reports exercise the real production model.
"""

from __future__ import annotations

import pytest

PW = "Sangat-Rahasia-2026!"


@pytest.fixture(scope="session")
def owner(client, trained_version):
    """A fresh trial organisation with its owner signed in (production model already promoted)."""
    operator = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    client.post(
        f"/api/v1/models/versions/{trained_version}/promote",
        json={"reason": "product tests", "force": True},
        headers=operator,
    )
    r = client.post(
        "/api/v1/auth/register",
        json={
            "organization_name": "PT Contoh Energi",
            "email": "Owner@Contoh.co.id",
            "name": "Owner",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert (
        body["organization"]["plan"] == "trial"
        and body["user"]["role"] == "owner"
        and body["user"]["email"] == "owner@contoh.co.id"
    )
    return {
        "headers": {"Authorization": f"Bearer {body['token']}"},
        "org": body["organization"],
        "user": body["user"],
    }


def test_register_validation_and_login_flow(client):
    bad = client.post(
        "/api/v1/auth/register",
        json={
            "organization_name": "X",
            "email": "nope",
            "name": "n",
            "password": "short",
            "accept_terms": True,
        },
    )
    assert bad.status_code == 422
    r = client.post(
        "/api/v1/auth/register",
        json={
            "organization_name": "PT Login Uji",
            "email": "login@uji.id",
            "name": "L",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201
    dup = client.post(
        "/api/v1/auth/register",
        json={
            "organization_name": "PT Lain",
            "email": "login@uji.id",
            "name": "L",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert dup.status_code == 409 and dup.json()["error"]["code"] == "EMAIL_TAKEN"
    wrong = client.post("/api/v1/auth/login", json={"email": "login@uji.id", "password": "salah-salah-salah"})
    assert wrong.status_code == 401
    ok = client.post("/api/v1/auth/login", json={"email": "LOGIN@uji.id", "password": PW})
    assert ok.status_code == 200
    token = ok.json()["token"]
    who = client.get("/api/v1/auth/whoami", headers={"Authorization": f"Bearer {token}"}).json()
    assert (
        who["authenticated"]
        and who["kind"] == "user"
        and who["role"] == "owner"
        and who["plan"] == "trial"
        and who["horizon_max"] == 3
    )
    me = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"}).json()
    assert me["user"]["email"] == "login@uji.id" and me["sessions"]
    assert client.post("/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}"}).status_code == 204
    assert client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"}).status_code == 401
    # password hashes never leak through any endpoint
    assert "password" not in ok.text.replace("must_change_password", "")


def test_members_roles_and_seat_limits(client, owner, admin):
    h = owner["headers"]
    inv = client.post(
        "/api/v1/org/members",
        json={"email": "analis@contoh.co.id", "name": "Analis", "role": "analyst"},
        headers=h,
    )
    assert inv.status_code == 201, inv.text
    temp = inv.json()["temporary_password"]
    # trial plan: 2 seats -> third member refused with the plan limit
    full = client.post(
        "/api/v1/org/members", json={"email": "ketiga@contoh.co.id", "name": "K", "role": "viewer"}, headers=h
    )
    assert full.status_code == 403 and full.json()["error"]["code"] == "SEAT_LIMIT"
    login = client.post("/api/v1/auth/login", json={"email": "analis@contoh.co.id", "password": temp}).json()
    ah = {"Authorization": f"Bearer {login['token']}"}
    assert login["user"]["must_change_password"] is True
    # analysts cannot administer members
    assert (
        client.post(
            "/api/v1/org/members", json={"email": "x@contoh.co.id", "name": "x", "role": "viewer"}, headers=ah
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/api/v1/auth/password", json={"current_password": temp, "new_password": PW + "x"}, headers=ah
        ).status_code
        == 204
    )
    assert (
        client.get("/api/v1/auth/me", headers=ah).status_code == 401
    )  # sessions revoked after password change
    members = client.get("/api/v1/org/members", headers=h).json()
    assert {m["email"] for m in members} == {"owner@contoh.co.id", "analis@contoh.co.id"}
    uid = next(m["id"] for m in members if m["email"] == "analis@contoh.co.id")
    upd = client.post(f"/api/v1/org/members/{uid}", json={"status": "disabled"}, headers=h)
    assert upd.status_code == 200 and upd.json()["status"] == "disabled"
    relogin = client.post("/api/v1/auth/login", json={"email": "analis@contoh.co.id", "password": PW + "x"})
    assert relogin.status_code == 403 and relogin.json()["error"]["code"] == "ACCOUNT_DISABLED"
    # operators see tenants but tenants never see each other
    orgs = client.get("/api/v1/admin/organizations", headers=admin).json()["items"]
    assert any(o["slug"].startswith("pt-contoh-energi") for o in orgs)
    assert client.get("/api/v1/admin/organizations", headers=h).status_code == 403
    assert (
        client.get("/api/v1/org/members", params={"org_id": owner["org"]["id"]}, headers=admin).status_code
        == 200
    )


def test_entitlements_are_enforced_server_side(client, owner, admin):
    h = owner["headers"]
    # trial: horizon max 3, no API access, no Excel
    too_far = client.post("/api/v1/forecasts", json={"horizon": 6}, headers=h)
    assert too_far.status_code == 403 and too_far.json()["error"]["code"] == "PLAN_LIMIT"
    assert client.post("/api/v1/org/api-keys", json={"name": "ci"}, headers=h).status_code == 403
    anon_scenario = client.post("/api/v1/forecasts", json={"horizon": 1, "scenario": {"brent": {}}})
    assert anon_scenario.status_code == 201  # empty scenario = no scenario
    anon_far = client.post("/api/v1/forecasts", json={"horizon": 4})
    assert anon_far.status_code == 403
    # upgrade by the operator, then the limits move
    up = client.post(
        f"/api/v1/admin/organizations/{owner['org']['id']}/plan",
        json={"plan": "professional", "reason": "test upgrade"},
        headers=admin,
    )
    assert (
        up.status_code == 200
        and up.json()["plan"] == "professional"
        and up.json()["entitlements"]["api_access"] is True
    )
    assert client.post("/api/v1/forecasts", json={"horizon": 6}, headers=h).status_code == 201
    key = client.post("/api/v1/org/api-keys", json={"name": "ci"}, headers=h)
    assert key.status_code == 201 and key.json()["key"].startswith("icp_live_")
    kh = {"Authorization": f"Bearer {key.json()['key']}"}
    who = client.get("/api/v1/auth/whoami", headers=kh).json()
    assert who["kind"] == "user" and who["role"] == "analyst" and who["org_id"] == owner["org"]["id"]
    assert client.get("/api/v1/org/members", headers=kh).status_code == 403  # keys are never administrative
    listed = client.get("/api/v1/org/api-keys", headers=h).json()
    assert listed and "key" not in listed[0] and listed[0]["prefix"] == key.json()["prefix"]
    assert client.post(f"/api/v1/org/api-keys/{key.json()['id']}/revoke", headers=h).status_code == 204
    assert client.get("/api/v1/auth/whoami", headers=kh).status_code == 401
    org = client.get("/api/v1/org", headers=h).json()
    assert org["usage_this_month"].get("forecast.run", 0) >= 1 and org["plan"] == "professional"


def test_public_snapshot_insights_and_latest_forecast(client, admin, owner):
    snap = client.get("/api/v1/public/market-snapshot").json()
    assert (
        snap["icp_period"]
        and snap["icp_usd"] > 0
        and snap["icp_idr"] > 1_000_000
        and snap["freshness"]["status"]
    )
    ins_anon = client.get("/api/v1/insights").json()
    assert ins_anon["disclaimer"] and len(ins_anon["insights"]) <= 3
    ins = client.get("/api/v1/insights", headers=owner["headers"]).json()
    kinds = {i["kind"] for i in ins["insights"]}
    assert "material_change" in kinds and all(i["generated"] for i in ins["insights"])
    assert all(
        i["sources"] and i["sources"][0]["version_id"]
        for i in ins["insights"]
        if i["kind"] != "forecast_change"
    )
    before = client.get("/api/v1/forecasts/latest")  # may already exist: ingest/train post-hooks publish one
    before_id = before.json()["id"] if before.status_code == 200 else None
    r = client.post(
        "/api/v1/jobs", json={"job_type": "refresh_forecast", "params": {"horizon": 4}}, headers=admin
    )
    job = client.app.state.jobs.wait(r.json()["id"], 120)
    assert job["status"] == "succeeded", job
    latest = client.get("/api/v1/forecasts/latest").json()
    assert (
        latest["workflow"] == "system"
        and latest["requested_by"] == "system"
        and len(latest["result"]["points"]) == 4
    )
    assert latest["id"] != before_id and latest["id"] == job["result"]["forecast_id"]
    assert client.get("/api/v1/public/market-snapshot").json()["latest_forecast"]["id"] == latest["id"]
    anon_list = client.get("/api/v1/forecasts").json()["items"]
    assert anon_list and all(f["workflow"] == "system" for f in anon_list)


def test_scenarios_alerts_and_reports(client, owner, admin):
    h = owner["headers"]
    sc = client.post(
        "/api/v1/scenarios",
        json={
            "name": "Brent tinggi",
            "description": "Brent naik",
            "horizon": 3,
            "inputs": {"brent": {}, "dubai": {}, "wti": {}, "usd_idr": {}},
        },
        headers=h,
    )
    assert sc.status_code == 201, sc.text
    sid = sc.json()["id"]
    run = client.post(f"/api/v1/scenarios/{sid}/run", headers=h)
    assert (
        run.status_code == 201 and run.json()["workflow"] == "scenario" and run.json()["scenario_id"] == sid
    )
    periods = [p["period"] for p in run.json()["result"]["points"]]
    upd = client.post(
        f"/api/v1/scenarios/{sid}",
        json={"inputs": {"brent": {periods[-1]: 120.0}, "dubai": {}, "wti": {}, "usd_idr": {}}},
        headers=h,
    )
    assert upd.status_code == 200 and upd.json()["inputs"] == {"brent": {periods[-1]: 120.0}}
    sc2 = client.post(
        "/api/v1/scenarios",
        json={"name": "Brent rendah", "horizon": 3, "inputs": {"brent": {periods[-1]: 50.0}}},
        headers=h,
    ).json()
    cmp = client.post("/api/v1/scenarios/compare", json={"scenario_ids": [sid, sc2["id"]]}, headers=h)
    assert cmp.status_code == 200, cmp.text
    rows = cmp.json()["rows"]
    last = rows[-1]["scenarios"]
    assert (
        last[sid]["delta_vs_baseline"] > 0 > last[sc2["id"]]["delta_vs_baseline"]
        or last[sid]["inputs"]["brent"]["source"] == "scenario"
    )
    assert last[sid]["conditional_on_scenario"] is True
    mine = client.get("/api/v1/scenarios", headers=h).json()["items"]
    assert {s["id"] for s in mine} == {sid, sc2["id"]}
    # another organisation cannot see them
    other = client.post(
        "/api/v1/auth/register",
        json={
            "organization_name": "PT Lain Sekali",
            "email": "lain@lain.id",
            "name": "L",
            "password": PW,
            "accept_terms": True,
        },
    ).json()
    oh = {"Authorization": f"Bearer {other['token']}"}
    assert client.get(f"/api/v1/scenarios/{sid}", headers=oh).status_code == 404
    assert client.get("/api/v1/scenarios", headers=oh).json()["items"] == []
    assert client.post(f"/api/v1/scenarios/{sid}/archive", headers=h).status_code == 204

    # alerts: rule creation evaluates immediately; events are tenant-scoped and de-duplicated
    kinds = {k["kind"] for k in client.get("/api/v1/alerts/kinds").json()}
    assert "icp_published" in kinds
    rule = client.post("/api/v1/alerts/rules", json={"kind": "icp_published"}, headers=h)
    assert rule.status_code == 201, rule.text
    bad = client.post(
        "/api/v1/alerts/rules", json={"kind": "icp_change_pct", "params": {"threshold_pct": 500}}, headers=h
    )
    assert bad.status_code == 422
    ev = client.get("/api/v1/alerts/events", headers=h).json()
    assert ev["unread"] >= 1 and ev["items"][0]["rule_id"] == rule.json()["id"]
    again = client.post("/api/v1/alerts/evaluate", headers=h).json()
    assert again["created"] == {}  # already notified for this ICP month
    assert client.get("/api/v1/alerts/events", headers=oh).json()["items"] == []
    assert client.post("/api/v1/alerts/events/read", headers=h).json()["marked"] >= 1
    assert client.get("/api/v1/alerts/events", headers=h).json()["unread"] == 0
    assert client.post(f"/api/v1/alerts/rules/{rule.json()['id']}/delete", headers=h).status_code == 204

    # reports: PDF + Excel + CSV, branded, downloadable, tenant-scoped
    rep = client.post("/api/v1/reports", json={"kind": "market_monthly"}, headers=h)
    assert rep.status_code == 201, rep.text
    rid = rep.json()["id"]
    plan_now = client.get("/api/v1/org", headers=h).json()[
        "entitlements"
    ]  # order-independent: Excel depends on the plan
    assert set(rep.json()["formats"]) == ({"pdf", "csv"} | ({"xlsx"} if plan_now["reports_excel"] else set()))
    pdf = client.get(f"/api/v1/reports/{rid}/download", params={"format": "pdf"}, headers=h)
    assert pdf.status_code == 200 and pdf.content[:4] == b"%PDF" and len(pdf.content) > 20_000
    xlsx = client.get(f"/api/v1/reports/{rid}/download", params={"format": "xlsx"}, headers=h)
    assert (
        (xlsx.status_code == 200 and xlsx.content[:2] == b"PK")
        if plan_now["reports_excel"]
        else xlsx.status_code == 404
    )
    csv_ = client.get(f"/api/v1/reports/{rid}/download", params={"format": "csv"}, headers=h)
    assert csv_.status_code == 200 and "period" in csv_.text.splitlines()[0]
    assert client.get(f"/api/v1/reports/{rid}", headers=oh).status_code == 404
    fc_rep = client.post(
        "/api/v1/reports",
        json={"kind": "forecast", "params": {"forecast_id": run.json()["id"]}, "formats": ["pdf"]},
        headers=h,
    )
    assert fc_rep.status_code == 201 and fc_rep.json()["formats"] == ["pdf"]
    cmp_rep = client.post(
        "/api/v1/reports",
        json={
            "kind": "scenario_comparison",
            "params": {"scenario_ids": [sc2["id"]]},
            "formats": ["pdf", "csv"],
        },
        headers=h,
    )
    assert cmp_rep.status_code == 201, cmp_rep.text
    prov = client.post("/api/v1/reports", json={"kind": "data_provenance", "formats": ["xlsx"]}, headers=h)
    assert prov.status_code == 201
    listed = client.get("/api/v1/reports", headers=h).json()["items"]
    assert {r["kind"] for r in listed} >= {
        "market_monthly",
        "forecast",
        "scenario_comparison",
        "data_provenance",
    }
    # operators can generate platform reports without an organisation
    op = client.post("/api/v1/reports", json={"kind": "market_monthly", "formats": ["pdf"]}, headers=admin)
    assert op.status_code == 201 and op.json()["org_id"] is None
