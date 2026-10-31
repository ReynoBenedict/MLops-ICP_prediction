"""Plan limits enforced by the backend on every path, for all four plans (Agent D, mission v6).

history_years: /data/history, /data/verification, dataset download, provenance report
forecast_horizon_max: POST /forecasts, /forecasts/latest, export CSV, scenarios (create/update/run/compare),
                      forecast reports (new run and stored system forecast)
insights: GET /insights (the market report section is a known gap, see notes-D.md D-07)
"""

from __future__ import annotations

import csv
import dataclasses
import io
from typing import Any

import pytest

from icp_api.entitlements import PLANS

API = "/api/v1"
PW = "Matriks-Paket-2026!"
PLAN_CODES = ("trial", "starter", "professional", "enterprise")


@pytest.fixture(scope="module")
def ready(client: Any, trained_version: str) -> dict[str, str]:
    op = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    client.post(
        f"{API}/models/versions/{trained_version}/promote",
        json={"reason": "plan matrix", "force": True},
        headers=op,
    )
    job = client.post(
        f"{API}/jobs", json={"job_type": "refresh_forecast", "params": {"horizon": 6}}, headers=op
    )
    assert client.app.state.jobs.wait(job.json()["id"], 120)["status"] == "succeeded"
    return op


@pytest.fixture(scope="module")
def orgs(client: Any, ready: dict[str, str]) -> dict[str, dict[str, Any]]:
    out = {}
    for code in PLAN_CODES:
        r = client.post(
            f"{API}/auth/register",
            json={
                "organization_name": f"PT Matriks {code} (uji lokal)",
                "email": f"owner@matriks-{code}.test",
                "name": "U",
                "password": PW,
                "accept_terms": True,
            },
        )
        assert r.status_code == 201, r.text
        org = r.json()["organization"]["id"]
        if code != "trial":
            up = client.post(
                f"{API}/admin/organizations/{org}/plan",
                json={"plan": code, "reason": "plan matrix"},
                headers=ready,
            )
            assert up.status_code == 200, up.text
        out[code] = {"h": {"Authorization": f"Bearer {r.json()['token']}"}, "org": org, "plan": PLANS[code]}
    return out


def _floor(client: Any, years: int) -> str | None:
    return client.app.state.container.datasets.history_floor(years) if years else None


@pytest.mark.parametrize("code", PLAN_CODES)
def test_history_depth(client, orgs, code):
    o = orgs[code]
    floor = _floor(client, o["plan"].history_years)
    first_icp = str(client.app.state.container.datasets.active("icp").series.index.min())
    hist = client.get(f"{API}/data/history", headers=o["h"]).json()
    ver = client.get(f"{API}/data/verification", headers=o["h"]).json()
    if floor:
        assert hist["earliest_period"] == floor and min(r["period"] for r in hist["points"]) >= floor
        assert ver["earliest_period"] == floor and ver["months"][0]["period"] >= floor
    else:
        assert ver["months"][0]["period"] == first_icp
        assert hist["history_years"] in (None, 0)
    vid = client.get(f"{API}/datasets/active", headers=o["h"]).json()["datasets"]["brent"]["active_version"][
        "id"
    ]
    dl = client.get(f"{API}/datasets/versions/{vid}/download", headers=o["h"])
    rows = list(csv.DictReader(io.StringIO(dl.text)))
    assert (rows[0]["period"] >= floor) if floor else ("_from-" not in dl.headers["content-disposition"])
    rep = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["csv"]}, headers=o["h"])
    assert rep.status_code == 201, rep.text
    text = client.get(f"{API}/reports/{rep.json()['id']}/download?format=csv", headers=o["h"]).content
    n_rows = len(list(csv.DictReader(io.StringIO(text.decode("utf-8-sig")))))
    assert n_rows == len(ver["months"])


@pytest.mark.parametrize("code", PLAN_CODES)
def test_forecast_horizon_on_every_path(client, orgs, code):
    o = orgs[code]
    hmax = o["plan"].forecast_horizon_max
    h = o["h"]
    assert client.post(f"{API}/forecasts", json={"horizon": hmax}, headers=h).status_code == 201
    over = client.post(f"{API}/forecasts", json={"horizon": hmax + 1}, headers=h)
    assert over.status_code in (403, 422) and over.status_code != 201
    latest = client.get(f"{API}/forecasts/latest", headers=h).json()
    assert len(latest["result"]["points"]) <= hmax
    exp = client.get(f"{API}/forecasts/{latest['id']}/export.csv", headers=h).text
    assert max(int(r["step"]) for r in csv.DictReader(io.StringIO(exp))) <= hmax
    # scenarios
    assert client.post(
        f"{API}/scenarios", json={"name": "lebih", "horizon": hmax + 1, "inputs": {}}, headers=h
    ).status_code in (403, 422)
    sc = client.post(f"{API}/scenarios", json={"name": "pas", "horizon": hmax, "inputs": {}}, headers=h)
    assert sc.status_code == 201, sc.text
    sid = sc.json()["id"]
    assert client.post(f"{API}/scenarios/{sid}", json={"horizon": hmax + 1}, headers=h).status_code in (
        403,
        422,
    )
    cmp_over = client.post(
        f"{API}/scenarios/compare", json={"scenario_ids": [sid], "horizon": hmax + 1}, headers=h
    )
    assert cmp_over.status_code in (403, 422)
    run = client.post(f"{API}/scenarios/{sid}/run", headers=h)
    assert run.status_code in (200, 201), run.text
    assert len(run.json()["result"]["points"]) <= hmax
    # reports: a new forecast run, and a stored system forecast
    rep_over = client.post(
        f"{API}/reports",
        json={"kind": "forecast", "formats": ["csv"], "params": {"horizon": hmax + 1}},
        headers=h,
    )
    assert rep_over.status_code in (403, 422)
    rep = client.post(
        f"{API}/reports",
        json={"kind": "forecast", "formats": ["csv"], "params": {"forecast_id": latest["id"]}},
        headers=h,
    )
    assert rep.status_code == 201, rep.text
    text = client.get(f"{API}/reports/{rep.json()['id']}/download?format=csv", headers=h).content.decode(
        "utf-8-sig"
    )
    assert 0 < len(list(csv.DictReader(io.StringIO(text)))) <= hmax


@pytest.mark.parametrize("code", PLAN_CODES)
@pytest.mark.parametrize("enabled", [True, False])
def test_insights_feature_gate(client, orgs, monkeypatch, code, enabled):
    monkeypatch.setitem(PLANS, code, dataclasses.replace(PLANS[code], insights=enabled))
    o = orgs[code]
    r = client.get(f"{API}/insights", headers=o["h"])
    assert r.status_code == (200 if enabled else 403), r.text
    if not enabled:
        assert r.json()["error"]["code"] == "FEATURE_NOT_IN_PLAN"


def test_scenario_saved_under_a_bigger_plan_cannot_run_beyond_the_new_plan(client, orgs, ready):
    o = orgs["starter"]
    sc = client.post(f"{API}/scenarios", json={"name": "enam", "horizon": 6, "inputs": {}}, headers=o["h"])
    assert sc.status_code == 201
    client.post(
        f"{API}/admin/organizations/{o['org']}/plan",
        json={"plan": "trial", "reason": "downgrade"},
        headers=ready,
    )
    try:
        r = client.post(f"{API}/scenarios/{sc.json()['id']}/run", headers=o["h"])
        assert r.status_code == 403 and r.json()["error"]["code"] == "PLAN_LIMIT"
    finally:
        client.post(
            f"{API}/admin/organizations/{o['org']}/plan",
            json={"plan": "starter", "reason": "restore"},
            headers=ready,
        )
