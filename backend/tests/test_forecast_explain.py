"""Forecast read-time explanation: plan clip, backtest evaluation, published actuals, notes,
scenario input usage, and the customer model card."""

from __future__ import annotations

import copy
import csv
import io
from typing import Any

import pytest

from icp_api.storage.repository import new_id

API = "/api/v1"
PW = "Proyeksi-Jelas-2026!"


@pytest.fixture(scope="module")
def setup(client: Any, trained_version: str) -> dict[str, Any]:
    op = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    client.post(
        f"{API}/models/versions/{trained_version}/promote",
        json={"reason": "explain tests", "force": True},
        headers=op,
    )
    job = client.post(
        f"{API}/jobs", json={"job_type": "refresh_forecast", "params": {"horizon": 6}}, headers=op
    )
    done = client.app.state.jobs.wait(job.json()["id"], 120)
    assert done["status"] == "succeeded", done
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Proyeksi Jelas",
            "email": "owner@proyeksi-jelas.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {
        "op": op,
        "h": {"Authorization": f"Bearer {r.json()['token']}"},
        "org": r.json()["organization"]["id"],
    }


def test_customers_see_system_forecasts_clipped_to_their_plan(client, setup):
    cust = client.get(f"{API}/forecasts/latest", headers=setup["h"]).json()
    ops = client.get(f"{API}/forecasts/latest", headers=setup["op"]).json()
    assert len(ops["result"]["points"]) == 6 and ops["plan_clip"] is None
    assert len(cust["result"]["points"]) == 3 and cust["result"]["horizon"] == 3
    assert cust["plan_clip"] == {"horizon_max": 3, "stored_horizon": 6, "clipped": True}
    assert cust["horizon"] == 6  # the stored record is unchanged
    raw = client.get(f"{API}/forecasts/{cust['id']}/export.csv", headers=setup["h"]).text
    steps = [int(r["step"]) for r in csv.DictReader(io.StringIO(raw))]
    assert steps == [1, 2, 3]
    full = client.get(f"{API}/forecasts/{cust['id']}/export.csv", headers=setup["op"]).text
    assert len(list(csv.DictReader(io.StringIO(full)))) == 6


def test_points_carry_mode_matched_backtest_evaluation_and_notes(client, setup):
    run = client.get(f"{API}/forecasts/latest", headers=setup["h"]).json()
    for p in run["result"]["points"]:
        ev = p["evaluation"]
        assert ev is not None and ev["mode"] == p["interval_basis"] and ev["horizon"] == p["step"]
        assert ev["n"] > 0 and ev["mae"] is not None
        assert p["kind_now"] == p["kind"] and p["actual"] is None
    notes = run["result"]["notes"]
    assert (
        notes["method"]["id"]
        and notes["method"]["en"]
        and notes["interval"]["en"]
        and notes["evaluation"]["id"]
    )


def test_published_actual_is_exposed_without_rewriting_the_record(client, setup):
    c = client.app.state.container
    latest = c.repo.latest_system_forecast()
    icp = c.datasets.active("icp").series
    last = icp.index.max()
    row = copy.deepcopy(latest)
    fid = new_id("fc")
    pts = row["result"]["points"][:2]
    pts[0].update(period=str(last - 1), step=1, kind="estimate_awaiting_publication")
    pts[1].update(period=str(last), step=2, kind="future_forecast")
    row["result"]["points"] = pts
    row["result"]["forecast_origin"] = str(last - 2)
    row.update(
        id=fid,
        created_at="2002-01-01T00:00:00.000000Z",
        workflow="api",
        org_id=setup["org"],
        forecast_origin=str(last - 2),
        horizon=2,
        export_object_key=None,
    )
    c.repo.insert_forecast(row)
    got = client.get(f"{API}/forecasts/{fid}", headers=setup["h"]).json()
    p0, p1 = got["result"]["points"]
    assert p1["kind"] == "future_forecast" and p1["kind_now"] == "official_published"
    assert p1["actual"]["period"] == str(last) and p1["actual"]["value"] == pytest.approx(float(icp[last]))
    assert p1["actual"]["label"]["id"] and p0["actual"]["value"] == pytest.approx(float(icp[last - 1]))
    stored = c.repo.get_forecast(fid)
    assert "actual" not in stored["result"]["points"][0]  # read-time only


def test_scenario_inputs_report_whether_the_method_uses_them(client, setup):
    run = client.get(f"{API}/forecasts/latest", headers=setup["h"]).json()
    month = run["result"]["points"][0]["period"]
    used = {b for b, v in run["result"]["points"][0]["benchmarks"].items() if v["used_by_model"]}
    r = client.post(
        f"{API}/forecasts",
        json={
            "horizon": 1,
            "scenario": {"dubai": {month: 70.0}, "brent": {month: 71.0}, "usd_idr": {month: 16000}},
        },
        headers=setup["h"],
    )
    assert r.status_code == 201, r.text
    usage = {u["input"]: u for u in r.json()["result"]["input_usage"]}
    assert usage["dubai"]["used_by_method"] == ("dubai" in used)
    assert usage["brent"]["used_by_method"] == ("brent" in used)
    assert usage["usd_idr"]["used_by_method"] is False and usage["dubai"]["scenario_months"] == [month]
    assert usage["dubai"]["note"]["en"]
    sc = client.post(
        f"{API}/scenarios",
        json={"name": "Dubai saja", "horizon": 1, "inputs": {"dubai": {month: 70.0}}},
        headers=setup["h"],
    ).json()
    assert sc["input_usage"][0]["input"] == "dubai" and sc["input_usage"][0]["used_by_method"] == (
        "dubai" in used
    )


def test_customer_model_card_hides_build_internals(client, setup, trained_version):
    cust = client.get(f"{API}/models/versions/{trained_version}", headers=setup["h"]).json()
    assert cust["git_commit"] is None and cust["config_sha256"] is None and cust["training_job_id"] is None
    assert "Code commit" not in (cust["model_card"] or "")
    listed = client.get(f"{API}/models", headers=setup["h"]).json()["versions"]
    assert all(v["git_commit"] is None and v["config_sha256"] is None for v in listed)
    op = client.get(f"{API}/models/versions/{trained_version}", headers=setup["op"]).json()
    assert op["config_sha256"]  # operators keep them
