"""Scenario inputs the active method ignores are flagged on every path; official values supersede
'awaiting publication' labels in reports (Agent D, mission v6)."""

from __future__ import annotations

import copy
import csv
import io
from typing import Any

import pytest

from icp_api.storage.repository import new_id

API = "/api/v1"
PW = "Asumsi-Jelas-2026!"


@pytest.fixture(scope="module")
def setup(client: Any, trained_version: str) -> dict[str, Any]:
    op = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    client.post(
        f"{API}/models/versions/{trained_version}/promote",
        json={"reason": "input tests", "force": True},
        headers=op,
    )
    job = client.post(
        f"{API}/jobs", json={"job_type": "refresh_forecast", "params": {"horizon": 6}}, headers=op
    )
    assert client.app.state.jobs.wait(job.json()["id"], 120)["status"] == "succeeded"
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Asumsi Jelas (uji lokal)",
            "email": "owner@asumsi-jelas.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    h = {"Authorization": f"Bearer {r.json()['token']}"}
    latest = client.get(f"{API}/forecasts/latest", headers=h).json()
    p0 = latest["result"]["points"][0]
    used = {b for b, v in p0["benchmarks"].items() if v["used_by_model"]}
    unused = sorted({"brent", "dubai", "wti"} - used)
    assert unused, "test model must ignore at least one benchmark"
    return {
        "op": op,
        "h": h,
        "org": r.json()["organization"]["id"],
        "month": p0["period"],
        "unused": unused[0],
    }


def test_post_forecast_flags_ignored_inputs(client, setup):
    r = client.post(
        f"{API}/forecasts",
        json={"horizon": 1, "scenario": {setup["unused"]: {setup["month"]: 70.0}}},
        headers=setup["h"],
    )
    assert r.status_code == 201, r.text
    res = r.json()["result"]
    assert res["ignored_inputs"] == [setup["unused"]]
    assert res["input_usage"][0]["effect"] == "ignored"
    assert res["notes"]["ignored_inputs"]["id"] and res["notes"]["ignored_inputs"]["en"]
    # the point does not pretend to be scenario-conditioned
    assert res["points"][0]["conditional_on_scenario"] is False


def test_scenario_compare_flags_ignored_inputs(client, setup):
    sc = client.post(
        f"{API}/scenarios",
        json={"name": "Abaikan", "horizon": 1, "inputs": {setup["unused"]: {setup["month"]: 70.0}}},
        headers=setup["h"],
    )
    assert sc.status_code == 201, sc.text
    assert sc.json()["input_usage"][0]["effect"] == "ignored"
    cmp = client.post(
        f"{API}/scenarios/compare", json={"scenario_ids": [sc.json()["id"]], "horizon": 1}, headers=setup["h"]
    )
    assert cmp.status_code == 200, cmp.text
    s0 = cmp.json()["scenarios"][0]
    assert s0["ignored_inputs"] == [setup["unused"]] and s0["input_usage"][0]["effect"] == "ignored"


def _stale_record(client: Any, setup: dict[str, Any]) -> str:
    """A stored org forecast whose first month has since been published by ESDM."""
    c = client.app.state.container
    row = copy.deepcopy(c.repo.latest_system_forecast())
    last = c.datasets.active("icp").series.index.max()
    pts = row["result"]["points"][:4]
    for i, p in enumerate(pts):
        p.update(
            period=str(last + i),
            step=i + 1,
            kind="estimate_awaiting_publication" if i == 0 else "future_forecast",
        )
    row["result"]["points"] = pts
    row["result"]["horizon"] = 4
    row["result"]["forecast_origin"] = str(last - 1)
    fid = new_id("fc")
    row.update(
        id=fid,
        created_at="2003-01-01T00:00:00.000000Z",
        workflow="api",
        org_id=setup["org"],
        forecast_origin=str(last - 1),
        horizon=4,
        export_object_key=None,
    )
    c.repo.insert_forecast(row)
    return fid


def test_forecast_report_from_a_stored_run_is_clipped_and_uses_official_values(client, setup):
    fid = _stale_record(client, setup)
    r = client.post(
        f"{API}/reports",
        json={"kind": "forecast", "formats": ["csv"], "params": {"forecast_id": fid}},
        headers=setup["h"],
    )
    assert r.status_code == 201, r.text
    dl = client.get(f"{API}/reports/{r.json()['id']}/download?format=csv", headers=setup["h"])
    assert dl.status_code == 200, dl.text
    rows = list(csv.DictReader(io.StringIO(dl.content.decode("utf-8-sig"))))
    assert len(rows) == 3  # trial plan horizon, not the 4 stored steps
    assert "menunggu publikasi" not in rows[0]["Jenis"]
    assert "resmi" in rows[0]["Jenis"].lower()
