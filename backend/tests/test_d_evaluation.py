"""Forecast evaluation honesty (Agent D, mission v6).

Every accuracy figure states its evaluation period, n, horizon, mode and baseline; interval coverage is
measured (never called calibrated) and under-coverage is flagged for the UI."""

from __future__ import annotations

import csv
import io
from typing import Any

import pandas as pd
import pytest

from icp_api.insights.context import step_evaluation
from icp_ml.evaluation import baseline_by_step, coverage_window

API = "/api/v1"
PW = "Evaluasi-Jujur-2026!"

FACTS: dict[str, Any] = {
    "version": "5",
    "method": "spread_brent_6m",
    "by_horizon": [
        {"mode": "hold_last", "horizon": 1, "n": 69, "mae": 6.1, "mase": 1.1, "dir_acc": 0.53},
        {"mode": "hold_last", "horizon": 3, "n": 67, "mae": 10.6, "mase": 1.9, "dir_acc": 0.5},
        {"mode": "observed", "horizon": 1, "n": 69, "mae": 0.8, "mase": 0.15, "dir_acc": 0.96},
    ],
    "coverage": {
        "hold_last": {
            "1": {"0.5": 0.5, "0.8": 0.81, "0.95": 0.93, "n": 28},
            "3": {"0.5": 0.4, "0.8": 0.65, "0.95": 0.8, "n": 27},
        },
        "observed": {"1": {"0.5": 0.5, "0.8": 0.8, "0.95": 0.95, "n": 12}},
    },
    "backtest_origins": "2020-12..2026-08",
    "baselines": {
        "hold_last": {"1": {"mae": 6.07, "n": 69}, "3": {"mae": 10.5, "n": 67}},
        "observed": {"1": {"mae": 6.07, "n": 69}},
    },
    "coverage_window": {"origin_start": "2024-04", "origin_end": "2026-08"},
}


def test_point_evaluation_states_period_n_horizon_mode_and_baseline():
    ev = step_evaluation(FACTS, "hold_last", 1)
    assert ev is not None
    assert (ev["mode"], ev["horizon"], ev["n"]) == ("hold_last", 1, 69)
    assert (ev["origin_start"], ev["origin_end"]) == ("2020-12", "2026-08")
    assert ev["baseline"]["method"] == "naive_last" and ev["baseline"]["mae"] == pytest.approx(6.07)
    assert ev["baseline"]["n"] == 69 and ev["baseline"]["label"]["id"] and ev["baseline"]["label"]["en"]
    assert ev["beats_baseline"] is False  # 6.1 vs 6.07
    assert ev["coverage_period"] == {"origin_start": "2024-04", "origin_end": "2026-08"}
    for lang in ("id", "en"):
        s = ev["summary"][lang]
        assert "2020-12" in s and "69" in s and "6.07" in s.replace(",", ".")
    assert ev["under_coverage"] == [] and ev["coverage_warning"] is None


def test_english_summary_uses_singular_and_plural_months():
    one, three = step_evaluation(FACTS, "hold_last", 1), step_evaluation(FACTS, "hold_last", 3)
    assert one is not None and three is not None
    assert "1 month ahead" in one["summary"]["en"] and "3 months ahead" in three["summary"]["en"]
    assert "(s)" not in one["summary"]["en"] + three["summary"]["en"]


def test_under_coverage_is_flagged_for_the_ui():
    ev = step_evaluation(FACTS, "hold_last", 3)
    assert ev is not None
    assert ev["under_coverage"] == ["0.8", "0.95"]  # 0.65 < 0.8 - 0.10 ; 0.8 < 0.95 - 0.10
    assert "80%" in ev["coverage_warning"]["id"] and "65%" in ev["coverage_warning"]["en"]


def test_small_coverage_samples_are_flagged_as_uncertain():
    ev = step_evaluation(FACTS, "observed", 1)
    assert ev is not None and ev["under_coverage"] == []
    assert ev["coverage_warning"] is not None and "12" in ev["coverage_warning"]["en"]


def test_missing_baseline_artefacts_degrade_to_null_not_fabricated():
    facts = {k: v for k, v in FACTS.items() if k not in ("baselines", "coverage_window", "backtest_origins")}
    ev = step_evaluation(facts, "hold_last", 1)
    assert ev is not None
    assert ev["baseline"] is None and ev["beats_baseline"] is None
    assert ev["origin_start"] is None and ev["coverage_period"] is None
    assert ev["summary"]["en"]  # still says n / horizon / mode


def test_baseline_by_step_is_naive_persistence_on_the_same_cases():
    df = pd.DataFrame(
        {
            "method": ["m"] * 4,
            "mode": ["hold_last", "hold_last", "observed", "hold_last"],
            "origin": ["2021-01", "2021-02", "2021-01", "2021-01"],
            "horizon": [1, 1, 1, 2],
            "target": ["2021-02", "2021-03", "2021-02", "2021-03"],
            "y_true": [10.0, 14.0, 10.0, 14.0],
            "y_pred": [0.0] * 4,
            "last_official": [8.0, 10.0, 8.0, 8.0],
        }
    )
    b = baseline_by_step(df)
    assert b["hold_last"]["1"] == {"mae": pytest.approx(3.0), "n": 2}
    assert b["hold_last"]["2"] == {"mae": pytest.approx(6.0), "n": 1}
    assert b["observed"]["1"]["mae"] == pytest.approx(2.0)


def test_coverage_window_matches_the_split_rule():
    origins = [str(p) for p in pd.period_range("2020-01", periods=20, freq="M")]
    df = pd.DataFrame({"origin": origins, "mode": "hold_last", "horizon": 1})
    assert coverage_window(df) == {"origin_start": "2021-01", "origin_end": "2021-08"}  # index 12 of 20
    assert coverage_window(df.head(9)) is None  # < 10 origins: no out-of-sample check


# --- API ------------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def setup(client: Any, trained_version: str) -> dict[str, Any]:
    op = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    client.post(
        f"{API}/models/versions/{trained_version}/promote",
        json={"reason": "evaluation tests", "force": True},
        headers=op,
    )
    job = client.post(
        f"{API}/jobs", json={"job_type": "refresh_forecast", "params": {"horizon": 3}}, headers=op
    )
    assert client.app.state.jobs.wait(job.json()["id"], 120)["status"] == "succeeded"
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Evaluasi Jujur (uji lokal)",
            "email": "owner@evaluasi-jujur.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"op": op, "h": {"Authorization": f"Bearer {r.json()['token']}"}, "version": trained_version}


def test_forecast_points_disclose_period_and_baseline(client, setup):
    run = client.get(f"{API}/forecasts/latest", headers=setup["h"]).json()
    for p in run["result"]["points"]:
        ev = p["evaluation"]
        assert ev["origin_start"] and ev["origin_end"] and ev["origin_start"] <= ev["origin_end"]
        assert ev["baseline"]["method"] == "naive_last" and ev["baseline"]["mae"] is not None
        assert ev["baseline"]["n"] == ev["n"]
        assert ev["summary"]["id"] and ev["summary"]["en"]
        assert isinstance(ev["under_coverage"], list)
    notes = run["result"]["notes"]
    assert "coverage_warning" in notes
    for lang in ("id", "en"):
        assert (
            "kalibr" not in notes["interval"][lang].lower()
            and "calibrat" not in notes["interval"][lang].lower()
        )


def test_model_card_has_an_evaluation_disclosure(client, setup):
    d = client.get(f"{API}/models/versions/{setup['version']}", headers=setup["h"]).json()
    disc = d["evaluation_disclosure"]
    assert disc["calibrated"] is False and disc["baseline_method"] == "naive_last"
    assert disc["origin_start"] and disc["origin_end"]
    modes = {r["mode"] for r in disc["rows"]}
    assert modes == {"observed", "hold_last"}
    for r in disc["rows"]:
        assert r["n"] > 0 and r["baseline_mae"] is not None and isinstance(r["under_coverage"], list)
    assert disc["note"]["id"] and disc["note"]["en"]


def test_export_csv_carries_evaluation_and_usage_columns(client, setup):
    run = client.get(f"{API}/forecasts/latest", headers=setup["h"]).json()
    text = client.get(f"{API}/forecasts/{run['id']}/export.csv", headers=setup["h"]).text
    rows = list(csv.DictReader(io.StringIO(text)))
    assert rows
    for col in (
        "kind_now",
        "official_icp_usd",
        "official_basis",
        "brent_used_by_method",
        "dubai_used_by_method",
        "wti_used_by_method",
        "eval_mode",
        "eval_n",
        "eval_mae_usd",
        "baseline_naive_mae_usd",
        "eval_origin_start",
        "eval_origin_end",
        "coverage_80",
        "coverage_n",
        "coverage_warning",
    ):
        assert col in rows[0], col
    pts = {p["period"]: p for p in run["result"]["points"]}
    for r in rows:
        p = pts[r["period"]]
        assert r["eval_n"] == str(p["evaluation"]["n"])
        for b in ("brent", "dubai", "wti"):
            assert r[f"{b}_used_by_method"] == str(p["benchmarks"][b]["used_by_model"]).lower()
