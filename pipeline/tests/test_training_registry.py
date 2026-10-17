"""End-to-end: backtest + train on synthetic data into a temporary MLflow store, validate, promote, forecast."""

from __future__ import annotations

import numpy as np
import pytest

from icp_ml.config import load_config, parse_config
from icp_ml.drift import drift_report, psi
from icp_ml.features import FEATURE_COLUMNS as FEATURES
from icp_ml.features import build_feature_frame
from icp_ml.forecasting import make_forecast
from icp_ml.registry import ModelRegistry
from icp_ml.training import DatasetRef, TrainingDataError, train_and_register

FAST_CONFIG = {
    "experiment_name": "test-exp",
    "model_name": "Test_Model",
    "min_feature_rows": 20,
    "methods": [
        "naive_last",
        "spread_brent_6m",
        {
            "name": "ridge_small",
            "family": "ridge",
            "params": {"alpha": 2.0},
            "features": ["brent_price", "icp_lag_1"],
        },
        # Tree models exercise the skops serialization path (trusted-type handling on save and load).
        {
            "name": "small_gbm",
            "family": "gradient_boosting",
            "params": {"n_estimators": 15, "max_depth": 2},
            "features": ["brent_price", "icp_lag_1"],
        },
        "blend_benchmarks",
    ],
    "backtest": {"horizons": [1, 2, 3], "min_train_rows": 24},
    "selection": {"mode": "observed", "horizons": [1, 2], "metric": "mae"},
}


@pytest.fixture
def store(tmp_path):
    return f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}", str(tmp_path / "artifacts")


def test_default_config_is_valid():
    cfg = load_config()
    assert cfg.methods and len(cfg.sha256) == 64 and cfg.selection.mode in cfg.backtest.modes


def test_config_rejects_unknown_families_and_features():
    with pytest.raises(ValueError):
        parse_config({"methods": [{"name": "x", "family": "deep_magic"}]})
    with pytest.raises(ValueError):
        parse_config({"methods": [{"name": "x", "family": "ridge", "features": ["moon_phase"]}]})
    with pytest.raises(ValueError):
        parse_config({"selection": {"mode": "dreams"}})


def test_train_validate_promote_forecast(synthetic_panel, store):
    cfg = parse_config(FAST_CONFIG)
    result = train_and_register(
        synthetic_panel,
        cfg,
        store[0],
        store[1],
        datasets=[DatasetRef("icp", "v1", "a" * 64)],
        job_id="job_test",
    )
    assert result.registered_version == "1" and len(result.candidates) == 5
    assert (
        result.best is not None and result.best.name == "spread_brent_6m"
    )  # data-generating rule, preferred when within 2%
    assert result.best.metrics["mae"] < result.naive["observed_h1_mae"]

    reg = ModelRegistry(store[0], "Test_Model")
    assert reg.production_version() is None
    info = reg.get_version("1")
    assert info.tags["icp.training_job_id"] == "job_test" and info.tags["icp.dataset.icp.sha256"] == "a" * 64
    spec = info.method_spec
    assert spec is not None and spec.name == "spread_brent_6m" and spec.benchmarks_used == ["brent"]
    frame = build_feature_frame(synthetic_panel)
    report = reg.validate_candidate("1", frame.tail(5), cfg.promotion)
    assert report.passed, report.to_dict()
    assert {c.name for c in report.checks} >= {
        "has_method_spec",
        "signature_matches_features",
        "beats_naive_baseline",
        "hold_last_not_worse_than_naive",
    }
    reg.set_production("1")
    assert reg.production_version() == "1"
    assert reg.validate_candidate("1", frame.tail(5), cfg.promotion).passed is False  # already production

    holdout = reg.holdout_predictions(info.run_id)
    assert (
        holdout is not None
        and list(holdout.columns) == ["period", "actual", "predicted"]
        and len(holdout) > 20
    )
    intervals = reg.intervals(info.run_id)
    assert (
        intervals
        and "observed" in intervals
        and "1" in intervals["observed"]
        and "0.8" in intervals["observed"]["1"]
    )
    assert reg.model_card(info.run_id) and "spread_brent_6m" in reg.model_card(info.run_id)
    metrics = reg.backtest_metrics(info.run_id)
    assert metrics is not None and set(metrics["mode"]) == {"observed", "hold_last"}
    runs, _ = reg.search_runs("test-exp", max_results=10)
    assert {r["candidate"] for r in runs} == {
        "naive_last",
        "spread_brent_6m",
        "ridge_small",
        "small_gbm",
        "blend_benchmarks",
    }
    for run in runs:  # every candidate (rules and tree models) must round-trip through skops
        model = reg.load_model_from_run(run["run_id"])
        assert np.isfinite(model.predict(frame.tail(3)[list(FEATURES)])).all()

    model = reg.load_model("1")
    fc = make_forecast(model, spec, synthetic_panel, horizon=3, intervals=intervals)
    assert len(fc.points) == 3 and all(np.isfinite(p.prediction) for p in fc.points)
    assert (
        fc.points[0].intervals and fc.points[0].interval_basis == "observed"
    )  # benchmarks exist for the first months after the origin


def test_training_refuses_too_little_data(synthetic_panel, store):
    with pytest.raises(TrainingDataError):
        train_and_register(
            synthetic_panel.iloc[:30], parse_config(FAST_CONFIG), store[0], store[1], datasets=[]
        )


def test_psi_and_drift_report(synthetic_panel):
    rng = np.random.default_rng(1)
    base = rng.normal(0, 1, 500)
    assert psi(base, rng.normal(0, 1, 500)) < 0.1
    assert psi(base, rng.normal(3, 1, 500)) > 0.2
    frame = build_feature_frame(synthetic_panel)
    report = drift_report(frame, frame.index[40], recent_months=12)
    assert (
        report.current_rows >= 3
        and set(report.features)
        and report.status in {"stable", "moderate", "significant"}
    )
