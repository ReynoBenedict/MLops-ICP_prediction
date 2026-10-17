from __future__ import annotations

import numpy as np

from icp_ml.backtest import (
    BacktestConfig,
    common_subset,
    forecast_path,
    interval_tables,
    leaderboard,
    rolling_origin,
    split_coverage,
    summarize,
)
from icp_ml.features import FEATURE_COLUMNS, TARGET_COLUMN, build_feature_frame, build_panel
from icp_ml.methods import DEFAULT_METHODS, MethodSpec, find_method

CFG = BacktestConfig(horizons=(1, 2, 3), min_train_rows=24)


def test_naive_path_never_sees_future_icp(synthetic_panel):
    res = rolling_origin(synthetic_panel, (find_method("naive_last"),), CFG)
    p = res.predictions
    # Persistence from the origin: every horizon predicts ICP(origin), never a later official value.
    assert np.allclose(p["y_pred"], p["last_official"])
    assert set(p["mode"]) == {"observed", "hold_last"}


def test_spread_rule_recovers_exact_relation(make_series):
    brent = make_series("2019-01", 60 + np.arange(60) * 0.5, "brent_price")
    icp = make_series("2019-01", 60 + np.arange(60) * 0.5 + 2.0, "icp_price")  # ICP = Brent + 2 exactly
    panel = build_panel(icp, {"brent": brent})
    res = rolling_origin(
        panel, (MethodSpec("rule", "spread_rule", {"benchmark": "brent", "window": 6}),), CFG
    )
    obs = res.predictions[res.predictions["mode"] == "observed"]
    assert np.allclose(obs["error"], 0, atol=1e-9)
    held = res.predictions[res.predictions["mode"] == "hold_last"]
    # Held-last benchmarks lag the trend by 0.5 per month of horizon.
    assert np.allclose(held["error"], -0.5 * held["horizon"], atol=1e-9)


def test_forecast_path_labels_sources_and_recurses(synthetic_panel):
    spec = find_method("ridge_benchmarks")
    origin = synthetic_panel[TARGET_COLUMN].dropna().index.max() - 1  # benchmarks exist for 3 months after it
    frame = build_feature_frame(synthetic_panel.loc[:origin], spec.requires)
    model = spec.build().fit(frame[FEATURE_COLUMNS], frame[TARGET_COLUMN])
    rows = forecast_path(
        model, spec, synthetic_panel, origin, (1, 2, 3, 4), "observed", {"brent_price": {origin + 3: 123.0}}
    )
    assert [r["sources"]["brent_price"] for r in rows] == [
        "observed",
        "observed",
        "scenario",
        "assumed_hold_last",
    ]
    assert rows[2]["benchmarks"]["brent_price"] == 123.0
    held = synthetic_panel.loc[origin, "brent_price"]
    assert rows[3]["benchmarks"]["brent_price"] == held
    hold = forecast_path(model, spec, synthetic_panel, origin, (1, 2), "hold_last")
    assert all(r["sources"]["brent_price"] == "assumed_hold_last" for r in hold)
    assert all(r["benchmarks"]["brent_price"] == held for r in hold)


def test_common_subset_leaderboard_and_intervals(synthetic_panel):
    res = rolling_origin(
        synthetic_panel,
        (find_method("naive_last"), find_method("spread_brent_6m"), find_method("ridge_full")),
        CFG,
    )
    common = common_subset(res.predictions)
    counts = common.groupby(["mode", "origin", "horizon"])["method"].nunique()
    assert (counts == 3).all()
    board = leaderboard(summarize(common), "observed", (1, 2, 3), "mae")
    assert board.iloc[0]["method"] == "spread_brent_6m"  # the data-generating rule wins
    assert board.set_index("method").loc["naive_last", "score"] > board.iloc[0]["score"]
    tables = interval_tables(res.predictions, "spread_brent_6m", (0.8,))
    lo, hi = tables["observed"]["1"]["0.8"]
    assert lo < 0 < hi
    cov = split_coverage(res.predictions, "spread_brent_6m", (0.8,))
    assert 0.5 <= cov["observed"]["1"]["0.8"] <= 1.0


def test_every_default_method_builds_and_declares_inputs():
    for m in DEFAULT_METHODS:
        est = m.build()
        assert hasattr(est, "fit") and hasattr(est, "predict")
        assert m.requires
        if m.family == "spread_rule":
            assert m.benchmarks_used == [m.params["benchmark"]]
