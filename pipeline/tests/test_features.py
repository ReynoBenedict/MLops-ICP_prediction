from __future__ import annotations

import pandas as pd
import pytest

from icp_ml.features import (
    FEATURE_COLUMNS,
    MissingFeatureData,
    build_feature_frame,
    build_panel,
    chronological_split,
    feature_row,
)
from icp_ml.schema import load_series


def test_lags_never_bridge_calendar_gaps(make_series):
    icp = make_series("2020-01", list(range(1, 13)), "icp_price").drop(pd.Period("2020-08", "M"))
    wti = make_series("2019-07", [50.0] * 18, "wti_price")
    frame = build_feature_frame(
        build_panel(icp, {"wti": wti}), ["icp_lag_1", "icp_lag_3", "icp_mean_3", "wti_price"]
    )
    # 2020-08 is missing: it is not a target, and any target whose lags touch it is excluded.
    for excluded in ("2020-08", "2020-09", "2020-10", "2020-11"):
        assert pd.Period(excluded, "M") not in frame.index
    assert pd.Period("2020-07", "M") in frame.index and pd.Period("2020-12", "M") in frame.index
    row = frame.loc[pd.Period("2020-07", "M")]
    assert row["icp_lag_1"] == 6 and row["icp_lag_3"] == 4 and row["icp_mean_3"] == 5


def test_feature_row_matches_frame_rows(synthetic_panel):
    frame = build_feature_frame(synthetic_panel)
    assert len(frame) >= 60
    for period, expected in frame.iterrows():
        row = feature_row(synthetic_panel, period)
        assert row == pytest.approx(expected[FEATURE_COLUMNS].to_dict())


def test_feature_row_matches_frame_on_real_data(repo_data):
    icp = load_series(repo_data[0], "icp")
    wti = load_series(repo_data[1], "wti")
    panel = build_panel(icp, {"wti": wti})
    required = [
        "wti_price",
        "icp_lag_1",
        "icp_lag_3",
        "icp_mean_3",
        "wti_lag_1",
        "spread_wti_lag_1",
        "spread_wti_mean_3",
    ]
    frame = build_feature_frame(panel, required)
    assert len(frame) >= 50
    for period, expected in frame.iterrows():
        row = feature_row(panel, period, required)
        assert {k: row[k] for k in required} == pytest.approx(expected[required].to_dict())


def test_feature_row_reports_missing_inputs(make_series):
    icp = make_series("2020-01", [1.0] * 6, "icp_price")
    wti = make_series("2020-01", [1.0] * 6, "wti_price")
    panel = build_panel(icp, {"wti": wti})
    with pytest.raises(MissingFeatureData) as exc:
        feature_row(panel, pd.Period("2020-07", "M"), ["wti_price", "icp_lag_1"])
    assert exc.value.missing == ["wti_price"]  # icp_lag_1 (2020-06) is present


def test_chronological_split_keeps_order(synthetic_panel):
    frame = build_feature_frame(synthetic_panel)
    train, test = chronological_split(frame, 0.2)
    assert train.index.max() < test.index.min()
    assert len(test) == round(len(frame) * 0.2)
    with pytest.raises(ValueError):
        chronological_split(frame.iloc[:2], 0.5)
