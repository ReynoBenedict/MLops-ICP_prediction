from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from icp_ml.features import FEATURE_COLUMNS, TARGET_COLUMN, MissingFeatureData, build_feature_frame
from icp_ml.forecasting import ForecastInputError, make_forecast
from icp_ml.methods import find_method

SPEC = find_method("spread_brent_6m")
INTERVALS = {
    "observed": {"1": {"0.8": [-1.0, 1.0]}, "2": {"0.8": [-1.5, 1.5]}},
    "hold_last": {"1": {"0.8": [-5.0, 5.0]}, "2": {"0.8": [-7.0, 7.0]}},
}


@pytest.fixture
def fitted(synthetic_panel):
    frame = build_feature_frame(synthetic_panel, SPEC.requires)
    return SPEC.build().fit(frame[FEATURE_COLUMNS], frame[TARGET_COLUMN])


@pytest.fixture
def panel(synthetic_panel):
    # Official ICP ends 2024-12; benchmarks run to 2025-02 (last complete month is in the past).
    p = synthetic_panel.copy()
    p.loc[p.index > pd.Period("2024-12", "M"), TARGET_COLUMN] = np.nan
    return p[p.index <= pd.Period("2025-02", "M")]


def test_kinds_sources_and_intervals(fitted, panel):
    fc = make_forecast(fitted, SPEC, panel, horizon=4, intervals=INTERVALS, today=date(2025, 3, 10))
    assert fc.forecast_origin == "2024-12" and fc.issue_date == "2025-03-10"
    assert [p.period for p in fc.points] == ["2025-01", "2025-02", "2025-03", "2025-04"]
    assert [p.kind for p in fc.points] == [
        "estimate_awaiting_publication",
        "estimate_awaiting_publication",
        "future_forecast",
        "future_forecast",
    ]
    assert [p.benchmarks["brent"].source for p in fc.points] == [
        "observed",
        "observed",
        "assumed_hold_last",
        "assumed_hold_last",
    ]
    assert [p.interval_basis for p in fc.points] == [
        "observed",
        "observed",
        None,
        None,
    ]  # no stored hold_last intervals for h3/h4
    assert fc.points[0].intervals["0.8"] == pytest.approx(
        [fc.points[0].prediction - 1, fc.points[0].prediction + 1]
    )
    assert fc.points[2].benchmarks["brent"].value == panel.loc[pd.Period("2024-12", "M"), "brent_price"]
    assert fc.points[0].prediction == pytest.approx(
        panel.loc[pd.Period("2025-01", "M"), "brent_price"] + fitted.spread_
    )
    assert all(
        p.benchmarks["brent"].used_by_model and not p.benchmarks["wti"].used_by_model for p in fc.points
    )
    assert fc.points[0].fx_source == "observed" and fc.points[3].fx_source == "assumed_hold_last"
    assert fc.points[0].prediction_idr == pytest.approx(
        fc.points[0].prediction * fc.points[0].fx_rate, rel=1e-6
    )
    assert not any(p.recursive for p in fc.points)  # the spread rule has no ICP lag


def test_scenario_overrides_and_validation(fitted, panel):
    fc = make_forecast(
        fitted,
        SPEC,
        panel,
        horizon=3,
        scenario={"brent": {"2025-03": 99.0}, "usd_idr": {"2025-03": 16000}, "wti": {"2025-03": 90.0}},
        intervals=INTERVALS,
        today=date(2025, 3, 10),
    )
    p3 = fc.points[2]
    assert p3.benchmarks["brent"].source == "scenario" and p3.prediction == pytest.approx(
        99.0 + fitted.spread_
    )
    assert p3.conditional_on_scenario and p3.interval_basis is None  # observed table has no h3 entry
    assert p3.fx_rate == 16000 and p3.fx_source == "scenario"
    assert any("wti" in w for w in fc.warnings)  # scenario input the method does not use
    with pytest.raises(ForecastInputError):
        make_forecast(fitted, SPEC, panel, horizon=2, scenario={"brent": {"2026-01": 50.0}})
    with pytest.raises(ForecastInputError):
        make_forecast(fitted, SPEC, panel, horizon=2, scenario={"brent": {"2025-01": -1.0}})
    with pytest.raises(ForecastInputError):
        make_forecast(fitted, SPEC, panel, horizon=2, scenario={"gold": {"2025-01": 1.0}})


def test_hold_last_basis_when_no_scenario(panel):
    spec = find_method("ridge_benchmarks")
    frame = build_feature_frame(panel, spec.requires)
    model = spec.build().fit(frame[FEATURE_COLUMNS], frame[TARGET_COLUMN])
    fc = make_forecast(model, spec, panel, horizon=3, intervals=INTERVALS, today=date(2025, 3, 10))
    assert [p.interval_basis for p in fc.points] == ["observed", "observed", None]
    assert fc.points[2].benchmarks["brent"].source == "assumed_hold_last"
    assert [p.recursive for p in fc.points] == [False, True, True]


def test_horizon_bounds_and_gaps(fitted, panel):
    for bad in (0, 7):
        with pytest.raises(ForecastInputError):
            make_forecast(fitted, SPEC, panel, horizon=bad)
    gap = panel.copy()
    gap.loc[pd.Period("2025-01", "M"), "brent_price"] = np.nan
    gap = gap[gap.index <= pd.Period("2025-01", "M")]
    # Brent for 2025-01 unknown -> held at last known (2024-12): forecast still possible but labelled.
    fc = make_forecast(fitted, SPEC, gap, horizon=1, today=date(2025, 2, 10))
    assert fc.points[0].benchmarks["brent"].source == "assumed_hold_last"
    spec = find_method("ridge_full")  # needs icp_lag_3 / icp_mean_3
    frame = build_feature_frame(panel, spec.requires)
    model = spec.build().fit(frame[FEATURE_COLUMNS], frame[TARGET_COLUMN])
    holed = panel.copy()
    holed.loc[pd.Period("2024-10", "M"), TARGET_COLUMN] = (
        np.nan
    )  # a gap inside the lag window is never bridged
    with pytest.raises(MissingFeatureData):
        make_forecast(model, spec, holed, horizon=1)


def test_forecast_is_reproducible_for_identical_inputs(fitted, panel):
    """Same model, data, scenario and issue date -> byte-identical result (no hidden randomness)."""
    kw = {
        "horizon": 3,
        "scenario": {"brent": {"2025-03": 90.0}},
        "intervals": INTERVALS,
        "today": date(2025, 3, 10),
    }
    first = make_forecast(fitted, SPEC, panel, **kw).to_dict()
    second = make_forecast(fitted, SPEC, panel, **kw).to_dict()
    assert first == second
    # A different issue date changes only the issue date and month kinds, never the numbers.
    later = make_forecast(fitted, SPEC, panel, **{**kw, "today": date(2025, 4, 10)}).to_dict()
    assert [p["prediction"] for p in later["points"]] == [p["prediction"] for p in first["points"]]
    assert (
        later["issue_date"] == "2025-04-10" and later["points"][2]["kind"] == "estimate_awaiting_publication"
    )
