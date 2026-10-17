"""Live ICP forecasting with explicit semantics for every output month.

Vocabulary (see docs/FORECASTING_METHODOLOGY.md)
--------------------------------------------------
* **forecast origin** - the latest month with an *official* ICP value.
* **issue date** - the day the forecast was produced; the same target month forecast on two
  different days can legitimately differ (more benchmark data, newer model).
* **kind** of each target month:
  - ``estimate_awaiting_publication``: the month has ended but ESDM has not published its ICP
    yet. Benchmark averages for that month are observed, so this is a nowcast with the
    ``observed``-mode backtest error.
  - ``future_forecast``: the month has not ended (or has not started). Its benchmarks are not
    observable; they come from the user's scenario or are held at the last known value, and the
    output is explicitly conditional on that assumption (``hold_last``-mode error otherwise).
* every benchmark input is labelled ``observed`` / ``scenario`` / ``assumed_hold_last``.

Intervals are empirical backtest error quantiles for the matching mode and horizon (stored with
the model); they are *not* a parametric formula and they are absent when the model has none.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import pandas as pd

from icp_ml.backtest import forecast_path
from icp_ml.features import BENCHMARK_COLUMNS, FX_COLUMN, TARGET_COLUMN, MissingFeatureData
from icp_ml.methods import BENCH, MethodSpec
from icp_ml.sources.base import month_end

MAX_HORIZON = 6
BENCHMARK_BOUNDS = (0.0, 500.0)
FX_BOUNDS = (1000.0, 100000.0)

Scenario = dict[str, dict[str, float]]  # {"brent": {"2026-10": 95.0}, "wti": {...}, "usd_idr": {...}}


class ForecastInputError(ValueError):
    pass


@dataclass
class BenchmarkInput:
    value: float | None
    source: str  # observed | scenario | assumed_hold_last | unavailable
    used_by_model: bool


@dataclass
class ForecastPoint:
    period: str
    step: int
    kind: str  # estimate_awaiting_publication | future_forecast
    prediction: float
    intervals: dict[str, list[float]]  # level -> [lower, upper]
    interval_basis: str | None  # observed | hold_last | None
    conditional_on_scenario: bool
    benchmarks: dict[str, BenchmarkInput]
    recursive: bool
    prediction_idr: float | None
    fx_rate: float | None
    fx_source: str | None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["benchmarks"] = {k: asdict(v) for k, v in self.benchmarks.items()}
        return d


@dataclass
class Forecast:
    forecast_origin: str
    issue_date: str
    horizon: int
    method: str
    points: list[ForecastPoint]
    warnings: list[str] = field(default_factory=list)
    interval_levels: list[float] = field(default_factory=list)
    interval_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "points": [p.to_dict() for p in self.points]}


def _validate_scenario(
    scenario: Scenario | None, targets: list[pd.Period]
) -> dict[str, dict[pd.Period, float]]:
    out: dict[str, dict[pd.Period, float]] = {}
    keys = {str(t) for t in targets}
    for name, values in (scenario or {}).items():
        if name not in BENCH and name != FX_COLUMN:
            raise ForecastInputError(f"Unknown scenario input '{name}'")
        col = BENCH.get(name, FX_COLUMN)
        lo, hi = FX_BOUNDS if name == FX_COLUMN else BENCHMARK_BOUNDS
        for key, value in values.items():
            if key not in keys:
                raise ForecastInputError(
                    f"Scenario for {name} {key} is outside the forecast window {targets[0]}..{targets[-1]}"
                )
            if not (isinstance(value, int | float) and math.isfinite(value) and lo < value <= hi):
                raise ForecastInputError(f"Scenario value for {name} {key} must be in ({lo:g}, {hi:g}]")
            out.setdefault(col, {})[pd.Period(key, freq="M")] = float(value)
    return out


def make_forecast(
    model: Any,
    spec: MethodSpec,
    panel: pd.DataFrame,
    horizon: int,
    scenario: Scenario | None = None,
    intervals: dict[str, Any] | None = None,
    today: date | None = None,
) -> Forecast:
    """Forecast ``horizon`` months after the last official ICP month in ``panel``.

    ``model`` is a fitted estimator over the full feature frame; ``spec`` says which inputs it
    needs. ``intervals`` is the model's stored ``{mode: {horizon: {level: [lo, hi]}}}`` table.
    """
    if not 1 <= horizon <= MAX_HORIZON:
        raise ForecastInputError(f"horizon must be between 1 and {MAX_HORIZON}")
    official = panel[TARGET_COLUMN].dropna()
    if official.empty:
        raise ForecastInputError("ICP history is empty")
    today = today or date.today()
    origin = official.index.max()
    targets = [origin + i for i in range(1, horizon + 1)]
    assumptions = _validate_scenario(scenario, targets)
    warnings: list[str] = []
    used = {BENCH[b] for b in spec.benchmarks_used}
    for col in assumptions:
        if col != FX_COLUMN and col not in used:
            warnings.append(f"Scenario for {col.removesuffix('_price')} is not used by method {spec.name}.")

    # Never look at anything after the forecast window, and never at ICP after the origin.
    work = panel[panel.index <= targets[-1]].copy()
    try:
        path = forecast_path(
            model,
            spec,
            work,
            origin,
            tuple(range(1, horizon + 1)),
            "observed",
            {k: v for k, v in assumptions.items() if k != FX_COLUMN},
        )
    except MissingFeatureData:
        raise
    fx_hist = panel[FX_COLUMN].dropna() if FX_COLUMN in panel else pd.Series(dtype=float)
    fx_assume = assumptions.get(FX_COLUMN, {})
    levels = (
        sorted({float(lv) for mode in (intervals or {}).values() for h in mode.values() for lv in h})
        if intervals
        else []
    )

    points: list[ForecastPoint] = []
    for row in path:
        target = pd.Period(row["target"], freq="M")
        if row.get("y_pred") is None or (isinstance(row.get("y_pred"), float) and math.isnan(row["y_pred"])):
            raise MissingFeatureData(target, [row.get("error", "unknown")])
        y = float(row["y_pred"])
        if not math.isfinite(y):
            raise ForecastInputError(f"Model produced a non-finite prediction for {target}")
        kind = "estimate_awaiting_publication" if today > month_end(target) else "future_forecast"
        bench_inputs = {
            b.removesuffix("_price"): BenchmarkInput(row["benchmarks"][b], row["sources"][b], b in used)
            for b in BENCHMARK_COLUMNS
        }
        used_sources = {v.source for k, v in bench_inputs.items() if v.used_by_model}
        conditional = "scenario" in used_sources
        basis = "observed" if used_sources <= {"observed", "scenario"} else "hold_last"
        if not used and spec.family == "naive":
            basis = "hold_last"
        step = int(row["horizon"])
        table = ((intervals or {}).get(basis) or {}).get(str(step)) or {}
        ivals = {
            str(lv): [round(y + float(lo), 4), round(y + float(hi), 4)] for lv, (lo, hi) in table.items()
        }
        if target in fx_assume:
            fx, fx_src = fx_assume[target], "scenario"
        elif target in fx_hist.index:
            fx, fx_src = float(fx_hist[target]), "observed"
        elif not fx_hist.empty:
            fx, fx_src = float(fx_hist.iloc[-1]), "assumed_hold_last"
        else:
            fx, fx_src = None, None
        points.append(
            ForecastPoint(
                period=str(target),
                step=step,
                kind=kind,
                prediction=round(y, 4),
                intervals=ivals,
                interval_basis=basis if ivals else None,
                conditional_on_scenario=conditional,
                benchmarks=bench_inputs,
                recursive=step > 1 and "icp_lag_1" in spec.requires,
                prediction_idr=round(y * fx, 0) if fx else None,
                fx_rate=fx,
                fx_source=fx_src,
            )
        )
    note = (
        "Intervals are empirical quantiles of rolling-origin backtest errors for the same horizon and input "
        "situation (observed benchmarks vs held-last benchmarks); scenario-based points are conditional on the scenario."
    )
    return Forecast(
        str(origin),
        today.isoformat(),
        horizon,
        spec.name,
        points,
        warnings,
        levels,
        note if intervals else "",
    )
