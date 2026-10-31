"""Immutable snapshot of everything the insight rules read (loaded once per request).

Rules are pure functions of this context, so they can be unit-tested with synthetic series."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd

BENCHMARKS = ("brent", "dubai", "wti")


@dataclass
class InsightContext:
    today: date
    icp: pd.Series  # PeriodIndex (freq M), sorted, no NaN
    benchmarks: dict[str, pd.Series] = field(default_factory=dict)
    fx: pd.Series | None = None
    versions: dict[str, dict[str, Any]] = field(default_factory=dict)  # dataset -> {id, period_end}
    freshness: dict[str, dict[str, Any]] = field(default_factory=dict)
    verification: list[dict[str, Any]] = field(default_factory=list)  # VerificationService.months("icp")
    latest_fc: dict[str, Any] | None = None
    prev_fc: dict[str, Any] | None = None
    evals: dict[str, dict[str, Any]] = field(default_factory=dict)  # model version -> evaluation facts
    production_version: str | None = None
    failed_runs: list[dict[str, Any]] = field(default_factory=list)  # latest run per dataset, failed

    @property
    def last(self) -> pd.Period | None:
        return self.icp.index.max() if len(self.icp) else None

    def eval_for(self, version: str | None) -> dict[str, Any] | None:
        return self.evals.get(str(version)) if version else None


def clean(series: pd.Series) -> pd.Series:
    s = series.dropna().sort_index()
    return s.astype(float)


def step_evaluation(facts: dict[str, Any] | None, mode: str | None, horizon: int) -> dict[str, Any] | None:
    """Backtest facts for one forecast step in one input situation (observed / hold_last)."""
    if not facts or mode not in ("observed", "hold_last"):
        return None
    row = next(
        (
            r
            for r in facts.get("by_horizon") or []
            if r.get("mode") == mode and int(r.get("horizon", 0)) == horizon
        ),
        None,
    )
    if row is None or not row.get("n"):
        return None
    cov = ((facts.get("coverage") or {}).get(mode) or {}).get(str(horizon)) or {}
    levels = {k: float(cov[k]) for k in ("0.5", "0.8", "0.95") if cov.get(k) is not None}
    coverage_n = int(cov["n"]) if cov.get("n") is not None else None
    # ML-01: coverage recomputed with the fit/test embargo from the stored backtest predictions.
    # The stored figure is kept as published; warnings use the stricter of the two per level.
    recomputed = facts.get("coverage_embargoed")
    ecov = ((recomputed or {}).get(mode) or {}).get(str(horizon)) or {}
    elevels = {k: float(ecov[k]) for k in ("0.5", "0.8", "0.95") if ecov.get(k) is not None}
    ecoverage_n = int(ecov["n"]) if ecov.get("n") is not None else None
    check: str | None = None
    if recomputed is not None and (elevels or not levels):
        same = levels.keys() == elevels.keys() and all(abs(levels[k] - elevels[k]) < 1e-9 for k in levels)
        check = "embargoed" if same else "without_embargo"
    effective = dict(levels)
    for k, share in elevels.items():
        effective[k] = min(share, effective.get(k, share))
    warn_n = coverage_n
    if elevels and any(effective[k] < levels.get(k, 2.0) for k in elevels):
        warn_n = ecoverage_n
    origin_start, origin_end = _origins(facts.get("backtest_origins"))
    base_row = ((facts.get("baselines") or {}).get(mode) or {}).get(str(horizon))
    baseline: dict[str, Any] | None = (
        {
            "method": "naive_last",
            "label": BASELINE_LABEL,
            "mae": float(base_row["mae"]),
            "n": int(base_row["n"]),
        }
        if base_row and base_row.get("mae") is not None
        else None
    )
    mae = row.get("mae")
    under = under_coverage_levels(effective)
    return {
        "mode": mode,
        "horizon": horizon,
        "n": int(row["n"]),
        "mae": mae,
        "mase": row.get("mase"),
        "dir_acc": row.get("dir_acc"),
        "coverage": levels or None,
        "coverage_n": coverage_n,
        "coverage_embargoed": elevels or None,
        "coverage_embargoed_n": ecoverage_n if elevels else None,
        "coverage_check": check,
        "origin_start": origin_start,
        "origin_end": origin_end,
        "baseline": baseline,
        "beats_baseline": (float(mae) < float(baseline["mae"])) if baseline and mae is not None else None,
        "coverage_period": facts.get("coverage_window") or None,
        "under_coverage": under,
        "coverage_warning": _coverage_warning(effective, under, warn_n),
        "summary": _summary(mode, horizon, int(row["n"]), mae, baseline, origin_start, origin_end),
    }


# --- evaluation disclosure texts ------------------------------------------------------------------
UNDER_COVERAGE_MARGIN = 0.10  # measured share this far below the nominal level is always flagged
# ML-02: for high nominal levels a 10-point margin is too lenient (a "95 %" range covering 86 % misses
# almost three times as often as promised), so a level is also flagged when the measured miss rate
# exceeds MISS_RATE_FACTOR x the nominal miss rate. Effective margins: 50 % -> 10 pts, 80 % -> 10 pts,
# 95 % -> 2.5 pts. With ~25 out-of-sample cases this flags borderline cases too; it is a disclosure,
# so erring towards a warning is intended.
MISS_RATE_FACTOR = 1.5


def under_coverage_margin(level: float) -> float:
    return round(min(UNDER_COVERAGE_MARGIN, (MISS_RATE_FACTOR - 1.0) * (1.0 - level)), 6)


def under_coverage_levels(levels: dict[str, float]) -> list[str]:
    """Levels whose measured coverage is materially below nominal (see ``MISS_RATE_FACTOR``)."""
    return [lv for lv, share in levels.items() if share < float(lv) - under_coverage_margin(float(lv))]


MIN_COVERAGE_N = 20  # fewer out-of-sample cases: coverage figures are flagged as rough
BASELINE_LABEL = {"id": "Ulang ICP bulan lalu", "en": "Repeat last month's ICP"}
MODE_TEXT = {
    "observed": {
        "id": "harga acuan bulan target sudah teramati (bulan sudah berakhir)",
        "en": "benchmark prices of the target month already observed (month has ended)",
    },
    "hold_last": {
        "id": "harga acuan diasumsikan bertahan pada nilai terakhir",
        "en": "benchmark prices held at their last known value",
    },
}


def _origins(raw: Any) -> tuple[str | None, str | None]:
    if not raw or ".." not in str(raw):
        return None, None
    a, b = str(raw).split("..", 1)
    return (a or None), (b or None)


def _num(v: float, lang: str) -> str:
    s = f"{v:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".") if lang == "id" else s


def _pct(v: float) -> str:
    return f"{round(v * 100):d}%"


def _summary(
    mode: str,
    horizon: int,
    n: int,
    mae: Any,
    baseline: dict[str, Any] | None,
    start: str | None,
    end: str | None,
) -> dict[str, str]:
    out = {}
    for lang in ("id", "en"):
        if lang == "id":
            period = f"titik awal {start} s.d. {end}" if start and end else "periode uji tidak tercatat"
            head = f"Uji mundur {n} kasus ({period}), {horizon} bulan ke depan, {MODE_TEXT[mode]['id']}"
            err = f": selisih absolut rata-rata USD {_num(float(mae), 'id')}" if mae is not None else ""
            base = (
                f", dibanding USD {_num(baseline['mae'], 'id')} bila ICP bulan lalu diulang pada kasus yang sama."
                if baseline
                else "; pembanding acuan sederhana tidak tersedia untuk versi model ini."
            )
        else:
            period = f"origins {start} to {end}" if start and end else "test period not recorded"
            months = "month" if horizon == 1 else "months"
            head = f"Backtest of {n} cases ({period}), {horizon} {months} ahead, {MODE_TEXT[mode]['en']}"
            err = f": mean absolute error USD {_num(float(mae), 'en')}" if mae is not None else ""
            base = (
                f", compared with USD {_num(baseline['mae'], 'en')} when last month's ICP is repeated on the same cases."
                if baseline
                else "; the simple baseline is not available for this model version."
            )
        out[lang] = head + err + base
    return out


def _coverage_warning(levels: dict[str, float], under: list[str], n: int | None) -> dict[str, str] | None:
    if under:
        id_parts = [
            f"rentang {_pct(float(lv))} hanya mencakup {_pct(levels[lv])} nilai resmi" for lv in under
        ]
        en_parts = [
            f"the {_pct(float(lv))} range contained only {_pct(levels[lv])} of official values"
            for lv in under
        ]
        return {
            "id": f"Pada uji mundur ({n} kasus), "
            + "; ".join(id_parts)
            + ". Rentang ini lebih sempit daripada ketidakpastian sebenarnya.",
            "en": f"In backtests ({n} cases), "
            + "; ".join(en_parts)
            + ". These ranges understate the real uncertainty.",
        }
    if levels and n is not None and n < MIN_COVERAGE_N:
        return {
            "id": f"Cakupan rentang hanya diuji pada {n} kasus; angka cakupan masih kasar.",
            "en": f"Range coverage was checked on only {n} cases; treat the coverage figures as rough.",
        }
    return None
