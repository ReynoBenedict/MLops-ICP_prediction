"""Deterministic insight rules. Each rule: (InsightContext) -> Finding | None. Numbers only.

Every rule documents its definition, minimum data, threshold and when it is NOT generated.
Importance reflects the consequence for reading the data or the forecast, never the size of a
price move alone. No rule asserts a cause."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from icp_api.insights.context import BENCHMARKS, InsightContext, step_evaluation
from icp_api.insights.evidence import DatasetRef, Finding, Importance, Loc, Parts, Value, Word

BENCH_NAME = {"brent": "Brent", "dubai": "Dubai", "wti": "WTI"}

# thresholds (see docs/FORECASTING_METHODOLOGY.md "Insight rules")
TREND_MIN_MOVES = 3  # consecutive same-direction monthly changes
EXTREME_WINDOW = 24  # months searched for the largest move
EXTREME_MIN_MOVES = 12
SPREAD_WINDOW = 36  # comparison months for the ICP-benchmark spread
SPREAD_MIN_PAIRS = 24
REVISION_FRACTION = 0.5  # of the backtest MAE for the step
DIRECTION_WEAK = 0.6  # direction hit rate below this is described as close to chance
CONFIRMED = frozenset({"two_documents", "decree_text"})


def _ref(ctx: InsightContext, *datasets: str) -> list[DatasetRef]:
    return [
        DatasetRef(ds, (ctx.versions.get(ds) or {}).get("id"), (ctx.versions.get(ds) or {}).get("period_end"))
        for ds in datasets
        if ds in ctx.versions
    ]


def _consecutive_moves(s: pd.Series) -> list[tuple[pd.Period, float]]:
    """(period, change vs previous month) for months whose previous month is present."""
    idx = set(s.index)
    return [(p, float(s[p] - s[p - 1])) for p in s.index if (p - 1) in idx]


def _dir(x: float) -> int:
    return 1 if x > 0 else -1 if x < 0 else 0


def _dir_word(d: int) -> Word:
    return Word("up" if d > 0 else "down" if d < 0 else "flat")


# -- 1. month-over-month ---------------------------------------------------------------------------
def icp_mom(ctx: InsightContext) -> Finding | None:
    """Official ICP M vs M-1. Needs both months. Not generated when M-1 is missing. Importance low."""
    m = ctx.last
    if m is None or (m - 1) not in ctx.icp.index:
        return None
    cur, prev = float(ctx.icp[m]), float(ctx.icp[m - 1])
    chg = cur - prev
    pct = chg / prev * 100 if prev else 0.0
    d = _dir(chg)
    v_latest = Value("icp_latest", cur, "usd", period=str(m))
    v_prev = Value("icp_previous", prev, "usd", period=str(m - 1))
    v_chg = Value("change_usd", chg, "usd_signed")
    v_pct = Value("change_pct", pct, "pct_signed")
    return Finding(
        "icp_mom",
        1,
        "material_change",
        "low",
        str(m),
        "icp_mom",
        {
            "period": Value("p", str(m), "period"),
            "prev_period": Value("p", str(m - 1), "period"),
            "dir": _dir_word(d),
            "pct_abs": Value("x", abs(pct), "pct"),
            "pct": v_pct,
            "chg_abs": Value("x", abs(chg), "usd"),
            "latest": v_latest,
            "previous": v_prev,
        },
        [v_latest, v_prev, v_pct],
        [v_latest, v_prev],
        [v_chg, v_pct],
        _ref(ctx, "icp"),
        "mom",
        {"subject": str(m), "window_start": str(m - 1), "window_end": str(m), "n": 2},
        direction=d,
    )


# -- 2. year-over-year -----------------------------------------------------------------------------
def icp_yoy(ctx: InsightContext) -> Finding | None:
    """Official ICP M vs M-12. Not generated when M-12 is missing. Importance low."""
    m = ctx.last
    if m is None or (m - 12) not in ctx.icp.index:
        return None
    cur, old = float(ctx.icp[m]), float(ctx.icp[m - 12])
    pct = (cur - old) / old * 100 if old else 0.0
    v_latest = Value("icp_latest", cur, "usd", period=str(m))
    v_old = Value("icp_year_ago", old, "usd", period=str(m - 12))
    v_pct = Value("change_pct", pct, "pct_signed")
    return Finding(
        "icp_yoy",
        1,
        "annual_comparison",
        "low",
        str(m),
        "icp_yoy",
        {
            "period": Value("p", str(m), "period"),
            "yoy_period": Value("p", str(m - 12), "period"),
            "higher": Word("higher" if pct >= 0 else "lower"),
            "pct_abs": Value("x", abs(pct), "pct"),
            "pct": v_pct,
            "latest": v_latest,
            "yoy": v_old,
        },
        [v_old, v_pct],
        [v_latest, v_old],
        [v_pct],
        _ref(ctx, "icp"),
        "yoy",
        {"subject": str(m), "window_start": str(m - 12), "window_end": str(m), "n": 2},
    )


# -- 3. trend run ----------------------------------------------------------------------------------
def icp_trend_run(ctx: InsightContext) -> Finding | None:
    """Consecutive same-direction monthly changes ending at M (no gaps). Fires at >= 3 moves
    (needs >= 4 consecutive months). Not generated across missing months or after a flat month.
    Importance low."""
    m = ctx.last
    if m is None:
        return None
    s = ctx.icp
    run, d, p = 0, 0, m
    while (p - 1) in s.index:
        step = _dir(float(s[p] - s[p - 1]))
        if step == 0 or (d and step != d):
            break
        d, run, p = step, run + 1, p - 1
    if run < TREND_MIN_MOVES:
        return None
    start = p
    cur, first = float(s[m]), float(s[start])
    pct = (cur - first) / first * 100 if first else 0.0
    v_run = Value("run_months", run, "int")
    v_start = Value("run_start", first, "usd", period=str(start))
    v_latest = Value("icp_latest", cur, "usd", period=str(m))
    v_pct = Value("change_pct", pct, "pct_signed")
    return Finding(
        "icp_trend_run",
        1,
        "trend",
        "low",
        str(m),
        "icp_trend_run",
        {
            "dir": _dir_word(d),
            "n": run,
            "start": v_start,
            "start_period": Value("p", str(start), "period"),
            "latest": v_latest,
            "period": Value("p", str(m), "period"),
            "pct": v_pct,
        },
        [v_run, v_start, v_pct],
        [v_start, v_latest],
        [v_run, v_pct],
        _ref(ctx, "icp"),
        "trend_run",
        {"subject": str(m), "window_start": str(start), "window_end": str(m), "n": run + 1},
        direction=d,
    )


# -- 4. largest move -------------------------------------------------------------------------------
def icp_extreme_move(ctx: InsightContext) -> Finding | None:
    """The M move is the largest absolute consecutive monthly move in the last 24 months.
    Needs >= 12 consecutive moves in the window and a non-zero move. Not generated otherwise.
    Importance medium: forecast ranges built from historical errors can understate risk."""
    m = ctx.last
    if m is None or (m - 1) not in ctx.icp.index:
        return None
    window = ctx.icp[ctx.icp.index > m - EXTREME_WINDOW]
    moves = _consecutive_moves(window)
    if len(moves) < EXTREME_MIN_MOVES:
        return None
    latest = next((v for p, v in moves if p == m), None)
    others = [(p, v) for p, v in moves if p != m]
    if latest is None or latest == 0 or not others:
        return None
    prior_p, prior_v = max(others, key=lambda t: abs(t[1]))
    if abs(latest) <= abs(prior_v):
        return None
    cur, prev = float(ctx.icp[m]), float(ctx.icp[m - 1])
    pct = latest / prev * 100 if prev else 0.0
    d = _dir(latest)
    first_move_period = min(p for p, _ in moves)
    v_latest = Value("icp_latest", cur, "usd", period=str(m))
    v_prev = Value("icp_previous", prev, "usd", period=str(m - 1))
    v_chg = Value("change_usd", latest, "usd_signed")
    v_prior = Value("prior_largest", prior_v, "usd_signed", period=str(prior_p))
    v_n = Value("moves_compared", len(moves), "int")
    return Finding(
        "icp_extreme_move",
        1,
        "unusual_movement",
        "medium",
        str(m),
        "icp_extreme_move",
        {
            "period": Value("p", str(m), "period"),
            "prev_period": Value("p", str(m - 1), "period"),
            "months": EXTREME_WINDOW,
            "dir": _dir_word(d),
            "chg_abs": Value("x", abs(latest), "usd"),
            "pct": Value("x", pct, "pct_signed"),
            "latest": v_latest,
            "previous": v_prev,
            "n": len(moves),
            "window_start": Value("p", str(first_move_period), "period"),
            "prior_abs": Value("x", abs(prior_v), "usd"),
            "prior_period": Value("p", str(prior_p), "period"),
        },
        [v_latest, v_chg, v_prior],
        [v_latest, v_prev],
        [v_chg, v_prior, v_n],
        _ref(ctx, "icp"),
        "extreme_move",
        {
            "subject": str(m),
            "window_start": str(first_move_period - 1),
            "window_end": str(m),
            "n": len(moves),
        },
        body_parts=[("icp_extreme_move.caveat", {})],
        direction=d,
    )


# -- 5. spread vs its own history ------------------------------------------------------------------
def spread_vs_history(ctx: InsightContext) -> Finding | None:
    """ICP minus benchmark at M compared with the min/max of the previous 36 months (>= 24 paired
    months). Fires only when the spread is outside that range. Not generated when no benchmark
    qualifies. Importance medium if a flagged benchmark is used by the production method, else low."""
    m = ctx.last
    if m is None:
        return None
    facts_model = ctx.eval_for(ctx.production_version)
    used = set((facts_model or {}).get("benchmarks_used") or [])
    hits: list[dict[str, Any]] = []
    for b in BENCHMARKS:
        bs = ctx.benchmarks.get(b)
        if bs is None or m not in bs.index:
            continue
        common = ctx.icp.index.intersection(bs.index)
        spread = (ctx.icp[common] - bs[common]).sort_index()
        hist = spread[(spread.index < m) & (spread.index >= m - SPREAD_WINDOW)]
        if len(hist) < SPREAD_MIN_PAIRS:
            continue
        cur = float(spread[m])
        lo, hi, med = float(hist.min()), float(hist.max()), float(hist.median())
        if lo <= cur <= hi:
            continue
        hits.append(
            {
                "b": b,
                "spread": cur,
                "low": lo,
                "high": hi,
                "median": med,
                "n": len(hist),
                "side": "above" if cur > hi else "below",
                "bench": float(bs[m]),
                "start": str(hist.index.min()),
                "end": str(hist.index.max()),
            }
        )
    if not hits:
        return None
    importance: Importance = "medium" if any(h["b"] in used for h in hits) else "low"
    lines: list[tuple[str, dict[str, Any]]] = []
    facts: list[Value] = []
    outputs: list[Value] = []
    inputs: list[Value] = [Value("icp_latest", float(ctx.icp[m]), "usd", period=str(m))]
    for h in hits:
        name = BENCH_NAME[h["b"]]
        slots = {"b": name}
        v_spread = Value("spread", h["spread"], "usd_signed", label_slots=slots)
        lines.append(
            (
                "spread_vs_history.line",
                {
                    "b": name,
                    "spread": v_spread,
                    "side": Word(h["side"]),
                    "n": h["n"],
                    "low": Value("x", h["low"], "usd_signed"),
                    "high": Value("x", h["high"], "usd_signed"),
                    "median": Value("x", h["median"], "usd_signed"),
                },
            )
        )
        facts.append(v_spread)
        inputs.append(Value("benchmark", h["bench"], "usd", period=str(m), label_slots=slots))
        outputs += [
            v_spread,
            Value("window_low", h["low"], "usd_signed", label_slots=slots),
            Value("window_high", h["high"], "usd_signed", label_slots=slots),
            Value("window_median", h["median"], "usd_signed", label_slots=slots),
        ]
        if h["b"] in used and facts_model:
            key = (
                "spread_vs_history.model_spread"
                if facts_model.get("method_family") == "spread_rule"
                else "spread_vs_history.model_input"
            )
            lines.append((key, {"b": name, "method": str(facts_model.get("method") or "-")}))
    outputs.append(Value("window_n", hits[0]["n"], "int"))
    names = Parts(tuple(("_name", {"b": BENCH_NAME[h["b"]]}) for h in hits), ", ")
    return Finding(
        "spread_vs_history",
        2,
        "benchmark_divergence",
        importance,
        str(m),
        "spread_vs_history",
        {"names": names, "months": SPREAD_WINDOW, "period": Value("p", str(m), "period")},
        facts,
        inputs,
        outputs,
        _ref(ctx, "icp", *[h["b"] for h in hits]),
        "spread_range",
        {
            "subject": str(m),
            "window_start": hits[0]["start"],
            "window_end": hits[0]["end"],
            "n": hits[0]["n"],
        },
        model={"version": str(facts_model["version"]), "method": str(facts_model.get("method"))}
        if facts_model and importance == "medium"
        else None,
        body_parts=[(k, {**s, "period": Value("p", str(m), "period")}) for k, s in lines],
    )


# -- 6. FX context ---------------------------------------------------------------------------------
def fx_context(ctx: InsightContext) -> Finding | None:
    """ICP x average JISDOR for M and M-1. Not generated without ICP and FX for both months.
    Importance low."""
    m = ctx.last
    fx = ctx.fx
    if (
        m is None
        or fx is None
        or m not in fx.index
        or (m - 1) not in fx.index
        or (m - 1) not in ctx.icp.index
    ):
        return None
    icp_now, icp_prev = float(ctx.icp[m]), float(ctx.icp[m - 1])
    fx_now, fx_prev = float(fx[m]), float(fx[m - 1])
    idr_now, idr_prev = icp_now * fx_now, icp_prev * fx_prev
    usd_pct = (icp_now - icp_prev) / icp_prev * 100
    fx_pct = (fx_now - fx_prev) / fx_prev * 100
    idr_pct = (idr_now - idr_prev) / idr_prev * 100
    v_idr = Value("icp_idr", idr_now, "idr", period=str(m))
    v_fx = Value("fx", fx_now, "rate", period=str(m))
    v_fxp = Value("fx_change_pct", fx_pct, "pct_signed")
    v_idrp = Value("idr_change_pct", idr_pct, "pct_signed")
    v_usdp = Value("usd_change_pct", usd_pct, "pct_signed")
    return Finding(
        "fx_context",
        1,
        "fx_context",
        "low",
        str(m),
        "fx_context",
        {
            "period": Value("p", str(m), "period"),
            "idr": v_idr,
            "idr_pct": v_idrp,
            "usd_pct": v_usdp,
            "fx_pct": v_fxp,
            "fx": v_fx,
        },
        [v_idr, v_fx, v_fxp],
        [
            Value("icp_latest", icp_now, "usd", period=str(m)),
            Value("icp_previous", icp_prev, "usd", period=str(m - 1)),
            v_fx,
            Value("fx_previous", fx_prev, "rate", period=str(m - 1)),
        ],
        [v_idr, Value("icp_idr_previous", idr_prev, "idr", period=str(m - 1)), v_usdp, v_fxp, v_idrp],
        _ref(ctx, "icp", "usd_idr"),
        "fx",
        {"subject": str(m), "window_start": str(m - 1), "window_end": str(m), "n": 2},
    )


# -- 7. forecast outlook ---------------------------------------------------------------------------
def forecast_outlook(ctx: InsightContext) -> Finding | None:
    """First point of the latest system forecast with its measured backtest skill (same mode and
    step): MAE, interval coverage and direction hit rate. Not generated when there is no system
    forecast, it is anchored on an older month than the latest official ICP, or the model version
    has no stored metrics for that step. Importance medium."""
    m = ctx.last
    fc = ctx.latest_fc
    if m is None or not fc or not fc.get("result") or fc.get("forecast_origin") != str(m):
        return None
    points = fc["result"].get("points") or []
    if not points:
        return None
    p = points[0]
    ev = step_evaluation(
        ctx.eval_for(fc.get("model_version")), p.get("interval_basis"), int(p.get("step", 1))
    )
    if ev is None or ev.get("mae") is None:
        return None
    pred = float(p["prediction"])
    iv = (p.get("intervals") or {}).get("0.8")
    if not iv:
        return None
    lo, hi = float(iv[0]), float(iv[1])
    origin_val = float(ctx.icp[m])
    rel = "rel_above" if pred > origin_val else "rel_below" if pred < origin_val else "rel_equal"
    v_pred = Value("prediction", pred, "usd", period=p["period"])
    v_lo = Value("interval_low", lo, "usd")
    v_hi = Value("interval_high", hi, "usd")
    v_mae = Value("backtest_mae", float(ev["mae"]), "usd")
    v_n = Value("backtest_n", ev["n"], "int")
    facts = [v_pred, v_lo, v_hi, v_mae]
    outputs = [v_pred, v_lo, v_hi, v_mae, v_n]
    parts: list[tuple[str, dict[str, Any]]] = [
        (
            "forecast_outlook.hold_last"
            if p.get("interval_basis") == "hold_last"
            else "forecast_outlook.observed",
            {},
        )
    ]
    cov = (ev.get("coverage") or {}).get("0.8")
    if cov is not None and ev.get("coverage_n"):
        v_cov = Value("coverage_80", cov, "share")
        facts.append(v_cov)
        outputs.append(v_cov)
        parts.append(
            ("forecast_outlook.skill", {"n": ev["n"], "mae": v_mae, "cov": v_cov, "cov_n": ev["coverage_n"]})
        )
    else:
        parts.append(("forecast_outlook.skill_nocov", {"n": ev["n"], "mae": v_mae}))
    if ev.get("dir_acc") is not None:
        v_dir = Value("direction_accuracy", float(ev["dir_acc"]), "share")
        facts.append(v_dir)
        outputs.append(v_dir)
        parts.append(("forecast_outlook.direction", {"rel": Word(rel), "dir": v_dir}))
        if float(ev["dir_acc"]) < DIRECTION_WEAK:
            parts.append(("forecast_outlook.direction_weak", {}))
    if lo <= origin_val <= hi:
        parts.append(("forecast_outlook.contains", {}))
    facts_model = ctx.eval_for(fc.get("model_version")) or {}
    return Finding(
        "forecast_outlook",
        1,
        "forecast_outlook",
        "medium",
        str(p["period"]),
        "forecast_outlook",
        {
            "period": Value("p", str(p["period"]), "period"),
            "pred": v_pred,
            "lo": v_lo,
            "hi": v_hi,
            "issue": Value("d", str(fc.get("issue_date") or ""), "date"),
            "origin": Value("p", str(m), "period"),
            "latest": Value("icp_latest", origin_val, "usd"),
        },
        facts,
        [Value("icp_latest", origin_val, "usd", period=str(m))],
        outputs,
        _ref(
            ctx,
            "icp",
            *[b for b in BENCHMARKS if (p.get("benchmarks") or {}).get(b, {}).get("used_by_model")],
        ),
        "outlook",
        {"subject": str(p["period"]), "window_start": str(m), "window_end": str(p["period"]), "n": ev["n"]},
        model={
            "version": str(fc.get("model_version")),
            "method": str(facts_model.get("method") or fc.get("method")),
        },
        forecast_ids=[str(fc["id"])],
        body_parts=parts,
    )


# -- 8. forecast revision --------------------------------------------------------------------------
def forecast_revision(ctx: InsightContext) -> Finding | None:
    """Largest change for a shared target month between the last two system forecasts, shown when
    abs(change) >= 0.5 x backtest MAE for that step and mode (scale-aware). Lists recorded
    differences (model version, origin, data versions, benchmark inputs); never a cause. Not
    generated without two forecasts sharing a month or without stored metrics. Importance medium."""
    cur, old = ctx.latest_fc, ctx.prev_fc
    if not cur or not old or not cur.get("result") or not old.get("result"):
        return None
    facts_model = ctx.eval_for(cur.get("model_version"))
    cp = {p["period"]: p for p in cur["result"].get("points") or []}
    op = {p["period"]: p for p in old["result"].get("points") or []}
    best: tuple[float, str, float] | None = None
    last = ctx.last
    for period in sorted(set(cp) & set(op)):
        if last is not None and pd.Period(period, freq="M") <= last:
            continue  # ESDM has published this month: the official value supersedes both forecasts
        d = float(cp[period]["prediction"]) - float(op[period]["prediction"])
        ev = step_evaluation(facts_model, cp[period].get("interval_basis"), int(cp[period].get("step", 1)))
        if ev is None or ev.get("mae") is None:
            continue
        thr = REVISION_FRACTION * float(ev["mae"])
        if abs(d) >= thr and (best is None or abs(d) > abs(best[0])):
            best = (d, period, thr)
    if best is None:
        return None
    d, period, thr = best
    items: list[tuple[str, dict[str, Any]]] = []
    if str(cur.get("model_version")) != str(old.get("model_version")):
        items.append(
            (
                "forecast_revision.item_model",
                {"a": str(old.get("model_version")), "b": str(cur.get("model_version"))},
            )
        )
    if cur.get("forecast_origin") != old.get("forecast_origin"):
        items.append(
            (
                "forecast_revision.item_origin",
                {
                    "a": Value("p", str(old.get("forecast_origin")), "period"),
                    "b": Value("p", str(cur.get("forecast_origin")), "period"),
                },
            )
        )
    dv_cur, dv_old = cur.get("dataset_versions") or {}, old.get("dataset_versions") or {}
    changed_ds = sorted(ds for ds in set(dv_cur) | set(dv_old) if dv_cur.get(ds) != dv_old.get(ds))
    if changed_ds:
        label = {"icp": "ICP", "usd_idr": "USD/IDR", **BENCH_NAME}
        items.append(
            ("forecast_revision.item_data", {"names": ", ".join(label.get(x, x) for x in changed_ds)})
        )
    for b in BENCHMARKS:
        a = (op[period].get("benchmarks") or {}).get(b) or {}
        c = (cp[period].get("benchmarks") or {}).get(b) or {}
        if c.get("used_by_model") and a.get("value") != c.get("value"):
            items.append(
                (
                    "forecast_revision.item_input",
                    {
                        "b": BENCH_NAME[b],
                        "a": Value("x", a.get("value"), "usd"),
                        "c": Value("x", c.get("value"), "usd"),
                    },
                )
            )
    parts = (
        [("forecast_revision.diffs", {"items": Parts(tuple(items))})]
        if items
        else [("forecast_revision.nodiff", {})]
    )
    v_new = Value("new_prediction", float(cp[period]["prediction"]), "usd", period=period)
    v_old = Value("old_prediction", float(op[period]["prediction"]), "usd", period=period)
    v_d = Value("revision", d, "usd_signed")
    v_thr = Value("threshold", thr, "usd")
    return Finding(
        "forecast_revision",
        1,
        "forecast_change",
        "medium",
        period,
        "forecast_revision",
        {
            "period": Value("p", period, "period"),
            "d_abs": Value("x", abs(d), "usd"),
            "d": v_d,
            "new": v_new,
            "old": v_old,
            "new_issue": Value("d", str(cur.get("issue_date") or ""), "date"),
            "old_issue": Value("d", str(old.get("issue_date") or ""), "date"),
            "thr": v_thr,
        },
        [v_new, v_old, v_d],
        [v_new, v_old],
        [v_d, v_thr],
        _ref(ctx, "icp"),
        "revision",
        {
            "subject": period,
            "window_start": str(old.get("forecast_origin") or cur.get("forecast_origin") or period),
            "window_end": period,
            "n": 2,
        },
        model={"version": str(cur.get("model_version")), "method": str(cur.get("method") or "")},
        forecast_ids=[str(cur["id"]), str(old["id"])],
        body_parts=parts,
        direction=_dir(d),
    )


# -- 9. freshness ----------------------------------------------------------------------------------
def data_freshness(ctx: InsightContext) -> Finding | None:
    """ICP behind its publication schedule (lag 1 month, 1 month grace). lagging -> low (normal in
    the first days of a month), stale -> high. Not generated when fresh or unknown."""
    fr = ctx.freshness.get("icp") or {}
    status = fr.get("status")
    if status not in ("lagging", "stale"):
        return None
    latest, expected = fr.get("latest_period"), fr.get("expected_period")
    v_latest = Value("latest_period", latest, "period")
    v_exp = Value("expected_period", expected, "period")
    v_behind = Value("months_behind", int(fr.get("months_behind") or 0), "int")
    return Finding(
        "data_freshness",
        1,
        "freshness",
        "high" if status == "stale" else "low",
        latest,
        "data_freshness",
        {
            "latest_period": v_latest,
            "expected": v_exp,
            "today": Value("d", ctx.today.isoformat(), "date"),
        },
        [v_latest, v_exp, v_behind],
        [v_latest],
        [v_exp, v_behind],
        _ref(ctx, "icp"),
        "freshness",
        {"subject": latest, "window_start": latest, "window_end": expected, "n": None},
        body_parts=[(f"data_freshness.{status}", {})],
    )


def benchmark_freshness(ctx: InsightContext) -> Finding | None:
    """Benchmarks/FX more than one month behind schedule (status stale). Importance medium.
    Not generated when every benchmark is fresh or only lagging."""
    stale = [ds for ds in (*BENCHMARKS, "usd_idr") if (ctx.freshness.get(ds) or {}).get("status") == "stale"]
    if not stale:
        return None
    label = {**BENCH_NAME, "usd_idr": "USD/IDR (JISDOR)"}
    items = Parts(
        tuple(
            (
                "_pair",
                {"a": label[ds], "b": Value("p", (ctx.freshness[ds] or {}).get("latest_period"), "period")},
            )
            for ds in stale
        ),
        ", ",
    )
    facts = [Value("latest_period", (ctx.freshness[ds] or {}).get("latest_period"), "period") for ds in stale]
    latest = sorted(str((ctx.freshness[ds] or {}).get("latest_period") or "") for ds in stale)
    expected = sorted(str((ctx.freshness[ds] or {}).get("expected_period") or "") for ds in stale)
    behind = [
        Value("months_behind", int((ctx.freshness[ds] or {}).get("months_behind") or 0), "int")
        for ds in stale
    ]
    return Finding(
        "benchmark_freshness",
        1,
        "freshness",
        "medium",
        None,
        "benchmark_freshness",
        {"names": ", ".join(label[ds] for ds in stale), "items": items},
        facts[:1],
        facts,
        behind,
        _ref(ctx, *stale),
        "freshness",
        {
            "subject": None,
            "window_start": latest[0] or None,
            "window_end": expected[-1] or latest[-1] or None,
            "n": len(stale),
        },
        body_parts=[],
    )


# -- 10. verification ------------------------------------------------------------------------------
def data_verification(ctx: InsightContext) -> Finding | None:
    """Active ICP months not confirmed by two ESDM documents or decree text. Not generated when
    every month is confirmed. Importance medium if a conflict / OCR-only / no-evidence month is in
    the last 12 months, else low."""
    months = ctx.verification
    if not months:
        return None
    pending = [x for x in months if x["basis"] not in CONFIRMED]
    if not pending:
        return None
    last = ctx.last
    recent_bad = any(
        x["basis"] in ("conflict", "ocr_single", "no_evidence")
        and last is not None
        and pd.Period(x["period"], freq="M") > last - 12
        for x in pending
    )
    counts: dict[str, int] = {}
    labels: dict[str, Loc] = {}
    for x in pending:
        counts[x["basis"]] = counts.get(x["basis"], 0) + 1
        labels[x["basis"]] = Loc(x["label"]["id"], x["label"]["en"])
    order = sorted(counts, key=lambda k: -counts[k])
    items = Parts(tuple(("_count", {"label": labels[k], "n": counts[k]}) for k in order), "; ")
    recent = [
        Value("p", x["period"], "period")
        for x in sorted(pending, key=lambda x: x["period"], reverse=True)[:6]
    ]
    v_total = Value("months_total", len(months), "int")
    v_pending = Value("months_unconfirmed", len(pending), "int")
    breakdown = [Value("basis_count", counts[k], "int", label_slots={"label": labels[k]}) for k in order]
    return Finding(
        "data_verification",
        1,
        "data_quality",
        "medium" if recent_bad else "low",
        None,
        "data_verification",
        {"n": len(pending), "total": len(months), "items": items, "months": recent},
        [v_pending, v_total],
        [v_total],
        [v_pending, *breakdown],
        _ref(ctx, "icp"),
        "verification",
        {
            "subject": None,
            "window_start": months[0]["period"],
            "window_end": months[-1]["period"],
            "n": len(months),
        },
    )


# -- 11. model evaluation --------------------------------------------------------------------------
def model_evaluation(ctx: InsightContext) -> Finding | None:
    """Production model's 1-month-ahead MAE when benchmarks are held (the situation of every
    future month without a scenario) vs repeating last month's ICP. Not generated without these
    stored metrics. Importance low."""
    f = ctx.eval_for(ctx.production_version)
    if not f or f.get("hold_last_h1_mae") is None or f.get("naive_hold_last_h1_mae") is None:
        return None
    ev = step_evaluation(f, "hold_last", 1)
    mae, naive = float(f["hold_last_h1_mae"]), float(f["naive_hold_last_h1_mae"])
    if ev and ev.get("baseline") and ev.get("mae") is not None:
        # like for like: the registry's naive figure covers only the cases common to every
        # candidate method (66), the model's MAE its own cases (69); prefer both on the same cases
        mae, naive = float(ev["mae"]), float(ev["baseline"]["mae"])
    n = ev["n"] if ev else None
    v_mae = Value("backtest_mae", mae, "usd")
    v_naive = Value("naive_mae", naive, "usd")
    origins = str(f.get("backtest_origins") or "")
    o_start, o_end = origins.split("..", 1) if ".." in origins else (None, None)
    # evidence: the training data versions the backtest ran on (from the model registry)
    trained = [
        DatasetRef(ds, (meta or {}).get("version_id"), None)
        for ds, meta in sorted((f.get("datasets") or {}).items())
    ]
    parts: list[tuple[str, dict[str, Any]]] = (
        [("model_evaluation.no_edge", {})] if mae >= naive * 0.98 else []
    )
    return Finding(
        "model_evaluation",
        1,
        "model_evaluation",
        "low",
        None,
        "model_evaluation",
        {
            "version": str(f["version"]),
            "method": str(f.get("method") or "-"),
            "mae": v_mae,
            "naive": v_naive,
            "n": n if n is not None else "-",
            "origins": str(f.get("backtest_origins") or "-"),
        },
        [v_mae, v_naive],
        [v_mae, v_naive],
        [v_mae, v_naive, Value("backtest_n", n, "int")],
        trained,
        "model_eval",
        {"subject": None, "window_start": o_start or None, "window_end": o_end or None, "n": n},
        model={"version": str(f["version"]), "method": str(f.get("method") or "")},
        body_parts=parts,
    )


# -- 12. ingestion failure (operators) -------------------------------------------------------------
def ingestion_failures(ctx: InsightContext) -> list[Finding]:
    """Latest run per dataset failed -> one operator-only finding per dataset (no raw error text).
    Importance high."""
    out = []
    for r in ctx.failed_runs:
        ds = str(r.get("dataset"))
        f = Finding(
            "ingestion_failure",
            1,
            "ingestion",
            "high",
            None,
            "ingestion_failure",
            {
                "label": {**BENCH_NAME, "icp": "ICP", "usd_idr": "USD/IDR"}.get(ds, ds),
                "run_id": str(r.get("id")),
                "started": str(r.get("started_at") or "")[:16].replace("T", " "),
            },
            [Value("run_id", str(r.get("id")), "text")],
            [Value("run_id", str(r.get("id")), "text")],
            [Value("started_at", str(r.get("started_at") or "")[:16].replace("T", " "), "text")],
            _ref(ctx, ds)
            or [DatasetRef(ds, None, None)],  # no active version yet: the run id is the evidence
            "ingestion",
            {
                "subject": None,
                "window_start": str(r.get("started_at") or "")[:7] or None,
                "window_end": str(r.get("finished_at") or r.get("started_at") or "")[:7] or None,
                "n": 1,
            },
            audience="operator",
        )
        f.key_suffix = ds
        out.append(f)
    return out


SINGLE_RULES: tuple[Callable[[InsightContext], Finding | None], ...] = (
    icp_mom,
    icp_yoy,
    icp_trend_run,
    icp_extreme_move,
    spread_vs_history,
    fx_context,
    forecast_outlook,
    forecast_revision,
    data_freshness,
    benchmark_freshness,
    data_verification,
    model_evaluation,
)


def run_all(ctx: InsightContext) -> list[Finding]:
    out = [f for rule in SINGLE_RULES if (f := rule(ctx)) is not None]
    return out + ingestion_failures(ctx)
