"""Insight engine quality gates (Agent D, mission v6).

* every emitted insight carries complete evidence (validator drops + logs the rest);
* randomized series never produce contradictory directions, duplicate topics, causal or overclaiming
  wording, or NaN/inf values;
* boundary tests for every rule threshold not covered in test_insight_engine.py.
"""

from __future__ import annotations

import dataclasses
import logging
import math
import random
import re
from datetime import date
from typing import Any

import pandas as pd
import pytest

from icp_api.insights import build_insights, with_evidence
from icp_api.insights import rules as R
from icp_api.insights.context import (
    MIN_COVERAGE_N,
    UNDER_COVERAGE_MARGIN,
    InsightContext,
    step_evaluation,
)
from icp_api.insights.evidence import DatasetRef, Finding, Value
from icp_api.insights.validate import EvidenceError, validate_finding
from icp_api.services.verification import LEGEND

CAUSAL = {
    "id": ["karena", "disebabkan", "akibat", "dipicu", "didorong oleh", "sebab", "pengaruh dari"],
    "en": [
        "because",
        "due to",
        "caused",
        "driven by",
        "as a result of",
        "owing to",
        "triggered by",
        "led to",
    ],
}
OVERCLAIM = {
    "id": ["akurat", "pasti", "dijamin", "menjamin", "terverifikasi", "kalibr"],
    "en": ["accurate", "certain", "guarantee", "verified", "calibrat"],
}


def series(values: list[float], end: str = "2026-09") -> pd.Series:
    idx = pd.period_range(end=pd.Period(end, freq="M"), periods=len(values), freq="M")
    return pd.Series(values, index=idx, dtype=float)


EVAL: dict[str, Any] = {
    "version": "5",
    "method": "spread_brent_6m",
    "method_family": "spread_rule",
    "description": "x",
    "benchmarks_used": ["brent"],
    "by_horizon": [
        {"mode": m, "horizon": h, "n": 60, "mae": 3.0 * h, "mase": 1.0, "dir_acc": 0.55}
        for m in ("observed", "hold_last")
        for h in (1, 2, 3)
    ],
    "hold_last_h1_mae": 6.1,
    "naive_hold_last_h1_mae": 6.09,
    "coverage": {"hold_last": {"1": {"0.5": 0.5, "0.8": 0.8, "0.95": 0.9, "n": 28}}},
    "backtest_origins": "2020-12..2026-08",
    "datasets": {"icp": {"version_id": "dsv_train_icp", "sha256": "x"}},
}


def ctx(icp: pd.Series, **kw: Any) -> InsightContext:
    base: dict[str, Any] = {
        "today": date(2026, 10, 19),
        "versions": {
            ds: {"id": f"dsv_{ds}", "period_end": str(icp.index.max())}
            for ds in ("icp", "brent", "dubai", "wti", "usd_idr")
        },
    }
    base.update(kw)
    return InsightContext(icp=icp, **base)


def fc(fid: str, origin: str, preds: list[float], issue: str = "2026-10-17", **kw: Any) -> dict[str, Any]:
    start = pd.Period(origin, freq="M")
    pts = [
        {
            "period": str(start + i + 1),
            "step": i + 1,
            "kind": "future_forecast",
            "prediction": p,
            "intervals": {"0.8": [p - 10, p + 10]},
            "interval_basis": "hold_last",
            "benchmarks": {
                "brent": {"value": 100.0, "source": "assumed_hold_last", "used_by_model": True},
                "dubai": {"value": 90.0, "source": "assumed_hold_last", "used_by_model": False},
                "wti": {"value": 95.0, "source": "assumed_hold_last", "used_by_model": False},
            },
        }
        for i, p in enumerate(preds)
    ]
    return {
        "id": fid,
        "issue_date": issue,
        "forecast_origin": origin,
        "model_version": "5",
        "method": "spread_brent_6m",
        "dataset_versions": {"icp": "a1", "brent": "b"},
        "result": {"points": pts},
        **kw,
    }


# -- validator ------------------------------------------------------------------------------------
def _finding(**over: Any) -> Finding:
    v = Value("icp_latest", 80.0, "usd", period="2026-09")
    base: dict[str, Any] = dict(
        rule_id="icp_mom",
        rule_version=1,
        kind="material_change",
        importance="low",
        period="2026-09",
        template="icp_mom",
        slots={},
        facts=[v],
        inputs=[v],
        outputs=[v],
        datasets=[DatasetRef("icp", "dsv_1", "2026-09")],
        method="mom",
        window={"subject": "2026-09", "window_start": "2026-08", "window_end": "2026-09", "n": 2},
    )
    base.update(over)
    return Finding(**base)


@pytest.mark.parametrize(
    "over",
    [
        {"datasets": []},
        {"datasets": [DatasetRef("icp", None, "2026-09")]},
        {"window": {}},
        {"window": {"subject": "2026-09", "window_start": None, "window_end": "2026-09", "n": 2}},
        {"inputs": []},
        {"outputs": []},
        {"method": "no_such_method"},
        {"outputs": [Value("x", float("nan"), "usd")]},
        {"facts": [Value("x", float("inf"), "usd")]},
        {"inputs": [Value("x", float("-inf"), "pct")]},
    ],
    ids=lambda o: next(iter(o)),
)
def test_validator_rejects_incomplete_evidence(over):
    with pytest.raises(EvidenceError):
        validate_finding(_finding(**over))


def test_validator_accepts_complete_evidence():
    validate_finding(_finding())


def test_engine_drops_and_logs_insights_without_evidence(monkeypatch, caplog):
    c = ctx(series([60.0] + [70.0] * 11 + [71.0]))
    good, bad_src = R.icp_yoy(c), R.icp_mom(c)
    assert good is not None and bad_src is not None
    bad = dataclasses.replace(bad_src, datasets=[])
    monkeypatch.setattr("icp_api.insights.run_all", lambda _c: [bad, good])
    with caplog.at_level(logging.WARNING):
        out = build_insights(c, "id")
    assert [i["rule_id"] for i in out["insights"]] == ["icp_yoy"]
    assert any("icp_mom" in r.getMessage() and "evidence" in r.getMessage() for r in caplog.records)


# -- fuzz -----------------------------------------------------------------------------------------
def _random_ctx(seed: int) -> InsightContext:
    rnd = random.Random(seed)
    n = rnd.randint(1, 70)
    v = rnd.uniform(30, 120)
    vals = []
    for _ in range(n):
        r = rnd.random()
        if r < 0.15:
            step = 0.0  # flat month
        elif r < 0.25:
            step = rnd.choice([-1, 1]) * rnd.uniform(10, 40)  # shock
        else:
            step = rnd.gauss(0, 3)
        v = max(1.0, v + step)
        vals.append(round(v, rnd.choice([2, 4])))
    icp = series(vals)
    if n > 5 and rnd.random() < 0.3:  # gaps
        icp = icp.drop(rnd.sample(list(icp.index[:-1]), k=rnd.randint(1, max(1, n // 5))))
    benchmarks = {}
    for b in ("brent", "dubai", "wti"):
        if rnd.random() < 0.8:
            off = rnd.uniform(-8, 8)
            s = pd.Series([x - off + rnd.gauss(0, rnd.choice([0.0, 0.5, 4.0])) for x in icp], index=icp.index)
            if rnd.random() < 0.2:
                s = s.iloc[:-1]
            benchmarks[b] = s
    fx = pd.Series([rnd.uniform(14000, 17000) for _ in icp], index=icp.index) if rnd.random() < 0.7 else None
    last = icp.index.max()
    ev = dict(EVAL)
    ev["by_horizon"] = [
        {**r, "dir_acc": rnd.choice([None, 0.4, 0.6, 0.9]), "mae": rnd.choice([0.0, 0.5, 5.0])}
        for r in EVAL["by_horizon"]
    ]
    latest = fc("fc_a", str(last), [float(icp.iloc[-1]) + rnd.gauss(0, 8) for _ in range(3)])
    prev = fc(
        "fc_b",
        str(last - rnd.choice([0, 1])),
        [float(icp.iloc[-1]) + rnd.gauss(0, 8) for _ in range(3)],
        issue="2026-10-01",
    )
    statuses = ["fresh", "lagging", "stale", None]
    fresh = {
        ds: {
            "status": rnd.choice(statuses),
            "latest_period": str(last),
            "expected_period": str(last + 1),
            "months_behind": rnd.randint(0, 3),
        }
        for ds in ("icp", "brent", "dubai", "wti", "usd_idr")
    }
    bases = list(LEGEND)
    verification = [
        {"period": str(p), "basis": (b := rnd.choice(bases)), "label": LEGEND[b]["label"]} for p in icp.index
    ]
    failed = (
        [{"id": "ing_x", "dataset": "wti", "status": "failed", "started_at": "2026-10-18T01:00:00Z"}]
        if rnd.random() < 0.3
        else []
    )
    return ctx(
        icp,
        benchmarks=benchmarks,
        fx=fx,
        freshness=fresh,
        verification=verification,
        latest_fc=latest if rnd.random() < 0.8 else None,
        prev_fc=prev if rnd.random() < 0.7 else None,
        evals={"5": ev},
        production_version="5",
        failed_runs=failed,
    )


def _walk(obj: Any) -> list[Any]:
    if isinstance(obj, dict):
        return [x for v in obj.values() for x in _walk(v)]
    if isinstance(obj, list):
        return [x for v in obj for x in _walk(v)]
    return [obj]


@pytest.mark.parametrize("seed", range(300))
def test_randomized_series_never_break_the_contract(seed):
    c = _random_ctx(seed)
    raw = R.run_all(c)
    assert with_evidence(raw) == raw  # real rules always carry complete evidence (nothing silently dropped)
    for audience in ("customer", "operator"):
        for lang in ("id", "en"):
            out = build_insights(c, lang, audience, strict=True)  # strict: contradictions raise
            items = out["insights"]
            ids = [i["id"] for i in items]
            assert len(ids) == len(set(ids)), ids
            topics = [(i["kind"], i["period"], i["rule_id"]) for i in items]
            assert len(topics) == len(set(topics)), topics
            for i in items:
                for x in _walk(i):
                    if isinstance(x, float):
                        assert math.isfinite(x), (i["rule_id"], x)
                text = " ".join([i["title"], i["body"], i["next_step"] or ""]).lower()
                assert not re.search(r"\b(nan|inf|infinity|none)\b", text), (i["rule_id"], text)
                for w in CAUSAL[lang] + OVERCLAIM[lang]:
                    assert not re.search(rf"{w}", text), (i["rule_id"], w, text)
                ev = i["evidence"]
                assert ev["datasets"] and all(
                    d["version_id"] or i["rule_id"] == "ingestion_failure" for d in ev["datasets"]
                )
                assert ev["periods"]["window_start"] and ev["periods"]["window_end"]
                assert ev["inputs"] and ev["outputs"] and ev["method"]["key"]


# -- boundaries -----------------------------------------------------------------------------------
def test_trend_run_boundary_is_three_moves():
    assert R.TREND_MIN_MOVES == 3
    assert R.icp_trend_run(ctx(series([70, 71, 72]))) is None
    assert R.icp_trend_run(ctx(series([70, 71, 72, 73]))) is not None


def test_extreme_window_boundary_is_24_months():
    """The window holds months M-23..M, so moves into M-22..M are compared (23 moves)."""

    def build(shift_into: int) -> pd.Series:
        vals = [70.0 + (0.5 if i % 2 else 0) for i in range(30)]
        vals.append(vals[-1] + 20)  # month M: +20
        k = len(vals) - 1 - shift_into  # level shift of +50 starting at month M-shift_into
        return series([v + (50 if i >= k else 0) for i, v in enumerate(vals)])

    assert R.icp_extreme_move(ctx(build(22))) is None  # +50 move into M-22 is compared and larger
    assert R.icp_extreme_move(ctx(build(23))) is not None  # move into M-23 is outside the comparison


def test_spread_exactly_at_the_window_edge_is_silent():
    n_hist = 36
    brent = series([80.0] * (n_hist + 1))
    hist = [80.0 + 2 + (i % 3) * 0.5 for i in range(n_hist)]  # spread range 2.0..3.0
    at_edge = ctx(series([*hist, 83.0]), benchmarks={"brent": brent})
    beyond = ctx(series([*hist, 83.01]), benchmarks={"brent": brent})
    assert R.spread_vs_history(at_edge) is None
    assert R.spread_vs_history(beyond) is not None


def test_revision_fires_at_exactly_half_the_mae():
    icp = series([90.0, 100.0])
    cur = fc("fc_new", "2026-09", [103.0])
    at = fc("fc_old", "2026-09", [100.0], issue="2026-10-10")  # change 3.0 == 0.5 x MAE 6.0
    ev = dict(EVAL, by_horizon=[{"mode": "hold_last", "horizon": 1, "n": 60, "mae": 6.0}])
    assert R.forecast_revision(ctx(icp, latest_fc=cur, prev_fc=at, evals={"5": ev})) is not None
    below = fc("fc_old", "2026-09", [100.01], issue="2026-10-10")
    assert R.forecast_revision(ctx(icp, latest_fc=cur, prev_fc=below, evals={"5": ev})) is None


def test_revision_is_not_emitted_for_months_already_published():
    icp = series([90.0, 100.0, 101.0])  # 2026-09 published; forecasts were for 2026-09
    cur = fc("fc_new", "2026-08", [110.0])
    old = fc("fc_old", "2026-08", [90.0], issue="2026-09-10")
    ev = dict(EVAL, by_horizon=[{"mode": "hold_last", "horizon": 1, "n": 60, "mae": 6.0}])
    assert R.forecast_revision(ctx(icp, latest_fc=cur, prev_fc=old, evals={"5": ev})) is None


@pytest.mark.parametrize(("dir_acc", "weak"), [(0.59, True), (0.6, False)])
def test_direction_weak_boundary(dir_acc, weak):
    ev = dict(EVAL, by_horizon=[{"mode": "hold_last", "horizon": 1, "n": 60, "mae": 3.0, "dir_acc": dir_acc}])
    f = R.forecast_outlook(ctx(series([90.0, 100.0]), latest_fc=fc("f", "2026-09", [105.0]), evals={"5": ev}))
    assert f is not None
    assert (("forecast_outlook.direction_weak", {}) in f.body_parts) is weak


@pytest.mark.parametrize(("mae", "no_edge"), [(6.09 * 0.98, True), (6.09 * 0.98 - 0.001, False)])
def test_model_evaluation_no_edge_boundary(mae, no_edge):
    ev = dict(EVAL, hold_last_h1_mae=mae)
    f = R.model_evaluation(ctx(series([70.0]), evals={"5": ev}, production_version="5"))
    assert f is not None and (f.body_parts == [("model_evaluation.no_edge", {})]) is no_edge


@pytest.mark.parametrize(("period", "importance"), [("2025-10", "medium"), ("2025-09", "low")])
def test_verification_recent_window_boundary(period, importance):
    icp = series([70.0] * 13)  # 2025-09 .. 2026-09
    months = [
        {"period": str(p), "basis": "two_documents", "label": LEGEND["two_documents"]["label"]}
        for p in icp.index
    ]
    for m in months:
        if m["period"] == period:
            m.update(basis="no_evidence", label=LEGEND["no_evidence"]["label"])
    f = R.data_verification(ctx(icp, verification=months))
    assert f is not None and f.importance == importance


@pytest.mark.parametrize(
    ("share", "flag"), [(0.8 - UNDER_COVERAGE_MARGIN, False), (0.8 - UNDER_COVERAGE_MARGIN - 0.001, True)]
)
def test_under_coverage_margin_boundary(share, flag):
    facts = dict(EVAL, coverage={"hold_last": {"1": {"0.8": share, "n": 40}}})
    ev = step_evaluation(facts, "hold_last", 1)
    assert ev is not None and ("0.8" in ev["under_coverage"]) is flag


@pytest.mark.parametrize(("n", "warn"), [(MIN_COVERAGE_N - 1, True), (MIN_COVERAGE_N, False)])
def test_small_coverage_sample_boundary(n, warn):
    facts = dict(EVAL, coverage={"hold_last": {"1": {"0.8": 0.8, "n": n}}})
    ev = step_evaluation(facts, "hold_last", 1)
    assert ev is not None and (ev["coverage_warning"] is not None) is warn


# -- overclaiming sweep over every API-provided text ------------------------------------------------
NEGATED = ("tidak menjamin", "bukan jaminan", "do not guarantee", "not a guarantee", "not guaranteed")


def _api_texts() -> list[tuple[str, str, str]]:
    from icp_api.insights.context import BASELINE_LABEL, MODE_TEXT
    from icp_api.insights.templates_en import TEMPLATES as EN
    from icp_api.insights.templates_id import TEMPLATES as ID
    from icp_api.services import explain
    from icp_api.services.verification import KIND_LABEL, _reason

    out = [("id", f"tpl.{k}", v) for k, v in ID.items()] + [("en", f"tpl.{k}", v) for k, v in EN.items()]
    locs: list[tuple[str, dict[str, str]]] = [
        *((f"method.{k}", v) for k, v in explain.METHOD_NOTES.items()),
        ("interval", explain.INTERVAL_NOTE),
        ("evaluation", explain.EVALUATION_NOTE),
        ("disclosure", explain.DISCLOSURE_NOTE),
        ("baseline", BASELINE_LABEL),
        *((f"mode.{k}", v) for k, v in MODE_TEXT.items()),
        *((f"legend.{k}.label", v["label"]) for k, v in LEGEND.items()),
        *((f"legend.{k}.explanation", v["explanation"]) for k, v in LEGEND.items()),
        *((f"kind.{k}", v) for k, v in KIND_LABEL.items()),
        ("reason.rejected", _reason({"verification_status": "rejected"})),
        ("reason.outvoted", _reason({"verification_status": "dissent", "note": "outvoted by 2"})),
        ("reason.other", _reason({"verification_status": "dissent"})),
        ("ignored", explain.ignored_note(["dubai"]) or {}),
        *((f"usage.{i}", explain._usage_note(i, u)) for i in ("brent", "usd_idr") for u in (True, False)),
    ]
    for key, loc in locs:
        out += [("id", key, loc["id"]), ("en", key, loc["en"])]
    return out


@pytest.mark.parametrize(("lang", "key", "text"), _api_texts(), ids=lambda x: str(x)[:40])
def test_api_texts_do_not_overclaim(lang, key, text):
    low = text.lower()
    for neg in NEGATED:
        low = low.replace(neg, "")
    for w in CAUSAL[lang] + OVERCLAIM[lang]:
        assert not re.search(rf"\b{w}", low), (key, w)
