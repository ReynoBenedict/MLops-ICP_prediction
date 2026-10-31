"""Deterministic insight engine: rule boundaries, consistency, templates and language."""

from __future__ import annotations

import re
import string
from datetime import date
from typing import Any

import pandas as pd
import pytest

from icp_api.insights import build_insights
from icp_api.insights import rules as R
from icp_api.insights.consistency import InconsistentFindings, reconcile
from icp_api.insights.context import InsightContext, step_evaluation
from icp_api.insights.narrative import compose
from icp_api.insights.templates_en import TEMPLATES as EN
from icp_api.insights.templates_id import TEMPLATES as ID
from icp_api.services.verification import LEGEND, classify_icp_month


def series(values: list[float], end: str = "2026-09") -> pd.Series:
    idx = pd.period_range(end=pd.Period(end, freq="M"), periods=len(values), freq="M")
    return pd.Series(values, index=idx, dtype=float)


def flat(n: int, v: float = 70.0, end: str = "2026-09") -> pd.Series:
    return series([v + (0.1 if i % 2 else -0.1) for i in range(n)], end)


def ctx(icp: pd.Series, **kw: Any) -> InsightContext:
    base: dict[str, Any] = {
        "today": date(2026, 10, 19),
        "versions": {"icp": {"id": "dsv_icp", "period_end": str(icp.index.max())}},
    }
    base.update(kw)
    return InsightContext(icp=icp, **base)


EVAL = {
    "version": "5",
    "method": "spread_brent_6m",
    "method_family": "spread_rule",
    "description": "x",
    "benchmarks_used": ["brent"],
    "by_horizon": [
        {"mode": "hold_last", "horizon": 1, "n": 69, "mae": 6.0, "mase": 1.1, "dir_acc": 0.53},
        {"mode": "hold_last", "horizon": 2, "n": 68, "mae": 9.0, "mase": 1.7, "dir_acc": 0.56},
        {"mode": "observed", "horizon": 1, "n": 69, "mae": 0.8, "mase": 0.15, "dir_acc": 0.96},
    ],
    "hold_last_h1_mae": 6.1,
    "naive_hold_last_h1_mae": 6.09,
    "coverage": {"hold_last": {"1": {"0.5": 0.64, "0.8": 0.857, "0.95": 0.857, "n": 28}}},
    "backtest_origins": "2020-12..2026-08",
}


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
        "dataset_versions": {"icp": "a", "brent": "b"},
        "result": {"points": pts},
        **kw,
    }


# -- month over month / year over year ---------------------------------------------------------------
def test_mom_needs_previous_month():
    assert R.icp_mom(ctx(series([70.0]))) is None
    s = series([80.0, 0.0, 88.0]).drop(pd.Period("2026-08", freq="M"))
    assert R.icp_mom(ctx(s)) is None
    f = R.icp_mom(ctx(series([80.0, 88.0])))
    assert f and f.importance == "low" and f.direction == 1
    assert {v.key: v.value for v in f.outputs}["change_pct"] == pytest.approx(10.0)


def test_mom_importance_is_not_driven_by_move_size():
    small = R.icp_mom(ctx(series([80.0, 80.1])))
    huge = R.icp_mom(ctx(series([80.0, 140.0])))
    assert small and huge and small.importance == huge.importance == "low"


def test_yoy_boundary():
    assert R.icp_yoy(ctx(series([70.0] * 12))) is None
    f = R.icp_yoy(ctx(series([50.0] + [70.0] * 11 + [60.0])))
    assert f and {v.key: v.value for v in f.outputs}["change_pct"] == pytest.approx(20.0)


# -- trend run ---------------------------------------------------------------------------------------
def test_trend_run_threshold_and_gaps():
    assert R.icp_trend_run(ctx(series([70, 71, 72]))) is None  # 2 moves
    f = R.icp_trend_run(ctx(series([75, 70, 71, 72, 73])))
    assert f and {v.key: v.value for v in f.facts}["run_months"] == 3 and f.direction == 1
    assert R.icp_trend_run(ctx(series([70, 71, 71, 72, 73]))) is None  # flat month breaks the run
    gap = series([70, 71, 72, 73, 74]).drop(pd.Period("2026-07", freq="M"))
    assert R.icp_trend_run(ctx(gap)) is None


# -- extreme move ------------------------------------------------------------------------------------
def test_extreme_move_needs_twelve_moves_and_strictly_largest():
    assert R.icp_extreme_move(ctx(series([70.0] * 11 + [90.0]))) is None  # 11 moves
    base = [70.0 + (i % 2) for i in range(24)]
    f = R.icp_extreme_move(ctx(series([*base, base[-1] + 5])))
    assert f and f.importance == "medium" and f.body_parts
    tie = series([70, 75] + [70.0 + (i % 2) for i in range(20)] + [75.0])  # equals an earlier +5 jump
    assert R.icp_extreme_move(ctx(tie)) is None


# -- spread ------------------------------------------------------------------------------------------
def _spread_ctx(last_spread: float, n_hist: int = 36, used: bool = True) -> InsightContext:
    n = n_hist + 1
    brent = series([80.0] * n)
    icp = series([80.0 + 2 + (i % 3) * 0.5 for i in range(n_hist)] + [80.0 + last_spread])
    ev = dict(EVAL, benchmarks_used=["brent"] if used else ["wti"])
    return ctx(icp, benchmarks={"brent": brent}, evals={"5": ev}, production_version="5")


def test_spread_inside_range_is_silent():
    assert R.spread_vs_history(_spread_ctx(2.5)) is None


def test_spread_outside_range_and_importance_by_model_use():
    f = R.spread_vs_history(_spread_ctx(9.0))
    assert f and f.importance == "medium" and f.model is not None
    out = {(v.key, v.label_slots.get("b")): v.value for v in f.outputs}
    assert out[("window_high", "Brent")] == pytest.approx(3.0)
    g = R.spread_vs_history(_spread_ctx(9.0, used=False))
    assert g and g.importance == "low"


def test_spread_needs_24_paired_months():
    assert R.spread_vs_history(_spread_ctx(9.0, n_hist=23)) is None
    assert R.spread_vs_history(_spread_ctx(9.0, n_hist=24)) is not None


# -- fx ----------------------------------------------------------------------------------------------
def test_fx_context():
    assert R.fx_context(ctx(series([80.0, 88.0]))) is None
    f = R.fx_context(ctx(series([80.0, 88.0]), fx=series([16000.0, 16000.0])))
    assert f and {v.key: v.value for v in f.outputs}["idr_change_pct"] == pytest.approx(10.0)


# -- forecast outlook / revision ---------------------------------------------------------------------
def test_outlook_pairs_direction_with_measured_skill():
    icp = series([90.0, 100.0])
    c = ctx(icp, latest_fc=fc("fc_1", "2026-09", [105.0, 106.0]), evals={"5": EVAL}, production_version="5")
    f = R.forecast_outlook(c)
    assert f is not None
    keys = [k for k, _ in f.body_parts]
    assert "forecast_outlook.direction" in keys and "forecast_outlook.direction_weak" in keys
    assert "forecast_outlook.contains" in keys  # 100 inside [95, 115]
    item = compose(f, "en", {})
    assert "53%" in item["body"] and "86%" in item["body"] and "28 cases" in item["body"]


def test_outlook_not_generated_for_stale_or_unevaluated_forecasts():
    icp = series([90.0, 100.0])
    assert R.forecast_outlook(ctx(icp, latest_fc=fc("f", "2026-08", [1.0]), evals={"5": EVAL})) is None
    assert R.forecast_outlook(ctx(icp, latest_fc=fc("f", "2026-09", [1.0]), evals={})) is None


def test_revision_threshold_scales_with_backtest_error():
    icp = series([90.0, 100.0])
    cur = fc("fc_new", "2026-09", [103.0], dataset_versions={"icp": "a2", "brent": "b"})
    small = fc("fc_old", "2026-09", [100.1], issue="2026-10-10")
    big = fc("fc_old", "2026-09", [99.0], issue="2026-10-10")
    assert R.forecast_revision(ctx(icp, latest_fc=cur, prev_fc=small, evals={"5": EVAL})) is None  # 2.9 < 3.0
    f = R.forecast_revision(ctx(icp, latest_fc=cur, prev_fc=big, evals={"5": EVAL}))
    assert f is not None and f.body_parts[0][0] == "forecast_revision.diffs"
    text = compose(f, "id", {})["body"]
    assert "versi data ICP" in text and "Penyebab" not in text


# -- freshness / verification / model / ingestion ----------------------------------------------------
def test_freshness_levels():
    icp = series([70.0, 71.0])
    assert R.data_freshness(ctx(icp, freshness={"icp": {"status": "fresh"}})) is None
    lag = R.data_freshness(
        ctx(
            icp,
            freshness={
                "icp": {
                    "status": "lagging",
                    "latest_period": "2026-08",
                    "expected_period": "2026-09",
                    "months_behind": 1,
                }
            },
        )
    )
    stale = R.data_freshness(
        ctx(
            icp,
            freshness={
                "icp": {
                    "status": "stale",
                    "latest_period": "2026-07",
                    "expected_period": "2026-09",
                    "months_behind": 2,
                }
            },
        )
    )
    assert lag and lag.importance == "low" and stale and stale.importance == "high"
    assert "seharusnya" not in compose(lag, "id", {})["body"]


def _vm(period: str, basis: str) -> dict[str, Any]:
    return {"period": period, "basis": basis, "label": LEGEND[basis]["label"]}


def test_verification_counts_and_importance():
    icp = series([70.0] * 3)
    ok = [_vm("2026-07", "two_documents"), _vm("2026-08", "decree_text"), _vm("2026-09", "two_documents")]
    assert R.data_verification(ctx(icp, verification=ok)) is None
    old_single = [_vm("2020-01", "single_document"), *ok]
    f = R.data_verification(ctx(icp, verification=old_single))
    assert f and f.importance == "low"
    recent_gap = [*ok[:2], _vm("2026-09", "no_evidence")]
    g = R.data_verification(ctx(icp, verification=recent_gap))
    assert g and g.importance == "medium"
    assert "Bukti belum tercatat: 1" in compose(g, "id", {})["body"]


def test_model_evaluation_no_edge_note():
    f = R.model_evaluation(ctx(series([70.0]), evals={"5": EVAL}, production_version="5"))
    assert f and f.body_parts == [("model_evaluation.no_edge", {})]


def test_model_evaluation_compares_model_and_baseline_on_the_same_cases():
    # the registry's naive figure covers fewer cases than the model's MAE; persistence recomputed on
    # the model's own backtest cases wins when the stored predictions are available
    ev = dict(EVAL, baselines={"hold_last": {"1": {"mae": 6.06, "n": 69}}})
    f = R.model_evaluation(ctx(series([70.0]), evals={"5": ev}, production_version="5"))
    assert f is not None
    facts = {v.key: v.value for v in f.facts}
    assert facts["backtest_mae"] == 6.0 and facts["naive_mae"] == 6.06 and f.window["n"] == 69
    assert f.body_parts == [("model_evaluation.no_edge", {})]  # 6.0 >= 6.06 * 0.98


def test_ingestion_failures_are_operator_only_and_never_show_raw_errors():
    run = {
        "id": "ing_1",
        "dataset": "brent",
        "status": "failed",
        "started_at": "2026-10-18T01:02:03Z",
        "error_message": "HTTPError 500 at https://internal.example/secret?token=abc",
    }
    c = ctx(series([70.0, 71.0]), failed_runs=[run])
    cust = build_insights(c, "id", "customer")
    op = build_insights(c, "id", "operator")
    assert not any(i["rule_id"] == "ingestion_failure" for i in cust["insights"])
    ops = [i for i in op["insights"] if i["rule_id"] == "ingestion_failure"]
    assert ops and ops[0]["audience"] == "operator" and ops[0]["importance"] == "high"
    assert "secret" not in str(op) and "HTTPError" not in str(op)


# -- consistency -------------------------------------------------------------------------------------
def test_extreme_move_folds_month_over_month():
    base = [70.0 + (i % 2) for i in range(24)]
    c = ctx(series([*base, base[-1] + 5]))
    rule_ids = [f.rule_id for f in reconcile(R.run_all(c), "customer", strict=True)]
    assert "icp_extreme_move" in rule_ids and "icp_mom" not in rule_ids


def test_contradictory_directions_are_detected():
    up = R.icp_mom(ctx(series([70.0, 75.0])))
    down = R.icp_trend_run(ctx(series([80, 79, 78, 77])))
    assert up and down
    with pytest.raises(InconsistentFindings):
        reconcile([up, down], "customer", strict=True)
    kept = reconcile([up, down], "customer")
    assert not any(f.rule_id in ("icp_mom", "icp_trend_run") for f in kept)


def test_ids_are_unique_and_order_follows_importance():
    base = [70.0 + (i % 2) for i in range(36)]
    c = ctx(
        series([*base, base[-1] + 8]),
        fx=series([16000.0] * 37),
        freshness={
            "icp": {
                "status": "stale",
                "latest_period": "2026-09",
                "expected_period": "2026-11",
                "months_behind": 2,
            }
        },
    )
    out = build_insights(c, "en", "customer", strict=True)["insights"]
    ids = [i["id"] for i in out]
    assert len(ids) == len(set(ids)) and len(out) <= 8
    order = {"high": 0, "medium": 1, "low": 2}
    assert [order[i["importance"]] for i in out] == sorted(order[i["importance"]] for i in out)


# -- templates and language --------------------------------------------------------------------------
def _slots(t: str) -> set[str]:
    return {f for _, f, _, _ in string.Formatter().parse(t) if f}


def test_templates_have_same_keys_and_slots():
    assert set(ID) == set(EN)
    for k in ID:
        assert _slots(ID[k]) == _slots(EN[k]), k


CAUSAL = re.compile(r"\b(karena|disebabkan|penyebab|akibat|because|caused|due to|driven by)\b", re.I)
JARGON = re.compile(r"simpangan baku|standard deviation|\bz\b|z-score", re.I)


def test_templates_have_no_causal_words_or_statistical_jargon():
    for t in (*ID.values(), *EN.values()):
        assert not CAUSAL.search(t), t
        assert not JARGON.search(t), t


def test_english_output_has_no_indonesian_months_or_decimal_commas():
    base = [70.0 + (i % 2) for i in range(36)]
    c = ctx(series([*base, 112.81]), fx=series([16000.0] * 37))
    out = build_insights(c, "en", "customer")
    text = " ".join(i["title"] + " " + i["body"] for i in out["insights"])
    assert "112.81" in text and "112,81" not in text
    assert not re.search(r"\b(Januari|Februari|Maret|Mei|Juni|Juli|Agustus|Oktober|Desember)\b", text)
    ind = build_insights(c, "id", "customer")
    assert "112,81" in " ".join(i["body"] for i in ind["insights"])


def test_step_evaluation_lookup():
    ev = step_evaluation(EVAL, "hold_last", 1)
    assert (
        ev
        and ev["n"] == 69
        and ev["coverage"] == {"0.5": 0.64, "0.8": 0.857, "0.95": 0.857}
        and ev["coverage_n"] == 28
    )
    assert step_evaluation(EVAL, "hold_last", 6) is None
    assert step_evaluation(EVAL, None, 1) is None


# -- verification classification ---------------------------------------------------------------------
def _row(kind: str, status: str, value: float, url: str, created: str = "2026-10-01") -> dict[str, Any]:
    return {
        "kind": kind,
        "verification_status": status,
        "value": value,
        "source_url": url,
        "document_sha256": None,
        "reference": None,
        "created_at": created,
    }


def test_classify_icp_month_bases():
    two = [_row("announcement", "verified", 112.81, "a"), _row("decree_text", "verified", 112.81, "d")]
    assert classify_icp_month(112.81, two)["basis"] == "two_documents"
    assert (
        classify_icp_month(112.81, [_row("decree_text", "verified", 112.81, "d")])["basis"] == "decree_text"
    )
    legacy = [_row("decree_ocr", "verified", 70.0, "d"), _row("legacy_csv", "verified", 70.0, "data/raw")]
    assert classify_icp_month(70.0, legacy)["basis"] == "document_plus_legacy"
    assert (
        classify_icp_month(70.0, [_row("announcement", "single_source", 70.0, "a")])["basis"]
        == "single_document"
    )
    assert (
        classify_icp_month(70.0, [_row("decree_ocr", "machine_extracted", 70.0, "d")])["basis"]
        == "ocr_single"
    )
    assert classify_icp_month(70.0, [])["basis"] == "no_evidence"
    rejected_only = [_row("legacy_dataset", "rejected", 60.0, "x")]
    assert classify_icp_month(70.0, rejected_only)["basis"] == "no_evidence"
    # the same document seen in two ingestions counts once
    dup = [
        _row("announcement", "verified", 70.0, "a"),
        _row("announcement", "verified", 70.0, "a", "2026-10-02"),
    ]
    assert classify_icp_month(70.0, dup)["basis"] == "single_document"
