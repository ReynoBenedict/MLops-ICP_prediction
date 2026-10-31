"""Read-time evaluation disclosure (audit v7 ML-01, ML-02, ML-03, ML-04)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import pytest

from icp_api.insights.context import step_evaluation, under_coverage_levels
from icp_api.services.explain import DISCLOSURE_NOTE, EVALUATION_NOTE, evaluation_extras


def _facts(stored: dict[str, float], embargoed: dict[str, float] | None) -> dict[str, Any]:
    facts: dict[str, Any] = {
        "by_horizon": [{"mode": "hold_last", "horizon": 6, "n": 60, "mae": 13.0}],
        "coverage": {"hold_last": {"6": {**stored, "n": 23}}},
        "backtest_origins": "2021-01..2026-08",
    }
    if embargoed is not None:
        facts["coverage_embargoed"] = {"hold_last": {"6": {**embargoed, "n": 23}}}
    return facts


def test_stored_coverage_without_embargo_is_disclosed_next_to_the_embargoed_recomputation():
    """ML-01 on the production v5 numbers (hold_last h6): 50 % range 0.739 stored, 0.435 embargoed."""
    ev = step_evaluation(
        _facts({"0.5": 0.739, "0.8": 0.87, "0.95": 0.913}, {"0.5": 0.435, "0.8": 0.87, "0.95": 0.913}),
        "hold_last",
        6,
    )
    assert ev is not None
    assert ev["coverage"] == {"0.5": 0.739, "0.8": 0.87, "0.95": 0.913}  # stored artefact, unchanged
    assert ev["coverage_embargoed"] == {"0.5": 0.435, "0.8": 0.87, "0.95": 0.913}
    assert ev["coverage_check"] == "without_embargo"
    assert "0.95" in ev["under_coverage"]  # 0.913 < 0.925 (ML-02)
    warning = ev["coverage_warning"]["en"]
    assert "43%" in warning or "91%" in warning  # the warning quotes the stricter (embargoed) figures


def test_matching_stored_and_recomputed_coverage_is_labelled_embargoed():
    same = {"0.5": 0.55, "0.8": 0.82, "0.95": 0.96}
    ev = step_evaluation(_facts(same, same), "hold_last", 6)
    assert ev["coverage_check"] == "embargoed" and ev["under_coverage"] == []


def test_without_stored_predictions_the_check_is_not_claimed():
    ev = step_evaluation(_facts({"0.5": 0.55, "0.8": 0.82, "0.95": 0.96}, None), "hold_last", 6)
    assert ev["coverage_check"] is None and ev["coverage_embargoed"] is None


@pytest.mark.parametrize(
    ("level", "share", "flagged"),
    [
        ("0.95", 0.857, True),  # v5 hold_last h1: 0.093 below nominal, missed by the old 0.10 rule
        ("0.95", 0.924, True),  # miss rate 7.6 % > 1.5 x 5 %
        ("0.95", 0.93, False),
        ("0.8", 0.69, True),
        ("0.8", 0.71, False),
        ("0.5", 0.39, True),
        ("0.5", 0.41, False),
    ],
)
def test_under_coverage_threshold_is_stricter_for_high_nominal_levels(level, share, flagged):
    """ML-02: flagged when the shortfall is >= 10 points OR the miss rate exceeds 1.5x nominal."""
    assert (level in under_coverage_levels({level: share})) is flagged


def test_disclosure_says_the_accuracy_comes_from_the_selection_backtest():
    """ML-03."""
    for note in (DISCLOSURE_NOTE, EVALUATION_NOTE):
        assert "select" in note["en"].lower()
        assert "memilih" in note["id"].lower()


class _Registry:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def backtest_predictions(self, run_id: str) -> pd.DataFrame:
        return self.df


def test_evaluation_extras_recompute_the_embargoed_coverage_from_stored_predictions():
    rng = np.random.default_rng(3)
    start = pd.Period("2020-01", freq="M")
    rows = []
    for i in range(30):
        for h in (1, 3):
            y = 80 + rng.normal(0, 4)
            e = rng.normal(0, 2)
            rows.append(
                {
                    "method": "m",
                    "mode": "hold_last",
                    "origin": str(start + i),
                    "horizon": h,
                    "target": str(start + i + h),
                    "y_true": y,
                    "y_pred": y + e,
                    "error": e,
                    "last_official": 80.0,
                }
            )
    out = evaluation_extras(_Registry(pd.DataFrame(rows)), "run1")
    assert set(out["coverage_embargoed"]["hold_last"]) == {"1", "3"}


def test_issue_date_and_month_end_use_jakarta_time():
    """ML-04: 2026-09-30 20:00 UTC is already 1 October in Jakarta (UTC+7)."""
    from icp_ml.sources.base import jakarta_today

    assert jakarta_today(datetime(2026, 9, 30, 20, 0, tzinfo=timezone.utc)).isoformat() == "2026-10-01"
    assert jakarta_today(datetime(2026, 9, 30, 16, 59, tzinfo=timezone.utc)).isoformat() == "2026-09-30"
