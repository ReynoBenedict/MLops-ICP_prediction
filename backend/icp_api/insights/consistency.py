"""Consistency checks over findings: dedupe, folding, sign agreement, ordering and caps."""

from __future__ import annotations

import logging

from icp_api.insights.evidence import Finding

logger = logging.getLogger(__name__)

IMPORTANCE_ORDER = {"high": 0, "medium": 1, "low": 2}
KIND_ORDER = [
    "freshness",
    "data_quality",
    "ingestion",
    "unusual_movement",
    "material_change",
    "forecast_outlook",
    "forecast_change",
    "benchmark_divergence",
    "trend",
    "annual_comparison",
    "fx_context",
    "model_evaluation",
]
CUSTOMER_CAP = 8


class InconsistentFindings(AssertionError):
    pass


def check_signs(findings: list[Finding]) -> None:
    """Rules describing the latest ICP move (mom, trend, extreme) must agree on its direction."""
    signs = {
        f.rule_id: f.direction
        for f in findings
        if f.rule_id in ("icp_mom", "icp_extreme_move") and f.direction is not None
    }
    trend = next((f for f in findings if f.rule_id == "icp_trend_run"), None)
    if trend is not None and trend.direction is not None:
        signs["icp_trend_run"] = trend.direction
    if len(set(signs.values())) > 1:
        raise InconsistentFindings(f"contradictory move directions: {signs}")


def reconcile(findings: list[Finding], audience: str, strict: bool = False) -> list[Finding]:
    """Apply dedupe/folding and ordering. ``strict`` raises on contradictions (tests); otherwise a
    contradictory set drops the move-description rules and logs a warning."""
    try:
        check_signs(findings)
    except InconsistentFindings:
        if strict:
            raise
        logger.warning("insight rules disagree on direction; dropping move findings")
        findings = [f for f in findings if f.rule_id not in ("icp_mom", "icp_extreme_move", "icp_trend_run")]
    # icp_extreme_move restates the month-over-month move: keep one headline per move
    extreme_periods = {f.period for f in findings if f.rule_id == "icp_extreme_move"}
    findings = [f for f in findings if not (f.rule_id == "icp_mom" and f.period in extreme_periods)]
    # one finding per id (and per kind + subject period + rule)
    seen: set[str] = set()
    unique: list[Finding] = []
    for f in findings:
        if f.id in seen:
            continue
        seen.add(f.id)
        unique.append(f)
    if audience != "operator":
        unique = [f for f in unique if f.audience == "customer"]
    unique.sort(
        key=lambda f: (
            IMPORTANCE_ORDER[f.importance],
            KIND_ORDER.index(f.kind) if f.kind in KIND_ORDER else len(KIND_ORDER),
            f.period or "",
        )
    )
    if audience != "operator":
        unique = unique[:CUSTOMER_CAP]
    return unique
