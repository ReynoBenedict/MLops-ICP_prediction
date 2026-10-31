"""Deterministic insight engine: rules -> findings -> consistency -> localized narrative.

Pure with respect to its ``InsightContext``; services/insights.py loads the context."""

from __future__ import annotations

import logging
from typing import Any

from icp_api.insights.consistency import reconcile
from icp_api.insights.context import InsightContext
from icp_api.insights.evidence import Finding
from icp_api.insights.narrative import TEMPLATES as _T
from icp_api.insights.narrative import Lang, compose
from icp_api.insights.rules import run_all
from icp_api.insights.validate import EvidenceError, validate_finding

logger = logging.getLogger(__name__)

ENGINE_VERSION = "2026-10.2"


def with_evidence(findings: list[Finding]) -> list[Finding]:
    """Drop (and log) findings that cannot be traced to dataset versions, a period window, inputs,
    outputs and a method - an insight without evidence is never published."""
    kept = []
    for f in findings:
        try:
            validate_finding(f)
        except EvidenceError as exc:
            logger.warning("insight %s dropped: missing evidence (%s)", f.id, exc)
            continue
        kept.append(f)
    return kept


def build_insights(
    ctx: InsightContext,
    lang: Lang,
    audience: str = "customer",
    dataset_labels: dict[str, dict[str, str]] | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    findings = reconcile(with_evidence(run_all(ctx)), audience, strict=strict)
    labels = dataset_labels or {}
    return {
        "lang": lang,
        "engine_version": ENGINE_VERSION,
        "insights": [compose(f, lang, labels) for f in findings],
        "disclaimer": _T[lang]["disclaimer"],
    }


__all__ = ["ENGINE_VERSION", "InsightContext", "build_insights"]
