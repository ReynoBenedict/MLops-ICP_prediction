"""Evidence contract for every emitted insight.

A finding is published only when it is traceable: at least one dataset with a version id, a period
window (start and end), the inputs the rule read, the outputs it computed, a known method key, and
only finite numbers. Anything else is dropped (and logged) by ``build_insights``."""

from __future__ import annotations

import math
from typing import Any

from icp_api.insights.evidence import Finding, Value
from icp_api.insights.narrative import TEMPLATES

VERSIONLESS_RULES = frozenset({"ingestion_failure"})


class EvidenceError(ValueError):
    pass


def _finite(v: Any) -> bool:
    if isinstance(v, Value):
        return _finite(v.value) and all(_finite(x) for x in v.label_slots.values())
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return True
    if isinstance(v, int | float):
        return math.isfinite(float(v))
    return True


def validate_finding(f: Finding) -> None:
    """Raise ``EvidenceError`` naming the first missing piece of evidence."""
    if not f.datasets or any(not d.dataset for d in f.datasets):
        raise EvidenceError("dataset and version")
    # a failed ingestion may concern a dataset without an active version; its run id (an input) is the evidence
    if f.rule_id not in VERSIONLESS_RULES and any(not d.version_id for d in f.datasets):
        raise EvidenceError("dataset and version")
    w = f.window or {}
    if not w.get("window_start") or not w.get("window_end"):
        raise EvidenceError("period window")
    if not f.inputs:
        raise EvidenceError("inputs")
    if not f.outputs:
        raise EvidenceError("outputs")
    if any(f"method.{f.method}" not in TEMPLATES[lang] for lang in ("id", "en")):
        raise EvidenceError("method key")
    for group in (f.facts, f.inputs, f.outputs, list(f.slots.values())):
        if not all(_finite(v) for v in group):
            raise EvidenceError("non-finite value")
