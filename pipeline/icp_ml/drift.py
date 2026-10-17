"""Feature drift measurement with the Population Stability Index (PSI).

PSI compares the distribution of each feature in a reference window with a recent window.
Conventional reading: < 0.10 stable, 0.10-0.20 moderate shift, >= 0.20 significant shift.

The windows are disjoint: the current window is the latest ``recent_months`` complete rows and
the reference is every earlier row the production model was fitted on. With monthly data the
windows are small (tens of rows), so bin proportions use additive (Laplace) smoothing - without
it a single empty bin dominates the score - and the report always includes the sample sizes.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from icp_ml.features import FEATURE_COLUMNS

MODERATE, SIGNIFICANT = 0.10, 0.20
_SMOOTHING = 0.5  # pseudo-count added to every bin


def psi(reference: np.ndarray, current: np.ndarray, bins: int = 5) -> float:
    reference = np.asarray(reference, dtype=float)
    current = np.asarray(current, dtype=float)
    if len(reference) < 2 or len(current) < 1:
        raise ValueError("Not enough observations to compute PSI")
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    if len(edges) < 2:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    k = len(edges) - 1
    ref_pct = (np.histogram(reference, edges)[0] + _SMOOTHING) / (len(reference) + _SMOOTHING * k)
    cur_pct = (np.histogram(current, edges)[0] + _SMOOTHING) / (len(current) + _SMOOTHING * k)
    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def _status(value: float) -> str:
    return "significant" if value >= SIGNIFICANT else ("moderate" if value >= MODERATE else "stable")


@dataclass
class DriftReport:
    reference_period: str
    current_period: str
    reference_rows: int
    current_rows: int
    features: dict[str, dict[str, Any]] = field(default_factory=dict)
    max_psi: float = 0.0
    status: str = "stable"
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def drift_report(
    frame: pd.DataFrame, fit_end: pd.Period, recent_months: int = 12, bins: int = 5
) -> DriftReport:
    """Compare the latest ``recent_months`` feature rows with the earlier rows (up to ``fit_end``)."""
    current = frame.tail(recent_months)
    reference = frame[(frame.index < current.index.min()) & (frame.index <= fit_end)]
    if len(current) < 3:
        raise ValueError("Fewer than 3 recent feature rows; drift cannot be assessed")
    if len(reference) < 10:
        raise ValueError("Reference window too small for drift analysis")
    unseen = int((current.index > fit_end).sum())
    note = (
        f"{unseen} of {len(current)} current rows are newer than the data the model was fitted on."
        if unseen
        else "All current rows were also part of the model's fitting data; this measures how recent "
        "conditions differ from earlier history, not post-deployment drift."
    )
    report = DriftReport(
        reference_period=f"{reference.index.min()}..{reference.index.max()}",
        current_period=f"{current.index.min()}..{current.index.max()}",
        reference_rows=len(reference),
        current_rows=len(current),
        note=note,
    )
    # Only features observed throughout both windows are compared (optional inputs may be sparse).
    usable = [
        c
        for c in FEATURE_COLUMNS
        if c in frame.columns and reference[c].notna().all() and current[c].notna().all()
    ]
    if not usable:
        raise ValueError("No feature is fully observed in both windows")
    for col in usable:
        value = psi(reference[col].to_numpy(), current[col].to_numpy(), bins=bins)
        report.features[col] = {
            "psi": round(value, 4),
            "status": _status(value),
            "reference_mean": round(float(reference[col].mean()), 4),
            "current_mean": round(float(current[col].mean()), 4),
        }
    report.max_psi = max(f["psi"] for f in report.features.values())
    report.status = _status(report.max_psi)
    return report
