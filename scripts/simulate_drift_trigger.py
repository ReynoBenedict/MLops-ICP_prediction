"""
simulate_drift_trigger.py
=========================
LK-12 — Scenario B: Data-drift simulation helper.

PURPOSE
-------
Simulates a feature distribution shift (data drift) event on the
ICP price prediction dataset. In a real system this script would be
called by a scheduled drift-detector job or a Prometheus alert.

Here it:
  1. Reads the existing clean_data.csv.
  2. Applies a controlled mean shift to lag_* and rolling_* features
     to simulate a population stability index (PSI) violation.
  3. Saves the shifted dataset to data/processed/clean_data_shifted.csv.
  4. Writes ct_trigger_event.json documenting the drift event.

The shifted dataset is for demonstration only and is NOT fed into
the real training pipeline automatically.  To actually retrain after
drift detection, trigger the CT workflow:

  gh workflow run continuous-training.yml \\
      -f trigger_reason=data_drift

SAFETY
------
* Does NOT overwrite clean_data.csv (original data preserved).
* Does NOT touch serving logic, FastAPI, or MLflow.
* Ruff-clean (E/F/W/I rules, line-length 120).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLEAN_DATA = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"
SHIFTED_DATA = PROJECT_ROOT / "data" / "processed" / "clean_data_shifted.csv"
TRIGGER_EVENT = PROJECT_ROOT / "ct_trigger_event.json"

# ── Drift parameters ──────────────────────────────────────────────────────────
SHIFT_MEAN_DELTA = 8.0       # USD/barrel additive shift on lag features
SHIFT_NOISE_STD = 3.0        # Gaussian noise std to simulate variance increase
DRIFT_THRESHOLD_PSI = 0.20   # Simulated PSI threshold (documented, not computed)
RMSE_THRESHOLD = 10.0        # Performance alert threshold (for cross-reference)


def load_clean_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[ERROR] clean_data.csv not found at: {path}")
        print("        Run 'python -m src.data_processing.prepare_data' first.")
        sys.exit(1)
    df = pd.read_csv(path)
    print(f"[INFO] Loaded {len(df)} rows from {path.name}")
    return df


def apply_drift(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Apply a controlled mean + noise shift to lag/rolling feature columns."""
    shifted = df.copy()
    feature_cols = [
        c for c in df.columns
        if c.startswith("lag_") or c.startswith("rolling_") or c.startswith("wti_")
    ]
    if not feature_cols:
        print("[WARN] No lag_*/rolling_*/wti_* feature columns found — no shift applied.")
        return shifted

    print(f"[INFO] Applying drift to {len(feature_cols)} feature columns: {feature_cols}")
    for col in feature_cols:
        noise = rng.normal(0.0, SHIFT_NOISE_STD, size=len(shifted))
        shifted[col] = shifted[col] + SHIFT_MEAN_DELTA + noise

    return shifted


def write_trigger_event(reason: str, psi_simulated: float) -> None:
    event = {
        "event": "ct_trigger",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "trigger_reason": reason,
        "scenario": "B — Data Drift",
        "drift_metric": "PSI (Population Stability Index)",
        "psi_simulated": psi_simulated,
        "drift_threshold": DRIFT_THRESHOLD_PSI,
        "drift_detected": psi_simulated > DRIFT_THRESHOLD_PSI,
        "rmse_threshold": RMSE_THRESHOLD,
        "action": "Trigger CT workflow with trigger_reason=data_drift",
        "shifted_dataset": str(SHIFTED_DATA),
        "note": (
            "Shifted dataset is for demonstration only. "
            "Real retraining uses original clean_data.csv."
        ),
    }
    with open(TRIGGER_EVENT, "w") as f:
        json.dump(event, f, indent=2)
    print(f"[INFO] ct_trigger_event.json written to: {TRIGGER_EVENT}")
    print(json.dumps(event, indent=2))


def main() -> None:
    print("=" * 60)
    print("  LK-12 — Scenario B: Data Drift Simulation")
    print("=" * 60)

    rng = np.random.default_rng(seed=42)

    df = load_clean_data(CLEAN_DATA)
    shifted_df = apply_drift(df, rng)

    SHIFTED_DATA.parent.mkdir(parents=True, exist_ok=True)
    shifted_df.to_csv(SHIFTED_DATA, index=False)
    print(f"[OK] Shifted dataset saved: {SHIFTED_DATA}")

    # Simulate PSI > threshold to represent detected drift
    psi_simulated = round(float(rng.uniform(0.22, 0.45)), 4)
    print(f"[INFO] Simulated PSI = {psi_simulated} (threshold = {DRIFT_THRESHOLD_PSI})")
    print(f"[INFO] Drift detected: {psi_simulated > DRIFT_THRESHOLD_PSI}")

    write_trigger_event("data_drift", psi_simulated)

    print("-" * 60)
    print("[NEXT] To trigger retraining via GitHub Actions, run:")
    print("       gh workflow run continuous-training.yml \\")
    print("           -f trigger_reason=data_drift")
    print("=" * 60)


if __name__ == "__main__":
    main()
