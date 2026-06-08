"""
simulate_drift_trigger.py — LK-12 Skenario B: Simulasi data drift.

Script ini mensimulasikan pergeseran distribusi fitur (feature distribution shift)
pada dataset ICP untuk memicu Continuous Training.

Cara kerja:
  1. Membaca data/processed/clean_data.csv
  2. Menambahkan pergeseran mean (+8 USD/bbl) dan noise Gaussian pada fitur
     lag_*, rolling_*, dan wti_*
  3. Menyimpan data hasil ke data/processed/clean_data_shifted.csv
  4. Menulis ct_trigger_event.json dengan nilai PSI yang disimulasikan

PENTING:
  Dataset yang digeser hanya digunakan sebagai bukti drift untuk memicu CT.
  Pipeline retraining produksi tetap menggunakan clean_data.csv yang valid,
  sehingga data demonstrasi ini tidak merusak model Production.

Untuk memicu retraining setelah drift terdeteksi:
  gh workflow run continuous-training.yml -f trigger_reason=data_drift
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLEAN_DATA = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"
SHIFTED_DATA = PROJECT_ROOT / "data" / "processed" / "clean_data_shifted.csv"
TRIGGER_EVENT = PROJECT_ROOT / "ct_trigger_event.json"

SHIFT_MEAN_DELTA = 8.0      # pergeseran mean dalam USD/bbl
SHIFT_NOISE_STD = 3.0       # standar deviasi noise Gaussian
DRIFT_THRESHOLD_PSI = 0.20  # batas PSI yang dianggap signifikan
RMSE_THRESHOLD = 10.0       # batas RMSE untuk alert performa


def load_clean_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[ERROR] File tidak ditemukan: {path}")
        print("        Jalankan 'python -m src.data_processing.prepare_data' terlebih dahulu.")
        sys.exit(1)
    df = pd.read_csv(path)
    print(f"[INFO] Data dimuat: {len(df)} baris dari {path.name}")
    return df


def apply_drift(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Terapkan pergeseran mean dan noise pada kolom fitur lag/rolling/wti."""
    shifted = df.copy()
    feature_cols = [
        c for c in df.columns
        if c.startswith("lag_") or c.startswith("rolling_") or c.startswith("wti_")
    ]
    if not feature_cols:
        print("[WARN] Kolom fitur lag_*/rolling_*/wti_* tidak ditemukan — tidak ada pergeseran.")
        return shifted

    print(f"[INFO] Menerapkan drift pada {len(feature_cols)} kolom: {feature_cols}")
    for col in feature_cols:
        noise = rng.normal(0.0, SHIFT_NOISE_STD, size=len(shifted))
        shifted[col] = shifted[col] + SHIFT_MEAN_DELTA + noise

    return shifted


def write_trigger_event(psi_simulated: float) -> None:
    event = {
        "event": "ct_trigger",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "trigger_reason": "data_drift",
        "scenario": "B - Data Drift",
        "drift_metric": "PSI (Population Stability Index)",
        "psi_simulated": psi_simulated,
        "drift_threshold": DRIFT_THRESHOLD_PSI,
        "drift_detected": psi_simulated > DRIFT_THRESHOLD_PSI,
        "rmse_threshold": RMSE_THRESHOLD,
        "shifted_dataset": str(SHIFTED_DATA),
        "note": (
            "Dataset yang digeser hanya digunakan sebagai bukti drift. "
            "Pipeline retraining tetap menggunakan clean_data.csv yang valid."
        ),
    }
    with open(TRIGGER_EVENT, "w") as f:
        json.dump(event, f, indent=2)
    print(f"[INFO] ct_trigger_event.json ditulis: {TRIGGER_EVENT}")
    print(json.dumps(event, indent=2))


def main() -> None:
    print("=" * 60)
    print("  LK-12 Skenario B: Simulasi Data Drift")
    print("=" * 60)

    rng = np.random.default_rng(seed=42)

    df = load_clean_data(CLEAN_DATA)
    shifted_df = apply_drift(df, rng)

    SHIFTED_DATA.parent.mkdir(parents=True, exist_ok=True)
    shifted_df.to_csv(SHIFTED_DATA, index=False)
    print(f"[OK] Dataset geser disimpan: {SHIFTED_DATA}")

    psi_simulated = round(float(rng.uniform(0.22, 0.45)), 4)
    print(f"[INFO] PSI simulasi = {psi_simulated} (batas = {DRIFT_THRESHOLD_PSI})")
    print(f"[INFO] Drift terdeteksi: {psi_simulated > DRIFT_THRESHOLD_PSI}")

    write_trigger_event(psi_simulated)

    print("-" * 60)
    print("[NEXT] Untuk memicu retraining, jalankan:")
    print("       gh workflow run continuous-training.yml \\")
    print("           -f trigger_reason=data_drift")
    print("=" * 60)


if __name__ == "__main__":
    main()
