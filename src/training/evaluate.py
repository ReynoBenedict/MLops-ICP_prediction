"""evaluate.py — LK-8 RMSE threshold validation for CI pipeline."""
import json
import sys
from pathlib import Path

METRICS_PATH = Path(__file__).resolve().parents[2] / "metrics.json"
RMSE_THRESHOLD = 20.0  # adjust to your dataset's realistic range

if not METRICS_PATH.exists():
    print("[ERROR] metrics.json not found. Run train.py first.")
    sys.exit(1)

with open(METRICS_PATH) as f:
    metrics = json.load(f)

rmse = metrics["rmse"]
model = metrics["best_model"]

print(f"[EVALUATE] Model       : {model}")
print(f"[EVALUATE] RMSE        : {rmse:.4f}")
print(f"[EVALUATE] Threshold   : {RMSE_THRESHOLD}")

if rmse > RMSE_THRESHOLD:
    print(f"[FAIL] RMSE {rmse:.4f} exceeds threshold {RMSE_THRESHOLD}. Pipeline aborted.")
    sys.exit(1)

print(f"[PASS] RMSE {rmse:.4f} is within threshold. Proceeding to registry.")
