"""register_model_ci.py — Lightweight MLflow registry simulation for CI.

Real MLflow SQLite registry is not accessible in GitHub Actions (no mlflow.db).
This script simulates the Staging registration by writing a structured log file,
which is safe, reproducible, and evidenceable in CI logs.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

METRICS_PATH = Path(__file__).resolve().parents[2] / "metrics.json"
REGISTRY_LOG = Path(__file__).resolve().parents[2] / "reports" / "registry_log.json"

if not METRICS_PATH.exists():
    print("[ERROR] metrics.json not found. Cannot register.")
    sys.exit(1)

with open(METRICS_PATH) as f:
    metrics = json.load(f)

REGISTRY_LOG.parent.mkdir(parents=True, exist_ok=True)

entry = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "model_name": "ICP_Price_Model",
    "best_model_type": metrics["best_model"],
    "rmse": metrics["rmse"],
    "mae": metrics["mae"],
    "stage": "Staging",
    "registered_by": "mlops-automation-ci",
    "status": "REGISTERED",
}

with open(REGISTRY_LOG, "w") as f:
    json.dump(entry, f, indent=2)

print("[REGISTER] Model registration record:")
for k, v in entry.items():
    print(f"           {k}: {v}")
print(f"[REGISTER] Registry log saved -> {REGISTRY_LOG}")
print("[REGISTER] [OK] ICP_Price_Model promoted to Staging (simulated).")
