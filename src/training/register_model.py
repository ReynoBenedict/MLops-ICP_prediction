from __future__ import annotations
from config.settings import MLFLOW_TRACKING_URI, MODEL_NAME
# src/training/register_model.py


import logging
import sys
import warnings

import mlflow
from mlflow import MlflowClient

# Filter warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")

logging.getLogger("mlflow").setLevel(logging.ERROR)



EXPERIMENT_NAME = "icp-price-prediction"

def get_best_run(client: MlflowClient, experiment_id: str) -> mlflow.entities.Run | None:
    """Find the best run based on RMSE."""
    runs = client.search_runs(
        experiment_ids=[experiment_id],
        filter_string="",
        run_view_type=mlflow.entities.ViewType.ACTIVE_ONLY,
        max_results=1,
        order_by=["metrics.rmse ASC"]
    )
    return runs[0] if runs else None

def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    
    print(f"[INFO] Initializing Model Registration: {MODEL_NAME}")
    print(f"[INFO] Tracking URI: {MLFLOW_TRACKING_URI}")
    
    # 1. Get Experiment
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    if not experiment:
        print(f"[ERROR] Experiment '{EXPERIMENT_NAME}' not found.")
        sys.exit(1)
        
    # 2. Find Best Run
    best_run = get_best_run(client, experiment.experiment_id)
    if not best_run:
        print("[ERROR] No runs found in experiment.")
        sys.exit(1)
        
    run_id = best_run.info.run_id
    rmse = best_run.data.metrics.get("rmse", float('inf'))
    model_type = best_run.data.params.get("model_type", "unknown")
    
    print(f"[INFO] Best Run Found: {run_id}")
    print(f"[INFO] Model Type: {model_type} | RMSE: {rmse:.4f}")
    
    # 3. Register Model
    print(f"[STEP] Registering Run {run_id} to Model Registry...")
    
    # Construct the model URI
    # Note: Artifact path is usually "model" as defined in train.py
    model_uri = f"runs:/{run_id}/model"
    
    try:
        result = mlflow.register_model(model_uri, MODEL_NAME)
        print(f"[SUCCESS] Registered {MODEL_NAME} version {result.version}")
        
        # Optionally tag the version as a candidate
        client.set_model_version_tag(
            name=MODEL_NAME,
            version=result.version,
            key="candidate",
            value="true"
        )
        
        print(f"[INFO] Version {result.version} is now ready for audit.")
    except Exception as e:
        print(f"[ERROR] Registration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

