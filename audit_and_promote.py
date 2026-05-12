import mlflow
from mlflow import MlflowClient
import pandas as pd
from pathlib import Path
import json
import warnings

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent
MLFLOW_DB = PROJECT_ROOT / "mlflow.db"
TRACKING_URI = f"sqlite:///{MLFLOW_DB.as_posix()}"
MODEL_NAME = "ICP_Price_Model"

def audit_and_promote():
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient()
    
    print("--- MLFLOW REGISTRY AUDIT ---")
    
    # 1. Check current versions
    try:
        versions = client.get_latest_versions(MODEL_NAME)
        for v in versions:
            print(f"Version {v.version} | Stage: {v.current_stage:10} | Run ID: {v.run_id}")
    except Exception as e:
        print(f"[ERROR] Could not fetch latest versions: {e}")
        return

    # 2. Find the best LinearRegression run
    print("\n--- SEARCHING FOR BEST LinearRegression RUN ---")
    experiment = client.get_experiment_by_name("icp-price-prediction")
    if not experiment:
        print("[ERROR] Experiment 'icp-price-prediction' not found.")
        return
        
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="params.model_type = 'LinearRegression'",
        order_by=["metrics.rmse ASC"]
    )
    
    if not runs:
        print("[ERROR] No LinearRegression runs found.")
        return
        
    best_run = runs[0]
    best_run_id = best_run.info.run_id
    best_rmse = best_run.data.metrics['rmse']
    print(f"Found Best LinearRegression Run: {best_run_id}")
    print(f"Best RMSE: {best_rmse:.4f}")

    # 3. Check if this run is already registered
    registered_version = None
    all_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    for v in all_versions:
        if v.run_id == best_run_id:
            registered_version = v.version
            break
            
    if not registered_version:
        print(f"\n[STEP] Registering Best Run ({best_run_id}) as new version...")
        mv = client.create_model_version(MODEL_NAME, best_run.info.artifact_uri + "/model", best_run_id)
        registered_version = mv.version
        print(f"New Version: {registered_version}")
    else:
        print(f"\n[INFO] Best Run already registered as Version {registered_version}")

    # 4. Promote to Production
    print(f"\n[STEP] Transitioning Version {registered_version} to Production...")
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=registered_version,
        stage="Production",
        archive_existing_versions=True
    )
    print("[SUCCESS] Production model updated.")

    # 5. Final Validation
    print("\n--- FINAL PRODUCTION STATE ---")
    prod_v = client.get_latest_versions(MODEL_NAME, stages=["Production"])[0]
    print(f"Production Model Version: {prod_v.version}")
    print(f"Production Run ID: {prod_v.run_id}")
    
    # Check signature of this run
    run = client.get_run(prod_v.run_id)
    params = run.data.params
    print(f"Model Type: {params.get('model_type')}")
    
    # Save the promotion proof to a file for the report
    proof = {
        "production_version": prod_v.version,
        "run_id": prod_v.run_id,
        "model_type": params.get('model_type'),
        "rmse": run.data.metrics['rmse'],
        "mae": run.data.metrics['mae']
    }
    with open(PROJECT_ROOT / "registry_promotion_proof.json", "w") as f:
        json.dump(proof, f, indent=2)

if __name__ == "__main__":
    audit_and_promote()
