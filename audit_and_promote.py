from config.settings import MLFLOW_TRACKING_URI, MODEL_NAME
# audit_and_promote.py
import mlflow
from mlflow import MlflowClient
import warnings

warnings.filterwarnings("ignore")


def get_model_rmse(client: MlflowClient, run_id: str) -> float:
    """Retrieve RMSE metric for a specific run."""
    run = client.get_run(run_id)
    return run.data.metrics.get("rmse", float('inf'))

def audit_and_promote():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    
    print("=" * 60)
    print("MLFLOW MODEL PROMOTION AUDIT")
    print("=" * 60)
    print(f"Model Name  : {MODEL_NAME}")
    print(f"Tracking URI: {MLFLOW_TRACKING_URI}")
    print("-" * 60)
    
    # 1. Get the Candidate Version (Latest Version)
    try:
        all_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        if not all_versions:
            print("[ERROR] No registered versions found for this model.")
            return
        
        # Latest version is the candidate
        candidate_v = sorted(all_versions, key=lambda x: int(x.version), reverse=True)[0]
        candidate_run_id = candidate_v.run_id
        candidate_rmse = get_model_rmse(client, candidate_run_id)
        
        print(f"Candidate Version: v{candidate_v.version}")
        print(f"Candidate Run ID : {candidate_run_id}")
        print(f"Candidate RMSE   : {candidate_rmse:.4f}")
    except Exception as e:
        print(f"[ERROR] Could not fetch candidate version: {e}")
        return

    # 2. Get the Current Production Version
    prod_v = None
    prod_rmse = float('inf')
    
    try:
        latest_prod = client.get_latest_versions(MODEL_NAME, stages=["Production"])
        if latest_prod:
            prod_v = latest_prod[0]
            prod_run_id = prod_v.run_id
            prod_rmse = get_model_rmse(client, prod_run_id)
            print(f"Current Production: v{prod_v.version} (RMSE: {prod_rmse:.4f})")
        else:
            print("Current Production: None (First deployment)")
    except Exception as e:
        print(f"[INFO] Production check failed (might be first run): {e}")

    # 3. Comparison Logic
    print("-" * 60)
    if candidate_rmse < prod_rmse:
        print(f"[DECISION] Candidate v{candidate_v.version} is BETTER than Production (or first run).")
        print(f"[ACTION] Promoting v{candidate_v.version} to Production...")
        
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=candidate_v.version,
            stage="Production",
            archive_existing_versions=True
        )
        print("[SUCCESS] Promotion complete.")
    else:
        print(f"[DECISION] Candidate v{candidate_v.version} is NOT better than Production.")
        print(f"[ACTION] Keeping v{prod_v.version} in Production.")
    
    print("=" * 60)

if __name__ == "__main__":
    audit_and_promote()

