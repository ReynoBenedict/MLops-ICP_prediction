# LK-07: Registrasi model ke MLflow Model Registry (Metric-Driven)
from __future__ import annotations

import logging
import math
import sys
import warnings
from pathlib import Path
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Filter warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")

logging.getLogger("mlflow").setLevel(logging.ERROR)
logging.getLogger("mlflow.sklearn").setLevel(logging.ERROR)

# Path configuration
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEAN_CSV    = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"
MLFLOW_DB           = PROJECT_ROOT / "mlflow.db"
MLFLOW_TRACKING_URI = f"sqlite:///{MLFLOW_DB.as_posix()}"
EXPERIMENT_NAME     = "icp-price-prediction"
MODEL_NAME          = "ICP_Price_Model"

CANDIDATE_TARGET_COLS = ["icp_price", "icp", "price", "harga"]

def detect_target_column(df: pd.DataFrame) -> str:
    """Detect the target column from a list of candidates."""
    for col in CANDIDATE_TARGET_COLS:
        if col in df.columns:
            return col
    raise ValueError(f"Target column not found. Available: {list(df.columns)}")

def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, list[str]]:
    """Load data and split into features and target."""
    if not CLEAN_CSV.exists():
        print(f"[ERROR] {CLEAN_CSV} not found.")
        sys.exit(1)
    
    df = pd.read_csv(CLEAN_CSV)
    target_col = detect_target_column(df)
    
    # Dynamically identify all feature columns
    feature_cols = [
        c for c in df.columns 
        if c.startswith("lag_") or c.startswith("rolling_") or c.startswith("wti_")
    ]
    
    if not feature_cols:
        print("[ERROR] No feature columns found. Ensure ingestion pipeline is correct.")
        sys.exit(1)
        
    print(f"[INFO] Features identified: {feature_cols}")
    
    X = df[feature_cols]
    y = df[target_col]
    
    split_idx = int(len(df) * 0.8)
    return X.iloc[:split_idx], X.iloc[split_idx:], y.iloc[:split_idx], y.iloc[split_idx:], feature_cols

def train_and_log(
    model: Any,
    model_type: str,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
    params: dict | None = None
) -> tuple[str, float, float]:
    """Train a model and log it to MLflow. Returns run_id and metrics."""
    params = params or {}
    
    with mlflow.start_run(run_name=f"Registry_Candidate_{model_type}") as run:
        mlflow.log_param("model_type", model_type)
        for k, v in params.items():
            mlflow.log_param(k, v)
            
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        
        rmse = math.sqrt(mean_squared_error(y_test, y_pred))
        mae = float(mean_absolute_error(y_test, y_pred))
        
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        
        signature = infer_signature(X_test, y_pred)
        
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=signature,
            input_example=X_test.iloc[:1],
            registered_model_name=MODEL_NAME
        )
        
        print(f"[{model_type:18}] RMSE: {rmse:.4f} | MAE: {mae:.4f}")
        return run.info.run_id, rmse, mae

def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient()
    
    print(f"[INFO] Initializing Model Registry Flow: {MODEL_NAME}")
    print("-" * 60)
    
    X_train, X_test, y_train, y_test, features = load_data()
    print("-" * 60)
    
    # Define candidate models
    candidates = [
        {
            "type": "LinearRegression",
            "model": LinearRegression(),
            "params": {}
        },
        {
            "type": "RandomForest_n50",
            "model": RandomForestRegressor(n_estimators=50, random_state=42),
            "params": {"n_estimators": 50}
        },
        {
            "type": "RandomForest_n100",
            "model": RandomForestRegressor(n_estimators=100, random_state=42),
            "params": {"n_estimators": 100}
        }
    ]
    
    best_run_id = None
    min_rmse = float('inf')
    
    print("[STEP] Training and evaluating candidates...")
    for cand in candidates:
        run_id, rmse, mae = train_and_log(
            cand["model"], cand["type"], X_train, X_test, y_train, y_test, cand["params"]
        )
        if rmse < min_rmse:
            min_rmse = rmse
            best_run_id = run_id
            best_type = cand["type"]

    print("-" * 60)
    print(f"[BEST] Champion Model: {best_type} (RMSE: {min_rmse:.4f})")
    
    # --- MODEL PROMOTION LOGIC ---
    print(f"[STEP] Promoting Run {best_run_id} to Production...")
    
    # Find the version associated with this run
    filter_string = f"run_id='{best_run_id}'"
    versions = client.search_model_versions(f"name='{MODEL_NAME}' and run_id='{best_run_id}'")
    
    if not versions:
        print("[ERROR] Could not find registered version for the best run.")
        return
        
    best_version = versions[0].version
    
    # Transition the best version to Production and archive old ones
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=best_version,
        stage="Production",
        archive_existing_versions=True
    )
    
    print(f"[SUCCESS] Version {best_version} ({best_type}) is now in Production.")
    print("-" * 60)

if __name__ == "__main__":
    main()
