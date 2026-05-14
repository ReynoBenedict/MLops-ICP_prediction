# src/training/infer.py
from __future__ import annotations

import logging
import sys
import warnings
from pathlib import Path

import mlflow.pyfunc
import pandas as pd
from mlflow import MlflowClient

# Filter warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")

logging.getLogger("mlflow").setLevel(logging.ERROR)

from config.settings import MLFLOW_TRACKING_URI, MODEL_NAME, CLEAN_DATA_PATH, FEATURE_COLUMNS

MODEL_URI = f"models:/{MODEL_NAME}/Production"

def load_latest_features() -> pd.DataFrame:
    """Load the latest row from clean_data.csv for inference."""
    if not CLEAN_DATA_PATH.exists():
        print(f"[ERROR] Processed data not found at {CLEAN_DATA_PATH}")
        sys.exit(1)
        
    df = pd.read_csv(CLEAN_DATA_PATH)
    
    # Selection of features defined in settings.py
    # This ensures alignment between training and inference
    try:
        latest_row = df[FEATURE_COLUMNS].iloc[[-1]]
        return latest_row
    except KeyError as e:
        print(f"[ERROR] Missing feature in data: {e}")
        print(f"Expected features: {FEATURE_COLUMNS}")
        sys.exit(1)

def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    print("=" * 60)
    print("ICP PRICE MODEL - PRODUCTION INFERENCE")
    print("=" * 60)
    
    # 1. Check Production Version
    try:
        prod_versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
        if not prod_versions:
            print(f"[ERROR] No Production model found for '{MODEL_NAME}'")
            return
        version = prod_versions[0].version
        print(f"  Model      : {MODEL_NAME} (v{version})")
        print(f"  Stage      : Production")
        print(f"  URI        : {MODEL_URI}")
    except Exception as e:
        print(f"[ERROR] Registry lookup failed: {e}")
        return

    # 2. Load Features
    print("-" * 60)
    print("[STEP] Loading latest features from processed data...")
    input_data = load_latest_features()
    
    # 3. Predict
    print("[STEP] Executing prediction...")
    try:
        # Load the production model
        model = mlflow.pyfunc.load_model(MODEL_URI)
        
        # MLflow pyfunc predict expects a DataFrame
        prediction = model.predict(input_data)
        
        print("-" * 60)
        print("INFERENCE DATA (Latest Sample):")
        for col in FEATURE_COLUMNS:
            print(f"  {col:20}: {input_data[col].iloc[0]:.4f}")
            
        print("-" * 60)
        print(f"  >>> Predicted ICP Price: ${prediction[0]:.4f} / barrel")
    except Exception as e:
        print(f"[ERROR] Inference failed: {e}")
        
    print("=" * 60)

if __name__ == "__main__":
    main()

