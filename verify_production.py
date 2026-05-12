import mlflow
import mlflow.sklearn
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
MLFLOW_DB = PROJECT_ROOT / "mlflow.db"
TRACKING_URI = f"sqlite:///{MLFLOW_DB.as_posix()}"

def verify():
    mlflow.set_tracking_uri(TRACKING_URI)
    model_name = "ICP_Price_Model"
    stage = "Production"
    
    print("--- VERIFYING PRODUCTION MODEL ---")
    
    try:
        model_uri = f"models:/{model_name}/{stage}"
        model = mlflow.pyfunc.load_model(model_uri)
        
        # 1. Type
        print(f"Model Type: {type(model)}")
        
        # 2. Signature
        if hasattr(model.metadata, "signature") and model.metadata.signature:
            inputs = model.metadata.signature.inputs
            feature_names = [col.name for col in inputs] if inputs else []
            print(f"Features: {feature_names}")
        else:
            print("Signature: MISSING")
            
        # 3. Inference Test
        # Use a dummy payload with all features
        test_payload = {
            "lag_1": 70.0,
            "lag_3": 70.0,
            "lag_6": 70.0,
            "rolling_mean_3": 70.0,
            "wti_price": 70.0,
            "wti_lag_1": 70.0,
            "wti_rolling_mean_3": 70.0
        }
        # Filter payload to only what the model expects if signature exists
        if feature_names:
            test_payload = {f: test_payload[f] for f in feature_names if f in test_payload}
            
        input_df = pd.DataFrame([test_payload])
        # Force ordering if feature_names exists
        if feature_names:
            input_df = input_df[feature_names]
            
        pred = model.predict(input_df)
        print(f"Prediction (@70.0 all): {pred[0]:.4f}")
        
        # 4. Sensitivity (WTI)
        test_payload_wti = test_payload.copy()
        if "wti_price" in test_payload_wti:
            test_payload_wti["wti_price"] += 10.0
            input_df_wti = pd.DataFrame([test_payload_wti])[feature_names if feature_names else test_payload_wti.keys()]
            pred_wti = model.predict(input_df_wti)
            print(f"Prediction (@80.0 WTI): {pred_wti[0]:.4f}")
            print(f"WTI Impact: {pred_wti[0] - pred[0]:.4f}")
        else:
            print("WTI feature not in model signature. Simulator won't work for WTI.")

    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")

if __name__ == "__main__":
    verify()
