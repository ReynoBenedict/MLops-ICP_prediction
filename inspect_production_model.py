import mlflow
import mlflow.sklearn
import pandas as pd
import json

import warnings
from config.settings import MLFLOW_TRACKING_URI, MODEL_NAME, PROJECT_ROOT

warnings.filterwarnings("ignore")

def inspect():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_name = MODEL_NAME
    stage = "Production"
    
    print("--- FINAL MODEL VALIDATION REPORT ---")
    print(f"Target: {model_name} [{stage}]")
    
    try:
        model_uri = f"models:/{model_name}/{stage}"
        
        # Load as pyfunc for inference testing
        pyfunc_model = mlflow.pyfunc.load_model(model_uri)
        
        # Load as sklearn for coefficient analysis
        sklearn_model = mlflow.sklearn.load_model(model_uri)
        
        # 1. Inspect Signature
        if hasattr(pyfunc_model.metadata, "signature") and pyfunc_model.metadata.signature:
            inputs = pyfunc_model.metadata.signature.inputs
            feature_names = inputs.input_names() if hasattr(inputs, "input_names") else [col.name for col in inputs]
            print(f"\n[1] MLflow Signature Features: {feature_names}")
        else:
            print("\n[1] MLflow Signature: MISSING (Fallback used)")
            # Use the fallback features from prediction_service if signature is missing
            feature_names = ["lag_1", "lag_3", "lag_6", "rolling_mean_3", "wti_price", "wti_lag_1", "wti_rolling_mean_3"]

        # 2. Inspect Underlying Model
        print(f"[2] Model Type: {type(sklearn_model)}")
        
        if hasattr(sklearn_model, "coef_"):
            coefs = dict(zip(feature_names, sklearn_model.coef_.tolist()))
            print("[3] Coefficients Analysis:")
            for feat, val in coefs.items():
                print(f"    - {feat:20}: {val:+.6f}")
            
            intercept = getattr(sklearn_model, "intercept_", 0.0)
            print(f"    - Intercept           : {intercept:+.6f}")
        else:
            print("[3] Model coefficients not available via .coef_")

        # 4. Sensitivity Test
        base_val = 70.0
        base_payload = {f: base_val for f in feature_names}
        
        baseline_pred = pyfunc_model.predict(pd.DataFrame([base_payload])[feature_names])[0]
        print("\n[4] Feature Sensitivity Test (+10.0 delta):")
        print(f"    Baseline Prediction (all @ {base_val}): ${baseline_pred:.4f}")
        
        sensitivity_results = []
        for feat in feature_names:
            test_payload = base_payload.copy()
            test_payload[feat] += 10.0
            new_pred = pyfunc_model.predict(pd.DataFrame([test_payload])[feature_names])[0]
            delta = new_pred - baseline_pred
            sensitivity_results.append({
                "feature": feat,
                "delta": round(delta, 4),
                "pct": round((delta/baseline_pred)*100, 2) if baseline_pred !=0 else 0
            })
            print(f"    - {feat:20}: Impact = {delta:+.4f} ({sensitivity_results[-1]['pct']:+.2f}%)")

        # Save report for AI audit
        report = {
            "model_type": str(type(sklearn_model)),
            "features": feature_names,
            "coefficients": coefs if hasattr(sklearn_model, "coef_") else None,
            "sensitivity": sensitivity_results
        }
        with open(PROJECT_ROOT / "final_engineering_validation.json", "w") as f:
            json.dump(report, f, indent=2)

    except Exception as e:
        print(f"\n[ERROR] Validation failed: {str(e)}")

if __name__ == "__main__":
    inspect()
