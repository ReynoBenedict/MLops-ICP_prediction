# LK-07: Inferensi menggunakan model Production dari MLflow Model Registry
from __future__ import annotations

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")

import logging
logging.getLogger("mlflow").setLevel(logging.ERROR)

from pathlib import Path
import pandas as pd
import mlflow.pyfunc
from mlflow import MlflowClient

PROJECT_ROOT        = Path(__file__).resolve().parents[2]
MLFLOW_DB           = PROJECT_ROOT / "mlflow.db"
MLFLOW_TRACKING_URI = f"sqlite:///{MLFLOW_DB.as_posix()}"
MODEL_NAME          = "ICP_Price_Model"
MODEL_URI           = f"models:/{MODEL_NAME}/Production"

# Input: nilai lag_1 (harga ICP bulan sebelumnya dalam USD/barrel)
SAMPLE_INPUT = pd.DataFrame({"lag_1": [72.5]})


def get_production_version(client: MlflowClient) -> str:
    """Kembalikan nomor versi yang berstage Production."""
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    for v in versions:
        if v.current_stage == "Production":
            return v.version
    return "unknown"


def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    prod_version = get_production_version(client)

    print("=" * 50)
    print("[INFO] ICP Price Model - Inference")
    print("=" * 50)
    print(f"  Model     : {MODEL_NAME}")
    print(f"  Version   : v{prod_version}")
    print(f"  Stage     : Production")
    print(f"  URI       : {MODEL_URI}")
    print("-" * 50)

    model = mlflow.pyfunc.load_model(MODEL_URI)

    lag_1_value = SAMPLE_INPUT["lag_1"].iloc[0]
    prediction  = model.predict(SAMPLE_INPUT)

    print(f"  Input     : lag_1 = {lag_1_value} USD/barrel")
    print(f"  Prediction: {prediction[0]:.4f} USD/barrel")
    print("=" * 50)


if __name__ == "__main__":
    main()
