# LK-07: Registrasi model ke MLflow Model Registry
from __future__ import annotations

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")

import logging
logging.getLogger("mlflow").setLevel(logging.ERROR)
logging.getLogger("mlflow.sklearn").setLevel(logging.ERROR)

import math
import sys
from pathlib import Path
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from mlflow import MlflowClient

PROJECT_ROOT = Path(__file__).resolve().parent
CLEAN_CSV    = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"
MLFLOW_DB           = PROJECT_ROOT / "mlflow.db"
MLFLOW_TRACKING_URI = f"sqlite:///{MLFLOW_DB.as_posix()}"
EXPERIMENT_NAME     = "icp-price-prediction"
MODEL_NAME          = "ICP_Price_Model"

CANDIDATE_TARGET_COLS = ["icp_price", "icp", "price", "harga"]


def detect_target_column(df: pd.DataFrame) -> str:
    for col in CANDIDATE_TARGET_COLS:
        if col in df.columns:
            return col
    skip = {"month", "year", "bulan", "tahun", "lag_1"}
    candidates = [c for c in df.select_dtypes("number").columns if c.lower() not in skip]
    if candidates:
        return candidates[0]
    raise ValueError(f"Kolom target tidak ditemukan: {list(df.columns)}")


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    if not CLEAN_CSV.exists():
        print("[ERROR] clean_data.csv tidak ditemukan. Jalankan: python prepare_data.py")
        sys.exit(1)
    df = pd.read_csv(CLEAN_CSV)
    target_col = detect_target_column(df)
    X = df[["lag_1"]]
    y = df[target_col]
    split_idx = int(len(df) * 0.8)
    return X.iloc[:split_idx], X.iloc[split_idx:], y.iloc[:split_idx], y.iloc[split_idx:]


def train_and_register(
    n_estimators: int,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
) -> str:
    """Latih model, log ke MLflow, daftarkan ke registry. Return run_id."""
    model = RandomForestRegressor(n_estimators=n_estimators, random_state=42)

    with mlflow.start_run(run_name=f"RF_n{n_estimators}_registry") as run:
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_param("n_estimators", n_estimators)

        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        rmse = math.sqrt(mean_squared_error(y_test, y_pred))
        mae  = float(mean_absolute_error(y_test, y_pred))
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae",  mae)

        signature = infer_signature(X_test, y_pred)

        mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            signature=signature,
            input_example=X_test.iloc[:1],
            registered_model_name=MODEL_NAME,
        )

        print(f"[TRAIN] RF n_estimators={n_estimators}  RMSE={rmse:.4f}  MAE={mae:.4f}")
        return run.info.run_id


def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    print(f"[INFO] Model Registry: {MODEL_NAME}")
    print("-" * 50)

    X_train, X_test, y_train, y_test = load_data()

    # --- Version 1: n_estimators=50 ---
    print("[STEP] Mendaftarkan Version 1 (n_estimators=50) ...")
    train_and_register(50, X_train, X_test, y_train, y_test)

    # --- Version 2: n_estimators=100 ---
    print("[STEP] Mendaftarkan Version 2 (n_estimators=100) ...")
    train_and_register(100, X_train, X_test, y_train, y_test)

    print("-" * 50)

    # Ambil semua versi yang terdaftar
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    versions_sorted = sorted(versions, key=lambda v: int(v.version))

    if len(versions_sorted) < 2:
        print("[WARN] Kurang dari 2 versi ditemukan. Pastikan tidak ada run duplikat yang sudah ada sebelumnya.")

    # Temukan dua versi terbaru dari run yang baru saja dibuat
    v1 = versions_sorted[-2] if len(versions_sorted) >= 2 else versions_sorted[0]
    v2 = versions_sorted[-1]

    v1_num = v1.version
    v2_num = v2.version

    # --- Stage Transition ---
    print(f"[STEP] Transisi Version {v1_num} -> Staging ...")
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=v1_num,
        stage="Staging",
        archive_existing_versions=False,
    )

    print(f"[STEP] Transisi Version {v2_num} -> Production ...")
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=v2_num,
        stage="Production",
        archive_existing_versions=True,  # arsipkan Production lama jika ada
    )

    print("-" * 50)
    print(f"[OK] Version {v1_num}  -> Staging   (n_estimators=50)")
    print(f"[OK] Version {v2_num}  -> Production (n_estimators=100)")
    print(f"[OK] Model '{MODEL_NAME}' berhasil didaftarkan ke MLflow Model Registry.")


if __name__ == "__main__":
    main()
