# Pipeline pelatihan model ICP dengan MLflow tracking
from __future__ import annotations

import math
import sys
import warnings
from pathlib import Path
from typing import Any

# Sembunyikan warning dari mlflow dan sklearn sebelum import
warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")

import logging
logging.getLogger("mlflow.sklearn").setLevel(logging.ERROR)
logging.getLogger("mlflow").setLevel(logging.ERROR)

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib
matplotlib.use("Agg")  # backend non-interaktif agar aman di semua OS
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parent
CLEAN_CSV    = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"

# SQLite backend — aman di Windows, direkomendasikan MLflow 3.x
MLFLOW_DB           = PROJECT_ROOT / "mlflow.db"
MLFLOW_TRACKING_URI = f"sqlite:///{MLFLOW_DB.as_posix()}"
EXPERIMENT_NAME     = "icp-price-prediction"

CANDIDATE_TARGET_COLS = ["icp_price", "icp", "price", "harga"]


def detect_target_column(df: pd.DataFrame) -> str:
    """Deteksi nama kolom target secara otomatis."""
    for col in CANDIDATE_TARGET_COLS:
        if col in df.columns:
            return col
    # Fallback: kolom numerik pertama yang bukan waktu atau lag
    skip = {"month", "year", "bulan", "tahun", "lag_1"}
    candidates = [c for c in df.select_dtypes("number").columns if c.lower() not in skip]
    if candidates:
        return candidates[0]
    raise ValueError(f"Kolom target tidak ditemukan. Kolom tersedia: {list(df.columns)}")


def load_data(
    csv_path: Path = CLEAN_CSV,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, str]:
    """Muat clean_data.csv dan split 80/20 secara kronologis."""
    if not csv_path.exists():
        print(f"[ERROR] File tidak ditemukan: {csv_path}")
        print("        Jalankan 'python prepare_data.py' terlebih dahulu.")
        sys.exit(1)

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        print(f"[ERROR] Gagal membaca {csv_path}: {exc}")
        sys.exit(1)

    print(f"[INFO] Memuat {len(df)} baris dari {csv_path}")

    try:
        target_col = detect_target_column(df)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    if "lag_1" not in df.columns:
        print("[ERROR] Kolom 'lag_1' tidak ada. Jalankan kembali prepare_data.py.")
        sys.exit(1)

    print(f"[INFO] Kolom target: '{target_col}'  |  Fitur: ['lag_1']")

    X = df[["lag_1"]]
    y = df[target_col]

    # Split time-series tanpa shuffle agar urutan terjaga
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print(f"[INFO] Split: {len(X_train)} train / {len(X_test)} test (test_size=0.2)")
    return X_train, X_test, y_train, y_test, target_col


def compute_rmse(y_true: pd.Series, y_pred: Any) -> float:
    """Hitung RMSE."""
    return math.sqrt(mean_squared_error(y_true, y_pred))


def compute_mae(y_true: pd.Series, y_pred: Any) -> float:
    """Hitung MAE."""
    return float(mean_absolute_error(y_true, y_pred))


def save_pred_plot(y_true: pd.Series, y_pred: Any, model_type: str) -> Path:
    """Buat dan simpan plot actual vs predicted sebagai PNG."""
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(y_true.values, marker="o", label="Actual", linewidth=1.5)
    ax.plot(y_pred,        marker="x", label="Predicted", linewidth=1.5, linestyle="--")
    ax.set_title(f"Actual vs Predicted — {model_type}")
    ax.set_xlabel("Test Sample Index")
    ax.set_ylabel("ICP Price")
    ax.legend()
    fig.tight_layout()
    out = PROJECT_ROOT / "reports" / f"pred_{model_type}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=100)
    plt.close(fig)
    return out


def run_experiment(
    model: Any,
    model_type: str,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
    extra_params: dict | None = None,
) -> tuple[float, float]:
    """Jalankan satu run MLflow: log param, latih, evaluasi, simpan model + plot."""
    extra_params = extra_params or {}

    with mlflow.start_run(run_name=model_type):
        mlflow.log_param("model_type", model_type)
        for key, val in extra_params.items():
            mlflow.log_param(key, val)

        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        rmse   = compute_rmse(y_test, y_pred)
        mae    = compute_mae(y_test, y_pred)

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae",  mae)

        mlflow.sklearn.log_model(model, name="model")

        plot_path = save_pred_plot(y_test, y_pred, model_type)
        mlflow.log_artifact(str(plot_path))

        print(f"[RUN]  {model_type:<35}  RMSE = {rmse:.4f}  MAE = {mae:.4f}")

    return rmse, mae


def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    print(f"[INFO] MLflow tracking URI : {MLFLOW_TRACKING_URI}")
    print(f"[INFO] Experiment name     : {EXPERIMENT_NAME}")
    print("-" * 60)

    X_train, X_test, y_train, y_test, target_col = load_data()
    print("-" * 60)

    experiments: list[dict] = [
        {
            "model":        LinearRegression(),
            "model_type":   "LinearRegression",
            "extra_params": {},
        },
        {
            "model":        RandomForestRegressor(n_estimators=10, random_state=42),
            "model_type":   "RandomForest_n10",
            "extra_params": {"n_estimators": 10},
        },
        {
            "model":        RandomForestRegressor(n_estimators=50, random_state=42),
            "model_type":   "RandomForest_n50",
            "extra_params": {"n_estimators": 50},
        },
        {
            "model":        RandomForestRegressor(n_estimators=100, random_state=42),
            "model_type":   "RandomForest_n100",
            "extra_params": {"n_estimators": 100},
        },
    ]

    print(f"[INFO] Menjalankan {len(experiments)} eksperimen...")
    print("-" * 60)

    results: list[tuple[str, float, float]] = []
    for exp in experiments:
        rmse, mae = run_experiment(
            model=exp["model"],
            model_type=exp["model_type"],
            X_train=X_train,
            X_test=X_test,
            y_train=y_train,
            y_test=y_test,
            extra_params=exp["extra_params"],
        )
        results.append((exp["model_type"], rmse, mae))

    print("-" * 60)
    print("[SUMMARY] Hasil semua eksperimen:")
    for name, rmse, mae in results:
        print(f"          {name:<35}  RMSE = {rmse:.4f}  MAE = {mae:.4f}")

    best_name, best_rmse, best_mae = min(results, key=lambda x: x[1])
    print(f"\n[BEST]   Model terbaik : {best_name}  (RMSE = {best_rmse:.4f}  MAE = {best_mae:.4f})")
    print("-" * 60)
    print("[INFO] Untuk membuka MLflow UI:")
    print(f"       mlflow ui --backend-store-uri {MLFLOW_TRACKING_URI}")
    print("       Lalu buka: http://127.0.0.1:5000")
    print(f"[INFO] Database MLflow : {MLFLOW_DB}")


if __name__ == "__main__":
    main()
