# =============================================================
# Dockerfile — ICP Price Prediction MLOps Pipeline
# Model  : RandomForest (scikit-learn)
# Tracker: MLflow + SQLite backend (mlflow.db)
# Registry: ICP_Price_Model (Production = n_estimators=100)
# =============================================================

FROM python:3.10-slim

# Metadata 
LABEL maintainer="MLops-ICP_prediction"
LABEL description="ICP Price Prediction pipeline: prepare → train → register → infer"
LABEL version="1.0"

# Environment 
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # MLflow SQLite backend (path dalam container)
    MLFLOW_TRACKING_URI=sqlite:////app/mlflow.db \
    # Suppress MLflow & sklearn verbose logs
    MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING=false

# System dependencies 
# build-essential: dibutuhkan oleh beberapa paket scipy/scikit-learn
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Work directory 
WORKDIR /app

# Python dependencies 
# Copy requirements lebih dulu agar Docker cache layer ini selama
# requirements.txt tidak berubah (tidak perlu install ulang tiap build).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code 
# File-file yang dikecualikan diatur di .dockerignore
COPY prepare_data.py train.py register_model.py infer.py model_registry.yaml ./
COPY data/   ./data/
COPY configs/ ./configs/

# Buat direktori runtime 
RUN mkdir -p /app/mlruns /app/reports /app/models

# Entry point 
CMD ["python", "infer.py"]
