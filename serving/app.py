"""
FastAPI Model Serving — ICP Price Prediction
Loads model from MLflow Registry at startup (once).
Model URI: models:/ICP_Price_Model/Production
"""

import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import mlflow
import mlflow.pyfunc
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MODEL_NAME", "ICP_Price_Model")
MODEL_STAGE = os.getenv("MODEL_STAGE", "Production")
MODEL_URI = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
STARTUP_RETRIES = int(os.getenv("STARTUP_RETRIES", "3"))
STARTUP_RETRY_DELAY = int(os.getenv("STARTUP_RETRY_DELAY", "10"))

DEFAULT_FEATURES = [
    "lag_1", "lag_3", "lag_6",
    "rolling_mean_3",
    "wti_price", "wti_lag_1", "wti_rolling_mean_3",
]

logger = logging.getLogger("serving")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")

# ---------------------------------------------------------------------------
# Global state (loaded once)
# ---------------------------------------------------------------------------
_model = None
_feature_names: List[str] = []
_model_version: str = "unknown"
_start_time: float = 0.0


def _extract_features(model) -> List[str]:
    """Try to infer feature names from MLflow model signature."""
    try:
        sig = model.metadata.signature
        if sig and sig.inputs:
            inputs = sig.inputs
            if hasattr(inputs, "input_names"):
                return inputs.input_names()
            if hasattr(inputs, "column_names"):
                return inputs.column_names()
            return [col.name for col in inputs]
    except Exception as exc:
        logger.warning("Could not extract features from signature: %s", exc)
    return DEFAULT_FEATURES


def _load_model():
    """Load Production model from MLflow Registry with retry logic."""
    global _model, _feature_names, _model_version, _start_time

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    logger.info("MLflow tracking URI: %s", MLFLOW_TRACKING_URI)
    logger.info("Loading model from: %s", MODEL_URI)

    last_exc = None
    for attempt in range(1, STARTUP_RETRIES + 1):
        try:
            logger.info("Model load attempt %d/%d", attempt, STARTUP_RETRIES)
            _model = mlflow.pyfunc.load_model(MODEL_URI)
            _feature_names = _extract_features(_model)
            logger.info("Feature names: %s", _feature_names)

            # Resolve version number
            try:
                client = mlflow.MlflowClient()
                versions = client.get_latest_versions(MODEL_NAME, stages=[MODEL_STAGE])
                if versions:
                    _model_version = str(versions[0].version)
            except Exception as exc:
                logger.warning("Could not resolve model version: %s", exc)

            _start_time = time.time()
            logger.info("Model loaded successfully — version %s", _model_version)
            return  # success
        except Exception as exc:
            last_exc = exc
            logger.error(
                "Model load attempt %d/%d failed: %s: %s",
                attempt, STARTUP_RETRIES, type(exc).__name__, exc,
            )
            if attempt < STARTUP_RETRIES:
                delay = STARTUP_RETRY_DELAY * attempt
                logger.info("Retrying in %d seconds...", delay)
                time.sleep(delay)

    # All retries exhausted
    logger.critical(
        "Failed to load model after %d attempts. Last error: %s: %s",
        STARTUP_RETRIES, type(last_exc).__name__, last_exc,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Lifespan: load model once at startup
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    yield


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="ICP Price Model API",
    description="Inference endpoint for ICP_Price_Model served from MLflow Registry",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------
class PredictRequest(BaseModel):
    """Input features for ICP price prediction."""
    lag_1: float = Field(..., description="ICP price lag 1 month")
    lag_3: float = Field(..., description="ICP price lag 3 months")
    lag_6: float = Field(..., description="ICP price lag 6 months")
    rolling_mean_3: float = Field(..., description="3-month rolling mean of ICP")
    wti_price: float = Field(..., description="Current WTI price")
    wti_lag_1: float = Field(..., description="WTI price lag 1 month")
    wti_rolling_mean_3: float = Field(..., description="3-month rolling mean of WTI")

    model_config = {"json_schema_extra": {
        "examples": [{
            "lag_1": 70.0, "lag_3": 68.0, "lag_6": 65.0,
            "rolling_mean_3": 69.0,
            "wti_price": 72.0, "wti_lag_1": 71.0, "wti_rolling_mean_3": 71.5,
        }]
    }}


class PredictResponse(BaseModel):
    prediction: float = Field(..., description="Predicted ICP price (USD/barrel)")
    model_name: str
    model_version: str
    features_used: List[str]


class HealthResponse(BaseModel):
    status: str
    model_name: str
    model_version: str
    model_uri: str
    uptime_seconds: float
    features: List[str]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health():
    """Health check — confirms model is loaded and serving."""
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return HealthResponse(
        status="healthy",
        model_name=MODEL_NAME,
        model_version=_model_version,
        model_uri=MODEL_URI,
        uptime_seconds=round(time.time() - _start_time, 2),
        features=_feature_names,
    )


@app.post("/predict", response_model=PredictResponse, tags=["Prediction"])
async def predict(request: PredictRequest):
    """Run inference using Production model from MLflow Registry."""
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        features_dict = request.model_dump()
        input_df = pd.DataFrame([features_dict])[_feature_names]
        raw = _model.predict(input_df)
        prediction = float(raw[0])
    except Exception as exc:
        logger.error("Prediction failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Prediction error: {exc}")

    return PredictResponse(
        prediction=prediction,
        model_name=MODEL_NAME,
        model_version=_model_version,
        features_used=_feature_names,
    )
