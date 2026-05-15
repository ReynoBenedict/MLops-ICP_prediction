import requests
import logging
import os
import warnings
import mlflow
import mlflow.pyfunc
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from config.settings import FEATURE_COLUMNS, MLFLOW_TRACKING_URI, MODEL_NAME, PRODUCTION_STAGE


# Configure logging
logger = logging.getLogger("prediction_service")


class InferenceError(Exception):
    """Custom exception for inference-related errors."""

    pass


class PredictionService:
    def __init__(
        self, tracking_uri: str = MLFLOW_TRACKING_URI, model_name: str = MODEL_NAME, stage: str = PRODUCTION_STAGE
    ):
        self.tracking_uri = tracking_uri
        self.model_name = model_name
        self.stage = stage
        self.model = None
        self.feature_names = None
        self.last_payload = None
        self.model_version = "Unknown"
        self.load_error = None
        self._client = None

    def check_mlflow_health(self, timeout: int = 2) -> bool:
        """Fast check if MLflow tracking server is reachable."""
        try:
            health_url = f"{self.tracking_uri}/health"
            response = requests.get(health_url, timeout=timeout)
            return response.status_code == 200
        except Exception:
            return False

    @property
    def client(self):
        if self._client is None:
            if not self.check_mlflow_health():
                raise InferenceError(f"MLflow tracking server at {self.tracking_uri} is unreachable.")
            
            try:
                mlflow.set_tracking_uri(self.tracking_uri)
                self._client = MlflowClient()
            except Exception as e:
                logger.error(f"MlflowClient initialization failed: {str(e)}")
                raise
        return self._client

    def _extract_feature_names(self):
        """Extract feature names from model signature or use defaults."""
        try:
            if hasattr(self.model.metadata, "signature") and self.model.metadata.signature:
                inputs = self.model.metadata.signature.inputs
                if hasattr(inputs, "input_names"):
                    names = inputs.input_names()
                elif hasattr(inputs, "column_names"):
                    names = inputs.column_names()
                else:
                    names = [col.name for col in inputs]
                return names
            logger.warning("No signature found in model metadata, using defaults")
            return FEATURE_COLUMNS
        except Exception as e:
            logger.warning(f"Signature extraction failed: {str(e)}, using defaults")
            return FEATURE_COLUMNS

    def _resolve_artifact_path(self):
        """Resolve the actual artifact path from MLflow registry."""
        try:
            versions = self.client.get_latest_versions(self.model_name, stages=[self.stage])

            if not versions:
                logger.error(f"No model versions found for {self.model_name} in stage {self.stage}")
                return None

            version = versions[0]

            # Check if source path exists
            if version.source and version.source.startswith("/"):
                if os.path.exists(version.source):
                    return version.source
                else:
                    logger.warning(f"Artifact path does not exist: {version.source}")

            return version.source
        except Exception as e:
            logger.error(f"Failed to resolve artifact path: {str(e)}")
            return None

    def _load_model(self):
        """Load model from MLflow with comprehensive error handling."""
        if self.model is not None:
            return

        model_uri = f"models:/{self.model_name}/{self.stage}"

        try:
            if not self.check_mlflow_health():
                raise InferenceError("Cannot load model: MLflow tracking server is offline.")

            self.model = mlflow.pyfunc.load_model(model_uri)

            # Get model version info
            latest_versions = self.client.get_latest_versions(self.model_name, stages=[self.stage])
            if latest_versions:
                self.model_version = str(latest_versions[0].version)

            # Extract feature names
            self.feature_names = self._extract_feature_names()

        except mlflow.exceptions.MlflowException as e:
            error_msg = f"MLflow error during load: {str(e)}"
            logger.error(error_msg)
            self.load_error = error_msg
            raise InferenceError(error_msg)
        except Exception as e:
            error_msg = f"Failed to load model: {str(e)}"
            logger.error(error_msg)
            self.load_error = error_msg
            raise InferenceError(error_msg)

    def predict(self, features_dict: dict) -> float:
        """Make a prediction with safe error handling."""
        if not features_dict:
            logger.error("Empty features dictionary provided")
            raise InferenceError("No features provided for prediction")

        try:
            self._load_model()
        except InferenceError as e:
            logger.error(f"Model loading failed: {str(e)}")
            raise

        self.last_payload = features_dict
        check_features = self.feature_names or FEATURE_COLUMNS

        # Check for missing features
        missing = [f for f in check_features if f not in features_dict]
        if missing:
            error_msg = f"Missing features: {missing}"
            logger.error(error_msg)
            raise InferenceError(error_msg)

        try:
            input_df = pd.DataFrame([features_dict])[check_features]

            prediction = self.model.predict(input_df)
            result = float(prediction[0])
            return result
        except Exception as e:
            error_msg = f"Inference failed: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Input features: {features_dict}")
            raise InferenceError(error_msg)

    def get_model_metadata(self):
        """Get model metadata with safe error handling."""
        try:
            self._load_model()
            meta = {
                "model_name": self.model_name,
                "version": self.model_version,
                "stage": self.stage,
                "flavor": list(self.model.metadata.flavors.keys()) if self.model.metadata.flavors else [],
                "features": self.feature_names or FEATURE_COLUMNS,
            }

            # Try to extract sklearn-specific metadata
            if "sklearn" in meta["flavor"]:
                try:
                    model_uri = f"models:/{self.model_name}/{self.stage}"
                    sk_model = mlflow.sklearn.load_model(model_uri)
                    if hasattr(sk_model, "coef_"):
                        meta["coefficients"] = dict(zip(self.feature_names or FEATURE_COLUMNS, sk_model.coef_.tolist()))
                    if hasattr(sk_model, "intercept_"):
                        meta["intercept"] = float(sk_model.intercept_)
                except Exception as e:
                    logger.warning(f"Failed to extract sklearn metadata: {str(e)}")
                    meta["internals_error"] = str(e)

            return meta
        except Exception as e:
            logger.error(f"Failed to get model metadata: {str(e)}")
            return {"error": str(e), "model_name": self.model_name}

    def get_feature_sensitivity(self, base_features: dict, delta: float = 10.0):
        """Calculate feature sensitivity with error handling."""
        if not base_features or self.model is None:
            logger.warning("Cannot calculate sensitivity: missing features or model")
            return {}

        results = {}
        try:
            baseline = self.predict(base_features)
            for feat in self.feature_names or FEATURE_COLUMNS:
                try:
                    test_payload = base_features.copy()
                    test_payload[feat] += delta
                    new_pred = self.predict(test_payload)
                    results[feat] = {"impact": round(new_pred - baseline, 4)}
                except Exception as e:
                    logger.warning(f"Failed to calculate sensitivity for {feat}: {str(e)}")
                    results[feat] = {"impact": 0.0, "error": str(e)}
        except Exception as e:
            logger.error(f"Feature sensitivity calculation failed: {str(e)}")

        return results



# Singleton instance
_service_instance = None


import streamlit as st

@st.cache_resource
def get_prediction_service() -> PredictionService:

    global _service_instance
    if _service_instance is None:
        _service_instance = PredictionService()
    return _service_instance
