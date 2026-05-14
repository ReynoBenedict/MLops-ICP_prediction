import pandas as pd
import mlflow
import mlflow.pyfunc
import mlflow.sklearn
from mlflow import MlflowClient
import logging
import warnings

from config.settings import MLFLOW_TRACKING_URI, MODEL_NAME, PRODUCTION_STAGE, FEATURE_COLUMNS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("prediction_service")

class InferenceError(Exception):
    """Custom exception for inference-related errors."""
    pass

class PredictionService:
    def __init__(self, tracking_uri: str = MLFLOW_TRACKING_URI, model_name: str = MODEL_NAME, stage: str = PRODUCTION_STAGE):
        self.tracking_uri = tracking_uri
        self.model_name = model_name
        self.stage = stage
        self.model = None
        self.feature_names = None
        self.last_payload = None
        self.model_version = "Unknown"
        
        mlflow.set_tracking_uri(self.tracking_uri)
        self.client = MlflowClient()

    def _extract_feature_names(self):
        try:
            if hasattr(self.model.metadata, 'signature') and self.model.metadata.signature:
                inputs = self.model.metadata.signature.inputs
                if hasattr(inputs, 'input_names'):
                    names = inputs.input_names()
                elif hasattr(inputs, 'column_names'):
                    names = inputs.column_names()
                else:
                    names = [col.name for col in inputs]
                return names
            return FEATURE_COLUMNS
        except Exception as e:
            logger.warning(f"Signature extraction failed: {str(e)}")
            return FEATURE_COLUMNS

    def _load_model(self):
        if self.model is not None:
            return
        model_uri = f"models:/{self.model_name}/{self.stage}"
        import os
        logger.info(f"CWD: {os.getcwd()}")
        try:
            logger.info(f"Loading model from: {model_uri}")
            logger.info(f"Tracking URI: {mlflow.get_tracking_uri()}")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                self.model = mlflow.pyfunc.load_model(model_uri)
            logger.info("Model loaded successfully")
            
            latest_versions = self.client.get_latest_versions(self.model_name, stages=[self.stage])
            if latest_versions:
                self.model_version = latest_versions[0].version
                logger.info(f"Model version: {self.model_version}")
            
            self.feature_names = self._extract_feature_names()
            logger.info(f"Feature names: {self.feature_names}")
        except mlflow.exceptions.MlflowException as e:
            logger.error(f"MLflow error during load: {str(e)}")
            # Try to see if we can get the source path
            try:
                v = self.client.get_latest_versions(self.model_name, stages=[self.stage])[0]
                logger.error(f"Model source: {v.source}")
                if v.source.startswith("/") and not os.path.exists(v.source):
                    logger.error(f"CRITICAL: Local path {v.source} does not exist in this container!")
            except:
                pass
            raise InferenceError(f"Model loading failed (MLflow): {str(e)}")
        except Exception as e:
            logger.error(f"Failed to load model: {str(e)}")
            raise InferenceError(f"Model loading failed (Generic): {str(e)}")

    def predict(self, features_dict: dict) -> float:
        self._load_model()
        self.last_payload = features_dict
        check_features = self.feature_names or FEATURE_COLUMNS
        
        logger.info(f"Predicting with features: {features_dict}")
        logger.info(f"Expected features: {check_features}")
        
        missing = [f for f in check_features if f not in features_dict]
        if missing:
            logger.error(f"Missing features: {missing}")
            raise InferenceError(f"Missing inputs: {missing}")
        try:
            input_df = pd.DataFrame([features_dict])[check_features]
            logger.info(f"Input dataframe shape: {input_df.shape}")
            logger.info(f"Input dataframe:\n{input_df}")
            
            prediction = self.model.predict(input_df)
            result = float(prediction[0])
            logger.info(f"Prediction result: {result}")
            return result
        except Exception as e:
            logger.error(f"Inference failed: {str(e)}")
            logger.error(f"Input features: {features_dict}")
            raise InferenceError(f"Prediction failed: {str(e)}")

    def get_model_metadata(self):
        try:
            self._load_model()
            meta = {
                "model_name": self.model_name,
                "version": self.model_version,
                "stage": self.stage,
                "flavor": list(self.model.metadata.flavors.keys()),
                "features": self.feature_names
            }
            if "sklearn" in meta["flavor"]:
                try:
                    model_uri = f"models:/{self.model_name}/{self.stage}"
                    sk_model = mlflow.sklearn.load_model(model_uri)
                    if hasattr(sk_model, "coef_"):
                        meta["coefficients"] = dict(zip(self.feature_names, sk_model.coef_.tolist()))
                    if hasattr(sk_model, "intercept_"):
                        meta["intercept"] = float(sk_model.intercept_)
                except Exception as e:
                    meta["internals_error"] = str(e)
            return meta
        except Exception as e:
            return {"error": str(e)}

    def get_feature_sensitivity(self, base_features: dict, delta: float = 10.0):
        if not base_features or self.model is None:
            return {}
        results = {}
        try:
            baseline = self.predict(base_features)
            for feat in self.feature_names:
                test_payload = base_features.copy()
                test_payload[feat] += delta
                new_pred = self.predict(test_payload)
                results[feat] = {"impact": round(new_pred - baseline, 4)}
        except Exception:
            pass
        return results

    def get_debug_info(self):
        try:
            meta = self.get_model_metadata()
            sensitivity = self.get_feature_sensitivity(self.last_payload) if self.last_payload else {}
            return {
                "active_model": meta,
                "inference_status": "Healthy" if self.model else "Not Loaded",
                "last_payload": self.last_payload,
                "sensitivity_analysis": sensitivity
            }
        except Exception as e:
            return {"critical_error": str(e)}

# Singleton instance
_service_instance = None

def get_prediction_service() -> PredictionService:
    global _service_instance
    if _service_instance is None:
        _service_instance = PredictionService()
    return _service_instance
