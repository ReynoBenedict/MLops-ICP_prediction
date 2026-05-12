from pathlib import Path
import logging

logger = logging.getLogger("settings")

# Base Paths - config/settings.py is in config/ folder
# Go up one level to reach project root (MLops-ICP_prediction/)
CONFIG_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CONFIG_DIR.parent

# Log resolved paths for debugging
logger.info(f"CONFIG_DIR: {CONFIG_DIR}")
logger.info(f"PROJECT_ROOT: {PROJECT_ROOT}")

# Data Paths
CLEAN_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"
METRICS_JSON_PATH = PROJECT_ROOT / "metrics.json"
MLFLOW_DB_PATH = PROJECT_ROOT / "mlflow.db"

# Log final paths
logger.info(f"CLEAN_DATA_PATH: {CLEAN_DATA_PATH}")
logger.info(f"MLFLOW_DB_PATH: {MLFLOW_DB_PATH}")
logger.info(f"Data file exists: {CLEAN_DATA_PATH.exists()}")
logger.info(f"MLflow DB exists: {MLFLOW_DB_PATH.exists()}")

# MLflow Configuration
MLFLOW_TRACKING_URI = f"sqlite:///{MLFLOW_DB_PATH.as_posix()}"
MODEL_NAME = "ICP_Price_Model"
PRODUCTION_STAGE = "Production"

logger.info(f"MLFLOW_TRACKING_URI: {MLFLOW_TRACKING_URI}")

# UI Configuration
PAGE_ICON = "https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/oil-well.svg"
THEME_COLOR_NAVY = "#002b5c"
THEME_COLOR_GOLD = "#b38b59"

# Feature Schema
FEATURE_COLUMNS = [
    "lag_1", "lag_3", "lag_6", "rolling_mean_3", 
    "wti_price", "wti_lag_1", "wti_rolling_mean_3"
]
