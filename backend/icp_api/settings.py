"""Environment-based configuration. See docs/ENVIRONMENTS.md for the full matrix.

All secrets come from the environment (``.env`` locally, the hosting platform's secret store in
deployment). Nothing in this module has a secret default. Production/staging settings are validated strictly at
startup so a misconfigured deployment fails fast instead of running insecurely.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VALID_ENVS = ("local", "test", "staging", "production")


class ConfigError(RuntimeError):
    pass


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE lines). Real environment variables always win."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None or v == "" else v.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    v = os.getenv(name)
    return default if not v else int(v)


def _list(name: str, default: str = "") -> list[str]:
    return [s.strip() for s in os.getenv(name, default).split(",") if s.strip()]


def _path(name: str, default: Path) -> Path:
    v = os.getenv(name)
    p = Path(v) if v else default
    return p if p.is_absolute() else (PROJECT_ROOT / p)


def _parse_admin_tokens(raw: str) -> dict[str, str]:
    """``ADMIN_API_TOKENS="alice:<token>,ci:<token>"`` -> {"alice": token, ...}."""
    tokens: dict[str, str] = {}
    for item in [s.strip() for s in raw.split(",") if s.strip()]:
        name, sep, token = item.partition(":")
        if not sep or not name or not token:
            raise ConfigError("ADMIN_API_TOKENS entries must look like name:token")
        tokens[name] = token
    return tokens


@dataclass(frozen=True)
class Settings:
    app_env: str
    host: str
    port: int
    log_level: str
    log_format: str
    # metadata store: sqlite | d1 (REST API + token) | d1_gateway (through the Worker's data plane)
    metadata_backend: str
    sqlite_path: Path
    d1_account_id: str
    d1_database_id: str
    d1_api_token: str = field(repr=False)
    # Worker data plane (METADATA_BACKEND=d1_gateway / OBJECT_STORE_BACKEND=gateway): the gateway
    # origin; requests carry GATEWAY_SHARED_SECRET, so no Cloudflare API token lives on the backend.
    gateway_internal_url: str = ""
    # object store: local | s3 (R2 S3 API) | gateway (Cloudflare KV through the Worker)
    object_store_backend: str = "local"
    local_object_dir: Path = PROJECT_ROOT / "var" / "objects"
    s3_endpoint_url: str = ""
    s3_bucket: str = ""
    s3_region: str = "auto"
    s3_access_key_id: str = field(default="", repr=False)
    s3_secret_access_key: str = field(default="", repr=False)
    # ML. model_source: mlflow (registry alias, needs a tracking store) | bundle (exported bundle in
    # the object store, no MLflow online; see icp_ml.bundle and docs/DEPLOYMENT.md)
    model_source: str = "mlflow"
    mlflow_tracking_uri: str = ""
    mlflow_artifact_root: str = ""
    model_name: str = "ICP_Price_Model"
    model_alias: str = "production"
    training_config_path: Path = PROJECT_ROOT / "pipeline" / "configs" / "training.yaml"
    model_refresh_seconds: int = 30
    seed_icp_path: Path = PROJECT_ROOT / "data" / "raw" / "dataset.csv"
    seed_wti_path: Path = PROJECT_ROOT / "data" / "raw" / "wti.csv"
    # Optional seeds for the other datasets (<dataset>.csv); exported from public sources, see docs/DATA_SOURCES.md.
    seed_dir: Path = PROJECT_ROOT / "data" / "seed"
    seed_from_repository: bool = True
    # Self-service sign-up (creates a trial organisation). Disable for invite-only deployments.
    public_registration_enabled: bool = True
    # security
    admin_tokens: dict[str, str] = field(default_factory=dict, repr=False)
    gateway_shared_secret: str = field(default="", repr=False)
    require_gateway_secret: bool = False
    metrics_token: str = field(default="", repr=False)
    cors_allowed_origins: tuple[str, ...] = ()
    max_request_bytes: int = 2_000_000
    request_timeout_seconds: int = 30
    rate_limit_enabled: bool = True
    rate_limit_forecast_per_minute: int = 30
    rate_limit_admin_per_minute: int = 30
    rate_limit_default_per_minute: int = 300
    # jobs
    job_max_concurrent: int = 1
    job_timeout_train_seconds: int = 1800
    job_timeout_ingest_seconds: int = 300
    job_heartbeat_seconds: int = 10
    job_stale_after_seconds: int = 120
    job_python: str = ""
    expose_api_docs: bool = True
    # external data sources
    source_cache_dir: Path = PROJECT_ROOT / "var" / "cache" / "sources"
    source_state_dir: Path = PROJECT_ROOT / "var" / "sources"
    eia_api_key: str = field(default="", repr=False)

    @property
    def is_deployed(self) -> bool:
        return self.app_env in ("staging", "production")

    @property
    def uses_gateway_data_plane(self) -> bool:
        return self.metadata_backend == "d1_gateway" or self.object_store_backend == "gateway"

    def validate(self) -> None:
        if self.app_env not in VALID_ENVS:
            raise ConfigError(f"APP_ENV must be one of {VALID_ENVS}")
        if self.metadata_backend not in ("sqlite", "d1", "d1_gateway"):
            raise ConfigError("METADATA_BACKEND must be sqlite, d1 or d1_gateway")
        if self.metadata_backend == "d1" and not (
            self.d1_account_id and self.d1_database_id and self.d1_api_token
        ):
            raise ConfigError("METADATA_BACKEND=d1 requires D1_ACCOUNT_ID, D1_DATABASE_ID and D1_API_TOKEN")
        if self.object_store_backend not in ("local", "s3", "gateway"):
            raise ConfigError("OBJECT_STORE_BACKEND must be local, s3 or gateway")
        if self.object_store_backend == "s3" and not (
            self.s3_endpoint_url and self.s3_bucket and self.s3_access_key_id and self.s3_secret_access_key
        ):
            raise ConfigError(
                "OBJECT_STORE_BACKEND=s3 requires S3_ENDPOINT_URL, S3_BUCKET and S3 credentials"
            )
        if self.uses_gateway_data_plane:
            if not self.gateway_internal_url.startswith(("http://", "https://")):
                raise ConfigError(
                    "METADATA_BACKEND=d1_gateway / OBJECT_STORE_BACKEND=gateway require GATEWAY_INTERNAL_URL "
                    "(the Worker origin, e.g. https://icp-gateway-staging.<subdomain>.workers.dev)"
                )
            if len(self.gateway_shared_secret) < 32:
                raise ConfigError("the gateway data plane requires a GATEWAY_SHARED_SECRET of >= 32 chars")
        if self.model_source not in ("mlflow", "bundle"):
            raise ConfigError("MODEL_SOURCE must be mlflow or bundle")
        if self.is_deployed:
            problems = []
            if self.metadata_backend not in ("d1", "d1_gateway"):
                problems.append(
                    "METADATA_BACKEND must be d1 or d1_gateway (deployment filesystems are not persistent)"
                )
            if self.object_store_backend not in ("s3", "gateway"):
                problems.append("OBJECT_STORE_BACKEND must be s3 (R2) or gateway (KV)")
            if self.model_source == "mlflow":
                if (
                    self.mlflow_tracking_uri.startswith(("sqlite:", "file:"))
                    or "://" not in self.mlflow_tracking_uri
                ):
                    problems.append(
                        "MLFLOW_TRACKING_URI must point to a persistent database (PostgreSQL) when MODEL_SOURCE=mlflow"
                    )
                if not self.mlflow_artifact_root.startswith("s3://"):
                    problems.append("MLFLOW_ARTIFACT_ROOT must be an s3:// URI (R2) when MODEL_SOURCE=mlflow")
            if not self.require_gateway_secret or len(self.gateway_shared_secret) < 32:
                problems.append(
                    "REQUIRE_GATEWAY_SECRET=true with a GATEWAY_SHARED_SECRET of >= 32 chars is required"
                )
            if any(len(t) < 32 for t in self.admin_tokens.values()):
                problems.append("admin tokens must be at least 32 characters")
            if "*" in self.cors_allowed_origins:
                problems.append("CORS_ALLOWED_ORIGINS must not contain '*'")
            if problems:
                raise ConfigError("Invalid deployed configuration: " + "; ".join(problems))

    def seed_files(self) -> dict[str, Path]:
        """Seed CSV per dataset: ICP/WTI from their configured paths, the rest from ``seed_dir/<dataset>.csv``."""
        from icp_ml.schema import DATASETS

        files = {"icp": self.seed_icp_path, "wti": self.seed_wti_path}
        for ds in DATASETS:
            if ds not in files:
                files[ds] = self.seed_dir / f"{ds}.csv"
        return files


def load_settings(env_file: Path | None = None) -> Settings:
    if os.getenv("APP_ENV", "local") in ("local",) or env_file is not None:
        _load_dotenv(env_file or PROJECT_ROOT / ".env")
    app_env = os.getenv("APP_ENV", "local")
    var = PROJECT_ROOT / "var"
    default_tracking = f"sqlite:///{(var / 'mlflow' / 'mlflow.db').as_posix()}"
    settings = Settings(
        app_env=app_env,
        # Hosted runtimes (Render sets RENDER, Replit sets REPL_ID) and deployed envs must bind all
        # interfaces; locally stay on loopback.
        host=os.getenv(
            "API_HOST",
            "0.0.0.0"  # noqa: S104
            if os.getenv("RENDER") or os.getenv("REPL_ID") or app_env in ("staging", "production")
            else "127.0.0.1",
        ),
        port=_int("PORT", _int("API_PORT", 8000)),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        log_format=os.getenv("LOG_FORMAT", "json" if app_env in ("staging", "production") else "console"),
        metadata_backend=os.getenv("METADATA_BACKEND", "sqlite"),
        sqlite_path=_path("SQLITE_PATH", var / "metadata.db"),
        d1_account_id=os.getenv("D1_ACCOUNT_ID", ""),
        d1_database_id=os.getenv("D1_DATABASE_ID", ""),
        d1_api_token=os.getenv("D1_API_TOKEN", ""),
        gateway_internal_url=os.getenv("GATEWAY_INTERNAL_URL", "").strip(),
        object_store_backend=os.getenv("OBJECT_STORE_BACKEND", "local"),
        local_object_dir=_path("LOCAL_OBJECT_DIR", var / "objects"),
        s3_endpoint_url=os.getenv("S3_ENDPOINT_URL", ""),
        s3_bucket=os.getenv("S3_BUCKET", ""),
        s3_region=os.getenv("S3_REGION", "auto"),
        s3_access_key_id=os.getenv("S3_ACCESS_KEY_ID", ""),
        s3_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY", ""),
        model_source=os.getenv("MODEL_SOURCE", "mlflow").strip().lower(),
        mlflow_tracking_uri=os.getenv("MLFLOW_TRACKING_URI", default_tracking),
        mlflow_artifact_root=os.getenv("MLFLOW_ARTIFACT_ROOT", str(var / "mlflow" / "artifacts")),
        model_name=os.getenv("MODEL_NAME", "ICP_Price_Model"),
        model_alias=os.getenv("MODEL_ALIAS", "production"),
        training_config_path=_path(
            "TRAINING_CONFIG_PATH", PROJECT_ROOT / "pipeline" / "configs" / "training.yaml"
        ),
        model_refresh_seconds=_int("MODEL_REFRESH_SECONDS", 30),
        seed_icp_path=_path("SEED_ICP_PATH", PROJECT_ROOT / "data" / "raw" / "dataset.csv"),
        seed_wti_path=_path("SEED_WTI_PATH", PROJECT_ROOT / "data" / "raw" / "wti.csv"),
        seed_dir=_path("SEED_DIR", PROJECT_ROOT / "data" / "seed"),
        seed_from_repository=_bool("SEED_FROM_REPOSITORY", True),
        public_registration_enabled=_bool("PUBLIC_REGISTRATION_ENABLED", True),
        admin_tokens=_parse_admin_tokens(os.getenv("ADMIN_API_TOKENS", "")),
        gateway_shared_secret=os.getenv("GATEWAY_SHARED_SECRET", ""),
        require_gateway_secret=_bool("REQUIRE_GATEWAY_SECRET", False),
        metrics_token=os.getenv("METRICS_TOKEN", ""),
        cors_allowed_origins=tuple(
            _list(
                "CORS_ALLOWED_ORIGINS",
                "http://localhost:5173,http://127.0.0.1:5173,http://localhost:8787,http://127.0.0.1:8787",
            )
        ),
        max_request_bytes=_int("MAX_REQUEST_BYTES", 2_000_000),
        request_timeout_seconds=_int("REQUEST_TIMEOUT_SECONDS", 30),
        rate_limit_enabled=_bool("RATE_LIMIT_ENABLED", True),
        rate_limit_forecast_per_minute=_int("RATE_LIMIT_FORECAST_PER_MINUTE", 30),
        rate_limit_admin_per_minute=_int("RATE_LIMIT_ADMIN_PER_MINUTE", 30),
        rate_limit_default_per_minute=_int("RATE_LIMIT_DEFAULT_PER_MINUTE", 300),
        job_max_concurrent=_int("JOB_MAX_CONCURRENT", 1),
        job_timeout_train_seconds=_int("JOB_TIMEOUT_TRAIN_SECONDS", 1800),
        job_timeout_ingest_seconds=_int("JOB_TIMEOUT_INGEST_SECONDS", 300),
        job_heartbeat_seconds=_int("JOB_HEARTBEAT_SECONDS", 10),
        job_stale_after_seconds=_int("JOB_STALE_AFTER_SECONDS", 120),
        job_python=os.getenv("JOB_PYTHON", ""),
        source_cache_dir=_path("SOURCE_CACHE_DIR", var / "cache" / "sources"),
        source_state_dir=_path("SOURCE_STATE_DIR", var / "sources"),
        eia_api_key=os.getenv("EIA_API_KEY", ""),
        expose_api_docs=_bool("EXPOSE_API_DOCS", app_env != "production"),
    )
    settings.validate()
    return settings
