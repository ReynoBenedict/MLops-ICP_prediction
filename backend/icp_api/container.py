"""Builds the service graph from settings. Shared by the API process and the job worker."""

from __future__ import annotations

from dataclasses import dataclass

from icp_api.services.accounts import AccountService
from icp_api.services.alerts import AlertService
from icp_api.services.datasets import DatasetService
from icp_api.services.forecasts import ForecastService
from icp_api.services.insights import InsightService
from icp_api.services.models import ModelService
from icp_api.services.reports import ReportService
from icp_api.services.scenarios import ScenarioService
from icp_api.settings import Settings
from icp_api.storage.objects import GatewayObjectStore, LocalObjectStore, ObjectStore, S3ObjectStore
from icp_api.storage.product import ProductRepository
from icp_api.storage.sql import D1HttpExecutor, GatewayD1Executor, SqlExecutor, SqliteExecutor


@dataclass
class Container:
    settings: Settings
    db: SqlExecutor
    repo: ProductRepository
    objects: ObjectStore
    datasets: DatasetService
    models: ModelService
    forecasts: ForecastService
    accounts: AccountService
    insights: InsightService
    alerts: AlertService
    scenarios: ScenarioService
    reports: ReportService


def build_sql(settings: Settings) -> SqlExecutor:
    if settings.metadata_backend == "d1":
        return D1HttpExecutor(settings.d1_account_id, settings.d1_database_id, settings.d1_api_token)
    if settings.metadata_backend == "d1_gateway":
        return GatewayD1Executor(settings.gateway_internal_url, settings.gateway_shared_secret)
    return SqliteExecutor(settings.sqlite_path)


def build_objects(settings: Settings) -> ObjectStore:
    if settings.object_store_backend == "gateway":
        return GatewayObjectStore(settings.gateway_internal_url, settings.gateway_shared_secret)
    if settings.object_store_backend == "s3":
        return S3ObjectStore(
            settings.s3_endpoint_url,
            settings.s3_bucket,
            settings.s3_access_key_id,
            settings.s3_secret_access_key,
            settings.s3_region,
        )
    return LocalObjectStore(settings.local_object_dir)


def build_container(
    settings: Settings, db: SqlExecutor | None = None, objects: ObjectStore | None = None
) -> Container:
    db = db or build_sql(settings)
    objects = objects or build_objects(settings)
    repo = ProductRepository(db)
    datasets = DatasetService(repo, objects)
    models = ModelService.from_config(
        repo,
        datasets,
        settings.mlflow_tracking_uri,
        settings.model_name,
        settings.model_alias,
        settings.model_refresh_seconds,
        settings.training_config_path,
        model_source=settings.model_source,
        objects=objects,
    )
    forecasts = ForecastService(repo, datasets, models, objects)
    accounts = AccountService(repo)
    insights = InsightService(repo, datasets)
    alerts = AlertService(repo, datasets)
    scenarios = ScenarioService(repo, forecasts)
    reports = ReportService(repo, datasets, forecasts, models, insights, scenarios, objects)
    return Container(
        settings,
        db,
        repo,
        objects,
        datasets,
        models,
        forecasts,
        accounts,
        insights,
        alerts,
        scenarios,
        reports,
    )
