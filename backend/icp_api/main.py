"""FastAPI application factory. Run with ``uvicorn icp_api.main:app`` or ``icp-api serve``."""

from __future__ import annotations

import hmac
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from icp_api import __version__
from icp_api.container import Container, build_container
from icp_api.errors import ApiError, install_error_handlers
from icp_api.logging_setup import configure_logging
from icp_api.middleware import RequestContextMiddleware
from icp_api.routers import accounts, data, forecasts, health, jobs, models, product
from icp_api.security import RateLimiter
from icp_api.services.jobs import JobManager
from icp_api.settings import Settings, load_settings
from icp_api.storage.migrations import apply_local
from icp_api.storage.sql import SqliteExecutor

logger = logging.getLogger("icp_api")


def bootstrap_storage(c: Container) -> None:
    """Local/test only: apply migrations and seed datasets from the repository when empty."""
    if isinstance(c.db, SqliteExecutor):
        applied = apply_local(c.db)
        if applied:
            logger.info("applied migrations: %s", ", ".join(applied))
    if c.settings.seed_from_repository:
        for name, path in c.settings.seed_files().items():
            if path.is_file() and c.repo.get_active_dataset(name) is None:
                c.datasets.seed_from_bytes(name, path.read_bytes(), source_detail=path.name)
                logger.info("seeded %s dataset from %s", name, path.name)


def create_app(
    settings: Settings | None = None, container: Container | None = None, enable_jobs: bool = True
) -> FastAPI:
    settings = settings or load_settings()
    configure_logging(settings.log_level, settings.log_format)
    c = container or build_container(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if not settings.is_deployed:
            bootstrap_storage(c)
        manager = JobManager(c.repo, settings) if enable_jobs else None
        if manager:
            try:
                manager.start()
            except Exception:
                logger.exception("job runner failed to start")
                manager = None
        app.state.jobs = manager
        logger.info(
            "icp-api started",
            extra={
                "env": settings.app_env,
                "metadata": c.db.backend,
                "objects": c.objects.backend,
                "version": __version__,
            },
        )
        yield
        if manager:
            manager.stop()

    app = FastAPI(
        title="ICP Forecasting API",
        version=__version__,
        description="Indonesian Crude Price forecasting: data management, forecasting, model lifecycle and "
        "pipeline operations. Admin operations require a bearer token.",
        lifespan=lifespan,
        docs_url="/docs" if settings.expose_api_docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if settings.expose_api_docs else None,
    )
    app.state.container = c
    app.state.rate_limiter = RateLimiter()
    app.state.jobs = None
    install_error_handlers(app)

    if settings.cors_allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.cors_allowed_origins),
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
            expose_headers=["X-Request-ID", "Retry-After", "Content-Disposition"],
            allow_credentials=False,
            max_age=600,
        )
    app.add_middleware(RequestContextMiddleware, settings=settings)

    for r in (
        health.router,
        data.router,
        forecasts.router,
        models.router,
        jobs.router,
        accounts.router,
        product.router,
    ):
        app.include_router(r)

    @app.get("/metrics", include_in_schema=False)
    def metrics(request: Request) -> PlainTextResponse:
        if settings.metrics_token:
            supplied = request.headers.get("authorization", "").encode()
            if not hmac.compare_digest(supplied, f"Bearer {settings.metrics_token}".encode()):
                raise ApiError(401, "AUTH_REQUIRED", "Metrics require a bearer token.")
        return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)

    return app


app: FastAPI  # created lazily on first access (see __getattr__)


def __getattr__(name: str) -> FastAPI:
    # Lazily build the ASGI app for `uvicorn icp_api.main:app` without side effects on import.
    if name == "app":
        global app
        app = create_app()
        return app
    raise AttributeError(name)
