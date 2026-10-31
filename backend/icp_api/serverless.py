"""ASGI app for serverless platforms (Vercel Functions; docs/DEPLOYMENT.md).

The platform imports the entry file at the root of the deploy folder (``infra/vercel/index.py``,
copied there by ``scripts/vercel-backend.mjs``), which imports ``app`` from this module. The deploy
folder keeps the repository layout, so ``PROJECT_ROOT`` is the bundle root and every file the API
reads (migrations, the training configuration) is inside the bundle. Differences from ``icp-api serve``:

* Nothing binds a socket (``Settings.serverless`` lifts the ``API_HOST`` rule) and there is no
  background job runner: an instance is paused between requests and may be stopped at any time, so
  the job endpoints answer ``503 JOBS_DISABLED``. Ingestion runs from the operator machine
  (``icp-api ingest ...`` against the gateway stores) instead.
* Only the temporary directory is writable. Every writable default (``load_settings(serverless=True)``)
  and every library cache (Matplotlib's ``MPLCONFIGDIR``, XDG caches, the source cache) lives under
  ``serverless_runtime_dir()`` (``/tmp/icp`` on Vercel).
* Lifespan events may or may not run. Nothing depends on them: the app is complete when this module
  builds it, local stores are bootstrapped here (deployed stores never are), and the production-model
  warm-up starts here (the lifespan's own start returns the running warm-up). The model also loads on
  first use, as in every other process.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import MutableMapping
from pathlib import Path
from typing import TYPE_CHECKING

from icp_api.settings import Settings, load_settings, serverless_runtime_dir

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger("icp_api")

# Locations that libraries (not Settings) read from the environment, relative to the runtime dir.
# Explicit environment values win (setdefault).
RUNTIME_ENV: dict[str, str] = {
    "MPLCONFIGDIR": "matplotlib",  # Matplotlib config and font cache (written on first use)
    "XDG_CACHE_HOME": "cache",
    "XDG_CONFIG_HOME": "config",
    "SOURCE_CACHE_DIR": "cache/sources",  # icp_ml.sources.base reads it when it is imported
    "SOURCE_STATE_DIR": "sources",
}
# Created up front: Matplotlib falls back to a fresh temporary directory (with a warning) when its
# configuration directory does not exist or is not writable.
_CREATE = ("MPLCONFIGDIR",)


def prepare_runtime(environ: MutableMapping[str, str] | None = None, root: Path | None = None) -> Path:
    """Point the libraries that write files at the writable runtime directory; returns that directory.

    Must run before the API modules are imported (some read their location at import time)."""
    env = os.environ if environ is None else environ
    root = root or serverless_runtime_dir()
    for name, relative in RUNTIME_ENV.items():
        env.setdefault(name, str(root / relative))
    for name in _CREATE:
        Path(env[name]).mkdir(parents=True, exist_ok=True)
    return root


def create_serverless_app(settings: Settings | None = None) -> FastAPI:
    """The API for one serverless process: no job runner, writable state under the runtime dir,
    complete without lifespan events."""
    started = time.perf_counter()
    prepare_runtime()
    # Imported after prepare_runtime(): module-level defaults read the runtime environment.
    from icp_api.main import bootstrap_storage, create_app

    settings = settings or load_settings(serverless=True)
    app = create_app(settings, enable_jobs=False)
    container = app.state.container
    if not settings.is_deployed:
        # Local verification of the bundle only (deployed stores are migrated with wrangler and are
        # never seeded by the API). Idempotent, so a lifespan that runs it again changes nothing.
        bootstrap_storage(container)
    # The platform may not run the lifespan: start loading the production model now. A request that
    # needs the model before the warm-up finishes loads it itself (same lock), as everywhere.
    container.models.start_warm_up()
    logger.info(
        "icp-api serverless app built",
        extra={
            "env": settings.app_env,
            "metadata": container.db.backend,
            "objects": container.objects.backend,
            "build_ms": round((time.perf_counter() - started) * 1000, 1),
        },
    )
    return app


app: FastAPI  # built on first access (see __getattr__), like icp_api.main.app


def __getattr__(name: str) -> FastAPI:
    # `from icp_api.serverless import app` (the deploy folder's entry file) builds the app once;
    # importing the module for its helpers (tests, tooling) has no side effects.
    if name == "app":
        global app
        app = create_serverless_app()
        return app
    raise AttributeError(name)
