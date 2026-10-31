"""Serverless entry (Vercel Functions): settings, writable paths, lifespan independence, bundle layout.

The deployed function imports infra/vercel/index.py from the root of a deploy folder that keeps the
repository layout (scripts/vercel-backend.mjs). These tests build the app the same way, without and with
lifespan events, and run the entry file from a copy of that layout in a fresh interpreter.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from icp_api.serverless import RUNTIME_ENV, create_serverless_app, prepare_runtime
from icp_api.services.models import ModelService
from icp_api.settings import ConfigError, load_settings
from icp_api.storage.migrations import current_version, expected_version

ROOT = Path(__file__).resolve().parents[2]
ADMIN = "serverless-admin-token-0123456789abcdefghij"
# Variables the session fixtures of other test modules may have set; each test here decides them itself.
_ICP_ENV = (
    "APP_ENV",
    "API_HOST",
    "METADATA_BACKEND",
    "OBJECT_STORE_BACKEND",
    "MODEL_SOURCE",
    "MODEL_NAME",
    "SQLITE_PATH",
    "LOCAL_OBJECT_DIR",
    "SOURCE_CACHE_DIR",
    "SOURCE_STATE_DIR",
    "MLFLOW_TRACKING_URI",
    "MLFLOW_ARTIFACT_ROOT",
    "TRAINING_CONFIG_PATH",
    "SEED_FROM_REPOSITORY",
    "ADMIN_API_TOKENS",
    "GATEWAY_SHARED_SECRET",
    "GATEWAY_INTERNAL_URL",
    "REQUIRE_GATEWAY_SECRET",
    "METRICS_TOKEN",
    "CORS_ALLOWED_ORIGINS",
    "RENDER",
    "PORT",
    *RUNTIME_ENV,
)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """No ICP settings inherited from other modules; the temporary directory is ``tmp_path``."""
    for name in _ICP_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    return tmp_path / "icp"


@pytest.fixture
def local_env(monkeypatch: pytest.MonkeyPatch, clean_env: Path) -> Path:
    """A local (not deployed) serverless process: SQLite and local objects under the runtime dir."""
    for k, v in {
        "APP_ENV": "test",
        "LOG_LEVEL": "WARNING",
        "MODEL_SOURCE": "bundle",
        "SEED_FROM_REPOSITORY": "false",
        "ADMIN_API_TOKENS": f"tester:{ADMIN}",
    }.items():
        monkeypatch.setenv(k, v)
    return clean_env


def _staging(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in {
        "APP_ENV": "staging",
        "METADATA_BACKEND": "d1_gateway",
        "OBJECT_STORE_BACKEND": "gateway",
        "MODEL_SOURCE": "bundle",
        "GATEWAY_INTERNAL_URL": "https://gw.example.workers.dev",
        "GATEWAY_SHARED_SECRET": "z" * 40,
        "REQUIRE_GATEWAY_SECRET": "true",
        "ADMIN_API_TOKENS": "ops:" + "t" * 40,
        "CORS_ALLOWED_ORIGINS": "https://gw.example.workers.dev",
        "METRICS_TOKEN": "m" * 40,
    }.items():
        monkeypatch.setenv(k, v)


def _under(path: Path, root: Path) -> bool:
    return Path(path).resolve().is_relative_to(root.resolve())


# -- settings ------------------------------------------------------------------------------------------
def test_serverless_settings_need_no_bind_address_and_keep_writable_defaults_under_tmp(
    monkeypatch, clean_env
):
    _staging(monkeypatch)
    # A container host still needs an explicit bind address (unchanged rule) ...
    with pytest.raises(ConfigError, match="API_HOST"):
        load_settings()
    # ... a serverless platform invokes the ASGI app directly, so the rule does not apply there.
    s = load_settings(serverless=True)
    assert s.serverless and s.is_deployed and s.host == "127.0.0.1"
    for path in (s.sqlite_path, s.local_object_dir, s.source_cache_dir, s.source_state_dir):
        assert _under(path, clean_env), path
    assert str(clean_env.as_posix()) in s.mlflow_tracking_uri
    assert _under(Path(s.mlflow_artifact_root), clean_env)
    # Read-only inputs stay in the bundle (the repository layout).
    assert _under(s.training_config_path, ROOT)
    # Every other deployed rule still applies.
    monkeypatch.setenv("METRICS_TOKEN", "short")
    with pytest.raises(ConfigError, match="METRICS_TOKEN"):
        load_settings(serverless=True)


def test_explicit_paths_win_over_the_serverless_defaults(monkeypatch, clean_env, tmp_path):
    _staging(monkeypatch)
    monkeypatch.setenv("SOURCE_CACHE_DIR", str(tmp_path / "elsewhere"))
    s = load_settings(serverless=True)
    assert s.source_cache_dir == tmp_path / "elsewhere"
    assert _under(s.source_state_dir, clean_env)


def test_prepare_runtime_points_library_caches_at_the_runtime_dir(tmp_path):
    environ = {"XDG_CACHE_HOME": "/explicit/cache"}
    root = prepare_runtime(environ, tmp_path / "rt")
    assert root == tmp_path / "rt"
    assert environ["MPLCONFIGDIR"] == str(root / "matplotlib") and (root / "matplotlib").is_dir()
    assert environ["SOURCE_CACHE_DIR"] == str(root / "cache" / "sources")
    assert environ["SOURCE_STATE_DIR"] == str(root / "sources")
    assert environ["XDG_CONFIG_HOME"] == str(root / "config")
    assert environ["XDG_CACHE_HOME"] == "/explicit/cache"  # explicit values win


# -- the app without and with lifespan events -----------------------------------------------------------------
def test_serverless_app_is_complete_without_lifespan_events(local_env):
    app = create_serverless_app()
    c = app.state.container
    assert c.settings.serverless and app.state.jobs is None
    assert _under(c.settings.sqlite_path, local_env) and _under(c.settings.local_object_dir, local_env)
    # Local stores are migrated when the app is built, not in the lifespan.
    assert current_version(c.db) == expected_version()
    assert Path(os.environ["MPLCONFIGDIR"]).is_dir() and _under(Path(os.environ["MPLCONFIGDIR"]), local_env)
    warm_up = c.models._warm_up_thread
    assert warm_up is not None  # started without a lifespan
    warm_up.join(30)
    assert not c.models.warming

    client = TestClient(app)  # no `with`: the lifespan never runs
    assert client.get("/health/live").status_code == 200
    who = client.get("/api/v1/auth/whoami")
    assert who.status_code == 200 and who.json()["kind"] == "public"
    admin = {"Authorization": f"Bearer {ADMIN}"}
    job = client.post("/api/v1/jobs", json={"job_type": "evaluate_alerts", "params": {}}, headers=admin)
    assert job.status_code == 503 and job.json()["error"]["code"] == "JOBS_DISABLED"
    ready = client.get("/health/ready", headers=admin).json()
    runner = next(ch for ch in ready["checks"] if ch["name"] == "job_runner")
    assert runner["status"] == "not_configured"
    # No exported bundle in this empty store: the model is reported, not faked.
    model = next(ch for ch in ready["checks"] if ch["name"] == "production_model")
    assert model["status"] == "degraded"


def test_lifespan_adds_no_job_runner_and_reuses_the_running_warm_up(local_env, monkeypatch):
    gate = threading.Event()

    def blocked(self: Any, force_refresh: bool = False) -> Any:
        gate.wait(10)
        raise RuntimeError("model load released by the test")

    monkeypatch.setattr(ModelService, "production", blocked)
    app = create_serverless_app()
    models = app.state.container.models
    first = models._warm_up_thread
    try:
        assert first is not None and first.is_alive() and models.warming
        assert models.start_warm_up() is first  # idempotent while it runs
        with TestClient(app) as client:  # the platform may run the lifespan as well
            assert app.state.jobs is None
            assert models._warm_up_thread is first
            assert client.get("/health/live").status_code == 200
        assert first.is_alive() and models._warm_up_thread is first
    finally:
        gate.set()
    first.join(10)
    assert not first.is_alive() and not models.warming
    again = models.start_warm_up()  # a finished warm-up may be started again
    assert again is not first
    again.join(10)


# -- the entry file in the deploy-folder layout ------------------------------------------------------------------
def _bundle(dest: Path) -> Path:
    """The layout scripts/vercel-backend.mjs builds (the parts the API reads)."""
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", "tests")
    shutil.copytree(ROOT / "backend" / "icp_api", dest / "backend" / "icp_api", ignore=ignore)
    shutil.copytree(ROOT / "pipeline" / "icp_ml", dest / "pipeline" / "icp_ml", ignore=ignore)
    shutil.copytree(ROOT / "pipeline" / "configs", dest / "pipeline" / "configs")
    shutil.copytree(
        ROOT / "infra" / "cloudflare" / "d1" / "migrations",
        dest / "infra" / "cloudflare" / "d1" / "migrations",
    )
    shutil.copy2(ROOT / "infra" / "vercel" / "index.py", dest / "index.py")
    return dest


_PROBE = """
import json, os, runpy, sys
ns = runpy.run_path(os.path.join(os.getcwd(), "index.py"), run_name="vercel_entry")
app = ns["app"]
import icp_api
from icp_api.settings import PROJECT_ROOT
from icp_api.storage.migrations import MIGRATIONS_DIR, current_version, expected_version
from fastapi.testclient import TestClient
client = TestClient(app)
c = app.state.container
print(json.dumps({
    "icp_api": icp_api.__file__, "project_root": str(PROJECT_ROOT), "migrations": str(MIGRATIONS_DIR),
    "schema_ok": current_version(c.db) == expected_version(),
    "live": client.get("/health/live").status_code, "jobs_disabled": app.state.jobs is None,
    "serverless": c.settings.serverless, "sqlite": str(c.settings.sqlite_path),
    "objects": str(c.settings.local_object_dir), "mplconfigdir": os.environ.get("MPLCONFIGDIR"),
    "dont_write_bytecode": sys.dont_write_bytecode, "mlflow_imported": "mlflow" in sys.modules,
}))
"""


def test_vercel_entry_serves_the_app_from_the_deploy_folder_layout(tmp_path):
    bundle = _bundle(tmp_path / "bundle")
    before = sorted(p.relative_to(bundle).as_posix() for p in bundle.rglob("*"))
    tmp = tmp_path / "t"
    tmp.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in _ICP_ENV and k != "PYTHONPATH"}
    env.update(
        {
            "TMPDIR": str(tmp),
            "APP_ENV": "test",
            "LOG_LEVEL": "WARNING",
            "MODEL_SOURCE": "bundle",
            "SEED_FROM_REPOSITORY": "false",
        }
    )
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE], cwd=bundle, env=env, capture_output=True, text=True, timeout=180
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    # The code and every file it reads come from the bundle, not from the repository.
    assert _under(Path(out["icp_api"]), bundle) and Path(out["project_root"]) == bundle.resolve()
    assert _under(Path(out["migrations"]), bundle) and out["schema_ok"]
    assert out["live"] == 200 and out["jobs_disabled"] and out["serverless"]
    # Writable state lives under <TMPDIR>/icp; nothing was written into the bundle (no __pycache__ either).
    runtime = tmp / "icp"
    for key in ("sqlite", "objects", "mplconfigdir"):
        assert _under(Path(out[key]), runtime), (key, out[key])
    assert out["dont_write_bytecode"] and not out["mlflow_imported"]
    after = sorted(p.relative_to(bundle).as_posix() for p in bundle.rglob("*"))
    assert after == before


# -- deploy-folder configuration ---------------------------------------------------------------------------------
def _pins(text: str) -> dict[str, str]:
    pins = {}
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        spec, _, marker = line.partition(";")
        name, _, version = spec.strip().partition("==")
        key = re.sub(r"[-_.]+", "-", name).lower()
        if marker and "python_version" in marker and "<" in marker:
            continue  # constraints for Python < 3.11; the function runs Python 3.12
        pins[key] = version.strip()
    return pins


# openpyxl's dependency is missing from requirements/constraints.txt (the development environment has the
# same 2.0.0); pinned here explicitly until the constraints file lists it.
_NOT_IN_CONSTRAINTS = {"et-xmlfile": "2.0.0"}


def test_vercel_requirements_are_pinned_to_the_audited_constraints():
    runtime = _pins((ROOT / "infra" / "vercel" / "requirements.txt").read_text(encoding="utf-8"))
    constraints = _pins((ROOT / "requirements" / "constraints.txt").read_text(encoding="utf-8"))
    assert {"fastapi", "pandas", "scikit-learn", "skops", "matplotlib", "reportlab", "openpyxl"} <= set(
        runtime
    )
    expected = {**constraints, **{k: v for k, v in _NOT_IN_CONSTRAINTS.items() if k not in constraints}}
    mismatched = {k: (v, expected.get(k)) for k, v in runtime.items() if expected.get(k) != v}
    assert not mismatched, mismatched
    # Not needed by the serverless API in bundle mode (and too large for the 500 MB bundle in mlflow's case).
    excluded = {
        "mlflow",
        "mlflow-skinny",
        "boto3",
        "psycopg2-binary",
        "uvicorn",
        "pymupdf",
        "pytesseract",
        "pdfplumber",
    }
    assert not excluded & set(runtime)


def test_vercel_function_config():
    config = json.loads((ROOT / "infra" / "vercel" / "vercel.json").read_text(encoding="utf-8"))
    assert config["regions"] == ["sin1"]
    fn = config["functions"]["index.py"]
    assert 0 < fn["maxDuration"] <= 300
    # Vercel's builder (@vercel/python 27) applies excludeFiles to the project's files only; dependencies come
    # from its own environment. Should that ever change, a blanket "**/tests/**" would strip
    # numpy._core.tests, which numpy.testing imports at runtime, so the pattern stays out.
    assert "**/tests/**" not in fn.get("excludeFiles", "")
    assert (ROOT / "infra" / "vercel" / ".python-version").read_text(encoding="utf-8").strip() == "3.12"
