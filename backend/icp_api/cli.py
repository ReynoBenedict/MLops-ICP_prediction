"""Backend operations CLI (``icp-api``).

icp-api serve [--reload]          run the API (host/port from API_HOST / PORT)
icp-api migrate [--check]         apply local SQLite migrations, or check the schema version (any backend)
icp-api seed                      import data/raw datasets as first versions when none are active
icp-api bootstrap                 migrate + seed + train + promote the first model if valid (local setup)
icp-api export-openapi --out F    write the OpenAPI contract
icp-api new-token                 print a random admin token
icp-api backup [--out DIR]        local backup: metadata DB, object store, MLflow store
icp-api export-model [--version N] [--overwrite]
                                  copy a registered model version (default: production alias) from
                                  MLflow into the configured object store as a serving bundle
                                  (MODEL_SOURCE=bundle deployments; see docs/DEPLOYMENT.md)
"""

from __future__ import annotations

import argparse
import json
import secrets
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from icp_api.settings import PROJECT_ROOT, load_settings


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    s = load_settings()
    uvicorn.run(
        "icp_api.main:app",
        host=s.host,
        port=s.port,
        reload=args.reload,
        proxy_headers=False,
        timeout_graceful_shutdown=5,  # gateway keep-alive connections must not block reloads/shutdown
        log_config=None,
        reload_dirs=[str(PROJECT_ROOT / "backend"), str(PROJECT_ROOT / "pipeline")] if args.reload else None,
    )
    return 0


def _migrate(args: argparse.Namespace) -> int:
    from icp_api.container import build_sql
    from icp_api.storage.migrations import apply_local, current_version, expected_version
    from icp_api.storage.sql import SqliteExecutor

    s = load_settings()
    db = build_sql(s)
    if not args.check:
        if not isinstance(db, SqliteExecutor):
            print(
                "D1 schema is managed by wrangler: "
                "npx wrangler d1 migrations apply <DB_NAME> --remote -c infra/cloudflare/d1/wrangler.jsonc"
            )
            return 2
        applied = apply_local(db)
        print(f"applied: {applied or 'nothing (up to date)'}")
    have, want = current_version(db), expected_version()
    print(json.dumps({"backend": db.backend, "schema_version": have, "expected": want}))
    return 0 if have == want else 1


def _seed(_: argparse.Namespace) -> int:
    from icp_api.container import build_container

    s = load_settings()
    c = build_container(s)
    for name, path in s.seed_files().items():
        if not path.is_file():
            print(f"{name}: seed file {path} missing (run `dvc pull` for data/raw)")
            continue
        row = c.datasets.seed_from_bytes(name, path.read_bytes(), source_detail=path.name)
        print(
            f"{name}: "
            + (f"seeded version {row['version']} ({row['id']})" if row else "already has an active version")
        )
    return 0


def _bootstrap(args: argparse.Namespace) -> int:
    from icp_api.container import build_container
    from icp_api.main import bootstrap_storage
    from icp_api.storage.repository import new_id, utcnow
    from icp_api.tasks import run_train

    s = load_settings()
    if s.is_deployed:
        print("bootstrap is for local environments; in deployment use `seed` and the training job API.")
        return 2
    c = build_container(s)
    bootstrap_storage(c)
    if c.models.registry.production_version() and not args.force:
        print(
            f"production model already set (v{c.models.registry.production_version()}); use --force to retrain"
        )
        return 0
    print("backtesting and training the forecasting methods (this takes ~1-3 min)...")
    job = {
        "id": new_id("boot"),
        "params": {"auto_promote": False},
        "request_id": None,
        "created_at": utcnow(),
    }
    summary = run_train(c, job)
    version = summary["registered_version"]
    print(
        json.dumps(
            {k: summary.get(k) for k in ("registered_version", "best", "naive", "backtest_period")}, indent=2
        )
    )
    current = c.models.registry.production_version()
    report = c.models.validate(version)
    if report["passed"] or current is None:
        result = c.models.promote(
            version,
            "local bootstrap: initial production model",
            force=not report["passed"],
            actor="system:bootstrap",
            request_id=None,
        )
        print(f"promoted v{version} to '{s.model_alias}' (forced={result['forced']})")
        if not report["passed"]:
            print("WARNING: validation gates failed; forced only because no production model existed:")
            print(json.dumps([c for c in report["checks"] if not c["passed"]], indent=2))
    else:
        print(f"v{version} not promoted; validation failed: {json.dumps(report['checks'], indent=2)}")
    return 0


def _export_openapi(args: argparse.Namespace) -> int:
    import os

    os.environ.setdefault("APP_ENV", "test")
    from icp_api.main import create_app

    app = create_app(enable_jobs=False)
    spec = app.openapi()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {out} ({len(spec['paths'])} paths)")
    return 0


def _ingest(args: argparse.Namespace) -> int:
    """Run a data connector in-process (operators / first setup); jobs do the same via the API."""
    from icp_api.container import build_container
    from icp_api.tasks import TASKS

    s = load_settings()
    c = build_container(s)
    from icp_api.main import bootstrap_storage

    if not s.is_deployed:
        bootstrap_storage(c)
    job_type = {"benchmarks": "ingest_benchmarks", "fx": "ingest_fx", "icp": "ingest_icp"}[args.source]
    params = {"full_backfill": args.full, "news_pages": args.news_pages} if args.source == "icp" else {}
    job = {"id": f"cli_{args.source}", "params": params, "request_id": None}
    out = TASKS[job_type](c, job)
    print(json.dumps(out, indent=2, default=str))
    return 0


def _export_model(args: argparse.Namespace) -> int:
    """Export the promoted model as a bundle into the *configured* object store.

    Run where MLflow is reachable (the operator machine). Point OBJECT_STORE_BACKEND at the target
    environment (e.g. `gateway` + GATEWAY_INTERNAL_URL + GATEWAY_SHARED_SECRET for the Rp0 staging
    deployment) to write straight into its store; secrets stay in the environment.
    """
    from icp_api.container import build_objects
    from icp_ml.bundle import export_bundle
    from icp_ml.registry import ModelRegistry, RegistryError

    s = load_settings()
    store = build_objects(s)
    store.ping()
    registry = ModelRegistry(s.mlflow_tracking_uri, s.model_name, s.model_alias)
    try:
        out = export_bundle(registry, store, version=args.version, overwrite=args.overwrite)
    except RegistryError as exc:
        print(f"export failed: {exc}")
        return 1
    print(
        json.dumps(
            {
                "object_store": store.backend,
                "version": out["version"],
                "written": out["written"],
                "skipped_existing": out["skipped"],
                "pointer": out["pointer"],
            },
            indent=2,
        )
    )
    return 0


def _clone_state(args: argparse.Namespace) -> int:
    """Copy the local SQLite metadata and local object files into the *configured* remote stores.

    Used once to carry a verified local state (reconciled datasets with their evidence, forecasts,
    promotions, accounts) into a fresh deployment whose schema was applied with wrangler. Local
    files are only read; remote writes are idempotent (`INSERT OR IGNORE`, write-once objects), so
    the command can be re-run. `sessions` (login tokens) and `app_schema` are never copied.
    """
    from icp_api.container import build_objects, build_sql
    from icp_api.storage.objects import ObjectExists
    from icp_api.storage.sql import SqliteExecutor

    s = load_settings()
    if s.metadata_backend == "sqlite" or s.object_store_backend == "local":
        print(
            "clone-state needs remote targets: set METADATA_BACKEND=d1_gateway and OBJECT_STORE_BACKEND=gateway"
        )
        return 2
    remote_db, remote_objects = build_sql(s), build_objects(s)
    remote_db.ping()
    remote_objects.ping()
    local_db = SqliteExecutor(Path(args.sqlite))
    objects_root = Path(args.objects)
    max_params = 100  # D1 bound-parameter limit per statement
    skip = {"app_schema", "sessions"}
    tables = [
        r["name"]
        for r in local_db.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
        )
        if r["name"] not in skip
    ]
    for table in tables:
        rows = local_db.query(f"SELECT * FROM {table} ORDER BY rowid")  # noqa: S608 - identifiers come from the local schema, checked against the remote one
        if not rows:
            continue
        cols = list(rows[0].keys())
        remote_cols = {c["name"] for c in remote_db.query(f"PRAGMA table_info({table})")}
        missing = [c for c in cols if c not in remote_cols]
        if missing:
            print(f"{table}: remote schema lacks columns {missing}; apply migrations first")
            return 1
        before = remote_db.query(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]  # noqa: S608 - identifiers come from the local schema, checked against the remote one
        per_stmt = max(1, max_params // len(cols))
        placeholders = "(" + ",".join("?" * len(cols)) + ")"
        for i in range(0, len(rows), per_stmt):
            chunk = rows[i : i + per_stmt]
            if args.dry_run:
                continue
            remote_db.execute(
                f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES "  # noqa: S608 - identifiers come from the local schema, checked against the remote one
                + ",".join([placeholders] * len(chunk)),
                [v for r in chunk for v in r.values()],
            )
        after = remote_db.query(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]  # noqa: S608 - identifiers come from the local schema, checked against the remote one
        print(f"{table}: local {len(rows)} rows; remote {before} -> {after}")
    content_types = {
        ".csv": "text/csv",
        ".json": "application/json",
        ".md": "text/markdown",
        ".pdf": "application/pdf",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".skops": "application/octet-stream",
    }
    written = present = 0
    for path in sorted(p for p in objects_root.rglob("*") if p.is_file()):
        key = path.relative_to(objects_root).as_posix()
        if args.dry_run:
            print(f"would copy {key}")
            continue
        try:
            remote_objects.put(
                key, path.read_bytes(), content_types.get(path.suffix, "application/octet-stream")
            )
            written += 1
        except ObjectExists:
            present += 1
    print(f"objects: copied {written}, already present {present}")
    return 0


def _new_token(_: argparse.Namespace) -> int:
    print(secrets.token_urlsafe(32))
    return 0


def _backup(args: argparse.Namespace) -> int:
    s = load_settings()
    if s.metadata_backend != "sqlite" or s.object_store_backend != "local":
        print(
            "Deployed backups use platform tooling; see docs/OPERATIONS.md "
            "(wrangler d1 export / Time Travel, wrangler kv key list+get, R2, Postgres)."
        )
        return 2
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = Path(args.out or PROJECT_ROOT / "var" / "backups") / stamp
    dest.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(str(s.sqlite_path))
    with sqlite3.connect(str(dest / "metadata.db")) as dst:
        src.backup(dst)  # consistent online copy
    src.close()
    if s.mlflow_tracking_uri.startswith("sqlite:///"):
        mlsrc = sqlite3.connect(s.mlflow_tracking_uri.removeprefix("sqlite:///"))
        with sqlite3.connect(str(dest / "mlflow.db")) as dst:
            mlsrc.backup(dst)
        mlsrc.close()
    shutil.copytree(s.local_object_dir, dest / "objects", dirs_exist_ok=True)
    artifacts = Path(s.mlflow_artifact_root)
    if artifacts.is_dir():
        shutil.copytree(artifacts, dest / "mlflow-artifacts", dirs_exist_ok=True)
    print(f"backup written to {dest}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="icp-api", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("serve")
    p.add_argument("--reload", action="store_true")
    p.set_defaults(func=_serve)
    p = sub.add_parser("migrate")
    p.add_argument("--check", action="store_true")
    p.set_defaults(func=_migrate)
    sub.add_parser("seed").set_defaults(func=_seed)
    p = sub.add_parser("bootstrap")
    p.add_argument("--force", action="store_true", help="retrain even if a production model exists")
    p.set_defaults(func=_bootstrap)
    p = sub.add_parser("export-openapi")
    p.add_argument("--out", default=str(PROJECT_ROOT / "packages" / "contracts" / "openapi.json"))
    p.set_defaults(func=_export_openapi)
    p = sub.add_parser(
        "ingest", help="fetch a source and store a new dataset version (fx | benchmarks | icp)"
    )
    p.add_argument("source", choices=["fx", "benchmarks", "icp"])
    p.add_argument("--full", action="store_true", help="icp: scan the whole ESDM news archive")
    p.add_argument("--news-pages", type=int, default=6)
    p.set_defaults(func=_ingest)
    sub.add_parser("new-token").set_defaults(func=_new_token)
    p = sub.add_parser("export-model", help="export a registered model version as a serving bundle")
    p.add_argument("--version", default=None, help="registry version (default: the production alias)")
    p.add_argument("--overwrite", action="store_true", help="rewrite bundle files that already exist")
    p.set_defaults(func=_export_model)
    p = sub.add_parser("backup")
    p.add_argument("--out", default=None)
    p.set_defaults(func=_backup)
    p = sub.add_parser(
        "clone-state", help="copy the local SQLite state and object files into the configured remote stores"
    )
    p.add_argument("--sqlite", default=str(PROJECT_ROOT / "var" / "metadata.db"))
    p.add_argument("--objects", default=str(PROJECT_ROOT / "var" / "objects"))
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_clone_state)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
