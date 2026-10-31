"""Backend operations CLI (``icp-api``).

icp-api serve [--reload]          run the API (host/port from API_HOST / PORT)
icp-api migrate [--check]         apply local SQLite migrations, or check the schema version (any backend)
icp-api seed                      import data/raw datasets as first versions when none are active
icp-api bootstrap                 migrate + seed + train + promote the first model if valid (local setup)
icp-api export-openapi --out F    write the OpenAPI contract
icp-api new-token                 print a random admin token
icp-api audit-prune [--older-than-days N] [--apply]
                                  delete audit rows older than N days (default AUDIT_RETENTION_DAYS;
                                  0 keeps everything); dry run unless --apply; never automatic
icp-api backup [--out DIR]        local backup: metadata DB, object store, MLflow store
icp-api export-model [--version N] [--overwrite]
                                  copy a registered model version (default: production alias) from
                                  MLflow into the configured object store as a serving bundle
                                  (MODEL_SOURCE=bundle deployments; see docs/DEPLOYMENT.md)
"""

from __future__ import annotations

import argparse
import json
import re
import secrets
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from icp_api.settings import PROJECT_ROOT, Settings, load_settings

# clone-state: the tables it may copy, each with constant read/count statements (no SQL is built from
# schema names). A local table missing here stops the command, so a new migration must add its
# table explicitly. `sessions` / `email_tokens` (credentials) and `app_schema` are never copied.
_CLONE_SQL: dict[str, tuple[str, str]] = {
    "alert_events": (
        "SELECT * FROM alert_events ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM alert_events",
    ),
    "alert_rules": (
        "SELECT * FROM alert_rules ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM alert_rules",
    ),
    "api_keys": (
        "SELECT * FROM api_keys ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM api_keys",
    ),
    "audit_events": (
        "SELECT * FROM audit_events ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM audit_events",
    ),
    "dataset_versions": (
        "SELECT * FROM dataset_versions ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM dataset_versions",
    ),
    "deletion_requests": (
        "SELECT * FROM deletion_requests ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM deletion_requests",
    ),
    "forecast_runs": (
        "SELECT * FROM forecast_runs ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM forecast_runs",
    ),
    "ingestion_runs": (
        "SELECT * FROM ingestion_runs ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM ingestion_runs",
    ),
    "jobs": (
        "SELECT * FROM jobs ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM jobs",
    ),
    "model_promotions": (
        "SELECT * FROM model_promotions ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM model_promotions",
    ),
    "organizations": (
        "SELECT * FROM organizations ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM organizations",
    ),
    "plan_change_requests": (
        "SELECT * FROM plan_change_requests ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM plan_change_requests",
    ),
    "report_runs": (
        "SELECT * FROM report_runs ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM report_runs",
    ),
    "saved_scenarios": (
        "SELECT * FROM saved_scenarios ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM saved_scenarios",
    ),
    "source_documents": (
        "SELECT * FROM source_documents ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM source_documents",
    ),
    "source_evidence": (
        "SELECT * FROM source_evidence ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM source_evidence",
    ),
    "usage_events": (
        "SELECT * FROM usage_events ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM usage_events",
    ),
    "users": (
        "SELECT * FROM users ORDER BY rowid",
        "SELECT COUNT(*) AS n FROM users",
    ),
}
_CLONE_SKIP = frozenset({"app_schema", "sessions", "email_tokens"})
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    s = load_settings()
    uvicorn.run(
        "icp_api.main:app",
        host=s.host,
        port=s.port,
        reload=args.reload,
        # Exactly one process: rate limits, sign-in throttles and quota headers use in-process state.
        # An explicit value also stops uvicorn from honouring a platform-provided WEB_CONCURRENCY.
        workers=1,
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
    tables = [
        r["name"]
        for r in local_db.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
        )
        if r["name"] not in _CLONE_SKIP
    ]
    unknown = [t for t in tables if t not in _CLONE_SQL]
    if unknown:
        print(f"clone-state does not know the tables {unknown}; add them to cli._CLONE_SQL first")
        return 1
    for table in tables:
        select_sql, count_sql = _CLONE_SQL[table]
        rows = local_db.query(select_sql)
        if not rows:
            continue
        cols = list(rows[0].keys())
        bad = [c for c in cols if not _IDENTIFIER.match(c)]
        if bad:
            print(f"{table}: unexpected column names {bad}; refusing to build an INSERT")
            return 1
        remote_cols = {c["name"] for c in remote_db.query(f'PRAGMA table_info("{table}")')}
        missing = [c for c in cols if c not in remote_cols]
        if missing:
            print(f"{table}: remote schema lacks columns {missing}; apply migrations first")
            return 1
        before = remote_db.query(count_sql)[0]["n"]
        per_stmt = max(1, max_params // len(cols))
        placeholders = "(" + ",".join("?" * len(cols)) + ")"
        for i in range(0, len(rows), per_stmt):
            chunk = rows[i : i + per_stmt]
            if args.dry_run:
                continue
            remote_db.execute(
                # table: a key of _CLONE_SQL; columns: validated identifiers present in the remote schema
                f'INSERT OR IGNORE INTO "{table}" ({",".join(cols)}) VALUES '  # noqa: S608
                + ",".join([placeholders] * len(chunk)),
                [v for r in chunk for v in r.values()],
            )
        after = remote_db.query(count_sql)[0]["n"]
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


def _non_negative(value: str) -> int:
    n = int(value)
    if n < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return n


def _audit_prune(args: argparse.Namespace) -> int:
    """Delete audit rows older than the retention period. Dry run unless --apply; never automatic.

    The period is --older-than-days, else AUDIT_RETENTION_DAYS; 0 (the default) keeps everything."""
    from datetime import timedelta

    from icp_api.container import build_sql
    from icp_api.storage.product import ProductRepository

    s = load_settings()
    days = s.audit_retention_days if args.older_than_days is None else args.older_than_days
    if days == 0:
        print(json.dumps({"retention_days": 0, "deleted": 0, "note": "retention 0 keeps every audit row"}))
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    repo = ProductRepository(build_sql(s))
    count = repo.count_audit_before(cutoff)
    if not args.apply:
        print(json.dumps({"retention_days": days, "cutoff": cutoff, "dry_run": True, "would_delete": count}))
        return 0
    deleted = repo.prune_audit_before(cutoff)
    repo.audit(
        "system:cli",
        "audit.prune",
        "success",
        details={"retention_days": days, "cutoff": cutoff, "deleted": deleted},
    )
    print(json.dumps({"retention_days": days, "cutoff": cutoff, "dry_run": False, "deleted": deleted}))
    return 0


def is_local_store(s: Settings) -> bool:
    """True when ``icp-api backup`` can back the store up itself (SQLite metadata, local objects)."""
    return s.metadata_backend == "sqlite" and s.object_store_backend == "local"


def _backup(args: argparse.Namespace) -> int:
    s = load_settings()
    if not is_local_store(s):
        print(
            "Deployed backups use platform tooling; see docs/OPERATIONS.md "
            "(wrangler d1 export / Time Travel, wrangler kv key list+get, R2, Postgres)."
        )
        return 2
    dest = write_local_backup(s, args.out)
    print(f"backup written to {dest}")
    return 0


def write_local_backup(s: Settings, out: str | Path | None = None) -> Path:
    """Write a local backup (metadata DB, MLflow store, objects, MLflow artifacts) into
    ``<out or var/backups>/<UTC stamp>/`` and return that directory. Local stores only."""
    if not is_local_store(s):
        raise ValueError("write_local_backup needs the local SQLite store and local objects")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = Path(out or PROJECT_ROOT / "var" / "backups") / stamp
    dest.mkdir(parents=True, exist_ok=True)
    sources = [(Path(s.sqlite_path), dest / "metadata.db")]
    if s.mlflow_tracking_uri.startswith("sqlite:///"):
        sources.append((Path(s.mlflow_tracking_uri.removeprefix("sqlite:///")), dest / "mlflow.db"))
    for source, target in sources:
        src = sqlite3.connect(str(source))
        dst = sqlite3.connect(str(target))
        try:
            src.backup(dst)  # consistent online copy
        finally:
            dst.close()
            src.close()
    shutil.copytree(s.local_object_dir, dest / "objects", dirs_exist_ok=True)
    artifacts = Path(s.mlflow_artifact_root)
    if artifacts.is_dir():
        shutil.copytree(artifacts, dest / "mlflow-artifacts", dirs_exist_ok=True)
    return dest


def _backfill_evidence(args: argparse.Namespace) -> int:
    """Record source evidence for ICP months of the active version that have none (additive and
    idempotent; dataset values are never changed). Reads the stored ESDM reconciliation state
    (``reconciliation.json`` in the source state directory); nothing is downloaded."""
    import pandas as pd

    from icp_api.container import build_container
    from icp_api.services.icp_official import OfficialIcpIngestion
    from icp_ml.sources import esdm_backfill, esdm_icp

    s = load_settings()
    c = build_container(s)
    ing = OfficialIcpIngestion(c)
    raw = esdm_backfill._load(ing.state_dir / "reconciliation.json", [])
    if not raw:
        print(f"no reconciliation state at {ing.state_dir}; run `icp-api ingest icp` first")
        return 1
    months = [esdm_icp.ReconciledMonth(**m) for m in raw]
    _, evidence, _ = ing.accepted_series(months)
    active = c.datasets.optional("icp")
    if active is None:
        print("no active ICP dataset")
        return 1
    series = active.series
    rows = []
    skipped = 0
    for e in evidence:
        p = pd.Period(e["period"], freq="M")
        if p not in series.index or abs(float(series[p]) - float(e["value"])) > 0.005:
            skipped += 1  # only document what the active version actually contains
            continue
        rows.append(
            {
                "period": e["period"],
                "value": e["value"],
                "kind": e["extraction_method"],
                "verification_status": e["verification_status"],
                "source_url": e["source_url"],
                "document_sha256": e.get("document_sha256"),
                "reference": e.get("reference"),
                "retrieved_at": e["retrieved_at"],
                "note": e.get("notes"),
                "dataset_version_id": active.version["id"],
            }
        )
    if args.dry_run:
        have = {c.repo.evidence_key(r) for r in c.repo.all_evidence("icp")}
        new = sum(1 for r in rows if c.repo.evidence_key(r) not in have)
        print(json.dumps({"candidates": len(rows), "would_insert": new, "skipped_value_mismatch": skipped}))
        return 0
    inserted = c.repo.insert_missing_evidence("icp", rows)
    c.repo.audit("system:cli", "dataset.icp.evidence_backfill", "success", target=active.version["id"])
    print(json.dumps({"candidates": len(rows), "inserted": inserted, "skipped_value_mismatch": skipped}))
    return 0


def _restore_check(args: argparse.Namespace) -> int:
    """Audit v7 OPS-01: read-only backup verification (see icp_api.restore_check)."""
    from icp_api.restore_check import restore_check

    report = restore_check(
        Path(args.backup_dir),
        Path(args.live_db) if args.live_db else None,
        Path(args.live_mlflow) if args.live_mlflow else None,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


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
    p = sub.add_parser(
        "audit-prune", help="delete audit rows older than the retention period (dry run unless --apply)"
    )
    p.add_argument("--older-than-days", type=_non_negative, default=None)
    p.add_argument("--apply", action="store_true", help="actually delete (default: report only)")
    p.set_defaults(func=_audit_prune)
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
    p = sub.add_parser(
        "backfill-evidence",
        help="record missing ICP source evidence from the stored ESDM reconciliation (additive, idempotent)",
    )
    p.add_argument("--dry-run", action="store_true", help="report what would be inserted")
    p.set_defaults(func=_backfill_evidence)
    p = sub.add_parser(
        "restore-check",
        help="verify a local backup read-only (integrity, schema version, row counts, objects); never restores",
    )
    p.add_argument("backup_dir")
    p.add_argument(
        "--live-db", default=None, help="live metadata.db to compare row counts with (opened read-only)"
    )
    p.add_argument(
        "--live-mlflow", default=None, help="live mlflow.db to compare row counts with (opened read-only)"
    )
    p.set_defaults(func=_restore_check)
    from icp_api.billing.cli import (
        register_cli as register_billing_cli,
    )  # Wave E: billing-reconcile (dry run)

    register_billing_cli(sub)
    from icp_api.services.provenance import register_cli  # Agent D v6: verify-provenance, backfill-dissent

    register_cli(sub)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
