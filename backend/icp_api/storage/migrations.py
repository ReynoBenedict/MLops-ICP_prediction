"""Schema migrations shared by local SQLite and Cloudflare D1.

Migration files live in ``infra/cloudflare/d1/migrations`` and are named ``NNNN_description.sql``.
* Local/test: ``icp-api migrate`` applies pending files atomically with this module.
* D1: ``wrangler d1 migrations apply <db> --remote`` applies the same files. The backend never
  mutates the D1 schema itself; it only verifies the version on readiness checks.

Every migration inserts its own row into ``app_schema``, so both paths record the same version.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from icp_api.settings import PROJECT_ROOT
from icp_api.storage.sql import SqlExecutor, SqliteExecutor, StorageError

MIGRATIONS_DIR = PROJECT_ROOT / "infra" / "cloudflare" / "d1" / "migrations"
_NAME = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    found = []
    for p in sorted(directory.glob("*.sql")):
        m = _NAME.match(p.name)
        if not m:
            raise StorageError(f"Invalid migration file name: {p.name}")
        found.append(Migration(int(m.group(1)), p.stem, p))
    versions = [m.version for m in found]
    if versions != list(range(1, len(found) + 1)):
        raise StorageError(f"Migrations must be numbered contiguously from 0001; found {versions}")
    return found


def current_version(db: SqlExecutor) -> int:
    try:
        rows = db.query("SELECT MAX(version) AS v FROM app_schema")
    except StorageError as exc:
        if "no such table" in str(exc).lower():
            return 0
        raise
    return int(rows[0]["v"] or 0) if rows else 0


def expected_version(directory: Path = MIGRATIONS_DIR) -> int:
    return len(discover(directory))


def apply_local(db: SqliteExecutor, directory: Path = MIGRATIONS_DIR) -> list[str]:
    applied = []
    have = current_version(db)
    for mig in discover(directory):
        if mig.version <= have:
            continue
        db.executescript(mig.path.read_text(encoding="utf-8"))
        if current_version(db) != mig.version:
            raise StorageError(f"Migration {mig.name} did not record itself in app_schema")
        applied.append(mig.name)
    return applied
