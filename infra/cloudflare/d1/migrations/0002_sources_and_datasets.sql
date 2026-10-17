-- 0002: open the dataset registry to benchmark/FX series and record value-level provenance.
-- SQLite/D1 cannot drop a CHECK constraint in place, so dataset_versions is rebuilt (D1 wraps the
-- file in a transaction; locally icp-api applies it atomically). Datasets are validated in code
-- (icp_ml.schema.DATASET_SPECS) instead of a hard-coded CHECK list.

CREATE TABLE dataset_versions_v2 (
  id                TEXT PRIMARY KEY,
  dataset           TEXT NOT NULL,
  version           INTEGER NOT NULL,
  object_key        TEXT NOT NULL,
  sha256            TEXT NOT NULL,
  size_bytes        INTEGER NOT NULL,
  row_count         INTEGER NOT NULL,
  period_start      TEXT,
  period_end        TEXT,
  source            TEXT NOT NULL,
  source_detail     TEXT,
  quality_json      TEXT NOT NULL,
  diff_json         TEXT,
  parent_version_id TEXT,
  is_active         INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0, 1)),
  created_at        TEXT NOT NULL,
  created_by        TEXT NOT NULL,
  -- new in 0002
  provenance_json   TEXT,
  UNIQUE (dataset, version),
  UNIQUE (dataset, sha256)
);
INSERT INTO dataset_versions_v2 (id, dataset, version, object_key, sha256, size_bytes, row_count, period_start, period_end,
  source, source_detail, quality_json, diff_json, parent_version_id, is_active, created_at, created_by)
SELECT id, dataset, version, object_key, sha256, size_bytes, row_count, period_start, period_end,
  source, source_detail, quality_json, diff_json, parent_version_id, is_active, created_at, created_by
FROM dataset_versions;
DROP INDEX IF EXISTS ix_dataset_versions_list;
DROP TABLE dataset_versions;
ALTER TABLE dataset_versions_v2 RENAME TO dataset_versions;
CREATE INDEX ix_dataset_versions_list ON dataset_versions (dataset, created_at DESC, id DESC);

-- Retrieved source artefacts (decree PDFs, announcements, workbooks, API responses), content-addressed.
CREATE TABLE source_documents (
  sha256       TEXT PRIMARY KEY,
  url          TEXT NOT NULL,
  provider     TEXT NOT NULL,
  content_type TEXT,
  size_bytes   INTEGER NOT NULL,
  retrieved_at TEXT NOT NULL,
  object_key   TEXT,
  note         TEXT
);

-- Value-level evidence: which document/extraction supports a (dataset, period, value).
CREATE TABLE source_evidence (
  id                  TEXT PRIMARY KEY,
  dataset             TEXT NOT NULL,
  period              TEXT NOT NULL,
  value               REAL,
  kind                TEXT NOT NULL,        -- decree_text | decree_ocr | announcement | announcement_backref | xlsx | web-service | api | legacy_dataset | manual
  verification_status TEXT NOT NULL,        -- verified | single_source | conflict | legacy_unverified | rejected
  source_url          TEXT NOT NULL,
  document_sha256     TEXT,
  reference           TEXT,
  retrieved_at        TEXT NOT NULL,
  note                TEXT,
  dataset_version_id  TEXT,
  created_at          TEXT NOT NULL
);
CREATE INDEX ix_source_evidence_lookup ON source_evidence (dataset, period, created_at DESC);

-- Ingestion runs: what was requested, retrieved, validated, added, changed, rejected, unresolved.
CREATE TABLE ingestion_runs (
  id            TEXT PRIMARY KEY,
  dataset       TEXT NOT NULL,
  connector     TEXT NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'no_change')),
  started_at    TEXT NOT NULL,
  finished_at   TEXT NOT NULL,
  job_id        TEXT,
  requested_by  TEXT NOT NULL,
  summary_json  TEXT NOT NULL,
  version_id    TEXT
);
CREATE INDEX ix_ingestion_runs_list ON ingestion_runs (dataset, started_at DESC, id DESC);

INSERT INTO app_schema (version, name, applied_at)
VALUES (2, '0002_sources_and_datasets', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
