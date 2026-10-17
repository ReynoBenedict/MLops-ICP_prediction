-- Application metadata schema (Cloudflare D1 in staging/production, SQLite file locally).
-- Portable SQLite SQL only: no BEGIN/COMMIT (D1 wraps each migration file itself), no
-- extensions, every write performed by the backend is a single atomic statement.
-- Each migration records itself in app_schema so the backend can check the schema version
-- without depending on wrangler's internal migration bookkeeping.

CREATE TABLE IF NOT EXISTS app_schema (
  version    INTEGER PRIMARY KEY,
  name       TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

-- Immutable dataset versions; the CSV itself lives in the object store (R2) at object_key.
CREATE TABLE dataset_versions (
  id                TEXT PRIMARY KEY,
  dataset           TEXT NOT NULL CHECK (dataset IN ('icp', 'wti')),
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
  UNIQUE (dataset, version),
  UNIQUE (dataset, sha256)
);
CREATE INDEX ix_dataset_versions_list ON dataset_versions (dataset, created_at DESC, id DESC);

-- Background jobs. The partial unique index allows at most one queued/running job per type,
-- which is the duplicate-execution guard (enforced atomically by the database).
CREATE TABLE jobs (
  id               TEXT PRIMARY KEY,
  job_type         TEXT NOT NULL,
  status           TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
  params_json      TEXT NOT NULL DEFAULT '{}',
  result_json      TEXT,
  error_code       TEXT,
  error_message    TEXT,
  attempt          INTEGER NOT NULL DEFAULT 0,
  max_attempts     INTEGER NOT NULL DEFAULT 1,
  retry_of         TEXT,
  idempotency_key  TEXT UNIQUE,
  timeout_seconds  INTEGER NOT NULL,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  requested_by     TEXT NOT NULL,
  request_id       TEXT,
  runner_id        TEXT,
  created_at       TEXT NOT NULL,
  started_at       TEXT,
  finished_at      TEXT,
  heartbeat_at     TEXT
);
CREATE UNIQUE INDEX ux_jobs_one_active_per_type ON jobs (job_type) WHERE status IN ('queued', 'running');
CREATE INDEX ix_jobs_list ON jobs (created_at DESC, id DESC);

-- Every forecast execution, successful or failed, with full model/data traceability.
CREATE TABLE forecast_runs (
  id                     TEXT PRIMARY KEY,
  created_at             TEXT NOT NULL,
  request_id             TEXT,
  requested_by           TEXT NOT NULL,
  status                 TEXT NOT NULL CHECK (status IN ('succeeded', 'failed')),
  horizon                INTEGER NOT NULL,
  model_name             TEXT,
  model_version          TEXT,
  model_run_id           TEXT,
  icp_dataset_version_id TEXT,
  wti_dataset_version_id TEXT,
  last_observed_period   TEXT,
  params_json            TEXT NOT NULL,
  result_json            TEXT,
  error_code             TEXT,
  error_message          TEXT,
  export_object_key      TEXT,
  duration_ms            INTEGER
);
CREATE INDEX ix_forecast_runs_list ON forecast_runs (created_at DESC, id DESC);

-- Production alias changes (promotion and rollback history).
CREATE TABLE model_promotions (
  id              TEXT PRIMARY KEY,
  created_at      TEXT NOT NULL,
  model_name      TEXT NOT NULL,
  action          TEXT NOT NULL CHECK (action IN ('promote', 'rollback')),
  from_version    TEXT,
  to_version      TEXT NOT NULL,
  reason          TEXT NOT NULL,
  forced          INTEGER NOT NULL DEFAULT 0,
  validation_json TEXT,
  actor           TEXT NOT NULL,
  request_id      TEXT
);
CREATE INDEX ix_model_promotions_list ON model_promotions (model_name, created_at DESC, id DESC);

-- Audit trail for sensitive actions (including denied attempts). No secrets, no raw IPs.
CREATE TABLE audit_events (
  id           TEXT PRIMARY KEY,
  created_at   TEXT NOT NULL,
  actor        TEXT NOT NULL,
  action       TEXT NOT NULL,
  target       TEXT,
  outcome      TEXT NOT NULL CHECK (outcome IN ('success', 'failure', 'denied')),
  request_id   TEXT,
  details_json TEXT
);
CREATE INDEX ix_audit_events_list ON audit_events (created_at DESC, id DESC);

INSERT INTO app_schema (version, name, applied_at)
VALUES (1, '0001_initial', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
