-- Accounts, organisations, entitlements and the business product surface (2026-10):
-- scenarios, alerts, reports, API keys and usage accounting. All tenant data is scoped by org_id;
-- platform operators (admin tokens) are not rows in these tables.

CREATE TABLE organizations (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  slug          TEXT NOT NULL UNIQUE,
  plan          TEXT NOT NULL DEFAULT 'trial' CHECK (plan IN ('trial', 'starter', 'professional', 'enterprise')),
  status        TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL,
  settings_json TEXT
);

CREATE TABLE users (
  id                   TEXT PRIMARY KEY,
  org_id               TEXT NOT NULL REFERENCES organizations(id),
  email                TEXT NOT NULL UNIQUE,            -- stored lower-cased
  name                 TEXT NOT NULL,
  password_hash        TEXT NOT NULL,                   -- scrypt, see services/accounts.py (never logged)
  role                 TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'analyst', 'viewer')),
  status               TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
  must_change_password INTEGER NOT NULL DEFAULT 0,
  created_at           TEXT NOT NULL,
  last_login_at        TEXT
);
CREATE INDEX ix_users_org ON users (org_id, created_at);

CREATE TABLE sessions (
  id           TEXT PRIMARY KEY,
  user_id      TEXT NOT NULL REFERENCES users(id),
  token_hash   TEXT NOT NULL UNIQUE,                    -- sha256 of the opaque bearer token
  created_at   TEXT NOT NULL,
  expires_at   TEXT NOT NULL,
  last_seen_at TEXT,
  revoked_at   TEXT,
  user_agent   TEXT
);
CREATE INDEX ix_sessions_user ON sessions (user_id, created_at DESC);

CREATE TABLE api_keys (
  id           TEXT PRIMARY KEY,
  org_id       TEXT NOT NULL REFERENCES organizations(id),
  name         TEXT NOT NULL,
  prefix       TEXT NOT NULL,                           -- first 12 chars, shown in the UI
  key_hash     TEXT NOT NULL UNIQUE,
  created_by   TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  last_used_at TEXT,
  revoked_at   TEXT
);
CREATE INDEX ix_api_keys_org ON api_keys (org_id, created_at DESC);

CREATE TABLE saved_scenarios (
  id               TEXT PRIMARY KEY,
  org_id           TEXT NOT NULL REFERENCES organizations(id),
  created_by       TEXT NOT NULL,
  name             TEXT NOT NULL,
  description      TEXT,
  horizon          INTEGER NOT NULL,
  inputs_json      TEXT NOT NULL,                       -- {"brent": {"YYYY-MM": usd}, ...}
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  last_forecast_id TEXT,
  archived_at      TEXT
);
CREATE INDEX ix_saved_scenarios_org ON saved_scenarios (org_id, created_at DESC, id DESC);

CREATE TABLE alert_rules (
  id                TEXT PRIMARY KEY,
  org_id            TEXT NOT NULL REFERENCES organizations(id),
  created_by        TEXT NOT NULL,
  kind              TEXT NOT NULL CHECK (kind IN ('icp_published', 'icp_change_pct', 'benchmark_change_pct', 'forecast_change',
                                                 'data_stale', 'ingestion_failed')),
  params_json       TEXT NOT NULL,
  enabled           INTEGER NOT NULL DEFAULT 1,
  created_at        TEXT NOT NULL,
  updated_at        TEXT NOT NULL,
  last_evaluated_at TEXT,
  last_triggered_at TEXT
);
CREATE INDEX ix_alert_rules_org ON alert_rules (org_id, created_at DESC, id DESC);

CREATE TABLE alert_events (
  id          TEXT PRIMARY KEY,
  rule_id     TEXT NOT NULL REFERENCES alert_rules(id),
  org_id      TEXT NOT NULL REFERENCES organizations(id),
  created_at  TEXT NOT NULL,
  dedupe_key  TEXT NOT NULL,                            -- one event per (rule, fact), e.g. the ICP period
  severity    TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
  title       TEXT NOT NULL,
  body        TEXT NOT NULL,
  data_json   TEXT,
  read_at     TEXT,
  UNIQUE (rule_id, dedupe_key)
);
CREATE INDEX ix_alert_events_org ON alert_events (org_id, created_at DESC, id DESC);

CREATE TABLE report_runs (
  id             TEXT PRIMARY KEY,
  org_id         TEXT,                                  -- NULL for platform-generated reports
  created_by     TEXT NOT NULL,
  kind           TEXT NOT NULL CHECK (kind IN ('market_monthly', 'forecast', 'scenario_comparison', 'data_provenance')),
  params_json    TEXT NOT NULL,
  status         TEXT NOT NULL CHECK (status IN ('succeeded', 'failed')),
  created_at     TEXT NOT NULL,
  finished_at    TEXT,
  object_key_pdf TEXT,
  object_key_xlsx TEXT,
  object_key_csv TEXT,
  error_message  TEXT,
  summary_json   TEXT
);
CREATE INDEX ix_report_runs_org ON report_runs (org_id, created_at DESC, id DESC);

CREATE TABLE usage_events (
  id         TEXT PRIMARY KEY,
  org_id     TEXT,
  user_id    TEXT,
  api_key_id TEXT,
  created_at TEXT NOT NULL,
  action     TEXT NOT NULL,                             -- forecast.run | report.generate | api.request | ...
  units      INTEGER NOT NULL DEFAULT 1,
  request_id TEXT
);
CREATE INDEX ix_usage_events_org ON usage_events (org_id, created_at DESC, id DESC);

ALTER TABLE forecast_runs ADD COLUMN org_id TEXT;
ALTER TABLE forecast_runs ADD COLUMN scenario_id TEXT;
CREATE INDEX ix_forecast_runs_org ON forecast_runs (org_id, created_at DESC, id DESC);

INSERT INTO app_schema (version, name, applied_at) VALUES (4, '0004_accounts_and_product', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
