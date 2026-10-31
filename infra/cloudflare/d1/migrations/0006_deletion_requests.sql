-- 0006: account / organisation deletion requests (privacy workflow, no e-mail, no hard delete).
-- A member asks for their account to be deleted, or the owner for the whole organisation; a platform
-- operator executes (anonymises tenant personal data, revokes credentials) or declines. Audit rows,
-- usage counters, forecast/report records and every official dataset are kept.
-- Additive only. Deployments must apply it before the API serves /org/deletion-requests:
--   local SQLite: `icp-api migrate` (also applied on API start-up)
--   Cloudflare D1: npx wrangler d1 migrations apply <DB_NAME> --remote -c infra/cloudflare/d1/wrangler.jsonc
CREATE TABLE deletion_requests (
  id              TEXT PRIMARY KEY,
  org_id          TEXT NOT NULL,
  scope           TEXT NOT NULL CHECK (scope IN ('account', 'organization')),
  user_id         TEXT,                                -- account scope: the account to delete; NULL for organization
  requested_by    TEXT NOT NULL,                       -- user id
  reason          TEXT CHECK (reason IS NULL OR length(reason) <= 500),
  status          TEXT NOT NULL CHECK (status IN ('pending', 'executed', 'declined', 'withdrawn')),
  created_at      TEXT NOT NULL,
  resolved_at     TEXT,
  resolved_by     TEXT,
  resolution_note TEXT CHECK (resolution_note IS NULL OR length(resolution_note) <= 500)
);
CREATE INDEX ix_deletion_requests_org ON deletion_requests (org_id, created_at DESC, id DESC);
CREATE INDEX ix_deletion_requests_status ON deletion_requests (status, created_at DESC, id DESC);
-- at most one pending request per (organisation, scope, account)
CREATE UNIQUE INDEX ux_deletion_requests_pending
  ON deletion_requests (org_id, scope, COALESCE(user_id, '')) WHERE status = 'pending';

-- set when an operator executes a deletion request (the row stays, anonymised)
ALTER TABLE organizations ADD COLUMN deleted_at TEXT;
ALTER TABLE users ADD COLUMN deleted_at TEXT;

INSERT INTO app_schema (version, name, applied_at) VALUES (6, '0006_deletion_requests', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
