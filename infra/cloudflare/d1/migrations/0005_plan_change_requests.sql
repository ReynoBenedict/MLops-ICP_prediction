-- 0005: plan change requests (no payment). A customer owner/admin asks for another plan; a platform
-- operator approves (the plan changes through the audited operator path) or declines with a note.
-- Additive only. Deployments must apply it before the API serves /org/plan-requests:
--   local SQLite: `icp-api migrate` (also applied on API start-up)
--   Cloudflare D1: npx wrangler d1 migrations apply <DB_NAME> --remote -c infra/cloudflare/d1/wrangler.jsonc
CREATE TABLE plan_change_requests (
  id              TEXT PRIMARY KEY,
  org_id          TEXT NOT NULL,
  requested_by    TEXT NOT NULL,                       -- user id
  previous_plan   TEXT NOT NULL,                       -- plan when the request was made
  requested_plan  TEXT NOT NULL CHECK (requested_plan IN ('trial', 'starter', 'professional', 'enterprise')),
  note            TEXT CHECK (note IS NULL OR length(note) <= 500),
  status          TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'declined', 'withdrawn')),
  created_at      TEXT NOT NULL,
  resolved_at     TEXT,
  resolved_by     TEXT,
  resolution_note TEXT CHECK (resolution_note IS NULL OR length(resolution_note) <= 500)
);
CREATE INDEX ix_plan_change_requests_org ON plan_change_requests (org_id, created_at DESC, id DESC);
CREATE INDEX ix_plan_change_requests_status ON plan_change_requests (status, created_at DESC, id DESC);
-- at most one pending request per organisation
CREATE UNIQUE INDEX ux_plan_change_requests_pending ON plan_change_requests (org_id) WHERE status = 'pending';

INSERT INTO app_schema (version, name, applied_at) VALUES (5, '0005_plan_change_requests', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
