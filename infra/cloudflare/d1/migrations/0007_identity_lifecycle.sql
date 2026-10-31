-- 0007: identity lifecycle (2026-10-20, audit v7 Wave B).
-- * Temporary passwords (admin invitation / reset) expire: users.temp_password_expires_at.
-- * E-mail verification state: users.email_verified_at (NULL = not verified; no e-mail is sent while
--   EMAIL_ENABLED=false, the default).
-- * Single-use, expiring e-mail tokens for verification, password reset and invitation delivery. Only
--   the SHA-256 of a token is stored; the clear token exists only in the outgoing message.
-- Session idle timeout uses the existing sessions.last_seen_at; password-hash versioning lives in the
-- users.password_hash text itself (legacy hashes keep working and are upgraded on sign-in).
-- Additive only (two nullable columns, one new table): every existing row is preserved.
--   local SQLite: `icp-api migrate` (also applied on API start-up)
--   Cloudflare D1: npx wrangler d1 migrations apply <DB_NAME> --remote -c infra/cloudflare/d1/wrangler.jsonc
ALTER TABLE users ADD COLUMN temp_password_expires_at TEXT;
ALTER TABLE users ADD COLUMN email_verified_at TEXT;

CREATE TABLE email_tokens (
  id         TEXT PRIMARY KEY,
  user_id    TEXT NOT NULL REFERENCES users(id),
  purpose    TEXT NOT NULL CHECK (purpose IN ('verify', 'reset', 'invite')),
  token_hash TEXT NOT NULL UNIQUE,                      -- sha256 of the single-use token
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  used_at    TEXT,                                      -- set when consumed or superseded
  request_id TEXT
);
CREATE INDEX ix_email_tokens_user ON email_tokens (user_id, purpose, created_at DESC);

INSERT INTO app_schema (version, name, applied_at) VALUES (7, '0007_identity_lifecycle', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
