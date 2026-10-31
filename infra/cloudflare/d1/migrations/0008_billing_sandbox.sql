-- 0008: provider-neutral billing records (2026-10-20, audit v7 Wave E; SANDBOX ONLY, BILLING_ENABLED=false
-- by default). No prices or plans are defined here: an order's amount is set by a platform operator
-- (owner decision) and copied into the order; the provider adapter only charges what the order says.
-- * billing_orders: one payment intent per order, finite state machine enforced in
--   backend/icp_api/billing/domain.py (created -> pending -> paid -> refunded; failed/expired/cancelled).
-- * billing_events: idempotent log of verified provider notifications, keyed by the provider's event
--   identity (UNIQUE (provider, event_key)), so a replayed notification is recorded once and changes nothing.
-- Additive only (two new tables); depends only on organizations (0004), not on 0007.
--   local SQLite: `icp-api migrate` (also applied on API start-up)
--   Cloudflare D1: npx wrangler d1 migrations apply <DB_NAME> --remote -c infra/cloudflare/d1/wrangler.jsonc
CREATE TABLE billing_orders (
  id           TEXT PRIMARY KEY,                       -- also the provider order_id (<= 50 chars)
  org_id       TEXT NOT NULL REFERENCES organizations(id),
  provider     TEXT NOT NULL CHECK (provider IN ('midtrans')),
  environment  TEXT NOT NULL CHECK (environment IN ('sandbox', 'production')),
  amount       INTEGER NOT NULL CHECK (amount > 0),    -- whole rupiah, as charged
  currency     TEXT NOT NULL CHECK (currency IN ('IDR')),
  status       TEXT NOT NULL CHECK (status IN ('created', 'pending', 'paid', 'failed', 'expired', 'cancelled', 'refunded')),
  description  TEXT NOT NULL,
  plan_code    TEXT,                                   -- informational link to a plan change; no price implied
  provider_ref TEXT,                                   -- provider transaction id once known
  checkout_url TEXT,                                   -- provider-hosted payment page (Snap redirect_url)
  created_by   TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);
CREATE INDEX ix_billing_orders_org ON billing_orders (org_id, created_at DESC, id DESC);
CREATE INDEX ix_billing_orders_status ON billing_orders (status, created_at);

CREATE TABLE billing_events (
  id           TEXT PRIMARY KEY,
  provider     TEXT NOT NULL,
  event_key    TEXT NOT NULL,                          -- provider event identity (see billing/midtrans.py)
  order_id     TEXT NOT NULL REFERENCES billing_orders(id),
  status_from  TEXT NOT NULL,
  status_to    TEXT NOT NULL,
  outcome      TEXT NOT NULL CHECK (outcome IN ('applied', 'no_change', 'ignored_transition')),
  payload_json TEXT NOT NULL,                          -- verified notification without the signature
  received_at  TEXT NOT NULL,
  request_id   TEXT,
  UNIQUE (provider, event_key)
);
CREATE INDEX ix_billing_events_order ON billing_events (order_id, received_at);

INSERT INTO app_schema (version, name, applied_at) VALUES (8, '0008_billing_sandbox', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
