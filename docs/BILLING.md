# Billing (sandbox only)

**Status (2026-10-20): implemented for the Midtrans sandbox, disabled by default, not live.**
- `BILLING_ENABLED=false` is the default, and every billing endpoint then answers `503 BILLING_DISABLED`,
  including the webhook.
- The production environment is refused in code.
- No price, plan price or tax rule exists in the code.
- Plans still change only through the audited operator path (`POST /api/v1/admin/organizations/{id}/plan`),
  or through an approved plan-change request.

The provider comparison and its official sources are in
[audits/BILLING_PROVIDER_COMPARISON.md](audits/BILLING_PROVIDER_COMPARISON.md). The decisions only the owner
can make are in [audits/OWNER_DECISIONS_REQUIRED.md](audits/OWNER_DECISIONS_REQUIRED.md) §3–6.

## Components

| Part | Where |
|---|---|
| Provider-neutral domain (statuses, transitions) | `backend/icp_api/billing/domain.py` |
| Configuration (`BILLING_ENABLED`, `MIDTRANS_*`) | `backend/icp_api/billing/config.py` (names and defaults in [ENVIRONMENTS.md](ENVIRONMENTS.md#identity-e-mail-and-billing-settings-backend)) |
| Midtrans Snap adapter (create transaction, status, signature) | `backend/icp_api/billing/midtrans.py` |
| Store (`billing_orders`, `billing_events`) | `backend/icp_api/billing/store.py`, migration `0008_billing_sandbox.sql` |
| Service (checkout, notification handling, reconciliation) | `backend/icp_api/billing/service.py` |
| API router (`/api/v1/billing/*`) | `backend/icp_api/billing/router.py` (endpoints in [API.md](API.md#billing-sandbox-only-disabled-by-default)) |
| Reconciliation CLI | `backend/icp_api/billing/cli.py` (`icp-api billing-reconcile`) |
| Tests (synthetic payloads only, no network) | `backend/tests/test_v7_billing.py`, `backend/tests/test_v7_migrations.py` |

## Flow

1. **Order.** A platform operator creates an order: `POST /billing/orders` with
   `{org_id, amount (integer IDR), currency "IDR", description, plan_code?}`.
   - The amount is an owner decision copied into the order; the adapter only charges what the order says.
   - `plan_code` is informational.
2. **Checkout.** An organisation owner or admin calls `POST /billing/orders/{id}/checkout`.
   - The backend creates a Snap transaction from the stored order only (Basic auth `serverkey:`, sandbox
     URL) and stores the provider-hosted `checkout_url`.
   - The order becomes `pending`. Repeating the call is idempotent.
   - Card or e-wallet data is entered only on Midtrans' page.
3. **Notification.** Midtrans calls `POST /billing/webhooks/midtrans`. The checks below decide whether the
   order status changes.
4. **Plan.** On `paid`, **no plan is changed automatically**. An operator verifies the payment and changes
   the plan through the audited path. Automatic activation needs an owner decision (see the activation gates).

## State machine

```
created ──► pending ──► paid ──► refunded
   │           ├──► failed
   ├──► cancelled ◄┤
   └──► expired  ◄─┘
```

- Allowed moves:
  - `created` → `pending`, `cancelled` or `expired`
  - `pending` → `paid`, `failed`, `expired` or `cancelled`
  - `paid` → `refunded`
- Terminal states: `failed`, `expired`, `cancelled`, `refunded`.
- The same status again is a no-op. Any other move is illegal.
- The webhook records an illegal move as `ignored_transition` and answers `200` with no change, for example a
  late `pending` after `paid`.
- Status updates are compare-and-set, so two concurrent notifications cannot both apply.

## Webhook checks (in this order)

1. **Billing enabled and correctly configured.** Otherwise `503 BILLING_DISABLED` or `BILLING_MISCONFIGURED`.
2. **Signature.** `signature_key = SHA512(order_id + status_code + gross_amount + ServerKey)` is recomputed
   and compared in constant time; a mismatch gives `401 BILLING_SIGNATURE_INVALID`. Source:
   https://docs.midtrans.com/docs/https-notification-webhooks, accessed 2026-10-20.
3. **Order known.** Otherwise `404 BILLING_ORDER_NOT_FOUND`.
4. **Environment.** The order's environment must equal the configured one; otherwise
   `409 BILLING_ENVIRONMENT_MISMATCH`.
5. **Amount and currency.** `gross_amount` and the currency must equal the stored order; otherwise
   `409 BILLING_AMOUNT_MISMATCH`. A malformed notification gives `422 BILLING_NOTIFICATION_INVALID`.
6. **Idempotent event log.** The key is
   (provider, `transaction_id|transaction_status|status_code|fraud_status`), unique in `billing_events`.
   - A replay answers `200` with `duplicate: true` and changes nothing.
   - The stored payload excludes `signature_key`.
7. **Status change** through the state machine (compare-and-set).

Not implemented yet:
- Calling the GET Status API on every notification. Midtrans recommends it; today it is done by the
  reconciliation command instead.
- An IP allow-list. Midtrans publishes no IP list in the pages read.

## Reconciliation

`node scripts/py.mjs -m icp_api.cli billing-reconcile [--limit N]` (default 200) compares local orders with
Midtrans' transaction status (`api.sandbox.midtrans.com/v2/{id}/status`).

**It is a dry run that never writes to the database** (tested). Verdicts:

| Verdict | Meaning |
|---|---|
| `ok` | Local and provider statuses agree |
| `behind_provider` | The provider has moved on; local is behind |
| `conflict` | The statuses disagree |
| `amount_mismatch` | The amounts differ |
| `unknown_at_provider` | The provider has no record of the order |
| `provider_error` | The provider call failed |

Exit codes: `0` all ok · `1` differences found · `2` not configured. Corrections are an operator decision.

## Environment and keys

- Only `MIDTRANS_ENVIRONMENT=sandbox` is accepted. `production` is refused, and the adapter uses sandbox URLs
  only (`app.sandbox.midtrans.com/snap/v1/transactions`, `api.sandbox.midtrans.com/v2/{id}/status`).
- **Key/environment consistency check.** A sandbox configuration requires a server key starting with
  `SB-Mid-server-`.
  - That prefix is the convention observed in Midtrans dashboards. It is **not stated** on
    https://docs.midtrans.com/docs/access-keys (accessed 2026-10-20).
  - The owner should confirm it with their sandbox account. If it is wrong, the check refuses a valid key
    with `503 BILLING_MISCONFIGURED`.
- `MIDTRANS_SERVER_KEY` is a secret. It belongs only in the backend's environment (platform secret, or the
  local `.env`), never in a file in the repository. Test files use obviously fake values.

## Error codes

| Status | Code |
|---|---|
| 503 | `BILLING_DISABLED` |
| 503 | `BILLING_MISCONFIGURED` |
| 404 | `BILLING_ORDER_NOT_FOUND` |
| 401 | `BILLING_SIGNATURE_INVALID` |
| 409 | `BILLING_AMOUNT_MISMATCH` |
| 409 | `BILLING_ENVIRONMENT_MISMATCH` |
| 409 | `BILLING_ORDER_STATE` |
| 422 | `BILLING_NOTIFICATION_INVALID` |
| 502 | `BILLING_PROVIDER_ERROR` |

Messages are in Indonesian.

## Activation gates (all owner decisions; none can be inferred from the code)

| Gate | Needed before |
|---|---|
| Prices, plans, currency, billing interval, trial and quotas (OWNER_DECISIONS §3) | any real order |
| Legal entity, NPWP/NIB, merchant onboarding and KYC with Midtrans (§1, §4) | production keys |
| Tax: PKP status, PPN display, e-Faktur, with an accountant (§6) | issuing invoices |
| Refund, cancellation, grace-period, proration and chargeback policy (§5) | taking payments |
| Production keys stored as platform secrets; production URLs added and tested in sandbox first; the `SB-Mid-server-` convention confirmed | enabling `production` (code change: production is refused today) |
| Automatic plan activation on `paid`: decide and implement (owner-approved mapping order → plan, audited), or keep the manual operator step | self-service upgrades |
| An always-on HTTPS host. The Quick Tunnel URL changes on restart and providers retry for hours (OWNER_DECISIONS §9) | receiving live webhooks |
| Legal texts updated: Privacy "payment processing" and subprocessor list (LEGAL-02) | public launch |
| Customer UI for orders and checkout (not built) | self-service |
| Expiry of stale `created` orders (no scheduler today) | production |

Deployment also needs migration `0008_billing_sandbox.sql` on D1 (after 0005–0007); see
[DEPLOYMENT.md](DEPLOYMENT.md).
