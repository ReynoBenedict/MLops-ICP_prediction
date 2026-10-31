# Commercial readiness assessment (2026-10-16, updated 2026-10-20)

Status vocabulary: **Implemented & tested** · **Implemented, awaiting credentials/decision** ·
**Partially implemented** · **Not implemented** · **Blocked**.

## Positioning

ICP Intelligence – intelijen harga minyak mentah Indonesia: official ICP history with document-level
provenance, benchmark and exchange-rate context, estimates for months awaiting publication,
scenario-conditional projections, generated insights, branded reports and alerts, for energy,
finance, planning and policy teams in Indonesia. Not a trading signal; not a substitute for the
official decree.

## Capability matrix

| Area | Status | Notes |
|---|---|---|
| Public landing page with real market snapshot, plans, terms & privacy | Implemented & tested (web) | No invented pricing, testimonials or adoption numbers; plan cards show feature matrices and `price_note_id` text |
| Self-service registration (trial org + owner), login, sessions, password change | Implemented & tested | `PUBLIC_REGISTRATION_ENABLED` toggles sign-up |
| Organisation membership, roles (viewer/analyst/admin/owner), seat limits | Implemented & tested | Invitations hand over a one-time password (no e-mail service) |
| Plans & entitlements enforced server-side (horizon, scenarios, reports, alerts, API access, seats) | Implemented & tested | Plans in `entitlements.py`; operator sets plan via `/admin/organizations/{id}/plan` |
| API access plans (org API keys, usage accounting, monthly quota) | Implemented & tested | Monthly usage per action in `usage_events`; API-key requests above the plan's `api_requests_per_month` get `429 QUOTA_EXCEEDED` until the next UTC month, with quota headers (since 2026-10-20) |
| Plan change requests | Implemented & tested | Owners/admins request a plan in the app; operators approve or decline in the operations console (approval uses the audited `set_plan` path). No payment is taken; migration `0005` must be applied to D1 on deployment |
| Billing / payment provider | Partially implemented (sandbox only, disabled) | Midtrans Snap sandbox adapter with order state machine, signature/amount/environment-checked idempotent webhook and a dry-run reconciliation CLI, behind `BILLING_ENABLED=false`; production refused, no prices, no automatic plan activation, no customer UI. Live use needs the owner gates in [BILLING.md](BILLING.md#activation-gates-all-owner-decisions-none-can-be-inferred-from-the-code) (merchant account/KYC, prices, PPN, refund policy, production keys, always-on host). Comparison: [audits/BILLING_PROVIDER_COMPARISON.md](audits/BILLING_PROVIDER_COMPARISON.md) |
| Transactional e-mail | Implemented, awaiting credentials/decision (disabled) | Reset, verification and invitation token service exists behind `EMAIL_ENABLED=false` (enforced: `true` is refused until a provider is integrated); needs a provider, a sending domain and SPF/DKIM/DMARC (OWNER_DECISIONS §8) |
| Usage limits / rate limits | Implemented & tested | Edge + backend rate limits (single backend instance enforced), sign-in throttle per (e-mail, IP) with an account ceiling, monthly API-key quota |
| Scenario analysis, insights, reports (PDF/XLSX/CSV), alerts (in-app) | Implemented & tested | E-mail/webhook delivery of alerts **not implemented** |
| Data source licensing | Implemented (documented) | See `DATA_SOURCES.md`: ESDM (government publication, attribution), EIA (public domain), World Bank (CC BY 4.0, dataset page to confirm), BI JISDOR (public release, attribution). Redistribution of derived monthly values is permitted; raw daily BI data is not resold |
| Visual assets licensing | Implemented & tested | `ASSET_INVENTORY.md` – only material that needs no credit is served: original SVGs and screenshots, two CC0 photographs from Wikimedia Commons; the CC BY / CC BY-SA photos are kept, not served. `npm run asset-licences` (CI) checks every image file against the licence register |
| Privacy policy, terms of use | Implemented (in-app pages, Indonesian; status and owner inputs in [LEGAL_AND_PRIVACY.md](LEGAL_AND_PRIVACY.md)) | Legal review by counsel still required before paid launch; legal entity to be named |
| Support & incident documentation | Partially implemented | `OPERATIONS.md` covers runbooks, monitoring and a minimal incident procedure; no ticketing, on-call or SLA |
| Data export and deletion requests | Implemented & tested | Owner self-service ZIP export; account/organisation deletion requests executed by an operator (anonymisation, audited, no hard delete). No grace period and no owner transfer; retention periods are an owner decision |
| Deployment | Staging live at zero cost (Cloudflare Worker + D1 + KV, backend container behind a Quick Tunnel, bundle model) | Not an always-on host: the API is offline while the operator machine is off. See [DEPLOYMENT.md](DEPLOYMENT.md) |
| Continuous training workflow | Implemented, awaiting credentials | GitHub Actions calls the deployed job API; requires `ICP_API_BASE_URL`/`ICP_ADMIN_TOKEN` |

## External account requirements (itemised)

1. Cloudflare account: Worker, D1 database, R2 bucket, rate-limit bindings, custom domain.
2. An always-on container host for the backend image with secrets `ADMIN_API_TOKENS`,
   `GATEWAY_SHARED_SECRET` (and D1/R2 or Postgres credentials only if those backends are used).
3. Optional: PostgreSQL for an online MLflow tracking store (not needed in bundle mode).
4. Optional: U.S. EIA API key (free), Tesseract on the host for scanned decrees.
5. Business: legal entity named in terms/privacy, tax registration for invoicing, payment provider
   merchant account, e-mail sending domain (transactional e-mail for invites/alerts).

## Honest limitations to state in sales material

* Future-month projections without a benchmark scenario are no better than persistence; the
  product's accuracy claim applies to elapsed months awaiting publication (see model card).
* Official ICP depends on ESDM publication timing; the platform shows freshness explicitly.
* Historical months with unresolved document conflicts are excluded, not guessed.
