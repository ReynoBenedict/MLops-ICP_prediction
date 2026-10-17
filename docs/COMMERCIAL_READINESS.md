# Commercial readiness assessment (2026-10-16)

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
| API access plans (org API keys, usage accounting) | Implemented & tested | Monthly usage per action in `usage_events`; hard monthly quota enforcement **partially implemented** (counted, not blocked) |
| Billing / payment provider | Not implemented | Interface point: `AccountService.set_plan`; a provider (e.g. Stripe/Xendit/Midtrans) would call it from a verified webhook. Requires merchant account, tax/invoice decisions (PPN), and test credentials – none available |
| Usage limits / rate limits | Implemented & tested (rate limits); quotas partial | Edge + backend rate limits; monthly API quota not enforced |
| Scenario analysis, insights, reports (PDF/XLSX/CSV), alerts (in-app) | Implemented & tested | E-mail/webhook delivery of alerts **not implemented** |
| Data source licensing | Implemented (documented) | See `DATA_SOURCES.md`: ESDM (government publication, attribution), EIA (public domain), World Bank (CC BY 4.0, dataset page to confirm), BI JISDOR (public release, attribution). Redistribution of derived monthly values is permitted; raw daily BI data is not resold |
| Visual assets licensing | Implemented (documented) | `ASSET_INVENTORY.md` – CC BY / CC BY-SA photos from Wikimedia Commons with attribution; original SVGs |
| Privacy policy, terms of use | Implemented (in-app pages, Indonesian; summary in `LEGAL_NOTICES.md`) | Legal review by counsel still required before paid launch; legal entity to be named |
| Support & incident documentation | Partially implemented | `OPERATIONS.md` covers runbooks; no ticketing/SLA definitions |
| Deployment | Staging live at zero cost (Cloudflare Worker + D1 + KV, backend container behind a Quick Tunnel, bundle model) | Not an always-on host: the API is offline while the operator machine is off. See `HOSTING_REPORT_2026-10.md` |
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
