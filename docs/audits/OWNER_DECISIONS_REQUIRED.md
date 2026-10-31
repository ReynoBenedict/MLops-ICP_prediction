# Owner decisions required (2026-10-20)

Status: CURRENT. These questions cannot be answered from the code, the data or public sources. Each needs the
owner, often with counsel or an accountant. Until a decision is recorded here, the product keeps the **safe
default** shown, and the related risk stays open in [FULL_RISK_REGISTER.md](FULL_RISK_REGISTER.md).

Record a decision by adding `Decided (date, by whom): …` under the group.

| # | Topic | Blocks | Safe default today |
|---|---|---|---|
| 1 | Legal identity and contact | LEGAL-01, public launch | "the Operator" + visible placeholders |
| 2 | Geography and customer type | pricing, tax, legal basis | Indonesian B2B teams (current positioning) |
| 3 | Prices, currency, interval, trial, quotas | BILL-01 | "price agreed at activation"; plans set by operator |
| 4 | Payment provider onboarding | BILL-01/02 | no provider, no payment code active |
| 5 | Payment model and lifecycle rules | BILL-02 | none (no payments) |
| 6 | Tax (PPN / e-Faktur) | invoicing | no invoices issued by the product |
| 7 | Privacy, terms, retention, deletion | LEGAL-01/02, SEC-05, SEC-12 | nothing deleted automatically; placeholders |
| 8 | Transactional e-mail | MAIL-01, SEC-04 | no e-mail; out-of-band one-time passwords |
| 9 | Hosting | OPS-06, OPS-01/02, CI-04 | laptop + Quick Tunnel staging only |
| 10 | Operator security, MFA, SLA | SEC-06, SEC-10, OPS-03 | static operator tokens; no SLA |
| 11 | Product claims and legal review | LEGAL-01, marketing | honest-limitations wording in COMMERCIAL_READINESS.md |

---

## 1. Legal identity and contact
- **Decide:** the legal name of the operating entity and data controller (PT or sole proprietor), registered
  address, NPWP/NIB, and one official contact channel for legal, privacy and security reports.
- **Recommended default:** set up a PT (or a registered sole proprietorship) **before** any paid customer, and use a
  role mailbox on the product domain (for example `privacy@`, `security@`).
- **Options and trade-offs:**
  - A PT takes longer to set up, but it meets every payment provider's KYC and B2B procurement requirements.
  - An individual or sole proprietor is faster, but some providers and enterprise buyers will not accept it.
    Stripe Indonesia, for example, needs NPWP-matched names and documents.
- **Why not inferable:** this is a legal fact about the owner. The terms say "the Operator" and show
  `[Owner to complete]` placeholders.

## 2. Geography and customer type
- **Decide:** whether the product is for Indonesia only or also abroad, and whether it serves businesses or also
  consumers or individuals.
- **Recommended default:** **Indonesian businesses only (B2B), prices in IDR**, with no consumer sign-up promise.
- **Trade-offs:**
  - Customers abroad add currency, tax and cross-border data-transfer duties. Stripe Indonesia also does not allow
    cross-border transactions.
  - Consumers add consumer-protection duties and age rules (Terms item `age`).
- **Why not inferable:** this is a business strategy choice.

## 3. Prices, currency, interval, trial and quotas
- **Decide:**
  - prices per plan (Trial, Analis, Profesional, Enterprise), currency, and monthly or annual billing;
  - trial length and what happens when it ends;
  - quotas: seats, API requests per month, reports and scenarios (today's values are in `backend/icp_api/entitlements.py`);
  - whether Enterprise stays "by quotation".
- **Recommended default:** IDR, annual or monthly invoices, a 14-day trial with no card that becomes read-only
  afterwards, and the current quotas, which are already enforced server-side.
- **Trade-offs:**
  - Monthly billing needs recurring infrastructure. Midtrans subscriptions support only cards and GoPay.
  - Annual invoices paid by VA or QRIS are simpler, but cash flow is lumpier.
- **Why not inferable:** the repository deliberately contains no prices; it says "Harga disepakati saat aktivasi"
  (price agreed at activation). The team must not invent prices.

## 4. Payment provider: merchant onboarding, KYC, methods and sandbox credentials
- **Decide:** the provider (see [BILLING_PROVIDER_COMPARISON.md](BILLING_PROVIDER_COMPARISON.md)), who opens the
  merchant account, which methods to enable (VA, QRIS, e-wallets, cards), and who holds the sandbox and production
  keys.
- **Recommended default:** Midtrans Snap. Start with sandbox keys only, given to the backend as secrets
  (`BILLING_ENABLED=false` until go-live). Enable VA and QRIS for B2B.
- **Trade-offs:**
  - Midtrans: an HMAC-like signature that includes the amount, and documented refund idempotency. Recurring
    payments are limited to cards and GoPay.
  - Xendit: webhooks are authenticated only by a static token (needs an API re-check), and its fees could not be
    verified from official pages.
  - Stripe: invite-only in Indonesia.
- **Why not inferable:** onboarding requires the owner's legal documents and bank account. The developers have no
  credentials and must not create accounts.

## 5. Payment model and lifecycle rules
- **Decide:**
  - one-time charges per period or automatic recurring billing;
  - the grace period after a missed payment;
  - cancellation (immediate or at period end);
  - the refund policy (full, partial or none; time windows);
  - proration on upgrade or downgrade;
  - chargeback handling;
  - what a suspended or unpaid organisation can still see.
- **Recommended default:**
  - one-time payment per period (invoice → Snap);
  - a 7-day grace period, then read-only;
  - cancellation at period end;
  - refunds only for duplicate or erroneous charges;
  - no proration: upgrades take effect immediately for the rest of the paid period, downgrades at renewal;
  - chargebacks lead to suspension pending review.
- **Trade-offs:** proration and automatic recurring billing improve the experience but multiply states and
  failure modes. Refund windows are limited by the provider (Midtrans: 7–365 days depending on the method).
- **Why not inferable:** these are contractual and commercial policies.

## 6. Tax: PPN and e-Faktur (with an accountant)
- **Decide:** whether the entity is (or must become) a VAT-registered business (PKP), the PPN rate and its display
  (prices inclusive or exclusive of tax), e-Faktur issuance, the invoice numbering series, and how the provider's
  fees (some VAT-exclusive) are booked.
- **Recommended default:** do **not** issue tax invoices from the product until an accountant has confirmed the
  PKP status and process. Show prices as "excl. PPN" if the business is a PKP.
- **Why not inferable:** this depends on the entity's tax registration and turnover. It requires professional
  advice; the developers must not guess tax rules.

## 7. Privacy, terms, retention, governing law, deletion and subprocessors
- **Decide:**
  - the legal basis per processing purpose;
  - retention periods (account data after an organisation leaves, audit log, reports/forecasts/scenarios/alerts);
  - the response time for data-subject requests;
  - governing law and forum, the liability cap, and the notice period for changes;
  - a grace period before an approved deletion is executed, and whether an owner-transfer flow is needed;
  - whether data exports may show operator names and notes, and other members' session user-agents (SEC-12);
  - the subprocessor list: Cloudflare today, plus future payment and e-mail providers, and the cross-border
    transfer statement.
- **Recommended default:**
  - Indonesian law (UU PDP, if counsel confirms it applies);
  - audit log kept 1 year (`AUDIT_RETENTION_DAYS=365`);
  - account data deleted or anonymised within 30 days of an approved request;
  - a 14-day deletion grace period;
  - only the requester may withdraw their own account-deletion request (SEC-05, a code fix);
  - redact operator names from exports;
  - publish the subprocessor list.
- **Why not inferable:** these are legal choices that need counsel. Fourteen placeholders are listed in
  `docs/LEGAL_AND_PRIVACY.md`.

## 8. Transactional e-mail: provider, domain and sender
- **Decide:** the provider (Resend, Postmark, Amazon SES and others; see the comparison), the sending domain and
  sender address, who manages the DNS records (SPF, DKIM, DMARC), and which mails to send (invitation,
  verification, password reset, alert notifications, billing receipts).
- **Recommended default:** a transactional-only provider on a subdomain (for example `mail.<domain>`), with
  `EMAIL_ENABLED=false` until DNS is verified. Send invitations and password resets first; alerts later.
- **Trade-offs:**
  - Free tiers: Resend 3,000/month and 100/day; Postmark 100/month; SES credits.
  - Mail leaves Indonesia, which the Privacy text must disclose.
  - Without e-mail, account enumeration can only be partly mitigated (SEC-04), and invitations stay manual.
- **Why not inferable:** this needs the owner's domain and an account with the provider.

## 9. Hosting: zero cost or paid always-on
- **Plain statement:** the current backend runs on the operator's laptop behind a **Cloudflare Quick Tunnel**.
  Cloudflare describes Quick Tunnels as for testing and development, with **no uptime guarantee**, and the hostname
  changes on every restart. This is **not reliable production hosting**:
  - the API is down whenever the laptop sleeps or goes offline;
  - payment-provider webhooks cannot reach a stable URL;
  - backups live on the same disk.
- **Decide:** stay zero-cost (a demo or staging only), or pay for an always-on backend.
  - Options include a paid Render instance; a small VPS or office machine with a **named** Cloudflare Tunnel and
    the owner's domain; or another container host.
  - Also decide the backup target and the budget.
- **Recommended default:** before the first paid customer, move to an always-on host with a named tunnel or a
  custom domain, `MODEL_SOURCE=bundle` kept as today, daily D1 exports, and an encrypted off-site copy of the
  MLflow store.
- **Trade-offs:** cost against availability. The Workers, D1 and KV free plans stop at their quotas (no charges);
  a paid host adds a monthly cost.
- **Pre-deployment checklist (owner-run, not run by this mission):**
  1. Back up D1.
  2. Apply migrations 0005–0008.
  3. Set `METRICS_TOKEN`.
  4. Redeploy the image and the Worker.
  5. Run the smoke test.
- **Why not inferable:** this is a budget and risk decision. Hosting prices must be re-checked on the official
  pages when deciding.

## 10. Operator authentication, MFA and SLA
- **Decide:**
  - whether operators move behind Cloudflare Access (an identity provider or one-time code e-mail login, which
    gives MFA) or onto a separate operator host;
  - whether customers get TOTP MFA;
  - whether an SLA, support hours or an on-call rota is offered, and the incident-notification promise;
  - who rotates the secrets, and how often.
- **Recommended default:**
  - Cloudflare Access in front of `/operasi` and the operator API before production;
  - no contractual SLA at launch ("best effort, business hours WIB");
  - quarterly secret rotation, using the documented procedure.
- **Why not inferable:** this is an organisational capacity and risk-appetite choice.

## 11. Product claims and legal review
- **Decide:** approve the marketing statements. Keep the honest limitations: no trading signal; projections
  without a scenario are no better than persistence; ESDM timing; excluded conflict months; intervals that are
  empirical, not calibrated. Approve how ML-02 (the 95% interval under-coverage threshold) and ML-03 (selection
  optimism) are disclosed. Arrange the counsel review of all copy that names ESDM, EIA, the World Bank or Bank
  Indonesia.
- **Recommended default:** reuse the "Honest limitations" section of `docs/COMMERCIAL_READINESS.md` verbatim in sales
  material, add the selection-optimism sentence, and add no accuracy claim beyond the backtest wording.
- **Why not inferable:** marketing claims are legal and reputational commitments.
