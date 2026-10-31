# Legal and privacy status

The binding text for users is shown in the app: Terms of Use (`/syarat`) and Privacy Policy
(`/privasi`), source `apps/web/src/pages/legal-content.ts`, Indonesian first with an English toggle.
This document records **status, the implementation facts that text relies on, and the decisions only
the owner can make**. It does not restate the legal text, and it is not legal advice.

**Status: draft, not reviewed by legal counsel.** Both pages show a visible draft note. No operating
entity has been named; the text says "the Operator" (*Operator layanan*). Owner-only items render as
amber "[Diisi pemilik: …]" / "[Owner to complete: …]" placeholders. Nothing in the placeholders was
invented.

## Owner inputs (open decisions)

| id | Page | Section | Owner must decide | Safe default currently shown |
|---|---|---|---|---|
| `entity` | Terms, Privacy | Terms 1 About the service; Privacy 1 Operator and scope | Legal name of the operating entity / data controller | Placeholder; text says "the Operator" |
| `address` | Terms, Privacy | same | Registered address | Placeholder |
| `contact` | Terms, Privacy | Terms 12 Contact; Privacy 10 Security, 11 Your rights, 13 Contact | Official contact channel (e-mail or form) for legal, privacy and security reports | Placeholder; owners/admins use the channel agreed at activation |
| `age` | Terms | 2 Accounts, organisations and roles | Minimum user age | Placeholder |
| `liability` | Terms | 10 Limitation of liability | Liability cap / wording valid under the chosen law | Generic "no indirect losses, to the extent permitted by law" + placeholder |
| `law` | Terms | 10 Limitation of liability | Governing law and dispute forum | Placeholder |
| `notice` | Terms, Privacy | Terms 11 Changes; Privacy 12 Changes | Notice period before changes take effect | "announced in the app" + placeholder |
| `basis` | Privacy | 6 Purposes and legal basis | Legal basis per purpose (e.g. under Indonesian personal-data law, if counsel confirms it applies) | Purposes listed; basis placeholder |
| `hostingBackend` | Privacy | 8 Infrastructure providers | Production analytics-server provider and location | Staging fact stated (Cloudflare Workers + D1/KV; backend on the operator machine behind a Cloudflare tunnel, see [DEPLOYMENT.md](DEPLOYMENT.md)); production placeholder |
| `transfer` | Privacy | 8 Infrastructure providers | Cross-border transfer statement and basis | Placeholder |
| `retentionAccount` | Privacy | 9 Retention | Retention of account data after an organisation leaves | Placeholder |
| `retentionAudit` | Privacy | 9 Retention | Audit-log retention period | Placeholder |
| `retentionContent` | Privacy | 9 Retention | Retention of reports, report files, forecasts, scenarios, alerts | Placeholder |
| `response` | Privacy | 11 Your rights | Response time for access/correction/deletion requests | Placeholder |

Also for the owner:

- Replace the draft date (`legal.updatedDate`, `<time dateTime>` in `LegalPages.tsx`) with the
  effective date once approved, and remove the draft note (`draftNote` in `legal-content.ts`).
- Confirm the "Operator" name used in the landing footer (`landing.footer.operator`) once the entity
  is set.
- Retention periods (`retentionAccount`, `retentionAudit`, `retentionContent`) and the response time for
  requests (`response`) are still open. The code now supports self-service export and deletion requests, so
  the owner only has to set the periods and the operator's handling time; nothing is deleted automatically.
- Decide whether deletion requests need a grace period before the operator executes them, and whether an
  owner-transfer flow is needed (today an owner can only request deletion of the whole organisation).
- Any change in the code must be reflected in `legal-content.ts` and here.

## Implementation facts the text relies on

Re-check these whenever the code changes; the legal text must match the implementation.

| Topic | Fact | Where |
|---|---|---|
| Personal data stored | member e-mail, display name, role, last sign-in time, session user agent (≤ 200 chars), e-mail verification time (`email_verified_at`, unused until e-mail exists), temporary-password expiry | `backend/icp_api/services/accounts.py`; migrations `0004`, `0007` |
| Passwords | scrypt hash with 16-byte salt; never readable | `services/accounts.py` |
| Session tokens, API keys | stored only as SHA-256; shown once; sessions expire after 30 days (absolute) or after `SESSION_IDLE_TIMEOUT_HOURS` without use (default 7 days); members can list and end their sessions; a password change revokes all of the user's sessions, including the current one; temporary passwords expire after `TEMP_PASSWORD_TTL_HOURS` (default 72 h); e-mail tokens (when e-mail is enabled) are stored only as SHA-256 | `services/accounts.py`, `security.py` |
| Audit log | actor, action, target, outcome, request id; no passwords, tokens or raw IP addresses | `audit_events` (migration `0001`), [SECURITY.md](SECURITY.md#audit-and-privacy) |
| Request logs | structured logs with request id; no IP address | `backend/icp_api/middleware.py` |
| IP addresses | used transiently for rate limiting (in-memory per backend process; the gateway keys on client IP, or a hash of the Authorization header for signed-in calls). Failed sign-ins are kept in memory for 15 minutes keyed by SHA-256 of the normalised e-mail plus the client IP; a successful sign-in clears the counter of that e-mail and IP, and a restart clears all of them | `security.py` (`FailureThrottle`), `apps/gateway/src/index.ts` |
| Browser storage | member session token in `localStorage` (`icp.sessionToken`); operator token in `sessionStorage` (`icp.operatorToken`); language preference in `localStorage`; scenario-compare results in `sessionStorage` (`icp.compare.*`, cleared at sign-out) | `apps/web/src/api/client.ts`, `i18n/index.tsx`, `auth.tsx` |
| Not used | cookies, analytics, third-party scripts, e-mail delivery (token service present but disabled: `EMAIL_ENABLED=false` enforced), payment processing (Midtrans **sandbox** adapter present but disabled: `BILLING_ENABLED=false`; enabling it adds Midtrans as a processor of order data and requires updating the Privacy text) | grep of `apps/web/src`, `index.html`, backend |
| Product data | usage events, report runs and report files in the object store, forecasts, scenarios, alerts | migration `0004`, `services/reports.py` |
| Data export | organisation owners download a ZIP of their organisation's data (Account → Data & privasi); generated on request, never stored on the server; no password, token or key hashes; `sessions.json` holds only the requesting owner's own sessions; platform-operator identities and internal notes are redacted (`platform-operator`) | `services/privacy.py`, `GET /org/export` |
| Deletion | members request deletion of their own account, owners of the whole organisation; only the requester can withdraw a request; an operator executes or declines. Execution anonymises in place (e-mail, name, password, status, sessions and API keys; organisation renamed and suspended, scenarios archived and renamed, alert rules disabled, e-mail tokens voided) and keeps audit, usage, report/forecast records and official data. No automatic deletion and no grace period | `GET/POST /org/deletion-requests`, `/admin/deletion-requests`, migration `0006` |
| Audit retention | never deleted automatically; an operator can prune rows older than `AUDIT_RETENTION_DAYS` with `icp-api audit-prune --apply` (dry run by default; 0 keeps everything) | `cli.py` |
| API usage | API-key requests are counted per month and refused at the plan quota | `usage_events`, `security.py` |
| Tenant separation | organisation data is never shown to another organisation (tested) | `backend/tests/test_access_control.py` |

## Attribution (as rendered in the product)

- **Data:** Ministry of Energy and Mineral Resources of the Republic of Indonesia (official ICP);
  U.S. Energy Information Administration (WTI, Brent; public domain); World Bank Commodity Price Data
  – The Pink Sheet (Dubai; CC BY 4.0); Bank Indonesia (JISDOR). Licences and caveats:
  [DATA_SOURCES.md](DATA_SOURCES.md).
- **Photographs:** none rendered. The two photographs the site shows (landing closing band, sign-in
  panel) are CC0 1.0 from Wikimedia Commons, which requires no attribution; source, author and licence
  check are recorded in [ASSET_INVENTORY.md](ASSET_INVENTORY.md). The CC BY / CC BY-SA photographs
  listed there are not served, so their credit is not shown either.
- **Software:** open-source libraries under their own licences (`pyproject.toml`, `package.json`).

The application provides information only and is not investment advice. Official ICP values are set
by the Ministry; forecasts are not official figures.
