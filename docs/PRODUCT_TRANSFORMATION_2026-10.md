# ICP Intelligence product transformation (2026-10)

Report of the full-stack product transformation carried out from 2026-10-23 to 2026-10-27. All work was done
and verified **locally**. Nothing was deployed, no Cloudflare or Render resource was changed, no remote D1
migration was run, no secret was changed, and no Git operation that changes state was run. The staging site
(`https://icp-gateway-staging.icp-gateway.workers.dev`) still serves the 2026-10-17 release.

Evidence for every claim below is in [evidence/2026-10-25-transformation/](evidence/2026-10-25-transformation/INDEX.md)
(before and after captures, measurements) or is named with the command that produced it. Where a number comes
from a local measurement it says so; nothing was measured on staging.

Contents: [1 Executive summary](#1-executive-summary) · [2 Baseline findings](#2-baseline-findings) ·
[3 Product and design](#3-product-and-design) · [4 Frontend](#4-frontend) · [5 Backend, gateway and API](#5-backend-gateway-and-api) ·
[6 Data quality](#6-data-quality) · [7 Security](#7-security) · [8 Performance](#8-performance-measurements) ·
[9 Tests](#9-tests) · [10 Browser and responsive verification](#10-browser-and-responsive-verification) ·
[11 Screenshots and evidence](#11-screenshots-and-evidence) · [12 Known limitations](#12-known-limitations) ·
[13 Outstanding decisions](#13-outstanding-decisions) · [14 Reproducing locally](#14-reproducing-locally)

## 1. Executive summary

ICP Intelligence was a working analytics application whose public face did not show the product, whose
states broke under a slow or unreachable backend, and whose interface had no consistent design language.
The transformation makes the official ICP, its evidence and its context the centre of the product, and
makes every state honest and designed.

- **Public site.** The first screen now shows the real product: the official ICP for the latest month
  (September 2026: USD 112.81/bbl) with its two ESDM source documents, the change from the previous month,
  Brent, the rupiah equivalent, and a 24-month ICP vs Brent chart drawn without a chart library. Guests
  see that a forecast exists, when it was issued and for how many months, but never its values. The stock
  photographs were replaced by real screenshots of the application. Every state (loading, slow, stale
  stored copy, outage, missing history, conflicting sources) is designed and explained.
- **Workspace.** A new top bar shows where the reader is and how current each data source is. The
  overview leads with the headline number and its evidence. Forecasts keep their tab and interval level in
  the URL and refuse to show results next to inputs that have changed. Scenario results and comparisons are
  labelled stale whenever their inputs change, including after a reload. Reports have explicit states
  (rejected, in progress, unknown outcome, failed, ready), never offer files for a failed run, and cannot
  be generated twice by a retry or a download click.
- **Reliability.** A returning member no longer stares at a grey page when the backend hangs, a member is
  no longer sent to the sign-in page during an outage, and the gateway keeps a last-known-good copy of the
  two public endpoints so the landing has figures to show when the backend is offline.
- **Data.** A parser defect that attached the wrong decree number to 28 evidence rows (including the
  September 2026 decree) was found, fixed and repaired with a backup; no price changed.
- **Security.** A Worker entry-point crash that would have blocked the next deployment was fixed, along
  with gateway hardening, a public-endpoint quota-exhaustion risk, dependency vulnerabilities and a
  duplicate-report race. No P0 or P1 security issue remained in the final review.
- **Performance and accessibility.** The landing ships less JavaScript than before (154.2 kB gzip
  against 162.2 kB at baseline) while showing much more; `/ringkasan` on a slow mobile profile is ready
  28 % sooner. axe reports 0 WCAG violations on every route; keyboard focus is always visible and never
  hidden under a sticky header; text follows the browser's text-size setting (checked at 200 % on 21
  routes at three widths).

What is not done: staging is not redeployed (the public URL still shows the old defects until the owner
deploys), the Terms and Privacy pages still carry owner placeholders, and the desktop `/ringkasan` largest
paint is 180 ms slower than at baseline (section 8). Section 13 lists the decisions that need the owner.

## 2. Baseline findings

The baseline was taken on 2026-10-23/25 against a frozen production build of the original UI (served
locally on `:4173`), the local backend, and the public staging URL (read only). Checks then: typecheck,
lint and build passed; 144 web tests, 33 gateway tests and 1,169 Python tests passed.

**Product and UX findings** (full write-up with causes and evidence:
[evidence/2026-10-25-transformation/INDEX.md](evidence/2026-10-25-transformation/INDEX.md); status as
verified on the final local build):

| ID | Finding | Status |
|---|---|---|
| B-P0-1 | Returning member with a hanging API: whole-page skeleton on the landing for 35 s, about 70 s before any message | Fixed: landing after at most 2 s, explained wait at about 10 s |
| B-P0-2 | Staging landing shows skeleton figures forever (API 530) | Partly: fixed in code and verified locally; **staging not redeployed** |
| B-P0-3 | Every report download click generated (and counted) another report | Fixed: one `POST /reports` per generate |
| B-P1-1 | Scenario results and comparisons not marked stale after an edit, even after a reload | Fixed |
| B-P1-2 | A signed-in member is sent to sign-in during an outage or hang | Fixed: stays, explains, retries |
| B-P1-3 | Ghost buttons had no visible keyboard focus (WCAG 2.4.7) | Fixed |
| B-P1-4 | Guest landing during a hang: skeletons for 35 s, no message | Fixed: explained at 10 s |
| B-P2-1 | Outage state: two identical alerts, "-" values, contradictory copy | Fixed |
| B-P2-2 | English locale showed Indonesian dataset names | Fixed |
| B-P2-3 | Mobile overview: onboarding pushed the ICP value below the first screen | Fixed |
| B-P2-4 | Performance: `/ringkasan` slow on a slow mobile profile; heavy JavaScript | Partly: see section 8 |
| B-P2-5 | Declared font "Inter" never loaded (typography differed per OS) | Fixed: self-hosted IBM Plex |
| B-P2-6 | Invalid `<dl>` structure (axe serious) on `/metodologi` and `/akun` | Fixed |
| B-P3-1 | Forecast interval level not in the URL | Fixed (`?tingkat=`) |
| B-P3-2 | Uppercase, letter-spaced labels | Fixed |
| B-P3-3 | Hero photograph covered by its caption on phones | Fixed (no photograph; market data panel) |
| B-P3-4 | Forecast tables cut at 1024 px | Fixed |
| B-P3-5 | Text-only zoom (browser text size) had no effect | Fixed: type in `rem` (section 10) |
| B-P3-6 | Visual tokens differed from the new design system | Fixed |

**Engineering findings from the same baseline** (each fixed during the transformation unless noted):

- **GW-01 (deployment blocker):** the Worker's main module exported non-function values, which crashes
  `workerd` at start (`Incorrect type for map entry 'CLIENT_IP_HEADER'`); dry-run builds did not catch it.
- **Decree references:** evidence rows cited the ICP *formula* decree (276.K) instead of the month's own
  decree for August and September 2026, 61.K/2022 for 23 months of 2022-2024, and a cited formula decree
  (71K/12/MEM/2020) for March 2020 (section 6).
- **Readiness:** the first `/health/ready` after a start took 6-9 s and could report `degraded` because the
  checks ran one after another.
- **Failed reports offered files**, and a failed run's download was attempted.
- **Timeouts:** the browser used 35 s for every request (including the public snapshot and the session
  check), the report timeout equalled the gateway's (a race), and data-plane retries could exceed the
  backend's 30 s request budget.
- **Dependencies:** pip-audit reported 39 known vulnerabilities (Pillow 12.3.0, mlflow 3.17.1, certifi
  2026.7.22).
- **Landing weight:** 516 kB of JavaScript (162 kB gzip) in 10 files on the first screen, including all
  workspace strings and the signed-in shell; no web font was ever loaded.

## 3. Product and design

**Positioning.** ICP Intelligence is presented as what it is: a workspace around the *official* monthly
ICP, published by ESDM a few weeks after the month ends, with the documents that prove each value, the
benchmarks and exchange rate that explain it, and forecasts that are explicitly conditional on their
assumptions. The interface keeps seven kinds of values visibly distinct: official observations,
benchmark series, derived values (rupiah equivalent, spreads), estimates awaiting publication, model
forecasts, scenario assumptions and uncertainty ranges. Nothing is called real-time; the public data is
called "the latest official ICP", and a stored copy says when it was stored.

**Design system.** [DESIGN_SYSTEM.md](DESIGN_SYSTEM.md) defines the tokens and rules, and every stylesheet
follows it:

- IBM Plex Sans (one variable file) for the interface and IBM Plex Serif 600 for public headings and
  insight headlines only, self-hosted under the SIL Open Font License.
- A cool canvas (`#F3F5F8`), navy for brand and headings, teal only for interaction, amber only for the
  brand mark and the decree stamp; validated data-series colours; flat panels; sentence case everywhere.
- Type and control heights in `rem` so text follows the reader's text-size setting; 40 px minimum touch
  targets on coarse pointers; a 2 px teal focus ring that is never hidden under a sticky header.
- Shared primitives for every recurring state: skeletons inside labelled status regions and always
  replaced, one page-level error with a retry when several requests fail, a slow-loading notice after 8 s,
  stale-result callouts that name what changed.

**Public site.**

- *Landing*, in this order: the market panel (the latest official ICP, its evidence chip, the two
  ESDM documents with links, change from the previous month, Brent, rupiah equivalent at the JISDOR
  average, and the 24-month ICP vs Brent chart with a table view, keyboard reading and a hatched "forecast
  window" that only says the forecast exists); how a figure goes from ESDM documents to the panel, with the
  month's real documents and confirmation counts; a product tour with real screenshots of the workspace
  (Indonesian and English); forecast honesty (the measured error with and without benchmark knowledge,
  next to "repeat last month"); plans from `/plans`; limits and sources.
- *Sign-in and registration*: a split layout from 1100 px with the latest official ICP and a sparkline, and the trial
  plan's real limits; the forms keep their validation and focus behaviour.
- *Methodology and legal pages*: a document layout with a sticky contents column.

**Workspace.**

- *Shell*: a top bar with the location ("Pasar › Ringkasan") and a data-status control whose popover lists
  each source with its period and status; operators see the environment and version instead. Below 900 px
  the sidebar becomes a modal drawer.
- *Overview*: the headline ICP with its evidence and change, benchmarks with their spread (in USD/bbl), the
  12-month range, the price chart with its range in the URL, the latest forecast with its band, the insight
  brief and the data-status strip; onboarding follows the figures.
- *Price history*: chart and monthly table with each month's evidence; a deep link `#bukti` focuses the
  evidence panel; future months are blank in the coverage grid, missing months are marked.
- *Forecast*: tabs (result, assumptions, accuracy, history) and the interval level in the URL; a run is
  compared with the form only when both start from the same official month, and a changed form gets a stale
  notice that names the changed inputs instead of sitting silently next to old results.
- *Scenarios*: two panes; every result shows the assumptions it was computed with; runs and comparisons are
  labelled stale after an edit or archive (also after a reload, without re-running anything); plan limits
  are visible before they block.
- *Insights*: filters with counts; the detail sheet names the kind of evidence, the month, the comparison
  window, the figures, why it matters (from the rule's own entry), method, limits and sources.
- *Reports*: option cards per report kind; one state per outcome; downloads per format with progress;
  every request carries an idempotency key.
- *Notifications, account, operator console*: restyled; record cards on phones; inline validation; every
  sideways-scrolling table is a named, focusable region.

## 4. Frontend

`apps/web` (React 19, Vite 8, React Router 7 data router, TypeScript).

- **Styles:** six legacy stylesheets with override layers were replaced by eight area files built on the
  tokens (`styles/tokens.css`, `base.css`, `components.css`, `public.css`, `shell.css`, `market.css`,
  `flows.css`, `ops.css`). Only the first four load with the landing; the others load with the lazily
  loaded modules that use them.
- **Layout split:** `components/public-layout.tsx` (public header, menu, footer), `components/layout.tsx`
  (signed-in shell, lazy), `components/chrome.tsx` (language toggle, route focus, sticky-header
  measurement).
- **API client (contract C3, `api/client.ts`):** per-request timeouts (public 10 s, session check 8 s,
  default 35 s, computations 60 s, reports 95 s, downloads 60 s; the report timeout sits above the
  gateway's 90 s); `publicGetMeta` reads the gateway's stale-copy headers; extra request headers (for
  `Idempotency-Key`) without letting a caller replace `Authorization`, `Accept` or `Content-Type`; shared
  GETs are dropped when a credential is set or removed.
- **Session handling (`auth.tsx`, `App.tsx`):** only a definite 401 ends a session; a credential the server
  could not check keeps the member in place with an explanation and background re-checks (5, 15, 30,
  60 s); the home page waits at most 2 s for the check and then shows the landing; a workspace deep link
  shows "Memeriksa sesi Anda di server…" if the check takes longer than 1.5 s, while a slow page chunk
  inside a confirmed session shows "Memuat halaman…" instead (`boot-fallback.test.tsx`).
- **Buttons default to `type="button"`:** the cause of the duplicate reports (B-P0-3); submit buttons say
  `type="submit"`.
- **Requests that must not run twice:** report requests carry one idempotency key per submission, reused
  only for the same form; a gateway 504, a broken connection or the browser timeout becomes an "outcome
  unknown" notice that re-reads the list instead of an outage message; a retry while the server is still
  generating the first request gets "still being generated" and waits the 15 s the server asks for.
- **Code splitting:** the signed-in shell, the sign-in pages, the workspace pages and their strings and
  stylesheets are separate chunks, prefetched ahead of need (the shell and the page of a workspace URL
  while the session is checked; the auth chunk when the landing is idle or a sign-in link is pointed at).
  Guards: `i18n/lazy-strings.test.ts` (no eagerly loaded module uses a lazily registered string),
  `vite.config.ts` `checkShellChunk` (no public chunk loads the shell; no cycle), and
  `scripts/check-landing-bundle.mjs` in CI with a 160,000-byte gzip budget for the landing JavaScript and a
  check that no public page loads the charting library.
- **Accessibility mechanics:** `scroll-padding-top` from the measured sticky header (`--sticky-top`), deep
  links that stay aligned while content above them loads (`lib/anchor.ts`), WAI-ARIA tabs, focus moved to
  results and explanations, plural-aware strings (`{n|one|other}`).

## 5. Backend, gateway and API

**Contract C1: public market snapshot** (`GET /api/v1/public/market-snapshot`, backward compatible). New
optional fields: `recent` (the last `PUBLIC_HISTORY_MONTHS` months, default 24, of official ICP and Brent;
a missing month is `null`, never 0 or an estimate), `coverage` (first and last month, months, confirmed
months, computed by the same service as `/data/verification`), `icp_evidence` (the latest month's basis and
documents; a decree number only when it is verifiably the document's own; links only to `https://*.go.id`)
and `generated_at`. The forecast teaser still carries no values. The snapshot is memoised in the process
for at most 60 s and invalidated by every write that can change it, so a repeat request runs no SQL
(before: about 280 rows read per anonymous request).

**Contract C2: gateway last-known-good copy** (`apps/gateway`). For exactly the two public GETs without an
`Authorization` header, a live answer is returned as is and stored in KV with its byte hash and a content
hash that ignores `generated_at`; a write happens only when the content changed (or the copy is 24 h old),
at most once per 5 minutes per key, and a failed write is remembered for 5 minutes. When the backend fails
(network error, 8 s timeout, status ≥ 500, or a non-JSON error page) the stored copy is served with
`X-ICP-Cache: stale` and `X-ICP-Cached-At`, and the browser says so. Any non-JSON error page is turned into
the standard error envelope, so no HTML (which can name the tunnel host) reaches the browser.

**Other backend changes** (`backend/icp_api`):

- Readiness checks run concurrently with a background model warm-up; the first probe reports `degraded`
  (loading) honestly instead of failing a sequential check.
- A per-request read cache removes repeated "which dataset versions are active" queries: a market report
  went from 60 SQL statements to 9, the snapshot from 12 to 3 before the memo (each statement is a D1 round
  trip when deployed).
- Failed report runs carry no file keys and list no formats; downloading one returns 404; a succeeded run
  whose file is missing returns `503 REPORT_OBJECT_MISSING`.
- Request deadline: after `REQUEST_TIMEOUT_SECONDS` (30 s) the data-plane clients start no read, retry or
  back-off, but a write's first attempt always starts, so a write sequence is never cut in the middle and a
  report cut at 30 s still records its run.
- `POST /reports` with an `Idempotency-Key` holds a per-key reservation while it runs; a retry during that
  time gets `409 IDEMPOTENCY_IN_PROGRESS` with `Retry-After: 15` instead of a second report.
- In a month whose official documents conflict, the snapshot withholds the headline value, its change and
  the rupiah equivalent, and lists no documents.
- `icp-api repair-decree-references [--apply]` (section 6) takes and verifies a backup before writing on a
  local store, refuses on a deployed store without `--backup-taken`, and always writes an audit row with the
  counts actually applied.
- Dependencies: mlflow 3.17.2, certifi 2026.10.19, Pillow 13.0.0 on Python ≥ 3.11 (CI and the API image);
  a Python 3.10 development venv keeps Pillow 12.3.0 (accepted residual risk DEP-01: no untrusted image is
  decoded).

**Gateway hardening:** a separate entry module (`src/worker.ts`) that exports only the handler (GW-01);
strict parsing of numeric variables; one deadline across retries (below Cloudflare's 100 s limit); an
https-only `BACKEND_URL` without credentials in deployed environments; the data plane refuses the
`edge-cache/` prefix; static headers add COOP, CORP, a stricter CSP and immutable caching for fonts.

API changes are backward compatible; `packages/contracts` was regenerated and `npm run contracts:check`
reports it up to date. Timeout budgets and the KV write budget are documented in
[OPERATIONS.md](OPERATIONS.md) and [DEPLOYMENT.md](DEPLOYMENT.md).

## 6. Data quality

- **Decree references corrected.** The parser took the first `NOMOR …K/…` in a decree's text. When the
  decree's own number was not readable (spaced digits, empty e-signature field), that first match was the
  ICP formula decree cited in its considerations. The new parser takes the number only from the title block
  naming the same month (or the annex header), and reports none if they disagree. Re-run on all 86 stored
  decrees: the 47 OCR-read decrees keep their numbers; 26 text-layer records change. The local store was
  repaired with `repair-decree-references --apply` after a backup (`var/backups/20261025T075817Z`): 28
  `source_evidence.reference` values changed (2026-08 → 352.K/MG.03/MEM.M/2026, 2026-09 →
  395.K/MG.03/MEM.M/2026, 2020-03 and 23 months of 2022-2024 → none), plus the same 26 numbers in the ESDM
  state files. **No price, status, basis or dataset version changed**; `verify-provenance` reports 93
  months and 0 disagreements. The staging D1 database still holds the old references (no remote calls were
  allowed); apply the same repair when staging is next migrated or cloned.
- **Public figures are traceable.** The landing's numbers come from the snapshot and match the stored data:
  ICP September 2026 = 112.81 (August 89.43, +26.1 %), Brent 114.16, JISDOR average 17,749, decree
  395.K/MG.03/MEM.M/2026 and the announcement of 13 October 2026; coverage 2019-01 to 2026-09, 93 months, 86
  confirmed (84 by two agreeing documents, 2 by the decree text), identical to `/data/verification`.
- **Gaps stay gaps.** A month without an official value is `null` in the API and a break in every chart and
  sparkline; nothing is interpolated or shown as 0.
- **Backtest figures have one source.** The landing, the methodology page and the forecast page show the
  registered production model v5's own backtest on its 69 cases (known benchmarks 0.84 USD/bbl at one
  month, unknown benchmarks 6.11), against 6.06 for "repeat last month" on the same 69 cases, recomputed
  from v5's stored backtest predictions. Until the integration review these pages showed 6.09, which is
  the registry's persistence figure on the 66 cases common to all candidate methods (where v5 scores 6.12):
  each number was right, but the pair compared different cases. The server-written accuracy insight now
  uses the same-case pair too; only the operator model page shows the registry metrics as stored. The
  landing dates its figures and says they do not update automatically
  ([FORECASTING_METHODOLOGY.md](FORECASTING_METHODOLOGY.md) §5 gives every figure with its source).
- **Corrections in documents:** the README now says 86 months confirmed (84 by two documents, 2 by the
  decree text) instead of "by two sources"; the landing's first step says documents are collected "when
  available" and applies the four steps to the ICP figure only.
- **Local test data changed for verification** (local test organisation "PT Uji UX V3 (lokal)" only, each
  time after a backup: `var/backups/20261025T073508Z`, `20261025T130916Z`, `20261026T180150Z`): QA
  forecasts, scenario runs and comparisons, report runs, two scenarios renamed to names that match their
  inputs ("Brent USD 120", "Brent USD 70", the latter archived), sessions minted and revoked by the sign-out
  journey. No other organisation was touched; no dataset, model artefact or report history was deleted.

## 7. Security

No P0 or P1 security issue remains open after three independent reviews (wave B package reviews, the
wave C security and data-honesty review, the wave D integration review). The wave D review of the final
integration changes (code and data claims, rendering and accessibility) found no P0 or P1 issue of any
kind. It found 21 P2/P3 issues: large-text clipping, chart labels at 200 %, conflict-month wording, the
6.06/6.09 baseline pair, a misplaced session note and documentation drift. All of them were fixed and
re-measured. Security findings and their status:

| ID | Severity | Finding | Status |
|---|---|---|---|
| GW-01 | High | Worker entry module crashes `workerd` at start (deployment blocker) | Fixed, with a test that reads `wrangler.jsonc` |
| GW-02 | Medium | Upstream 52x/530 HTML (can name the tunnel host) passed to the browser | Fixed (JSON envelope) |
| GW-07 | Medium | 39 known dependency vulnerabilities | Fixed for CI and the API image (pip-audit clean on Python 3.11); DEP-01 accepted for a 3.10 dev venv |
| FE-01 | Medium | Any failed session check signed the member out locally | Fixed: only a 401 |
| FE-02 | Medium | A stale stored copy was shown as "Mutakhir" (up to date) | Fixed |
| B-P0-3 | High (data) | Download clicks generated and counted extra reports | Fixed |
| W2-R1 | P1 | A report slower than the gateway limit looked like an outage, and its retry could create a duplicate | Fixed: unknown-outcome state and idempotency key |
| SEC-C-01 | P2 | One client could exhaust the free D1 daily read quota through the anonymous snapshot (about 280 rows per call) | Fixed: memo, 0 statements on a repeat |
| SEC-C-03 | P3 | A retry during a still-running report could duplicate it | Fixed: per-key reservation, `409 IDEMPOTENCY_IN_PROGRESS` |
| PLAT-B-01 | P2 | The request deadline could refuse the final write of a report (no row, orphan files) | Fixed: a write's first attempt always starts |
| SEC-C-04 | P3 | A conflicting month would show the headline value next to a chart gap | Fixed: withheld |
| SEC-C-07 | P3 | The decree repair took no backup itself and audited last | Fixed |
| SEC-C-02 | P2 | Public product screenshots showed QA artefacts | Fixed: data renamed, screenshots re-captured |
| UX-24 | – | Unused CC BY-SA photographs were served without attribution | Fixed: moved out of the served folder |
| GW-03 … GW-06, GW-08, GW-09 | Low | Lax numeric variables, retry deadline, http backend URL, reserved KV prefix, non-JSON 4xx pages, KV reads per failing request | Fixed |
| FE-03, FE-04 | Low / Info | Downloads had no timeout; abort listener kept; shared GETs keyed by token | Fixed (60 s timeout, listener removed, cache dropped on credential change) |

Verified properties (wave C security review, plus the after-run QA guest guard, pip-audit from the wave B
platform review and the secret-scan self-test from wave A; commands and outputs in their scratch folders).
bandit 1.9.4 was re-run on the final tree on 2026-10-27 with the CI flags (`-ll --ignore-nosec`): 0 medium
or high issues (7 low-severity notes, below the CI threshold). pip-audit was not re-run, and no
dependency changed after wave C:

- Policy v4 holds: an anonymous probe of all 89 API paths found only health, `whoami`, `/plans` and the
  public snapshot open; every other route answers 401.
- Guests never see a forecast value: 6 public routes × 2 languages × 2 widths contained none of 298 value
  strings from 46 forecast runs; the product screenshots passed both guards of the capture script (no
  result headings, none of 274 value strings).
- Role, plan and operator enforcement on the server is unchanged; no access-control assertion was
  weakened.
- The KV copy cannot be poisoned by a guest (fixed keys, two exact paths, no query string, no
  `Authorization`, hash checked before serving).
- Idempotency is scoped by caller, organisation and a body fingerprint; a replay across users or
  organisations is impossible.
- Supply chain: CI actions pinned to commit SHAs, `contents: read`; `npm audit --omit=dev` 0
  vulnerabilities; bandit (`-ll`, `--ignore-nosec`) 0 issues; secret scan clean (and its self-test).

Residual risks (recorded in [audits/RESIDUAL_RISKS.md](audits/RESIDUAL_RISKS.md)): CSP still allows inline
styles; no HSTS on the `workers.dev` static assets (SEC-15); the snapshot memo, the report reservation and
the rate limits are per process (`API_INSTANCES=1`); the KV write throttle is best effort across Worker
isolates; Terms and Privacy are drafts with placeholders (LEGAL-01).

## 8. Performance measurements

All measurements are local: production builds served by `vite preview` against the local backend,
headless Chrome, 3 runs per case (median), HTTP cache disabled. "Slow mobile" is 1.6 Mbps down, 750 kbps
up, 150 ms RTT and 4× CPU throttling. Raw data:
[measurements-before.txt](evidence/2026-10-25-transformation/before/measurements-before.txt),
[measurements-after.txt](evidence/2026-10-25-transformation/after/measurements-after.txt).

| Page, profile | Measure | Before | After |
|---|---|---|---|
| `/` desktop | Content ready (h1, no skeleton) | 240 ms | 214 ms |
| `/` desktop | Requests | 17 | 12 |
| `/` slow mobile | Largest contentful paint | 2,356 ms | 2,176 ms |
| `/ringkasan` slow mobile | Content ready | 5,430 ms | 3,887 ms (−28 %) |
| `/ringkasan` slow mobile | Largest contentful paint | 4,368 ms | 3,148 ms (−28 %) |
| `/ringkasan` desktop | Largest contentful paint | 504 ms | 684 ms (+36 %) |
| `/ringkasan` desktop | Content ready | 632 ms | 697 ms (+10 %) |
| `/` slow mobile | Content ready | 2,611 ms | 2,657 ms (+2 %) |
| `/` desktop | Largest contentful paint | 204 ms | 236 ms (+16 %; only 1 of 3 runs recorded an LCP) |
| `/ringkasan` | Requests | 31 | 24 |
| `/ringkasan` | JavaScript transferred | 285.3 kB | 306.2 kB |
| both | Web fonts | 0 (never loaded) | 65.3 kB (IBM Plex Sans + Serif 600) |

**Landing JavaScript** (entry module plus modulepreloads, `scripts/check-landing-bundle.mjs`, gzip level 9):

| Build | Raw | Gzip | Files |
|---|---|---|---|
| Baseline | 516,250 B | 162,207 B | 10 |
| After wave B (redesign, before splitting) | 588,170 B | 180,854 B | 3 |
| After wave C (splitting, lazy strings) | 487,632 B | 152,876 B | 2 |
| Final (integration build, 2026-10-27 18:30) | 491,426 B | 154,156 B | 2 |

The final build adds 1.3 kB gzip for the integration changes (text-size-aware chart geometry, the
withheld-month wording, the session and page-loading notes). The landing CSS is 14,098 B gzip (baseline
13,390 B), with the workspace area sheets (shell 2.8, market 2.3, flows 3.8 and operator 1.7 kB gzip as
reported by Vite) loading only with their pages. The charting library (385 kB raw) never loads on a
public page. The page timings above were measured on the wave C build; they were not re-run on the
integration build, whose JavaScript differs by those 1.3 kB.

**Backend** (local SQLite, wave A profiling; times in ms, median of warm runs; SQL = metadata statements per
request, each a D1 round trip when deployed): market report 321 → 307 ms, 60 → 9 statements;
`/public/market-snapshot` 12 → 3 statements, and 0 on a repeat after the memo (first call 0.88 s on a cold
copy, repeats 5-8 ms); `/data/summary` 12 → 2; `/insights` 20 → 5. Readiness after a cold start: the first
probe answers in about 5 s (`degraded`, loading) instead of 6-9 s, then `ready` in 0.15-1.3 s.

**Honest trade-offs.** The desktop `/ringkasan` largest paint is 180 ms slower than at baseline (still well
under the 2.5 s "good" threshold): the page now loads the shell and page chunks after the entry module, its
JavaScript grew by 21 kB, and the self-hosted fonts add 65 kB that the baseline never loaded. The slow
mobile profile, where these costs matter most, improved by 28 %. Three smaller regressions are in the table:
desktop `/ringkasan` content ready +65 ms, `/` slow-mobile content ready +46 ms and `/` desktop LCP +32 ms
(one recorded run). No D1 or staging latency was measured.

## 9. Tests

| Suite | Baseline | Final | Notes |
|---|---|---|---|
| Web (Vitest) | 144 | 254 | 20 files; run with `--maxWorkers=2`; three consecutive full runs on 2026-10-27, 0 failures |
| Gateway (Vitest) | 33 | 83 | 5 files: KV copy, write budget, entry point, normalisation |
| Python (pytest) | 1,169 | 1,227 | backend and pipeline; full run 2026-10-27 in 5 min 35 s, 0 failures, default order |

New or substantially extended suites:

- Web: `landing.test.tsx` (21: values and units, documents, forecast lock for guests, stale copy, outage,
  timeouts, gaps, keyboard chart, conflict month withheld everywhere), `workspace-core.test.tsx` (25),
  `w2-flows.test.tsx` (29: staleness rules, idempotent reports, unknown outcomes, the 409 in-progress wait
  with and without a server wait time, plan visibility), `boot-fallback.test.tsx` (2: session note only
  while the session is checked), `wave-c-fixes.test.tsx`, `i18n/lazy-strings.test.ts`, `auth.test.tsx`,
  client timeouts, header filtering and the shared-GET reset on a credential change.
- Gateway: `edge-cache.test.ts` (KV copy, integrity, throttles, refresh, failed writes, error
  normalisation), `entrypoint.test.ts`.
- Python: public snapshot (C1, memo, conflict withholding), request read cache, readiness, report failures,
  request deadline, report reservation, decree parser regressions with real excerpts, repair CLI with backup,
  guest access with history caps 0/3/24/36, the accuracy insight on the same backtest cases, singular and
  plural server texts.

One existing web test was changed for timing, not behaviour. `flows.test.tsx` failed once in a loaded
full run: its probe records the new route as soon as it renders, but sign-out deliberately clears the
stored token only after the private shell has unmounted, which can be a moment later. A temporary probe
test reproduced the gap deterministically: with a slowed render, the shell had gone but the token was still
stored. The test now checks storage after the "Anda telah keluar" feedback, which the app shows only after
the token is cleared, so the assertion is unchanged and also pins that order.

Tests whose assertions changed did so only where copy or behaviour changed on purpose (for example
"Proyeksi ICP" as the forecast heading, lower-case direction words, the removed photo credit, the corrected
viewer note that had claimed viewers can compare scenarios); each change is listed in the wave reports. No
security, privacy or honesty assertion was weakened. Most new backend and gateway tests were checked to fail
without their fix (test-first runs and mutation checks recorded in the wave reports); the rest were reviewed
for that property by the independent reviewers.

Static checks on the final tree (2026-10-27): ruff and `ruff format --check` clean (139 files), mypy clean
(90 files), ESLint `--max-warnings 0` clean for web and gateway, TypeScript clean (contracts, web,
gateway), contracts up to date, secret scan clean. The web build, the gateway's local dry-run build
(`wrangler deploy --dry-run`, nothing uploaded) and the landing guard (154,156 B gzip against the 160,000 B
budget, no charting code) pass.

## 10. Browser and responsive verification

Headless Chrome (system Chrome through playwright-core) with axe-core 4.10, on local production builds. The
matrix, axe, keyboard, touch, states and journeys below ran on the wave C build (2026-10-27 01:12) and were
not re-run on the integration build; the integration build was checked with the 200 % text sweep, 12 axe
scans at 100 %, Tab walks at 200 % on 3 pages and images 46–53. One browser at a time on the owner's
laptop.

- **Matrix:** 20 routes (6 public, 8 member, 6 operator) at 320, 390, 768, 1024, 1440 and 1920 px, 120
  captures: 0 horizontal overflow, 0 console or HTTP errors, 0 skeletons left after load, exactly one h1 per
  page, 0 uppercase text. The UX review ran the same routes plus a 404 page in Indonesian at all six widths
  and in English at 390 and 1440 (168 renders) with the same results. In English the only Indonesian text
  found on any route is a server-stored organisation name ("Organisasi dihapus") on the operator tenants
  page.
- **axe (WCAG 2.0/2.1/2.2 A and AA):** 0 violations on 28 guest and member runs (baseline: 56 real nodes
  plus a verified false positive) and on 12 operator runs.
- **Keyboard:** 30 Tab walks (15 pages × 1440/390): 0 focus stops without an indicator, 0 hidden or partly
  hidden under a sticky header (WCAG 2.2 SC 2.4.11). Skip links, drawer, dialogs and sheets trap and return
  focus; tabs use arrow keys; the landing chart reads month by month.
- **Text-only zoom** (B-P3-5; integration build, Chrome text size 200 %): 21 routes (6 public, 9 member, 6
  operator) at 1440, 768 and 390 px, 63 runs, with a probe that measures every visible text node,
  including SVG chart text, against the viewport and its clipping ancestors. Headings and body text double
  (`/ringkasan` h1 24 → 48 px, body 15 → 30 px), and the top bar grows with `--sticky-top` equal to its
  measured height in every run that has a header. There is no horizontal overflow, no control label
  spilling out, no text below 20 px, and no text cut except single-line ellipses by design (the phone top bar
  shortens the product name to "IC…"; two long source URLs and one actor id on operator pages) and the
  deliberate three-line clamp of the overview's insight teasers. At the default size, 28 route/width pairs
  compared against the pre-fix build
  show no size change to any control. The only intended height changes are two rows that now wrap at
  390 px. Images 46–49.
- **Integration states** (images 50–53; `50` and `51` are mocked and labelled on the image, `52` holds the
  whoami request and `53` delays a page chunk): a report retried after a 504 and answered `409
  IDEMPOTENCY_IN_PROGRESS` shows "still being generated" with the first send time and a countdown, and
  sends the same key and body. A conflict month says "withheld" in the headline, legend, table, summary
  and rupiah line, and the withheld value appears nowhere. The session note appears only while the
  session is checked; a slow page chunk shows "Memuat halaman…".
- **Touch** (wave C UX review at 390 px, emulated coarse pointer): no target below 40 px except the skip link
  (39 px) and two truncated document links on `/operasi/data` (19 px, adequately spaced).
- **States** (simulated in the browser with request mocks or held requests, except the real empty
  notifications list and the data-status popover): outage (530), hang, recovery, stale public copy, partial
  failure, slow notice, empty lists, report outcome unknown and still in progress, conflicting sources,
  data-status popover.
- **Journeys:** forecast (1.7 s), scenario run, edit, re-run, compare and archive dialog, report with both
  downloads (exactly one `POST /reports`), insight sheet with Back and deep link, language switch,
  sign-out with Back.

Not tested: Safari, Firefox, real phones and tablets (touch was emulated), screen readers (only axe, the
accessibility tree and keyboard), the staging deployment.

## 11. Screenshots and evidence

- [evidence/2026-10-25-transformation/INDEX.md](evidence/2026-10-25-transformation/INDEX.md): status of every
  baseline finding with its evidence, the before → after image table and the headline numbers.
- [before/](evidence/2026-10-25-transformation/before/INDEX.md): 30 images of the original UI and the
  staging site, `measurements-before.txt`.
- [after/](evidence/2026-10-25-transformation/after/INDEX.md): the same 30 views on the wave C build plus
  the new states (stale copy, slow notice, stale comparisons, unknown report outcome, data-status popover,
  partial failure, landing during the session check, recovery after an outage, interval in the URL, operator
  console), and images 46–53 from the final integration build (200 % text, the 409 notice, the withheld
  conflict month, the session and page-loading notes); `measurements-after.txt` has a section for each build. Images 01–45 predate the
  integration changes, so they show px-sized type and, where the landing's accuracy section is in view (for
  example `17`), the earlier 6,09 figure; no image before `51` shows a conflict month.
- Product screenshots used by the landing's tour: `apps/web/public/img/product/*.webp` (8 images, Indonesian
  and English, from the local test organisation; listed in [ASSET_INVENTORY.md](ASSET_INVENTORY.md)).

The browser harness and QA scripts that produced the evidence (playwright-core with the system Chrome, the
matrix, states, accessibility, performance and journey scripts) were run from a scratch folder outside the
repository; the INDEX names each script and what it measures.

## 12. Known limitations

- **Staging is unchanged.** The public URL still shows the baseline defects until the owner deploys; the
  gateway's stale-copy cache has only run in unit tests and under local `wrangler dev`, not against real KV.
- **The staging D1 database** still holds the wrong decree references (section 6).
- **Desktop `/ringkasan`** largest paint is 180 ms slower than at baseline (section 8).
- **Legal pages:** Terms and Privacy still show the owner placeholders (company details, contact, governing
  terms); they need owner and counsel input.
- **Per-process state:** the snapshot memo, the report reservation, rate limits and the sign-in throttle
  assume one API process (`API_INSTANCES=1`); writes made by the operator CLI reach the snapshot within 60 s.
- **Scenario edits by another member** are detected when the scenario list reloads, not live.
- **Workspace deep link during a hang:** brand and navigation appear only after the session check, up to the
  8 s whoami timeout; until then the placeholder says "Memeriksa sesi Anda di server…" from 1.5 s (evidence
  INDEX open item 4).
- **Anonymised organisations** keep the stored name "Organisasi dihapus", which the English operator console
  shows as is (UX-26; the tenant API does not expose the deletion time the interface would need to label it).
- **Large text, partly measured:** 200 % was swept on 21 routes; 150 %, dialogs, sheets and the scenario
  editor's open states at 200 %, and a text-size change while a page is open (handled by a resize probe)
  were not measured. On a phone at 200 % the landing chart labels only the newest month on its x-axis, and
  the app top bar shortens the product name to "IC…"; on `/harga` at 1440 the focusable table region is
  taller than the viewport, so part of it sits under the top bar.
- **Evidence images 01–45** were not re-captured after the integration changes (section 11).
- **Two persistence figures exist by design:** the operator model page shows the registry's 6.09 (66
  cases common to all methods); the landing, the methodology page and the forecast page show 6.06 (v5's
  own 69 cases). The forecast page's accuracy tab falls back to the registry's 6.09 only when no forecast
  run with point evaluations is loaded, and the accuracy insight does so only without stored predictions.
- **D1 statement savings** are counted locally, not measured on D1; staging latency was not measured.
- **Browsers and devices:** only Chrome on Windows was used; no screen-reader pass.
- **Python 3.10 development venv** keeps Pillow 12.3.0 (DEP-01).
- **The browser QA harness** is not part of the repository (section 11).

## 13. Outstanding decisions

| # | Decision | Why it is the owner's | Recommendation |
|---|---|---|---|
| 1 | Deploy the transformed release to staging | Deployment and its timing; also applies pending D1 migrations 0005–0008 (staging is at schema 4; see [DEPLOYMENT.md](DEPLOYMENT.md)) and the D1 decree-reference repair | Deploy, then run the smoke test and `curl -s -D - -o /dev/null <staging>/api/v1/plans` to see `x-icp-cache` |
| 2 | Fill the Terms and Privacy placeholders | Legal identity and counsel review | Provide the company details; have counsel review before any paid use |
| 3 | Accept the desktop `/ringkasan` trade-off or invest in it | Product priority | Accept for now (still under 0.7 s); revisit with a modulepreload of the shell for signed-in visitors |
| 4 | Router type | The declarative router would cut about 54 kB but removes the unsaved-changes guard (`useBlocker`) | Keep the data router |
| 5 | Move the development venv to Python 3.11 | Local tooling choice | Yes, to close DEP-01 (3.10 reaches end of life in October 2026) |
| 6 | Optional backend refinements | Contract changes | Stop bumping a scenario's `updated_at` on runs, or add `inputs_updated_at`; a language-neutral session device kind; expose `deleted_at` on the operator tenant list so anonymised organisations get a translated label (UX-26); the PDF title in sentence case |
| 7 | HSTS and a custom domain | Hosting | HSTS becomes meaningful with a custom domain |
| 8 | Public accuracy copy: "repeat last month" 6,09 → 6,06 | A figure readers see changes (both are correct; 6,06 is the like-for-like comparison) | Keep 6,06 (the landing tour's screenshots do not show this figure; evidence images that show the landing's accuracy section, such as `17`, which the README uses, still show 6,09) |

## 14. Reproducing locally

```bash
# setup (once)
npm install
npm run setup

# run the stack: API on :8000, web app on http://localhost:5173
npm run dev
# (Windows, background shells: run the API without --reload)
.venv/Scripts/python -m uvicorn icp_api.main:app --host 127.0.0.1 --port 8000

# all quality gates: lint, types, contracts, tests, builds, landing guard, secret scan
npm run check

# individual gates
npm run lint
npm run typecheck
npm run contracts:check
npm run test -w @icp/web -- --maxWorkers=2
npm run test -w @icp/gateway
node scripts/py.mjs -m pytest -q
npm run build && npm run landing-guard -- --max-gzip-bytes 160000
node scripts/check-landing-bundle.mjs --self-test
node scripts/secret-scan.mjs && node scripts/secret-scan.mjs --self-test

# the Worker with the stale-copy cache in front of the local API (port 8787)
npm run dev:edge

# data operations used in this transformation (local store; back up first)
npm run backup
node scripts/py.mjs -m icp_api.cli repair-decree-references            # dry run
node scripts/py.mjs -m icp_api.cli repair-decree-references --apply    # takes and verifies a backup itself
node scripts/py.mjs -m icp_api.cli verify-provenance
```

Browser checks need a Chromium-based browser and playwright-core with axe-core (not repository
dependencies); the evidence INDEX lists what each check measured.
