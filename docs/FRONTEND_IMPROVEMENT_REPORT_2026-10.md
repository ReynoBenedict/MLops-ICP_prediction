# Frontend improvement report (end-to-end pass, 2026-10-17)

Scope: the existing `apps/web` application (React 19 / Vite / TypeScript), improved in place.
No rewrite, no Git operations, no cloud deployment, no change to the ML model, datasets,
ingestion pipeline or reconciliation rules. The single backend change is additive (an Indonesian
`definition_id` on dataset provenance) and is covered by the contract check and backend tests.

Everything below was executed locally against the running backend (`http://127.0.0.1:8000`,
restarted on the current code) and the Vite dev server (`http://localhost:5173`).

## 1. Audit findings and status

| # | Priority | Finding (before) | Status |
|---|---|---|---|
| 1 | P0 | Forecast history and result header showed run ids (`fc_…`) and raw method ids (`spread_brent_6m`) | Fixed: `methodLabel()` everywhere, ids removed from the UI (kept in CSV export) |
| 2 | P0 | Report success callout and report table showed report ids; "ID proyeksi" text field expected an `fc_…` id | Fixed: success callout names the report kind, date and offers the downloads; forecast picked from a list of real issues |
| 3 | P0 | Scenario cards showed "Terakhir dijalankan: fc_…"; run-result title carried the run id; comparison showed the raw method id | Fixed: "pernah dijalankan / belum pernah dijalankan", result titled by scenario name, method label |
| 4 | P0 | Alert inbox rendered raw English severities (`warning`, `info`) | Fixed: `alert.sev.*` labels (Informasi / Perlu dicermati / Peringatan / Kritis) |
| 5 | P0 | Sidebar showed "Sistem · local · API 2.0.0" and "Masuk sebagai operator" to every visitor; "Syarat Penggunaan" sat in the product navigation | Fixed: system card only for operators; operator link removed (operators use the Operator tab on `/masuk`); legal links moved to a product footer |
| 6 | P0 | `ErrorState` printed `error.message` verbatim (potentially raw exception text) and "ID permintaan" | Fixed: authored 4xx messages only; 5xx / network / client faults get a generic explanation and a "Kode referensi" |
| 7 | P0 | Em dashes and spaced en dashes in copy and as value placeholders; arrows typed as "→ / ←" | Fixed: copy rewritten (comma, semicolon, parentheses), ranges read "Apr 2026 s.d. Jun 2026", missing values render "-", arrows are icons; guarded by `i18n.test.ts` |
| 8 | P0 | Dataset definitions in Eksplorasi Harga were English only | Fixed: Indonesian `definition_id` from the backend (`DatasetSpec.definition_id`), English fallback |
| 9 | P0 | Freshness / kind legends used truncated help text ("…") | Fixed: short labels plus full help as tooltip |
| 10 | P1 | After "Buat proyeksi", "Jalankan", "Bandingkan" or "Buat laporan" nothing moved the user to the result | Fixed: scroll to the result panel and focus its heading; report downloads offered in the success callout; "Buat laporan perbandingan" link from the comparison to `/laporan?jenis=perbandingan&skenario=…` |
| 11 | P1 | Sign-in gates lost the destination; tab state on Laporan not linkable | Fixed: `/masuk?next=…` (validated same-origin paths), `/laporan?tab=pemberitahuan` |
| 12 | P1 | No visible focus on the dark sidebar and custom controls; small touch targets | Fixed: focus rings for nav / tabs / segmented / horizon buttons / checkboxes; 40 px targets on coarse pointers; Escape closes the drawer |
| 13 | P1 | Simulasi and Laporan left the right half of a 1440 px screen empty | Fixed: list + results and form + list two-column layouts (`layout-list-detail`, `layout-form-list`), collapsing at 1100 px |
| 14 | P1 | Legal pages were a narrow 820 px column with a small "Diperbarui" line and no footer | Fixed: 72 ch reading width, intro, visible "Terakhir diperbarui" box, section rules, contact note, footer with links, Terms/Privacy switch in the header |
| 15 | P2 | Mixed card radii, ad-hoc transitions | Fixed: `--r-card`, `--motion-fast/base` tokens, 150/200 ms transitions, page fade-in; all disabled under `prefers-reduced-motion` |
| 16 | P2 | No visual for how an official ICP comes about; generic database icon for every source | Fixed: `IllIcpProcess` explainer on Proyeksi, `MarkDecree` / `MarkBarrel` / `MarkFx` source marks on Ringkasan (original SVG, inventoried) |
| 17 | P2 | Chart tooltip range used an en dash; spread series label built by string concatenation | Fixed: `rangeText()`, dictionary label |
| 18 | P2 | Insight cards appended dataset version ids to the source list | Fixed: labels only |
| 19 | P0 (found in captures) | Report list showed the creator as `user:usr_…` | Fixed: role words instead of ids |

## 2. Changes made

Frontend (`apps/web/src`):

- `components/layout.tsx`: operator-only system card, no operator link, public-only "Beranda", `ShellFooter`, Escape to close the drawer, `aria-controls` on the menu button, sign-in button carries `next`.
- `components/ui.tsx`: `ErrorState` rewritten (see finding 6), `GateNotice` carries `next`.
- `components/forecast.tsx`: `methodLabel` never returns an id; `datasetList()`; `exportForecastCsv` file name by issue date + origin; single CSV button (server export when available); `ProcessExplainer`; kind legend; no dashes.
- `components/illustrations.tsx`: `IllIcpProcess`, `MarkDecree`, `MarkBarrel`, `MarkFx`.
- `components/charts.tsx`: range text in the band tooltip, `"-"` placeholder.
- `pages/ForecastPage.tsx`: result subtitle without run id, history table without ids (horizon, "dengan skenario", method label), scroll + focus to `#hasil`, sign-in links with `next`, process explainer in the limitations panel.
- `pages/ReportsPage.tsx`: two-column layout, forecast picker, scenario preselection from the query string, success callout with downloads, no ids, severity labels, dataset labels, tab in the query string.
- `pages/ScenariosPage.tsx`: two-column list + results, results empty state, result titled by scenario name, method label, report link, scroll + focus.
- `pages/LegalPages.tsx`: new frame (header switch, intro, updated box, contact, footer).
- `pages/OverviewPage.tsx`, `pages/ExplorerPage.tsx`, `pages/InsightsPage.tsx`, `pages/LandingPage.tsx`, `pages/AuthPages.tsx` (`safeNext`, `next` handling, icon arrows), `pages/AccountPage.tsx`, `pages/ops/*` (placeholder dashes).
- `format.ts` (`NA`, `rangeText`), `i18n/index.tsx` (placeholder), `i18n/dictionary.ts` (about 60 keys added or rewritten).
- `styles.css`, `styles-brand.css`: tokens, motion, focus, touch targets, layouts, legal, footer, explainer.
- Tests: `pages.test.tsx` (+3: legacy redirect and hidden internals, alerts deep link, no-leak check on the forecast view), `i18n.test.ts` (+1: no em/en dashes in customer-facing copy, 300+ keys), `format.test.ts` updated.

Backend (additive only): `pipeline/icp_ml/schema.py` (`DatasetSpec.definition_id` with Indonesian text for all six datasets), `backend/icp_api/services/datasets.py` (provenance exposes it), `backend/icp_api/schemas.py` (`DatasetProvenance.definition_id`), `packages/contracts` regenerated.

Docs: `apps/web/README.md` (deep links, what users never see, motion), `docs/ASSET_INVENTORY.md` (new SVG rows), this report.

## 3. Routes checked

`/`, `/masuk` (incl. `?next=`), `/daftar`, `/syarat`, `/privasi`, `/ringkasan`, `/harga`, `/proyeksi`
(incl. a run with a scenario), `/skenario`, `/insight`, `/laporan` (incl. report generation and
`?tab=pemberitahuan`), `/akun`, `/operasi`, `/operasi/data`, `/operasi/model`; legacy `/forecast`
redirect (unit test). Widths: 1440, 1280, 768, 390 (see section 7).

## 4. Feature status

| Feature | Status | Evidence |
|---|---|---|
| Landing page with live public snapshot and plans | end-to-end | `/public/market-snapshot`, `/plans`; unit test; screenshots |
| Registration (trial organisation) and login (user / operator) | end-to-end | backend tests `test_product.py`; login validation unit test; `next` redirect unit-tested via gate links |
| Ringkasan Pasar (official ICP, benchmarks, FX, rupiah equivalent, freshness, forecast, insights, quality notes) | end-to-end | live data; screenshots |
| Eksplorasi Harga (ranges, benchmarks, spreads, rupiah, table, CSV, provenance with Indonesian definitions, verification grid) | end-to-end | live data incl. new `definition_id`; screenshots |
| Proyeksi ICP (run, horizon by plan, scenario for members, intervals, assumptions, method panel, history, CSV) | end-to-end | `POST /forecasts` executed in the screenshot run with a Brent scenario; backend tests |
| Simulasi Skenario (create, edit, archive, run, compare up to 4, report link) | end-to-end for organisation members | backend tests `test_product.py` (create/run/compare); UI flows unit-rendered. Not re-screenshotted with a member session (see section 8). For operators the page shows a validation notice because scenarios are organisation-scoped (`Operators must specify org_id.`) |
| Insight Pasar | end-to-end | live rule-based insights; screenshots |
| Laporan (PDF / Excel / CSV; monthly, forecast, scenario comparison, provenance) | end-to-end | report generated in the screenshot run (operator, platform report); backend tests; downloads via authenticated fetch |
| Pemberitahuan (rules, evaluate, inbox, mark read) | end-to-end in-app; e-mail / webhook unavailable (stated in the UI) | backend tests; member session required for the inbox (operators see the notice) |
| Akun & Organisasi (profile, password, members, API keys, plan & usage) | end-to-end | backend tests; plan changes are operator-only; **payment unavailable** and stated as such ("belum ada pembayaran mandiri") |
| Operasi Platform (health, data & versions, models, jobs, audit, tenants) | end-to-end | screenshots (operator) |
| Deep links (`?next`, `?tab`, `?jenis&skenario`, `#hasil`) | end-to-end | unit tests; screenshot `1440-login` shows the `next` note |

Nothing in the UI fakes a response: every button calls the real API or a client-side export of
real data; features that are not available (payment, e-mail delivery) are described as such.

## 5. Assets and licences

New original SVG artwork (project licence, no third-party source): `IllIcpProcess`, `MarkDecree`,
`MarkBarrel`, `MarkFx` in `apps/web/src/components/illustrations.tsx`, documented in
`docs/ASSET_INVENTORY.md`. Existing licensed photographs (Wikimedia Commons, CC BY 4.0 / CC BY-SA
4.0 / CC BY-SA 2.0) are unchanged and still credited on the landing page; the credit line now also
names the illustrations as original work. No new third-party assets were added.

## 6. Test and gate results (executed 2026-10-17)

| Gate | Result |
|---|---|
| `npm run lint -w @icp/web` (eslint, max-warnings 0) | pass |
| `npm run typecheck -w @icp/web` (tsc) | pass |
| `npm run test -w @icp/web` (vitest) | 19 passed (4 files; was 15) |
| `npm run build -w @icp/web` (vite) | pass |
| `npm run lint:py` (ruff check + format) | pass |
| `npm run contracts:check` | "API contract is up to date" |
| `npm run secret-scan` | no findings |
| `npm run test -w @icp/gateway` | 8 passed |
| `pytest backend/tests/test_api.py -k data` | 2 passed |
| full `pytest` | 82 passed, 1 warning (deprecation notice from Starlette's test client), 6 min 13 s |

## 7. Screenshots

Before: the 32 images of the previous pass (member session; removed on 2026-10-17, superseded).
After: `docs/screenshots/after-v2/` (44 images captured; on 2026-10-17 pruned to 27 public, operator and
mobile captures - member pages and width duplicates are superseded by `docs/screenshots/ux-v3/after/`) with `metrics-report.txt` (page
height, horizontal overflow, minimum font size, visible error callouts per capture).

| Width | Captures (public visitor) | Captures (operator token) |
|---|---|---|
| 1440 | landing, login (`?next=/skenario`), register, syarat, privasi, ringkasan, harga, proyeksi, skenario (gate), insight, laporan (gate) | ringkasan, harga, proyeksi, proyeksi with a Brent scenario run, skenario (org-scoped notice), insight, laporan, laporan after "Buat laporan", laporan `?tab=pemberitahuan`, akun, ops kesehatan, ops data, ops model |
| 1280 | landing, syarat | ringkasan, proyeksi, laporan |
| 768 | landing, syarat | ringkasan, harga, proyeksi, laporan |
| 390 | landing, login, insight, nav drawer open, syarat | ringkasan, harga, proyeksi, laporan |

`metrics-report.txt`: every capture reports `hOverflow=no`, `minFont=12.5px` and `errors=[]`
(no visible error callout) after the re-capture of four pages. The scripted actions all resolved
("Buat proyeksi" with a Brent scenario value, "Buat laporan", drawer open).

Pages were inspected visually after capture; observations are in section 8.

## 8. Remaining issues and limitations

- Visual inspection of the captures found three defects that were fixed and re-captured: the
  report list showed the creator as `user:usr_…` (now "anggota organisasi" / "operator platform"),
  the legal-page header overflowed at 390 px (links now wrap under the brand row), and the process
  explainer was squeezed into a narrow column at 1440 px (now a two-column panel).
- Operators see the authored English validation message "Operators must specify org_id." on
  organisation-scoped pages (Simulasi, Pemberitahuan rules). It is operator-only; a translated
  notice would need either a backend message change or an operator tenant selector.

- Member-session screenshots were not retaken. No test account password is documented, the
  directive forbids creating accounts or changing roles for screenshots, the server refuses operator
  password resets of organisation owners, and recovering a session from a browser profile was not
  permitted. The after-v2 set therefore covers the public visitor and the operator. Member-only
  views (Pemberitahuan inbox, Akun & Organisasi, Simulasi list/compare) were covered by the unit tests,
  the backend tests and, since 2026-10-17, by the member-session captures in
  `docs/screenshots/ux-v3/after/` (see `docs/UX_V3_REPORT_2026-10.md`)
  with `shots2.mjs` and a session token.
- Operators cannot use organisation-scoped pages (Simulasi, Pemberitahuan, Akun) without an
  `org_id`; the UI shows the server's validation message. A tenant selector for operators is a
  product decision, not a bug.
- `jsdom` prints "Not implemented: window.scrollTo" during unit tests (harmless; the calls are
  guarded).

## 9. Backend blockers

None for this pass. Prepared-but-unverified items are unchanged and documented in
`docs/COMMERCIAL_READINESS.md`: payment provider, e-mail / webhook delivery, monthly API quota
enforcement (counted only), account self-deletion, MFA.

## 10. Readiness

- **UX readiness (Market Intelligence area):** ready for a member pilot. End users see no run,
  report or dataset identifiers, no method ids, no environment or API versions, no raw exceptions,
  and no operator entry points; copy is Indonesian-first with an English toggle; flows after every
  action move the user to the result; four widths verified without horizontal overflow (see
  `metrics-report.txt`).
- **Deployment readiness:** unchanged from `docs/RELEASE_REPORT_2026-10.md`: the Docker image and
  gateway build locally; nothing has been deployed, no cloud resource was touched, and the first
  live run must verify D1 / R2 / Worker behaviour. This pass makes no deployment claim.
