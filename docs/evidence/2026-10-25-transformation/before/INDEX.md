# Baseline ("before") evidence, 2026-10-25

Captured on 2026-10-25 by the QA baseline pass, before the product transformation. Source: the frozen
production build of the original UI at `http://127.0.0.1:4173` against the local backend (`:8000`), headless
Chrome through playwright-core, reduced motion, device scale 1, Indonesian locale unless stated. Exception:
`01` and `02` show the deployed staging site (`icp-gateway-staging.icp-gateway.workers.dev`, release of
2026-10-17, backend tunnel offline: the API answers HTTP 530); `02` reuses an earlier capture from the same
day (Playwright MCP session, 375 px wide).

Signed-in views use the local test organisation "PT Uji UX V3 (lokal)" (member "Penguji UX", plan Trial).
Scenarios, forecasts and reports visible in these images were created in that throwaway organisation by the
QA journeys. No image shows a token, password, e-mail address or secret. Outage and hang states are
simulated in the browser (every `/api/v1` call answered with HTML 530, or never answered).

Images are JPEG, at most 400 kB; long pages are cropped or scaled (the size is in the file list of the QA
report). In full-page captures of the workspace the dark sidebar stops at the first viewport height: it is a
sticky element, so this is how a full-page screenshot renders it, not a layout defect.

The prioritised backlog that these images support (IDs `B-P0-1` and so on), with measured numbers, causes and
verification steps, is the QA report `BASELINE_FINDINGS.md`, delivered to the main agent with this folder.
The scripts that produced the captures can be re-run against the new build to make the "after" set.

| File | What it proves |
|---|---|
| `01-staging-landing-skeletons-45s-1440.jpg` | Deployed staging landing 45 s after load: the four market KPIs are still grey skeletons, with no error and no retry, while both public API calls answered 530 (B-P0-2). |
| `02-staging-landing-skeletons-390.jpg` | Same staging state on a phone (375 px): skeleton KPIs. The header also wraps, with "Daftar" pushed onto a second line (B-P0-2). |
| `03-hang-member-landing-fullpage-skeleton-12s-1440.jpg` | A visitor with a stored member session opens `/` while the API hangs. After 12 s the whole page is still a grey skeleton, with no brand, no navigation and no message. It stays that way until the 35 s session-check timeout (B-P0-1). |
| `04-hang-guest-landing-kpi-skeletons-12s-1440.jpg` | A guest opens `/` while the API hangs. After 12 s the snapshot KPIs are still skeletons and nothing tells the visitor that the data is late (B-P1-4). |
| `05-hang-guest-landing-timeout-40s-1440.jpg` | The same guest at 40 s: after the 35 s client timeout the error "Server lambat menjawab" finally appears, twice (snapshot and plans) (B-P1-4, B-P2-1). |
| `06-hang-member-ringkasan-sent-to-signin-40s-1440.jpg` | A signed-in member opens `/ringkasan` while the API hangs. After 35 s of skeleton the member is sent to the sign-in page, as if signed out, with no word about the outage (B-P1-2). |
| `07-outage530-guest-landing-1440.jpg` | Landing during an HTTP 530 outage (the baseline build has an error branch). There are two identical error callouts, the KPI tiles show "-", and the message says the numbers shown are not updated although no number is shown (B-P2-1). |
| `08-outage530-member-ringkasan-sent-to-signin-390.jpg` | A member opens `/ringkasan` during a 530 outage and is redirected to the sign-in form after one retry, with no outage message (B-P1-2). |
| `09-scenario-result-stale-after-edit-1440.jpg` | A scenario was run with Brent 95, then edited to Brent 70 and saved. The card shows "Brent … 70,00", but the result panel still shows the run computed with 95 (Oct 2026 USD 93,74) under the same scenario name, with no stale marker (B-P1-1). |
| `10-report-download-creates-duplicates-1440.jpg` | One "Buat laporan" click followed by clicks on the PDF and CSV download buttons produced three identical "Laporan Pasar Bulanan" rows at 14.37: each download click submitted the form again (B-P0-3). |
| `11-archive-dialog-cancel-focus-invisible-1440.jpg` | Archive confirmation opened with the keyboard. Focus is on "Batal", but the button shows no focus indicator. The dialog itself traps focus, closes with Escape or Batal, and archives nothing (B-P1-3). |
| `12-ghost-button-focus-invisible-copylink-1440.jpg` | Insight sheet with keyboard focus on "Salin tautan" (a ghost button): no outline and no ring are visible (B-P1-3). |
| `13-english-ringkasan-indonesian-dataset-labels-1440.jpg` | English UI on `/ringkasan`: the "Latest data update" panel lists the datasets with Indonesian names ("Harga Brent (rata-rata bulanan)" …) (B-P2-2). |
| `14-ringkasan-390-onboarding-pushes-kpis-below-fold.jpg` | `/ringkasan` at 390 px: the "Langkah pertama" card fills the first screen, and the official ICP value starts at about y = 1000 px, below an 844 px viewport (B-P2-3). |
| `15-landing-320-small-hero-photo.jpg` | Landing at 320 px: the hero photo is a small inset (about 210 × 140 px) that is mostly covered by its caption (B-P3-3). Note: the editorial photo further down had not finished lazy-loading in the full-page matrix capture. |
| `16-proyeksi-1024-tables-cut-at-right.jpg` | `/proyeksi` at 1024 px: the forecast table and the input-assumption table are cut at the right edge (horizontal scroll inside the frame, no affordance) (B-P3-4). |
| `17-landing-1440-full.jpg` | Reference: the whole public landing at 1440 px, normal conditions (scaled to 50 %). |
| `18-ringkasan-1440-full.jpg` | Reference: the whole `/ringkasan` at 1440 px, normal conditions (scaled to 50 %). |
| `19-harga-390-record-cards.jpg` | Reference: `/harga` at 390 px. The monthly table becomes record cards, the page is 7,639 px long, and there is no horizontal overflow. |
| `20-masuk-1440.jpg` | Reference: the sign-in page at 1440 px. |
| `21-empty-scenarios-1440.jpg` | Empty state: an organisation with no saved scenario shows an explained empty list and an empty results panel. |
| `22-empty-notifications-1440.jpg` | Empty state: no alert rules and no events. |
| `23-forecast-3-month-run-1440.jpg` | Journey: a 3-month forecast ran in about 1.6 s. The URL gets `?proyeksi=<id>`, focus moves to the "Hasil proyeksi" heading, and a status message is announced. |
| `24-scenario-compare-1440.jpg` | Journey: two scenarios compared against the baseline forecast, with the chart, distinct markers and the table. |
| `25-signout-back-shows-signin-1440.jpg` | Journey: after signing out, browser Back lands on the sign-in page (`/masuk?next=%2Fringkasan`), not on private data. The old token is rejected (HTTP 401). |
| `26-skip-link-first-tab-1440.jpg` | Accessibility (works): the first Tab on the landing shows the "Lompat ke konten" skip link, which moves focus to `<main>`. |
| `27-drawer-open-focus-trapped-390.jpg` | Accessibility (works): the app drawer at 390 px is a modal dialog. Focus moves in, 30 Tabs never leave it, the page behind is inert, and Escape closes it and returns focus to the menu button. |
| `28-signin-inline-errors-1440.jpg` | Accessibility (works): submitting the empty sign-in form marks both fields invalid, moves focus to the e-mail field and ties each error with `aria-describedby`. |
| `29-ringkasan-browser-zoom-200-reflow.jpg` | Accessibility (works): `/ringkasan` at 200 % browser zoom (720 CSS px) reflows with no horizontal overflow and no clipped text. Text-only zoom has no effect because all sizes are in px (B-P3-5). |
| `30-report-generated-pdf-csv-1440.jpg` | Journey: a Market monthly report generated in about 1.9 s with PDF and CSV (XLSX is locked on the Trial plan). The files downloaded were a 75,591-byte PDF and an 856-byte CSV. |
| `measurements-before.txt` | Raw script output: performance medians (desktop and Fast 3G + 4× CPU), axe results for every route at 1440 and 390, and document height, overflow and console errors for all 84 matrix captures. |
