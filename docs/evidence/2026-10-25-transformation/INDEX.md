# Product transformation evidence: before and after

This folder holds the evidence for the 2026-10 product transformation of ICP Intelligence.

| Folder | Captured | Build under test |
|---|---|---|
| [`before/`](before/INDEX.md) | 2026-10-25 | Frozen production build of the original UI (`:4173`); `01`/`02` show the deployed staging site |
| [`after/`](after/INDEX.md) | 2026-10-27, 01:12–01:53 WIB (`01`–`45`) | Production build of the transformed tree (`vite preview` on `:4176`) |
| [`after/`](after/INDEX.md) | 2026-10-27, 18:30–18:45 WIB (`46`–`53`) | Integration build: the same tree after the main agent's integration changes and review D's fixes (`vite preview` on `:4180`) |

Both runs used the same local backend (`:8000`), the same local test organisation "PT Uji UX V3 (lokal)", and the same
QA scripts. The after run used copies of those scripts adapted to the new markup, with the measured semantics kept. It
also covered states and pages the baseline could not have: the operator console, stale public copy, slow notice,
partial failure, report outcome unknown, and the data-status popover. The numbers are in `before/measurements-before.txt`
and `after/measurements-after.txt`.

The integration build was checked again where the integration changes touch the baseline findings:
- the baseline text-zoom check (`a11y.mjs zoom`), plus a 200 % text sweep of every route;
- the landing bundle guard and the stylesheet sizes;
- the guest forecast guard;
- the new states: report in progress, conflict month, boot note and page-loading note.

Performance, the 120-capture matrix, the states and the journeys were not re-run on it.

Nothing was deployed. The staging site still runs the 2026-10-17 release, so it still shows the baseline defect
B-P0-2. Every "fixed" below is verified on a local build.

## Status of the baseline findings

The UX reviewer gave a status for each finding before the last round of web fixes. The after run below confirms or
corrects it, and the "Change" column says where it differs. B-P3-3, B-P3-5 and the open items were updated on
2026-10-27 at 18:45 WIB from the integration build.

| ID | P | Finding (baseline) | Status | After evidence | Change |
|---|---|---|---|---|---|
| B-P0-1 | P0 | Returning member with a hanging API: whole-page skeleton on the landing for 35 s, about 70 s before any message | **fixed** | Hanging API, stored session, `/`: the 2 s session-wait skeleton, then at 3 s the landing h1 and navigation, with no sign-in buttons while the session is checked. The panel explains the wait with Retry at 10.3 s, and the header reads "Buka ruang kerja" from 9 s (`03`, `40`). | Reviewer's residue (explanation 0.4 s after the 12 s target) is gone: 10.3 s. |
| B-P0-2 | P0 | Staging landing shows skeleton KPIs forever (API 530) | **partly** | **In code:** with every call answering 530, the landing explains the failure at 0.3 s, with exactly one alert, Retry and the ESDM link, and 0 skeletons (`01`, `02`, `07`). A stored public copy is shown with its time and no "Mutakhir" (`31`, `32`). The gateway cache (contract C2) is covered by gateway unit tests. **Still open:** staging is not redeployed, so the public URL still shows the defect until the owner deploys. | Same status. The phone layout of the error (UX-03) is now fixed. |
| B-P0-3 | P0 | Each report download click created another report | **fixed** | One generate plus PDF and CSV downloads: exactly one `POST /reports`, list 17 → 18. Both files are valid; the buttons are `type="button"` (`10`, `30`). | Same. |
| B-P1-1 | P1 | Scenario results and comparisons not marked stale after an edit (even after reload) | **fixed** | In the browser: after an edit, the run is labelled stale and offers "Jalankan ulang", which clears the label. A comparison is labelled stale on screen and after a reload, and restoring the inputs clears it (`09`, `34`, `35`). | Now verified in the browser. The reviewer had unit tests only. |
| B-P1-2 | P1 | Signed-in member sent to sign-in during an outage or hang | **fixed** | 530: the URL stays `/ringkasan`, an explanation with Retry appears at 1.8 s, and there is no password field. Retry after recovery opens "Ringkasan pasar". Hang: explained at 8.2 s, URL kept. Operator console behaves the same (`06`, `08`, `41`). | Same. |
| B-P1-3 | P1 | Ghost buttons had no visible keyboard focus | **fixed** | Tab walks on 30 page/width runs: 0 stops without an indicator. The dialog's "Batal", "Salin tautan" and the scenario edit icon button all show a 2 px teal outline (`11`, `12`, `43`). | Same. |
| B-P1-4 | P1 | Guest landing during a hang: skeletons for 35 s, no message | **fixed** | The panel skeleton carries a loading line. One explained error with Retry appears at 10.2 s; at 12 s there are 0 skeletons and 1 alert, at 1440 and 390 (`04`, `05`). | Same. |
| B-P2-1 | P2 | Outage state: two identical alerts, "-" values, self-contradicting copy | **fixed** | 530: exactly one `role=alert`, 0 "-" values, copy matches the screen. The phone layout keeps the error in the first screen (`01`, `02`, `07`). The English outage copy was checked by the UX reviewer, not re-captured here. | Reviewer's open phone issue (UX-03) is fixed. |
| B-P2-2 | P2 | English locale showed Indonesian dataset names | **fixed** | English `/` and `/ringkasan` at 1440 and 390: 0 Indonesian leftovers by the baseline heuristic (`13`). | Same. The server-stored "Organisasi dihapus" in the operator console (UX-26, platform) is outside this finding. |
| B-P2-3 | P2 | Mobile overview: onboarding pushed the ICP value below the first screen | **fixed** | At 390 × 844 the value 112,81 is at y = 362–413, and onboarding comes after the market cards (`14`). | Same. |
| B-P2-4 | P2 | Performance: `/ringkasan` slow on Fast 3G; heavy JS | **partly** | **Improved:** Fast 3G `/ringkasan` content ready 5,430 → 3,887 ms and LCP 4,368 → 3,148 ms (both −28 %); requests 31 → 24 and 17 → 12; landing JS 162.8 → 155.8 KB transferred; landing guard 152,876 B gzip under the 160,000 B budget (integration build: 154,156 B gzip, +1,280 B, still under budget). **Worse:** `/ringkasan` JS 285.3 → 306.2 KB; total transfer up by the 65.3 KB of self-hosted fonts; desktop `/ringkasan` LCP 504 → 684 ms (+36 %). | Corrected: the desktop `/ringkasan` LCP regression is real, not "unchanged within noise". PERF's own after-run also measured 684 ms. |
| B-P2-5 | P2 | Declared font "Inter" was never loaded | **fixed** | `document.fonts`: IBM Plex Sans Variable and IBM Plex Serif 600 loaded (65.3 KB). The body font is IBM Plex Sans Variable. | Same. |
| B-P2-6 | P2 | Invalid `<dl>` structure (axe serious) on `/metodologi`, `/akun` | **fixed** | axe: 0 violations on all 28 guest and member route/width runs, and on the 12 operator runs (baseline: 56 real nodes plus 80 false-positive contrast nodes). | Same. |
| B-P3-1 | P3 | Interval level not in the URL | **fixed** | `?tingkat=95` survives a reload; the header reads "Rentang 95% USD/bbl" (`42`). | Same. |
| B-P3-2 | P3 | Uppercase, letter-spaced labels | **fixed** | 0 uppercase text nodes in 120 captures. Navigation and report kinds are now sentence case ("Riwayat harga", "Laporan pasar bulanan"). | Reviewer's Title Case residue (UX-08) is fixed. |
| B-P3-3 | P3 | Small hero photo at phone widths | **fixed** | No hero photo; the official ICP market panel reads at full width at 320 and 390 (`15`). | Same. The unused photo files (UX-24) no longer ship: they moved to `docs/assets/photos/`, and the integration build's `img/` holds only `img/product/*` (681,713 B of photos fewer). `docs/assets/photo-sources.json` names the new paths (open item 5). |
| B-P3-4 | P3 | `/proyeksi` tables cut at 1024 px | **fixed** | At 1024 the result table fits (scroll width 684 = client width 684), with the kind folded under the month (`16`). | Same. |
| B-P3-5 | P3 | Type in px: text-only zoom has no effect | **fixed** | Integration build, Chrome text size 200 % (root 16 → 32 px). **Baseline check (`a11y.mjs zoom`):** on `/ringkasan` the h1 goes 24 → 48 px and the body 15 → 30 px (first after run: 24 → 24 and 15 → 15). **Sweep of 21 routes at 1440, 768 and 390 px (63 runs):** body 30 px everywhere; h1 48 px, 52 px on document pages and 68 px on the landing. Headers grow: member 56 → 73/81 px, public 61 → 89/91 px. `--sticky-top` equals the measured header height in all 57 runs with a header. Results: 0 horizontal overflow, 0 control labels spilling, 0 text nodes below 20 px. The only cut text is 8 nodes of the deliberate three-line insight clamp on `/ringkasan`. **Tab walk at 200 %:** 0 focus targets fully hidden. **axe at 100 %:** 0 violations in 12 scans. Browser zoom 200 % still reflows (`29`). Images `46`–`49`. | Was open. Fixed by the rem type scale and control heights (integration change 5), with review D's fixes on top: wrapping labels, tabs and the "show all" row; a scrolling sidebar; chart geometry that follows `--fs-meta`; and the phone name ellipsis. Before those fixes, review D found 6 phone routes with cut labels at 200 %, overlapping landing chart labels and a sidebar showing 1 of 7 links. In this run there are no cut labels, the chart labels are apart at 390 px (the width checked; fix D reports 1440 too) and the sidebar shows 5 of 7. Not re-measured: 150 % text, and dialogs, sheets and the scenario editor's open states at 200 %. |
| B-P3-6 | P3 | Visual tokens differed from the design system | **fixed** | Canvas `rgb(243,245,248)` (#F3F5F8), body 15 px IBM Plex Sans, serif navy hero h1, no uppercase labels, sentence-case navigation, teal only for interaction. | Reviewer's Title Case residue is fixed. |

**Summary:** 16 of the 18 findings are fixed (B-P3-5 since the integration build). B-P0-2 is fixed in code but not
deployed, so it stays partly fixed. B-P2-4 is partly fixed: Fast 3G is better, but desktop `/ringkasan` LCP and the
transfer size are worse (not re-measured on the integration build).

## Before → after images

| # | Before | After | What changed |
|---|---|---|---|
| 01 | `01-staging-landing-skeletons-45s-1440.jpg` | `01-guest-landing-api-530-explained-1440.jpg` | Skeletons forever on staging → one explained error at 0.3 s on the new build (staging itself not redeployed) |
| 02 | `02-staging-landing-skeletons-390.jpg` | `02-guest-landing-api-530-explained-390.jpg` | Same at phone width; header no longer wraps |
| 03 | `03-hang-member-landing-fullpage-skeleton-12s-1440.jpg` | `03-hang-member-landing-explained-12s-1440.jpg` | Grey page → complete landing with an explained wait |
| 04 | `04-hang-guest-landing-kpi-skeletons-12s-1440.jpg` | `04-hang-guest-landing-explained-12s-1440.jpg` | Skeleton KPIs → one explained error with Retry |
| 05 | `05-hang-guest-landing-timeout-40s-1440.jpg` | `05-hang-guest-landing-one-alert-40s-1440.jpg` | Two identical alerts → one |
| 06 | `06-hang-member-ringkasan-sent-to-signin-40s-1440.jpg` | `06-hang-member-ringkasan-session-unconfirmed-40s-1440.jpg` | Sent to sign-in → stays, explains, offers Retry |
| 07 | `07-outage530-guest-landing-1440.jpg` | `07-outage530-guest-landing-one-alert-1440.jpg` | Two alerts and "-" values → one alert, no dashes |
| 08 | `08-outage530-member-ringkasan-sent-to-signin-390.jpg` | `08-outage530-member-ringkasan-session-unconfirmed-390.jpg` | Sign-in form → outage explanation |
| 09 | `09-scenario-result-stale-after-edit-1440.jpg` | `09-scenario-result-labelled-stale-after-edit-1440.jpg` | Silent stale result → labelled, with the assumptions used and "Jalankan ulang" |
| 10 | `10-report-download-creates-duplicates-1440.jpg` | `10-report-downloads-create-no-duplicates-1440.jpg` | Three reports → one |
| 11 | `11-archive-dialog-cancel-focus-invisible-1440.jpg` | `11-archive-dialog-cancel-focus-visible-1440.jpg` | Invisible focus → 2 px teal outline |
| 12 | `12-ghost-button-focus-invisible-copylink-1440.jpg` | `12-ghost-button-focus-visible-copylink-1440.jpg` | Invisible focus → 2 px teal outline |
| 13 | `13-english-ringkasan-indonesian-dataset-labels-1440.jpg` | `13-english-ringkasan-english-dataset-labels-1440.jpg` | Indonesian dataset names → English |
| 14 | `14-ringkasan-390-onboarding-pushes-kpis-below-fold.jpg` | `14-ringkasan-390-kpis-first.jpg` | ICP value at about y = 1000 → y = 362 |
| 15 | `15-landing-320-small-hero-photo.jpg` | `15-landing-320-live-panel-no-photo.jpg` | Covered photo inset → market data panel |
| 16 | `16-proyeksi-1024-tables-cut-at-right.jpg` | `16-proyeksi-1024-tables-fit.jpg` | Cut tables → fitting table |
| 17 | `17-landing-1440-full.jpg` | `17-landing-1440-full.jpg` | Reference: redesigned landing |
| 18 | `18-ringkasan-1440-full.jpg` | `18-ringkasan-1440-full.jpg` | Reference: redesigned overview |
| 19 | `19-harga-390-record-cards.jpg` | `19-harga-390-record-cards.jpg` | Reference: record cards now carry the evidence status |
| 20 | `20-masuk-1440.jpg` | `20-masuk-1440.jpg` | Reference: sign-in page |
| 21 | `21-empty-scenarios-1440.jpg` | `21-empty-scenarios-1440.jpg` | Empty state (after: list emptied by a mock) |
| 22 | `22-empty-notifications-1440.jpg` | `22-empty-notifications-1440.jpg` | Empty state |
| 23 | `23-forecast-3-month-run-1440.jpg` | `23-forecast-3-month-run-1440.jpg` | Journey: 1.6 s → 1.7 s, same behaviour |
| 24 | `24-scenario-compare-1440.jpg` | `24-scenario-compare-1440.jpg` | Journey: comparison with distinct markers |
| 25 | `25-signout-back-shows-signin-1440.jpg` | `25-signout-back-shows-signin-1440.jpg` | Journey: unchanged (works) |
| 26 | `26-skip-link-first-tab-1440.jpg` | `26-skip-link-first-tab-1440.jpg` | Unchanged (works) |
| 27 | `27-drawer-open-focus-trapped-390.jpg` | `27-drawer-open-focus-trapped-390.jpg` | Unchanged (works); the drawer is now the light customer sidebar |
| 28 | `28-signin-inline-errors-1440.jpg` | `28-signin-inline-errors-1440.jpg` | Empty e-mail now says "wajib diisi" instead of "tidak valid" |
| 29 | `29-ringkasan-browser-zoom-200-reflow.jpg` | `29-ringkasan-browser-zoom-200-reflow.jpg` | Still reflows. Text-only zoom was still open at this capture; it is fixed on the integration build (B-P3-5, `46`–`49`) |
| 30 | `30-report-generated-pdf-csv-1440.jpg` | `30-report-generated-pdf-csv-1440.jpg` | Journey: 1.9 s → 2.1 s; focus moves to the first download |
| 31–45 | – | see [`after/INDEX.md`](after/INDEX.md) | New states: stale public copy, slow notice, stale comparison (and after reload), report outcome unknown, data-status popover, partial failure, landing during the session check, recovery after an outage, interval in the URL, ghost focus, operator console, and one residual (`45`, superseded by `52`) |
| 46–49 | – | see [`after/INDEX.md`](after/INDEX.md) | Integration build, Chrome text size 200 %: landing at 390, `/ringkasan` at 1440, `/harga` at 390, `/laporan` at 390 with the grown top bar (B-P3-5) |
| 50–53 | – | see [`after/INDEX.md`](after/INDEX.md) | Integration build. Two simulated states, labelled on the image: the report "still being generated" notice (`50`, 504 then 409) and the conflict-month landing (`51`). Two loading states: the boot note on a workspace deep link while whoami hangs (`52`) and the page-loading note while only a chunk is slow (`53`) |

## Headline numbers

| Measure | Before | After |
|---|---|---|
| axe violations (WCAG 2.x A/AA), 14 routes × 1440/390 | 16 rule hits, 136 nodes (56 excluding the verified sidebar false positive) | 0; operator console (new, 12 runs): 0, best-practice 0 |
| Focus stops without an indicator, or hidden under a sticky header (30 Tab walks) | not measured; ghost buttons had none | 0 without an indicator, 0 hidden, 0 partly hidden |
| Horizontal overflow | 0 of 84 captures | 0 of 120 captures (operator console included) |
| Console and HTTP errors in the matrix | 0 | 0 |
| Skeletons left after load, pages without exactly one h1, uppercase text | not measured | 0, 0, 0 |
| `/ringkasan` Fast 3G content ready / LCP (ms) | 5,430 / 4,368 | 3,887 / 3,148 |
| `/ringkasan` desktop LCP (ms) | 504 | 684 |
| `/` Fast 3G LCP (ms) | 2,356 | 2,176 |
| Landing JS transferred (KB) | 162.8 | 155.8 (guard: 152,876 B gzip, budget 160,000) |
| Landing guard, integration build (B, raw / gzip) | – | 491,426 / 154,156 (first after run 487,632 / 152,876); no Recharts code; 5,844 B gzip under budget |
| Entry CSS (B, raw / gzip) | 65,266 / 13,390 | 75,695 / 13,974; integration build 76,318 / 14,098 |
| Stock photos in the build's `img/` (B; 6 files) | 681,713 (the landing used them) | 681,713 unused in the first after run; 0 in the integration build |
| `/ringkasan` JS transferred (KB) | 285.3 | 306.2 |
| Web fonts (KB) | 0 (font never loaded) | 65.3 (IBM Plex Sans + Serif 600) |
| Chrome text size 200 %, `/ringkasan` h1 / body (px) | 28 → 28 / 14.5 → 14.5 (no effect) | 24 → 24 / 15 → 15 in the first after run; 24 → 48 / 15 → 30 in the integration build |
| Text size 200 %, 21 routes × 1440/768/390 (integration build) | not measured | 0 of 63 with horizontal overflow; 0 spilling control labels; 0 text below 20 px; 8 cut text nodes, all in the deliberate insight clamp |
| Time to an explained state, guest `/`, API 530 / hang | two alerts by 8 s (time not measured) / about 35 s | 0.3 s / 10.2 s |
| Member `/ringkasan`, API 530 / hang | sent to sign-in / sent to sign-in after 35 s | explained in place at 1.8 s / 8.2 s |
| Workspace deep link, hanging whoami: first text on screen | none until sign-in at 35 s | none until 8 s in the first after run; "Memeriksa sesi Anda di server…" from 1.5 s in the integration build (seen at 2.5 s, not at 0.9 s) |
| Report generate plus 2 downloads | 3 `POST /reports` | 1 |

## Open items seen in the after run

1. **Staging not redeployed (B-P0-2).** The public staging URL still shows the 2026-10-17 behaviour. Deploying is
   the owner's decision.
2. **Text-only zoom (B-P3-5): resolved on the integration build.** See the B-P3-5 row and `after/measurements-after.txt`
   section 8. Remaining gaps:
   - Not measured on this build: 150 % text, changing the text size while a page is open, and dialogs, sheets and the
     scenario editor's open states at 200 %.
   - At 200 % on a phone the app top bar shortens the product name to "IC…".
   - On `/harga` at 1440 the focusable table region is taller than the viewport, so part of it sits under the top bar
     (as before).
3. **Desktop `/ringkasan` LCP +180 ms (B-P2-4).** The LCP element is now the page description, painted with the web
   font. Options: preload the serif only on pages that use it above the fold, or check `size-adjust` fallback metrics
   so the swap does not move the LCP candidate. `/ringkasan` also loads 21 KB more JS.
4. **Workspace deep link during a hang (`after/45`): mostly resolved.** The placeholder now says "Memeriksa sesi Anda
   di server…" from 1.5 s (`after/52`). Brand and navigation still appear only after the session check, up to the 8 s
   whoami timeout (within contract C3). The side effect review D found, the same note inside the signed-in shell while
   only a page chunk loads, is fixed: that case now says "Memuat halaman…" (`after/53`).
5. **Unused photos (UX-24): resolved.** The six files moved byte-identically to `docs/assets/photos/`, and the
   integration build no longer contains them. `docs/assets/photo-sources.json` now names the `docs/assets/photos/…`
   paths and "Not used (was: …)" placements (review D, RD-09; main agent, 2026-10-27, checksums unchanged).
6. **Legal placeholders (UX-06).** `/syarat` and `/privasi` still show the owner placeholders; this needs owner and
   counsel input.
7. **Measurement artefact.** On desktop `/`, two of three runs recorded no LCP entry; content-ready is available for
   all runs.
8. **"Repeat last month" figure changed on public pages (review D, RD-05).**
   - The landing and the methodology page now compare the model with 6,06, persistence on the model's own 69 cases. The
     first after run showed 6,09, the registry metric on 66 cases.
   - Resolved by the main agent (2026-10-27): the hold-last accuracy insight now compares 6,11 with 6,06 on the same
     69 cases (registry figures only as a fallback, with a test), and the report and README name both figures with
     their case sets. The operator model page shows the registry metrics as stored (6,09 on 66 cases), by design.
   - The copy change is visible to readers; it is listed as owner decision 8 in the report.
9. **Server texts with "(s)" plurals (review D, RD-11): resolved.** The per-point backtest summary and the "Sources
   differ" note now choose singular or plural in English, each with a test (main agent, 2026-10-27).
10. **Images `01`–`45` predate the integration changes.** No image before `51` shows a conflict month. They show px type and 6,09
    where the landing's accuracy figures appear. Images `46`–`53` show the current behaviour for the parts that
    changed.

## Scripts

The after scripts are kept in the QA scratch folder (`qa/v2/`), next to the baseline scripts:

| Script | What it does |
|---|---|
| `matrix2.mjs` | Matrix, operator console added |
| `states2.mjs` | Sequential (one browser), with the new states added |
| `journeys2.mjs` | Journeys adapted to the new markup |
| `obscured2.mjs` | Focus-not-obscured walk |
| `axe-ops.mjs` | axe on the operator console |
| `misc2.mjs` | Targeted checks |
| `guest-guard.mjs` | Policy v4: no forecast values on guest pages |
| `probe-offenders.mjs` | Checks that off-screen elements are only in the drawer |
| `curate_after.py` | Curates the after images into this folder |
| `textzoom-int.mjs` | Integration build: text size 200 % on 21 routes × 1440/768/390, Tab walks at 200 %, axe at 100 %. A copy of review D's checker as fix D re-ran it, with a write-blocking safety net |
| `integration-shots.mjs` | Integration build: images `46`–`53` and their measurements (route mocks for `50` and `51`; whoami held for `52`; chunk delayed for `53`) |
| `curate_int.py` | Curates `46`–`53` (JPEG q75, at most 160 kB; draws the "simulated state" band on `50` and `51`) |

The baseline scripts `a11y.mjs`, `perf.mjs` and `resources.mjs` ran unchanged. On the integration build, `a11y.mjs zoom`
and `v2/guest-guard.mjs` ran unchanged (`QA_OUT=qa/after-int`, `QA_DATA=qa/data-after-int`, `ICP_BASE` set to `:4180`).
