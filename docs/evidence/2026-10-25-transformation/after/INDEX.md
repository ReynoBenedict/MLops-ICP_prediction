# "After" evidence, 2026-10-27

Captured on 2026-10-27 (WIB) after waves A to C of the product transformation. Source: a production build of the
current tree served by `vite preview` on `127.0.0.1:4176`, with `/api` proxied to the same local backend (`:8000`) that
the baseline used. Headless Chrome through playwright-core, reduced motion, device scale 1, Indonesian unless stated.
Outage, hang, stale-copy, partial-failure and report-timeout states are simulated in the browser:
- outage: every `/api/v1` call answered with an HTML 530;
- hang: calls are never answered;
- stale copy: the real public snapshot is served with the gateway's `X-ICP-Cache: stale` headers;
- partial failure: chosen calls answer 500;
- report timeout: a mocked 504 for `POST /reports`, so nothing reaches the server.

Staging (`icp-gateway-staging`) was not re-captured: nothing was deployed, and it still serves the 2026-10-17 release.
Images `01` and `02` therefore show the same failure (the API answers 530 with no stored copy) on the new build, not
on staging.

Signed-in views use the local test organisation "PT Uji UX V3 (lokal)" (member "Penguji UX", plan Trial). Before the
captures, its two QA scenarios were renamed to match their stored inputs ("Brent USD 120", "Brent USD 70"; the
second was then archived to free a trial slot), and the history gained one each of the Data provenance, Forecast and
Scenario comparison reports. The journeys then created "Brent USD 90", one forecast and one monthly report. The
database was backed up first (`var/backups/20261026T180150Z`). No image shows a token, password, e-mail address or
secret. No guest image shows a forecast value: the landing shows only the locked teaser, and a scripted check of all
public routes found none (`measurements-after.txt`, section 7).

Images are JPEG q75; `01`–`45` are at most 400 kB each and `46`–`53` at most 160 kB. Some are scaled or cropped. In
full-page captures of the workspace, the sidebar stops at the first viewport height, because it is sticky. Numbers
`01`–`30` mirror the before set: the same screen, or the state that replaced the defect. Numbers `31`–`45` are states that
did not exist before. The before → after comparison and the status of each baseline finding are in
[`../INDEX.md`](../INDEX.md).

**Images `46`–`53` come from a later build.** They were captured on 2026-10-27 between 18:30 and 18:45 WIB from a production
build of the tree after the main agent's integration changes and review D's fixes, served by `vite preview` on
`127.0.0.1:4180` with the same backend. They show the rem type scale at 200 % text, the report "still being generated"
notice, the conflict-month landing, the boot note and the page-loading note. Images `01`–`45` predate those changes:
- they show px type;
- the first-run build's landing compared the model with 6.09 for "repeat last month"; the integration build uses 6.06,
  measured on the same 69 cases;
- they show the earlier conflict wording.

Text size 200 % means Chrome's text-size setting, which the scripts set with CDP
`Page.setFontSizes` (standard 32 px, fixed 26 px). No page zoom is involved. Two of the new images show simulated
states and are labelled as such: a band across the top of each says so.
- `50`: `POST /reports` is answered by browser mocks (a 504, then a 409 `IDEMPOTENCY_IN_PROGRESS`).
- `51`: the public snapshot is rewritten in the browser to a conflict month.

Every other write request was aborted in the browser, so nothing reached the server. The numbers are in
`measurements-after.txt`, sections 2, 3, 7 and 8.

| File | What it shows |
|---|---|
| `01-guest-landing-api-530-explained-1440.jpg` | The failure that left staging on skeletons (every API call 530, no stored copy) on the new build: by 2.5 s the market panel shows one explained error with "Coba lagi" and a link to the ESDM source, and the rest of the landing is intact (first explained state at 0.3 s). |
| `02-guest-landing-api-530-explained-390.jpg` | Same at 390 px: the error sits at the top of the panel, inside the first screen, with full-width actions (UX-03 fix). |
| `03-hang-member-landing-explained-12s-1440.jpg` | A visitor with a stored session opens `/` while the API hangs. At 12 s the landing is complete, the panel explains the 10 s wait with Retry, and the header offers "Buka ruang kerja" (baseline: whole-page skeleton). |
| `04-hang-guest-landing-explained-12s-1440.jpg` | Guest, API hanging, 12 s: one explained error with Retry instead of skeleton KPIs (explained at 10.2 s). |
| `05-hang-guest-landing-one-alert-40s-1440.jpg` | Same guest at 40 s: still exactly one message (baseline: two identical callouts after 35 s). |
| `06-hang-member-ringkasan-session-unconfirmed-40s-1440.jpg` | Member on `/ringkasan` while the API hangs: the URL stays, and "Sesi Anda belum dapat diperiksa" explains the slow server with Retry from 8.2 s (baseline: sent to the sign-in page). |
| `07-outage530-guest-landing-one-alert-1440.jpg` | Whole landing during a 530 outage (scaled to 50 %): one alert, no "-" values, no contradicting copy. |
| `08-outage530-member-ringkasan-session-unconfirmed-390.jpg` | Member on `/ringkasan` at 390 px during a 530 outage: an explanation with Retry, no password field, URL unchanged. |
| `09-scenario-result-labelled-stale-after-edit-1440.jpg` | A scenario run with Brent 95, then edited to 90 and saved: the result keeps its numbers but is labelled "Asumsi skenario diubah setelah hasil ini dibuat", shows the assumptions it used (95) and offers "Jalankan ulang". |
| `10-report-downloads-create-no-duplicates-1440.jpg` | One "Buat laporan" plus PDF and CSV downloads: exactly one new row ("Baru") and one `POST /reports` (list 17 → 18). |
| `11-archive-dialog-cancel-focus-visible-1440.jpg` | Archive confirmation opened with the keyboard: the initially focused "Batal" has a 2 px teal outline. Nothing was archived. |
| `12-ghost-button-focus-visible-copylink-1440.jpg` | Insight sheet with keyboard focus on "Salin tautan" (ghost button): visible 2 px teal outline. |
| `13-english-ringkasan-english-dataset-labels-1440.jpg` | English `/ringkasan`: the Data status panel lists Official ICP, Brent, Dubai, WTI and JISDOR rate in English. |
| `14-ringkasan-390-kpis-first.jpg` | `/ringkasan` at 390 px: the official ICP value (112,81) is at y = 362–413, inside the first 844 px screen; the onboarding card comes after the market cards. |
| `15-landing-320-live-panel-no-photo.jpg` | Landing at 320 px: no hero photo; the official ICP market panel follows the headline and reads at full width. |
| `16-proyeksi-1024-tables-fit.jpg` | `/proyeksi` at 1024 px: the result table fits its frame (scroll width 684 = client width 684), with the value kind folded under the month. |
| `17-landing-1440-full.jpg` | Reference: the whole public landing at 1440 px (scaled to 50 %), including the re-captured product tour image. |
| `18-ringkasan-1440-full.jpg` | Reference: the whole `/ringkasan` at 1440 px (scaled to 50 %). |
| `19-harga-390-record-cards.jpg` | Reference: `/harga` at 390 px, the monthly table as record cards with evidence status; no horizontal overflow. |
| `20-masuk-1440.jpg` | Reference: the sign-in page, with the owner/admin password hint. |
| `21-empty-scenarios-1440.jpg` | Empty state of the scenario list and results. The organisation has scenarios now, so the list was emptied by a mock for this capture. |
| `22-empty-notifications-1440.jpg` | Empty state: no alert rules and no events (real). |
| `23-forecast-3-month-run-1440.jpg` | Journey: a 3-month forecast ran in 1.7 s, `?proyeksi=<id>`, focus on "Hasil proyeksi", status announced. |
| `24-scenario-compare-1440.jpg` | Journey: two scenarios compared against the base forecast (chart with distinct markers, table). |
| `25-signout-back-shows-signin-1440.jpg` | Journey: after signing out, Back lands on `/masuk?next=%2Fringkasan`; the old token gets 401. |
| `26-skip-link-first-tab-1440.jpg` | Accessibility: the first Tab on the landing shows "Lompat ke konten", which moves focus to `<main>`. |
| `27-drawer-open-focus-trapped-390.jpg` | Accessibility: the app drawer at 390 px traps focus (30 Tabs, 0 escapes), the page behind is inert, and Escape returns focus to the menu button. |
| `28-signin-inline-errors-1440.jpg` | Accessibility: an empty sign-in form marks both fields "Kolom ini wajib diisi.", ties the errors with `aria-describedby` and focuses the e-mail field. |
| `29-ringkasan-browser-zoom-200-reflow.jpg` | Accessibility: `/ringkasan` at 200 % browser zoom (720 CSS px) reflows with no horizontal overflow. At this capture text-only zoom still had no effect; it works on the integration build (B-P3-5, now fixed; see `46`–`49`). |
| `30-report-generated-pdf-csv-1440.jpg` | Journey: a monthly market report generated in 2.1 s; focus moves to the first download button. |
| `31-stale-public-copy-landing-1440.jpg` | New state: the gateway serves its last stored public copy. The panel shows the figures with "Layanan analitik belum menjawab saat ini. Angka ini salinan publik terakhir yang tersimpan pada 26 Okt 2026, 22.00 WIB." and no "Mutakhir" badge. |
| `32-stale-public-copy-landing-390.jpg` | The same stale-copy state at 390 px. |
| `33-slow-notice-ringkasan-9s-1440.jpg` | New state: two of the page's requests hang, and after 8 s one notice says how long the page waits before it explains a failure. |
| `34-scenario-comparison-stale-after-edit-1440.jpg` | New state: one compared scenario was edited (120 → 125). The comparison names it ("Diubah setelah perbandingan dibuat") and offers "Bandingkan ulang". |
| `35-scenario-comparison-still-stale-after-reload-1440.jpg` | The same comparison after a page reload: still labelled stale (baseline: the old value came back from the tab cache without a label). |
| `36-report-outcome-unknown-504-1440.jpg` | New state: `POST /reports` ends in a 504 (mocked). The page says the outcome is unknown, points to the list, and retries with the same `Idempotency-Key`. |
| `37-data-status-popover-1440.jpg` | New: the top-bar data status popover (period and status of ICP, Brent, Dubai, WTI and JISDOR). |
| `38-data-status-popover-390.jpg` | The same popover at 390 px. |
| `39-partial-failure-named-parts-ringkasan-1440.jpg` | New state: insights and evidence fail (500). One error names the missing parts, and the ICP card says "Bukti tidak tersedia". |
| `40-hang-member-landing-renders-3s-1440.jpg` | A stored session and a hanging API: at 3 s the landing and its navigation are on screen, with no sign-in or sign-up buttons while the session is checked. |
| `41-outage-member-ringkasan-recovered-after-retry-1440.jpg` | After a 530 outage the API returns: Retry leads straight to "Ringkasan pasar", with no password prompt. |
| `42-forecast-interval-95-kept-in-url-1440.jpg` | The 95 % interval is in the URL (`?tingkat=95`) and survives a reload. |
| `43-ghost-button-focus-visible-scenario-edit-1440.jpg` | The scenario card's edit icon button (ghost), reached with Tab: visible 2 px teal outline with a 2 px offset. |
| `44-operator-health-1440.jpg` | Reference (new): the operator console's system health page at 1440 px (scaled to 50 %). |
| `45-hang-workspace-deeplink-page-skeleton-3s-1440.jpg` | Residual: a workspace deep link (`/ringkasan`) with a stored session and a hanging API shows a bare page skeleton, with no brand, navigation or text, until the 8 s session-check timeout. This is within contract C3. Superseded on the integration build by `52`, which adds a session-check note from 1.5 s. |
| `46-text200-landing-390-chart-labels-apart.jpg` | Text size 200 %, guest landing at 390 px, from the top through the market panel. Root 32 px, h1 68 px, body 30 px, and the public header grows to 91 px. The two latest chart values (112,81 and 114,16) no longer overlap (review D finding RD-02), x labels do not touch, and all chart text stays inside the panel. No page overflow. Forecast window locked, with no values. |
| `47-text200-ringkasan-1440-topbar-grows.jpg` | Text size 200 %, `/ringkasan` at 1440 px (scaled to 75 %). The top bar grows from 56 to 73 px and `--sticky-top` follows it (73 px). The h1 is 48 px and the body 30 px. 5 of 7 sidebar links are visible without scrolling (review D: 1 of 7). |
| `48-text200-harga-390-show-all-row-wraps.jpg` | Text size 200 %, `/harga` at 390 px. The "Menampilkan 12 dari 36 bulan" row and its "Tampilkan semua (36)" button wrap inside the screen (review D: started at −37 px and cut at the right). Record cards with the evidence chip "Dua dokumen ESDM sepakat" on two lines. |
| `49-text200-laporan-390-topbar-81px-name-ellipsis.jpg` | Text size 200 %, a member page (`/laporan`) at 390 px. The top bar has grown to 81 px. The product name ends in an ellipsis ("IC…") instead of running under the data-status button (review D: RD-5). The h1 and the report-kind options wrap below the header. |
| `50-mocked-report-504-then-409-still-generating-1440.jpg` | **Simulated (mocked).** `/laporan` at 1440 px (scaled to 75 %), labelled by the band at the top. The first `POST /reports` gets a 504, and "Coba lagi" gets a 409 `IDEMPOTENCY_IN_PROGRESS` (Retry-After 15). The notice "Laporan ini masih dibuat di server" names the first send time ("Permintaan pertama dikirim 27 Okt 2026, 18.38 WIB"). It has its own countdown, "Tombol Coba lagi aktif kembali dalam 14 detik". "Coba lagi" and "Buat laporan" are disabled. Both sends used the same `Idempotency-Key` and body, and neither reached the server. |
| `51-mocked-conflict-month-landing-value-withheld-1440.jpg` | **Simulated (mocked).** The landing's market panel at 1440 px, with the public snapshot rewritten in the browser so that September 2026 is a conflict month, the way the backend withholds one; the band at the top says so. "nilai ditahan: dokumen resmi belum sepakat" and the chip "Sumber tidak sepakat" are shown. The rupiah fact reads "ditahan sampai dokumen resmi ICP sepakat", and the legend reads "Bulan dengan nilai ditahan". The withheld month is drawn as a band, and only Brent gets an end label. The real September value appears nowhere on the page. The data is not real for this state. |
| `52-hang-workspace-deeplink-boot-note-2.5s-1440.jpg` | A workspace deep link (`/ringkasan`) with a stored session while `/auth/whoami` is held: at 2.5 s the placeholder says "Memeriksa sesi Anda di server…" inside its `role=status` region (no text at 0.9 s; the note appears after 1.5 s). After the release the page opens "Ringkasan pasar". Top 600 px of the viewport. Compare `45`. |
| `53-slow-page-chunk-loading-note-in-shell-1440.jpg` | The session is confirmed (whoami answered in 158 ms) but the page's code chunk is delayed by 4.5 s. Inside the signed-in shell the placeholder says "Memuat halaman…", not the session text (review D finding RD-01 / RD-8, fixed). Scaled to 75 %. |
| `measurements-after.txt` | Numbers behind this set, each compared with the baseline: performance medians, bundle sizes, axe and focus-walk totals, overflow and document heights for 120 captures, state timings, journeys and the policy v4 guards. Section 8 and the "integration build" lines of sections 2, 3 and 7 belong to images `46`–`53`. |
