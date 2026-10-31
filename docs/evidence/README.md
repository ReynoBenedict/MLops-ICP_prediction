# Evidence index

Screenshots and raw check output kept as evidence for [../PRODUCT_TRANSFORMATION_2026-10.md](../PRODUCT_TRANSFORMATION_2026-10.md),
[../REMEDIATION_REPORT_2026-10.md](../REMEDIATION_REPORT_2026-10.md) and the reports in [../archive/](../archive/).
Each folder is a dated capture of the local application; only
the newest folder shows the current UI. All captures use local test accounts (`.test` / `.invalid` e-mail
domains, organisations named "(lokal)"); none shows a token, password or secret.

Rules: one dated folder per verification run; keep only captures that prove a finding, a fix, a before/after
change or a current view used by the project README; no width or state duplicates; raw check output as
`.txt` next to the images; list every file here (the transformation folder lists its files in its own
`INDEX.md` files, which this index links). On 2026-10-20 the historical folders were pruned from 53 to 6
images (each deletion is listed in [../DOCUMENTATION_INVENTORY.md](../DOCUMENTATION_INVENTORY.md)).

## 2026-10-25-transformation/ (current)

Before and after evidence for [../PRODUCT_TRANSFORMATION_2026-10.md](../PRODUCT_TRANSFORMATION_2026-10.md):
`before/` (2026-10-25, the frozen production build of the original UI and the staging site) and `after/`
(2026-10-27, a production build of the transformed tree), each with its own `INDEX.md` and a measurements file
(performance, bundles, accessibility, matrix, states, journeys, policy v4 guards). The folder
[INDEX.md](2026-10-25-transformation/INDEX.md) gives the status of every baseline finding, the before → after
image table and the headline numbers. All captures use the local test organisation "PT Uji UX V3 (lokal)";
states that needed an unavailable or failing backend were produced with request mocks and are labelled so.

## 2026-10-20-production-readiness/

Wave F verification of the v7 production-readiness audit: `npm run check` log, bandit/pip-audit output,
independent security spot checks, data-plane check, restore rehearsal and six reviewed screenshots
(sessions, confirmations, error messages). Every file is listed in
[2026-10-20-production-readiness/INDEX.md](2026-10-20-production-readiness/INDEX.md).

## 2026-10-20-v6/

Captured 2026-10-20 on the dev stack with reduced motion, viewport size. Files `01`–`11` by the frontend
agent (`ux6\pw\shots-evidence.mjs`), the README views by the verification agent (`ux6\q\qshots.mjs`).

| File | Shows |
|---|---|
| `1440-landing.png`, `1440-ringkasan.png`, `1440-proyeksi.png`, `1440-laporan.png`, `1440-insight.png`, `0390-ringkasan.png` | Public landing page and customer workspace as of 2026-10-20 (used by the project README until 2026-10-27; the README now shows the transformation's after captures) |
| `01-forecast-accuracy-disclosure-1440.png`, `02-forecast-accuracy-disclosure-390.png` | Per-month accuracy as the server sentence: test period, n, horizon, mode, MAE vs the "repeat last month" baseline on the same cases, verdict chip, coverage with its own n and period (D-02); the 390 px layout stacks without clipping |
| `03-forecast-coverage-warning-1440.png` | Run-level coverage warning "Rentang mungkin terlalu sempit" (D-02) |
| `04-account-data-privacy-1440.png` | "Data & privasi" panel: owner ZIP export and organisation deletion request (S-12, S-13) |
| `05-account-deletion-confirm-390.png` | Deletion request behind a confirmation sheet; nothing is sent before confirming |
| `06-ops-jobs-no-overflow-320.png` | Operator jobs page fits at 320 px (F-3; tiles were 15 px outside the viewport) |
| `07-ops-model-evaluation-disclosure-1440.png` | Operator model card: evaluation disclosure, "empiris, tidak terkalibrasi" (D-02) |
| `08-ops-deletion-queue-1440.png` | Operator deletion-request queue (S-13) |
| `09-login-throttle-scope-390.png` | Sign-in `429 scope=client` explained as "from this device" (S-3; mocked response, fake e-mail) |
| `10-mobile-touch-targets-footer-390.png` | Footer, auth links and language buttons ≥ 28 px on touch screens (F-4) |
| `11-explorer-recorded-icp-legend-1440.png` | Legend "ICP tercatat" instead of the overclaiming "Teramati (resmi)" (D-08) |

## 2026-10-17-ux-v3/ (historical)

UX v3 pass, referenced by [UX_V3_REPORT_2026-10](../archive/UX_V3_REPORT_2026-10.md) §6–7.

| File | Shows |
|---|---|
| `before/1280-harga-after-logout.png` → `after/1280-after-logout.png` | Before/after: logout used to leave the user on the private page with an anonymous shell; now it returns to the public home with feedback |
| `after/1440-offline-ringkasan.png` | Backend offline: explained error with retry instead of endless skeletons |
| `after/browser-checks.txt`, `after/browser-checks-2.txt` | Raw browser check results of that pass |

## 2026-10-17-frontend-v2/ (historical)

Release-candidate frontend, referenced by the archived frontend and release reports.

| File | Shows |
|---|---|
| `1440-ringkasan-public.png` | **Before** access policy v4: a guest (sign-in buttons in the sidebar) saw the workspace including forecast values. Current behaviour: guests are redirected to `/masuk?next=…` and the public snapshot has no forecast values (verified 2026-10-20 at five widths) |
| `metrics-report.txt` | Per-capture layout measurements of the 44 captures taken that day (most images since removed) |

## 2026-10-16-react-v1/ (historical)

| File | Shows |
|---|---|
| `1440-overview.png` | **Before** the official-data redesign: first React version on the legacy series (gaps, ICP 6 months stale, model v3). Referenced by [RELEASE_REPORT_2026-10](../archive/RELEASE_REPORT_2026-10.md) §20 |
