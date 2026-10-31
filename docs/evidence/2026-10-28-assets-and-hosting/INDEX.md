# Visual assets: evidence (2026-10-27/28)

Screenshots and measurements for the visual-assets change: illustrated "Cara kerja" steps, a photograph
behind the landing's closing call to action and behind the sign-in / registration brand panel, an
illustration beside the methodology title, an illustrated not-found page, and the workspace empty-state
illustrations redrawn in the token palette. Sources, licences and checksums of every image are in
[`docs/ASSET_INVENTORY.md`](../../ASSET_INVENTORY.md); placement, licence policy and performance rules
in [`docs/DESIGN_SYSTEM.md`](../../DESIGN_SYSTEM.md#imagery).

Updated on 2026-10-28 after an independent review of the first version. The closing band's crop
changed, the sign-in photograph was re-encoded and lost `fetchpriority="low"`, both photographs are
hidden in forced-colours themes, and the licence guard was tightened and added to CI. The affected views
were captured again from the final build, and the results below were measured again (see
[Review findings](#review-findings-and-how-they-were-resolved)).

## How these were captured

| | Before | First version (reviewed) | Final |
|---|---|---|---|
| Build | Production build of the tree before this change (`vite build`, frozen copy) | Production build the review examined (frozen copy) | Final production build; `apps/web/dist` is byte-identical to the frozen copy used here |
| Served by (`vite preview`) | `127.0.0.1:4190` (captures), `:4211` (2026-10-28 measurements) | `:4191` (captures), `:4217` (measurements) | `:4216` (captures and measurements) |
| Public data | The local API (`:8000`) through the preview proxy: real official data (ICP September 2026 = 112,81) | Same | Same |

- Headless Chrome (playwright-core 1.56.1, system Chrome), one browser at a time, device scale 1,
  reduced motion, Indonesian unless marked `-en`.
- `01`–`30` were captured from the before build and the first version, except `04`, `05`, `07`, `09`
  and `11`–`14`, which were captured again from the final build on 2026-10-28 (same file names).
  `31`–`36` are new views of the final build. `37` and `38` are the first version of the closing band
  (byte-identical copies of the first `04` and `07`), kept for comparison.
- Section images are clips of a full-page capture (no sticky header drawn over them). Page images are
  the first screen at the given width.
- Workspace views (`26`–`30`) use a fully mocked API in the browser (no request reached a backend and no
  account was used): `whoami` answers a fictitious trial owner, the notification and report lists are
  empty, every other endpoint answers 404. The warnings around those panels come from the endpoints the
  mock does not answer, so `27`–`30` are cropped to the empty-state panel.
- JPEG quality 75, each file at most 160 kB (largest 96 kB); nothing was downscaled.

## Files

| File | Build | View | Shows |
|---|---|---|---|
| `01-landing-steps-1440-before.jpg` | before | `/` 1440, section "Cara kerja" | Four text-only steps |
| `02-landing-steps-1440-after.jpg` | first version | same | One original illustration per step on a flat tile under the numbered rule; titles and fact lines still aligned across the four columns (unchanged in the final build) |
| `03-landing-closing-1440-before.jpg` | before | `/` 1440, closing band | Plain navy band |
| `04-landing-closing-1440-after.jpg` | final | same | Navy-toned photograph of ships on the horizon (CC0) under a navy shade; the horizon and its three ships run through the band's bottom padding, below the text and the buttons |
| `05-landing-closing-1440-en-after.jpg` | final | same, English | English copy over the same band |
| `06-landing-steps-390-after.jpg` | first version | `/` 390, "Cara kerja" | Phones: each heading beside its number, the illustration under it (260 px tile), then the text |
| `07-landing-closing-390-after.jpg` | final | `/` 390, closing band | Stacked text and buttons over an even 86 % shade; one ship on the horizon in the bottom padding, below the buttons |
| `08-landing-steps-320-after.jpg` | first version | `/` 320, "Cara kerja" | Narrowest supported width |
| `09-landing-closing-320-after.jpg` | final | `/` 320, closing band | Same at 320 px |
| `10-masuk-1440-before.jpg` | before | `/masuk` 1440 | Plain navy brand panel |
| `11-masuk-1440-after.jpg` | final | same | Navy-toned photograph of an offshore platform at dusk (CC0, 16 kB AVIF) behind the panel; the live card and the list keep their contrast |
| `12-daftar-1440-after.jpg` | final | `/daftar` 1440 | Registration uses the same panel |
| `13-masuk-1101-after.jpg` | final | `/masuk` 1101 × 768 | Narrowest width with the panel: the text column stays fully shaded |
| `14-masuk-1920-after.jpg` | final | `/masuk` 1920 × 1080 | Widest: more of the photograph shows to the right of the text column (1200 px rendition) |
| `15-masuk-390-after.jpg` | first version | `/masuk` 390 | Phones: no panel, so no photograph is requested (unchanged in the final build) |
| `16-metodologi-1440-before.jpg` | before | `/metodologi` 1440 | Empty column right of the article |
| `17-metodologi-1440-after.jpg` | first version | same | Original illustration beside the title (document, check, chart with its range); the process explainer below in the benchmark series colours |
| `18-metodologi-1280-after.jpg` | first version | `/metodologi` 1280 | Below 1281 px the column does not exist: no illustration, and the file is not downloaded |
| `19-metodologi-390-after.jpg` | first version | `/metodologi` 390 | Phones: unchanged reading layout |
| `20-metodologi-process-1440-before.jpg` | before | process explainer | Bars in navy / old teal / amber |
| `21-metodologi-process-1440-after.jpg` | first version | same | One bar per benchmark in its series colour (Brent, Dubai, WTI), amber only on the decree seal |
| `22-not-found-1440-before.jpg` | before | unknown URL, guest, 1440 | One sentence in a full-width panel |
| `23-not-found-1440-after.jpg` | first version | same | Drawing of a chart that breaks at a missing month, the message and "Kembali ke beranda"; the panel now lines up with the header |
| `24-not-found-390-after.jpg` | first version | same, 390 | Drawing above the message |
| `25-not-found-1440-en-after.jpg` | first version | same, English | "Back to the home page" |
| `26-not-found-workspace-1440-after.jpg` | first version | unknown URL, signed in (mocked), 1440 | The same page inside the workspace shell |
| `27-empty-notifications-before.jpg` | before | `/pemberitahuan` inbox, empty (mocked) | Bell with an amber dot |
| `28-empty-notifications-after.jpg` | first version | same | Token palette (quiet grey dot; amber is reserved for the decree seal) |
| `29-empty-reports-before.jpg` | before | `/laporan` history, empty (mocked) | Old teal and an amber chip |
| `30-empty-reports-after.jpg` | first version | same | Token teal check and teal-line chip |
| `31-landing-closing-768-after.jpg` | final | `/` 768, closing band | Tablet: buttons under the text, the ships along the bottom padding |
| `32-landing-closing-1101-after.jpg` | final | `/` 1101, closing band | Same rule at 1101 px |
| `33-landing-closing-1920-after.jpg` | final | `/` 1920, closing band | Widest: the ships stay in the bottom padding |
| `34-landing-closing-390-text200-after.jpg` | final | `/` 390, text at 200 % | The band grows with the text; the horizon follows the band's bottom edge and stays below the buttons |
| `35-landing-closing-1440-forced-after.jpg` | final | `/` 1440, forced colours | Windows contrast theme: the photograph is hidden (its shade would be dropped) and the band uses the theme's background; the image is not even downloaded |
| `36-masuk-1440-forced-after.jpg` | final | `/masuk` 1440, forced colours | The brand panel without the photograph; the check icons and the live card read against the theme's background |
| `37-landing-closing-1440-first-version.jpg` | first version | `/` 1440, closing band | For comparison with `04`: the centred crop put the third ship behind the buttons (a dark stub between them) |
| `38-landing-closing-390-first-version.jpg` | first version | `/` 390, closing band | For comparison with `07`: the ships' silhouettes ran under the lead line like stray dashes |

## Results (final build unless noted)

**Accessibility (axe-core, WCAG 2.0/2.1/2.2 A and AA tags).** **0 violations in 19 scans** of the
final build:
- `/` at 1440, 768, 390 and 320, `/masuk` at 1440 and 390, and `/daftar` at 1440;
- six routes (`/`, `/metodologi`, `/masuk`, `/daftar`, `/syarat` and an unknown URL) at 390 and 1440.

The first version also had 0 violations in 13 scans, among them the signed-in (mocked) not-found,
notifications and reports views. The review fixes did not change those views. No console error or
warning in any view.

**Horizontal overflow, by element bounds.** `html, body { overflow-x: clip }` keeps the page width
equal to the window width, so the first version's check (page width against window width) could not
reveal overflow (review finding P3-4). The check was therefore done again by comparing every rendered
element's box with the window width. An element inside an ancestor that crops it within the window
counts as cropped, not as overflow.

It covered 36 views: `/`, `/metodologi`, `/masuk`, `/daftar`, `/syarat` and an unknown URL at 320, 390,
768, 1101, 1440 and 1920 px. It also covered the 15 captured views, including 200 % text at 390 px.
- **Nothing extends past the right edge.**
- The only cropped content is the product-tour screenshot inside `.tour-crop`, which is cropped on
  purpose.
- The skip link and the screen-reader-only status region sit off-screen to the left on purpose.

**Text over the photographs.** axe cannot judge contrast over an image, so it was measured. Every text
box was recorded, the text was made transparent, and the text colour was compared with the **lightest**
pixel behind it.
- **Closing band** (320, 390, 390 at 200 % text, 768, 1101, 1440, 1920): lowest **6.23 : 1**, which is
  the primary button's label on its own teal fill. The lead line directly over the photograph is at
  least 6.81 : 1.
- **Brand panel** (1101, 1440, 1920; sign-in and registration): lowest **4.87 : 1**, the "naik" label
  inside the change chip, which has its own background. That is the same minimum as without the
  photographs. All other text on the panel is at least 5.27 : 1.

**Performance of `/`.** Method: fresh context, cache disabled, 1440 × 900. The slow profile is
1.6 Mbps with 150 ms RTT and a 4× CPU slowdown. Before and final were interleaved on 2026-10-28.
Figures are medians of 9 slow runs and 20 desktop runs per build.

| `/` | Before | Final | Brief's reference (wave C build, 2026-10-26) |
|---|---|---|---|
| Slow LCP | 2,284 ms | 2,316 ms (+1.4 %) | 2,176 ms |
| Slow content ready | 2,788 ms | 2,826 ms (+1.4 %) | 2,657 ms |
| Desktop content ready | 211 ms | 214 ms | 214 ms |
| Desktop LCP | 214 ms (6 of 20 runs report one) | 240 ms (8 of 20) | 236 ms (1 of 3 runs reported one) |
| Requests / image bytes on load | 12 / 56,102 B | 12 / 56,102 B | 12 / 54.8 kB |
| LCP element (slow) | `H1` in 9 of 9 | `H1` in 9 of 9 | `H1` |

- **Machine speed:** the before build measures about 5 % slower than the brief's reference on this
  machine today. The comparison that matters is before against final in the same session.
- **Desktop LCP:** Chrome reports no desktop LCP entry in most runs for both builds (the hero fades
  in), so desktop LCP rests on a few runs and is noise-level.
- **Desktop content ready:** a first batch of 9 runs gave 203 ms against 220 ms. The 20-run batch
  above shows parity. Single runs spread from 189 to 261 ms, with one outlier at 312 ms.
- **First-screen requests:** the new images add none.
  - The step sprite is requested only when the steps come within 200 px of the viewport.
  - The closing photograph is lazy.
  - The methodology illustration is lazy and hidden below 1281 px.
  - The sign-in photograph exists only on the sign-in and registration pages.

**Performance of `/masuk` (review finding P2-1).** Same method, 1440 × 900. The three builds were
interleaved, with medians of 9 runs per build and profile.

| `/masuk` | Without the photograph (before) | First version (32 kB AVIF, `fetchpriority="low"`) | Final (16 kB AVIF, normal priority) |
|---|---|---|---|
| Slow LCP | 2,172 ms (a list item, 9 of 9) | 2,596 ms (the photograph, 9 of 9) | 2,376 ms (the photograph, 9 of 9) |
| Slow content ready (form shown) | 2,308 ms | 2,343 ms | 2,312 ms |
| Desktop LCP | 436 ms (a list item) | 460 ms (the photograph) | 428 ms (the photograph) |
| Desktop content ready | 443 ms | 453 ms | 432 ms |
| Requests / image bytes | 13 / 0 B | 14 / 32,361 B | 14 / 16,740 B |

On windows from 1101 px the photograph is still the largest paint of the sign-in and registration
pages. That is accepted and recorded as performance rule 6 in `DESIGN_SYSTEM.md`:
- The slow-connection LCP cost went from +424 ms (+19.5 %) to +204 ms (+9.4 %).
- The form is ready at the same time as without the photograph.
- On an unthrottled connection LCP is at parity.

A preload of the photograph was tried and not kept. It brought the slow LCP back to the time without
the photograph (2,192 ms against 2,192 ms, 9 interleaved runs). But in two batches the form became
ready 40 to 157 ms later than without the preload, because the photograph then competes with the
page's scripts.

**Bundles and licences.**
- **Landing bundle:** the landing JavaScript is 492,587 B, 154,504 B gzip, against the 160,000 B
  budget (first version: 492,760 B, 154,549 B gzip; before: 491,426 B, 154,156 B gzip).
- **Licence guard:** `npm run asset-licences` passes. All 25 image files under `apps/web/public/` are
  in the licence register (19 original, 6 CC0), and the SHA-256 of every file matches, originals
  included. Its 23 self-test cases pass, and it now also runs in CI.
- **Final build:** `apps/web/dist` is byte-identical to the build measured and captured here.

## Review findings and how they were resolved

| Finding | Resolution | Evidence |
|---|---|---|
| P2-1: the sign-in photograph became the largest paint (+16 % slow LCP) | The 750 px AVIF went from 32 kB to 16 kB, normal fetch priority, no preload. The remaining cost (+9.4 % slow LCP, form unchanged) is accepted and recorded as performance rule 6 | `/masuk` table above; `11`–`14` |
| P2-2: licence guard not in CI | CI step "Asset licence guard" (`npm run asset-licences`) in `.github/workflows/ci.yml` | `ci.yml` |
| P3-1: original files had no checksum | Every register row has the SHA-256 of the shipped file, and every row is checked. An `original` row may not cite an external source. SVGs are hashed with CRLF read as LF, so a Windows checkout and CI agree | Licence register; guard self-test |
| P3-2: missing formats; bundled or inlined images unchecked | `.apng`, `.jfif`, `.svgz`, `.jxl`, `.heic`/`.heif` (and other aliases) are listed. The guard fails on image files in `apps/web/src`, on image imports, `new URL()` and CSS `url()` that are not public paths, and on `data:image` URIs | Guard self-test (23 cases) |
| P3-3: stale photograph statements in the docs | `README.md`, `docs/LEGAL_AND_PRIVACY.md`, `docs/README.md`, `docs/COMMERCIAL_READINESS.md` and the inventory now describe the two CC0 photographs (no credit needed) and the unserved CC BY / BY-SA files | Those files |
| P3-4: overflow evidence method was blind | Element-bounds check over 36 + 15 views (above) | Results above |
| P3-5: ships behind the buttons and under the lead line | The picture box is twice the band's height and starts 24 px above it, so the photograph's horizon runs through the band's bottom padding at every width. Only the 1600 px rendition ships (on phones the photograph is drawn wider than the screen) | `04`, `07`, `09`, `31`–`34` against `37`, `38` |
| P3-6: forced colours dropped the shades | Both photographs are hidden in forced-colours themes; the sign-in photograph is not mounted there | `35`, `36` |
