# Asset inventory

Every visual asset shipped with the product, where it is used, and its licence. This file is an
internal record; the site itself shows no credit line.

**Policy (since 2026-10-27).** The web app ships only material whose licence needs no credit where
it is shown: original work of this project, CC0 or public-domain works, and photographs under the
Unsplash License or the Pexels License. Material that needs a credit (CC BY, CC BY-SA, unknown
licences) is not served; Pertamina, ESDM or other company photographs and logos, and anything that
imitates an ESDM document, letterhead or logo, are never used. Every image file under
`apps/web/public/` has a row in the [licence register](#licence-register) at the end of this file,
with its licence and the SHA-256 of the file as shipped, which `scripts/check-asset-licences.mjs`
checks (`npm run asset-licences`, part of `npm run check` and of CI); the same script fails on an image bundled
from `apps/web/src` or inlined as a `data:image` URI. The two photographs the site uses are described
under [Photographs (no credit required)](#photographs-no-credit-required); the three CC BY / CC BY-SA
photographs the project used until 2026-10-24 are kept, unserved, in `docs/assets/photos/` (section
[Third-party photographs that need a credit](#third-party-photographs-that-need-a-credit-kept-not-served)),
with their source records in `docs/assets/photo-sources.json`.

## Original SVG assets

All files below are original work created for this project (2026-10-16), no third-party source;
they may be used under the repository licence. Palette of the brand files: navy `#123A5C`, teal
`#008C8C`, amber `#D99A2B`. The inline illustrations in `components/illustrations.tsx` were
redrawn in the design-token palette on 2026-10-27 (teal `#0F6B73` only for confirmed or actionable
marks, the data-series colours where a drawing shows data, amber only as the decree seal), the same
palette as the static illustrations in the next section.

| File | Placement | Notes |
|---|---|---|
| `apps/web/public/brand/logo-symbol.svg` | favicon source, app shell brand mark, PDF report header (drawn with the same geometry in `backend/icp_api/services/reports.py`) | Three rising rounded bars + amber upward stroke on a navy tile |
| `apps/web/public/favicon.svg` | browser tab icon | Copy of the logo symbol |
| `apps/web/public/brand/logo-wordmark.svg` | documentation, light backgrounds | Symbol + "ICP Intelligence" + tagline |
| `apps/web/public/brand/logo-wordmark-dark.svg` | dark backgrounds (sidebar, footer, auth side panel) | Teal tile variant |
| `apps/web/public/brand/app-icon-512.svg` | `apple-touch-icon`, future PWA/app icon | 512×512, gradient tile |
| `apps/web/public/brand/hero-composite.svg` | not currently used (was the landing hero fallback when `public/img/hero-refinery.*` was missing; the landing hero has shown the market panel since 2026-10-25); labelled "ilustrasi · bukan peta atau lokasi faktual" | Stylised refinery/tanker silhouettes, price curve, source → ICP flow |
| `apps/web/public/brand/og-image.svg` | Open Graph / social preview (`<meta property="og:image">`; rasterise to `og-image.png` for platforms that need PNG) | 1200×630 |
| `apps/web/src/components/brand.tsx` (`LogoSymbol`, `Wordmark`) | inline logo in the sidebar, topbar, auth and legal pages | React rendering of the same geometry |
| `apps/web/src/components/illustrations.tsx` — `IllNoData` | empty insight list, empty charts | dashed curve on an empty chart |
| `apps/web/src/components/illustrations.tsx` — `IllNoForecast` | "no forecast published yet" (overview, forecast page) | calendar with question mark |
| `apps/web/src/components/illustrations.tsx` — `IllScenarios` | empty scenario list | a base line (ICP blue) and two diverging scenario paths in the scenario colours with square/triangle markers |
| `apps/web/src/components/illustrations.tsx` — `IllAlerts` | empty notification inbox | bell with quiet dot |
| `apps/web/src/components/illustrations.tsx` — `IllUpgrade` | sign-in / plan gate notices (`GateNotice`) | lock with upward arrow |
| `apps/web/src/components/illustrations.tsx` — `IllReports` | empty report list | document with check |
| `apps/web/src/components/illustrations.tsx` — `IllPipeline` | not used by the public pages since 2026-10-25 (was the landing feature card and the auth side panel) | sources → verification → forecast |
| `apps/web/src/components/illustrations.tsx` — `IllTimeline` | not used by the public pages since 2026-10-25 (was the landing feature card: elapsed vs future months) | observed / origin / assumed markers |
| `apps/web/src/components/illustrations.tsx` — `IllIcpProcess` (added 2026-10-17) | Proyeksi ICP, "Keandalan dan keterbatasan" panel: process explainer (world prices → ministerial decree → official value on the platform) | three-stage explainer (benchmark bars in the Brent, Dubai and WTI series colours); paired with the numbered steps `meth.process.1..3` |
| `apps/web/src/components/illustrations.tsx` — `MarkDecree`, `MarkBarrel`, `MarkFx` (added 2026-10-17) | Ringkasan Pasar, "Pembaruan data terakhir" source list: source-type marks for decree/announcement (ICP), market price (Brent/Dubai/WTI) and exchange rate (JISDOR) | 16 px inline marks, `aria-hidden`, always next to the dataset label |
| `apps/web/src/components/charts.tsx` — `MarkerSwatch` / `dotFor` | scenario comparison chart and legend | circle / square / triangle / diamond markers so scenario identity never relies on colour alone |

No maps, refinery locations or pipeline routes are depicted; geography is never implied. These are
original ICP Intelligence artwork (no third-party attribution needed). Until 2026-10-24 the landing
footer credited them as "Ilustrasi dan ikon: karya orisinal ICP Intelligence"; that credit line was
removed together with the photo credits when the landing stopped using photographs.

## Original illustrations (static SVG files)

Added 2026-10-27. Original work drawn for this project by hand as SVG (no third-party source, no
template, no generated imagery); they may be used under the repository licence. Served from
`apps/web/public/img/illus/` as files, not inlined into JavaScript, so the landing bundle does not grow
with them. Style: navy `#123A5C` outlines on white and `#F7F9FB` paper, lines `#E1E6EC` / `#C8D0DA`,
teal `#0F6B73` only for a confirmed check, the data-series colours where a drawing shows data
(ICP `#1D6FB8`, Brent `#E8692F`), the coverage-grid and gap colours of the product, amber `#D99A2B`
only as the decree seal. No text elements, no logos, emblems or letterheads (document sheets carry
plain lines only), no maps or places, no figures. All are decorative (an `<img alt="" aria-hidden>` or
a CSS background on an `aria-hidden` element), always next to text that carries the meaning.

| File | Placement | Content |
|---|---|---|
| `apps/web/public/img/illus/steps.svg` (6.4 kB; a sprite of four 280 × 168 cells, groups `documents`, `verify`, `market`, `analysis`) | Landing, "Cara kerja": one cell per step, as the CSS background of a decorative tile (`.flow-art` in `public.css`), so the four drawings cost one request | 1 "Dokumen resmi": two generic document sheets, a round seal on the front sheet, an archive-tray badge. 2 "Verifikasi": a highlighted document line linked to a value card through a teal check; a row of month cells in the coverage-grid colours with one cell marked for weaker evidence. 3 "Konteks pasar": a chart card with two lines in the ICP and Brent series colours; three source marks (barrel, globe, exchange arrows). 4 "Analisis": a schematic workspace window (light sidebar with the active item, two KPI tiles, a chart whose line continues dashed inside a shaded range; no values, so no forecast is shown) |
| `apps/web/public/img/illus/meth-evidence.svg` (3.1 kB) | Data & metodologi, beside the title from 1281 px (lazy, so narrower screens never download it) | A document with a seal under a magnifier with a teal check; a row of month cells (one weaker, one future month blank); a chart card with observed points and a dashed range |
| `apps/web/public/img/illus/not-found.svg` (1.1 kB) | Not-found page, public site and workspace | A chart whose line breaks at a missing month (the product's gap tint and dashed edges) with a question mark drawn as a path |

## Photographs (no credit required)

Added 2026-10-27. Two photographs from Wikimedia Commons, both dedicated to the public domain by their
photographers under **CC0 1.0** (`https://creativecommons.org/publicdomain/zero/1.0/`), which needs
no attribution. Verified on 2026-10-27 through the Commons `imageinfo` API (`extmetadata`:
`LicenseShortName` "CC0", `License` "cc0", `AttributionRequired` false, `Restrictions` empty) and the
file pages; the SHA-1 of each downloaded original equals the API's `sha1`. The site shows no credit line
(none is required); this record is internal. Both are used as decorative backgrounds (`alt=""`,
`aria-hidden`) under a navy shade, never captioned, never named on the site, and never presented as
Indonesian, as a Pertamina or ESDM site, or as any specific company, place or event.

**1. Three ships on the horizon** (`apps/web/public/img/photo/ships-on-horizon-*`)

| Field | Value |
|---|---|
| Source page | [File:3tankers@ThessalonikiPort.jpg](https://commons.wikimedia.org/wiki/File:3tankers@ThessalonikiPort.jpg) |
| Author | John Maravelakis; first published on Unsplash in 2014 (CC0 at the time), imported to Commons on 2014-03-23; Commons category "Files from external sources with reviewed licenses" |
| Licence | CC0 1.0 (public domain dedication), no attribution required |
| What it shows | Three ships in a row on the horizon of a calm sea under an overcast sky, off Thessaloniki, Greece (2014-03-01). The Commons title calls them tankers; its categories also list bulk carriers and unidentified freighters, so this inventory calls them ships |
| Downloaded original | 4608 × 2592 JPEG, 10,400,966 B; SHA-256 `1e60ed20b669292aa605b869565808d54fc36d4249799e974dc2568752f7befb`; SHA-1 `19a402b1a9b79664fe55c0017b3bd0fd9f0653fc` (= Commons API); retrieved 2026-10-27 |
| Modifications | Cropped to 4608 × 2304 (rows 118–2422, horizon centred); Gaussian blur 1.6 px at full size (softens the grain of the original's filter); Lanczos resize to 1600 × 800; colour replaced by a navy tone (Pillow `ImageOps.autocontrast` 0.5 % then `ImageOps.colorize`: shadows `#0B2138`, mid-tones `#3A6283`, highlights `#CFDCE8`); AVIF (quality 42) and WebP (quality 72, method 6). The 800 × 400 renditions shipped on 2026-10-27 were removed on 2026-10-28: the band now keeps the horizon in its bottom padding, which on phones draws the photograph several times wider than the screen, so the browser would only have enlarged them |
| Shipped files | `ships-on-horizon-1600.avif` 13,105 B, `ships-on-horizon-1600.webp` 19,570 B |
| Placement | Landing, behind the closing call to action, under a navy shade (at least 72 %); lazy; the horizon with the three ships runs through the band's bottom padding, below the text and the buttons (`public.css`, `.closing-photo`); not shown in forced-colours (contrast) themes |

**2. Offshore oil platform at dusk** (`apps/web/public/img/photo/offshore-platform-dusk-*`)

| Field | Value |
|---|---|
| Source page | [File:Holstein at Dusk.jpg](https://commons.wikimedia.org/wiki/File:Holstein_at_Dusk.jpg) |
| Author | GuavaTrain (Wikimedia Commons user), own work, taken 2024-01-27, uploaded 2024-01-31 |
| Licence | CC0 1.0 (public domain dedication), no attribution required |
| What it shows | According to its Commons description, the offshore oil platform "Holstein" in Green Canyon, Gulf of Mexico (United States), lit at dusk. No company name or logo is legible in the shipped crop (checked at full resolution); the site does not name the platform |
| Downloaded original | 4000 × 3000 JPEG, 3,184,342 B; SHA-256 `a013715ba203358190bfce5adf322310052713b0d18ef6b628692604a434b659`; SHA-1 `daacbbcb2fd7ce54f047be634fb9943667cab46b` (= Commons API); retrieved 2026-10-27 |
| Modifications | Cropped to a 2250 × 3000 portrait (columns 1155–3405, platform centred); Gaussian blur 0.8 px; Lanczos resize to 1200 × 1600 and 750 × 1000; navy tone (shadows `#04121F`, mid-tones `#24496B`, highlights `#E6EEF6`); AVIF (quality 24) and WebP (quality 40, method 6). Re-encoded on 2026-10-28 from the same toned images (first shipped at AVIF 42 / WebP 64: 32,093 B for the 750 px AVIF), because on wide windows the photograph is the sign-in page's largest paint; under the panel's shade the two encodings look the same |
| Shipped files | `offshore-platform-dusk-1200.avif` 31,688 B, `offshore-platform-dusk-1200.webp` 71,502 B, `offshore-platform-dusk-750.avif` 16,472 B, `offshore-platform-dusk-750.webp` 40,682 B |
| Placement | Sign-in and registration brand panel (from 1101 px), under a navy shade; mounted only while the panel is visible and not in a forced-colours (contrast) theme; requested with the page at normal priority and not preloaded (a preload delayed the form on slow connections; the trade-off is in `docs/DESIGN_SYSTEM.md`, Imagery); `srcset` 750w/1200w |

Candidates looked at and not used (2026-10-27): *Marco Polo at Dusk* and *Oil Platform Crew Transfer*
(CC0, same photographer; the first is dominated by a gas flare, the second is a crew transfer to a
supply vessel that its description names),
*Crude oil tanker Eagle San Diego* (CC0; the ship's name is legible), *Crude Oil Tanker in Port Arthur,
Texas* (CC0; one identifiable ship in a named port), *An oil rig offshore Vungtau* (public domain; black
flare smoke dominates), *Preemraff Lysekil oil refinery on a foggy night* (CC0; too dark at web sizes),
and every CC BY / CC BY-SA candidate (would need a visible credit).

## Product screenshots (original, generated)

Added 2026-10-25 with the wave B public site. Original screenshots of the ICP Intelligence workspace
itself, taken from the local test organisation (official data and that organisation's own records
only; no third-party content), used by the landing product tour (`components/public/ProductTour.tsx`).
They are regenerated by the product-shot capture script (`capture-product-shots.mjs`, part of the
2026-10 transformation tooling, not of the repository), which also rewrites the manifest
`apps/web/src/components/public/product-shots.ts` (sizes and the latest official month shown). They
may be used under the repository licence. A new capture changes the files, so their SHA-256 values in
the [licence register](#licence-register) must be updated in the same change (`npm run asset-licences`
prints the new value of every changed file).

Rules the capture enforces: no forecast values (policy v4; the run stops when a value of any of the
organisation's forecast or scenario runs, or a result heading, is inside a frame); the member's name
and e-mail are blurred; freshness chips are hidden (a static image cannot stay "Mutakhir"); the
main column only (no sidebar user card); 1192 × 860 px WebP, at most 160 kB each.

| File | Placement | Content |
|---|---|---|
| `apps/web/public/img/product/riwayat-harga.webp` / `riwayat-harga-en.webp` | landing product tour, tab "Riwayat harga" (Indonesian / English) | Price history page with its full ICP vs Brent chart |
| `apps/web/public/img/product/insight.webp` / `insight-en.webp` | tab "Insight" | A price-change insight (not forecast-related) with its evidence sheet open |
| `apps/web/public/img/product/skenario.webp` / `skenario-en.webp` | tab "Skenario" | Scenario page with the editor open: inputs only, example name and description typed for the capture and never saved |
| `apps/web/public/img/product/laporan.webp` / `laporan-en.webp` | tab "Laporan" | Reports page: report kinds, formats, contents preview and the report history |

## Fonts (self-hosted)

Added 2026-10-25 with the design system (`docs/DESIGN_SYSTEM.md`, typography). Extracted unmodified
from the Fontsource npm packages (no npm dependency is added; the files are copied into the app).
Served from `apps/web/public/fonts/` at `/fonts/…` (CSP `font-src 'self'`); the file names carry the
package version, so a new version is a new URL. Declared in `apps/web/src/styles/base.css`
(`font-display: swap`, latin `unicode-range`); only the sans file is preloaded (`apps/web/index.html`).
No style uses the serif 500 face yet, so browsers never download that file; it ships for longer serif
text at body sizes (see `docs/DESIGN_SYSTEM.md`, typography).

| File | Family / weights | Source package | Package file | Licence | Licence file |
|---|---|---|---|---|---|
| `apps/web/public/fonts/ibm-plex-sans-var-latin-5.3.0.woff2` (45.7 kB) | IBM Plex Sans, variable `wght` 100-700, latin | `@fontsource-variable/ibm-plex-sans` 5.3.0 | `files/ibm-plex-sans-latin-wght-normal.woff2` | SIL Open Font License 1.1 (© 2019 IBM Corp.) | `apps/web/public/fonts/LICENSE-ibm-plex-sans.txt` (package `LICENSE`) |
| `apps/web/public/fonts/ibm-plex-serif-latin-500-5.3.0.woff2` (20.3 kB) | IBM Plex Serif 500, latin | `@fontsource/ibm-plex-serif` 5.3.0 | `files/ibm-plex-serif-latin-500-normal.woff2` | SIL Open Font License 1.1 (© 2020 IBM Corp.) | `apps/web/public/fonts/LICENSE-ibm-plex-serif.txt` (package `LICENSE`) |
| `apps/web/public/fonts/ibm-plex-serif-latin-600-5.3.0.woff2` (20.5 kB) | IBM Plex Serif 600, latin | `@fontsource/ibm-plex-serif` 5.3.0 | `files/ibm-plex-serif-latin-600-normal.woff2` | SIL Open Font License 1.1 (© 2020 IBM Corp.) | `apps/web/public/fonts/LICENSE-ibm-plex-serif.txt` (package `LICENSE`) |

SHA-256: sans `e2291e842cf5af167122a22881a740c7f2dda7716f1e8cd76680264f4a859470`, serif 500
`677082fc2e9ee60701b874fe5983c98ea9fb3b588cc1b02058ff9bf29ea783e4`, serif 600
`e279e4f8baa0d4634d98868092f2198ae0dcd9e142c935822c385b9843352a9b`. The OFL allows bundling and
redistribution with software; the fonts are not sold on their own and the licence text ships next to
them. The files are used unmodified (subsetting or other changes would make them a Modified Version
under the OFL).

## Third-party photographs that need a credit (kept, not served)

**Status 2026-10-25: not currently used.** Since the wave B public site the landing hero shows the
market panel and the product tour shows screenshots of the application; no page renders these
photographs, so the landing no longer carries the "Kredit foto" line.

**Moved 2026-10-27 (not deleted):** the six files were moved byte-identical (SHA-256 checked) from
`apps/web/public/img/` to `docs/assets/photos/`, so the public site no longer serves unused CC BY and
CC BY-SA images at public URLs without their attribution. Under the policy at the top of this file
they stay unused: they need a visible credit, and they show Pertamina vessels. The licence guard
refuses any CC BY or CC BY-SA file under `apps/web/public/`.

All three were retrieved on 2026-10-16 from Wikimedia Commons; licence, author and original
file checksum were read from the Commons `imageinfo` API (`docs/assets/photo-sources.json`). They
are kept in `docs/assets/photos/` as resized JPEG (q82) + WebP (q80) renditions; no other
modification. Attribution was shown on the landing page ("Kredit foto") while they were used and is kept here. The two
CC BY-SA images are used unmodified apart from resizing; if they are ever adapted, the adaptation
must be shared under the same licence.

| File (placement) | Subject | Source page | Creator | Licence | Attribution required | Retrieved | Modifications |
|---|---|---|---|---|---|---|---|
| `photos/hero-refinery.jpg` / `.webp` (was the landing hero, 1920×1280) | Pertamina oil tanker crossing Sibolga Bay, North Sumatra (2025-02-08) | [File:Kapal Tanker Pertamina di Teluk Sibolga.jpg](https://commons.wikimedia.org/wiki/File:Kapal_Tanker_Pertamina_di_Teluk_Sibolga.jpg) | Nafisathallah | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0) | Yes | 2026-10-16 | resized from 4073×2715, re-encoded |
| `photos/editorial-tanker.jpg` / `.webp` (was a landing editorial image, 1400×937) | Pertamina tanker *Plaju* leaving Benoa, Bali (2017-01-16) | [File:Plaju leaving Benoa (1).jpg](https://commons.wikimedia.org/wiki/File:Plaju_leaving_Benoa_(1).jpg) | Sabung.hamster (Commons user) | [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0) | Yes (share-alike) | 2026-10-16 | resized from 3842×2572, re-encoded |
| `photos/editorial-port.jpg` / `.webp` (was a landing editorial image, 1400×922) | Products tanker KURAU / PERTAMINA 59 with storage tanks, Benoa Harbour, Bali (2012-08-19) | [File:KURAU-PERTAMINA 59 (IMO- 9004932), Benoa Harbour …](https://commons.wikimedia.org/wiki/File:KURAU-PERTAMINA_59_(IMO-_9004932),_Benoa_Harbour,_Denpasar,_Bali,_August_19_2012._(7892224776).jpg) | Andrew Thomas (Shrewsbury, UK) | [CC BY-SA 2.0](https://creativecommons.org/licenses/by-sa/2.0) | Yes (share-alike) | 2026-10-16 | resized from the 1920 px Commons rendition, re-encoded |

Rejected during sourcing: a 500px import titled "Shining Complex Of Memory" (Dumai) – on
inspection it shows palm trees at dawn, not the refinery; Tropenmuseum/KITLV colonial-era
archive photographs (public domain but not representative of today's market); Pertamina press
photos (copyright, no reuse licence).

### Attribution notice (as rendered in the product while the photographs were used)

Foto: Nafisathallah (CC BY 4.0); Sabung.hamster (CC BY-SA 4.0); Andrew Thomas (CC BY-SA 2.0) –
via Wikimedia Commons.

## Licence register

Checked by `scripts/check-asset-licences.mjs` (`npm run asset-licences`, also a CI step; `--self-test`
checks the checker). Every image file under `apps/web/public/` has exactly one row; every row names an existing
file; the licence is one of `original`, `CC0`, `public domain`, `Unsplash License`, `Pexels License`;
every row gives the SHA-256 of the file as shipped, originals included, so replacing or relabelling an
image needs a deliberate edit of its row (an SVG is hashed with CRLF line endings read as LF, so a
Windows checkout with `core.autocrlf` and the CI checkout agree; the SVGs are stored with LF, and every
other format is hashed byte for byte); a third-party file links its source page (the checksum of
the downloaded original is in its section above), and an `original` file cites no external source.
Add, change or remove the row in the same change as the file.

| File | Licence | Source | SHA-256 (shipped file) |
|---|---|---|---|
| `apps/web/public/favicon.svg` | original | this project (copy of the logo symbol) | 4cbebb74294864e64ea71c08708db4f2b313c4cace660f81b5ad53230133b552 |
| `apps/web/public/brand/app-icon-512.svg` | original | this project | be584c061a0ca50e1d3a16d1a2dbde75bd02ff5962e57a10a57a6bd5573f1b2b |
| `apps/web/public/brand/hero-composite.svg` | original | this project (not used) | f7e8488d52f3ed24bb5389e60fd004490094a0f5af2a7090a9830c31d2f6c25d |
| `apps/web/public/brand/logo-symbol.svg` | original | this project | 4cbebb74294864e64ea71c08708db4f2b313c4cace660f81b5ad53230133b552 |
| `apps/web/public/brand/logo-wordmark-dark.svg` | original | this project | a3dcf8daf095385f4887013b5b9aca77241ff38de9fe3820b54acd94e8dae7e4 |
| `apps/web/public/brand/logo-wordmark.svg` | original | this project | 9dea8dd20851020c3c700e5d533d49605d935ac1dc05dde689c4a6b4e722a884 |
| `apps/web/public/brand/og-image.svg` | original | this project | 261c4bd0b52f2264a60249c693b404197e5f426044fe3433f6c70853f4dabf41 |
| `apps/web/public/brand/og-image.png` | original | this project (rendering of og-image.svg) | 29ad759541d070273270293078b549942e0d2e597794c45f2431ce6e617ebba9 |
| `apps/web/public/img/illus/meth-evidence.svg` | original | this project | c794bc5b3c8b9a15426745fbb177ddb377335d2b473f45a1b3004d6627d37f68 |
| `apps/web/public/img/illus/not-found.svg` | original | this project | 49e3490277ddef112024ac73cdc1fe066cd51ec98c8a684a5a0d07ca5bc0ff64 |
| `apps/web/public/img/illus/steps.svg` | original | this project | 92a9b36ea5c2477fe2e918543d6a83e6108f159213ca5f857c0b89c41a3f48c0 |
| `apps/web/public/img/product/insight.webp` | original | this project (screenshot of the application) | f6895b702da621b5455880203a631277c7b575331eef9aa52395b6845473de66 |
| `apps/web/public/img/product/insight-en.webp` | original | this project (screenshot of the application) | a6a2af2aff9a9db6010cfb3ab683bc6ab0375b0bf59fd7222e7286b72bc10931 |
| `apps/web/public/img/product/laporan.webp` | original | this project (screenshot of the application) | fd05990008d8abdcc32d813713d87c76e95ffd68252989c346d20a8230541afb |
| `apps/web/public/img/product/laporan-en.webp` | original | this project (screenshot of the application) | 62e5dab7f79329c645b5700c4f7ffe42d023bd9f96ec9cff83a226baa69236d6 |
| `apps/web/public/img/product/riwayat-harga.webp` | original | this project (screenshot of the application) | 8d4cfeb830cb21f0acd0f864d9b7c6bbee4907a4f535775c0d39f607986bbdd6 |
| `apps/web/public/img/product/riwayat-harga-en.webp` | original | this project (screenshot of the application) | 0d6865bcb62317b992961c3f377d4ae7822b02ada229b08ba1b7395471801b12 |
| `apps/web/public/img/product/skenario.webp` | original | this project (screenshot of the application) | d60e62ecf1077ae5505fd2ae45e683bf95f1c910bb778907182a2b7ac11f86a1 |
| `apps/web/public/img/product/skenario-en.webp` | original | this project (screenshot of the application) | e1e54bdc11618ef4d6e3382c5fb88ca7c486a73c4b0c892a749458fdd1370965 |
| `apps/web/public/img/photo/ships-on-horizon-1600.avif` | CC0 | [Commons: 3tankers@ThessalonikiPort.jpg](https://commons.wikimedia.org/wiki/File:3tankers@ThessalonikiPort.jpg) | 2e0c17171bf0df6cfeebb3f7945ef9984b8a0ce5621c48b97d88eb616163a4c4 |
| `apps/web/public/img/photo/ships-on-horizon-1600.webp` | CC0 | [Commons: 3tankers@ThessalonikiPort.jpg](https://commons.wikimedia.org/wiki/File:3tankers@ThessalonikiPort.jpg) | 8769456720715a866055d7c68812b0023d779391fe453de6adae4f7cd6efc9df |
| `apps/web/public/img/photo/offshore-platform-dusk-750.avif` | CC0 | [Commons: Holstein at Dusk.jpg](https://commons.wikimedia.org/wiki/File:Holstein_at_Dusk.jpg) | a6dd9d4b054a4dd9978676b88df021242a225b7657727913546703efd127f7a9 |
| `apps/web/public/img/photo/offshore-platform-dusk-750.webp` | CC0 | [Commons: Holstein at Dusk.jpg](https://commons.wikimedia.org/wiki/File:Holstein_at_Dusk.jpg) | af5725d93d29ee3fe48521da8f97d0b0b9c9b02349dfa6102332b811115dfc14 |
| `apps/web/public/img/photo/offshore-platform-dusk-1200.avif` | CC0 | [Commons: Holstein at Dusk.jpg](https://commons.wikimedia.org/wiki/File:Holstein_at_Dusk.jpg) | 90214430e0ac38dc8e1fa7463bd353f7251c4662200bdc65f637a07b59488cb4 |
| `apps/web/public/img/photo/offshore-platform-dusk-1200.webp` | CC0 | [Commons: Holstein at Dusk.jpg](https://commons.wikimedia.org/wiki/File:Holstein_at_Dusk.jpg) | 4ebadba4a4801e200d113a9753dfb4998d9954d26858104ad44627e0db63dcc9 |
