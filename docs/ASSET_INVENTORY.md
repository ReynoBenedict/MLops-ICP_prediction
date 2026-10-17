# Asset inventory

Every visual asset shipped with the product, where it is used, and its licence. Third-party
photography is listed in its own section (appended from `docs/assets/photo-sources.json`).

## Original SVG assets

All files below are original work created for this project (2026-10-16), no third-party source;
they may be used under the repository licence. Palette: navy `#123A5C`, teal `#008C8C`,
amber `#D99A2B`.

| File | Placement | Notes |
|---|---|---|
| `apps/web/public/brand/logo-symbol.svg` | favicon source, app shell brand mark, PDF report header (drawn with the same geometry in `backend/icp_api/services/reports.py`) | Three rising rounded bars + amber upward stroke on a navy tile |
| `apps/web/public/favicon.svg` | browser tab icon | Copy of the logo symbol |
| `apps/web/public/brand/logo-wordmark.svg` | documentation, light backgrounds | Symbol + "ICP Intelligence" + tagline |
| `apps/web/public/brand/logo-wordmark-dark.svg` | dark backgrounds (sidebar, footer, auth side panel) | Teal tile variant |
| `apps/web/public/brand/app-icon-512.svg` | `apple-touch-icon`, future PWA/app icon | 512×512, gradient tile |
| `apps/web/public/brand/hero-composite.svg` | landing hero fallback when `public/img/hero-refinery.*` is missing; labelled "ilustrasi · bukan peta atau lokasi faktual" | Stylised refinery/tanker silhouettes, price curve, source → ICP flow |
| `apps/web/public/brand/og-image.svg` | Open Graph / social preview (`<meta property="og:image">`; rasterise to `og-image.png` for platforms that need PNG) | 1200×630 |
| `apps/web/src/components/brand.tsx` (`LogoSymbol`, `Wordmark`) | inline logo in the sidebar, topbar, auth and legal pages | React rendering of the same geometry |
| `apps/web/src/components/illustrations.tsx` — `IllNoData` | empty insight list, empty charts | dashed curve on an empty chart |
| `apps/web/src/components/illustrations.tsx` — `IllNoForecast` | "no forecast published yet" (overview, forecast page) | calendar with question mark |
| `apps/web/src/components/illustrations.tsx` — `IllScenarios` | empty scenario list, landing feature card | diverging paths with square/triangle markers |
| `apps/web/src/components/illustrations.tsx` — `IllAlerts` | empty notification inbox | bell with quiet dot |
| `apps/web/src/components/illustrations.tsx` — `IllUpgrade` | sign-in / plan gate notices (`GateNotice`) | lock with upward arrow |
| `apps/web/src/components/illustrations.tsx` — `IllReports` | empty report list | document with check |
| `apps/web/src/components/illustrations.tsx` — `IllPipeline` | landing feature card, auth side panel | sources → verification → forecast |
| `apps/web/src/components/illustrations.tsx` — `IllTimeline` | landing feature card (elapsed vs future months) | observed / origin / assumed markers |
| `apps/web/src/components/illustrations.tsx` — `IllIcpProcess` (added 2026-10-17) | Proyeksi ICP, "Keandalan dan keterbatasan" panel: process explainer (world prices → ministerial decree → official value on the platform) | three-stage explainer; paired with the numbered steps `forecast.process1..3` |
| `apps/web/src/components/illustrations.tsx` — `MarkDecree`, `MarkBarrel`, `MarkFx` (added 2026-10-17) | Ringkasan Pasar, "Pembaruan data terakhir" source list: source-type marks for decree/announcement (ICP), market price (Brent/Dubai/WTI) and exchange rate (JISDOR) | 16 px inline marks, `aria-hidden`, always next to the dataset label |
| `apps/web/src/components/charts.tsx` — `MarkerSwatch` / `dotFor` | scenario comparison chart and legend | circle / square / triangle / diamond markers so scenario identity never relies on colour alone |

No maps, refinery locations or pipeline routes are depicted; geography is never implied. The
product footer and landing page credit these as "Ilustrasi dan ikon: karya orisinal ICP Intelligence".

## Third-party photographs (Wikimedia Commons)

All three were retrieved on 2026-10-16 from Wikimedia Commons; licence, author and original
file checksum were read from the Commons `imageinfo` API (`docs/assets/photo-sources.json`). They
are served from `apps/web/public/img/` as resized JPEG (q82) + WebP (q80) renditions; no other
modification. Attribution is shown on the landing page ("Kredit foto") and repeated here. The two
CC BY-SA images are used unmodified apart from resizing; if they are ever adapted, the adaptation
must be shared under the same licence.

| File (placement) | Subject | Source page | Creator | Licence | Attribution required | Retrieved | Modifications |
|---|---|---|---|---|---|---|---|
| `img/hero-refinery.jpg` / `.webp` (landing hero, 1920×1280) | Pertamina oil tanker crossing Sibolga Bay, North Sumatra (2025-02-08) | [File:Kapal Tanker Pertamina di Teluk Sibolga.jpg](https://commons.wikimedia.org/wiki/File:Kapal_Tanker_Pertamina_di_Teluk_Sibolga.jpg) | Nafisathallah | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0) | Yes | 2026-10-16 | resized from 4073×2715, re-encoded |
| `img/editorial-tanker.jpg` / `.webp` (landing editorial, 1400×937) | Pertamina tanker *Plaju* leaving Benoa, Bali (2017-01-16) | [File:Plaju leaving Benoa (1).jpg](https://commons.wikimedia.org/wiki/File:Plaju_leaving_Benoa_(1).jpg) | Sabung.hamster (Commons user) | [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0) | Yes (share-alike) | 2026-10-16 | resized from 3842×2572, re-encoded |
| `img/editorial-port.jpg` / `.webp` (landing editorial, 1400×922) | Products tanker KURAU / PERTAMINA 59 with storage tanks, Benoa Harbour, Bali (2012-08-19) | [File:KURAU-PERTAMINA 59 (IMO- 9004932), Benoa Harbour …](https://commons.wikimedia.org/wiki/File:KURAU-PERTAMINA_59_(IMO-_9004932),_Benoa_Harbour,_Denpasar,_Bali,_August_19_2012._(7892224776).jpg) | Andrew Thomas (Shrewsbury, UK) | [CC BY-SA 2.0](https://creativecommons.org/licenses/by-sa/2.0) | Yes (share-alike) | 2026-10-16 | resized from the 1920 px Commons rendition, re-encoded |

Rejected during sourcing: a 500px import titled "Shining Complex Of Memory" (Dumai) – on
inspection it shows palm trees at dawn, not the refinery; Tropenmuseum/KITLV colonial-era
archive photographs (public domain but not representative of today's market); Pertamina press
photos (copyright, no reuse licence).

### Attribution notice (as rendered in the product)

Foto: Nafisathallah (CC BY 4.0); Sabung.hamster (CC BY-SA 4.0); Andrew Thomas (CC BY-SA 2.0) –
via Wikimedia Commons.
