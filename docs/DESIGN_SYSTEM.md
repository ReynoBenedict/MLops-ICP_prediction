# Design system

Authoritative source for the visual and interaction language of the ICP Intelligence web app
(`apps/web`). Tokens live in `apps/web/src/styles/tokens.css`; this document explains them and the rules
that go with them. If the CSS and this document disagree, fix one of them in the same change.
How the styles are organised in code is described under [Implementation](#implementation).

## Concept: the official number, in context

ICP Intelligence is anchored to one monthly figure: the official Indonesian Crude Price set by a
ministerial decree. The interface treats that number the way a reference document does (unit, period,
source, verification status, publication date are never more than a glance away) and surrounds it with
the clarity of an analytics instrument (tabular numerals, recessive grids, precise states).

Principles, in priority order:

1. **Every number carries its provenance.** A value is shown with its unit and period; where the reader
   could mistake it for something else it also shows its kind (official, benchmark, derived, estimate,
   forecast, scenario) and its source or verification status. No bare numbers.
2. **Two voices.** Serif type is the *narrative* voice (public headlines, insight headlines, methodology
   and legal headings): sentences written about the data. Sans type is the *interface and data* voice.
   Data values (KPIs, tables, chart labels, axis ticks) are never set in the serif. A sentence in the
   serif may contain figures (an insight headline names its number); those figures use lining
   numerals (`font-variant-numeric: lining-nums`) so they align with the sans values around them.
3. **Flat by default.** Panels and cards sit on the canvas with a 1 px line, no shadow. Elevation is
   reserved for things that float above the page: menus, popovers, dialogs, sheets, toasts and the
   public product frame.
4. **States are designed, not defaulted.** Loading, empty, stale, partial, error and success states get
   the same care as the happy path. A skeleton never stands in for an error: every request ends in data,
   an explained empty state or an explained error with a way forward.
5. **Motion answers actions.** UI motion (120-240 ms) only confirms what the user did or shows what
   changed. At most one orchestrated, non-triggered moment per page (the landing chart draw-in). Everything
   respects `prefers-reduced-motion`.
6. **Sentence case everywhere.** No uppercase, letter-spaced labels or eyebrows. Hierarchy comes from size,
   weight and colour, not from capitals.

## Colour

### Surfaces and lines

| Token | Value | Use |
|---|---|---|
| `--canvas` | `#F3F5F8` | App background (cool paper) |
| `--canvas-alt` | `#F7F9FB` | Alternating public sections |
| `--surface` | `#FFFFFF` | Panels, cards, inputs, chart surface |
| `--surface-2` | `#F7F9FB` | Inset areas: table headers, footers of panels, read-only fields |
| `--surface-hover` | `#EEF2F6` | Hover on rows, list items, ghost buttons |
| `--surface-selected` | `#E7F2F2` | Selected rows, active nav item |
| `--line` | `#E1E6EC` | Default borders and dividers |
| `--line-strong` | `#C8D0DA` | Input borders, secondary-button borders |
| `--line-subtle` | `#EDF0F4` | Table row separators, chart grid |

### Ink

| Token | Value | Contrast on white | Use |
|---|---|---|---|
| `--ink` | `#0E2235` | 15.9 : 1 | Headings, numerals, primary text |
| `--ink-2` | `#33475B` | 9.6 : 1 | Body text |
| `--muted` | `#5B6B7D` | 5.6 : 1 | Secondary text, labels, metadata |
| `--faint` | `#8795A5` | 3.1 : 1 | Decorative strokes and icons only, never text |

On dark brand surfaces (`--navy-deep`): `--on-dark` `#FFFFFF`, `--on-dark-2` `#C9D5E2`,
`--on-dark-muted` `#9FB2C7` (all at least 4.5 : 1 on `--navy-deep`). Overlays on those surfaces:
`--on-dark-line` (white 16 %, borders), `--on-dark-fill` (white 5 %, cards), `--on-dark-hover`
(white 8 %), `--on-dark-selected` (`#5FD0D6` at 16 %, active nav item); focus on dark surfaces is
`--focus-on-dark` `#5FD0D6`.

Overlays: `--scrim` `rgba(14,34,53,.45)` behind dialogs, sheets and the navigation drawer;
`--photo-scrim` `rgba(14,34,53,.72)` under photo captions.

### Brand and accent

| Token | Value | Use |
|---|---|---|
| `--navy` | `#123A5C` | Brand colour: logo, public headings, primary nav emphasis |
| `--navy-deep` | `#0D2C47` | Dark brand surfaces: public CTA band, footer, operator console sidebar |
| `--teal` | `#0F6B73` | **The interactive colour**: primary buttons, links, focus, selected states |
| `--teal-strong` | `#0B5359` | Hover/pressed for teal |
| `--teal-soft` | `#E6F1F1` | Tinted backgrounds for teal states |
| `--teal-line` | `#B9DBD9` | Borders of teal-tinted elements (brand badge, completed onboarding step) |
| `--teal-ring` | `rgba(15,107,115,.28)` | Focus ring of text inputs |
| `--amber` | `#D99A2B` | Brand mark and the public "official decree" stamp only |
| `--amber-ink` / `--amber-soft` | `#7A4E06` / `#FBF2DE` | Text and fill of that stamp |

Teal is never a data series and amber is never a warning; warnings use the status tokens.

### Status (always icon + text, never colour alone)

Tokens: `--<state>` (ink), `--<state>-soft` (fill), `--<state>-line` (line), for example `--warn`,
`--warn-soft`, `--warn-line`.

| State | Ink | Fill | Line |
|---|---|---|---|
| good | `#067647` | `#ECF8F1` | `#ABE2C3` |
| warn | `#A94B05` | `#FFF7EA` | `#F6D39A` |
| bad | `#B42318` | `#FEF1F0` | `#F5C2BD` |
| info | `#1A5FB4` | `#EEF5FD` | `#BCD6F5` |
| neutral | `#33475B` | `#F0F2F5` | `#DCE2E8` |

A price change is not a status: direction chips (`Delta`) use neutral ink with an arrow and a word
(naik / turun / stabil), because a rising price is good news for a seller and bad news for a buyer.
Evidence chips (`lib/verification.tsx`) use a teal icon for confirmed months, the `--warn` ink icon for
weaker evidence and `--bad` for conflicts (amber is not used for them).

### Value kinds (categories, not statuses)

`KindChip` and the "asumsi" tag use their own neutral category colours so that status colours stay
reserved for problems: `--cat-estimate` `#1F4F8F` on `--cat-estimate-soft` `#EEF3FB` (line
`#C7D7EE`), `--cat-forecast` `#4A3FA6` on `--cat-forecast-soft` `#F2F0FB` (line `#D6D0F2`),
`--cat-assumed` `#5B4A1A` on `--cat-assumed-soft` `#F4F2EA` (dashed line `#C9B98A`). Observed and
derived values use the neutral badge.

### Data series

Validated with the data-visualization palette validator on the white chart surface (lightness band,
chroma floor, adjacent-pair CVD separation, normal-vision floor and 3 : 1 contrast all pass).

| Token | Value | Series |
|---|---|---|
| `--series-icp` | `#1D6FB8` | Official ICP (always the first series) |
| `--series-brent` | `#E8692F` | Brent |
| `--series-dubai` | `#6250D6` | Dubai |
| `--series-wti` | `#8A5A0B` | WTI, rupiah value |
| `--series-spread` | `#0F6B73` | ICP minus benchmark spread (single-series charts only) |
| `--series-s1..s4` | `#E8692F`, `#6250D6`, `#8A5A0B`, `#0F8F6B` | Scenario comparison, with distinct marker shapes |

Rules: colour follows the entity, never its rank; at most one y-axis per chart; forecasts use the ICP hue
dashed; assumed inputs are dotted, thinner and labelled "asumsi"; uncertainty bands use `--band`
(`rgba(29,111,184,.14)`); the forecast period is tinted `--forecast-zone` (`rgba(29,111,184,.09)`);
missing months are breaks, never interpolated, tinted `--gap-zone` (`rgba(181,71,8,.07)`; a single
missing month is drawn as a 7 px band in `--gap-mark` `rgba(181,71,8,.16)`; legend and coverage
borders use `--gap-line` `#E19A3C`). The data-coverage grid marks observed months with
`--coverage-present` `#8FB8E0` (line `#7AA9D6`). Grid `--line-subtle`, axis line
`--line-strong`, tick text `--muted` at `--fs-meta` (12.5 px at the default text size). Chart text
follows the reader's text size, so geometry that depends on it (label widths and spacing, axis room,
the x-label interval) is computed from the px size of `--fs-meta` (`lib/text-size.ts`), not from a
fixed 12.5 px. A month whose official ICP is withheld because its documents disagree is drawn like a
missing month but named "ditahan" in the legend, tooltip, table and summary. Every chart has a one-sentence summary and a table
view with the same values and units. Charts that are on the public site are plain SVG (no charting
library in the public bundle). Recharts needs literal colours for gradient stops and marker fills, so
`SERIES_COLORS` in `components/charts.tsx` repeats the `--series-*` values; change both together.
Everything else in charts (grid, axes, ticks, zones, marker outlines) uses `var(--token)`.

## Typography

| Token | Value |
|---|---|
| `--font-sans` | `"IBM Plex Sans Variable", "IBM Plex Sans", "Segoe UI", system-ui, sans-serif` |
| `--font-serif` | `"IBM Plex Serif", Georgia, "Times New Roman", serif` |
| `--font-mono` | `ui-monospace, "Cascadia Mono", Consolas, monospace` (identifiers and code only) |

Fonts are self-hosted (SIL Open Font License 1.1; the CSP allows fonts from `'self'` only). IBM Plex
Sans is one variable file (weights 100-700). IBM Plex Serif is declared in the 500 and 600 weights;
every serif rule today uses 600, so the 600 file is downloaded only on pages that show serif text and
the 500 file is never downloaded (a declared face costs nothing until a rule uses it). The 500 face
is kept for longer serif text at body sizes, where 600 is too heavy. Latin subset, `font-display: swap`. The sans file is preloaded.
The files live in `apps/web/public/fonts/` with the package version in the file name
(`ibm-plex-sans-var-latin-5.3.0.woff2`, ...) and are served unhashed at `/fonts/…` with an immutable
cache header. *Deviation from the first draft* (which bundled them through `src/assets/fonts/` with
Vite hashes): the preload link in the static `index.html` needs a stable URL that Vite does not
rewrite, and the version in the name gives the same cache safety as a hash. Sources, versions and
checksums are listed in `docs/ASSET_INVENTORY.md`.

Numerals: `font-variant-numeric: tabular-nums` on every number that can be compared (tables, KPIs,
chart ticks, tooltips). Right-align numbers in tables.

Scale (UI 15 px base; major-third steps for headings). The sizes are px values at the default 16 px
browser text size; the tokens are written in `rem` (`--fs-body: 0.9375rem`), so text follows the
reader's browser text-size setting (WCAG 1.4.4, baseline finding B-P3-5). Controls that hold text
(buttons, inputs, segmented and filter buttons, the data-status button) give their heights in `rem`
for the same reason; spacing, radii and layout widths stay in px. Text inside drawings and boxes
follows too: the wordmark, the numbered step circles and the avatar are sized in rem, and chart
geometry follows `--fs-meta` (see Data series). At 200 % text the app top bar grows
(`min-height: var(--topbar-h)`), sticky offsets follow its measured height (`--sticky-top`), headings
break a word that is wider than the column, buttons, badges and evidence chips wrap a label that is
wider than their container (see Button), tab rows wrap, the desktop sidebar scrolls as a whole and
the phone top bar shortens the product name with an ellipsis.

What was checked (2026-10-27, Chrome text size 200 %, i.e. a 32 px default font set with CDP
`Page.setFontSizes`, on a production build): 6 public, 9 member and 6 operator routes at 1440, 768
and 390 px, with a probe that measures every visible text node (spans, button and chip labels, SVG
chart text) against the viewport and every clipping ancestor. Result: no horizontal page overflow, no
control whose label spills out of it, no text below 20 px, and no text cut except deliberate
truncation (ellipsis on long URLs, identifiers and the phone product name; the three-line clamp of
the overview's insight teasers, whose full text is on the insight page). axe-core (WCAG 2.2 AA) at
100 % on 6 routes and a Tab walk at 200 % on 3 routes found no violation and no focus target fully
hidden. Pages not in that route list (dialogs, sheets, the scenario editor's open states) were not
measured at 200 %.

| Token | Size / line height | Use |
|---|---|---|
| `--fs-meta` | 12.5 / 1.45 | Metadata, chart ticks, captions (the minimum for running text) |
| `--fs-sm` | 13.5 / 1.5 | Secondary text, table text on dense screens, form hints |
| `--fs-body` | 15 / 1.55 | Interface body text |
| `--fs-lead` | 17 / 1.55 | Lead paragraphs |
| `--fs-h4` | 16 / 1.35, 600 | Panel titles |
| `--fs-h3` | 19 / 1.3, 600 | Section titles inside pages |
| `--fs-h2` | 24 / 1.25, 600 | App page titles |
| `--fs-kpi` | 32 / 1.1, 600, tabular | KPI values |
| `--fs-hero-num` | 48 / 1.05, 600, tabular | The single headline number on a page |
| `--fs-display` | `clamp(2.125rem, 3.6vw + 0.5rem, 3.375rem)` (34-54 px) / 1.08, serif 600 | Landing hero h1 |
| `--fs-display-2` | `clamp(1.625rem, 1.6vw + 0.875rem, 2.25rem)` (26-36 px) / 1.15, serif 600 | Public section h2; h1 of document pages (methodology, legal) |

Document pages (methodology, legal) use `--fs-display-2` for their h1 and serif `--fs-h3` for section
h2, so the title fits the 72ch reading column; `--fs-display` is reserved for the landing hero. The
sign-in and registration h1 is serif `--fs-h2` (a form heading, not a hero). Insight headlines are
serif `--fs-h2` (lead story) and `--fs-h4` (compact list). Serif headings are `--navy`; numbering
inside a serif heading (legal section numbers) is set in the sans.

Weights: 400 body, 500 labels and controls, 600 headings and emphasised numbers, 700 only for the
hero number. Body text is `--ink-2`; headings, numerals and primary cell text are `--ink`. Letter-spacing: `-0.015em` for sizes of 24 px and up, `0` below. Measure: prose 60-72
characters (`max-width: 68ch`); serif prose may run to 72ch with line height 1.65.

## Space, shape, elevation

Spacing scale (4 px base): `--s-1` 4, `--s-2` 8, `--s-3` 12, `--s-4` 16, `--s-5` 20, `--s-6` 24,
`--s-8` 32, `--s-10` 40, `--s-12` 48, `--s-16` 64, `--s-20` 80, `--s-24` 96 px.

Radii by role, not one radius for everything:

| Token | Value | Use |
|---|---|---|
| `--r-xs` | 4 px | Tags, small chips, table cells with fills |
| `--r-sm` | 6 px | Buttons, inputs, segmented controls |
| `--r-md` | 10 px | Panels, cards, tables frames, callouts |
| `--r-lg` | 14 px | Dialogs, sheets, the public product frame |
| `--r-pill` | 999 px | Status badges, language toggle |

Elevation: `--e-1` `0 1px 2px rgba(14,34,53,.06)` (sticky bars), `--e-2`
`0 8px 24px rgba(14,34,53,.10), 0 2px 6px rgba(14,34,53,.06)` (menus, popovers, the public product
frame), `--e-3` `0 24px 64px rgba(14,34,53,.22)` (dialogs, sheets, toasts). Panels and cards use no
shadow.

## Layout

| Token | Value |
|---|---|
| `--sidebar-w` | 248 px (customer and operator shells) |
| `--topbar-h` | 56 px (app top bar) |
| `--content-max` | 1440 px (app pages) |
| `--public-max` | 1200 px (public site content) |
| `--page-pad` | 32 px; 24 px at 1280 px and below; 16 px at 560 px and below |
| `--grid-gap` | 20 px; 16 px at 900 px and below (gap of page grids and stacks) |

Breakpoints (used literally in media queries; CSS variables cannot be used there): 560 px (phone),
900 px (tablet: the app sidebar becomes a drawer, public navigation collapses), 1100 px (two-column
layouts stack), 1440 px (wide). The layout is tested at 320, 390, 768, 1024, 1440 and 1920 px with no
horizontal page scroll. Tables that cannot fit at 560 px and below become record cards (`.table.rc`).

Alignment: left-aligned text everywhere (no centred heroes or centred paragraphs); numbers
right-aligned in tables; one primary action per view, placed top right of the page header or at the
end of a form.

## Motion

| Token | Value | Use |
|---|---|---|
| `--dur-1` | 120 ms | Hover, press, colour changes |
| `--dur-2` | 180 ms | Toggles, tabs, toasts, popovers |
| `--dur-3` | 240 ms | Sheets, drawers, result reveals |
| `--ease-out` | `cubic-bezier(.2,.7,.2,1)` | Entering |
| `--ease-in-out` | `cubic-bezier(.4,0,.2,1)` | Moving |

Animate opacity and transform only; never delay input; never animate a number counting up (values are
final when shown). Under `prefers-reduced-motion: reduce` all animations and transitions are disabled.

## Interaction states

Every interactive element defines default, hover, focus-visible, active (pressed), disabled and, where
it triggers a request, loading. Focus: `outline: 2px solid var(--teal); outline-offset: 2px` (on dark
surfaces `#5FD0D6`). Loading buttons keep focus (`aria-disabled`, `aria-busy`) instead of becoming
`disabled`. Touch targets are at least 40 px high on coarse pointers (`@media (pointer: coarse)`),
including number fields, checkboxes and the language toggle.

Focus is never hidden under a sticky header (WCAG 2.2 SC 2.4.11): `html` has
`scroll-padding-top: calc(var(--sticky-top) + 8px)`, where `--sticky-top` is the measured height of
the app top bar or the public header (`useStickyTop` in `components/chrome.tsx`, kept current with a
ResizeObserver). In-page targets add only a small `scroll-margin-top` (`--s-2`), so a deep link lands
16 px below the header; deep links whose content above them is still loading stay aligned until the
reader scrolls or interacts (`lib/anchor.ts` `keepAnchored`).

## Components (implemented in `apps/web/src/components/ui.tsx` unless noted)

- **Button**: primary (teal fill), secondary (surface, `--line-strong` border), ghost, danger,
  danger-solid; sizes sm 32 px, md 38 px, lg 44 px (minimum heights in rem); radius `--r-sm`; no
  shadow. A label stays on one line while the button fits its container and wraps inside the button
  when it is wider (phones at large text sizes); `min-width: fit-content` keeps buttons from
  shrinking beside their neighbours. Badges, evidence chips and the "asumsi" tag wrap the same way,
  except in table cells, where they keep one line (the column takes their width and the table frame
  scrolls) unless the table is compact or shown as record cards.
- **Inputs**: 40 px, `--line-strong` border, teal focus ring (`--teal-ring`), invalid state with
  `--bad` border plus a text error tied with `aria-describedby`.
- **Panel**: surface, `--line` border, `--r-md`; head with title (16 px/600), optional subtitle and
  actions; optional footer on `--surface-2`.
- **Kpi / headline number**: label (13.5 px/500 `--muted`), value (32 px tabular; 48 px for the single
  page headline), unit in `--muted`, meta line with period and status.
- **Badge / status chip**: pill, icon + text; **KindChip** for value kinds (official, derived,
  estimate, forecast, scenario, assumed) uses neutral category fills, not status colours.
- **Callout**: info / good / warn / bad with icon, title and an optional action; errors use
  `ErrorState`, which always says what happened and offers the next step (retry, sign in, change plan);
  its optional `missing` list names the parts of the page that could not be loaded ("Tidak tersedia
  saat ini: …").
- **Tables**: sentence-case headers (12.5 px/600 `--muted` on `--surface-2`), 1 px `--line-subtle` row
  separators, numbers right-aligned and tabular, units in the header, record cards at 560 px and below.
- **Tabs / Segmented**: WAI-ARIA tabs with roving focus; selection lives in the URL when it changes what
  the page shows. A tab row that does not fit (phones, large text) wraps to a second line instead of
  scrolling sideways behind a fade (`.tabs` in `components.css`; every tab row, including the
  operator console's and the landing's product tour).
- **Coverage grid** (price history, operator data page): one cell per month; a month after the latest
  official month is blank, so a future month never looks like a missing one.
- **Skeleton**: `--surface-2` blocks with a hairline `--line-subtle` inset (so they also read on the
  canvas) and a subtle `--surface-hover` shimmer; always inside a labelled `role="status"` region and
  always replaced by data, an empty state or an error.
- **Dialogs and sheets**: portalled to `document.body`, focus trapped, Escape closes, focus returns to
  the trigger; sheets are right drawers on desktop and bottom sheets on phones (`components/modal.tsx`).
- **Toasts**: bottom right, `--e-3`, auto-dismiss for success only; errors stay until dismissed.
- **PageHeader / ViewBar**: the page header takes an optional eyebrow and a `meta` line (for example the
  "as of" month); `ViewBar` with `ViewField`s holds the controls that change the view (range, interval
  level), whose values live in the URL.
- **Sparkline**: a small line without axes for trend context inside cards; gaps are breaks, never
  zeros. The public site has its own SVG variant (`components/public/Sparkline.tsx`, class `.ptrend`).
- **Data status** (`components/data-status.tsx`): the top-bar control and popover that state how
  current each source is (`/data/summary`, one shared request). It is read again on every route change,
  retries after a failure (15, 30, 60, then every 120 s) and never shows "Mutakhir" for a stale copy.
- **Slow and failed loading**: `useSlow` / `SlowNotice` show one notice after 8 s of waiting, which says
  how long the page waits before it explains a failure. When two or more requests of a page fail, the
  page shows one `ErrorState` with one retry and each block shows a short "Tidak tersedia" line, instead
  of one error per block.
- **Table frames**: `useTableFrame(threshold)` reports whether a table's frame is narrow and whether
  the table overflows it. Narrow frames use `.table.compact` (wrapping headers) and `.table .fold` (a
  secondary column folded under the first, as the forecast tables do with the value kind); a header
  sticks to the page only while the table fits (`.page-sticky.fits`). Every `.table-scroll` shows an
  edge shadow from 561 px when more columns exist to the side.
- **Stale results**: a result computed from inputs that have since changed keeps its numbers but carries
  a stale callout naming what changed, plus a short stale tag directly above the chart and the table,
  and offers the re-run action ("Jalankan ulang", "Bandingkan ulang").
- **Public components** (`components/public/`): `MarketPanel` (the landing's market panel and its
  loading, stale and error states, which keep the frame's size), `MarketChart` (plain SVG, no chart
  library; the latest values get their own gutter so no number sits inside the locked forecast window),
  `PlanTable` / `PlanCard`, `ProductTour` (WAI-ARIA tabs over product screenshots), `DocLayout` (document
  pages with a sticky contents column and an optional decorative `art` column from 1281 px).

## Imagery

Images support a section; they never replace or cover data. Fewer, better images: each placement below
has a reason, and anything not listed stays without an image.

| Where | What | Why |
|---|---|---|
| Landing hero | No image (the market panel is the hero) | The official number is the first thing to read; an image there would compete with it and could become the largest paint of the first screen |
| Landing, "Cara kerja" | Four original illustrations, one per step, on flat tiles under the numbered rule (on phones under each heading) | The section explains a sequence; a picture per step makes it scannable |
| Landing, product tour | Screenshots of the application | Shows the real product (unchanged) |
| Landing, closing call to action | A navy-toned photograph of ships on the horizon behind the band; the horizon runs through the band's bottom padding, below the text and the buttons | One real-world image in the page's one dark band; the mood before the call to action |
| Sign-in and registration panel (from 1101 px) | A navy-toned photograph of an offshore platform at dusk behind the panel | Gives the brand panel depth without moving the live card or the form; on these wide windows it is the page's largest paint, a measured trade-off (performance rule 6) |
| Data & metodologi (from 1281 px) | An original illustration beside the title, in the free column right of the 68ch article | Fills the empty column with the page's subject (document, check, chart with its range); hidden where the column does not exist |
| Not-found page (public and workspace) | An original drawing of a chart that breaks at a missing month, plus a way back home | Turns a dead end into an explained one in the product's own vocabulary |
| Workspace empty states | The inline spot illustrations of `components/illustrations.tsx` | Unchanged placements, redrawn in the token palette |
| Legal pages, plans, honesty, sources | No image | Legal text stays sober; the other sections already show data |

**Illustration style.** Navy `#123A5C` outlines (1.5–2.2 px at 1×) on white and `--surface-2` paper,
lines `--line` / `--line-strong`; teal only for a confirmed check (the evidence-chip meaning); the
data-series colours where a drawing shows data (ICP line `--series-icp`, Brent `--series-brent`,
scenario lines and markers as in the scenario chart, benchmark bars in the Brent / Dubai / WTI
colours); the coverage-grid and gap colours for months; amber only as the decree seal. No text
inside drawings, no logos, emblems or letterheads (a document is plain lines), no maps or places. The
static files are hand-written SVG and the only literal colours outside `tokens.css` besides the
inline illustration and logo components and `SERIES_COLORS`.

**Photographs.** Generic subjects only, never presented as Indonesian, as a Pertamina or ESDM site, or
as any specific company, place or event; never captioned or named on the site. Each is toned to the
navy palette (luminance mapped to three navy stops) and shown under a shade built from `--navy-deep`
with `color-mix()`, so it reads as part of the page rather than as stock imagery. Text over a
photograph must keep at least 4.5 : 1 against the lightest pixel behind it (3 : 1 for large text);
the shade is sized for that: closing band at least 72 %, 86 % evenly below 900 px; sign-in panel at
least 88 % across the 520 px text column. Where the subject sits matters as much as the shade: the
closing photograph's horizon, with its ships, runs through the band's bottom padding at every width,
never behind the buttons or under a line of text. Re-measured on 2026-10-28 (320 to 1920 px, and with
text at 200 %) with the text hidden and the background sampled per text box: closing band at least
6.23 : 1, sign-in panel at least 4.87 : 1, the same minimum as without the photographs.

**Performance rules.**
1. No image in the first screen of the landing: `/` keeps its hero heading as the largest paint, and
   the first load keeps its request count and image bytes.
2. Below the fold: `loading="lazy"` and `decoding="async"`, with the intrinsic `width` / `height` (or
   an `aspect-ratio` box), so nothing shifts while it loads.
3. Several small drawings in one section are one sprite, requested when the section comes within
   200 px of the viewport (`img/illus/steps.svg`, a CSS background of `.flow-art` under `.flow-near`):
   four separate requests delayed the market panel's data on slow connections (HTTP/1.1 has six
   connections per host).
4. Nothing hidden is downloaded: an image for a breakpoint-only area is mounted only while the area is
   shown (the sign-in photograph, from 1101 px) or is lazy inside a `display: none` column (the
   methodology art, below 1281 px).
5. AVIF with a WebP fallback in a `<picture>`, only in the renditions the layout can use (the closing
   photograph is drawn wider than a phone screen, so it ships one 1600 px rendition), each at most
   about 150 kB at its largest width.
6. Off the landing, a decorative photograph may be a page's largest paint only when it is small and
   the page's content does not wait for it. The sign-in photograph is that case on windows from
   1101 px: 16 kB at the size served (750 px AVIF), normal fetch priority (`fetchpriority="low"` only
   made the largest paint later) and no preload (a preload brought the largest paint back to the time
   without the photograph, but the form became ready 40 to 157 ms later, because the photograph then
   competes with the page's scripts). Measured on 2026-10-28 on `/masuk` at 1440 × 900 with a slow
   connection (1.6 Mbps, 150 ms RTT, 4× CPU), medians of 9 interleaved runs per build:

   | `/masuk`, slow connection | Without the photograph | First version (32 kB, low priority) | Now (16 kB, normal priority) |
   |---|---|---|---|
   | Largest paint | 2,172 ms (a text line) | 2,596 ms (the photograph) | 2,376 ms (the photograph) |
   | Form ready | 2,308 ms | 2,343 ms | 2,312 ms |

   On an unthrottled connection the largest paint is the same with and without the photograph
   (436, 460 and 428 ms). Any new image is measured this way, before against after, on the pages it
   appears on.

**Accessibility.** Every image here is decorative: `alt=""` with `aria-hidden="true"` (or a CSS
background on an `aria-hidden` element); the text next to it carries the meaning. Informative images
(the product tour screenshots) keep a description in both languages. `src/assets.test.tsx` checks the
marking, the intrinsic sizes, the lazy loading, the sign-in photograph's mount rules (wide window, no
forced colours) and that every referenced file exists.

**Forced colours.** Windows contrast themes drop gradient backgrounds, so a shade would vanish and
leave its photograph at full strength behind text and icons. Both photographs are hidden there
(`@media (forced-colors: active)` in `public.css`; the sign-in photograph is not even mounted), and the
bands fall back to the theme's own background. The illustrations are not affected: they are `<img>`
elements or `url()` backgrounds, which forced colours keep, and they carry no text.

**Licences.** Only material that needs no credit: original work, CC0, public domain, Unsplash
License, Pexels License. CC BY / CC BY-SA and unknown licences are refused, as are company photographs
and logos (Pertamina, ESDM or any other) and anything that imitates an ESDM document, letterhead or
logo. Every image file under `apps/web/public/` has exactly one row in the licence register of
`docs/ASSET_INVENTORY.md`: licence, source (a third-party row links its source page) and the SHA-256 of
the shipped file, originals included, so replacing or relabelling an image is always a deliberate
register edit. `npm run asset-licences` (in `npm run check` and in CI) fails on a missing or stale row,
a licence that needs a credit, a changed file, an `original` row that cites an external source, an
image file inside `apps/web/src`, an image import, `new URL()` or CSS `url()` that is not a public
path, and a `data:image` URI. The app has no dark mode (`color-scheme: light`), so images have one
treatment.

## Writing in the interface

Plain verbs, sentence case, the reader's vocabulary ("Riwayat harga", not "Data explorer"). A button
names its result ("Buat laporan"), and the confirmation uses the same verb ("Laporan dibuat"). Errors say
what happened and what to do, without apologising. Every data claim is supported by the data shown:
nothing is called real-time, verified or official unless the server says so (`/data/verification`,
`freshness`, value kinds). Forecasts are always described as conditional on their assumptions, never as
future prices. Indonesian is the default language; every string exists in Indonesian and English
(`apps/web/src/i18n/`). Percentages are written without a space in both languages ("+26,1%",
"+26.1%"), matching the server's insight texts. English plurals in the web dictionaries come from
`{n|one|other}` placeholders, never "(s)" (`i18n.test.ts` checks the web strings). The two server texts
the workspace shows with a count choose singular or plural in code, each with a test: the per-point
backtest summary (`backend/icp_api/insights/context.py`) and the "Sources differ" note
(`backend/icp_api/services/verification.py`). English-only technical text (logs, ingestion provenance
warnings, restore checks) may still say "(s)". A name inside a sentence keeps sentence case
("Membuat laporan pasar bulanan…").

Strings are split by area. The eager dictionaries (shell, common, provenance, landing, auth, legal,
methodology) are in the entry bundle; the workspace dictionaries (`i18n/workspace.ts`) and the
operator dictionary (`i18n/ops.ts`) register themselves when their lazily loaded modules load.
`i18n/lazy-strings.test.ts` walks the import graph and fails when an eagerly loaded module uses a key
that only a lazy dictionary defines; in development a missing key logs one console warning.

## Implementation

Stylesheets (`apps/web/src/styles/`; one file per area so the public site, the workspace pages and the
operator console can evolve independently). `main.tsx` imports only `tokens.css`, `base.css`,
`components.css` and `public.css`, so the landing ships no workspace CSS. The other area files load
with the lazily loaded modules that use them: `shell.css` with the app shell (`components/layout.tsx`),
`market.css` with the overview, price-history, forecast and scenario pages, `flows.css` with the
scenario, insight, report, notification and account pages, `ops.css` with the operator pages.

Cascade order: the entry files come first, in this order: `tokens.css`, `base.css`, `components.css`,
`public.css`. The area files follow in the order their chunks load: `shell.css` usually first
(`App.tsx` `preloadWorkspace` requests the shell and the page together), then the page files in the
order the reader opens pages (the scenario page brings both `market.css` and `flows.css`). That order
is not fixed, so **area files must not compete on the same selectors**: a rule for a class lives in
exactly one area file, and an area file may only add to a shared class from `components.css` with a
more specific selector (`.fc-results .tabs`, `.tour .tabs`), never by repeating the same selector.
A class used by both the public site and the workspace (`.page`, `.row-end`) belongs in
`components.css`.

| File | Contents |
|---|---|
| `tokens.css` | Every token of this document as a custom property, nothing else |
| `base.css` | `@font-face`, reset, element defaults, focus, selection, reduced motion, keyframes, layout/text utilities |
| `components.css` | Shared primitives: page header, panels, KPI, stats, delta, badges and chips, buttons, forms, segmented, tabs, callouts, empty/skeleton, tables and record cards, chart legend/tooltip/boxes, data-coverage grid (price history and the operator data page), process explainer, plan card, dialogs, sheets, toasts |
| `shell.css` | Signed-in shell: sidebar, drawer, top bar, main column, page container, shell footer, operator variant, console band |
| `public.css` | Public header, menu and footer, landing sections, sign-in and registration, methodology and legal pages |
| `market.css` | Overview, price history, forecast page, accuracy disclosure, scenario assumption editor |
| `flows.css` | Scenario list and comparison, insight feed and sheet, reports, notifications, account and plan requests |
| `ops.css` | Operator console pages |

Rules: colours, type sizes, radii, shadows, durations and spacing come from tokens (`var(--…)`); the
only literal colours outside `tokens.css` are the SVG illustration and logo components, the static
illustrations in `public/img/illus/` and `SERIES_COLORS` (see Imagery). No `text-transform` capitals and no letter-spaced labels. Put a rule in the file of
the page that uses it; a class used by several areas belongs in `components.css`.

Shell: the customer sidebar is light (`--surface`, `--line` border, active item `--surface-selected` with
a 3 px teal indicator); the operator console keeps a dark `--navy-deep` sidebar and top bar with the
`--on-dark` inks, so the two audiences stay visually distinct. The desktop sidebar scrolls as a whole
when it is taller than the window (short windows, large text), so the main navigation always keeps its
full height. Below 900 px the sidebar becomes a modal drawer and the 56 px top bar matches the sidebar
it opens. Layout components: `components/layout.tsx`
(`AppShell`), `components/public-layout.tsx` (`PublicLayout`, `PublicFooter`), `components/chrome.tsx`
(`LanguageToggle`, `useRouteFocus`, shared by both).

Toggle buttons that select one value (horizon picker, insight category chips, language toggle) show the
selected value as a solid teal fill; the segmented control shows it raised on `--surface`.
