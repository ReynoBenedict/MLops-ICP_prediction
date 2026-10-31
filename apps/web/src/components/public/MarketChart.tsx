/**
 * Public 24-month chart of official ICP vs Brent (landing hero). Plain SVG, no charting library
 * (DESIGN_SYSTEM.md: the public bundle carries none). Rules applied:
 * - one y-axis in USD/bbl with nice ticks; x labels every 6 months (every 12 when narrow);
 * - ICP 2 px in --series-icp, Brent 1.5 px in --series-brent; missing official months are breaks,
 *   never interpolated, tinted --gap-zone (a single missing month: a 7 px --gap-mark band);
 * - the last points are marked and labelled directly (the legend lives in the panel), in a gutter of
 *   their own right of the last month: data | end labels | forecast window; a series is labelled
 *   only when it has a value in the newest month (a withheld latest ICP, whose official documents
 *   disagree, must not leave the previous month's value in the latest position);
 * - text follows the reader's text size (tokens in rem): label widths, the axis room, the end-label
 *   spacing and the x-label interval scale with the px size of --fs-meta (lib/text-size.ts), so
 *   labels do not collide at 200 % text;
 * - after that gutter, when a forecast has been issued, a hatched window `zone` months wide marks
 *   the forecast period WITHOUT values (policy v4: guests never see forecast values): no label,
 *   tooltip or crosshair is ever drawn inside it;
 * - crosshair + tooltip on hover, touch and keyboard focus (Left/Right/Home/End move between
 *   months, Escape clears); a tapped month stays shown until a tap elsewhere; a polite live readout
 *   for screen readers; the SVG itself is hidden from assistive technology because the table view
 *   and the summary carry the same values;
 * - one draw-in on first load (clip only, 700 ms), nothing under prefers-reduced-motion (base.css).
 */
import { Lock } from "lucide-react";
import { useId, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent, type PointerEvent } from "react";
import { fmtNum, fmtSigned, fmtUsd, period } from "../../format";
import { useT } from "../../i18n";
import { META_PX, useMetaPx } from "../../lib/text-size";
import type { PublicMonth } from "./snapshot";
import { linePath } from "./svg-path";

/** Clean tick step (1, 2, 2.5, 5 × 10^k) for a raw step. */
function niceStep(raw: number): number {
  const p = 10 ** Math.floor(Math.log10(raw));
  const f = raw / p;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * p;
}

/** Y domain and ticks that enclose every value (about `target` intervals). */
export function niceTicks(values: number[], target = 4): number[] {
  if (!values.length) return [0, 1];
  let lo = Math.min(...values), hi = Math.max(...values);
  if (lo === hi) { lo -= 1; hi += 1; }
  const step = niceStep((hi - lo) / target);
  const min = Math.floor(lo / step) * step, max = Math.ceil(hi / step) * step;
  const out: number[] = [];
  for (let v = min; v <= max + step / 2; v += step) out.push(Number(v.toFixed(6)));
  return out;
}

/** Width of an element, following resizes (falls back to `fallback` where layout is unavailable). */
function useWidth(fallback: number) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(fallback);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const read = () => { const w = el.clientWidth; if (w > 0) setWidth(Math.round(w)); };
    read();
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(read);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, width] as const;
}

/** Index of the last non-null value (-1 when there is none). */
const lastIndex = (values: Array<number | null>) => values.reduce<number>((acc, v, i) => (v !== null ? i : acc), -1);

/** Indices of values with no neighbour on either side (a line cannot show them: they get a dot). */
function isolated(values: Array<number | null>): number[] {
  return values.flatMap((v, i) => (v !== null && (values[i - 1] ?? null) === null && (values[i + 1] ?? null) === null ? [i] : []));
}

/** Runs of consecutive months without an official ICP value: [start, end] inclusive. */
export function gapRuns(values: Array<number | null>): Array<[number, number]> {
  const runs: Array<[number, number]> = [];
  values.forEach((v, i) => {
    if (v !== null) return;
    const last = runs[runs.length - 1];
    if (last && last[1] === i - 1) last[1] = i; else runs.push([i, i]);
  });
  return runs;
}

const TEXT_W = 7.1; // approximate advance of a 12.5 px (META_PX) tabular digit/letter, for label placement

/** Text for a month without an ICP value: withheld (documents disagree) or no official value. */
export function noIcpText(p: string, withheld: string | null | undefined, t: ReturnType<typeof useT>["t"]): string {
  return withheld && p === withheld ? t("landing.chart.withheld") : t("landing.chart.noValue");
}

/**
 * `withheld`: the month (YYYY-MM) whose ICP the server withholds because its official documents
 * disagree (snapshot `icp_evidence.basis` "conflict"); it is drawn like a month without a value but
 * named as withheld in the tooltip and the readout.
 */
export function MarketChart({ months, zone, animate, withheld = null }: { months: PublicMonth[]; zone: number; animate: boolean; withheld?: string | null }) {
  const { t, intl, locale } = useT();
  const [wrapRef, width] = useWidth(640);
  // Chart text is var(--fs-meta): scale the text-dependent geometry with it (1 at the default size).
  const scale = useMetaPx() / META_PX;
  const textW = TEXT_W * scale;
  const [active, setActive] = useState<number | null>(null);
  const [readout, setReadout] = useState("");
  // The draw-in belongs to the first render of this chart only (a resize must not cut or replay it).
  const [anim] = useState(animate);
  const pointerFocus = useRef(false);
  // The tooltip's own size, measured after it renders, keeps it inside the chart on any width.
  const tipRef = useRef<HTMLDivElement>(null);
  const [tipSize, setTipSize] = useState({ w: 176, h: 96 });
  useLayoutEffect(() => {
    const el = tipRef.current;
    if (!el) return;
    const w = el.offsetWidth, h = el.offsetHeight;
    if (w > 0 && (w !== tipSize.w || h !== tipSize.h)) setTipSize({ w, h });
  }, [active, locale, tipSize.w, tipSize.h]);
  const uid = useId().replace(/[^a-zA-Z0-9_-]/g, "");
  const n = months.length;

  const geo = useMemo(() => {
    const height = (width < 480 ? 200 : 240) + Math.round((scale - 1) * 40);
    const icp = months.map((m) => m.icp);
    const brent = months.map((m) => m.brent);
    const values = [...icp, ...brent].filter((v): v is number => v !== null);
    const ticks = niceTicks(values, height < 220 ? 3 : 4);
    const lo = ticks[0]!, hi = ticks[ticks.length - 1]!;
    const tickLabels = ticks.map((v) => fmtNum(v, intl, Number.isInteger(v) ? 0 : 1));
    const left = Math.max(...tickLabels.map((s) => s.length)) * textW + 10;
    const top = Math.round(12 * Math.max(1, scale * 0.8)), bottom = Math.round(26 * scale), right = 8;
    // The end labels sit in their own gutter right of the last month (point + 9 px + text + 8 px),
    // so they never fall inside the forecast window, which carries no values.
    const lastIcp = lastIndex(icp), lastBrent = lastIndex(brent);
    const endValues = [lastIcp >= 0 && lastIcp === n - 1 ? icp[lastIcp]! : null, lastBrent >= 0 && lastBrent === n - 1 ? brent[lastBrent]! : null]
      .filter((v): v is number => v !== null);
    const gutter = endValues.length ? Math.max(...endValues.map((v) => fmtNum(v, intl, 2).length)) * textW + 17 : 0;
    const slots = n + zone;
    const slot = Math.max(40, width - left - right - gutter) / Math.max(1, slots);
    const plotH = height - top - bottom;
    const x = (i: number) => left + slot * (i + 0.5);
    const y = (v: number) => top + (1 - (v - lo) / (hi - lo || 1)) * plotH;
    const dataRight = left + slot * n;
    const zoneX = dataRight + gutter;
    const plotRight = zone > 0 ? zoneX + slot * zone : dataRight;
    // x labels: the last month and every 6 (12, 18, ...) months before it, so the newest month is
    // always named and two labels never meet (large text or a narrow chart: a longer interval).
    const labelW = 8 * textW;
    const every = Math.max(1, Math.ceil((labelW + 8) / (slot * 6))) * 6;
    const xLabels = months.flatMap((m, i) => ((n - 1 - i) % every === 0 ? [{ i, text: period(m.period, locale) }] : []));
    const labelDy = Math.round(18 * scale);
    return { height, icp, brent, ticks, tickLabels, left, top, plotH, slot, x, y, xLabels, labelW, labelDy, gutter, dataRight, zoneX, plotRight, lastIcp, lastBrent };
  }, [months, zone, width, intl, locale, n, scale, textW]);

  const { height, icp, brent, ticks, tickLabels, left, top, plotH, slot, x, y, xLabels, labelW, labelDy, gutter, dataRight, zoneX, plotRight, lastIcp, lastBrent } = geo;
  const plotBottom = top + plotH;
  const gaps = gapRuns(icp);

  // End labels (value only; the line beside each label carries the identity, the legend names it).
  // Higher value above, at least one text line apart (17 px for 12.5 px text, scaled with the text
  // size); a short leader joins a label that had to move. Only values of the newest month are labelled.
  type EndLabel = { key: "icp" | "brent"; i: number; v: number; ly: number };
  const ends: EndLabel[] = [];
  if (lastIcp >= 0 && lastIcp === n - 1) ends.push({ key: "icp", i: lastIcp, v: icp[lastIcp]!, ly: y(icp[lastIcp]!) });
  if (lastBrent >= 0 && lastBrent === n - 1) ends.push({ key: "brent", i: lastBrent, v: brent[lastBrent]!, ly: y(brent[lastBrent]!) });
  const sep = 17 * scale;
  if (ends.length === 2) {
    const [a, b] = ends[0]!.ly <= ends[1]!.ly ? [ends[0]!, ends[1]!] : [ends[1]!, ends[0]!];
    if (b.ly - a.ly < sep) { const mid = (a.ly + b.ly) / 2; a.ly = mid - sep / 2; b.ly = mid + sep / 2; }
  }
  const endText = (v: number) => fmtNum(v, intl, 2);
  // Right of the point inside the gutter; left of it only if the text would not fit before the window or the edge.
  const placeLeft = (e: EndLabel) => x(e.i) + 9 + endText(e.v).length * textW > (zone > 0 ? zoneX : width) - 2;

  const tip = active !== null ? months[active] : null;
  const spread = tip && tip.icp !== null && tip.brent !== null ? tip.icp - tip.brent : null;
  const fmtVal = (v: number | null, missing: string) => (v === null ? missing : fmtUsd(v, intl));

  const say = (i: number | null) => {
    if (i === null) { setReadout(""); return; }
    const m = months[i]!;
    const s = m.icp !== null && m.brent !== null ? fmtSigned(m.icp - m.brent, intl, 2) : "-";
    setReadout(t("landing.chart.readout", { period: period(m.period, locale, true), icp: fmtVal(m.icp, noIcpText(m.period, withheld, t)), brent: fmtVal(m.brent, t("landing.chart.noBrent")), spread: s }));
  };
  const move = (i: number | null, announce: boolean) => { setActive(i); if (announce) say(i); };

  const onPointer = (e: PointerEvent<SVGSVGElement>) => {
    if (e.type === "pointerdown") pointerFocus.current = true;
    const box = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - box.left;
    // Left of the plot: the first month; in the end-label gutter: the last month; over the forecast
    // window: nothing (it has no values to show).
    move(px < left ? 0 : px < dataRight ? Math.min(n - 1, Math.floor((px - left) / slot)) : px < dataRight + gutter ? n - 1 : null, false);
  };
  // A finger lifting fires pointerleave: a tapped month stays shown until a tap elsewhere (blur).
  const onLeave = (e: PointerEvent<SVGSVGElement>) => { if (e.pointerType !== "touch") move(null, false); };
  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const cur = active ?? n;
    const next = e.key === "ArrowLeft" ? Math.max(0, cur - 1) : e.key === "ArrowRight" ? Math.min(n - 1, active === null ? n - 1 : cur + 1)
      : e.key === "Home" ? 0 : e.key === "End" ? n - 1 : e.key === "Escape" ? null : undefined;
    if (next === undefined) return;
    e.preventDefault();
    move(next, true);
  };

  // Tooltip placement: right of the crosshair, else left of it; on a chart too narrow for either,
  // clamped inside the chart and moved to the half of the plot away from the month's points.
  // It never reaches into the forecast window (official values must not read as the hidden forecast).
  let tipX = 0, tipY = top;
  if (active !== null) {
    const cx = x(active);
    const limit = zone > 0 ? zoneX : width;
    const right = cx + 14, leftSide = cx - 14 - tipSize.w;
    if (right + tipSize.w <= limit) tipX = right;
    else if (leftSide >= 0) tipX = leftSide;
    else {
      tipX = Math.max(0, Math.min(limit - tipSize.w, cx - tipSize.w / 2));
      const ys = [icp[active], brent[active]].filter((v): v is number => v !== null).map(y);
      const upper = ys.length > 0 && Math.min(...ys) < top + plotH / 2;
      tipY = upper ? Math.max(top, plotBottom - tipSize.h) : top;
    }
  }
  const first = months[0], last = months[n - 1];

  return (
    <div ref={wrapRef} className={`mkt-chart${anim ? " mkt-anim" : ""}`} style={{ height }}
      role="group" tabIndex={0} aria-label={t("landing.chart.aria", { from: period(first?.period, locale, true), to: period(last?.period, locale, true) })}
      onKeyDown={onKey} onBlur={() => move(null, false)}
      // Keyboard focus starts at the newest month; a click keeps the month under the pointer.
      onFocus={() => { if (!pointerFocus.current && active === null) move(n - 1, true); pointerFocus.current = false; }}>
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true" focusable="false"
        onPointerMove={onPointer} onPointerDown={onPointer} onPointerLeave={onLeave}>
        {zone > 0 && (
          <defs>
            <pattern id={`hatch-${uid}`} width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
              <line x1="0" y1="0" x2="0" y2="7" className="mkt-hatch-line" />
            </pattern>
          </defs>
        )}
        {/* Grid and y ticks */}
        {ticks.map((v, k) => (
          <g key={v}>
            <line x1={left} x2={plotRight} y1={y(v)} y2={y(v)} className={k === 0 ? "mkt-axis" : "mkt-grid"} />
            <text x={left - 8} y={y(v)} dy="0.32em" textAnchor="end" className="mkt-tick">{tickLabels[k]}</text>
          </g>
        ))}
        {/* Missing official months */}
        {gaps.map(([a, b]) => a === b
          ? <rect key={a} x={x(a) - 3.5} y={top} width={7} height={plotH} className="mkt-gap-mark" />
          : <rect key={a} x={x(a) - slot / 2} y={top} width={slot * (b - a + 1)} height={plotH} className="mkt-gap-zone" />)}
        {/* Forecast window: no values, only its extent */}
        {zone > 0 && (
          <g className="mkt-zone">
            <rect x={zoneX} y={top} width={plotRight - zoneX} height={plotH} className="mkt-zone-fill" />
            <rect x={zoneX} y={top} width={plotRight - zoneX} height={plotH} fill={`url(#hatch-${uid})`} />
            <line x1={zoneX} x2={zoneX} y1={top} y2={plotBottom} className="mkt-zone-edge" />
            {plotRight - zoneX >= 22 && <Lock x={zoneX + (plotRight - zoneX) / 2 - 7} y={plotBottom - 22} width={14} height={14} className="mkt-zone-lock" aria-hidden="true" />}
          </g>
        )}
        {/* x labels */}
        {xLabels.map(({ i, text }) => {
          const cx = x(i);
          const anchor = cx - labelW / 2 < 0 ? "start" : cx + labelW / 2 > width ? "end" : "middle";
          return <text key={i} x={anchor === "start" ? Math.max(0, cx - 4) : cx} y={plotBottom + labelDy} textAnchor={anchor} className="mkt-tick">{text}</text>;
        })}
        {/* Series */}
        <g className="mkt-series">
          <path d={linePath(brent, x, y)} className="mkt-line brent" />
          {isolated(brent).map((i) => <circle key={`b${i}`} cx={x(i)} cy={y(brent[i]!)} r={2} className="mkt-dot brent" />)}
          <path d={linePath(icp, x, y)} className="mkt-line icp" />
          {isolated(icp).map((i) => <circle key={`i${i}`} cx={x(i)} cy={y(icp[i]!)} r={2.5} className="mkt-dot icp" />)}
        </g>
        {/* Marked and labelled last points */}
        <g className="mkt-ends">
          {ends.map((e) => {
            const px = x(e.i), py = y(e.v);
            const onLeft = placeLeft(e);
            const tx = onLeft ? px - 9 : px + 9;
            return (
              <g key={e.key}>
                {Math.abs(e.ly - py) > 3 && <line x1={px + (onLeft ? -4 : 4)} x2={tx + (onLeft ? 2 : -2)} y1={py} y2={e.ly} className="mkt-leader" />}
                <circle cx={px} cy={py} r={e.key === "icp" ? 4 : 3} className={`mkt-end ${e.key}`} />
                <text x={tx} y={e.ly} dy="0.32em" textAnchor={onLeft ? "end" : "start"} className={`mkt-end-label ${e.key}`}>{endText(e.v)}</text>
              </g>
            );
          })}
        </g>
        {/* Crosshair */}
        {active !== null && (
          <g className="mkt-cross">
            <line x1={x(active)} x2={x(active)} y1={top} y2={plotBottom} className="mkt-cross-line" />
            {brent[active] !== null && <circle cx={x(active)} cy={y(brent[active]!)} r={4} className="mkt-end brent" />}
            {icp[active] !== null && <circle cx={x(active)} cy={y(icp[active]!)} r={4.5} className="mkt-end icp" />}
          </g>
        )}
        {/* Pointer target: the months and their label gutter, so the reader aims at a month, not at a 2 px line. */}
        <rect x={left} y={top} width={dataRight + gutter - left} height={plotH} className="mkt-hit" />
      </svg>
      {tip && (
        <div ref={tipRef} className="mkt-tip" style={{ left: tipX, top: tipY }} aria-hidden="true">
          <div className="mkt-tip-title">{period(tip.period, locale, true)}</div>
          <div className="mkt-tip-row"><span className="mkt-key icp" /><strong>{tip.icp === null ? noIcpText(tip.period, withheld, t) : fmtNum(tip.icp, intl, 2)}</strong><span>{t("landing.chart.icp")}</span></div>
          <div className="mkt-tip-row"><span className="mkt-key brent" /><strong>{tip.brent === null ? t("landing.chart.noBrent") : fmtNum(tip.brent, intl, 2)}</strong><span>{t("landing.chart.brent")}</span></div>
          {spread !== null && <div className="mkt-tip-row"><span className="mkt-key none" /><strong>{fmtSigned(spread, intl, 2)}</strong><span>{t("landing.chart.spread")}</span></div>}
          <div className="mkt-tip-unit">{t("landing.panel.unit")}</div>
        </div>
      )}
      <div className="sr-only" aria-live="polite">{readout}</div>
    </div>
  );
}

/**
 * One sentence that says what the chart shows (screen readers; the table view has every value). A
 * withheld month (`withheld`, documents disagree) is named as such, not counted as a month without
 * an official value.
 */
export function chartSummary(months: PublicMonth[], t: ReturnType<typeof useT>["t"], intl: string, locale: "id" | "en", withheld: string | null = null): string {
  const withIcp = months.filter((m): m is PublicMonth & { icp: number } => m.icp !== null);
  if (withIcp.length === 0) return "";
  const firstM = withIcp[0]!, lastM = withIcp[withIcp.length - 1]!;
  const maxM = withIcp.reduce((a, b) => (b.icp > a.icp ? b : a));
  const minM = withIcp.reduce((a, b) => (b.icp < a.icp ? b : a));
  const spreads = months.flatMap((m) => (m.icp !== null && m.brent !== null ? [m.icp - m.brent] : []));
  const meanSpread = spreads.length ? spreads.reduce((a, b) => a + b, 0) / spreads.length : null;
  const usd = (v: number) => fmtUsd(v, intl);
  const held = withheld ? months.find((m) => m.period === withheld && m.icp === null) ?? null : null;
  const gaps = months.length - withIcp.length - (held ? 1 : 0);
  const sentence = t("landing.chart.summary", {
    first: usd(firstM.icp), firstPeriod: period(firstM.period, locale, true), last: usd(lastM.icp), lastPeriod: period(lastM.period, locale, true),
    max: usd(maxM.icp), maxPeriod: period(maxM.period, locale, true), min: usd(minM.icp), minPeriod: period(minM.period, locale, true),
    spread: meanSpread === null ? "-" : fmtSigned(meanSpread, intl, 2),
  });
  const parts = [sentence];
  if (gaps > 0) parts.push(t("landing.chart.summaryGaps", { n: gaps }));
  if (held) parts.push(t("landing.chart.summaryWithheld", { period: period(held.period, locale, true) }));
  return parts.join(" ");
}
