/**
 * Derived market context computed client-side from the real `/data/history` series.
 * Nothing here invents data: month-over-month and year-over-year changes are only reported when
 * both months were actually observed, and gaps are reported as gaps.
 */
import type { HistoryPoint } from "../api/types";
import { shiftPeriod } from "../format";
import type { Zone } from "../components/charts";

export type Key = "icp_price" | "wti_price" | "brent_price" | "dubai_price" | "usd_idr" | "icp_idr";

export interface SeriesContext {
  latestPeriod: string | null;
  latest: number | null;
  prevPeriod: string | null;
  prev: number | null;
  momPct: number | null;
  yoyPeriod: string | null;
  yoy: number | null;
  yoyPct: number | null;
  high12: number | null;
  low12: number | null;
}

const val = (p: HistoryPoint | undefined, key: Key): number | null => {
  const v = p?.[key];
  return typeof v === "number" ? v : null;
};

export function seriesContext(points: HistoryPoint[], key: Key): SeriesContext {
  const byPeriod = new Map(points.map((p) => [p.period, p]));
  const observed = points.filter((p) => typeof p[key] === "number");
  const last = observed.at(-1);
  if (!last) return { latestPeriod: null, latest: null, prevPeriod: null, prev: null, momPct: null, yoyPeriod: null, yoy: null, yoyPct: null, high12: null, low12: null };
  const latest = last[key] as number;
  const prevPeriod = shiftPeriod(last.period, -1);
  const yoyPeriod = shiftPeriod(last.period, -12);
  const prev = val(byPeriod.get(prevPeriod), key);
  const yoy = val(byPeriod.get(yoyPeriod), key);
  const window = observed.filter((p) => p.period > yoyPeriod).map((p) => p[key] as number);
  return {
    latestPeriod: last.period, latest, prevPeriod, prev,
    momPct: prev ? ((latest - prev) / prev) * 100 : null,
    yoyPeriod, yoy, yoyPct: yoy ? ((latest - yoy) / yoy) * 100 : null,
    high12: window.length ? Math.max(...window) : null,
    low12: window.length ? Math.min(...window) : null,
  };
}

/** Contiguous runs of missing months inside the observed range, as chart zones. */
export function gapZones(points: HistoryPoint[], key: Key = "icp_price", label?: string): Zone[] {
  const observed = points.filter((p) => typeof p[key] === "number").map((p) => p.period);
  if (!observed.length) return [];
  const first = observed[0]!, last = observed.at(-1)!;
  const zones: Zone[] = [];
  let start: string | null = null, prev: string | null = null;
  for (const p of points) {
    if (p.period < first || p.period > last) continue;
    if (typeof p[key] !== "number") {
      if (!start) start = p.period;
      prev = p.period;
    } else if (start && prev) {
      zones.push({ from: start, to: prev, kind: "gap", label });
      start = prev = null;
    }
  }
  return zones;
}

export function filterRange(points: HistoryPoint[], years: number | "all"): HistoryPoint[] {
  if (years === "all" || !points.length) return points;
  const last = points.at(-1)!.period;
  const from = shiftPeriod(last, -12 * years + 1);
  return points.filter((p) => p.period >= from);
}

/** ICP minus a benchmark for months where both are observed. */
export function spreadSeries(points: HistoryPoint[], bench: Key): Array<{ period: string; spread: number | null }> {
  return points.map((p) => {
    const a = val(p, "icp_price"), b = val(p, bench);
    return { period: p.period, spread: a !== null && b !== null ? a - b : null };
  });
}

export function mean(values: Array<number | null | undefined>): number | null {
  const xs = values.filter((v): v is number => typeof v === "number" && Number.isFinite(v));
  return xs.length ? xs.reduce((s, v) => s + v, 0) / xs.length : null;
}

export { shiftPeriod };
