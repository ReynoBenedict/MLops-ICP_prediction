/**
 * Public data for the public site (landing, sign-in, registration): the anonymous market snapshot
 * and the plan catalogue, with their delivery state (live or the gateway's stored copy, contract C2).
 * Both go through `publicGetMeta`, so one page load costs one request per resource however many
 * components read it. Nothing here ever reads forecast values: the snapshot's `latest_forecast` is a
 * teaser (policy v4) and `forecastTeaser` only keeps its date, origin and horizon.
 */
import { useEffect, useState } from "react";
import { publicGetMeta, TIMEOUTS, type PublicResult } from "../../api/client";
import { useResource, type Resource } from "../../api/hooks";
import type { MarketSnapshot, PlanInfo } from "../../api/types";
import { dateOnly } from "../../format";
import type { useT } from "../../i18n";

/** The official ESDM page that lists every ICP determination (reachable when this service is not). */
export const ESDM_ICP_URL = "https://www.migas.esdm.go.id/post/harga-minyak-mentah";

/** Seconds the public pages wait for public data before showing an error (client contract C3). */
export const PUBLIC_WAIT_SECONDS = Math.round(TIMEOUTS.public / 1000);

export type SnapshotResource = Resource<PublicResult<MarketSnapshot>>;

export function usePublicSnapshot(): SnapshotResource {
  return useResource((s) => publicGetMeta<MarketSnapshot>("/public/market-snapshot", { signal: s }), []);
}

export function usePlans(): Resource<PublicResult<PlanInfo[]>> {
  return useResource((s) => publicGetMeta<PlanInfo[]>("/plans", { signal: s }), []);
}

/** True once `active` has lasted longer than `ms` (a "still waiting" hint inside a loading state). */
export function useSlow(active: boolean, ms = 3000): boolean {
  const [slow, setSlow] = useState(false);
  useEffect(() => {
    setSlow(false);
    if (!active) return;
    const id = window.setTimeout(() => setSlow(true), ms);
    return () => window.clearTimeout(id);
  }, [active, ms]);
  return slow && active;
}

/** Teaser data from either snapshot shape: v4 `{available, issue_date, forecast_origin, horizon}` or the older one with points. Never values. */
export function forecastTeaser(raw: unknown): { issueDate: string | null; origin: string | null; horizon: number | null } | null {
  if (!raw || typeof raw !== "object") return null;
  const f = raw as { available?: unknown; issue_date?: unknown; forecast_origin?: unknown; horizon?: unknown; points?: unknown };
  const available = f.available === true || (f.available === undefined && Array.isArray(f.points) && f.points.length > 0);
  if (!available) return null;
  const horizon = typeof f.horizon === "number" ? f.horizon : Array.isArray(f.points) ? f.points.length : null;
  return {
    issueDate: typeof f.issue_date === "string" ? f.issue_date : null,
    origin: typeof f.forecast_origin === "string" ? f.forecast_origin : null,
    horizon: horizon && horizon > 0 ? Math.min(Math.round(horizon), 24) : null,
  };
}

/**
 * The JISDOR rate the same-month rupiah equivalent was computed with (`icp_idr = icp_usd × rate`).
 * `usd_idr` has no period of its own (it is the latest FX month), so the rate shown next to the
 * rupiah value is the one implied by that value, rounded to whole rupiah.
 */
export function impliedRate(s: Pick<MarketSnapshot, "icp_usd" | "icp_idr">): number | null {
  if (s.icp_usd == null || s.icp_idr == null || !(s.icp_usd > 0)) return null;
  return Math.round(s.icp_idr / s.icp_usd);
}

/** One source document of the latest ICP month (contract C1 `icp_evidence.documents`). */
export type EvidenceDoc = NonNullable<MarketSnapshot["icp_evidence"]>["documents"][number];

/**
 * A source document in words: "Kepmen ESDM 395.K/..." (the decree's own number only when the API
 * gives it; never an invented one) or "Pengumuman Ditjen Migas, 13 Oktober 2026" (date only when known).
 */
export function docLabel(doc: EvidenceDoc, t: ReturnType<typeof useT>["t"], intl: string): string {
  if (doc.kind === "decree") return doc.reference ? t("landing.panel.decree", { ref: doc.reference }) : t("landing.panel.decreeNoRef");
  return doc.published_on ? t("landing.panel.announcement", { date: dateOnly(doc.published_on, intl) }) : t("landing.panel.announcementNoDate");
}

/** A month of the public 24-month history (official ICP and Brent; null = no value, never 0). */
export interface PublicMonth { period: string; icp: number | null; brent: number | null }

const finite = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);

/** `recent` from the snapshot, validated and in ascending order. Empty when the server sends none. */
export function recentMonths(s: MarketSnapshot | null | undefined): PublicMonth[] {
  const raw = (s as { recent?: unknown } | null | undefined)?.recent;
  if (!Array.isArray(raw)) return [];
  const out: PublicMonth[] = [];
  for (const r of raw) {
    if (!r || typeof r !== "object") continue;
    const m = r as { period?: unknown; icp_usd?: unknown; brent_usd?: unknown };
    if (typeof m.period !== "string" || !/^\d{4}-\d{2}$/.test(m.period)) continue;
    out.push({ period: m.period, icp: finite(m.icp_usd), brent: finite(m.brent_usd) });
  }
  return out.sort((a, b) => (a.period < b.period ? -1 : a.period > b.period ? 1 : 0));
}
