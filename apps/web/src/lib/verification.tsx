/**
 * Per-month verification of the official ICP, from GET /data/verification (plan-clipped, server
 * labels per basis in both languages). The frontend adds no judgement of its own:
 * - the label and explanation come from the server;
 * - a month the server does not list reads "no evidence recorded" (never "observed");
 * - only `two_documents` / `decree_text` look confirmed (teal icon); weaker evidence gets the `--warn`
 *   ink icon, a conflict `--bad`, no evidence grey (amber is reserved for the brand, see DESIGN_SYSTEM.md).
 */
import { CircleCheck, CircleDashed, CircleMinus, Eye, TriangleAlert } from "lucide-react";
import { useMemo } from "react";
import { api } from "../api/client";
import { useResource } from "../api/hooks";
import { CONFIRMED_BASES, loc, type Basis, type LocalizedText, type VerificationResponse } from "../api/product-types";
import { useT, type Key } from "../i18n";

export interface MonthVerification {
  basis: Basis;
  label: string;
  explanation: string | null;
  reference: string | null;
  /** Server text when another source reading differs from the value used ("Sumber berbeda: …"); null when none. */
  dissent: string | null;
}

export interface VerificationState {
  byPeriod: Map<string, MonthVerification>;
  counts: Partial<Record<Basis, number>>;
  /** Months in range with at least one differing source reading (server count; 0 when not sent). */
  dissentMonths: number;
  ready: boolean;
  loading: boolean;
  error: unknown;
  /** Verification of a month (falls back to "no evidence recorded"). */
  forPeriod: (p: string) => MonthVerification;
  /** Label of a basis in the active language (server legend first). */
  labelOf: (b: Basis) => { label: string; explanation: string | null };
  /** Ask the server again (retry after an error). */
  reload: () => void;
}

export function useVerification(dataset = "icp"): VerificationState {
  const { t, locale } = useT();
  const res = useResource((s) => api<VerificationResponse>("/data/verification", { query: { dataset }, signal: s }), [dataset]);
  return useMemo(() => {
    const data = res.data;
    const labelOf = (b: Basis) => {
      const legend = data?.legend?.[b];
      return { label: loc(legend?.label, locale) ?? t(`verify.${b}` as Key), explanation: loc(legend?.explanation, locale) ?? t(`verify.${b}.help` as Key) };
    };
    const byPeriod = new Map<string, MonthVerification>();
    for (const m of data?.months ?? []) {
      const dissent = (m as typeof m & { dissent_label?: LocalizedText | null }).dissent_label;
      byPeriod.set(m.period, { basis: m.basis, label: loc(m.label, locale) ?? labelOf(m.basis).label, explanation: loc(m.explanation, locale), reference: m.reference, dissent: loc(dissent, locale) });
    }
    const none: MonthVerification = { basis: "no_evidence", ...labelOf("no_evidence"), reference: null, dissent: null };
    return {
      byPeriod, counts: (data?.counts ?? {}) as Partial<Record<Basis, number>>, dissentMonths: (data as (typeof data & { dissent_months?: number }) | null)?.dissent_months ?? 0, ready: !!data, loading: res.loading, error: res.error,
      forPeriod: (p: string) => byPeriod.get(p) ?? none, labelOf, reload: res.reload,
    };
  }, [res.data, res.loading, res.error, res.reload, t, locale]);
}

/** Evidence chip: icon + text; teal icon for confirmed, `--warn` ink icon for weaker evidence, `--bad` for a conflict, grey for none. Never green. */
export function EvidenceBadge({ v }: { v: Pick<MonthVerification, "basis" | "label" | "explanation"> }) {
  const confirmed = CONFIRMED_BASES.has(v.basis);
  const cls = v.basis === "conflict" ? "conflict" : confirmed ? "confirmed" : v.basis === "no_evidence" ? "none" : "weak";
  const Icon = v.basis === "conflict" ? TriangleAlert : confirmed ? CircleCheck : v.basis === "no_evidence" ? CircleMinus : v.basis === "ocr_single" ? Eye : CircleDashed;
  return <span className={`evidence-dot ${cls}`} title={v.explanation ?? undefined}><Icon size={14} aria-hidden="true" />{v.label}</span>;
}

/** Evidence chip plus, when the server reports one, the "differing source" note (never changes the status). */
export function EvidenceCell({ v }: { v: MonthVerification }) {
  return <>{<EvidenceBadge v={v} />}{v.dissent && <span className="sub dissent-note">{v.dissent}</span>}</>;
}
