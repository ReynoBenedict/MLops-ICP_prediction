/**
 * Insight feed building blocks. Everything a reader sees about the data (headline, summary,
 * figures, method note, next step) is backend text, already localized; this module only lays it
 * out. Importance is shown by position and a marker (navy dot / teal ring / none), never with
 * alarm colours, because a large price move is not a problem in itself.
 */
import { ArrowRight, ChevronRight, Copy, Database, LineChart } from "lucide-react";
import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import { importanceOf, type Fact, type FeedInsight } from "../api/product-types";
import { dateTime, fmtNum, period } from "../format";
import { useT, type Key } from "../i18n";
import { Sheet } from "./modal";
import { useToast } from "./toast";
import { Badge } from "./ui";

export type Importance = "high" | "medium" | "low";

/** A figure ready to render: label + display text (unit included by the server). */
export interface ShownFact { key: string; label: string; display: string }

/** Facts from the v2 payload, or the v1 `metrics` formatted as value + unit (no wording added). */
export function factsOf(i: FeedInsight, intl: string): ShownFact[] {
  if (i.facts?.length) return i.facts.map((f: Fact) => ({ key: f.key, label: f.label, display: f.display }));
  return (i.metrics ?? []).map((m, idx) => ({
    key: `m${idx}`,
    label: m.label,
    display: m.value === null || m.value === undefined ? "-"
      : typeof m.value === "number" ? `${fmtNum(m.value, intl, Number.isInteger(m.value) || Math.abs(m.value) >= 1000 ? 0 : 2)}${m.unit ? ` ${m.unit}` : ""}`
      : `${m.value}${m.unit ? ` ${m.unit}` : ""}`,
  }));
}

export function useKindLabel(): (kind: string) => string {
  const { t } = useT();
  return (kind: string) => {
    const key = `ins.kind.${kind}` as Key;
    const s = t(key);
    // translate() falls back to the key's last segment when the key does not exist.
    return s === kind.replaceAll("_", " ") ? t("ins.kind.other") : s;
  };
}

/** Benchmark an insight is about (spread insights), for the Price History deep link. */
export function benchmarkOf(i: FeedInsight): "brent" | "dubai" | "wti" | null {
  const ds = [...(i.evidence?.datasets ?? []).map((d) => d.dataset), ...(i.sources ?? []).map((s) => s.dataset ?? "")];
  return (ds.find((d) => d === "brent" || d === "dubai" || d === "wti") as "brent" | "dubai" | "wti" | undefined) ?? null;
}

const FORECAST_KINDS = new Set(["forecast_outlook", "forecast_change", "model_evaluation"]);

export function ImportanceMarker({ level, withLabel = true }: { level: Importance; withLabel?: boolean }) {
  const { t } = useT();
  const label = t(`ins.imp.${level}` as Key);
  return (
    <span className={`imp imp-${level}`}>
      {level !== "low" && <span className="imp-dot" aria-hidden="true" />}
      {withLabel ? <span className="imp-label">{label}</span> : <span className="sr-only">{label}</span>}
    </span>
  );
}

/** "Harga · Sep 2026" caption (kind and period, sentence case). */
export function InsightCaption({ i, marker = true }: { i: FeedInsight; marker?: boolean }) {
  const { locale } = useT();
  const kindLabel = useKindLabel();
  return (
    <div className="ins-caption">
      <span>{kindLabel(i.kind)}{i.period ? ` · ${period(i.period, locale)}` : ""}</span>
      {marker && <ImportanceMarker level={importanceOf(i)} />}
    </div>
  );
}

export function evidenceRef(i: FeedInsight, locale: "id" | "en"): string | null {
  const d = i.evidence?.datasets?.[0];
  if (d) return [d.label, d.provider, d.period_end ? period(d.period_end, locale) : null].filter(Boolean).join(" · ");
  const s = i.sources?.[0];
  if (s) return [s.label, s.period_end ? period(s.period_end, locale) : null].filter(Boolean).join(" · ");
  return null;
}

function Section({ title, children, className = "" }: { title: string; children: ReactNode; className?: string }) {
  return <section className={`ins-sec ${className}`.trim()}><h3>{title}</h3>{children}</section>;
}

function FactList({ facts }: { facts: ShownFact[] }) {
  return <dl className="ins-facts">{facts.map((f, idx) => <div key={`${f.key}-${idx}`} className="ins-fact-row"><dt>{f.label}</dt><dd>{f.display}</dd></div>)}</dl>;
}

/** The kind of evidence an insight rests on (historical, context, forecast-related, data quality). */
export type EvidenceCategory = "historical" | "contextual" | "forecast" | "quality" | "other";
const CATEGORY: Record<string, EvidenceCategory> = {
  material_change: "historical", annual_comparison: "historical", trend: "historical", unusual_movement: "historical",
  benchmark_divergence: "contextual", fx_context: "contextual",
  forecast_outlook: "forecast", forecast_change: "forecast", model_evaluation: "forecast",
  data_quality: "quality", freshness: "quality", ingestion: "quality",
};
export const categoryOf = (kind: string): EvidenceCategory => CATEGORY[kind] ?? "other";

/**
 * Why an insight has its importance: the rule's own reason from the rule catalogue
 * (docs/INSIGHT_ENGINE.md), for the level the rule gave it. Any other rule or level falls back to
 * the general text for that level (W2-R9).
 */
const WHY_BY_RULE: Record<string, Partial<Record<Importance, Key>>> = {
  icp_mom: { low: "ins.whyRule.icp_mom" },
  icp_yoy: { low: "ins.whyRule.icp_yoy" },
  icp_trend_run: { low: "ins.whyRule.icp_trend_run" },
  icp_extreme_move: { medium: "ins.whyRule.icp_extreme_move" },
  spread_vs_history: { medium: "ins.whyRule.spread_vs_history.medium", low: "ins.whyRule.spread_vs_history.low" },
  fx_context: { low: "ins.whyRule.fx_context" },
  forecast_outlook: { medium: "ins.whyRule.forecast_outlook" },
  forecast_revision: { medium: "ins.whyRule.forecast_revision" },
  data_freshness: { high: "ins.whyRule.data_freshness.high", low: "ins.whyRule.data_freshness.low" },
  benchmark_freshness: { medium: "ins.whyRule.benchmark_freshness" },
  data_verification: { medium: "ins.whyRule.data_verification.medium", low: "ins.whyRule.data_verification.low" },
  model_evaluation: { low: "ins.whyRule.model_evaluation" },
  ingestion_failure: { high: "ins.whyRule.ingestion_failure" },
};
export function whyKey(ruleId: string | null | undefined, level: Importance): Key {
  return (ruleId ? WHY_BY_RULE[ruleId]?.[level] : undefined) ?? (`ins.why.${level}` as Key);
}

/**
 * Detail sheet: right drawer on desktop, bottom sheet on phones (see components/modal.tsx). It makes
 * the evidence explicit: what the insight says, which month and comparison window, the figures it
 * read and computed, what kind of evidence it is, why it has its importance, how it is calculated,
 * its limits, and where the underlying data lives.
 */
export function InsightDetail({ i, open, onClose, generatedAt, engineVersion, disclaimer }:
  { i: FeedInsight; open: boolean; onClose: () => void; generatedAt: string; engineVersion?: string; disclaimer?: string }) {
  const { t, locale, intl } = useT();
  const { show } = useToast();
  const kindLabel = useKindLabel();
  const level = importanceOf(i);
  const facts = factsOf(i, intl);
  const ev = i.evidence ?? null;
  const bench = benchmarkOf(i);
  const forecastId = ev?.forecast_ids?.[0] ?? null;
  const category = categoryOf(i.kind);
  const showForecast = !!forecastId || FORECAST_KINDS.has(i.kind);
  const copy = async () => {
    const url = `${window.location.origin}/insight?insight=${encodeURIComponent(i.id)}`;
    try { await navigator.clipboard.writeText(url); show(t("ins.linkCopied"), "good"); }
    catch { show(t("ins.linkCopyFailed"), "info"); }
  };
  const w = ev?.periods;
  const subject = w?.subject ?? i.period;
  const sources = ev?.datasets?.length ? ev.datasets : (i.sources ?? []).map((s) => ({ dataset: s.dataset ?? "", label: s.label, provider: s.provider ?? "", version_id: s.version_id ?? null, period_end: s.period_end ?? null }));
  return (
    <Sheet open={open} onClose={onClose} title={i.title} className="ins-sheet"
      eyebrow={<><span>{kindLabel(i.kind)}{i.period ? ` · ${period(i.period, locale)}` : ""}</span><ImportanceMarker level={level} /></>}
      footer={<>
        <Link className="btn sm primary" to={bench ? `/harga?rentang=3&acuan=${bench}` : "/harga"}><LineChart size={15} aria-hidden="true" />{t("ins.toHistory")}</Link>
        {showForecast && <Link className="btn sm" to={forecastId ? `/proyeksi?proyeksi=${encodeURIComponent(forecastId)}` : "/proyeksi"}>{t("ins.toForecast")} <ArrowRight size={14} aria-hidden="true" /></Link>}
        <button type="button" className="btn sm ghost" onClick={() => void copy()}><Copy size={15} aria-hidden="true" />{t("common.copyLink")}</button>
      </>}>
      <Section title={t("ins.sec.what")}><p>{i.body}</p></Section>

      <dl className="ins-meta-grid">
        <div><dt>{t("ins.meta.kind")}</dt><dd><Badge tone={category === "forecast" ? "cat-forecast" : "neutral"}>{t(`ins.cat.${category}` as Key)}</Badge></dd></div>
        <div><dt>{t("ins.meta.month")}</dt><dd>{subject ? period(subject, locale, true) : t("ins.meta.none")}</dd></div>
        <div><dt>{t("ins.meta.importance")}</dt><dd><ImportanceMarker level={level} /></dd></div>
        <div><dt>{t("ins.meta.generated")}</dt><dd>{dateTime(generatedAt, intl)}</dd></div>
      </dl>

      <Section title={t("ins.sec.data")}>
        {facts.length > 0 && <FactList facts={facts} />}
        {ev && ev.inputs.length > 0 && <><h4>{t("ins.sec.inputs")}</h4><FactList facts={ev.inputs} /></>}
        {ev && ev.outputs.length > 0 && <><h4>{t("ins.sec.outputs")}</h4><FactList facts={ev.outputs} /></>}
        {!ev && <p className="small muted">{t("ins.noEvidence")}</p>}
      </Section>
      {w && (w.window_start || w.subject) && (
        <Section title={t("ins.sec.period")}>
          {w.subject && <p className="small">{t("ins.subjectPeriod", { period: period(w.subject, locale, true) })}</p>}
          {w.window_start && w.window_end && (
            <p className="small">{t("ins.windowLine", { range: t("ins.window", { from: period(w.window_start, locale), to: period(w.window_end, locale) }) })}{w.n ? ` (${t("ins.windowN", { n: w.n })})` : ""}</p>
          )}
        </Section>
      )}
      <Section title={t("ins.sec.kind")}>
        <p className="small">{t(`ins.catBody.${category}` as Key)}</p>
      </Section>
      <Section title={t("ins.sec.why")}><p className="small">{t(whyKey(i.rule_id, level))}</p></Section>
      {ev?.method?.text && (
        <Section title={t("ins.sec.method")}>
          <p className="small">{ev.method.text}</p>
          <details className="ins-tech">
            <summary>{t("ins.technical")}</summary>
            <dl className="ins-facts small">
              {i.rule_id && <div className="ins-fact-row"><dt>{t("ins.tech.rule")}</dt><dd className="mono">{i.rule_id}{i.rule_version ? ` v${i.rule_version}` : ""}</dd></div>}
              {engineVersion && <div className="ins-fact-row"><dt>{t("ins.tech.engine")}</dt><dd className="mono">{engineVersion}</dd></div>}
              {ev.model && <div className="ins-fact-row"><dt>{t("ins.tech.model")}</dt><dd className="mono">v{ev.model.version} · {ev.model.method}</dd></div>}
              {ev.datasets.length > 0 && <div className="ins-fact-row"><dt>{t("ins.tech.datasets")}</dt><dd className="mono">{ev.datasets.map((d) => `${d.dataset}${d.version_id ? `: ${d.version_id}` : ""}`).join(", ")}</dd></div>}
              {ev.forecast_ids.length > 0 && <div className="ins-fact-row"><dt>{t("ins.tech.forecasts")}</dt><dd className="mono">{ev.forecast_ids.join(", ")}</dd></div>}
            </dl>
          </details>
        </Section>
      )}
      {i.next_step && <Section title={t("ins.sec.next")}><p className="small">{i.next_step}</p></Section>}
      <Section title={t("ins.sec.limits")}>
        <p className="small">{t("ins.limits")}</p>
        {category === "forecast" && <p className="small">{t("ins.limitsForecast")}</p>}
        {disclaimer && <p className="small muted">{disclaimer}</p>}
      </Section>
      <Section title={t("ins.sec.links")}>
        {sources.length > 0 && (
          <ul className="ins-sources">
            {sources.map((d, k) => (
              <li key={`${d.dataset}-${k}`}><Database size={14} aria-hidden="true" />
                <span>{[d.label, d.provider, d.period_end ? t("ins.dataThrough", { period: period(d.period_end, locale) }) : null].filter(Boolean).join(" · ")}</span></li>
            ))}
          </ul>
        )}
        <ul className="ins-links">
          {category === "quality" && <li><Link className="link-inline" to="/harga#bukti">{t("ins.link.evidence")} <ArrowRight size={13} aria-hidden="true" /></Link></li>}
          <li><Link className="link-inline" to="/metodologi">{t("ins.link.method")} <ArrowRight size={13} aria-hidden="true" /></Link></li>
        </ul>
      </Section>
    </Sheet>
  );
}

/** One-line row: headline, key figure, period, chevron. The whole row is the button. */
export function InsightRow({ i, onOpen }: { i: FeedInsight; onOpen: () => void }) {
  const { locale, intl } = useT();
  const fact = factsOf(i, intl)[0];
  const level = importanceOf(i);
  return (
    <li>
      <button type="button" className="ins-row" onClick={onOpen}>
        <ImportanceMarker level={level} withLabel={false} />
        <span className="ins-row-title">{i.title}</span>
        {fact && <span className="ins-row-fact">{fact.display}</span>}
        {i.period && <span className="ins-row-period">{period(i.period, locale)}</span>}
        <ChevronRight size={16} aria-hidden="true" className="ins-row-chev" />
      </button>
    </li>
  );
}

/** Lead story: the most important item, with a value strip and the evidence reference. */
export function InsightLead({ i, onOpen }: { i: FeedInsight; onOpen: () => void }) {
  const { t, locale, intl } = useT();
  const facts = factsOf(i, intl).slice(0, 3);
  const ref = evidenceRef(i, locale);
  return (
    <article className="ins-lead">
      <InsightCaption i={i} />
      <h2 className="ins-lead-title">{i.title}</h2>
      <p className="ins-lead-body">{i.body}</p>
      {facts.length > 0 && (
        <dl className="ins-strip">
          {facts.map((f, idx) => <div key={`${f.key}-${idx}`}><dt>{f.label}</dt><dd>{f.display}</dd></div>)}
        </dl>
      )}
      <div className="ins-lead-foot">
        {ref ? <span className="xs muted">{t("ins.evidenceRef")}: {ref}</span> : <span />}
        <button type="button" className="btn sm" onClick={onOpen} aria-label={t("ins.open", { title: i.title })}>{t("ins.readMore")} <ArrowRight size={14} aria-hidden="true" /></button>
      </div>
    </article>
  );
}

/** Compact item (right column): caption, headline, first figure. */
export function InsightCompact({ i, onOpen }: { i: FeedInsight; onOpen: () => void }) {
  const { intl } = useT();
  const fact = factsOf(i, intl)[0];
  return (
    <li>
      <button type="button" className="ins-compact" onClick={onOpen}>
        <InsightCaption i={i} />
        <span className="ins-compact-title">{i.title}</span>
        {fact && <span className="ins-compact-fact">{fact.label}: <strong>{fact.display}</strong></span>}
      </button>
    </li>
  );
}

