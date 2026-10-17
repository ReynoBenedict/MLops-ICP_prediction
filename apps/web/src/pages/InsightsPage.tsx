/** Insight Pasar: rule-generated insights grouped by severity, with figures, sources and next steps. */
import type { InsightsResponse } from "../api/types";
import { ArrowRight, Database, Lightbulb } from "lucide-react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { IllNoData } from "../components/illustrations";
import { Badge, Callout, EmptyState, ErrorState, PageHeader, Skeleton } from "../components/ui";
import { dateTime, fmtNum, period } from "../format";
import { useT, type Key } from "../i18n";

const ORDER = ["warning", "notice", "info"] as const;

export default function InsightsPage() {
  const { t, td, locale, intl } = useT();
  const { principal } = useAuth();
  const ins = useResource((s) => api<InsightsResponse>("/insights", { signal: s }), [principal.kind]);
  const items = ins.data?.insights ?? [];
  const basisPeriod = (ins.data?.basis as { icp_period?: string } | undefined)?.icp_period;

  return (
    <div className="page">
      <PageHeader eyebrow={t("ins.eyebrow")} icon={Lightbulb} title={t("ins.title")} description={t("ins.desc")} />
      {ins.data && <div className="row small muted"><span>{t("ins.generatedAt", { date: dateTime(ins.data.generated_at, intl) })}</span>{basisPeriod && <span>· {t("ins.basis", { period: period(basisPeriod, locale, true) })}</span>}</div>}
      {ins.error && <ErrorState error={ins.error} onRetry={ins.reload} />}
      {ins.loading && !ins.data ? <Skeleton height={300} /> : !items.length && ins.data ? (
        <div className="panel"><EmptyState illustration={<IllNoData size={160} />} title={t("ins.empty")}>{t("ins.emptyBody")}</EmptyState></div>
      ) : (
        ORDER.map((sev) => {
          const group = items.filter((i) => i.severity === sev);
          if (!group.length) return null;
          return (
            <section key={sev} className="stack-sm">
              <h2 className="section-title" style={{ marginBottom: 0 }}>{t(`ins.sev.${sev}` as Key)} <span className="muted">({group.length})</span></h2>
              <div className="grid cols-2">
                {group.map((i) => (
                  <article key={i.id} className={`insight-card ${i.severity}`}>
                    <div className="row"><Badge tone="outline">{t(`ins.kind.${i.kind}` as Key)}</Badge>{i.period && <span className="xs muted">{period(i.period, locale, true)}</span>}<span className="xs muted">· {t("ins.generated")}</span></div>
                    <h3>{i.title}</h3>
                    <p className="small">{i.body}</p>
                    {i.metrics.length > 0 && (
                      <div className="insight-metrics">
                        {i.metrics.map((m) => <span className="m" key={m.label}><span className="l">{m.label}</span><span className="v">{typeof m.value === "number" ? fmtNum(m.value, intl, Number.isInteger(m.value) || Math.abs(m.value) >= 1000 ? 0 : 2) : typeof m.value === "string" ? td("status", m.value, m.value) : "-"} {m.unit}</span></span>)}
                      </div>
                    )}
                    {i.next_step && <div className="small"><strong>{t("ins.nextStep")}:</strong> {i.next_step}</div>}
                    <div className="xs muted row" style={{ gap: 6 }}><Database size={12} aria-hidden="true" />{t("ins.sources")}: {i.sources.map((s) => s.label).join("; ") || t("common.none")}</div>
                  </article>
                ))}
              </div>
            </section>
          );
        })
      )}
      {ins.data && <Callout tone="info" action={principal.kind === "public" ? <Link className="btn sm primary" to="/masuk?next=%2Finsight">{t("shell.signin")} <ArrowRight size={14} aria-hidden="true" /></Link> : undefined}>{ins.data.disclaimer}{principal.kind === "public" && ` ${t("ins.teaser")}`}</Callout>}
    </div>
  );
}
