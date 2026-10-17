import type { Job, JobList } from "../../api/types";
import { Activity, Ban, Beaker, Bell, ChevronDown, ChevronRight, Database, Fuel, Landmark, Play, RefreshCw, RotateCcw, Send, Workflow, type LucideIcon } from "lucide-react";
import { Fragment, useState, type ReactNode } from "react";
import { api } from "../../api/client";
import { useResource } from "../../api/hooks";
import { Button, Callout, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, StatusBadge } from "../../components/ui";
import { dateTime } from "../../format";
import { useT, type Key } from "../../i18n";

function duration(start: string | null, end: string | null): string {
  if (!start) return "-";
  const ms = (end ? new Date(end) : new Date()).getTime() - new Date(start).getTime();
  if (!Number.isFinite(ms) || ms < 0) return "-";
  const s = Math.round(ms / 1000);
  return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`;
}

function JobOutcome({ j }: { j: Job }) {
  const { t } = useT();
  const r = j.result ?? {};
  if (j.error_code) return <span className="small"><strong className="down-text">{j.error_code}</strong> <span className="muted">{j.error_message}</span></span>;
  if (j.job_type === "train" && j.result) {
    const promo = r.promotion as { promoted: boolean } | undefined;
    return <span className="small">{t("ops.jobs.registeredV", { v: String(r.registered_version), best: String(r.best) })}{promo ? ` · ${promo.promoted ? t("ops.jobs.promotedYes") : t("ops.jobs.promotedNo")}` : ""}</span>;
  }
  if (j.job_type.startsWith("ingest_") && j.result) {
    const parts = Object.entries(r).filter(([k, v]) => !["system_forecast", "alerts"].includes(k) && v && typeof v === "object").map(([k, v]) => { const o = v as { committed?: boolean; added?: unknown[]; changed?: unknown[] }; return `${k}: ${o.committed ? "+" : "="}${o.added?.length ?? 0}/${o.changed?.length ?? 0}`; });
    return <span className="small">{parts.join(" · ") || "-"}</span>;
  }
  if (j.job_type === "refresh_forecast" && j.result) return <span className="small mono">{String(r.forecast_id)} · {String(r.method)}</span>;
  return <span className="small muted">{j.status === "running" ? t("ops.jobs.inProgress") : "-"}</span>;
}

export default function JobsPage() {
  const { t, intl } = useT();
  const [autoPromote, setAutoPromote] = useState(false);
  const [newsPages, setNewsPages] = useState(6);
  const [fullBackfill, setFullBackfill] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<string | null>(null);
  const jobs = useResource((s) => api<JobList>("/jobs", { query: { limit: 15, cursor }, signal: s }), [cursor], 4000);
  const items = jobs.data?.items ?? [];
  const activeByType = new Map(items.filter((j) => j.status === "queued" || j.status === "running").map((j) => [j.job_type, j]));
  const run = async (key: string, fn: () => Promise<unknown>) => { setError(null); setSubmitting(key); try { await fn(); jobs.reload(); } catch (e) { setError(e); } finally { setSubmitting(null); } };
  const submit = (job_type: string, params: Record<string, unknown>) => run(job_type, () => api<Job>("/jobs", { method: "POST", json: { job_type, params } }));
  const card = (type: string, Icon: LucideIcon, title: string, desc: string, params: Record<string, unknown>, children?: ReactNode) => {
    const activeJob = activeByType.get(type);
    return (
      <div key={type} className="health-tile" style={{ gap: 10, padding: 18 }}>
        <div className="row-between"><span className="health-name" style={{ textTransform: "none" }}><span className="kpi-icon brand"><Icon size={16} /></span>{title}</span>{activeJob && <StatusBadge status={activeJob.status} />}</div>
        <p className="small muted">{desc}</p>{children}
        <div><Button variant="primary" icon={Play} loading={submitting === type} disabled={!!activeJob || !!submitting} onClick={() => submit(type, params)}>{activeJob ? t("ops.jobs.alreadyRunning") : t("ops.jobs.start")}</Button></div>
      </div>
    );
  };
  return (
    <div className="page">
      <PageHeader eyebrow={t("nav.ops")} icon={Workflow} title={t("ops.jobs.title")} description={t("ops.jobs.desc")} />
      <Panel title={t("ops.jobs.controls")} icon={Workflow} subtitle={t("ops.jobs.controlsSub")}>
        <div className="health-grid" style={{ gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))" }}>
          {card("train", Beaker, t("ops.jobs.train"), t("ops.jobs.trainBody"), { auto_promote: autoPromote },
            <label className="check"><input type="checkbox" checked={autoPromote} onChange={(e) => setAutoPromote(e.target.checked)} /><span>{t("ops.jobs.autoPromote")}</span></label>)}
          {card("ingest_benchmarks", Fuel, t("ops.jobs.benchmarks"), t("ops.jobs.benchmarksBody"), {})}
          {card("ingest_fx", Landmark, t("ops.jobs.fx"), t("ops.jobs.fxBody"), {})}
          {card("ingest_icp", Database, t("ops.jobs.icp"), t("ops.jobs.icpBody"), { news_pages: newsPages, full_backfill: fullBackfill },
            <div className="row"><label className="field" style={{ width: 120 }}><span className="field-label xs">{t("ops.jobs.newsPages")}</span><input className="input" type="number" min={1} max={120} value={newsPages} onChange={(e) => setNewsPages(Number(e.target.value))} /></label>
              <label className="check"><input type="checkbox" checked={fullBackfill} onChange={(e) => setFullBackfill(e.target.checked)} /><span>{t("ops.jobs.fullBackfill")}</span></label></div>)}
          {card("refresh_forecast", Send, t("ops.jobs.refresh"), t("ops.jobs.refreshBody"), { horizon: 6 })}
          {card("evaluate_alerts", Bell, t("ops.jobs.alerts"), t("ops.jobs.alertsBody"), {})}
        </div>
        {error ? <div className="mt-3"><ErrorState error={error} /></div> : null}
      </Panel>
      <Panel title={t("ops.jobs.list")} icon={Activity} subtitle={t("ops.jobs.listSub")} flush actions={<Button size="sm" variant="ghost" icon={RefreshCw} onClick={jobs.reload}>{t("common.refresh")}</Button>}>
        {jobs.error ? <div className="panel-body"><ErrorState error={jobs.error} onRetry={jobs.reload} /></div> : !jobs.data ? <div className="panel-body"><Skeleton height={180} /></div> : !items.length ? <EmptyState icon={Workflow} title={t("ops.jobs.none")} /> : (
          <div className="table-scroll"><table className="table">
            <thead><tr><th style={{ width: 36 }} aria-label={t("common.details")} /><th>{t("ops.jobs.list")}</th><th>{t("common.status")}</th><th className="hide-sm">{t("ops.jobs.attempt")}</th><th className="hide-md">{t("ops.jobs.started")}</th><th className="num hide-sm">{t("ops.jobs.duration")}</th><th className="hide-md">{t("ops.jobs.outcome")}</th><th /></tr></thead>
            <tbody>{items.map((j) => (
              <Fragment key={j.id}>
                <tr className="clickable" onClick={() => setOpen(open === j.id ? null : j.id)}>
                  <td>{open === j.id ? <ChevronDown size={16} /> : <ChevronRight size={16} />}</td>
                  <td className="primary-cell">{t(`ops.jobs.type.${j.job_type}` as Key)}<span className="sub">{t("common.by")} {j.requested_by}{j.retry_of ? ` · ${t("ops.jobs.retry").toLowerCase()}` : ""}</span></td>
                  <td><StatusBadge status={j.status} />{j.cancel_requested && j.status === "running" && <span className="sub">{t("ops.jobs.cancelling")}</span>}</td>
                  <td className="hide-sm">{j.attempt}/{j.max_attempts}</td><td className="hide-md">{dateTime(j.started_at ?? j.created_at, intl)}</td>
                  <td className="num hide-sm">{duration(j.started_at, j.finished_at)}</td><td className="hide-md" style={{ maxWidth: 380 }}><JobOutcome j={j} /></td>
                  <td className="num" onClick={(e) => e.stopPropagation()}>
                    {(j.status === "queued" || j.status === "running") && <Button size="sm" variant="danger" icon={Ban} disabled={j.cancel_requested} onClick={() => run(`cancel-${j.id}`, () => api(`/jobs/${j.id}/cancel`, { method: "POST" }))}>{t("ops.jobs.cancelBtn")}</Button>}
                    {(j.status === "failed" || j.status === "cancelled") && <Button size="sm" icon={RotateCcw} onClick={() => run(`retry-${j.id}`, () => api(`/jobs/${j.id}/retry`, { method: "POST" }))}>{t("ops.jobs.retry")}</Button>}
                  </td>
                </tr>
                {open === j.id && (
                  <tr><td colSpan={8} style={{ background: "var(--surface-sunken)" }}>
                    <div className="grid cols-2" style={{ gap: 16 }}>
                      <dl className="dl small"><dt>ID</dt><dd className="mono">{j.id}</dd><dt>{t("common.created")}</dt><dd>{dateTime(j.created_at, intl)}</dd><dt>{t("ops.jobs.finished")}</dt><dd>{dateTime(j.finished_at, intl)}</dd>
                        <dt>{t("ops.jobs.timeout")}</dt><dd>{Math.round(j.timeout_seconds / 60)} min</dd><dt>{t("ops.jobs.params")}</dt><dd className="mono">{JSON.stringify(j.params)}</dd>{j.retry_of && <><dt>{t("ops.jobs.retryOf")}</dt><dd className="mono">{j.retry_of}</dd></>}</dl>
                      <div className="stack-sm">{j.error_code ? <Callout tone="bad" title={j.error_code}>{j.error_message}</Callout> : <JobOutcome j={j} />}
                        {j.result && <pre className="mono xs" style={{ margin: 0, maxHeight: 220, overflow: "auto", background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 8, padding: 10 }}>{JSON.stringify(j.result, null, 2)}</pre>}</div>
                    </div>
                  </td></tr>
                )}
              </Fragment>
            ))}</tbody>
          </table></div>
        )}
        <Pager hasPrev={!!cursor} hasNext={!!jobs.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(jobs.data?.next_cursor ?? null)} />
      </Panel>
    </div>
  );
}
