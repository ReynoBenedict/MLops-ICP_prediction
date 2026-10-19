/**
 * Laporan & Pemberitahuan: generate/download branded reports; alert rules and the notification inbox.
 * Deep links: `/laporan?tab=pemberitahuan`, `/laporan?jenis=perbandingan&skenario=<id,id>` (from the
 * scenario page) preselect the tab, the report kind and the scenarios.
 */
import type { AlertEventList, AlertKind, AlertRule, ForecastRunList, OrganizationDetail, ReportList, ReportRun, ScenarioList } from "../api/types";
import { Bell, BellPlus, Check, Download, FileText, Play, Trash2 } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent } from "react";
import { useSearchParams } from "react-router-dom";
import { api, downloadWithAuth } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { IllAlerts, IllReports } from "../components/illustrations";
import { useToast } from "../components/toast";
import { Badge, Button, Callout, ConfirmDialog, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, StatusBadge, Tabs, scrollToResult } from "../components/ui";
import { dateOnly, dateTime, period } from "../format";
import { useT, type Key } from "../i18n";

type Kind = "market_monthly" | "forecast" | "scenario_comparison" | "data_provenance";
type Fmt = "pdf" | "xlsx" | "csv";
const KIND_PARAM: Record<string, Kind> = { bulanan: "market_monthly", proyeksi: "forecast", perbandingan: "scenario_comparison", provenans: "data_provenance" };
const SEVERITY_TONE: Record<string, "warn" | "info" | "bad"> = { critical: "bad", warning: "warn", notice: "info", info: "info" };

function ReportsTab({ ent, initialKind, initialScenarios }: { ent: OrganizationDetail["entitlements"] | null; initialKind: Kind | null; initialScenarios: string[] }) {
  const { t, locale, intl } = useT();
  const { isOperator, hasRole } = useAuth();
  const { show } = useToast();
  const [kind, setKind] = useState<Kind>(initialKind === "scenario_comparison" && !hasRole("analyst") ? "market_monthly" : initialKind ?? "market_monthly");
  const [formats, setFormats] = useState<Fmt[]>(["pdf", "xlsx", "csv"]);
  const [forecastId, setForecastId] = useState("");
  const [scenarioIds, setScenarioIds] = useState<string[]>(initialScenarios);
  const [cursor, setCursor] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [created, setCreated] = useState<ReportRun | null>(null);
  const createdRef = useRef<HTMLDivElement>(null);
  const list = useResource((s) => api<ReportList>("/reports", { query: { limit: 10, cursor }, signal: s }), [cursor]);
  const scenarios = useResource((s) => kind === "scenario_comparison" && !isOperator ? api<ScenarioList>("/scenarios", { query: { limit: 50 }, signal: s }) : Promise.resolve(null), [kind, isOperator]);
  const forecasts = useResource((s) => kind === "forecast" ? api<ForecastRunList>("/forecasts", { query: { limit: 10 }, signal: s }) : Promise.resolve(null), [kind]);
  const allowed = (f: Fmt) => isOperator || !ent || (f === "pdf" ? ent.reports_pdf : f === "xlsx" ? ent.reports_excel : true);

  useEffect(() => {
    if (!created) return;
    scrollToResult(createdRef.current, "nearest");
    createdRef.current?.querySelector<HTMLElement>("button, a")?.focus();
  }, [created]);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (busy) return; // one report at a time
    setBusy(true); setError(null); setCreated(null);
    try {
      const params: Record<string, unknown> = {};
      if (kind === "forecast" && forecastId) params.forecast_id = forecastId;
      if (kind === "scenario_comparison") params.scenario_ids = scenarioIds;
      const r = await api<ReportRun>("/reports", { method: "POST", json: { kind, params, formats: formats.filter(allowed) } });
      setCreated(r); list.reload();
      if (r.status === "succeeded") show(t("toast.reportReady"), "good");
    } catch (err) { setError(err); } finally { setBusy(false); }
  };
  const [dlBusy, setDlBusy] = useState<string | null>(null);
  const dl = async (r: ReportRun, f: string) => {
    const key = `${r.id}:${f}`;
    if (dlBusy) return;
    setDlBusy(key); setError(null);
    try { await downloadWithAuth(`/reports/${r.id}/download`, { format: f }, `icp-intelligence_${r.kind}_${r.created_at.slice(0, 10)}.${f}`); }
    catch (err) { setError(err); }
    finally { setDlBusy(null); }
  };
  // Only formats included in the plan are offered (platform reports may carry more formats).
  const downloads = (r: ReportRun) => <div className="row">{r.formats.filter((f) => allowed(f as Fmt)).map((f) => <Button key={f} size="sm" icon={Download} loading={dlBusy === `${r.id}:${f}`} disabled={!!dlBusy && dlBusy !== `${r.id}:${f}`} onClick={() => void dl(r, f)}>{f.toUpperCase()}</Button>)}</div>;

  return (
    <div className="layout-form-list">
      <Panel title={t("rep.generate")} icon={FileText}>
        <form onSubmit={submit} className="stack">
          <div className="stack-sm">
            <span className="field-label">{t("rep.kind")}</span>
            {(["market_monthly", "forecast", "scenario_comparison", "data_provenance"] as Kind[]).map((k) => (
              <label key={k} className="check"><input type="radio" name="kind" checked={kind === k} onChange={() => setKind(k)} disabled={k === "scenario_comparison" && !hasRole("analyst")} />
                <span><strong>{t(`rep.kind.${k}` as Key)}</strong><br /><span className="small muted">{k === "scenario_comparison" && !hasRole("analyst") ? t("rep.viewerComparison") : t(`rep.kind.${k}.help` as Key)}</span></span></label>
            ))}
          </div>
          {kind === "forecast" && (
            <label className="field"><span className="field-label">{t("rep.forecastPick")}</span>
              <select className="select" value={forecastId} onChange={(e) => setForecastId(e.target.value)}>
                <option value="">{t("rep.forecastNew")}</option>
                {(forecasts.data?.items ?? []).filter((r) => r.status === "succeeded").map((r) => (
                  <option key={r.id} value={r.id}>{t("rep.forecastOption", { date: dateOnly(r.issue_date ?? r.created_at, intl), origin: period(r.forecast_origin ?? r.last_observed_period, locale), n: r.horizon })}</option>
                ))}
              </select>
              <span className="field-hint">{t("rep.forecastPickHelp")}</span></label>
          )}
          {kind === "scenario_comparison" && (
            <div className="field"><span className="field-label">{t("rep.scenarioIds")}</span>
              {initialScenarios.length > 0 && <span className="field-hint">{t("rep.scenarioPreselected")}</span>}
              {scenarios.loading && !scenarios.data ? <Skeleton height={60} /> : scenarios.data?.items.length ? scenarios.data.items.map((s) => (
                <label key={s.id} className="check"><input type="checkbox" checked={scenarioIds.includes(s.id)} onChange={(e) => setScenarioIds(e.target.checked ? [...scenarioIds, s.id].slice(0, 4) : scenarioIds.filter((x) => x !== s.id))} /><span>{s.name}</span></label>
              )) : <span className="small muted">{t("scen.empty")}</span>}
            </div>
          )}
          <div className="stack-sm">
            <span className="field-label">{t("rep.formats")}</span>
            <div className="row" style={{ gap: 16 }}>
              {(["pdf", "xlsx", "csv"] as Fmt[]).map((f) => (
                <label key={f} className="check"><input type="checkbox" checked={formats.includes(f) && allowed(f)} disabled={!allowed(f)}
                  onChange={(e) => setFormats(e.target.checked ? [...formats, f] : formats.filter((x) => x !== f))} />
                  <span>{f.toUpperCase()} {!allowed(f) && <Badge tone="warn">{t("rep.formatNotInPlan")}</Badge>}</span></label>
              ))}
            </div>
          </div>
          <ErrorState error={error} />
          <div><Button type="submit" variant="primary" icon={Play} loading={busy} disabled={!hasRole("viewer") || formats.filter(allowed).length === 0 || (kind === "scenario_comparison" && !scenarioIds.length)}>{t("rep.generate")}</Button></div>
          {created && (
            <div ref={createdRef}>
              {created.status === "succeeded"
                ? <Callout tone="good" title={t("rep.generated")}>{t("rep.generatedBody", { kind: t(`rep.kind.${created.kind}` as Key), date: dateTime(created.created_at, intl) })}<div className="mt-3">{downloads(created)}</div></Callout>
                : <Callout tone="bad" title={t("error.generic")}>{t("rep.failedBody")}</Callout>}
            </div>
          )}
        </form>
      </Panel>
      <Panel title={t("rep.list")} icon={Download} flush>
        {list.error ? <div className="panel-body"><ErrorState error={list.error} onRetry={list.reload} /></div> : !list.data ? <div className="panel-body"><Skeleton height={140} /></div> : !list.data.items.length ? (
          <EmptyState illustration={<IllReports size={150} />} title={t("rep.empty")}>{t("rep.emptyBody")}</EmptyState>
        ) : (
          <div className="table-scroll">
            <table className="table">
              <thead><tr><th>{t("rep.kind")}</th><th>{t("common.created")}</th><th>{t("common.status")}</th><th>{t("rep.formats")}</th></tr></thead>
              <tbody>{list.data.items.map((r) => (
                <tr key={r.id} className={created?.id === r.id ? "selected" : ""}>
                  <td className="primary-cell">{t(`rep.kind.${r.kind}` as Key)}{r.org_id ? null : <span className="sub">{t("rep.platform")}</span>}</td>
                  <td>{dateTime(r.created_at, intl)}<span className="sub">{r.created_by.startsWith("user:") ? t("rep.byMember") : r.org_id ? r.created_by : t("rep.byOperator")}</span></td>
                  <td><StatusBadge status={r.status} />{r.status === "failed" && <span className="sub">{t("rep.failedBody")}</span>}</td>
                  <td>{r.status === "succeeded" ? downloads(r) : null}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
        <Pager hasPrev={!!cursor} hasNext={!!list.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(list.data?.next_cursor ?? null)} />
      </Panel>
    </div>
  );
}

function AlertsTab({ ent, used }: { ent: OrganizationDetail["entitlements"] | null; used: number }) {
  const { t, intl } = useT();
  const { hasRole, isOperator } = useAuth();
  const kinds = useResource((s) => api<AlertKind[]>("/alerts/kinds", { signal: s }), []);
  const rules = useResource((s) => api<AlertRule[]>("/alerts/rules", { signal: s }), []);
  const [cursor, setCursor] = useState<string | null>(null);
  const events = useResource((s) => api<AlertEventList>("/alerts/events", { query: { limit: 15, cursor }, signal: s }), [cursor]);
  const [kind, setKind] = useState<AlertRule["kind"]>("icp_published");
  const [params, setParams] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const selectedKind = kinds.data?.find((k) => k.kind === kind);
  const act = async (key: string, fn: () => Promise<void>) => { setBusy(key); setError(null); try { await fn(); } catch (e) { setError(e); } finally { setBusy(null); } };
  const [deleting, setDeleting] = useState<AlertRule | null>(null);
  const reloadAll = () => { rules.reload(); events.reload(); };

  const create = (e: FormEvent) => {
    e.preventDefault();
    const p: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(params)) if (v !== "") p[k] = Number.isFinite(Number(v)) && k !== "benchmark" && k !== "dataset" ? Number(v) : v;
    void act("create", async () => { await api("/alerts/rules", { method: "POST", json: { kind, params: p } }); setNotice(t("alert.created")); setParams({}); reloadAll(); });
  };
  const paramLabel = (k: string) => k === "threshold_pct" ? `${t("alert.threshold")} (${t("alert.unit.pct")})` : k === "threshold_usd" ? `${t("alert.threshold")} (${t("alert.unit.usd")})` : k === "benchmark" ? t("alert.benchmark") : k === "dataset" ? t("alert.dataset") : k;
  const datasetLabel = (d: string) => t(`ds.${d}` as Key);
  const severityLabel = (sev: string) => (["info", "notice", "warning", "critical"].includes(sev) ? t(`alert.sev.${sev}` as Key) : t("alert.sev.info"));

  return (
    <div className="grid split-side">
      <div className="stack">
        <Panel title={t("alert.rules")} icon={BellPlus} subtitle={ent ? t("alert.limit", { plan: ent.label_id, n: ent.alert_rules_max, used }) : undefined}>
          <form onSubmit={create} className="stack-sm">
            <label className="field"><span className="field-label">{t("alert.kind")}</span>
              <select className="select" value={kind} onChange={(e) => { setKind(e.target.value as AlertRule["kind"]); setParams({}); }}>
                {(kinds.data ?? []).map((k) => <option key={k.kind} value={k.kind}>{k.label_id}</option>)}
              </select></label>
            {selectedKind && Object.entries(selectedKind.params).map(([k, def]) => (
              <label key={k} className="field"><span className="field-label">{paramLabel(k)}</span>
                {k === "benchmark" ? <select className="select" value={params[k] ?? String(def)} onChange={(e) => setParams({ ...params, [k]: e.target.value })}><option value="brent">Brent</option><option value="dubai">Dubai</option><option value="wti">WTI</option></select>
                  : k === "dataset" ? <select className="select" value={params[k] ?? String(def)} onChange={(e) => setParams({ ...params, [k]: e.target.value })}>{["icp", "brent", "dubai", "wti", "usd_idr"].map((d) => <option key={d} value={d}>{datasetLabel(d)}</option>)}</select>
                  : <input className="input" inputMode="decimal" value={params[k] ?? ""} placeholder={String(def)} onChange={(e) => setParams({ ...params, [k]: e.target.value })} />}
              </label>
            ))}
            <div><Button type="submit" variant="primary" icon={BellPlus} loading={busy === "create"} disabled={!hasRole("analyst") || (!isOperator && !!ent && !ent.alerts)}>{t("alert.add")}</Button></div>
            {!hasRole("analyst") && <p className="field-hint">{t("alert.viewerNote")}</p>}
            {notice && <Callout tone="good">{notice}</Callout>}
            <ErrorState error={error} compact />
          </form>
          <div className="divider mt-3" />
          {rules.loading && !rules.data ? <Skeleton height={80} /> : !rules.data?.length ? <p className="small muted mt-3">{t("alert.noRules")}</p> : (
            <div className="stack-sm mt-3">
              {rules.data.map((r) => (
                <div key={r.id} className="row-between" style={{ borderBottom: "1px solid var(--border)", paddingBottom: 8 }}>
                  <div><strong className="small">{r.label_id ?? r.kind}</strong><div className="xs muted">{Object.entries(r.params).map(([k, v]) => `${paramLabel(k)}: ${k === "dataset" ? datasetLabel(String(v)) : String(v)}`).join(" · ") || t("common.none")}</div>
                    <div className="xs muted">{t("alert.lastTriggered")}: {r.last_triggered_at ? dateTime(r.last_triggered_at, intl) : t("scen.never")}</div></div>
                  <div className="row" style={{ flexWrap: "nowrap" }}>
                    <label className="check xs"><input type="checkbox" checked={r.enabled} disabled={!hasRole("analyst")} onChange={(e) => void act(`en-${r.id}`, async () => { await api(`/alerts/rules/${r.id}`, { method: "POST", json: { enabled: e.target.checked } }); rules.reload(); })} />{t("alert.enabled")}</label>
                    <Button size="sm" variant="ghost" icon={Trash2} aria-label={`${t("common.delete")}: ${r.label_id ?? r.kind}`} disabled={!hasRole("analyst")} loading={busy === `del-${r.id}`} onClick={() => setDeleting(r)} />
                  </div>
                </div>
              ))}
            </div>
          )}
          <p className="xs muted mt-3">{t("alert.inAppOnly")}</p>
        </Panel>
      </div>
      <Panel title={t("alert.inbox")} icon={Bell} subtitle={events.data ? t("alert.unread", { n: events.data.unread }) : undefined} flush
        actions={<>
          <Button size="sm" variant="ghost" icon={Play} loading={busy === "eval"} disabled={!hasRole("analyst")} onClick={() => void act("eval", async () => { await api("/alerts/evaluate", { method: "POST" }); reloadAll(); })}>{t("alert.evaluateNow")}</Button>
          <Button size="sm" icon={Check} disabled={!events.data?.unread} onClick={() => void act("readall", async () => { await api("/alerts/events/read", { method: "POST" }); reloadAll(); })}>{t("alert.markAllRead")}</Button>
        </>}>
        {events.error ? <div className="panel-body"><ErrorState error={events.error} onRetry={events.reload} /></div> : !events.data ? <div className="panel-body"><Skeleton height={140} /></div> : !events.data.items.length ? (
          <EmptyState illustration={<IllAlerts size={150} />} title={t("alert.empty")}>{t("alert.emptyBody")}</EmptyState>
        ) : (
          <div className="table-scroll">
            <table className="table">
              <thead><tr><th>{t("ops.audit.when")}</th><th>{t("alert.inbox")}</th><th /></tr></thead>
              <tbody>{events.data.items.map((e) => (
                <tr key={e.id} className={e.read_at ? "read" : ""}>
                  <td className="nowrap">{dateTime(e.created_at, intl)}<span className="sub"><Badge tone={SEVERITY_TONE[e.severity] ?? "info"}>{severityLabel(e.severity)}</Badge></span></td>
                  <td><strong>{e.title}</strong><span className="sub">{e.body}</span></td>
                  <td className="num">{!e.read_at && <Button size="sm" variant="ghost" onClick={() => void act(`rd-${e.id}`, async () => { await api("/alerts/events/read", { method: "POST", query: { event_id: e.id } }); reloadAll(); })}>{t("alert.markRead")}</Button>}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
        <Pager hasPrev={!!cursor} hasNext={!!events.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(events.data?.next_cursor ?? null)} />
      </Panel>
      <ConfirmDialog open={!!deleting} busy={!!deleting && busy === `del-${deleting.id}`}
        title={t("alert.deleteConfirmTitle", { name: deleting?.label_id ?? deleting?.kind ?? "" })} confirmLabel={t("common.delete")}
        onCancel={() => setDeleting(null)}
        onConfirm={() => {
          const r = deleting;
          if (!r) return;
          void act(`del-${r.id}`, async () => { await api(`/alerts/rules/${r.id}/delete`, { method: "POST" }); reloadAll(); }).finally(() => setDeleting(null));
        }}>
        {t("alert.deleteConfirmBody")}
      </ConfirmDialog>
    </div>
  );
}

export default function ReportsPage() {
  const { t } = useT();
  const { isUser, isOperator } = useAuth();
  const [params, setParams] = useSearchParams();
  const tab: "reports" | "alerts" = params.get("tab") === "pemberitahuan" ? "alerts" : "reports";
  const setTab = (v: "reports" | "alerts") => { const next = new URLSearchParams(params); if (v === "alerts") next.set("tab", "pemberitahuan"); else next.delete("tab"); setParams(next, { replace: true }); };
  const initialKind = KIND_PARAM[params.get("jenis") ?? ""] ?? null;
  const initialScenarios = (params.get("skenario") ?? "").split(",").map((s) => s.trim()).filter(Boolean).slice(0, 4);
  // Customer-only route (the router keeps guests and operators out).
  const org = useResource((s) => isUser ? api<OrganizationDetail>("/org", { signal: s }) : Promise.resolve(null), [isUser]);
  return (
    <div className="page">
      <PageHeader eyebrow={t("rep.eyebrow")} icon={FileText} title={t("rep.title")} description={t("rep.desc")} />
      <Tabs label={t("rep.eyebrow")} value={tab} onChange={setTab} tabs={[{ value: "reports", label: t("rep.tabReports"), icon: FileText }, { value: "alerts", label: t("rep.tabAlerts"), icon: Bell }]} />
      {tab === "reports" ? <ReportsTab ent={org.data?.entitlements ?? null} initialKind={initialKind} initialScenarios={initialScenarios} /> : isOperator ? <Callout tone="info">{t("acct.youOperator")}</Callout> : <AlertsTab ent={org.data?.entitlements ?? null} used={org.data?.alert_rules ?? 0} />}
    </div>
  );
}
