/**
 * Pemberitahuan: alert rules and the notification inbox (own route, so badge, title and Back
 * behave predictably). Every change (rule added/removed/toggled, rules checked, event read)
 * signals `notifyAlertsChanged()` so the sidebar badge updates at once, and reloads the
 * organisation so the plan counter ("x terpakai") is current. Event texts come from the server
 * in both languages (title_en/body_en when the server provides them).
 */
import type { AlertEvent, AlertEventList, AlertKind, AlertRule, OrganizationDetail } from "../api/types";
import { ArrowRight, Bell, BellPlus, Check, Play, Trash2 } from "lucide-react";
import { useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { api, ApiError } from "../api/client";
import { useResource } from "../api/hooks";
import { labelFor, type AlertEventLocalized, type LabelEn } from "../api/product-types";
import { useAuth } from "../auth";
import { IllAlerts } from "../components/illustrations";
import { Badge, Button, Callout, ConfirmDialog, EmptyState, ErrorState, Field, PageHeader, Pager, Panel, Skeleton } from "../components/ui";
import { dateTime, fmtNum, period } from "../format";
import { useT, type Key } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../styles/flows.css"; // page styles, loaded with this lazy chunk (not by the public site)
import { notifyAlertsChanged } from "../lib/alerts";

const SEVERITY_TONE: Record<string, "warn" | "info" | "bad"> = { critical: "bad", warning: "warn", notice: "info", info: "info" };
type Event = AlertEvent & AlertEventLocalized;
type Rule = AlertRule & LabelEn;
type Kind = AlertKind & LabelEn;

/** Where an event points to: the month in Price History, the forecast issue, or the dataset. */
export function eventLink(e: Event): { to: string; key: Key; vars?: Record<string, string> } | null {
  const l = e.link;
  if (l?.kind === "forecast" && l.forecast_id) return { to: `/proyeksi?proyeksi=${encodeURIComponent(l.forecast_id)}`, key: "alert.link.forecast" };
  const p = l?.period ?? e.period ?? null;
  if (p && (l?.kind === "period" || !l)) return { to: `/harga?bulan=${encodeURIComponent(p)}`, key: "alert.link.period", vars: { period: p } };
  if (l?.kind === "dataset") return { to: "/harga", key: "alert.link.dataset" };
  return null;
}

export function AlertsView({ ent, used, onChanged, entError = null, onRetryEnt }:
  { ent: OrganizationDetail["entitlements"] | null; used: number; onChanged: () => void; entError?: unknown; onRetryEnt?: () => void }) {
  const { t, intl, locale } = useT();
  const { hasRole, isOperator } = useAuth();
  const kinds = useResource((s) => api<Kind[]>("/alerts/kinds", { signal: s }), []);
  const rules = useResource((s) => api<Rule[]>("/alerts/rules", { signal: s }), []);
  const [cursor, setCursor] = useState<string | null>(null);
  const events = useResource((s) => api<AlertEventList>("/alerts/events", { query: { limit: 15, cursor }, signal: s }), [cursor]);
  const [kind, setKind] = useState<AlertRule["kind"]>("icp_published");
  const [params, setParams] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<Rule | null>(null);
  const selectedKind = kinds.data?.find((k) => k.kind === kind);
  const atLimit = !!ent && !isOperator && used >= ent.alert_rules_max;
  const changed = () => { rules.reload(); events.reload(); notifyAlertsChanged(); onChanged(); };
  const act = async (key: string, fn: () => Promise<void>) => { setBusy(key); setError(null); try { await fn(); } catch (e) { setError(e); } finally { setBusy(null); } };

  // Inline validation of the numeric thresholds (empty = the server's default shown as placeholder).
  const paramError = (k: string): string | null => {
    const v = (params[k] ?? "").trim();
    if (!v || k === "benchmark" || k === "dataset") return null;
    const n = Number(v.replace(",", "."));
    if (!Number.isFinite(n)) return t("alert.paramNumber");
    if (n <= 0) return t("alert.paramPositive");
    return null;
  };
  const invalid = Object.keys(params).some((k) => paramError(k) !== null);
  const create = (e: FormEvent) => {
    e.preventDefault();
    if (invalid) return;
    const p: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(params)) {
      if (v === "") continue;
      const n = Number(v.replace(",", "."));
      p[k] = Number.isFinite(n) && k !== "benchmark" && k !== "dataset" ? n : v;
    }
    setNotice(null);
    void act("create", async () => { await api("/alerts/rules", { method: "POST", json: { kind, params: p } }); setNotice(t("alert.created")); setParams({}); changed(); });
  };
  const paramLabel = (k: string) => k === "threshold_pct" ? `${t("alert.threshold")} (${t("alert.unit.pct")})` : k === "threshold_usd" ? `${t("alert.threshold")} (${t("alert.unit.usd")})` : k === "benchmark" ? t("alert.benchmark") : k === "dataset" ? t("alert.dataset") : k;
  const datasetLabel = (d: string) => t(`ds.${d}` as Key);
  const paramValue = (k: string, v: unknown) => k === "dataset" ? datasetLabel(String(v))
    : k === "benchmark" ? String(v).toUpperCase() === "WTI" ? "WTI" : String(v).charAt(0).toUpperCase() + String(v).slice(1)
    : k === "threshold_pct" && typeof v === "number" ? `${fmtNum(v, intl, 1)}%`
    : k === "threshold_usd" && typeof v === "number" ? `USD ${fmtNum(v, intl, 2)}`
    : String(v);
  const severityLabel = (sev: string) => (["info", "notice", "warning", "critical"].includes(sev) ? t(`alert.sev.${sev}` as Key) : t("alert.sev.info"));
  const ruleName = (r: Rule) => (locale === "en" ? r.label_en : null) ?? r.label_id ?? r.kind;
  const title = (e: Event) => (locale === "en" ? e.title_en : null) ?? e.title;
  const body = (e: Event) => (locale === "en" ? e.body_en : null) ?? e.body;

  // When the service is down every panel fails at once: one explanation with one retry, not one per panel.
  const down = !!rules.error && !!events.error;
  const kindsReady = !!kinds.data?.length;
  const kindsFailed = !!kinds.error && !kinds.data;
  const reloadAll = () => { kinds.reload(); rules.reload(); events.reload(); onRetryEnt?.(); };
  // A context title for an error whose body (what happened, what to do) comes from ErrorState.
  const titled = (e: unknown, title: string) => (e instanceof ApiError ? { [e.code]: title } : undefined);

  return (
    <div className="stack">
    {down && <ErrorState error={rules.error} onRetry={reloadAll} />}
    {/* The plan (rule limit) could not be read: say so instead of silently dropping the limit (W2-R4). */}
    {!down && !ent && !isOperator && !!entError && <ErrorState error={entError} onRetry={onRetryEnt} compact titles={titled(entError, t("alert.planUnknownTitle"))} />}
    <div className="grid split-side">
      <Panel title={t("alert.rules")} icon={BellPlus} subtitle={ent ? t("alert.limit", { plan: labelFor(ent, locale), n: ent.alert_rules_max, used }) : undefined}>
        <form onSubmit={create} className="stack-sm">
          <label className="field"><span className="field-label">{t("alert.kind")}</span>
            <select className="select" value={kind} disabled={!kindsReady} onChange={(e) => { setKind(e.target.value as AlertRule["kind"]); setParams({}); }}>
              {(kinds.data ?? []).map((k) => <option key={k.kind} value={k.kind}>{labelFor(k, locale)}</option>)}
            </select></label>
          {/* Without the rule kinds the form cannot be filled in: explain it and offer a retry (W2-R3). */}
          {kindsFailed && !down && (
            <div className="stack-sm">
              <ErrorState error={kinds.error} onRetry={kinds.reload} compact titles={titled(kinds.error, t("alert.kindsError"))} />
              <span className="field-hint">{t("alert.kindsErrorHint")}</span>
            </div>
          )}
          {selectedKind && Object.entries(selectedKind.params).map(([k, def]) => (
            k === "benchmark" || k === "dataset" ? (
              <label key={k} className="field"><span className="field-label">{paramLabel(k)}</span>
                {k === "benchmark" ? <select className="select" value={params[k] ?? String(def)} onChange={(e) => setParams({ ...params, [k]: e.target.value })}><option value="brent">Brent</option><option value="dubai">Dubai</option><option value="wti">WTI</option></select>
                  : <select className="select" value={params[k] ?? String(def)} onChange={(e) => setParams({ ...params, [k]: e.target.value })}>{["icp", "brent", "dubai", "wti", "usd_idr"].map((d) => <option key={d} value={d}>{datasetLabel(d)}</option>)}</select>}
              </label>
            ) : (
              <Field key={k} label={paramLabel(k)} error={paramError(k)}
                hint={typeof def === "number" ? t("alert.paramDefault", { v: fmtNum(def, intl, k === "threshold_pct" ? 1 : 2) }) : undefined}
                render={(p) => <input {...p} className="input" inputMode="decimal" value={params[k] ?? ""} placeholder={typeof def === "number" ? fmtNum(def, intl, k === "threshold_pct" ? 1 : 2) : String(def)} onChange={(e) => setParams({ ...params, [k]: e.target.value })} />} />
            )
          ))}
          <div><Button type="submit" variant="primary" icon={BellPlus} loading={busy === "create"} disabled={!kindsReady || !hasRole("analyst") || (!isOperator && !!ent && !ent.alerts) || atLimit || invalid}>{t("alert.add")}</Button></div>
          {atLimit && <p className="field-hint">{t("alert.atLimit", { n: ent?.alert_rules_max ?? 0 })}</p>}
          {!hasRole("analyst") && <p className="field-hint">{t("alert.viewerNote")}</p>}
          <div role="status" aria-live="polite">{notice && <Callout tone="good">{notice}</Callout>}</div>
          <ErrorState error={error} compact />
        </form>
        <div className="divider mt-3" />
        {rules.loading && !rules.data ? <Skeleton height={80} /> : rules.error ? (down ? <p className="small muted mt-3">{t("alert.notLoaded")}</p> : <ErrorState error={rules.error} onRetry={rules.reload} compact />) : !rules.data?.length ? <p className="small muted mt-3">{t("alert.noRules")}</p> : (
          <ul className="rule-list mt-3">
            {rules.data.map((r) => (
              <li key={r.id}>
                <div className="grow">
                  <strong className="small">{ruleName(r)}</strong>
                  <div className="xs muted">{Object.entries(r.params).map(([k, v]) => `${paramLabel(k)}: ${paramValue(k, v)}`).join(" · ") || t("alert.noParams")}</div>
                  <div className="xs muted">{t("alert.lastTriggered")}: {r.last_triggered_at ? dateTime(r.last_triggered_at, intl) : t("scen.never")}</div>
                </div>
                <div className="rule-actions">
                  <label className="check xs"><input type="checkbox" checked={r.enabled} disabled={!hasRole("analyst")}
                    onChange={(e) => void act(`en-${r.id}`, async () => { await api(`/alerts/rules/${r.id}`, { method: "POST", json: { enabled: e.target.checked } }); changed(); })} />{t("alert.enabled")}</label>
                  <Button size="sm" variant="ghost" icon={Trash2} aria-label={`${t("common.delete")}: ${ruleName(r)}`} disabled={!hasRole("analyst")} loading={busy === `del-${r.id}`} onClick={() => setDeleting(r)} />
                </div>
              </li>
            ))}
          </ul>
        )}
        {/* Checking the rules belongs with the rules; its result lands in the inbox. */}
        <div className="rule-eval">
          <Button size="sm" icon={Play} loading={busy === "eval"} disabled={!hasRole("analyst") || !rules.data?.length} aria-describedby="rule-eval-hint"
            onClick={() => void act("eval", async () => { await api("/alerts/evaluate", { method: "POST" }); changed(); })}>{t("alert.evaluateNow")}</Button>
          <p className="xs muted" id="rule-eval-hint">{t("alert.evaluateHint")}</p>
        </div>
        <p className="xs muted mt-3">{t("alert.inAppOnly")}</p>
      </Panel>
      <Panel title={t("alert.inbox")} icon={Bell} subtitle={events.data ? t("alert.unread", { n: events.data.unread }) : undefined} flush
        actions={<Button size="sm" icon={Check} disabled={!events.data?.unread} loading={busy === "readall"}
          onClick={() => void act("readall", async () => { await api("/alerts/events/read", { method: "POST" }); changed(); })}>{t("alert.markAllRead")}</Button>}>
        {events.error ? <div className="panel-body">{down ? <p className="small muted">{t("alert.notLoaded")}</p> : <ErrorState error={events.error} onRetry={events.reload} />}</div> : !events.data ? <div className="panel-body"><Skeleton height={140} /></div> : !events.data.items.length ? (
          <EmptyState illustration={<IllAlerts size={150} />} title={t("alert.empty")}>{t("alert.emptyBody")}</EmptyState>
        ) : (
          <div className="table-scroll" role="region" tabIndex={0} aria-label={t("alert.inbox")}>
            <table className="table rc inbox">
              <caption className="sr-only">{t("alert.inbox")}</caption>
              <thead><tr><th>{t("alert.detected")}</th><th>{t("alert.message")}</th><th><span className="sr-only">{t("common.actions")}</span></th></tr></thead>
              <tbody>{(events.data.items as Event[]).map((e) => {
                const link = eventLink(e);
                return (
                  <tr key={e.id} className={e.read_at ? "read" : "unread"}>
                    <td className="nowrap rc-title" data-label={t("alert.detected")}>
                      <span>{!e.read_at && <span className="unread-dot" aria-hidden="true" />}{dateTime(e.created_at, intl)}</span>
                      <span className="event-meta">
                        <Badge tone={SEVERITY_TONE[e.severity] ?? "info"}>{severityLabel(e.severity)}</Badge>
                        <span className="xs muted">{e.read_at ? t("alert.read") : t("alert.unreadOne")}</span>
                      </span>
                    </td>
                    <td className="rc-full" data-label="">
                      <strong>{title(e)}</strong><span className="sub">{body(e)}</span>
                      {link && <Link className="small link-inline event-link" to={link.to}>{t(link.key, { period: link.vars?.period ? period(link.vars.period, locale) : "" })} <ArrowRight size={13} aria-hidden="true" /></Link>}
                    </td>
                    <td className="num rc-full" data-label="">{!e.read_at && <Button size="sm" variant="ghost" loading={busy === `rd-${e.id}`}
                      onClick={() => void act(`rd-${e.id}`, async () => { await api("/alerts/events/read", { method: "POST", query: { event_id: e.id } }); changed(); })}>{t("alert.markRead")}</Button>}</td>
                  </tr>
                );
              })}</tbody>
            </table>
          </div>
        )}
        <Pager hasPrev={!!cursor} hasNext={!!events.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(events.data?.next_cursor ?? null)} />
      </Panel>
      <ConfirmDialog open={!!deleting} busy={!!deleting && busy === `del-${deleting.id}`}
        title={t("alert.deleteConfirmTitle", { name: deleting ? ruleName(deleting) : "" })} confirmLabel={t("common.delete")}
        onCancel={() => setDeleting(null)}
        onConfirm={() => {
          const r = deleting;
          if (!r) return;
          void act(`del-${r.id}`, async () => { await api(`/alerts/rules/${r.id}/delete`, { method: "POST" }); changed(); }).finally(() => setDeleting(null));
        }}>
        {t("alert.deleteConfirmBody")}
      </ConfirmDialog>
    </div>
    </div>
  );
}

export default function NotificationsPage() {
  const { t } = useT();
  const { isUser } = useAuth();
  const org = useResource((s) => isUser ? api<OrganizationDetail>("/org", { signal: s }) : Promise.resolve(null), [isUser]);
  return (
    <div className="page">
      <PageHeader eyebrow={t("nav.alerts")} icon={Bell} title={t("alert.pageTitle")} description={t("alert.pageDesc")} />
      <AlertsView ent={org.data?.entitlements ?? null} used={org.data?.alert_rules ?? 0} onChanged={org.reload} entError={org.data ? null : org.error} onRetryEnt={org.reload} />
    </div>
  );
}
