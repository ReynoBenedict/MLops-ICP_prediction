/**
 * Operator: customer organisations. Plan and status are two separate actions, each confirmed in a
 * dialog with a required reason (audited server-side). Plan change requests sent by customers are
 * listed for approval or decline; approving changes the plan on the server (audited).
 */
import type { OrganizationDetail, OrganizationList } from "../../api/types";
import { Building2, Inbox, UserX } from "lucide-react";
import { useState } from "react";
import { api, ApiError } from "../../api/client";
import { useResource } from "../../api/hooks";
import type { PlanChangeRequest } from "../../api/product-types";
import { useToast } from "../../components/toast";
import "../../i18n/ops"; // operator strings, registered when this lazy chunk loads
import { Badge, Button, ConfirmDialog, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, StatusBadge, TableScroll } from "../../components/ui";
import { dateTime } from "../../format";
import { useT, type Key } from "../../i18n";
import "../../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../../styles/ops.css"; // page styles, loaded with this lazy chunk (not by the public site)

type Org = { id: string; name: string; slug: string; plan: string; status: string; created_at: string; members?: number; active_members?: number };
const PLANS = ["trial", "starter", "professional", "enterprise"] as const;
type Edit = { org: Org; kind: "plan" | "status"; value: string; reason: string };
type Resolve = { req: PlanChangeRequest; decision: "approve" | "decline"; note: string };

type DeletionReq = { id: string; org_id: string; org_name: string | null; scope: "account" | "organization"; user_name: string | null;
  requested_by: string; requested_by_name: string | null; reason: string | null; status: string; created_at: string };

/**
 * Deletion requests from customers (ux6/API-v6.md §3). Executing anonymises the account or the whole
 * organisation (no hard delete; audit, usage and official data are kept); declining keeps everything.
 * Both are confirmed with a dialog and audited server-side.
 */
function DeletionQueue({ onChanged, quiet }: { onChanged: () => void; quiet?: boolean }) {
  const { t, intl } = useT();
  const { show } = useToast();
  const list = useResource((s) => api<{ items: DeletionReq[] }>("/admin/deletion-requests", { query: { status: "pending" }, signal: s }), []);
  const unsupported = list.error instanceof ApiError && [404, 405].includes(list.error.status);
  const [act, setAct] = useState<{ req: DeletionReq; decision: "execute" | "decline"; note: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const items = list.data?.items ?? [];
  if (unsupported) return null;
  const apply = async () => {
    if (!act) return;
    setBusy(true); setError(null);
    try {
      await api("/admin/deletion-requests/" + act.req.id + "/resolve", { method: "POST", json: { decision: act.decision, note: act.note.trim() || null } });
      show(t(act.decision === "execute" ? "ops.tenants.del.executed" : "ops.tenants.del.declined"), "good");
      setAct(null); list.reload(); onChanged();
    } catch (e) { setError(e); } finally { setBusy(false); }
  };
  const who = (r: DeletionReq) => r.scope === "organization" ? t("ops.tenants.del.scopeOrg") : t("ops.tenants.del.scopeAccount", { name: r.user_name ?? "-" });
  return (
    <Panel title={t("ops.tenants.del.title")} icon={UserX} subtitle={t("ops.tenants.del.sub")} flush>
      {list.error ? <div className="panel-body">{quiet ? <p className="small muted">{t("ops.notLoaded")}</p> : <ErrorState error={list.error} onRetry={list.reload} compact />}</div>
        : !list.data ? <div className="panel-body"><Skeleton height={60} /></div>
        : !items.length ? <div className="panel-body"><p className="small muted">{t("ops.tenants.del.none")}</p></div> : (
        <TableScroll label={t("ops.tenants.del.title")}>
          <table className="table rc">
            <thead><tr><th>{t("acct.orgName")}</th><th>{t("ops.tenants.del.scope")}</th><th>{t("ops.tenants.req.note")}</th><th>{t("ops.tenants.req.when")}</th><th><span className="sr-only">{t("common.actions")}</span></th></tr></thead>
            <tbody>{items.map((r) => (
              <tr key={r.id}>
                <td className="primary-cell rc-title">{r.org_name ?? r.org_id}<span className="sub">{r.requested_by_name ?? r.requested_by}</span></td>
                <td data-label={t("ops.tenants.del.scope")}>{who(r)}</td>
                <td data-label={t("ops.tenants.req.note")} className="small">{r.reason || <span className="muted">{t("common.none")}</span>}</td>
                <td data-label={t("ops.tenants.req.when")}>{dateTime(r.created_at, intl)}</td>
                <td className="num rc-full"><div className="row row-end">
                  <Button size="sm" variant="danger" onClick={() => { setError(null); setAct({ req: r, decision: "execute", note: "" }); }}>{t("ops.tenants.del.execute")}</Button>
                  <Button size="sm" onClick={() => { setError(null); setAct({ req: r, decision: "decline", note: "" }); }}>{t("ops.tenants.req.decline")}</Button>
                </div></td>
              </tr>
            ))}</tbody>
          </table>
        </TableScroll>
      )}
      <ConfirmDialog open={!!act} busy={busy} danger={act?.decision === "execute"}
        title={act ? t(act.decision === "execute" ? "ops.tenants.del.executeTitle" : "ops.tenants.del.declineTitle", { what: who(act.req), name: act.req.org_name ?? act.req.org_id }) : ""}
        confirmLabel={act?.decision === "execute" ? t("ops.tenants.del.execute") : t("ops.tenants.req.decline")}
        onCancel={() => setAct(null)} onConfirm={() => void apply()}>
        {act && (
          <div className="stack-sm dialog-form">
            <p className="small">{act.decision === "execute" ? (act.req.scope === "organization" ? t("ops.tenants.del.executeOrgBody") : t("ops.tenants.del.executeAccountBody")) : t("ops.tenants.del.declineBody")}</p>
            <label className="field"><span className="field-label">{t("ops.tenants.req.resolutionNote")} <span className="muted">({t("common.optional")})</span></span>
              <input className="input" value={act.note} maxLength={500} onChange={(e) => setAct({ ...act, note: e.target.value })} /></label>
            <ErrorState error={error} compact />
          </div>
        )}
      </ConfirmDialog>
    </Panel>
  );
}

export default function TenantsPage() {
  const { t, intl } = useT();
  const { show } = useToast();
  const [cursor, setCursor] = useState<string | null>(null);
  const orgs = useResource((s) => api<OrganizationList>("/admin/organizations", { query: { limit: 25, cursor }, signal: s }), [cursor]);
  const requests = useResource((s) => api<{ items: PlanChangeRequest[]; next_cursor?: string | null }>("/admin/plan-requests", { query: { status: "pending", limit: 50 }, signal: s }), []);
  const requestsUnsupported = requests.error instanceof ApiError && [404, 405].includes(requests.error.status);
  const [edit, setEdit] = useState<Edit | null>(null);
  const [resolve, setResolve] = useState<Resolve | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [formError, setFormError] = useState<string | null>(null);
  const items = (orgs.data?.items ?? []) as unknown as Org[];
  const planLabel = (p: string) => t(`plan.${p}` as Key);
  const orgName = (id: string) => items.find((o) => o.id === id)?.name ?? id;

  const applyEdit = async () => {
    if (!edit) return;
    if (edit.reason.trim().length < 5) { setFormError(t("ops.tenants.reasonShort")); return; }
    setBusy(true); setError(null); setFormError(null);
    try {
      const body = edit.kind === "plan" ? { plan: edit.value, reason: edit.reason.trim() } : { status: edit.value, reason: edit.reason.trim() };
      const r = await api<OrganizationDetail>(`/admin/organizations/${edit.org.id}/${edit.kind}`, { method: "POST", json: body });
      show(t("ops.tenants.saved", { name: r.name, plan: planLabel(r.plan), status: t(`status.${r.status}` as Key) }), "good");
      setEdit(null); orgs.reload();
    } catch (e) { setError(e); } finally { setBusy(false); }
  };
  const applyResolve = async () => {
    if (!resolve) return;
    setBusy(true); setError(null);
    try {
      await api(`/admin/plan-requests/${resolve.req.id}/resolve`, { method: "POST", json: { decision: resolve.decision, note: resolve.note.trim() || null } });
      show(t(resolve.decision === "approve" ? "ops.tenants.req.approved" : "ops.tenants.req.declined", { name: resolve.req.org_name ?? orgName(resolve.req.org_id) }), "good");
      setResolve(null); requests.reload(); orgs.reload();
    } catch (e) { setError(e); } finally { setBusy(false); }
  };
  const openEdit = (org: Org, kind: "plan" | "status") => { setError(null); setFormError(null); setEdit({ org, kind, value: kind === "plan" ? org.plan : org.status === "active" ? "suspended" : "active", reason: "" }); };
  const reqList = requests.data?.items ?? [];
  const down = !!orgs.error && !!requests.error && !requestsUnsupported;

  return (
    <div className="page">
      <PageHeader eyebrow={t("nav.ops")} icon={Building2} title={t("ops.tenants.title")} description={t("ops.tenants.desc")} />
      {/* Service down: one explanation and one retry instead of an error in every panel. */}
      {down && <ErrorState error={orgs.error} onRetry={() => { orgs.reload(); requests.reload(); }} />}

      <Panel title={t("ops.tenants.req.title")} icon={Inbox} subtitle={t("ops.tenants.req.sub")} flush>
        {requestsUnsupported ? <div className="panel-body"><p className="small muted">{t("ops.tenants.req.unsupported")}</p></div>
          : requests.error ? <div className="panel-body">{down ? <p className="small muted">{t("ops.notLoaded")}</p> : <ErrorState error={requests.error} onRetry={requests.reload} compact />}</div>
          : !requests.data ? <div className="panel-body"><Skeleton height={80} /></div>
          : !reqList.length ? <div className="panel-body"><p className="small muted">{t("ops.tenants.req.none")}</p></div> : (
          <TableScroll label={t("ops.tenants.req.title")}>
            <table className="table rc">
              <thead><tr><th>{t("acct.orgName")}</th><th>{t("ops.tenants.req.change")}</th><th>{t("ops.tenants.req.note")}</th><th>{t("ops.tenants.req.when")}</th><th><span className="sr-only">{t("common.actions")}</span></th></tr></thead>
              <tbody>{reqList.map((r) => (
                <tr key={r.id}>
                  <td className="primary-cell rc-title">{r.org_name ?? orgName(r.org_id)}<span className="sub">{r.requested_by_name ?? r.requested_by}</span></td>
                  <td data-label={t("ops.tenants.req.change")}>{planLabel(r.previous_plan)} → <strong>{planLabel(r.requested_plan)}</strong></td>
                  <td data-label={t("ops.tenants.req.note")} className="small">{r.note || <span className="muted">{t("common.none")}</span>}</td>
                  <td data-label={t("ops.tenants.req.when")}>{dateTime(r.created_at, intl)}</td>
                  <td className="num rc-full"><div className="row row-end">
                    <Button size="sm" variant="primary" onClick={() => { setError(null); setResolve({ req: r, decision: "approve", note: "" }); }}>{t("ops.tenants.req.approve")}</Button>
                    <Button size="sm" onClick={() => { setError(null); setResolve({ req: r, decision: "decline", note: "" }); }}>{t("ops.tenants.req.decline")}</Button>
                  </div></td>
                </tr>
              ))}</tbody>
            </table>
          </TableScroll>
        )}
      </Panel>

      <DeletionQueue onChanged={orgs.reload} quiet={down} />

      <Panel flush title={t("nav.ops.tenants")} icon={Building2}>
        {orgs.error ? <div className="panel-body">{down ? <p className="small muted">{t("ops.notLoaded")}</p> : <ErrorState error={orgs.error} onRetry={orgs.reload} />}</div> : !orgs.data ? <div className="panel-body"><Skeleton height={140} /></div> : !items.length ? <EmptyState icon={Building2} title={t("ops.tenants.none")} /> : (
          <TableScroll label={t("nav.ops.tenants")}>
            <table className="table rc">
              <thead><tr><th>{t("acct.orgName")}</th><th>{t("ops.tenants.plan")}</th><th>{t("common.status")}</th>{items.some((o) => typeof o.active_members === "number") && <th className="num">{t("acct.members")}</th>}<th className="hide-md">{t("common.created")}</th><th><span className="sr-only">{t("common.actions")}</span></th></tr></thead>
              <tbody>{items.map((o) => (
                <tr key={o.id}>
                  <td className="primary-cell rc-title">{o.name}<span className="sub mono">{o.slug}</span></td>
                  <td data-label={t("ops.tenants.plan")}><Badge tone="brand">{planLabel(o.plan)}</Badge></td>
                  <td data-label={t("common.status")}><StatusBadge status={o.status} /></td>
                  {items.some((x) => typeof x.active_members === "number") && <td className="num" data-label={t("acct.members")}>{o.active_members ?? "-"}</td>}
                  <td className="hide-md" data-label={t("common.created")}>{dateTime(o.created_at, intl)}</td>
                  <td className="num rc-full"><div className="row row-end">
                    <Button size="sm" onClick={() => openEdit(o, "plan")} aria-label={`${t("ops.tenants.setPlan")}: ${o.name}`}>{t("ops.tenants.setPlan")}</Button>
                    <Button size="sm" variant={o.status === "active" ? "danger" : "secondary"} onClick={() => openEdit(o, "status")} aria-label={`${t("ops.tenants.setStatus")}: ${o.name}`}>{t("ops.tenants.setStatus")}</Button>
                  </div></td>
                </tr>
              ))}</tbody>
            </table>
          </TableScroll>
        )}
        <Pager hasPrev={!!cursor} hasNext={!!orgs.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(orgs.data?.next_cursor ?? null)} />
      </Panel>

      <ConfirmDialog open={!!edit} busy={busy} danger={edit?.kind === "status" && edit.value === "suspended"}
        title={edit ? t(edit.kind === "plan" ? "ops.tenants.planTitle" : "ops.tenants.statusTitle", { name: edit.org.name }) : ""}
        confirmLabel={edit?.kind === "plan" ? t("ops.tenants.setPlan") : t("ops.tenants.setStatus")}
        onCancel={() => setEdit(null)} onConfirm={() => void applyEdit()}>
        {edit && (
          <div className="stack-sm dialog-form">
            {edit.kind === "plan" ? (
              <label className="field"><span className="field-label">{t("ops.tenants.plan")}</span>
                <select className="select" value={edit.value} onChange={(e) => setEdit({ ...edit, value: e.target.value })}>{PLANS.map((p) => <option key={p} value={p}>{planLabel(p)}</option>)}</select></label>
            ) : (
              <label className="field"><span className="field-label">{t("common.status")}</span>
                <select className="select" value={edit.value} onChange={(e) => setEdit({ ...edit, value: e.target.value })}><option value="active">{t("status.active")}</option><option value="suspended">{t("status.suspended")}</option></select></label>
            )}
            <label className="field"><span className="field-label">{t("ops.models.reason")}</span>
              <input className="input" value={edit.reason} onChange={(e) => setEdit({ ...edit, reason: e.target.value })} minLength={5} maxLength={500} /></label>
            {formError && <span className="field-error" role="alert">{formError}</span>}
            <ErrorState error={error} compact />
          </div>
        )}
      </ConfirmDialog>

      <ConfirmDialog open={!!resolve} busy={busy} danger={resolve?.decision === "decline"}
        title={resolve ? t(resolve.decision === "approve" ? "ops.tenants.req.approveTitle" : "ops.tenants.req.declineTitle", { name: resolve.req.org_name ?? orgName(resolve.req.org_id), plan: planLabel(resolve.req.requested_plan) }) : ""}
        confirmLabel={resolve?.decision === "approve" ? t("ops.tenants.req.approve") : t("ops.tenants.req.decline")}
        onCancel={() => setResolve(null)} onConfirm={() => void applyResolve()}>
        {resolve && (
          <div className="stack-sm dialog-form">
            <p>{t(resolve.decision === "approve" ? "ops.tenants.req.approveBody" : "ops.tenants.req.declineBody")}</p>
            <label className="field"><span className="field-label">{t("ops.tenants.req.resolutionNote")} <span className="muted">({t("common.optional")})</span></span>
              <input className="input" value={resolve.note} maxLength={500} onChange={(e) => setResolve({ ...resolve, note: e.target.value })} /></label>
            <ErrorState error={error} compact />
          </div>
        )}
      </ConfirmDialog>
    </div>
  );
}
