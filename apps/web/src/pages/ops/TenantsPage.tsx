import type { OrganizationDetail, OrganizationList } from "../../api/types";
import { Building2 } from "lucide-react";
import { useState } from "react";
import { api } from "../../api/client";
import { useResource } from "../../api/hooks";
import { Badge, Button, Callout, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, StatusBadge } from "../../components/ui";
import { dateTime } from "../../format";
import { useT } from "../../i18n";

type Org = { id: string; name: string; slug: string; plan: string; status: string; created_at: string };

export default function TenantsPage() {
  const { t, intl } = useT();
  const [cursor, setCursor] = useState<string | null>(null);
  const orgs = useResource((s) => api<OrganizationList>("/admin/organizations", { query: { limit: 25, cursor }, signal: s }), [cursor]);
  const [edit, setEdit] = useState<{ id: string; plan: string; status: string; reason: string } | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [result, setResult] = useState<OrganizationDetail | null>(null);
  const items = (orgs.data?.items ?? []) as unknown as Org[];
  const apply = async (kind: "plan" | "status") => {
    if (!edit) return;
    setBusy(kind); setError(null);
    try {
      const body = kind === "plan" ? { plan: edit.plan, reason: edit.reason } : { status: edit.status, reason: edit.reason };
      setResult(await api<OrganizationDetail>(`/admin/organizations/${edit.id}/${kind}`, { method: "POST", json: body }));
      orgs.reload();
    } catch (e) { setError(e); } finally { setBusy(null); }
  };
  return (
    <div className="page">
      <PageHeader eyebrow={t("nav.ops")} icon={Building2} title={t("ops.tenants.title")} description={t("ops.tenants.desc")} />
      {edit && (
        <Panel title={items.find((o) => o.id === edit.id)?.name ?? edit.id} icon={Building2}>
          <div className="row" style={{ alignItems: "flex-end" }}>
            <label className="field"><span className="field-label">{t("shell.plan", { plan: "" }).trim()}</span><select className="select" value={edit.plan} onChange={(e) => setEdit({ ...edit, plan: e.target.value })}>{["trial", "starter", "professional", "enterprise"].map((p) => <option key={p}>{p}</option>)}</select></label>
            <label className="field"><span className="field-label">{t("common.status")}</span><select className="select" value={edit.status} onChange={(e) => setEdit({ ...edit, status: e.target.value })}><option value="active">active</option><option value="suspended">suspended</option></select></label>
            <label className="field grow"><span className="field-label">{t("ops.models.reason")}</span><input className="input" value={edit.reason} onChange={(e) => setEdit({ ...edit, reason: e.target.value })} minLength={5} /></label>
            <Button variant="primary" loading={busy === "plan"} disabled={edit.reason.trim().length < 5} onClick={() => void apply("plan")}>{t("ops.tenants.setPlan")}</Button>
            <Button variant="danger" loading={busy === "status"} disabled={edit.reason.trim().length < 5} onClick={() => void apply("status")}>{t("ops.tenants.setStatus")}</Button>
            <Button variant="ghost" onClick={() => setEdit(null)}>{t("common.close")}</Button>
          </div>
          <ErrorState error={error} compact />
          {result && <Callout tone="good">{result.name}: {result.plan} · {result.status} · {result.active_members} {t("acct.members").toLowerCase()}</Callout>}
        </Panel>
      )}
      <Panel flush title={t("nav.ops.tenants")} icon={Building2}>
        {orgs.error ? <div className="panel-body"><ErrorState error={orgs.error} onRetry={orgs.reload} /></div> : !orgs.data ? <div className="panel-body"><Skeleton height={140} /></div> : !items.length ? <EmptyState icon={Building2} title={t("ops.tenants.none")} /> : (
          <div className="table-scroll"><table className="table">
            <thead><tr><th>{t("acct.orgName")}</th><th className="hide-sm">{t("ops.tenants.slug")}</th><th>{t("shell.plan", { plan: "" }).trim()}</th><th>{t("common.status")}</th><th className="hide-md">{t("common.created")}</th><th /></tr></thead>
            <tbody>{items.map((o) => (
              <tr key={o.id}><td className="primary-cell">{o.name}<span className="sub mono">{o.id}</span></td><td className="hide-sm mono small">{o.slug}</td><td><Badge tone="brand">{o.plan}</Badge></td><td><StatusBadge status={o.status} /></td>
                <td className="hide-md">{dateTime(o.created_at, intl)}</td><td className="num"><Button size="sm" onClick={() => { setResult(null); setEdit({ id: o.id, plan: o.plan, status: o.status, reason: "" }); }}>{t("ops.tenants.setPlan")}</Button></td></tr>
            ))}</tbody></table></div>
        )}
        <Pager hasPrev={!!cursor} hasNext={!!orgs.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(orgs.data?.next_cursor ?? null)} />
      </Panel>
    </div>
  );
}
