import type { AuditList } from "../../api/types";
import { ScrollText } from "lucide-react";
import { useState } from "react";
import { api } from "../../api/client";
import { useResource } from "../../api/hooks";
import { Button, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, StatusBadge } from "../../components/ui";
import { dateTime } from "../../format";
import { useT } from "../../i18n";

export default function AuditPage() {
  const { t, intl } = useT();
  const [cursor, setCursor] = useState<string | null>(null);
  const [action, setAction] = useState("");
  const [applied, setApplied] = useState("");
  const audit = useResource((s) => api<AuditList>("/audit", { query: { limit: 25, cursor, action: applied || undefined }, signal: s }), [cursor, applied]);
  return (
    <div className="page">
      <PageHeader eyebrow={t("nav.ops")} icon={ScrollText} title={t("ops.audit.title")} description={t("ops.audit.desc")} />
      <Panel flush title={t("ops.audit.title")} icon={ScrollText}
        actions={<form className="row" onSubmit={(e) => { e.preventDefault(); setCursor(null); setApplied(action.trim()); }}>
          <input className="input" style={{ height: 32, width: 240 }} placeholder={t("ops.audit.filter")} value={action} onChange={(e) => setAction(e.target.value)} aria-label={t("ops.audit.filter")} />
          <Button size="sm" type="submit">{t("ops.audit.apply")}</Button>
          {applied && <Button size="sm" variant="ghost" type="button" onClick={() => { setAction(""); setApplied(""); setCursor(null); }}>{t("ops.audit.clear")}</Button>}
        </form>}>
        {audit.error ? <div className="panel-body"><ErrorState error={audit.error} /></div> : !audit.data ? <div className="panel-body"><Skeleton height={160} /></div> :
          !audit.data.items.length ? <EmptyState icon={ScrollText} title={applied ? t("ops.audit.noMatch") : t("ops.audit.none")} /> : (
          <div className="table-scroll">
            <table className="table">
              <thead><tr><th>{t("ops.audit.when")}</th><th>{t("ops.audit.action")}</th><th>{t("ops.audit.outcome")}</th><th className="hide-sm">{t("ops.audit.actor")}</th><th className="hide-md">{t("ops.audit.target")}</th><th className="hide-md">{t("ops.audit.request")}</th></tr></thead>
              <tbody>{audit.data.items.map((e) => (
                <tr key={e.id}><td className="nowrap">{dateTime(e.created_at, intl)}</td><td><span className="tag">{e.action}</span></td><td><StatusBadge status={e.outcome} /></td>
                  <td className="hide-sm">{e.actor}</td><td className="hide-md mono small">{e.target ?? "-"}</td><td className="hide-md mono small">{e.request_id ?? "-"}</td></tr>
              ))}</tbody>
            </table>
          </div>
        )}
        <Pager hasPrev={!!cursor} hasNext={!!audit.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(audit.data?.next_cursor ?? null)} />
      </Panel>
    </div>
  );
}
