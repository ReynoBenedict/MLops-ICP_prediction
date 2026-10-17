import type { SystemStatus } from "../../api/types";
import { Beaker, CircleCheck, CircleX, Database, HardDrive, RefreshCw, Server, ShieldCheck, TriangleAlert, Workflow, type LucideIcon } from "lucide-react";
import { api } from "../../api/client";
import { useResource } from "../../api/hooks";
import { Badge, Button, ErrorState, PageHeader, Panel, Skeleton, Stat, StatusBadge, statusTone } from "../../components/ui";
import { useT } from "../../i18n";

const DEP_ICON: Record<string, LucideIcon> = {
  metadata_store: Database, object_store: HardDrive, model_registry: Beaker, production_model: Beaker, datasets: Database, job_runner: Workflow,
};

export default function HealthPage() {
  const { t } = useT();
  const status = useResource((s) => api<SystemStatus>("/system/status", { signal: s }), [], 15000);
  const st = status.data;
  const tone = st ? statusTone(st.status) : "neutral";
  const HeroIcon = tone === "good" ? CircleCheck : tone === "bad" ? CircleX : TriangleAlert;
  const issues = st?.checks.filter((c) => c.status !== "ok" && c.status !== "not_configured") ?? [];
  return (
    <div className="page">
      <PageHeader eyebrow={t("nav.ops")} icon={Server} title={t("ops.health.title")} description={t("ops.health.desc")}
        actions={<Button icon={RefreshCw} onClick={status.reload} loading={status.loading && !!st}>{t("ops.health.recheck")}</Button>} />
      {status.error ? <ErrorState error={status.error} onRetry={status.reload} /> : (
        <section className="panel"><div className="panel-body" style={{ paddingTop: 20 }}>
          {!st ? <Skeleton height={90} /> : (
            <div className="grid" style={{ gridTemplateColumns: "minmax(260px, 1fr) minmax(0, 2fr)", alignItems: "center" }}>
              <div className="hero-status">
                <span className={`hero-icon ${tone === "neutral" ? "warn" : tone}`}><HeroIcon size={26} aria-hidden="true" /></span>
                <div><div className="small muted">{t("ops.health.overall")}</div>
                  <div style={{ fontSize: 22, fontWeight: 700 }}>{st.status === "ready" ? t("ops.health.allOk") : t(`status.${st.status}`)}</div>
                  <div className="small muted">{issues.length ? t("ops.health.needAttention", { n: issues.length }) : t("ops.health.allPassed")}</div></div>
              </div>
              <div className="stat-grid">
                <Stat label={t("ops.health.env")} value={st.environment} /><Stat label={t("ops.health.api")} value={st.version} />
                <Stat label={t("ops.health.metadata")} value={st.metadata_backend} hint={t("ops.health.schema", { v: st.schema_version ?? "?", exp: st.expected_schema_version })} />
                <Stat label={t("ops.health.objects")} value={st.object_store_backend} />
                <Stat label={t("ops.health.modelSource")} value={st.model_source === "bundle" ? t("ops.health.modelBundle") : t("ops.health.modelMlflow")} />
                <Stat label={t("ops.health.admin")} value={st.admin_enabled ? t("ops.health.enabled") : t("ops.health.disabledV")} />
              </div>
            </div>
          )}
        </div></section>
      )}
      <Panel title={t("ops.health.deps")} icon={ShieldCheck} subtitle={t("ops.health.depsSub")}>
        {!st ? <Skeleton height={120} /> : (
          <div className="health-grid">
            {st.checks.map((c) => { const Icon = DEP_ICON[c.name] ?? Server; return (
              <div key={c.name} className={`health-tile${c.status === "down" ? " down" : c.status === "degraded" ? " degraded" : ""}`}>
                <span className="health-name"><Icon size={16} aria-hidden="true" />{c.name.replaceAll("_", " ")}</span>
                <div className="row"><StatusBadge status={c.status} />{c.critical ? <Badge tone="outline">{t("ops.health.critical")}</Badge> : <Badge>{t("ops.health.nonCritical")}</Badge>}</div>
                <div className="health-detail">{c.detail ?? "-"}</div>
                {c.latency_ms != null && <div className="xs muted">{t("ops.health.checkedIn", { ms: c.latency_ms })}</div>}
              </div>); })}
          </div>
        )}
      </Panel>
    </div>
  );
}
