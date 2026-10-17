/** Akun & Organisasi: profile, password, members, API keys, plan & usage. */
import type { ApiKeyCreated, ApiKeyInfo, InviteResponse, MeResponse, OrganizationDetail, UserPublic } from "../api/types";
import { Check, KeyRound, Minus, Settings, ShieldCheck, UserPlus, Users } from "lucide-react";
import { useState, type FormEvent } from "react";
import { api } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { Badge, Button, Callout, ErrorState, GateNotice, PageHeader, Panel, Skeleton, StatusBadge } from "../components/ui";
import { dateTime } from "../format";
import { useT, type Key } from "../i18n";

function useAct() {
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const act = async (key: string, fn: () => Promise<void>) => { setBusy(key); setError(null); try { await fn(); } catch (e) { setError(e); } finally { setBusy(null); } };
  return { busy, error, act, setError };
}

function Profile({ me, reload }: { me: MeResponse; reload: () => void }) {
  const { t, intl } = useT();
  const { refresh, signOut } = useAuth();
  const [name, setName] = useState(me.user.name);
  const [cur, setCur] = useState(""); const [nw, setNw] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const { busy, error, act } = useAct();
  return (
    <div className="grid cols-2" style={{ alignItems: "start" }}>
      <Panel title={t("acct.profile")} icon={Settings}>
        <form className="stack-sm" onSubmit={(e: FormEvent) => { e.preventDefault(); void act("name", async () => { await api("/auth/me", { method: "POST", json: { name } }); setMsg(t("acct.saved")); reload(); await refresh(); }); }}>
          <label className="field"><span className="field-label">{t("auth.fullName")}</span><input className="input" value={name} onChange={(e) => setName(e.target.value)} maxLength={120} required /></label>
          <dl className="dl small"><dt>{t("auth.email")}</dt><dd>{me.user.email}</dd><dt>{t("acct.role")}</dt><dd>{t(`acct.role.${me.user.role}` as Key)}</dd>
            <dt>{t("acct.memberSince")}</dt><dd>{dateTime(me.user.created_at, intl)}</dd><dt>{t("acct.lastLogin")}</dt><dd>{dateTime(me.user.last_login_at, intl)}</dd></dl>
          <div><Button type="submit" variant="primary" loading={busy === "name"}>{t("common.save")}</Button></div>
          {msg && <Callout tone="good">{msg}</Callout>}
        </form>
      </Panel>
      <Panel title={t("acct.security")} icon={ShieldCheck} id="keamanan">
        <form className="stack-sm" onSubmit={(e: FormEvent) => { e.preventDefault(); void act("pw", async () => { await api("/auth/password", { method: "POST", json: { current_password: cur, new_password: nw } }); setMsg(t("acct.passwordChanged")); await signOut(); }); }}>
          {me.user.must_change_password && <Callout tone="warn">{t("shell.mustChangePassword")}</Callout>}
          <label className="field"><span className="field-label">{t("acct.currentPassword")}</span><input className="input" type="password" autoComplete="current-password" value={cur} onChange={(e) => setCur(e.target.value)} required /></label>
          <label className="field"><span className="field-label">{t("acct.newPassword")}</span><input className="input" type="password" autoComplete="new-password" value={nw} onChange={(e) => setNw(e.target.value)} required minLength={10} /><span className="field-hint">{t("auth.passwordHint")}</span></label>
          <div><Button type="submit" variant="primary" loading={busy === "pw"} disabled={nw.length < 10}>{t("acct.changePassword")}</Button></div>
          <ErrorState error={error} compact />
          <div className="section-title mt-3">{t("acct.sessions")}</div>
          <ul className="source-list">{me.sessions.filter((s) => !s.revoked_at).slice(0, 5).map((s) => <li key={s.id}><span className="src-icon"><KeyRound size={13} /></span><span className="xs">{dateTime(s.created_at, intl)} · {s.user_agent?.slice(0, 60) ?? "-"}</span></li>)}</ul>
        </form>
      </Panel>
    </div>
  );
}

function Members({ org, reload }: { org: OrganizationDetail; reload: () => void }) {
  const { t, intl } = useT();
  const { hasRole, principal } = useAuth();
  const members = useResource((s) => api<UserPublic[]>("/org/members", { signal: s }), []);
  const [form, setForm] = useState({ email: "", name: "", role: "analyst" as "admin" | "analyst" | "viewer" });
  const [invited, setInvited] = useState<InviteResponse | null>(null);
  const [temp, setTemp] = useState<{ id: string; pw: string } | null>(null);
  const { busy, error, act } = useAct();
  const canAdmin = hasRole("admin");
  return (
    <Panel title={t("acct.members")} icon={Users} subtitle={t("acct.seatLimit", { plan: org.entitlements.label_id, n: org.entitlements.seats })}>
      <div className="stack">
        {canAdmin && (
          <form className="row" style={{ alignItems: "flex-end" }} onSubmit={(e: FormEvent) => { e.preventDefault(); void act("invite", async () => { setInvited(await api<InviteResponse>("/org/members", { method: "POST", json: form })); setForm({ email: "", name: "", role: "analyst" }); members.reload(); reload(); }); }}>
            <label className="field grow"><span className="field-label">{t("auth.email")}</span><input className="input" type="email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} required /></label>
            <label className="field grow"><span className="field-label">{t("auth.fullName")}</span><input className="input" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required /></label>
            <label className="field"><span className="field-label">{t("acct.role")}</span><select className="select" value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value as typeof form.role })}>
              <option value="viewer">{t("acct.role.viewer")}</option><option value="analyst">{t("acct.role.analyst")}</option>{hasRole("owner") && <option value="admin">{t("acct.role.admin")}</option>}</select></label>
            <Button type="submit" variant="primary" icon={UserPlus} loading={busy === "invite"}>{t("acct.invite")}</Button>
          </form>
        )}
        {invited && <Callout tone="good" title={`${invited.member.email} · ${t("acct.tempPassword")}`}><code className="mono" style={{ fontSize: 15 }}>{invited.temporary_password}</code><div className="small mt-2">{invited.note}</div></Callout>}
        {temp && <Callout tone="good" title={t("acct.tempPassword")}><code className="mono" style={{ fontSize: 15 }}>{temp.pw}</code></Callout>}
        <ErrorState error={error} compact />
        {members.loading && !members.data ? <Skeleton height={120} /> : (
          <div className="table-scroll" style={{ border: "1px solid var(--border)", borderRadius: 10 }}>
            <table className="table">
              <thead><tr><th>{t("auth.fullName")}</th><th>{t("acct.role")}</th><th>{t("common.status")}</th><th className="hide-sm">{t("acct.lastLogin")}</th>{canAdmin && <th />}</tr></thead>
              <tbody>{(members.data ?? []).map((m) => (
                <tr key={m.id}>
                  <td className="primary-cell">{m.name}<span className="sub">{m.email}</span></td>
                  <td>{canAdmin && m.role !== "owner" && m.email !== principal.name ? (
                    <select className="select" style={{ height: 32, width: 150 }} value={m.role} onChange={(e) => void act(`role-${m.id}`, async () => { await api(`/org/members/${m.id}`, { method: "POST", json: { role: e.target.value } }); members.reload(); })}>
                      <option value="viewer">{t("acct.role.viewer")}</option><option value="analyst">{t("acct.role.analyst")}</option>{hasRole("owner") && <option value="admin">{t("acct.role.admin")}</option>}</select>
                  ) : t(`acct.role.${m.role}` as Key)}</td>
                  <td><StatusBadge status={m.status} />{m.must_change_password && <span className="sub">{t("acct.tempPassword").split(" (")[0]}</span>}</td>
                  <td className="hide-sm">{dateTime(m.last_login_at, intl)}</td>
                  {canAdmin && <td className="num"><div className="row" style={{ justifyContent: "flex-end" }}>
                    {m.role !== "owner" && <Button size="sm" variant={m.status === "active" ? "danger" : "secondary"} loading={busy === `st-${m.id}`}
                      onClick={() => void act(`st-${m.id}`, async () => { await api(`/org/members/${m.id}`, { method: "POST", json: { status: m.status === "active" ? "disabled" : "active" } }); members.reload(); reload(); })}>{m.status === "active" ? t("acct.disable") : t("acct.enable")}</Button>}
                    {m.role !== "owner" && <Button size="sm" variant="ghost" loading={busy === `pw-${m.id}`} onClick={() => void act(`pw-${m.id}`, async () => { const r = await api<{ temporary_password: string }>(`/org/members/${m.id}/reset-password`, { method: "POST" }); setTemp({ id: m.id, pw: r.temporary_password }); })}>{t("acct.resetPassword")}</Button>}
                  </div></td>}
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
      </div>
    </Panel>
  );
}

function ApiKeys({ org }: { org: OrganizationDetail }) {
  const { t, intl } = useT();
  const { hasRole } = useAuth();
  const keys = useResource((s) => hasRole("admin") && org.entitlements.api_access ? api<ApiKeyInfo[]>("/org/api-keys", { signal: s }) : Promise.resolve([]), [org.entitlements.api_access]);
  const [name, setName] = useState("");
  const [created, setCreated] = useState<ApiKeyCreated | null>(null);
  const { busy, error, act } = useAct();
  return (
    <Panel title={t("acct.apiKeys")} icon={KeyRound} subtitle={t("acct.apiHow")}>
      {!org.entitlements.api_access ? <Callout tone="info">{t("acct.apiNotInPlan")}</Callout> : !hasRole("admin") ? <Callout tone="info">{t("ops.lockedBody")}</Callout> : (
        <div className="stack">
          <form className="row" style={{ alignItems: "flex-end" }} onSubmit={(e: FormEvent) => { e.preventDefault(); void act("key", async () => { setCreated(await api<ApiKeyCreated>("/org/api-keys", { method: "POST", json: { name } })); setName(""); keys.reload(); }); }}>
            <label className="field grow"><span className="field-label">{t("acct.keyName")}</span><input className="input" value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} /></label>
            <Button type="submit" variant="primary" icon={KeyRound} loading={busy === "key"}>{t("acct.newKey")}</Button>
          </form>
          {created && <Callout tone="good" title={created.name}><code className="mono" style={{ fontSize: 14, overflowWrap: "anywhere" }}>{created.key}</code><div className="small mt-2">{created.note}</div></Callout>}
          <ErrorState error={error} compact />
          <div className="table-scroll" style={{ border: "1px solid var(--border)", borderRadius: 10 }}>
            <table className="table"><thead><tr><th>{t("acct.keyName")}</th><th>{t("common.created")}</th><th>{t("acct.lastUsed")}</th><th /></tr></thead>
              <tbody>{(keys.data ?? []).map((k) => (
                <tr key={k.id}><td className="primary-cell">{k.name}<span className="sub mono">{k.prefix}…</span></td><td>{dateTime(k.created_at, intl)}</td><td>{dateTime(k.last_used_at, intl)}</td>
                  <td className="num">{k.revoked_at ? <Badge tone="neutral">{t("acct.revoked")}</Badge> : <Button size="sm" variant="danger" loading={busy === `rv-${k.id}`} onClick={() => void act(`rv-${k.id}`, async () => { await api(`/org/api-keys/${k.id}/revoke`, { method: "POST" }); keys.reload(); })}>{t("acct.revoke")}</Button>}</td></tr>
              ))}</tbody></table>
          </div>
        </div>
      )}
    </Panel>
  );
}

function PlanPanel({ org, reload }: { org: OrganizationDetail; reload: () => void }) {
  const { t } = useT();
  const { hasRole } = useAuth();
  const [name, setName] = useState(org.name);
  const { busy, error, act } = useAct();
  const ent = org.entitlements;
  const feats: Array<[boolean, string]> = [
    [true, t("landing.plan.seats", { n: ent.seats })], [true, t("landing.plan.horizon", { n: ent.forecast_horizon_max })],
    [ent.scenarios, t("landing.plan.scenarios", { n: ent.saved_scenarios_max })], [ent.reports_pdf, t("landing.plan.reportsPdf")],
    [ent.reports_excel, t("landing.plan.reportsExcel")], [ent.alerts, t("landing.plan.alerts", { n: ent.alert_rules_max })],
    [ent.api_access, ent.api_access ? t("landing.plan.api", { n: ent.api_requests_per_month }) : t("landing.plan.noApi")],
  ];
  const usage = org.usage_this_month;
  return (
    <Panel title={t("acct.plan")} icon={ShieldCheck} id="paket">
      <div className="grid cols-3" style={{ alignItems: "start" }}>
        <div className="stack-sm">
          <form className="stack-sm" onSubmit={(e: FormEvent) => { e.preventDefault(); void act("rename", async () => { await api("/org", { method: "POST", json: { name } }); reload(); }); }}>
            <label className="field"><span className="field-label">{t("acct.orgName")}</span><input className="input" value={name} onChange={(e) => setName(e.target.value)} disabled={!hasRole("admin")} /></label>
            {hasRole("admin") && <div><Button type="submit" size="sm" loading={busy === "rename"}>{t("acct.rename")}</Button></div>}
          </form>
          <div className="row"><Badge tone="brand">{ent.label_id}</Badge><StatusBadge status={org.status} /></div>
          <p className="small muted">{ent.price_note_id}. {t("acct.planChange")}</p>
          <ErrorState error={error} compact />
        </div>
        <div><div className="section-title">{t("acct.entitlements")}</div>
          <ul className="plan" style={{ border: "none", padding: 0 }}>{feats.map(([on, l], i) => <li key={i} className={on ? "" : "off"}>{on ? <Check size={15} /> : <Minus size={15} />}{l}</li>)}</ul></div>
        <div><div className="section-title">{t("acct.usage")}</div>
          <dl className="dl small">{["forecast.run", "scenario.run", "scenario.compare", "report.generate", "api.request"].map((k) => <span key={k} style={{ display: "contents" }}><dt>{t(`acct.usage.${k}` as Key)}</dt><dd>{usage[k] ?? 0}</dd></span>)}</dl>
          <div className="xs muted mt-2">{t("acct.members")}: {org.active_members}/{ent.seats} · {t("nav.scenarios")}: {org.saved_scenarios}/{ent.saved_scenarios_max} · {t("alert.rules")}: {org.alert_rules}/{ent.alert_rules_max}</div></div>
      </div>
      <div className="plan-grid mt-4">
        {org.plans.map((p) => <div key={p.code} className={`plan${p.code === org.plan ? " current" : ""}`}><h3>{p.label_id}</h3><div className="price-note">{p.price_note_id}</div>
          <ul><li><Check size={14} />{t("landing.plan.seats", { n: p.seats })}</li><li><Check size={14} />{t("landing.plan.horizon", { n: p.forecast_horizon_max })}</li><li className={p.api_access ? "" : "off"}>{p.api_access ? <Check size={14} /> : <Minus size={14} />}{p.api_access ? t("landing.plan.api", { n: p.api_requests_per_month }) : t("landing.plan.noApi")}</li></ul></div>)}
      </div>
    </Panel>
  );
}

export default function AccountPage() {
  const { t } = useT();
  const { isUser, isOperator } = useAuth();
  const me = useResource((s) => isUser ? api<MeResponse>("/auth/me", { signal: s }) : Promise.resolve(null), [isUser]);
  const org = useResource((s) => isUser ? api<OrganizationDetail>("/org", { signal: s }) : Promise.resolve(null), [isUser]);
  if (!isUser) {
    return <div className="page"><PageHeader eyebrow={t("acct.eyebrow")} icon={Settings} title={t("acct.title")} description={t("acct.desc")} />
      {isOperator ? <Callout tone="info">{t("acct.youOperator")}</Callout> : <GateNotice title={t("common.signinRequired")} signin />}</div>;
  }
  return (
    <div className="page">
      <PageHeader eyebrow={t("acct.eyebrow")} icon={Settings} title={t("acct.title")} description={t("acct.desc")} />
      {(me.error || org.error) && <ErrorState error={me.error ?? org.error} onRetry={() => { me.reload(); org.reload(); }} />}
      {!me.data || !org.data ? ((me.loading || org.loading) && !me.error && !org.error ? <Skeleton height={300} /> : null) : (<>
        <Profile me={me.data} reload={me.reload} />
        <PlanPanel org={org.data} reload={org.reload} />
        <Members org={org.data} reload={org.reload} />
        <ApiKeys org={org.data} />
      </>)}
    </div>
  );
}
