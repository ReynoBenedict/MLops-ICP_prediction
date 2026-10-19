/** Akun & Organisasi: profile, password, members, API keys, plan & usage. */
import type { ApiKeyCreated, ApiKeyInfo, InviteResponse, MeResponse, OrganizationDetail, UserPublic } from "../api/types";
import { Check, KeyRound, Minus, Settings, ShieldCheck, UserPlus, Users } from "lucide-react";
import { useEffect, useState, type FormEvent } from "react";
import { useLocation } from "react-router-dom";
import { api } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { useToast } from "../components/toast";
import { Badge, Button, Callout, ErrorState, PageHeader, Panel, Skeleton, StatusBadge } from "../components/ui";
import { dateTime, fmtNum } from "../format";
import { useT, type Key } from "../i18n";

/** Readable device label from a user agent ("Chrome di Windows"); scripts and unknown agents get a plain label. */
export function deviceLabel(ua: string | null | undefined, t: ReturnType<typeof useT>["t"]): string {
  if (!ua) return t("acct.device.unknown");
  if (/python|curl|httpx|urllib|okhttp|node-fetch|axios|postman|wget|go-http/i.test(ua)) return t("acct.device.script");
  const browser = /Edg\//.test(ua) ? "Edge" : /OPR\//.test(ua) ? "Opera" : /Firefox\//.test(ua) ? "Firefox" : /Chrome\//.test(ua) ? "Chrome" : /Safari\//.test(ua) ? "Safari" : null;
  const os = /Windows/.test(ua) ? "Windows" : /Android/.test(ua) ? "Android" : /iPhone|iPad|iOS/.test(ua) ? "iOS" : /Mac OS X|Macintosh/.test(ua) ? "macOS" : /Linux/.test(ua) ? "Linux" : null;
  if (browser && os) return t("acct.device.on", { browser, os });
  return browser ?? os ?? t("acct.device.unknown");
}

function useAct() {
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const act = async (key: string, fn: () => Promise<void>) => { setBusy(key); setError(null); try { await fn(); } catch (e) { setError(e); } finally { setBusy(null); } };
  return { busy, error, act, setError };
}

function Profile({ me, reload }: { me: MeResponse; reload: () => void }) {
  const { t, intl } = useT();
  const { refresh, signOut, principal } = useAuth();
  const { show } = useToast();
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
        <form className="stack-sm" onSubmit={(e: FormEvent) => { e.preventDefault(); void act("pw", async () => { await api("/auth/password", { method: "POST", json: { current_password: cur, new_password: nw } }); show(t("acct.passwordChanged"), "good"); await signOut(); }); }}>
          {(me.user.must_change_password || principal.mustChangePassword) && <Callout tone="warn">{t("shell.mustChangePassword")}</Callout>}
          <label className="field"><span className="field-label">{t("acct.currentPassword")}</span><input className="input" type="password" autoComplete="current-password" value={cur} onChange={(e) => setCur(e.target.value)} required /></label>
          <label className="field"><span className="field-label">{t("acct.newPassword")}</span><input className="input" type="password" autoComplete="new-password" value={nw} onChange={(e) => setNw(e.target.value)} required minLength={10} /><span className="field-hint">{t("auth.passwordHint")}</span></label>
          <div><Button type="submit" variant="primary" loading={busy === "pw"} disabled={nw.length < 10}>{t("acct.changePassword")}</Button></div>
          <ErrorState error={error} compact />
          <div className="section-title mt-3">{t("acct.sessions")}</div>
          <ul className="source-list">{me.sessions.filter((s) => !s.revoked_at).slice(0, 5).map((s) => <li key={s.id}><span className="src-icon"><KeyRound size={13} aria-hidden="true" /></span><span className="xs">{deviceLabel(s.user_agent, t)} · {dateTime(s.last_seen_at ?? s.created_at, intl)}</span></li>)}</ul>
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
          <form className="row invite-form" style={{ alignItems: "flex-end" }} onSubmit={(e: FormEvent) => { e.preventDefault(); void act("invite", async () => { setInvited(await api<InviteResponse>("/org/members", { method: "POST", json: form })); setForm({ email: "", name: "", role: "analyst" }); members.reload(); reload(); }); }}>
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
      {!org.entitlements.api_access ? <Callout tone="info">{t("acct.apiNotInPlan")}</Callout> : !hasRole("admin") ? <Callout tone="info">{t("acct.apiAdminOnly")}</Callout> : (
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
  const { t, intl } = useT();
  const { hasRole } = useAuth();
  const [name, setName] = useState(org.name);
  const { busy, error, act } = useAct();
  const ent = org.entitlements;
  const feats: Array<[boolean, string]> = [
    [true, t("landing.plan.seats", { n: ent.seats })], [true, t("landing.plan.horizon", { n: ent.forecast_horizon_max })],
    [ent.scenarios, t("landing.plan.scenarios", { n: ent.saved_scenarios_max })], [ent.reports_pdf, t("landing.plan.reportsPdf")],
    [ent.reports_excel, t("landing.plan.reportsExcel")], [ent.alerts, t("landing.plan.alerts", { n: ent.alert_rules_max })],
    [ent.api_access, ent.api_access ? t("landing.plan.api", { n: fmtNum(ent.api_requests_per_month, intl, 0) }) : t("landing.plan.noApi")],
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
          <p className="small muted">{ent.price_note_id}.</p>
          <ErrorState error={error} compact />
        </div>
        <div><div className="section-title">{t("acct.entitlements")}</div>
          <ul className="check-list">{feats.map(([on, l], i) => <li key={i} className={on ? "" : "off"}>{on ? <Check size={15} aria-hidden="true" /> : <Minus size={15} aria-hidden="true" />}{l}</li>)}</ul></div>
        <div><div className="section-title">{t("acct.usage")}</div>
          <dl className="dl small">{["forecast.run", "scenario.run", "scenario.compare", "report.generate", "api.request"].map((k) => <span key={k} style={{ display: "contents" }}><dt>{t(`acct.usage.${k}` as Key)}</dt><dd>{usage[k] ?? 0}</dd></span>)}</dl>
          <div className="xs muted mt-2">{t("acct.members")}: {org.active_members}/{ent.seats} · {t("nav.scenarios")}: {org.saved_scenarios}/{ent.saved_scenarios_max} · {t("alert.rules")}: {org.alert_rules}/{ent.alert_rules_max}</div></div>
      </div>
      <Callout tone="info" title={t("acct.upgradeTitle")}>{t("acct.upgradeBody")}</Callout>
      <div className="plan-grid mt-4">
        {org.plans.map((p) => <div key={p.code} className={`plan${p.code === org.plan ? " current" : ""}`}><h3>{p.label_id}{p.code === org.plan && <> <Badge tone="brand">{t("acct.currentPlan")}</Badge></>}</h3><div className="price-note">{p.price_note_id}</div>
          <ul><li><Check size={14} aria-hidden="true" />{t("landing.plan.seats", { n: p.seats })}</li><li><Check size={14} aria-hidden="true" />{t("landing.plan.horizon", { n: p.forecast_horizon_max })}</li><li className={p.api_access ? "" : "off"}>{p.api_access ? <Check size={14} aria-hidden="true" /> : <Minus size={14} aria-hidden="true" />}{p.api_access ? t("landing.plan.api", { n: fmtNum(p.api_requests_per_month, intl, 0) }) : t("landing.plan.noApi")}</li></ul></div>)}
      </div>
    </Panel>
  );
}

/** Scroll to the section named in the hash (#keamanan, #paket) once it has rendered. */
function useHashTarget(ready: boolean) {
  const { hash } = useLocation();
  useEffect(() => {
    if (!ready || !hash) return;
    const el = document.getElementById(decodeURIComponent(hash.slice(1)));
    if (!el) return;
    try { el.scrollIntoView({ block: "start" }); } catch { /* jsdom */ }
    el.querySelector<HTMLElement>("input")?.focus({ preventScroll: true });
  }, [ready, hash]);
}

export default function AccountPage() {
  const { t } = useT();
  // Customer-only route (the router keeps guests and operators out).
  const { principal } = useAuth();
  // A temporary password blocks every organisation endpoint: load only the profile until it is replaced.
  const restricted = principal.mustChangePassword;
  const me = useResource((s) => api<MeResponse>("/auth/me", { signal: s }), []);
  const org = useResource((s) => restricted ? Promise.resolve(null) : api<OrganizationDetail>("/org", { signal: s }), [restricted]);
  const loaded = !!me.data && (restricted || !!org.data);
  useHashTarget(loaded);
  return (
    <div className="page">
      <PageHeader eyebrow={t("acct.eyebrow")} icon={Settings} title={t("acct.title")} description={t("acct.desc")} />
      {restricted && <Callout tone="warn">{t("acct.passwordFirst")}</Callout>}
      {(me.error || org.error) && <ErrorState error={me.error ?? org.error} onRetry={() => { me.reload(); org.reload(); }} />}
      {!loaded ? ((me.loading || org.loading) && !me.error && !org.error ? <Skeleton height={300} /> : null) : (<>
        <Profile me={me.data!} reload={me.reload} />
        {org.data && <>
          <PlanPanel org={org.data} reload={org.reload} />
          <Members org={org.data} reload={org.reload} />
          <ApiKeys org={org.data} />
        </>}
      </>)}
    </div>
  );
}
