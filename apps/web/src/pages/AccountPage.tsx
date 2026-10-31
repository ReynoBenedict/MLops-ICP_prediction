/** Akun & Organisasi: profile, password, members, API keys, plan & usage, data export & deletion requests. */
import type { ActiveSession, ApiKeyCreated, ApiKeyInfo, InviteResponse, MeResponse, OrganizationDetail, RevokedSessions, TemporaryPasswordResponse, UserPublic } from "../api/types";
import { Download, FileArchive, KeyRound, Settings, ShieldCheck, UserPlus, Users } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
import { api, ApiError, downloadWithAuth } from "../api/client";
import { labelFor, type PlanChangeRequest } from "../api/product-types";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { useToast } from "../components/toast";
import { Badge, Button, Callout, ConfirmDialog, ErrorState, errorText, Field, focusFirstInvalid, isCredentialError, PageHeader, Panel, retryAfterSeconds, Skeleton, StatusBadge } from "../components/ui";
import { PlanCard } from "../components/public/plans";
import { dateOnly, dateTime, fmtNum } from "../format";
import { useT, type Key } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import { keepAnchored } from "../lib/anchor";
import "../styles/flows.css"; // page styles, loaded with this lazy chunk (not by the public site)

/** Readable device label from a user agent ("Chrome di Windows"); scripts and unknown agents get a plain label. */
export function deviceLabel(ua: string | null | undefined, t: ReturnType<typeof useT>["t"]): string {
  if (!ua) return t("acct.device.unknown");
  if (/python|curl|httpx|urllib|okhttp|node-fetch|axios|postman|wget|go-http/i.test(ua)) return t("acct.device.script");
  const browser = /Edg\//.test(ua) ? "Edge" : /OPR\//.test(ua) ? "Opera" : /Firefox\//.test(ua) ? "Firefox" : /Chrome\//.test(ua) ? "Chrome" : /Safari\//.test(ua) ? "Safari" : null;
  const os = /Windows/.test(ua) ? "Windows" : /Android/.test(ua) ? "Android" : /iPhone|iPad|iOS/.test(ua) ? "iOS" : /Mac OS X|Macintosh/.test(ua) ? "macOS" : /Linux/.test(ua) ? "Linux" : null;
  if (browser && os) return t("acct.device.on", { browser, os });
  return browser ?? os ?? t("acct.device.unknown");
}

/**
 * The session list's device label in the reader's language. The server sends a short Indonesian label
 * derived from the user agent ("Chrome di Windows", "Peramban", "Skrip atau klien API", "Perangkat
 * tidak dikenal") and never the user agent itself, so the known shapes are mapped to strings here;
 * anything else is shown as sent.
 */
export function sessionDevice(label: string, t: ReturnType<typeof useT>["t"]): string {
  const name = (s: string) => (s === "Peramban" ? t("acct.device.browser") : s);
  if (label === "Perangkat tidak dikenal") return t("acct.device.unknown");
  if (label === "Skrip atau klien API") return t("acct.device.script");
  const on = /^(.+) di (.+)$/.exec(label);
  if (on) return t("acct.device.on", { browser: name(on[1]!), os: on[2]! });
  return name(label);
}

/** "42 detik" or, from two minutes on, "15 menit" (the password throttle window is long). */
function waitText(seconds: number | null, t: ReturnType<typeof useT>["t"]): string {
  if (seconds == null) return "-";
  return seconds >= 120 ? t("time.minutes", { n: Math.ceil(seconds / 60) }) : t("time.seconds", { n: seconds });
}

function useAct() {
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const act = async (key: string, fn: () => Promise<void>) => { setBusy(key); setError(null); try { await fn(); } catch (e) { setError(e); } finally { setBusy(null); } };
  return { busy, error, act, setError };
}

function Profile({ me, reload }: { me: MeResponse; reload: () => void }) {
  const { t, intl, locale } = useT();
  const { refresh, signOut, principal } = useAuth();
  const { show } = useToast();
  const [name, setName] = useState(me.user.name);
  const [cur, setCur] = useState(""); const [nw, setNw] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const [pwErrors, setPwErrors] = useState<{ cur?: string; nw?: string }>({});
  const pwForm = useRef<HTMLFormElement>(null);
  const { busy, error, act, setError } = useAct();
  const submitPassword = (e: FormEvent) => {
    e.preventDefault();
    const found: { cur?: string; nw?: string } = {};
    if (!cur) found.cur = t("auth.fieldRequired");
    if (nw.length < 10) found.nw = t("auth.passwordShort");
    setPwErrors(found);
    if (found.cur || found.nw) { requestAnimationFrame(() => focusFirstInvalid(pwForm.current)); return; }
    void act("pw", async () => {
      try {
        await api("/auth/password", { method: "POST", json: { current_password: cur, new_password: nw } });
      } catch (err) {
        // A wrong current password or a weak new one belongs to its field, not to a generic callout.
        if (isCredentialError(err)) { setPwErrors({ cur: errorTextFor(err) }); requestAnimationFrame(() => focusFirstInvalid(pwForm.current)); return; }
        if (err instanceof ApiError && err.code === "WEAK_PASSWORD") { setPwErrors({ nw: errorTextFor(err) }); requestAnimationFrame(() => focusFirstInvalid(pwForm.current)); return; }
        throw err;
      }
      // One message: every session (this one included) ends on the server; the sign-out below is quiet.
      show(t("acct.passwordChanged"), "good");
      await signOut();
    });
  };
  const errorTextFor = (err: unknown) => errorText(err, t, locale).body;
  return (
    <div className="grid cols-2 align-start">
      <Panel title={t("acct.profile")} icon={Settings} id="profil">
        <form className="stack-sm" onSubmit={(e: FormEvent) => { e.preventDefault(); void act("name", async () => { await api("/auth/me", { method: "POST", json: { name } }); setMsg(t("acct.saved")); reload(); await refresh(); }); }}>
          <Field label={t("auth.fullName")} required render={(p) => <input {...p} className="input" value={name} onChange={(e) => setName(e.target.value)} maxLength={120} required autoComplete="name" />} />
          <dl className="kv">
            <div><dt>{t("auth.email")}</dt><dd>{me.user.email}</dd></div>
            <div><dt>{t("acct.role")}</dt><dd>{t(`acct.role.${me.user.role}` as Key)}</dd></div>
            <div><dt>{t("acct.memberSince")}</dt><dd>{dateTime(me.user.created_at, intl)}</dd></div>
            <div><dt>{t("acct.lastLogin")}</dt><dd>{dateTime(me.user.last_login_at, intl)}</dd></div>
          </dl>
          <div><Button type="submit" variant="primary" loading={busy === "name"}>{t("common.save")}</Button></div>
          <div role="status" aria-live="polite">{msg && <Callout tone="good">{msg}</Callout>}</div>
        </form>
      </Panel>
      <Panel title={t("acct.security")} icon={ShieldCheck} id="keamanan">
        <form className="stack-sm" onSubmit={submitPassword} noValidate ref={pwForm}>
          {(me.user.must_change_password || principal.mustChangePassword) && <Callout tone="warn">{t("shell.mustChangePassword")}{me.user.temporary_login_expires_at ? ` ${t("acct.tempValidUntil", { time: dateTime(me.user.temporary_login_expires_at, intl) })}` : ""}</Callout>}
          <Field label={t("acct.currentPassword")} error={pwErrors.cur} required render={(p) => (
            <input {...p} className="input" type="password" autoComplete="current-password" value={cur} onChange={(e) => { setCur(e.target.value); setError(null); }} required />)} />
          <Field label={t("acct.newPassword")} hint={t("auth.passwordHint")} error={pwErrors.nw} required render={(p) => (
            <input {...p} className="input" type="password" autoComplete="new-password" value={nw} onChange={(e) => { setNw(e.target.value); setError(null); }} required minLength={10} />)} />
          <div><Button type="submit" variant="primary" loading={busy === "pw"}>{t("acct.changePassword")}</Button></div>
          <ErrorState error={error} compact signinLink={false} titles={{ RATE_LIMITED: t("error.passwordPaused") }}
            bodies={{ RATE_LIMITED: (e) => t("error.passwordPausedBody", { wait: waitText(retryAfterSeconds(e), t) }) }} />
        </form>
      </Panel>
    </div>
  );
}

/**
 * Active sessions of the signed-in member (GET /auth/sessions): device, sign-in, last activity and
 * both expiries in WIB. Another session can be ended (confirmation), or all others at once. The
 * current session is never offered here: signing out is what ends it. Only member sessions can use
 * these endpoints (operators and API keys get 403), so the page is customer-only already and the
 * panel additionally renders nothing for operators.
 */
function Sessions() {
  const { t, intl } = useT();
  const { isUser } = useAuth();
  const { show } = useToast();
  const list = useResource((s) => isUser ? api<ActiveSession[]>("/auth/sessions", { signal: s }) : Promise.resolve([]), [isUser]);
  const [confirm, setConfirm] = useState<{ kind: "one"; s: ActiveSession } | { kind: "others" } | null>(null);
  const [announce, setAnnounce] = useState("");
  const [showAll, setShowAll] = useState(false);
  const { busy, error, act } = useAct();
  if (!isUser) return null;
  const rows = [...(list.data ?? [])].sort((a, b) => Number(b.current) - Number(a.current) || (b.last_seen_at ?? b.created_at).localeCompare(a.last_seen_at ?? a.created_at));
  const others = rows.filter((s) => !s.current).length;
  // The current session and the most recently active ones first; the rest on request.
  const visible = showAll ? rows : rows.slice(0, SESSIONS_SHOWN);

  const run = () => {
    if (!confirm) return;
    const c = confirm;
    void act("sess", async () => {
      try {
        if (c.kind === "one") {
          await api(`/auth/sessions/${encodeURIComponent(c.s.id)}/revoke`, { method: "POST" });
          const msg = t("sess.revoked", { device: sessionDevice(c.s.device, t) });
          setAnnounce(msg); show(msg, "good");
        } else {
          const r = await api<RevokedSessions>("/auth/sessions/revoke-others", { method: "POST" });
          const msg = t("sess.revokedOthers", { n: r.revoked });
          setAnnounce(msg); show(msg, "good");
        }
      } finally { list.reload(); }
    }).finally(() => setConfirm(null));
  };

  return (
    <Panel title={t("sess.title")} icon={KeyRound} id="sesi" subtitle={t("sess.lead")}
      actions={<Button size="sm" variant="danger" disabled={others === 0} onClick={() => setConfirm({ kind: "others" })}>{t("sess.revokeOthers")}</Button>}>
      <div className="stack-sm">
        <div className="sr-only" role="status" aria-live="polite">{announce}</div>
        <ErrorState error={error} compact titles={{ NOT_FOUND: t("error.sessionGone") }} />
        {list.error ? <ErrorState error={list.error} onRetry={list.reload} compact /> : list.loading && !list.data ? <Skeleton height={80} /> : (
          <div className="table-scroll table-frame" role="region" tabIndex={0} aria-label={t("sess.title")}>
            <table className="table rc">
              <caption className="sr-only">{t("sess.title")} (WIB)</caption>
              <thead><tr><th>{t("sess.device")}</th><th>{t("sess.created")}</th><th>{t("sess.lastSeen")}</th><th>{t("sess.expires")}</th><th>{t("sess.idle")}</th><th><span className="sr-only">{t("common.actions")}</span></th></tr></thead>
              <tbody>{visible.map((s) => (
                <tr key={s.id} aria-current={s.current ? "true" : undefined}>
                  <td className="primary-cell rc-title">{sessionDevice(s.device, t)}{s.current && <> <span className="badge brand">{t("sess.current")}</span></>}</td>
                  <td data-label={t("sess.created")}>{dateTime(s.created_at, intl)}</td>
                  <td data-label={t("sess.lastSeen")}>{dateTime(s.last_seen_at, intl)}</td>
                  <td data-label={t("sess.expires")}>{dateTime(s.expires_at, intl)}</td>
                  <td data-label={t("sess.idle")}>{dateTime(s.idle_expires_at ?? null, intl)}</td>
                  <td className="num rc-full">{s.current
                    ? <span className="xs muted">{t("sess.useSignOut")}</span>
                    : <Button size="sm" variant="ghost" aria-label={t("sess.revokeFor", { device: sessionDevice(s.device, t), time: dateTime(s.created_at, intl) })} onClick={() => setConfirm({ kind: "one", s })}>{t("sess.revoke")}</Button>}</td>
                </tr>
              ))}</tbody>
            </table>
            {rows.length > SESSIONS_SHOWN && (
              <div className="table-more"><Button size="sm" variant="ghost" aria-expanded={showAll} onClick={() => setShowAll((v) => !v)}>{showAll ? t("common.showLess") : t("common.showAll", { n: rows.length })}</Button></div>
            )}
          </div>
        )}
        {list.data && others === 0 && <p className="xs muted">{t("sess.noOthers")}</p>}
      </div>
      <ConfirmDialog open={confirm !== null} busy={busy === "sess"}
        title={!confirm ? "" : confirm.kind === "one" ? t("sess.revokeTitle", { device: sessionDevice(confirm.s.device, t) }) : t("sess.revokeOthersTitle")}
        confirmLabel={!confirm ? "" : confirm.kind === "one" ? t("sess.revoke") : t("sess.revokeOthers")}
        onCancel={() => setConfirm(null)} onConfirm={run}>
        {!confirm ? null : confirm.kind === "one" ? t("sess.revokeBody") : t("sess.revokeOthersBody")}
      </ConfirmDialog>
    </Panel>
  );
}

/** Sessions listed before "show all" (a long-lived account collects many API-client sessions). */
const SESSIONS_SHOWN = 5;

type MemberRole = "admin" | "analyst" | "viewer";
type MemberConfirm = { kind: "role"; m: UserPublic; role: MemberRole } | { kind: "disable"; m: UserPublic } | { kind: "reset"; m: UserPublic };

/** One-time secrets (temporary password, new API key): rendered inside a live region so they are announced. */
function SecretRegion({ children }: { children: ReactNode }) {
  return <div role="status" aria-live="polite" aria-atomic="true">{children}</div>;
}

function Members({ org, reload, me }: { org: OrganizationDetail; reload: () => void; me: MeResponse }) {
  const { t, intl, locale } = useT();
  const { hasRole } = useAuth();
  const members = useResource((s) => api<UserPublic[]>("/org/members", { signal: s }), []);
  const [form, setForm] = useState({ email: "", name: "", role: "analyst" as MemberRole });
  const [invited, setInvited] = useState<InviteResponse | null>(null);
  const [temp, setTemp] = useState<{ id: string; name: string; pw: string; expires: string | null } | null>(null);
  const [draft, setDraft] = useState<Record<string, MemberRole>>({});
  const [confirm, setConfirm] = useState<MemberConfirm | null>(null);
  const { busy, error, act } = useAct();
  const canAdmin = hasRole("admin");
  // Own row: matched by user id (and e-mail as a fallback), never by display name.
  const isSelf = (m: UserPublic) => m.id === me.user.id || m.email.toLowerCase() === me.user.email.toLowerCase();
  // Only the owner manages administrators (the server answers 403 otherwise): an admin sees other
  // admins read-only instead of buttons that would fail.
  const manageable = (m: UserPublic) => canAdmin && m.role !== "owner" && !isSelf(m) && (m.role !== "admin" || hasRole("owner"));
  const roleName = (r: string) => t(`acct.role.${r}` as Key);
  const roleOptions = <><option value="viewer">{roleName("viewer")}</option><option value="analyst">{roleName("analyst")}</option>{hasRole("owner") && <option value="admin">{roleName("admin")}</option>}</>;

  const runConfirm = () => {
    if (!confirm) return;
    const c = confirm;
    void act(`confirm-${c.m.id}`, async () => {
      if (c.kind === "role") {
        await api(`/org/members/${c.m.id}`, { method: "POST", json: { role: c.role } });
        setDraft((d) => { const n = { ...d }; delete n[c.m.id]; return n; });
        members.reload();
      } else if (c.kind === "disable") {
        await api(`/org/members/${c.m.id}`, { method: "POST", json: { status: "disabled" } });
        members.reload(); reload();
      } else {
        const r = await api<TemporaryPasswordResponse>(`/org/members/${c.m.id}/reset-password`, { method: "POST" });
        setTemp({ id: c.m.id, name: c.m.name, pw: r.temporary_password, expires: r.expires_at ?? null });
        members.reload();
      }
    }).finally(() => setConfirm(null));
  };

  return (
    <Panel title={t("acct.members")} icon={Users} id="anggota" subtitle={t("acct.seatLimit", { plan: labelFor(org.entitlements, locale), n: org.entitlements.seats })}>
      <div className="stack">
        {canAdmin && (
          <form className="row invite-form" onSubmit={(e: FormEvent) => { e.preventDefault(); void act("invite", async () => { setInvited(await api<InviteResponse>("/org/members", { method: "POST", json: form })); setForm({ email: "", name: "", role: "analyst" }); members.reload(); reload(); }); }}>
            <label className="field grow"><span className="field-label">{t("auth.email")}</span><input className="input" type="email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} required /></label>
            <label className="field grow"><span className="field-label">{t("auth.fullName")}</span><input className="input" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required /></label>
            <label className="field"><span className="field-label">{t("acct.role")}</span><select className="select" value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value as MemberRole })}>{roleOptions}</select></label>
            <Button type="submit" variant="primary" icon={UserPlus} loading={busy === "invite"}>{t("acct.invite")}</Button>
          </form>
        )}
        <SecretRegion>
          {invited && <Callout tone="good" title={`${invited.member.email} · ${t("acct.tempPassword")}`}><code className="mono secret-value">{invited.temporary_password}</code>{invited.member.temporary_login_expires_at && <div className="small mt-2">{t("acct.tempValidUntil", { time: dateTime(invited.member.temporary_login_expires_at, intl) })}</div>}<div className="small mt-2">{invited.note}</div></Callout>}
          {temp && <Callout tone="good" title={`${temp.name} · ${t("acct.tempPassword")}`}><code className="mono secret-value">{temp.pw}</code>{temp.expires && <div className="small mt-2">{t("acct.tempValidUntil", { time: dateTime(temp.expires, intl) })}</div>}</Callout>}
        </SecretRegion>
        <ErrorState error={error} compact titles={{ FORBIDDEN: t("error.ownerOnly") }} />
        {members.loading && !members.data ? <Skeleton height={120} /> : (
          <div className="table-scroll table-frame" role="region" tabIndex={0} aria-label={t("acct.members")}>
            <table className="table rc">
              <thead><tr><th>{t("auth.fullName")}</th><th>{t("acct.role")}</th><th>{t("common.status")}</th><th className="hide-sm">{t("acct.lastLogin")}</th>{canAdmin && <th><span className="sr-only">{t("common.actions")}</span></th>}</tr></thead>
              <tbody>{(members.data ?? []).map((m) => {
                const pending = draft[m.id];
                return (
                  <tr key={m.id}>
                    <td className="primary-cell rc-title"><span>{m.name}{isSelf(m) && <> <span className="badge neutral">{t("acct.you")}</span></>}<span className="sub">{m.email}</span></span></td>
                    <td data-label={t("acct.role")}>{manageable(m) ? (
                      <div className="row role-edit">
                        <select className="select role-select" aria-label={t("acct.roleFor", { name: m.name })} value={pending ?? m.role}
                          onChange={(e) => { const v = e.target.value as MemberRole; setDraft((d) => { const n = { ...d }; if (v === m.role) delete n[m.id]; else n[m.id] = v; return n; }); }}>
                          {roleOptions}</select>
                        {pending && pending !== m.role && <Button size="sm" variant="primary" aria-label={t("acct.applyRoleFor", { name: m.name })} onClick={() => setConfirm({ kind: "role", m, role: pending })}>{t("acct.applyRole")}</Button>}
                      </div>
                    ) : roleName(m.role)}</td>
                    <td data-label={t("common.status")}><StatusBadge status={m.status} />{m.must_change_password && <span className="sub">{t("acct.tempPassword").split(" (")[0]}{m.temporary_login_expires_at ? ` · ${t("acct.tempExpiresMember", { time: dateTime(m.temporary_login_expires_at, intl) })}` : ""}</span>}</td>
                    <td className="hide-sm" data-label={t("acct.lastLogin")}>{dateTime(m.last_login_at, intl)}</td>
                    {canAdmin && <td className="num rc-full"><div className="row row-end">
                      {manageable(m) && (m.status === "active"
                        ? <Button size="sm" variant="danger" aria-label={`${t("acct.disable")}: ${m.name}`} onClick={() => setConfirm({ kind: "disable", m })}>{t("acct.disable")}</Button>
                        : <Button size="sm" variant="secondary" aria-label={`${t("acct.enable")}: ${m.name}`} loading={busy === `st-${m.id}`}
                            onClick={() => void act(`st-${m.id}`, async () => { await api(`/org/members/${m.id}`, { method: "POST", json: { status: "active" } }); members.reload(); reload(); })}>{t("acct.enable")}</Button>)}
                      {manageable(m) && <Button size="sm" variant="ghost" aria-label={`${t("acct.resetPassword")}: ${m.name}`} onClick={() => setConfirm({ kind: "reset", m })}>{t("acct.resetPassword")}</Button>}
                    </div></td>}
                  </tr>
                );
              })}</tbody>
            </table>
          </div>
        )}
      </div>
      <ConfirmDialog open={confirm !== null} busy={!!confirm && busy === `confirm-${confirm.m.id}`} danger={confirm?.kind !== "role"}
        title={!confirm ? "" : confirm.kind === "role" ? t("acct.confirmRoleTitle", { name: confirm.m.name, role: roleName(confirm.role) })
          : confirm.kind === "disable" ? t("acct.confirmDisableTitle", { name: confirm.m.name }) : t("acct.confirmResetTitle", { name: confirm.m.name })}
        confirmLabel={!confirm ? "" : confirm.kind === "role" ? t("acct.confirmRoleButton") : confirm.kind === "disable" ? t("acct.disable") : t("acct.resetPassword")}
        onCancel={() => setConfirm(null)} onConfirm={runConfirm}>
        {!confirm ? null : confirm.kind === "role" ? t("acct.confirmRoleBody") : confirm.kind === "disable" ? t("acct.confirmDisableBody") : t("acct.confirmResetBody")}
      </ConfirmDialog>
    </Panel>
  );
}

function ApiKeys({ org }: { org: OrganizationDetail }) {
  const { t, intl } = useT();
  const { hasRole } = useAuth();
  const keys = useResource((s) => hasRole("admin") && org.entitlements.api_access ? api<ApiKeyInfo[]>("/org/api-keys", { signal: s }) : Promise.resolve([]), [org.entitlements.api_access]);
  const [name, setName] = useState("");
  const [created, setCreated] = useState<ApiKeyCreated | null>(null);
  const [revoking, setRevoking] = useState<ApiKeyInfo | null>(null);
  const { busy, error, act } = useAct();
  return (
    <Panel title={t("acct.apiKeys")} icon={KeyRound} id="api" subtitle={t("acct.apiHow")}>
      {!org.entitlements.api_access ? <Callout tone="info">{t("acct.apiNotInPlan")}</Callout> : !hasRole("admin") ? <Callout tone="info">{t("acct.apiAdminOnly")}</Callout> : (
        <div className="stack">
          <form className="row invite-form" onSubmit={(e: FormEvent) => { e.preventDefault(); void act("key", async () => { setCreated(await api<ApiKeyCreated>("/org/api-keys", { method: "POST", json: { name } })); setName(""); keys.reload(); }); }}>
            <label className="field grow"><span className="field-label">{t("acct.keyName")}</span><input className="input" value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} /></label>
            <Button type="submit" variant="primary" icon={KeyRound} loading={busy === "key"}>{t("acct.newKey")}</Button>
          </form>
          <SecretRegion>
            {created && <Callout tone="good" title={created.name}><code className="mono secret-value">{created.key}</code><div className="small mt-2">{created.note}</div></Callout>}
          </SecretRegion>
          <ErrorState error={error} compact />
          <div className="table-scroll table-frame" role="region" tabIndex={0} aria-label={t("acct.apiKeys")}>
            <table className="table rc"><thead><tr><th>{t("acct.keyName")}</th><th>{t("common.created")}</th><th>{t("acct.lastUsed")}</th><th><span className="sr-only">{t("common.actions")}</span></th></tr></thead>
              <tbody>{(keys.data ?? []).map((k) => (
                <tr key={k.id}><td className="primary-cell rc-title">{k.name}<span className="sub mono">{k.prefix}…</span></td><td data-label={t("common.created")}>{dateTime(k.created_at, intl)}</td><td data-label={t("acct.lastUsed")}>{dateTime(k.last_used_at, intl)}</td>
                  <td className="num rc-full">{k.revoked_at ? <Badge tone="neutral">{t("acct.revoked")}</Badge> : <Button size="sm" variant="danger" aria-label={`${t("acct.revoke")}: ${k.name}`} onClick={() => setRevoking(k)}>{t("acct.revoke")}</Button>}</td></tr>
              ))}</tbody></table>
          </div>
        </div>
      )}
      <ConfirmDialog open={revoking !== null} busy={!!revoking && busy === `rv-${revoking.id}`}
        title={revoking ? t("acct.confirmRevokeTitle", { name: revoking.name }) : ""} confirmLabel={t("acct.revoke")}
        onCancel={() => setRevoking(null)}
        onConfirm={() => { if (!revoking) return; const k = revoking; void act(`rv-${k.id}`, async () => { await api(`/org/api-keys/${k.id}/revoke`, { method: "POST" }); keys.reload(); }).finally(() => setRevoking(null)); }}>
        {t("acct.confirmRevokeBody")}
      </ConfirmDialog>
    </Panel>
  );
}

const PLAN_ORDER = ["trial", "starter", "professional", "enterprise"] as const;

/**
 * Plan change *request* (no payment, no checkout): an owner or admin asks for another plan with an
 * optional note; the operator reviews it in the console. The status is read from the server; when
 * the server does not offer requests yet (404/405) the page only explains how plan changes work.
 */
function PlanRequests({ org }: { org: OrganizationDetail }) {
  const { t, intl, locale } = useT();
  const { hasRole } = useAuth();
  const { show } = useToast();
  const canRequest = hasRole("admin");
  const requests = useResource((s) => api<{ items: PlanChangeRequest[] }>("/org/plan-requests", { signal: s }), [org.id]);
  const unsupported = requests.error instanceof ApiError && [404, 405].includes(requests.error.status);
  const items = requests.data?.items ?? [];
  const pending = items.find((r) => r.status === "pending") ?? null;
  const latest = pending ?? [...items].sort((a, b) => b.created_at.localeCompare(a.created_at))[0] ?? null;
  const targets = org.plans.filter((p) => p.code !== org.plan);
  const [target, setTarget] = useState<string>(targets[0]?.code ?? "");
  const [note, setNote] = useState("");
  const [withdrawing, setWithdrawing] = useState(false);
  const { busy, error, act, setError } = useAct();
  const planName = (code: string) => {
    const p = org.plans.find((x) => x.code === code);
    return p ? labelFor(p, locale) : planLabel(code);
  };
  const planLabel = (code: string) => t(`plan.${code}` as Key);

  if (requests.loading && !requests.data && !requests.error) return <Skeleton height={80} />;
  if (unsupported) return <Callout tone="info" title={t("acct.upgradeTitle")}>{t("acct.upgradeBody")}</Callout>;
  if (requests.error) return <ErrorState error={requests.error} onRetry={requests.reload} compact />;

  const statusLine = (r: PlanChangeRequest) => {
    const vars = { plan: planName(r.requested_plan), date: dateOnly(r.status === "pending" ? r.created_at : (r.resolved_at ?? r.created_at), intl) };
    return t(`acct.request.status.${r.status}` as Key, vars);
  };

  return (
    <div className="plan-request stack-sm" aria-live="polite">
      <div className="section-title">{t("acct.request.title")}</div>
      {latest && (
        <div className={`request-status ${latest.status}`}>
          <div className="row-between">
            <strong>{statusLine(latest)}</strong>
            <Badge tone={latest.status === "pending" ? "info" : "neutral"}>{t(`acct.request.badge.${latest.status}` as Key)}</Badge>
          </div>
          {latest.note && <div className="small muted">{t("acct.request.yourNote")}: {latest.note}</div>}
          {latest.resolution_note && <div className="small">{t("acct.request.operatorNote")}: {latest.resolution_note}</div>}
          {latest.status === "pending" && canRequest && (
            <div><Button size="sm" variant="ghost" onClick={() => setWithdrawing(true)}>{t("acct.request.withdraw")}</Button></div>
          )}
        </div>
      )}
      {!pending && canRequest && targets.length > 0 && (
        <form className="stack-sm" onSubmit={(e: FormEvent) => {
          e.preventDefault();
          void act("request", async () => {
            try {
              await api("/org/plan-requests", { method: "POST", json: { requested_plan: target, note: note.trim() || null } });
            } catch (err) {
              if (err instanceof ApiError && err.status === 409) { requests.reload(); throw err; }
              throw err;
            }
            setNote("");
            requests.reload();
            show(t("acct.request.sent", { plan: planName(target) }), "good");
          });
        }}>
          <div className="grid cols-2">
            <label className="field"><span className="field-label">{t("acct.request.target")}</span>
              <select className="select" value={target} onChange={(e) => setTarget(e.target.value)}>
                {targets.map((p) => <option key={p.code} value={p.code}>{labelFor(p, locale)}</option>)}
              </select></label>
            <label className="field"><span className="field-label">{t("acct.request.note")} <span className="muted">({t("common.optional")})</span></span>
              <textarea className="textarea plain" value={note} maxLength={500} rows={2} onChange={(e) => setNote(e.target.value)} />
              <span className="field-hint">{t("acct.request.noteHint", { n: 500 - note.length })}</span></label>
          </div>
          <p className="small muted">{t("acct.request.how")}</p>
          <div><Button type="submit" variant="primary" loading={busy === "request"} disabled={!target}>{t("acct.request.submit")}</Button></div>
        </form>
      )}
      {!canRequest && <p className="small muted">{t("acct.request.adminOnly")}</p>}
      {error instanceof ApiError && error.status === 409 ? <Callout tone="info">{t("acct.request.alreadyPending")}</Callout> : <ErrorState error={error} compact />}
      <ConfirmDialog open={withdrawing} busy={busy === "withdraw"} danger={false}
        title={t("acct.request.withdrawTitle")} confirmLabel={t("acct.request.withdrawConfirm")}
        onCancel={() => setWithdrawing(false)}
        onConfirm={() => {
          if (!pending) return;
          setError(null);
          void act("withdraw", async () => {
            await api(`/org/plan-requests/${pending.id}/withdraw`, { method: "POST" });
            requests.reload();
            show(t("acct.request.withdrawn"), "good");
          }).finally(() => setWithdrawing(false));
        }}>
        {t("acct.request.withdrawBody", { plan: pending ? planName(pending.requested_plan) : "" })}
      </ConfirmDialog>
    </div>
  );
}

/** A deletion request (GET/POST /org/deletion-requests, ux6/API-v6.md §3). */
interface DeletionRequest {
  id: string; scope: "account" | "organization"; user_id: string | null; user_name: string | null; requested_by: string; requested_by_name: string | null;
  reason: string | null; status: "pending" | "executed" | "declined" | "withdrawn"; created_at: string; resolved_at: string | null; resolution_note: string | null;
}

/**
 * Data & privacy: the owner downloads the organisation's own data (ZIP, generated on demand, not kept
 * on the server); any member can ask for their account to be deleted, the owner for the whole
 * organisation. Deletion is carried out by the platform operator (anonymisation, audited); nothing is
 * deleted by this page. Pieces are hidden on servers that do not offer the endpoints (404/405).
 */
function DataPrivacy({ org, me }: { org: OrganizationDetail; me: MeResponse }) {
  const { t, intl } = useT();
  const { show } = useToast();
  const owner = me.user.role === "owner";
  const scope: DeletionRequest["scope"] = owner ? "organization" : "account";
  const list = useResource((s) => api<{ items: DeletionRequest[] }>("/org/deletion-requests", { signal: s }), [org.id]);
  const unsupported = list.error instanceof ApiError && [404, 405].includes(list.error.status);
  const items = list.data?.items ?? [];
  const pending = items.find((r) => r.status === "pending" && r.scope === scope && (scope === "organization" || r.user_id === me.user.id)) ?? null;
  const [reason, setReason] = useState("");
  const [confirm, setConfirm] = useState<null | "request" | "withdraw">(null);
  const { busy, error, act } = useAct();
  const today = new Date().toISOString().slice(0, 10).replaceAll("-", "");
  return (
    <Panel title={t("acct.data.title")} icon={FileArchive} id="data" subtitle={t("acct.data.sub")}>
      <div className="grid cols-2 align-start">
        <div className="stack-sm">
          <div className="section-title">{t("acct.data.exportTitle")}</div>
          <p className="small">{t("acct.data.exportBody")}</p>
          {owner ? (
            <div><Button icon={Download} loading={busy === "export"} onClick={() => void act("export", async () => {
              await downloadWithAuth("/org/export", undefined, "icp-intelligence_org-export_" + org.id + "_" + today + ".zip");
              show(t("acct.data.exported"), "good");
            })}>{t("acct.data.exportButton")}</Button></div>
          ) : <p className="small muted">{t("acct.data.exportOwnerOnly")}</p>}
        </div>
        <div className="stack-sm" aria-live="polite">
          <div className="section-title">{owner ? t("acct.data.deleteOrgTitle") : t("acct.data.deleteAccountTitle")}</div>
          {unsupported ? <p className="small muted">{t("acct.data.deleteUnavailable")}</p> : list.error ? <ErrorState error={list.error} onRetry={list.reload} compact /> : (<>
            <p className="small">{owner ? t("acct.data.deleteOrgBody") : t("acct.data.deleteAccountBody")}</p>
            {pending ? (
              <div className="request-status pending">
                <div className="row-between"><strong>{t("acct.data.pending", { date: dateOnly(pending.created_at, intl) })}</strong><Badge tone="info">{t("acct.request.badge.pending")}</Badge></div>
                {pending.reason && <div className="small muted">{t("acct.request.yourNote")}: {pending.reason}</div>}
                <div><Button size="sm" variant="ghost" onClick={() => setConfirm("withdraw")}>{t("acct.request.withdraw")}</Button></div>
              </div>
            ) : (
              <form className="stack-sm" onSubmit={(e: FormEvent) => { e.preventDefault(); setConfirm("request"); }}>
                <label className="field"><span className="field-label">{t("acct.data.reason")} <span className="muted">({t("common.optional")})</span></span>
                  <textarea className="textarea plain" value={reason} maxLength={500} rows={2} onChange={(e) => setReason(e.target.value)} /></label>
                <div><Button type="submit" variant="danger">{owner ? t("acct.data.deleteOrgButton") : t("acct.data.deleteAccountButton")}</Button></div>
              </form>
            )}
            {items.filter((r) => r !== pending && r.status !== "pending").slice(0, 3).map((r) => (
              <p key={r.id} className="xs muted">{t(("acct.data.past." + r.status) as Key, { date: dateOnly(r.resolved_at ?? r.created_at, intl) })}{r.resolution_note ? " · " + r.resolution_note : ""}</p>
            ))}
          </>)}
          <ErrorState error={error} compact />
        </div>
      </div>
      <ConfirmDialog open={confirm !== null} busy={busy === "delete"} danger={confirm === "request"}
        title={confirm === "withdraw" ? t("acct.data.withdrawTitle") : owner ? t("acct.data.confirmOrgTitle") : t("acct.data.confirmAccountTitle")}
        confirmLabel={confirm === "withdraw" ? t("acct.request.withdrawConfirm") : t("acct.data.confirmButton")}
        onCancel={() => setConfirm(null)}
        onConfirm={() => void act("delete", async () => {
          if (confirm === "withdraw" && pending) { await api("/org/deletion-requests/" + pending.id + "/withdraw",{ method: "POST" }); show(t("acct.data.withdrawn"), "good"); }
          else { await api("/org/deletion-requests", { method: "POST", json: { scope, reason: reason.trim() || null } }); setReason(""); show(t("acct.data.requested"), "good"); }
          list.reload();
        }).finally(() => setConfirm(null))}>
        {confirm === "withdraw" ? t("acct.data.withdrawBody") : owner ? t("acct.data.confirmOrgBody") : t("acct.data.confirmAccountBody")}
      </ConfirmDialog>
    </Panel>
  );
}

function PlanPanel({ org, reload }: { org: OrganizationDetail; reload: () => void }) {
  const { t, locale, intl } = useT();
  const { hasRole } = useAuth();
  const [name, setName] = useState(org.name);
  const { busy, error, act } = useAct();
  const ent = org.entitlements;
  const usage = org.usage_this_month;
  const plans = [...org.plans].sort((a, b) => PLAN_ORDER.indexOf(a.code as (typeof PLAN_ORDER)[number]) - PLAN_ORDER.indexOf(b.code as (typeof PLAN_ORDER)[number]));
  return (
    <Panel title={t("acct.plan")} icon={ShieldCheck} id="paket">
      <div className="grid cols-2 align-start">
        <div className="stack-sm">
          <form className="stack-sm" onSubmit={(e: FormEvent) => { e.preventDefault(); void act("rename", async () => { await api("/org", { method: "POST", json: { name } }); reload(); }); }}>
            <label className="field"><span className="field-label">{t("acct.orgName")}</span><input className="input" value={name} onChange={(e) => setName(e.target.value)} disabled={!hasRole("admin")} /></label>
            {hasRole("admin") && <div><Button type="submit" size="sm" loading={busy === "rename"}>{t("acct.rename")}</Button></div>}
          </form>
          <div className="row"><span className="small muted">{t("acct.currentPlan")}:</span><Badge tone="brand">{labelFor(ent, locale)}</Badge><StatusBadge status={org.status} /></div>
          <ErrorState error={error} compact />
          <PlanRequests org={org} />
        </div>
        <div className="stack-sm">
          <div>
            <div className="section-title">{t("acct.limitsTitle")}</div>
            {/* Plan limits in use, before they block anything (seats, saved scenarios, alert rules). */}
            <dl className="kv plan-limits">
              {([[t("acct.members"), org.active_members, ent.seats], [t("acct.limit.scenarios"), org.saved_scenarios, ent.saved_scenarios_max], [t("alert.rules"), org.alert_rules, ent.alert_rules_max]] as const).map(([label, used, max]) => (
                <div key={label} className={used >= max ? "full" : undefined}>
                  <dt>{label}</dt>
                  <dd><span className="limit-num">{t("acct.limitOf", { used, max: fmtNum(max, intl, 0) })}</span>
                    <span className="limit-meter" aria-hidden="true"><span style={{ width: `${Math.min(100, (used / Math.max(1, max)) * 100)}%` }} /></span></dd>
                </div>
              ))}
            </dl>
          </div>
          <div>
            <div className="section-title">{t("acct.usage")}</div>
            <dl className="kv">{["forecast.run", "scenario.run", "scenario.compare", "report.generate", "api.request"].map((k) => (
              <div key={k}><dt>{t(`acct.usage.${k}` as Key)}</dt><dd className="num">{usage[k] ?? 0}{k === "api.request" && ent.api_access ? ` / ${fmtNum(ent.api_requests_per_month, intl, 0)}` : ""}</dd></div>
            ))}</dl>
            <p className="xs muted mt-2">{t("acct.usageNote")}</p>
            {ent.api_access && <p className="xs muted">{t("acct.quotaNote", { n: ent.api_requests_per_month })}</p>}
          </div>
        </div>
      </div>
      <div className="section-title mt-4">{t("acct.plansCompare")}</div>
      <div className="plan-grid">
        {plans.map((p) => <PlanCard key={p.code} p={p} current={p.code === org.plan} currentLabel={t("acct.currentPlan")} />)}
      </div>
    </Panel>
  );
}


/**
 * Scroll to the section named in the hash (#keamanan, #paket) once it has rendered, and keep it
 * there while the sections above it load and grow (sessions, plan, members: lib/anchor.ts).
 * Focus moves into the section: its first field (e.g. the current password), else its heading;
 * focusing scrolls only when that element is not fully in view, so it is never left off-screen.
 */
function useHashTarget(ready: boolean) {
  const { hash } = useLocation();
  useEffect(() => {
    if (!ready || !hash) return;
    const el = document.getElementById(decodeURIComponent(hash.slice(1)));
    if (!el) return;
    try { el.scrollIntoView({ block: "start" }); } catch { /* jsdom */ }
    const stop = keepAnchored(el);
    const target = el.querySelector<HTMLElement>("input:not([disabled])") ?? el.querySelector<HTMLElement>("h2");
    if (target?.tagName === "H2" && !target.hasAttribute("tabindex")) target.setAttribute("tabindex", "-1");
    target?.focus();
    return stop;
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
  // In-page contents: the page holds seven sections; each link moves focus into its section.
  const sections: Array<[string, Key]> = [["profil", "acct.profile"], ["keamanan", "acct.security"], ["sesi", "sess.title"],
    ...(restricted ? [] : [["paket", "acct.plan"], ["anggota", "acct.members"], ["api", "acct.apiKeys"], ["data", "acct.data.title"]] as Array<[string, Key]>)];
  return (
    <div className="page acct-page">
      <PageHeader eyebrow={t("acct.eyebrow")} icon={Settings} title={t("acct.title")} description={t("acct.desc")} />
      {loaded && (
        <nav className="acct-nav" aria-label={t("acct.navLabel")}>
          <ul>{sections.map(([id, key]) => <li key={id}><Link to={{ hash: `#${id}` }}>{t(key)}</Link></li>)}</ul>
        </nav>
      )}
      {restricted && <Callout tone="warn">{t("acct.passwordFirst")}</Callout>}
      {(me.error || org.error) && <ErrorState error={me.error ?? org.error} onRetry={() => { me.reload(); org.reload(); }} />}
      {!loaded ? ((me.loading || org.loading) && !me.error && !org.error ? <Skeleton height={300} /> : null) : (<>
        <Profile me={me.data!} reload={me.reload} />
        <Sessions />
        {org.data && <>
          <PlanPanel org={org.data} reload={org.reload} />
          <Members org={org.data} reload={org.reload} me={me.data!} />
          <ApiKeys org={org.data} />
          <DataPrivacy org={org.data} me={me.data!} />
        </>}
      </>)}
    </div>
  );
}
