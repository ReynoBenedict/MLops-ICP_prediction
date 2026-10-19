/**
 * Layouts. Three audiences, three navigations:
 * - PublicLayout (guests, and anyone on the public pages): top navigation + footer, no app sidebar.
 * - AppShell for customers: workspace sidebar (Pasar / Analisis / Keluaran) and an account card.
 * - AppShell for operators: operations-first sidebar, a "Pratinjau produk" group with the read-only
 *   market pages, an environment band and no customer items (scenarios, reports, account).
 * Internal details (environment, API version) are only shown to operators.
 */
import type { AlertEventList, SystemStatus } from "../api/types";
import {
  Activity, Bell, Building2, ChartLine, Database, FileText, FlaskConical, Globe, LayoutDashboard, Lightbulb, LoaderCircle, LogOut,
  Menu, ScrollText, Settings, ShieldCheck, SlidersHorizontal, Workflow, X, type LucideIcon,
} from "lucide-react";
import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { Link, NavLink, useLocation, useNavigate } from "react-router-dom";
import { api } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { useT, type Key } from "../i18n";
import { LogoSymbol, Wordmark } from "./brand";
import { useToast } from "./toast";
import { Skeleton, StatusBadge } from "./ui";

interface NavItem {
  to: string;
  label: Key;
  icon: LucideIcon;
  end?: boolean;
  badge?: number;
  /** Custom active test (query-string tabs share one path). */
  match?: (pathname: string, search: URLSearchParams) => boolean;
}
interface NavGroup { label: Key; items: NavItem[] }

export function LanguageToggle({ dark }: { dark?: boolean }) {
  const { locale, setLocale, t } = useT();
  return (
    <div className={`lang-toggle${dark ? " dark" : ""}`} role="group" aria-label={t("shell.language")}>
      <Globe size={14} aria-hidden="true" />
      <button type="button" aria-pressed={locale === "id"} onClick={() => setLocale("id")}>ID</button>
      <button type="button" aria-pressed={locale === "en"} onClick={() => setLocale("en")}>EN</button>
    </div>
  );
}

function Brand({ to }: { to: string }) {
  const { t } = useT();
  return (
    <Link to={to} className="brand" aria-label={t("brand.name")}>
      <LogoSymbol size={34} tone="teal" />
      <span>
        <span className="brand-name" style={{ display: "block" }}>{t("brand.name")}</span>
        <span className="brand-sub">{t("brand.tagline")}</span>
      </span>
    </Link>
  );
}

/** Product footer under every customer page: legal links and the no-advice note. */
export function ShellFooter() {
  const { t } = useT();
  return (
    <footer className="main-foot">
      <span>{t("shell.footer.note")}</span>
      <nav className="row" aria-label={t("landing.footer.legal")}>
        <Link to="/metodologi">{t("nav.methodology")}</Link>
        <Link to="/syarat">{t("landing.footer.terms")}</Link>
        <Link to="/privasi">{t("landing.footer.privacy")}</Link>
      </nav>
    </footer>
  );
}

/* ---------- Public website ---------------------------------------------------------------------- */

export function PublicFooter({ credits }: { credits?: boolean }) {
  const { t } = useT();
  const { principal } = useAuth();
  const productId = useId();
  const legalId = useId();
  const signedIn = principal.kind !== "public";
  return (
    <footer className="landing-footer">
      <div className="inner">
        <div>
          <Wordmark dark size={30} />
          <p className="mt-3">{t("landing.footer.operator")}</p>
          <p className="mt-2">{t("landing.footer.noAdvice")}</p>
          {credits && <p className="mt-2 xs footer-credit"><strong>{t("landing.photoCreditLabel")}:</strong> {t("landing.photoCredit")} {t("landing.illCredit")}</p>}
        </div>
        <nav aria-label={t("landing.footer.product")}>
          <h2 id={productId} className="footer-h">{t("landing.footer.product")}</h2>
          <ul>
            <li><Link to="/">{t("shell.landing")}</Link></li>
            <li><Link to="/metodologi">{t("nav.methodology")}</Link></li>
            {!signedIn && <li><Link to="/#paket">{t("nav.plans")}</Link></li>}
            <li>{signedIn ? <Link to={principal.kind === "operator" ? "/operasi" : "/ringkasan"}>{t("nav.openWorkspace")}</Link> : <Link to="/masuk">{t("shell.signin")}</Link>}</li>
          </ul>
        </nav>
        <nav aria-label={t("landing.footer.legal")}>
          <h2 id={legalId} className="footer-h">{t("landing.footer.legal")}</h2>
          <ul><li><Link to="/syarat">{t("landing.footer.terms")}</Link></li><li><Link to="/privasi">{t("landing.footer.privacy")}</Link></li></ul>
        </nav>
      </div>
    </footer>
  );
}

/**
 * Header + main + footer for the public pages. Below 900 px the links collapse behind a menu
 * button (nothing is hidden without a replacement).
 */
export function PublicLayout({ children, credits, className = "" }: { children: ReactNode; credits?: boolean; className?: string }) {
  const { t } = useT();
  const { principal, ready } = useAuth();
  const [open, setOpen] = useState(false);
  const location = useLocation();
  useEffect(() => { setOpen(false); }, [location.pathname, location.hash]);
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setOpen(false); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);
  const signedIn = ready && principal.kind !== "public";
  const workspace = principal.kind === "operator" ? "/operasi" : "/ringkasan";
  return (
    <div className={`landing ${className}`}>
      <a href="#main" className="skip-link">{t("shell.skip")}</a>
      <header className="landing-nav">
        <Link to="/" className="brand-link" aria-label={t("brand.name")}><Wordmark size={32} /></Link>
        <button type="button" className="btn sm public-menu-btn" aria-expanded={open} aria-controls="public-menu" onClick={() => setOpen((o) => !o)}>
          {open ? <X size={16} aria-hidden="true" /> : <Menu size={16} aria-hidden="true" />}{t("nav.menu")}
        </button>
        <nav id="public-menu" className={`public-menu${open ? " open" : ""}`} aria-label={t("nav.site")}>
          <div className="links">
            <NavLink to="/metodologi" className={({ isActive }) => (isActive ? "active" : undefined)}>{t("nav.methodology")}</NavLink>
            {principal.kind === "user" ? <Link to="/akun#paket">{t("nav.plans")}</Link> : principal.kind === "public" ? <Link to="/#paket">{t("nav.plans")}</Link> : null}
          </div>
          <div className="public-actions">
            <LanguageToggle />
            {signedIn ? <Link className="btn sm primary" to={workspace}>{t("nav.openWorkspace")}</Link> : (<>
              <Link className="btn sm" to="/masuk">{t("shell.signin")}</Link>
              <Link className="btn sm primary" to="/daftar">{t("shell.register")}</Link>
            </>)}
          </div>
        </nav>
      </header>
      <main id="main" tabIndex={-1} className="public-main">{children}</main>
      <PublicFooter credits={credits} />
    </div>
  );
}

/* ---------- Signed-in application shell ------------------------------------------------------- */

/** True while the viewport uses the off-canvas navigation drawer (same breakpoint as styles.css). */
function useDrawerLayout(): boolean {
  const query = "(max-width: 900px)";
  const get = () => { try { return window.matchMedia(query).matches; } catch { return false; } };
  const [drawer, setDrawer] = useState(get);
  useEffect(() => {
    let mq: MediaQueryList;
    try { mq = window.matchMedia(query); } catch { return; }
    const on = () => setDrawer(mq.matches);
    mq.addEventListener?.("change", on);
    return () => mq.removeEventListener?.("change", on);
  }, []);
  return drawer;
}

export function customerNav(unread: number): NavGroup[] {
  return [
    { label: "nav.group.market", items: [
      { to: "/ringkasan", label: "nav.overview", icon: LayoutDashboard },
      { to: "/harga", label: "nav.explorer", icon: ChartLine },
    ] },
    { label: "nav.group.analysis", items: [
      { to: "/proyeksi", label: "nav.forecast", icon: Activity },
      { to: "/skenario", label: "nav.scenarios", icon: SlidersHorizontal },
      { to: "/insight", label: "nav.insights", icon: Lightbulb },
    ] },
    { label: "nav.group.output", items: [
      { to: "/laporan", label: "nav.reports", icon: FileText, match: (p, q) => p === "/laporan" && q.get("tab") !== "pemberitahuan" },
      { to: "/laporan?tab=pemberitahuan", label: "nav.alerts", icon: Bell, badge: unread, match: (p, q) => p === "/laporan" && q.get("tab") === "pemberitahuan" },
    ] },
  ];
}

export function operatorNav(): NavGroup[] {
  return [
    { label: "nav.group.ops", items: [
      { to: "/operasi", label: "nav.ops.health", icon: ShieldCheck, end: true },
      { to: "/operasi/data", label: "nav.ops.data", icon: Database },
      { to: "/operasi/model", label: "nav.ops.models", icon: FlaskConical },
      { to: "/operasi/pekerjaan", label: "nav.ops.jobs", icon: Workflow },
      { to: "/operasi/audit", label: "nav.ops.audit", icon: ScrollText },
      { to: "/operasi/organisasi", label: "nav.ops.tenants", icon: Building2 },
    ] },
    { label: "nav.group.preview", items: [
      { to: "/ringkasan", label: "nav.overview", icon: LayoutDashboard },
      { to: "/harga", label: "nav.explorer", icon: ChartLine },
      { to: "/proyeksi", label: "nav.forecast", icon: Activity },
      { to: "/insight", label: "nav.insights", icon: Lightbulb },
    ] },
  ];
}

export function AppShell({ children }: { children: ReactNode }) {
  const { t, td, locale } = useT();
  const { principal, ready, isOperator, isUser, signOut } = useAuth();
  const [navOpen, setNavOpen] = useState(false);
  const [signingOut, setSigningOut] = useState(false);
  const location = useLocation();
  const navigate = useNavigate();
  const { show } = useToast();
  const drawer = useDrawerLayout();
  const menuBtn = useRef<HTMLButtonElement>(null);
  const closeBtn = useRef<HTMLButtonElement>(null);
  const wasOpen = useRef(false);
  const leaving = useRef<(() => void) | null>(null);
  useEffect(() => () => { leaving.current?.(); }, []);

  /**
   * Sign-out policy: end the session on the server first; only then clear the client and leave
   * the dashboard for the public home page (replace, so Back does not return to a private view;
   * private routes re-check the session anyway). If the server cannot be reached the session is
   * kept and the user is told - the UI never pretends to have signed out.
   */
  const handleSignOut = async () => {
    if (signingOut) return;
    setSigningOut(true);
    try {
      // Leave the private page first (navigation is a transition: wait until this shell has unmounted),
      // then the session state is cleared - so the route guard never bounces to the sign-in page.
      await signOut(() => new Promise<void>((resolve) => { leaving.current = resolve; navigate("/", { replace: true, state: { signedOut: true } }); }));
      show(t("toast.signedOut"), "good");
    } catch {
      show(t("toast.signOutFailed"), "bad");
    } finally {
      setSigningOut(false);
    }
  };
  const status = useResource((s) => isOperator ? api<SystemStatus>("/system/status", { signal: s }) : Promise.resolve(null), [isOperator], 60000);
  // A member with a temporary password may not read alerts yet (403): do not ask.
  const alertsAllowed = isUser && !principal.mustChangePassword;
  const alerts = useResource((s) => alertsAllowed ? api<AlertEventList>("/alerts/events", { query: { limit: 1, unread_only: true }, signal: s }).catch(() => null) : Promise.resolve(null), [alertsAllowed], 60000);
  const unread = alerts.data?.unread ?? 0;

  useEffect(() => { setNavOpen(false); try { window.scrollTo(0, 0); } catch { /* jsdom */ } }, [location.pathname]);
  // Drawer focus: move into the drawer when it opens, back to the menu button when it closes.
  useEffect(() => {
    if (navOpen) { wasOpen.current = true; closeBtn.current?.focus(); }
    else if (wasOpen.current) { wasOpen.current = false; menuBtn.current?.focus(); }
  }, [navOpen]);
  useEffect(() => {
    if (!navOpen) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setNavOpen(false); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [navOpen]);

  const groups = isOperator ? operatorNav() : customerNav(unread);
  const search = new URLSearchParams(location.search);
  const home = isOperator ? "/operasi" : "/ringkasan";
  const navLabel = isOperator ? t("nav.console") : t("nav.workspace");

  const renderItem = ({ to, label, icon: Icon, end, badge, match }: NavItem) => {
    const inner = (<>
      <Icon size={18} aria-hidden="true" />{t(label)}
      {badge ? <span className="nav-badge" aria-label={t("alert.unread", { n: badge })}>{badge}</span> : null}
    </>);
    if (match) {
      const active = match(location.pathname, search);
      return <Link key={to} to={to} className={active ? "active" : undefined} aria-current={active ? "page" : undefined}>{inner}</Link>;
    }
    return <NavLink key={to} to={to} end={end} className={({ isActive }) => (isActive ? "active" : undefined)}>{inner}</NavLink>;
  };

  const isOpsPage = location.pathname.startsWith("/operasi");
  return (
    <div className={`app${isOperator ? " app-operator" : ""}`}>
      <a href="#main" className="skip-link">{t("shell.skip")}</a>
      <header className="topbar">
        <Brand to={home} />
        <div className="row topbar-actions">
          <LanguageToggle dark />
          <button ref={menuBtn} className="icon-btn" onClick={() => setNavOpen(true)} aria-label={t("nav.open")} aria-expanded={navOpen} aria-controls="sidebar"><Menu size={20} /></button>
        </div>
      </header>
      {navOpen && <div className="sidebar-scrim" onClick={() => setNavOpen(false)} aria-hidden="true" />}

      <aside className={`sidebar${navOpen ? " open" : ""}`} id="sidebar" inert={drawer && !navOpen ? true : undefined}
        aria-label={drawer ? navLabel : undefined}>
        <div className="sidebar-top">
          <Brand to={home} />
          {navOpen && <button ref={closeBtn} className="btn ghost sm btn-ghost-dark" onClick={() => setNavOpen(false)} aria-label={t("nav.close")}><X size={16} /></button>}
        </div>
        <nav className="nav" aria-label={navLabel}>
          {groups.map((g) => (
            <div key={g.label} className="nav-group">
              <div className="nav-section">{t(g.label)}</div>
              {g.items.map(renderItem)}
            </div>
          ))}
        </nav>

        <div className="sidebar-foot">
          <div className="hide-mobile"><LanguageToggle dark /></div>
          {isOperator && (
            <div className="env-card">
              <div className="row" style={{ justifyContent: "space-between" }}>
                <span>{t("shell.system")}</span>
                {status.data ? <StatusBadge status={status.data.status} /> : <span className="xs">{status.error ? t("shell.unreachable") : t("shell.checking")}</span>}
              </div>
              {status.data && <div className="xs env-meta">{status.data.environment} · API {status.data.version}</div>}
            </div>
          )}
          <div className="user-card">
            <span className="avatar" aria-hidden="true">{isOperator ? "OP" : (principal.name ?? "?").slice(0, 2).toUpperCase()}</span>
            <div className="grow" style={{ minWidth: 0 }}>
              <div className="truncate user-name">{principal.name}</div>
              {isOperator ? <div className="xs user-meta">{t("shell.operator")}</div> : (<>
                <div className="xs user-meta truncate" title={principal.orgName ?? undefined}>{principal.orgName} · {t(`acct.role.${principal.role}` as Key)}</div>
                {principal.plan && <div className="xs user-plan">{t("shell.planLine", { plan: td("plan", principal.plan, principal.plan) })}</div>}
              </>)}
            </div>
            <button className="btn sm btn-ghost-dark" onClick={() => void handleSignOut()} disabled={signingOut} aria-busy={signingOut || undefined}
              aria-label={signingOut ? t("shell.signingOut") : t("shell.signout")} title={t("shell.signout")}>
              {signingOut ? <LoaderCircle size={15} className="spin" aria-hidden="true" /> : <LogOut size={15} aria-hidden="true" />}
            </button>
          </div>
          {isUser && (
            <nav className="nav nav-account" aria-label={t("nav.account")}>
              <NavLink to="/akun" className={({ isActive }) => (isActive ? "active" : undefined)}><Settings size={18} aria-hidden="true" />{t("nav.account")}</NavLink>
            </nav>
          )}
        </div>
      </aside>

      <main className="main" id="main" lang={locale} tabIndex={-1}>
        {isOperator && (
          <div className="console-band" role="note">
            <ShieldCheck size={15} aria-hidden="true" />
            {status.data ? t("shell.consoleBand", { env: status.data.environment, v: status.data.version }) : t("shell.consoleBandShort")}
          </div>
        )}
        {principal.mustChangePassword && location.pathname !== "/akun" && (
          <div className="callout warn" role="alert" style={{ marginBottom: 16 }}>
            <Bell size={18} aria-hidden="true" />
            <div className="grow">{t("shell.mustChangePassword")}</div>
            <Link className="btn sm primary" to="/akun#keamanan">{t("acct.changePassword")}</Link>
          </div>
        )}
        {/* Until the server has confirmed who is signed in, render a neutral skeleton instead of
            guessing. The key remounts page state when the route or the signed-in principal
            changes, so private data never lingers after sign-out or a session change. */}
        {ready ? (
          <div key={`${principal.kind}:${principal.orgId ?? ""}:${location.pathname}`} className="route-view">{children}</div>
        ) : (
          <div className="page" role="status" aria-label={t("common.loading")}><Skeleton height={36} width="40%" /><Skeleton height={120} /><Skeleton height={320} /></div>
        )}
        {!(isOperator && isOpsPage) && <ShellFooter />}
      </main>
    </div>
  );
}
