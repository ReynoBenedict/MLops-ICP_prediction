/**
 * Signed-in application shell. Two audiences, two navigations (the public site has its own layout,
 * components/public-layout.tsx):
 * - AppShell for customers: light workspace sidebar (Pasar / Analisis / Keluaran) and an account card.
 * - AppShell for operators: dark, operations-first sidebar, a "Pratinjau produk" group with the
 *   read-only market pages, an environment band and no customer items (scenarios, reports, account).
 * Internal details (environment, API version) are only shown to operators.
 *
 * Top bar (sticky, 56 px): on desktop it sits above the main column with the location line
 * ("Analisis › Proyeksi"), the data status control (customers; components/data-status.tsx) or the
 * environment and version (operators), and the language toggle. Below 900 px it carries the menu
 * button, the wordmark and a compact data status; the language toggle moves into the drawer.
 */
import type { AlertEventList, SystemStatus } from "../api/types";
import {
  Activity, Bell, BookOpen, Building2, ChartLine, ChevronRight, Database, FileText, FlaskConical, LayoutDashboard, Lightbulb, LoaderCircle, LogOut,
  Menu, ScrollText, Settings, ShieldCheck, SlidersHorizontal, Workflow, X, type LucideIcon,
} from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { Link, NavLink, useLocation, useNavigate } from "react-router-dom";
import { api, sharedGet } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { useT, type Key } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../styles/shell.css"; // shell styles, loaded with this lazy chunk (not by the public site)
import { onAlertsChanged } from "../lib/alerts";
import { LogoSymbol } from "./brand";
import { LanguageToggle, useRouteFocus, useStickyTop } from "./chrome";
import { DataStatus } from "./data-status";
import { useModal } from "./modal";
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

/** Logo + product name; `dark` for the operator console's dark sidebar and top bar. */
function Brand({ to, dark }: { to: string; dark?: boolean }) {
  const { t } = useT();
  return (
    <Link to={to} className="brand" aria-label={t("brand.name")}>
      <LogoSymbol size={34} tone={dark ? "teal" : "navy"} />
      <span>
        <span className="brand-name">{t("brand.name")}</span>
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

/* ---------- Signed-in application shell ------------------------------------------------------- */

/** True while the viewport uses the off-canvas navigation drawer (same breakpoint as styles/shell.css). */
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
      { to: "/laporan", label: "nav.reports", icon: FileText },
      { to: "/pemberitahuan", label: "nav.alerts", icon: Bell, badge: unread },
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

/** Matches a navigation item against the current path (same rule as NavLink: exact for `end`, else prefix). */
function itemMatches(item: NavItem, pathname: string): boolean {
  return item.end ? pathname === item.to : pathname === item.to || pathname.startsWith(`${item.to}/`);
}

/** "Group › Page" for the top bar, from the navigation the reader sees; null when the page is not in it. */
export function locationTrail(groups: NavGroup[], pathname: string, extra: NavItem[] = []): { group: Key | null; page: Key } | null {
  for (const g of groups) {
    const hit = g.items.find((i) => itemMatches(i, pathname));
    if (hit) return { group: g.label, page: hit.label };
  }
  const own = extra.find((i) => itemMatches(i, pathname));
  return own ? { group: null, page: own.label } : null;
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
  const asideRef = useRef<HTMLElement>(null);
  const topbarRef = useRef<HTMLElement>(null);
  const mainRef = useRef<HTMLElement>(null);
  const skipRef = useRef<HTMLAnchorElement>(null);
  const navigated = useRef(false);
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
  const status = useResource((s) => isOperator ? sharedGet<SystemStatus>("/system/status", { signal: s }) : Promise.resolve(null), [isOperator], 60000);
  // A member with a temporary password may not read alerts yet (403): do not ask.
  const alertsAllowed = isUser && !principal.mustChangePassword;
  const alerts = useResource((s) => alertsAllowed ? api<AlertEventList>("/alerts/events", { query: { limit: 1, unread_only: true }, signal: s }).catch(() => null) : Promise.resolve(null), [alertsAllowed], 60000);
  const unread = alerts.data?.unread ?? 0;
  const reloadAlerts = alerts.reload;
  // Rules created, events read or evaluated anywhere in the app: refresh the badge at once.
  useEffect(() => onAlertsChanged(reloadAlerts), [reloadAlerts]);

  // Navigation: close the drawer without sending focus back to the menu button (the new page's
  // heading receives focus instead), and start the new page at the top.
  useStickyTop(topbarRef);
  const announce = useRouteFocus(mainRef, () => {
    navigated.current = true;
    setNavOpen(false);
    try { window.scrollTo(0, 0); } catch { /* jsdom */ }
  });
  // The drawer (≤ 900 px) is a modal dialog: focus trapped, page behind inert and not scrollable,
  // Escape or the scrim close it, focus returns to the menu button.
  const drawerOpen = drawer && navOpen;
  useEffect(() => { if (navOpen) navigated.current = false; }, [navOpen]);
  useEffect(() => { if (!drawer) setNavOpen(false); }, [drawer]);
  useModal(drawerOpen, asideRef, {
    onClose: () => setNavOpen(false),
    inertTargets: () => [topbarRef.current, mainRef.current, skipRef.current],
    returnFocus: () => !navigated.current,
  });

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
  const trail = locationTrail(groups, location.pathname, isUser ? [{ to: "/akun", label: "nav.account", icon: Settings }] : []);
  // The data status reads /data/summary; a member who must replace a temporary password may not yet.
  const dataStatusAllowed = isUser && ready && !principal.mustChangePassword;
  const envStatus = status.data
    ? <><StatusBadge status={status.data.status} /><span className="topbar-env-text">{t("shell.env", { env: status.data.environment, v: status.data.version })}</span></>
    : <span className="topbar-env-text">{status.error ? t("shell.unreachable") : t("shell.checking")}</span>;
  return (
    <div className={`app${isOperator ? " app-operator" : ""}`}>
      <a href="#main" className="skip-link" ref={skipRef}>{t("shell.skip")}</a>
      <div className="sr-only" role="status" aria-live="polite">{announce}</div>
      {drawerOpen && <div className="sidebar-scrim" onClick={() => setNavOpen(false)} aria-hidden="true" />}

      <aside ref={asideRef} className={`sidebar${navOpen ? " open" : ""}`} id="sidebar" inert={drawer && !navOpen ? true : undefined}
        role={drawerOpen ? "dialog" : undefined} aria-modal={drawerOpen ? true : undefined} aria-label={drawer ? navLabel : undefined}>
        <div className="sidebar-top">
          <Brand to={home} dark={isOperator} />
          {drawerOpen && <button className="btn ghost sm btn-ghost-dark" onClick={() => setNavOpen(false)} aria-label={t("nav.close")}><X size={16} /></button>}
        </div>
        <nav className="nav nav-main" aria-label={navLabel}>
          {groups.map((g) => (
            <div key={g.label} className="nav-group">
              <div className="nav-section">{t(g.label)}</div>
              {g.items.map(renderItem)}
            </div>
          ))}
        </nav>

        <div className="sidebar-foot">
          {/* In the drawer layout the top bar has no room for these; on desktop the top bar shows them. */}
          <div className="drawer-only"><LanguageToggle dark={isOperator} /></div>
          {isOperator && (
            <div className="env-card drawer-only">
              <div className="row">
                <span>{t("shell.system")}</span>
                {status.data ? <StatusBadge status={status.data.status} /> : <span className="xs">{status.error ? t("shell.unreachable") : t("shell.checking")}</span>}
              </div>
              {status.data && <div className="xs env-meta">{status.data.environment} · API {status.data.version}</div>}
            </div>
          )}
          <div className="user-card">
            <span className="avatar" aria-hidden="true">{isOperator ? "OP" : (principal.name ?? "?").slice(0, 2).toUpperCase()}</span>
            <div className="grow">
              <div className="user-name">{principal.name}</div>
              {isOperator ? <div className="xs user-meta">{t("shell.operator")}</div> : (<>
                <div className="xs user-meta user-org">{principal.orgName}</div><div className="xs user-meta">{t(`acct.role.${principal.role}` as Key)}</div>
                {principal.plan && <div className="xs user-plan">{t("shell.planLine", { plan: td("plan", principal.plan, principal.plan) })}</div>}
              </>)}
            </div>
            <button className="btn sm btn-ghost-dark" onClick={() => void handleSignOut()} disabled={signingOut} aria-busy={signingOut || undefined}
              aria-label={signingOut ? t("shell.signingOut") : t("shell.signout")} title={t("shell.signout")}>
              {signingOut ? <LoaderCircle size={15} className="spin" aria-hidden="true" /> : <LogOut size={15} aria-hidden="true" />}
            </button>
          </div>
          {isUser && (
            <nav className="nav nav-account" aria-label={t("nav.accountHelp")}>
              <NavLink to="/akun" className={({ isActive }) => (isActive ? "active" : undefined)}><Settings size={18} aria-hidden="true" />{t("nav.account")}</NavLink>
              <NavLink to="/metodologi"><BookOpen size={18} aria-hidden="true" />{t("nav.help")}</NavLink>
            </nav>
          )}
        </div>
      </aside>

      <div className="app-main">
      <header className="topbar" ref={topbarRef} aria-label={t("shell.topbar")}>
        <div className="topbar-inner">
          <button ref={menuBtn} type="button" className="icon-btn topbar-menu" onClick={() => setNavOpen(true)} aria-label={t("nav.open")} aria-expanded={navOpen} aria-controls="sidebar"><Menu size={20} aria-hidden="true" /></button>
          <div className="topbar-brand"><Brand to={home} dark={isOperator} /></div>
          {trail && (
            <nav className="topbar-loc" aria-label={t("shell.location")}>
              <ol>
                {trail.group && <li className="topbar-loc-group">{t(trail.group)}<ChevronRight size={14} aria-hidden="true" /></li>}
                <li><span aria-current="page">{t(trail.page)}</span></li>
              </ol>
            </nav>
          )}
          <div className="topbar-actions">
            {isOperator ? <div className="topbar-env">{envStatus}</div> : <DataStatus enabled={dataStatusAllowed} />}
            <div className="topbar-lang"><LanguageToggle dark={isOperator} /></div>
          </div>
        </div>
      </header>
      <main className="main" id="main" ref={mainRef} lang={locale} tabIndex={-1}>
        {isOperator && (
          <div className="console-band" role="note">
            <ShieldCheck size={15} aria-hidden="true" />
            {status.data ? t("shell.consoleBand", { env: status.data.environment, v: status.data.version }) : t("shell.consoleBandShort")}
          </div>
        )}
        {principal.mustChangePassword && location.pathname !== "/akun" && (
          <div className="callout warn shell-notice" role="alert">
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
    </div>
  );
}
