/**
 * Application shell: sidebar with the two product areas (Market Intelligence for everyone,
 * Platform Operations for operators), language toggle, principal card and alert inbox badge.
 * Internal details (environment, API version, operator sign-in) are only shown to operators;
 * end users get a product footer with the legal links.
 */
import type { AlertEventList, SystemStatus } from "../api/types";
import {
  Activity, Bell, ChartLine, Database, FileText, FlaskConical, Globe, Home, LayoutDashboard, Lightbulb, LogIn, LogOut,
  LoaderCircle, Menu, ScrollText, Settings, ShieldCheck, SlidersHorizontal, UserRound, Workflow, X, Building2, type LucideIcon,
} from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { Link, NavLink, useLocation, useNavigate } from "react-router-dom";
import { api } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { useT, type Key } from "../i18n";
import { LogoSymbol } from "./brand";
import { useToast } from "./toast";
import { Skeleton, StatusBadge } from "./ui";

interface NavItem { to: string; label: Key; icon: LucideIcon; end?: boolean; badge?: number }

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

function Brand({ to = "/" }: { to?: string }) {
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

/** Product footer under every shell page: legal links and the no-advice note. */
export function ShellFooter() {
  const { t } = useT();
  return (
    <footer className="main-foot">
      <span>{t("shell.footer.note")}</span>
      <nav className="row" aria-label={t("landing.footer.terms")}>
        <Link to="/syarat">{t("landing.footer.terms")}</Link>
        <Link to="/privasi">{t("landing.footer.privacy")}</Link>
      </nav>
    </footer>
  );
}

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

export function AppShell({ children }: { children: ReactNode }) {
  const { t, locale } = useT();
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
      await signOut();
      navigate("/", { replace: true });
      show(t("toast.signedOut"), "good");
    } catch {
      show(t("toast.signOutFailed"), "bad");
    } finally {
      setSigningOut(false);
    }
  };
  const status = useResource((s) => isOperator ? api<SystemStatus>("/system/status", { signal: s }) : Promise.resolve(null), [isOperator], 60000);
  const alerts = useResource((s) => isUser ? api<AlertEventList>("/alerts/events", { query: { limit: 1, unread_only: true }, signal: s }) : Promise.resolve(null), [isUser], 60000);
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

  const market: NavItem[] = [
    { to: "/ringkasan", label: "nav.overview", icon: LayoutDashboard },
    { to: "/harga", label: "nav.explorer", icon: ChartLine },
    { to: "/proyeksi", label: "nav.forecast", icon: Activity },
    { to: "/skenario", label: "nav.scenarios", icon: SlidersHorizontal },
    { to: "/insight", label: "nav.insights", icon: Lightbulb },
    { to: "/laporan", label: "nav.reports", icon: FileText, badge: unread },
  ];
  const ops: NavItem[] = [
    { to: "/operasi", label: "nav.ops.health", icon: ShieldCheck, end: true },
    { to: "/operasi/data", label: "nav.ops.data", icon: Database },
    { to: "/operasi/model", label: "nav.ops.models", icon: FlaskConical },
    { to: "/operasi/pekerjaan", label: "nav.ops.jobs", icon: Workflow },
    { to: "/operasi/audit", label: "nav.ops.audit", icon: ScrollText },
    { to: "/operasi/organisasi", label: "nav.ops.tenants", icon: Building2 },
  ];

  const renderItems = (items: NavItem[]) => items.map(({ to, label, icon: Icon, end, badge }) => (
    <NavLink key={to} to={to} end={end} className={({ isActive }) => (isActive ? "active" : undefined)}>
      <Icon size={18} aria-hidden="true" />{t(label)}
      {badge ? <span className="nav-badge" aria-label={t("alert.unread", { n: badge })}>{badge}</span> : null}
    </NavLink>
  ));

  return (
    <div className="app">
      <a href="#main" className="skip-link">{t("shell.skip")}</a>
      <header className="topbar">
        <Brand />
        <div className="row" style={{ gap: 8 }}>
          <LanguageToggle dark />
          <button ref={menuBtn} className="icon-btn" onClick={() => setNavOpen(true)} aria-label={t("nav.open")} aria-expanded={navOpen} aria-controls="sidebar"><Menu size={20} /></button>
        </div>
      </header>
      {navOpen && <div className="sidebar-scrim" onClick={() => setNavOpen(false)} aria-hidden="true" />}

      <aside className={`sidebar${navOpen ? " open" : ""}`} id="sidebar" inert={drawer && !navOpen ? true : undefined}
        aria-label={drawer ? t("nav.market") : undefined}>
        <div className="row-between">
          <Brand />
          {navOpen && <button ref={closeBtn} className="btn ghost sm btn-ghost-dark" onClick={() => setNavOpen(false)} aria-label={t("nav.close")}><X size={16} /></button>}
        </div>
        <nav className="nav" aria-label={t("nav.market")}>
          <div className="nav-section">{t("nav.market")}</div>
          {renderItems(market)}
          {(isUser || isOperator) && (
            <NavLink to="/akun" className={({ isActive }) => (isActive ? "active" : undefined)}>
              <Settings size={18} aria-hidden="true" />{t("nav.account")}
            </NavLink>
          )}
          {isOperator && (<><div className="nav-section">{t("nav.ops")}</div>{renderItems(ops)}</>)}
          {principal.kind === "public" && (<>
            <div className="nav-section" />
            <NavLink to="/" end className={({ isActive }) => (isActive ? "active" : undefined)}><Home size={18} aria-hidden="true" />{t("shell.landing")}</NavLink>
          </>)}
        </nav>

        <div className="sidebar-foot">
          <div className="hide-mobile"><LanguageToggle dark /></div>
          {isOperator && (
            <div className="env-card">
              <div className="row" style={{ justifyContent: "space-between" }}>
                <span>{t("shell.system")}</span>
                {status.data ? <StatusBadge status={status.data.status} /> : <span className="xs">{status.error ? t("shell.unreachable") : t("shell.checking")}</span>}
              </div>
              {status.data && <div className="xs" style={{ marginTop: 6, color: "#8fa0b6" }}>{status.data.environment} · API {status.data.version}</div>}
            </div>
          )}
          {principal.kind !== "public" ? (
            <div className="user-card">
              <span className="avatar" aria-hidden="true">{isOperator ? "OP" : (principal.name ?? "?").slice(0, 2).toUpperCase()}</span>
              <div className="grow" style={{ minWidth: 0 }}>
                <div className="truncate" style={{ color: "#fff", fontWeight: 600, fontSize: 13.5 }}>{principal.name}</div>
                <div className="xs truncate" style={{ color: "#8fa0b6" }}>
                  {isOperator ? t("shell.operator") : `${principal.orgName ?? ""} · ${t(`acct.role.${principal.role}` as Key)}${principal.plan ? ` · ${t("shell.plan", { plan: principal.plan })}` : ""}`}
                </div>
              </div>
              <button className="btn sm btn-ghost-dark" onClick={() => void handleSignOut()} disabled={signingOut} aria-busy={signingOut || undefined}
                aria-label={signingOut ? t("shell.signingOut") : t("shell.signout")} title={t("shell.signout")}>
                {signingOut ? <LoaderCircle size={15} className="spin" aria-hidden="true" /> : <LogOut size={15} aria-hidden="true" />}
              </button>
            </div>
          ) : (
            <div className="stack-sm">
              <Link className="btn primary" to={`/masuk?next=${encodeURIComponent(location.pathname + location.search)}`}><LogIn size={16} aria-hidden="true" />{t("shell.signin")}</Link>
              <Link className="btn btn-ghost-dark" to="/daftar"><UserRound size={16} aria-hidden="true" />{t("shell.register")}</Link>
            </div>
          )}
        </div>
      </aside>

      <main className="main" id="main" lang={locale}>
        {principal.mustChangePassword && location.pathname !== "/akun" && (
          <div className="callout warn" role="alert" style={{ marginBottom: 16 }}>
            <Bell size={18} aria-hidden="true" />
            <div className="grow">{t("shell.mustChangePassword")}</div>
            <Link className="btn sm primary" to="/akun#keamanan">{t("acct.changePassword")}</Link>
          </div>
        )}
        {/* Until the server has confirmed who is signed in, render a neutral skeleton instead of
            guessing (prevents a "sign in required" flash on deep links to private pages). The key
            remounts page state when the route or the signed-in principal changes, so private data
            never lingers after sign-out or a session change. */}
        {ready ? (
          <div key={`${principal.kind}:${principal.orgId ?? ""}:${location.pathname}`} className="route-view">{children}</div>
        ) : (
          <div className="page" role="status" aria-label={t("common.loading")}><Skeleton height={36} width="40%" /><Skeleton height={120} /><Skeleton height={320} /></div>
        )}
        <ShellFooter />
      </main>
    </div>
  );
}
