/**
 * Routes and the product boundary.
 * - Public site (no app shell): /, /metodologi, /masuk, /daftar, /syarat, /privasi.
 * - Workspace (RequireUser): guests are sent to /masuk?next=<path> before any shell renders.
 *   Market pages are open to customers and operators (operators see them under "Pratinjau produk").
 * - Customer-only (RequireCustomer): scenarios, reports & notifications, account. Operators get a
 *   notice, not an error (their organisation endpoints are forbidden server-side).
 * - Operations (RequireOperator): /operasi/*. Customers get a plain "not found".
 * - A member with a temporary password is kept on /akun#keamanan until it is replaced.
 * Guards wait for the server-confirmed principal (`ready`), so a deep link never flashes a redirect.
 * A stored credential the server could not check (timeout, 5xx, offline; contract C3) is not a
 * signed-out member: workspace routes explain it and retry instead of redirecting to sign-in.
 *
 * Code splitting: the entry bundle holds the public landing only. The signed-in shell, the sign-in and
 * registration pages, the other public pages and every workspace page are separate chunks (with their
 * strings and stylesheets), fetched ahead of need: the shell and the page of a workspace URL together
 * while the stored credential is checked (on the home page the shell alone), the auth pages when the
 * public site is idle or a sign-in link is hovered or focused. scripts/check-landing-bundle.mjs guards
 * the entry bundle in CI; vite.config.ts keeps the shell and its small helpers in one chunk.
 */
import { Building2, RefreshCw, ShieldCheck } from "lucide-react";
import { lazy, Suspense, useEffect, useState, type ReactNode, type SyntheticEvent } from "react";
import { Link, Navigate, Outlet, Route, Routes, useLocation } from "react-router-dom";
import { getOperatorToken, getSessionToken, publicGetMeta } from "./api/client";
import { useAuth } from "./auth";
import { PublicLayout } from "./components/public-layout";
import { Button, Callout, ErrorState, PageHeader, Skeleton, useSlow } from "./components/ui";
import { useT, type Key } from "./i18n";
import LandingPage from "./pages/LandingPage";

// Each loader is a plain function, so a chunk can be fetched before React renders its component.
const loadShell = () => import("./components/layout");
const loadAuthPages = () => import("./pages/AuthPages");
const loadOverview = () => import("./pages/OverviewPage");
const loadExplorer = () => import("./pages/ExplorerPage");
const loadForecast = () => import("./pages/ForecastPage");
const loadScenarios = () => import("./pages/ScenariosPage");
const loadInsights = () => import("./pages/InsightsPage");
const loadReports = () => import("./pages/ReportsPage");
const loadNotifications = () => import("./pages/NotificationsPage");
const loadAccount = () => import("./pages/AccountPage");
const loadHealth = () => import("./pages/ops/HealthPage");
const loadDataOps = () => import("./pages/ops/DataOpsPage");
const loadModelsOps = () => import("./pages/ops/ModelsOpsPage");
const loadJobs = () => import("./pages/ops/JobsPage");
const loadAudit = () => import("./pages/ops/AuditPage");
const loadTenants = () => import("./pages/ops/TenantsPage");

const AppShell = lazy(() => loadShell().then((m) => ({ default: m.AppShell })));
const LoginPage = lazy(() => loadAuthPages().then((m) => ({ default: m.LoginPage })));
const RegisterPage = lazy(() => loadAuthPages().then((m) => ({ default: m.RegisterPage })));
const MethodologyPage = lazy(() => import("./pages/MethodologyPage"));
// Legal texts are long and rarely read: keep them out of the entry bundle.
const TermsPage = lazy(() => import("./pages/LegalPages").then((m) => ({ default: m.TermsPage })));
const PrivacyPage = lazy(() => import("./pages/LegalPages").then((m) => ({ default: m.PrivacyPage })));
const OverviewPage = lazy(loadOverview);
const ExplorerPage = lazy(loadExplorer);
const ForecastPage = lazy(loadForecast);
const ScenariosPage = lazy(loadScenarios);
const InsightsPage = lazy(loadInsights);
const ReportsPage = lazy(loadReports);
const NotificationsPage = lazy(loadNotifications);
const AccountPage = lazy(loadAccount);
const HealthPage = lazy(loadHealth);
const DataOpsPage = lazy(loadDataOps);
const ModelsOpsPage = lazy(loadModelsOps);
const JobsPage = lazy(loadJobs);
const AuditPage = lazy(loadAudit);
const TenantsPage = lazy(loadTenants);

/** The page module of each workspace path, fetched together with the shell instead of after it. */
const WORKSPACE_PAGES: Record<string, () => Promise<unknown>> = {
  "/ringkasan": loadOverview, "/harga": loadExplorer, "/proyeksi": loadForecast, "/skenario": loadScenarios,
  "/insight": loadInsights, "/laporan": loadReports, "/pemberitahuan": loadNotifications, "/akun": loadAccount,
  "/operasi": loadHealth, "/operasi/data": loadDataOps, "/operasi/model": loadModelsOps,
  "/operasi/pekerjaan": loadJobs, "/operasi/audit": loadAudit, "/operasi/organisasi": loadTenants,
};

/** Fetch ahead of need. A failure is ignored here: React reports it when it renders the component. */
function prefetch(...loaders: Array<(() => Promise<unknown>) | undefined>): void {
  for (const load of loaders) load?.().catch(() => undefined);
}

/** The shell first, so its stylesheet precedes the page's (the cascade order of the unsplit build). */
const preloadWorkspace = (pathname: string) => prefetch(loadShell, WORKSPACE_PAGES[pathname]);

const hasStoredCredential = () => !!(getSessionToken() || getOperatorToken());

/** Runs `task` once the page has loaded and the browser is idle; returns a cancel function. */
function whenIdle(task: () => void): () => void {
  let cancelled = false;
  let idleId: number | undefined;
  let timerId: number | undefined;
  const schedule = () => {
    if (cancelled) return;
    if (typeof window.requestIdleCallback === "function") idleId = window.requestIdleCallback(() => task(), { timeout: 4000 });
    else timerId = window.setTimeout(task, 1000);
  };
  if (document.readyState === "complete") schedule();
  else window.addEventListener("load", schedule, { once: true });
  return () => {
    cancelled = true;
    window.removeEventListener("load", schedule);
    if (idleId !== undefined) window.cancelIdleCallback?.(idleId);
    if (timerId !== undefined) window.clearTimeout(timerId);
  };
}

const AUTH_PATHS = new Set(["/masuk", "/daftar"]);

/** Pointer or keyboard on a sign-in or registration link: fetch the auth pages now. */
function prefetchAuthOnIntent(event: SyntheticEvent) {
  const link = event.target instanceof Element ? event.target.closest("a[href]") : null;
  if (link instanceof HTMLAnchorElement && AUTH_PATHS.has(link.pathname)) prefetch(loadAuthPages);
}

/**
 * Page placeholder. `checking`: the stored credential is still being checked (the `!ready` branches),
 * and a check that takes longer than a moment says so (up to the 8 s whoami limit). Without it the
 * placeholder stands in for a lazily loaded chunk (the shell or a page) after the session was
 * confirmed, and a slow chunk says only that the page is loading.
 */
function Fallback({ checking = false }: { checking?: boolean }) {
  const { t } = useT();
  const slow = useSlow(true, 1500);
  return (
    <div className="page no-anim boot" role="status" aria-label={t("common.loading")}>
      {slow && <p className="small muted boot-note">{t(checking ? "shell.bootChecking" : "shell.loadingPage")}</p>}
      <Skeleton height={120} /><Skeleton height={320} />
    </div>
  );
}

/** Public routes wait for their (prefetched, small) chunk without a page skeleton. */
function PublicFallback() {
  const { t } = useT();
  return <div role="status" aria-label={t("common.loading")} />;
}

/** Workspace routes: signed-in principals only. Guests go to sign-in with the deep link preserved. */
function RequireUser() {
  const { principal, ready, isUser, sessionCheckError } = useAuth();
  const location = useLocation();
  const { pathname } = location;
  // Signed in, or a stored credential that is being checked: fetch the shell and this page meanwhile.
  const likelySignedIn = ready ? principal.kind !== "public" : hasStoredCredential();
  useEffect(() => { if (likelySignedIn) preloadWorkspace(pathname); }, [likelySignedIn, pathname]);
  if (!ready) return <Fallback checking />;
  if (principal.kind === "public") {
    if (sessionCheckError) return <PublicLayout><SessionUnconfirmed error={sessionCheckError} /></PublicLayout>;
    const next = encodeURIComponent(location.pathname + location.search + location.hash);
    const operator = location.pathname.startsWith("/operasi") ? "&operator=1" : "";
    return <Navigate to={`/masuk?next=${next}${operator}`} replace />;
  }
  if (isUser && principal.mustChangePassword && location.pathname !== "/akun") return <Navigate to="/akun#keamanan" replace />;
  return <Suspense fallback={<Fallback />}><AppShell><Suspense fallback={<Fallback />}><Outlet /></Suspense></AppShell></Suspense>;
}

/**
 * The stored credential could not be checked: the server gave no answer in time, answered 5xx, or the
 * device is offline. The member is not signed out: the credential is kept and re-checked in the
 * background (auth.tsx), and this page offers an immediate retry instead of the sign-in form.
 */
function SessionUnconfirmed({ error }: { error: unknown }) {
  const { t } = useT();
  const { refresh } = useAuth();
  const [busy, setBusy] = useState(false);
  const retry = () => { setBusy(true); void refresh().finally(() => setBusy(false)); };
  return (
    <div className="page"><div className="panel"><div className="panel-body stack-sm">
      <h1 className="page-title">{t("shell.sessionUnconfirmed")}</h1>
      <p className="muted">{t("shell.sessionUnconfirmedBody")}</p>
      <ErrorState error={error} compact signinLink={false} />
      <div className="row">
        <Button variant="primary" icon={RefreshCw} loading={busy} onClick={retry}>{t("common.retry")}</Button>
        <Link className="btn" to="/">{t("shell.landing")}</Link>
      </div>
    </div></div></div>
  );
}

/** Pages that belong to a customer organisation (operators are forbidden from those endpoints). */
function RequireCustomer() {
  const { isOperator } = useAuth();
  return isOperator ? <CustomerOnlyNotice /> : <Outlet />;
}

/** Operations console: operators only; everyone else sees a plain "not found" (the area is not advertised). */
function RequireOperator() {
  const { isOperator } = useAuth();
  return isOperator ? <Outlet /> : <NotFound />;
}

function CustomerOnlyNotice() {
  const { t } = useT();
  return (
    <div className="page">
      <PageHeader eyebrow={t("shell.consoleBandShort")} icon={ShieldCheck} title={t("shell.customerOnlyTitle")} />
      <Callout tone="info" action={<div className="row"><Link className="btn sm primary" to="/operasi/organisasi"><Building2 size={15} aria-hidden="true" />{t("shell.toOrgs")}</Link><Link className="btn sm" to="/operasi">{t("shell.toConsole")}</Link></div>}>
        {t("shell.customerOnlyBody")}
      </Callout>
    </div>
  );
}

/** Old links to the notifications tab (`/laporan?tab=pemberitahuan`) now open the notifications page. */
function LegacyAlertsTab({ children }: { children: ReactNode }) {
  const { search } = useLocation();
  return new URLSearchParams(search).get("tab") === "pemberitahuan" ? <Navigate to="/pemberitahuan" replace /> : <>{children}</>;
}

/**
 * Unknown page. The decorative illustration (an original SVG) draws a chart whose line breaks at a
 * missing month, the way the product marks months without an official value.
 */
function NotFound() {
  const { t } = useT();
  return (
    <div className="page"><div className="panel"><div className="panel-body not-found">
      <img className="not-found-art" src="/img/illus/not-found.svg" width={240} height={160} alt="" aria-hidden="true" decoding="async" />
      <div className="not-found-text">
        <h1 className="page-title">{t("shell.notFound")}</h1><p className="muted mt-2">{t("shell.notFoundHint")}</p>
        <Link className="btn not-found-home" to="/">{t("shell.notFoundHome")}</Link>
      </div>
    </div></div></div>
  );
}

/** Unknown URL: inside the shell for signed-in principals, on the public site for guests. */
function NotFoundAnywhere() {
  const { principal, ready } = useAuth();
  if (!ready) return <Fallback checking />;
  if (principal.kind === "public") return <PublicLayout><NotFound /></PublicLayout>;
  return <Suspense fallback={<Fallback />}><AppShell><NotFound /></AppShell></Suspense>;
}

/**
 * How long the home page waits for the stored credential's check before it shows the public landing
 * anyway (ms). A quick answer still takes a returning member straight to the workspace; a slow one
 * (up to the 8 s whoami timeout) no longer keeps them in front of a whole-page skeleton.
 */
const HOME_SESSION_WAIT_MS = 2000;

function Home() {
  const { principal, ready } = useAuth();
  const location = useLocation();
  // Arriving from sign-out: show the public home even while the session state is being cleared.
  const signedOut = !!(location.state as { signedOut?: boolean } | null)?.signedOut;
  const [waited, setWaited] = useState(false);
  // A stored credential is probably a returning member on the way to the workspace: fetch the shell meanwhile.
  // The landing's public data is requested meanwhile too, so a landing shown after the wait (slow or
  // failing check) has its data, or its explained error, about 2 s sooner. Anonymous and shared with
  // the landing's own requests (api/client publicGetMeta), so nothing is fetched twice.
  useEffect(() => {
    if (ready || !hasStoredCredential()) return;
    prefetch(loadShell);
    void publicGetMeta("/public/market-snapshot").catch(() => undefined);
    void publicGetMeta("/plans").catch(() => undefined);
  }, [ready]);
  useEffect(() => {
    if (ready) return;
    const id = window.setTimeout(() => setWaited(true), HOME_SESSION_WAIT_MS);
    return () => window.clearTimeout(id);
  }, [ready]);
  const landing = ready ? signedOut || principal.kind === "public" : waited;
  // Once the landing is on screen it stays: a session confirmed later (a slow check, or the background
  // re-check after an unreachable server) must not move the reader away mid-page; the header then
  // offers the workspace.
  const [landed, setLanded] = useState(false);
  useEffect(() => { if (landing) setLanded(true); }, [landing]);
  if (landing || landed) return <LandingPage />;
  if (!ready) return <Fallback checking />;
  return <Navigate to={principal.kind === "operator" ? "/operasi" : "/ringkasan"} replace />;
}

/**
 * Public pages fade in on navigation; signed-in pages animate inside the shell. Once a public page is
 * idle the sign-in and registration chunk is fetched (and on the sign-in and registration pages the
 * shell), so the next step does not wait for the network.
 */
function PublicRoutes() {
  const location = useLocation();
  const onAuthPage = AUTH_PATHS.has(location.pathname);
  useEffect(() => whenIdle(() => (onAuthPage ? prefetch(loadShell) : prefetch(loadAuthPages))), [onAuthPage]);
  return (
    <div key={location.pathname} className="route-fade" onPointerOver={prefetchAuthOnIntent} onFocus={prefetchAuthOnIntent} onTouchStart={prefetchAuthOnIntent}>
      <Suspense fallback={<PublicFallback />}><Outlet /></Suspense>
    </div>
  );
}

const TITLES: Record<string, Key> = {
  "/metodologi": "nav.methodology", "/masuk": "shell.signin", "/daftar": "shell.register", "/syarat": "legal.termsTitle", "/privasi": "legal.privacyTitle",
  "/ringkasan": "nav.overview", "/harga": "nav.explorer", "/proyeksi": "nav.forecast", "/skenario": "nav.scenarios", "/insight": "nav.insights",
  "/laporan": "nav.reports", "/pemberitahuan": "nav.alerts", "/akun": "nav.account", "/operasi": "nav.ops.health", "/operasi/data": "nav.ops.data", "/operasi/model": "nav.ops.models",
  "/operasi/pekerjaan": "nav.ops.jobs", "/operasi/audit": "nav.ops.audit", "/operasi/organisasi": "nav.ops.tenants",
};

/** Every route gets its own document title ("<Halaman> · ICP Intelligence"), in the active language. */
function DocumentTitle() {
  const { t } = useT();
  const { pathname } = useLocation();
  const { principal, ready } = useAuth();
  // Signed-in customers get a plain "not found" on the operations console (RequireOperator), so the
  // tab title must not name the console either (UX-06). Guests are sent to sign-in instead.
  const hidden = pathname.startsWith("/operasi") && ready && principal.kind === "user";
  useEffect(() => {
    const brand = t("brand.name");
    const key: Key | undefined = hidden ? undefined : TITLES[pathname];
    document.title = pathname === "/" ? `${brand} · ${t("brand.tagline")}` : key ? `${t(key)} · ${brand}` : `${t("shell.notFound")} · ${brand}`;
  }, [pathname, t, hidden]);
  return null;
}

export default function App() {
  return (
    <>
      <DocumentTitle />
      <Routes>
        <Route element={<PublicRoutes />}>
          <Route path="/" element={<Home />} />
          <Route path="/metodologi" element={<MethodologyPage />} />
          <Route path="/masuk" element={<LoginPage />} />
          <Route path="/daftar" element={<RegisterPage />} />
          <Route path="/syarat" element={<TermsPage />} />
          <Route path="/privasi" element={<PrivacyPage />} />
        </Route>
        <Route element={<RequireUser />}>
          <Route path="/ringkasan" element={<OverviewPage />} />
          <Route path="/harga" element={<ExplorerPage />} />
          <Route path="/proyeksi" element={<ForecastPage />} />
          <Route path="/insight" element={<InsightsPage />} />
          <Route element={<RequireCustomer />}>
            <Route path="/skenario" element={<ScenariosPage />} />
            <Route path="/laporan" element={<LegacyAlertsTab><ReportsPage /></LegacyAlertsTab>} />
            <Route path="/pemberitahuan" element={<NotificationsPage />} />
            <Route path="/akun" element={<AccountPage />} />
          </Route>
          <Route element={<RequireOperator />}>
            <Route path="/operasi" element={<HealthPage />} />
            <Route path="/operasi/data" element={<DataOpsPage />} />
            <Route path="/operasi/model" element={<ModelsOpsPage />} />
            <Route path="/operasi/pekerjaan" element={<JobsPage />} />
            <Route path="/operasi/audit" element={<AuditPage />} />
            <Route path="/operasi/organisasi" element={<TenantsPage />} />
          </Route>
        </Route>
        {/* short and legacy routes */}
        <Route path="/forecast" element={<Navigate to="/proyeksi" replace />} />
        <Route path="/data" element={<Navigate to="/operasi/data" replace />} />
        <Route path="/models" element={<Navigate to="/operasi/model" replace />} />
        <Route path="/operations" element={<Navigate to="/operasi" replace />} />
        <Route path="*" element={<NotFoundAnywhere />} />
      </Routes>
    </>
  );
}
