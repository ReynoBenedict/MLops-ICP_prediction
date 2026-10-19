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
 */
import { Building2, ShieldCheck } from "lucide-react";
import { lazy, Suspense, useEffect } from "react";
import { Link, Navigate, Outlet, Route, Routes, useLocation } from "react-router-dom";
import { useAuth } from "./auth";
import { AppShell, PublicLayout } from "./components/layout";
import { Callout, PageHeader, Skeleton } from "./components/ui";
import { useT, type Key } from "./i18n";
import LandingPage from "./pages/LandingPage";
import { LoginPage, RegisterPage } from "./pages/AuthPages";
import { PrivacyPage, TermsPage } from "./pages/LegalPages";

const MethodologyPage = lazy(() => import("./pages/MethodologyPage"));
const OverviewPage = lazy(() => import("./pages/OverviewPage"));
const ExplorerPage = lazy(() => import("./pages/ExplorerPage"));
const ForecastPage = lazy(() => import("./pages/ForecastPage"));
const ScenariosPage = lazy(() => import("./pages/ScenariosPage"));
const InsightsPage = lazy(() => import("./pages/InsightsPage"));
const ReportsPage = lazy(() => import("./pages/ReportsPage"));
const AccountPage = lazy(() => import("./pages/AccountPage"));
const HealthPage = lazy(() => import("./pages/ops/HealthPage"));
const DataOpsPage = lazy(() => import("./pages/ops/DataOpsPage"));
const ModelsOpsPage = lazy(() => import("./pages/ops/ModelsOpsPage"));
const JobsPage = lazy(() => import("./pages/ops/JobsPage"));
const AuditPage = lazy(() => import("./pages/ops/AuditPage"));
const TenantsPage = lazy(() => import("./pages/ops/TenantsPage"));

function Fallback() {
  const { t } = useT();
  return <div className="page no-anim boot" role="status" aria-label={t("common.loading")}><Skeleton height={120} /><Skeleton height={320} /></div>;
}

/** Workspace routes: signed-in principals only. Guests go to sign-in with the deep link preserved. */
function RequireUser() {
  const { principal, ready, isUser } = useAuth();
  const location = useLocation();
  if (!ready) return <Fallback />;
  if (principal.kind === "public") {
    const next = encodeURIComponent(location.pathname + location.search + location.hash);
    const operator = location.pathname.startsWith("/operasi") ? "&operator=1" : "";
    return <Navigate to={`/masuk?next=${next}${operator}`} replace />;
  }
  if (isUser && principal.mustChangePassword && location.pathname !== "/akun") return <Navigate to="/akun#keamanan" replace />;
  return <AppShell><Suspense fallback={<Fallback />}><Outlet /></Suspense></AppShell>;
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

function NotFound() {
  const { t } = useT();
  return (
    <div className="page"><div className="panel"><div className="panel-body">
      <h1 className="page-title">{t("shell.notFound")}</h1><p className="muted mt-2">{t("shell.notFoundHint")}</p>
    </div></div></div>
  );
}

/** Unknown URL: inside the shell for signed-in principals, on the public site for guests. */
function NotFoundAnywhere() {
  const { principal, ready } = useAuth();
  if (!ready) return <Fallback />;
  if (principal.kind === "public") return <PublicLayout><NotFound /></PublicLayout>;
  return <AppShell><NotFound /></AppShell>;
}

function Home() {
  const { principal, ready } = useAuth();
  const location = useLocation();
  if (!ready) return <Fallback />;
  // Arriving from sign-out: show the public home even while the session state is being cleared.
  if ((location.state as { signedOut?: boolean } | null)?.signedOut) return <LandingPage />;
  if (principal.kind === "operator") return <Navigate to="/operasi" replace />;
  return principal.kind === "public" ? <LandingPage /> : <Navigate to="/ringkasan" replace />;
}

/** Public pages fade in on navigation; signed-in pages animate inside the shell. */
function PublicRoutes() {
  const location = useLocation();
  return <div key={location.pathname} className="route-fade"><Suspense fallback={<Fallback />}><Outlet /></Suspense></div>;
}

const TITLES: Record<string, Key> = {
  "/metodologi": "nav.methodology", "/masuk": "shell.signin", "/daftar": "shell.register", "/syarat": "legal.termsTitle", "/privasi": "legal.privacyTitle",
  "/ringkasan": "nav.overview", "/harga": "nav.explorer", "/proyeksi": "nav.forecast", "/skenario": "nav.scenarios", "/insight": "nav.insights",
  "/laporan": "nav.reports", "/akun": "nav.account", "/operasi": "nav.ops.health", "/operasi/data": "nav.ops.data", "/operasi/model": "nav.ops.models",
  "/operasi/pekerjaan": "nav.ops.jobs", "/operasi/audit": "nav.ops.audit", "/operasi/organisasi": "nav.ops.tenants",
};

/** Every route gets its own document title ("<Halaman> · ICP Intelligence"), in the active language. */
function DocumentTitle() {
  const { t } = useT();
  const { pathname, search } = useLocation();
  useEffect(() => {
    const brand = t("brand.name");
    const key: Key | undefined = pathname === "/laporan" && new URLSearchParams(search).get("tab") === "pemberitahuan" ? "nav.alerts" : TITLES[pathname];
    document.title = pathname === "/" ? `${brand} · ${t("brand.tagline")}` : key ? `${t(key)} · ${brand}` : `${t("shell.notFound")} · ${brand}`;
  }, [pathname, search, t]);
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
            <Route path="/laporan" element={<ReportsPage />} />
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
        <Route path="/pemberitahuan" element={<Navigate to="/laporan?tab=pemberitahuan" replace />} />
        <Route path="/forecast" element={<Navigate to="/proyeksi" replace />} />
        <Route path="/data" element={<Navigate to="/operasi/data" replace />} />
        <Route path="/models" element={<Navigate to="/operasi/model" replace />} />
        <Route path="/operations" element={<Navigate to="/operasi" replace />} />
        <Route path="*" element={<NotFoundAnywhere />} />
      </Routes>
    </>
  );
}
