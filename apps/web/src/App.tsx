import { lazy, Suspense } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import { useAuth } from "./auth";
import { AppShell } from "./components/layout";
import { GateNotice, Skeleton } from "./components/ui";
import { useT } from "./i18n";
import LandingPage from "./pages/LandingPage";
import { LoginPage, RegisterPage } from "./pages/AuthPages";
import { PrivacyPage, TermsPage } from "./pages/LegalPages";

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
  return <div className="page no-anim" role="status"><Skeleton height={120} /><Skeleton height={320} /></div>;
}

function OperatorOnly({ children }: { children: React.ReactNode }) {
  const { isOperator, ready } = useAuth();
  const { t } = useT();
  if (!ready) return <Fallback />;
  if (!isOperator) return <div className="page"><GateNotice title={t("ops.locked")}>{t("ops.lockedBody")}</GateNotice></div>;
  return <>{children}</>;
}

function Home() {
  const { principal, ready } = useAuth();
  if (!ready) return <Fallback />;
  return principal.kind === "public" ? <LandingPage /> : <Navigate to="/ringkasan" replace />;
}

function NotFound() {
  const { t } = useT();
  return (
    <div className="page"><div className="panel"><div className="panel-body">
      <h1 className="page-title">{t("shell.notFound")}</h1><p className="muted mt-2">{t("shell.notFoundHint")}</p>
    </div></div></div>
  );
}

export default function App() {
  const location = useLocation();
  const bare = ["/", "/masuk", "/daftar", "/syarat", "/privasi"].includes(location.pathname);
  if (bare) {
    return (
      <div key={location.pathname} className="route-fade">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/masuk" element={<LoginPage />} />
          <Route path="/daftar" element={<RegisterPage />} />
          <Route path="/syarat" element={<TermsPage />} />
          <Route path="/privasi" element={<PrivacyPage />} />
        </Routes>
      </div>
    );
  }
  return (
    <AppShell>
      <Suspense fallback={<Fallback />}>
        <Routes>
          <Route path="/ringkasan" element={<OverviewPage />} />
          <Route path="/harga" element={<ExplorerPage />} />
          <Route path="/proyeksi" element={<ForecastPage />} />
          <Route path="/skenario" element={<ScenariosPage />} />
          <Route path="/insight" element={<InsightsPage />} />
          <Route path="/laporan" element={<ReportsPage />} />
          <Route path="/akun" element={<AccountPage />} />
          <Route path="/operasi" element={<OperatorOnly><HealthPage /></OperatorOnly>} />
          <Route path="/operasi/data" element={<OperatorOnly><DataOpsPage /></OperatorOnly>} />
          <Route path="/operasi/model" element={<OperatorOnly><ModelsOpsPage /></OperatorOnly>} />
          <Route path="/operasi/pekerjaan" element={<OperatorOnly><JobsPage /></OperatorOnly>} />
          <Route path="/operasi/audit" element={<OperatorOnly><AuditPage /></OperatorOnly>} />
          <Route path="/operasi/organisasi" element={<OperatorOnly><TenantsPage /></OperatorOnly>} />
          {/* legacy routes from the previous app */}
          <Route path="/forecast" element={<Navigate to="/proyeksi" replace />} />
          <Route path="/data" element={<Navigate to="/operasi/data" replace />} />
          <Route path="/models" element={<Navigate to="/operasi/model" replace />} />
          <Route path="/operations" element={<Navigate to="/operasi" replace />} />
          <Route path="*" element={<NotFound />} />
        </Routes>
      </Suspense>
    </AppShell>
  );
}
