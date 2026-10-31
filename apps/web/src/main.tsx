import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import App from "./App";
import { AuthProvider } from "./auth";
import { ToastProvider } from "./components/toast";
import { I18nProvider, readLocale } from "./i18n";
// Design system (docs/DESIGN_SYSTEM.md): tokens, base, shared primitives and the public site load
// with the entry. The other area stylesheets load with the lazy modules that use them, after these:
// shell.css with components/layout.tsx, market.css with the market and forecast pages, flows.css with
// the scenario, insight, report, notification and account pages, ops.css with the /operasi pages.
import "./styles/tokens.css";
import "./styles/base.css";
import "./styles/components.css";
import "./styles/public.css";

document.documentElement.lang = readLocale();

/**
 * A data router (instead of <BrowserRouter>) so forms can block navigation with unsaved changes,
 * including the browser Back button. The app keeps its own <Routes> under one splat route.
 */
const router = createBrowserRouter([{
  path: "*",
  element: (
    <I18nProvider>
      <ToastProvider>
        <AuthProvider>
          <App />
        </AuthProvider>
      </ToastProvider>
    </I18nProvider>
  ),
}]);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
);
