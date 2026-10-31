/**
 * The page placeholder (App.tsx Fallback) says "checking your session" only while the stored
 * credential is being checked; once the session is confirmed, a slowly loading page chunk shows a
 * neutral "loading the page" note inside the signed-in shell (review D, RD-01).
 */
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { AuthProvider } from "./auth";
import { ToastProvider } from "./components/toast";
import { I18nProvider } from "./i18n";

// The overview page is not ready until the test releases it: the stub suspends (React `use`) inside
// the same Suspense boundary that waits for a slow page chunk in the app.
const gate = vi.hoisted(() => {
  let open: () => void = () => undefined;
  const opened = new Promise<void>((resolve) => { open = resolve; });
  return { opened, open: () => open() };
});
vi.mock("./pages/OverviewPage", async () => {
  const { use } = await vi.importActual<typeof import("react")>("react");
  return { default: function SlowOverview() { use(gate.opened); return <h1>Ringkasan pasar</h1>; } };
});

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const who = { authenticated: true, name: "Penguji UX", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 };

function server(whoami: () => Promise<Response>) {
  localStorage.setItem("icp.sessionToken", "icp_sess_test");
  vi.stubGlobal("fetch", (url: string) => {
    const path = String(url).split("?")[0] ?? "";
    if (path === "/api/v1/auth/whoami") return whoami();
    return Promise.resolve(json({ error: { code: "NOT_FOUND", message: "nope", request_id: null, details: {} } }, 404));
  });
}
const mount = (path: string) => render(
  <MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><ToastProvider><AuthProvider><App /></AuthProvider></ToastProvider></I18nProvider></MemoryRouter>,
);
afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); });

describe("page placeholder", () => {
  it("says the session is being checked while the stored credential's check is pending", async () => {
    server(() => new Promise<Response>(() => undefined)); // the check never answers within the test
    mount("/ringkasan");
    expect(await screen.findByText("Memeriksa sesi Anda di server…", {}, { timeout: 3000 })).toBeInTheDocument();
    expect(screen.queryByText("Memuat halaman…")).toBeNull();
  });

  it("does not say the session is being checked once it is confirmed and only the page is loading", async () => {
    server(() => Promise.resolve(json(who)));
    mount("/ringkasan");
    // The signed-in shell is on screen (the session is confirmed) while the page chunk is still on its way.
    expect(await screen.findByText("Memuat halaman…", {}, { timeout: 3000 })).toBeInTheDocument();
    expect(screen.getByText("Penguji UX")).toBeInTheDocument();
    expect(screen.queryByText("Memeriksa sesi Anda di server…")).toBeNull();
    gate.open();
    expect(await screen.findByRole("heading", { name: "Ringkasan pasar" })).toBeInTheDocument();
    expect(screen.queryByText("Memuat halaman…")).toBeNull();
  });
});
