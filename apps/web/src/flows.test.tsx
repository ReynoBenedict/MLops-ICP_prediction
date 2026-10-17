/**
 * Regression tests for the UX v3 session flows: sign-in with a deep link, sign-out (server first,
 * then client, then the public home page with feedback), failed sign-out keeping the session,
 * Back navigation after sign-out, the auth-readiness gate, session expiry, offline errors and the
 * confirmation dialog.
 */
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useState } from "react";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { ApiError, api, nullOn404 } from "./api/client";
import { AuthProvider } from "./auth";
import { ToastProvider } from "./components/toast";
import { ConfirmDialog, ErrorState } from "./components/ui";
import { I18nProvider } from "./i18n";
import { safeNext } from "./pages/AuthPages";

const TOKEN = "icp_sess_test_token";
function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const err = (status: number, code: string) => json({ error: { code, message: code, request_id: "req_1", details: {} } }, status);
const user = { authenticated: true, name: "Penguji UX", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 };
const anon = { authenticated: false, name: null, role: "public", kind: "public", org_id: null, org_name: null, plan: null, must_change_password: false, horizon_max: 3 };

let lastLocation = "";
function Probe() {
  const loc = useLocation();
  lastLocation = loc.pathname + loc.search;
  return null;
}
let goBack: (() => void) | null = null;
function BackButton() {
  const navigate = useNavigate();
  goBack = () => navigate(-1);
  return null;
}

const mount = (path: string | string[]) => render(
  <MemoryRouter initialEntries={Array.isArray(path) ? path : [path]} initialIndex={Array.isArray(path) ? path.length - 1 : 0}>
    <I18nProvider initial="id"><ToastProvider><AuthProvider><App /><Probe /><BackButton /></AuthProvider></ToastProvider></I18nProvider>
  </MemoryRouter>,
);

/** A small stateful fake of the gateway: one session token, logout invalidates it. */
function fakeServer(opts: { logout?: "ok" | "network" } = {}) {
  const state = { valid: new Set<string>(), logoutCalls: 0 };
  vi.stubGlobal("fetch", async (url: string, init: RequestInit = {}) => {
    const auth = (init.headers as Record<string, string> | undefined)?.Authorization ?? "";
    const token = auth.replace("Bearer ", "");
    const path = url.split("?")[0];
    if (path === "/api/v1/auth/login") {
      state.valid.add(TOKEN);
      return json({ token: TOKEN, expires_at: "2026-11-16T00:00:00Z", user: { id: "u1", name: "Penguji UX", email: "ux@test" }, organization: { id: "org_1", name: "PT Uji" } });
    }
    if (path === "/api/v1/auth/whoami") return json(token && state.valid.has(token) ? user : anon);
    if (path === "/api/v1/auth/logout") {
      state.logoutCalls += 1;
      if (opts.logout === "network") throw new TypeError("Failed to fetch");
      state.valid.delete(token);
      return new Response(null, { status: 204 });
    }
    if (token && !state.valid.has(token)) return err(401, "AUTH_INVALID");
    if (path === "/api/v1/scenarios") return json({ items: [], next_cursor: null });
    if (path === "/api/v1/public/market-snapshot") return err(404, "NOT_FOUND");
    return err(404, "NOT_FOUND");
  });
  return state;
}

afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); lastLocation = ""; goBack = null; });

describe("sign-in and sign-out flow", () => {
  it("login -> deep link -> logout -> public home with feedback -> Back re-checks the session -> login again", async () => {
    const server = fakeServer();
    // History: the member was on /akun earlier, then followed a sign-in link with a deep link.
    mount(["/akun", "/masuk?next=%2Fskenario"]);
    fireEvent.change(await screen.findByLabelText("Email"), { target: { value: "ux@test.id" } });
    fireEvent.change(screen.getByLabelText("Kata sandi"), { target: { value: "rahasia-panjang" } });
    fireEvent.click(screen.getByRole("button", { name: "Masuk" }));

    // Deep link honoured; the sign-in page is replaced, not stacked in history.
    await waitFor(() => expect(lastLocation).toBe("/skenario"));
    expect(await screen.findByText(/Selamat datang, Penguji UX/)).toBeInTheDocument();
    expect(localStorage.getItem("icp.sessionToken")).toBe(TOKEN);

    fireEvent.click(await screen.findByRole("button", { name: "Keluar" }));
    await waitFor(() => expect(lastLocation).toBe("/"));
    expect(server.logoutCalls).toBe(1);
    expect(localStorage.getItem("icp.sessionToken")).toBeNull();
    expect(await screen.findByText(/Anda telah keluar/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Keluar" })).toBeNull();

    // Browser Back lands on the earlier private page, which must re-check the session.
    act(() => goBack?.());
    await waitFor(() => expect(lastLocation).toBe("/akun"));
    expect(await screen.findByText("Masuk diperlukan")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Keluar" })).toBeNull();

    // Sign in again from the gate: it returns to the page that asked for it.
    fireEvent.click(screen.getAllByRole("link", { name: "Masuk" })[0]!);
    await waitFor(() => expect(lastLocation).toMatch(/^\/masuk\?next=%2Fakun/));
    fireEvent.change(await screen.findByLabelText("Email"), { target: { value: "ux@test.id" } });
    fireEvent.change(screen.getByLabelText("Kata sandi"), { target: { value: "rahasia-panjang" } });
    fireEvent.click(screen.getByRole("button", { name: "Masuk" }));
    await waitFor(() => expect(lastLocation).toBe("/akun"));
    expect(await screen.findByRole("button", { name: "Keluar" })).toBeInTheDocument();
  });

  it("keeps the session and says so when the server cannot be reached during sign-out", async () => {
    const server = fakeServer({ logout: "network" });
    localStorage.setItem("icp.sessionToken", TOKEN);
    server.valid.add(TOKEN);
    mount("/skenario");
    fireEvent.click(await screen.findByRole("button", { name: "Keluar" }));
    expect(await screen.findByText(/Gagal keluar/)).toBeInTheDocument();
    expect(localStorage.getItem("icp.sessionToken")).toBe(TOKEN);
    expect(lastLocation).toBe("/skenario");
    expect(screen.getByRole("button", { name: "Keluar" })).toBeEnabled();
  });

  it("does not flash 'sign in required' on a private deep link while the session is being verified", async () => {
    let release: (() => void) | null = null;
    vi.stubGlobal("fetch", async (url: string) => {
      if (url.startsWith("/api/v1/auth/whoami")) {
        await new Promise<void>((r) => { release = r; });
        return json(user);
      }
      if (url.startsWith("/api/v1/scenarios")) return json({ items: [], next_cursor: null });
      return err(404, "NOT_FOUND");
    });
    localStorage.setItem("icp.sessionToken", TOKEN);
    mount("/skenario");
    await waitFor(() => expect(release).not.toBeNull());
    expect(screen.queryByText("Masuk untuk membuat dan membandingkan skenario.")).toBeNull();
    act(() => release?.());
    expect(await screen.findByRole("button", { name: "Keluar" })).toBeInTheDocument();
    expect(screen.queryByText("Masuk untuk membuat dan membandingkan skenario.")).toBeNull();
  });

  it("signs out locally and tells the user when the server rejects the stored session", async () => {
    const server = fakeServer();
    localStorage.setItem("icp.sessionToken", TOKEN);
    server.valid.add(TOKEN);
    mount("/skenario");
    await screen.findByRole("button", { name: "Keluar" });
    server.valid.delete(TOKEN); // revoked elsewhere
    await act(async () => { await api("/scenarios").catch(() => undefined); });
    expect(await screen.findByText(/Sesi Anda telah berakhir/)).toBeInTheDocument();
    await waitFor(() => expect(localStorage.getItem("icp.sessionToken")).toBeNull());
    expect(await screen.findByText("Masuk untuk membuat dan membandingkan skenario.")).toBeInTheDocument();
  });
});

describe("redirect safety", () => {
  it("accepts only same-origin app paths as the post-login destination", () => {
    expect(safeNext("/skenario?x=1")).toBe("/skenario?x=1");
    for (const bad of ["//evil.example", "https://evil.example", "/\\evil", "/masuk", "/", null, "skenario"]) expect(safeNext(bad)).toBeNull();
  });
});

describe("loading and error honesty", () => {
  it("treats only 404 as 'nothing yet'; an unreachable backend stays an error", async () => {
    await expect(nullOn404(Promise.reject(new ApiError(404, "NOT_FOUND", "x", null)))).resolves.toBeNull();
    await expect(nullOn404(Promise.reject(new ApiError(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "x", null)))).rejects.toBeInstanceOf(ApiError);
  });

  it("explains an offline backend instead of a generic failure, with retry", () => {
    const retry = vi.fn();
    render(<MemoryRouter><I18nProvider initial="id"><ErrorState error={new ApiError(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "upstream", "gw_1")} onRetry={retry} /></I18nProvider></MemoryRouter>);
    expect(screen.getByText("Layanan data sedang tidak tersedia")).toBeInTheDocument();
    expect(screen.queryByText("upstream")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Coba lagi" }));
    expect(retry).toHaveBeenCalledOnce();
  });
});

describe("confirmation dialog", () => {
  function Harness({ onConfirm }: { onConfirm: () => void }) {
    const [open, setOpen] = useState(false);
    return (
      <I18nProvider initial="id">
        <button onClick={() => setOpen(true)}>Arsipkan</button>
        <ConfirmDialog open={open} title="Arsipkan skenario?" confirmLabel="Ya, arsipkan" onConfirm={onConfirm} onCancel={() => setOpen(false)} />
      </I18nProvider>
    );
  }

  it("focuses the safe choice, closes on Escape and returns focus to the trigger", async () => {
    const confirm = vi.fn();
    render(<Harness onConfirm={confirm} />);
    const trigger = screen.getByRole("button", { name: "Arsipkan" });
    trigger.focus();
    fireEvent.click(trigger);
    const dialog = await screen.findByRole("alertdialog", { name: "Arsipkan skenario?" });
    expect(dialog).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Batal" })).toHaveFocus();
    fireEvent.keyDown(document, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull());
    expect(trigger).toHaveFocus();
    expect(confirm).not.toHaveBeenCalled();
  });
});
