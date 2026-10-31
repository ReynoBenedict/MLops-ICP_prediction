/**
 * Contract C3: the boot check of a stored credential (/auth/whoami) can never hold the app longer
 * than the whoami timeout. A non-401 failure ends the boot state as a guest, keeps the stored
 * credential, says why (`sessionCheckError`) and re-checks it in the background; only a 401 forgets
 * it. Workspace routes explain an unconfirmed session instead of sending the member to sign-in.
 * The re-check after a 401 on another request (AUTH_EXPIRED_EVENT) ends the session only on a
 * definite answer.
 */
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { AUTH_EXPIRED_EVENT, SESSION_KEY, TIMEOUTS, clearSharedGets } from "./api/client";
import { AuthProvider, SESSION_RECHECK_DELAYS_MS, useAuth } from "./auth";
import { ToastProvider } from "./components/toast";
import { I18nProvider } from "./i18n";

// The route tests only need the shell; a stub page keeps the lazy import (charts) out of these tests.
vi.mock("./pages/OverviewPage", () => ({ default: () => <h1>Ringkasan</h1> }));

function Probe() {
  const { ready, principal, sessionCheckError } = useAuth();
  return <div data-testid="probe" data-unconfirmed={String(!!sessionCheckError)}>{ready ? `ready:${principal.kind}` : "booting"}</div>;
}
const probe = () => screen.getByTestId("probe");

const renderAuth = () => render(<I18nProvider initial="id"><ToastProvider><AuthProvider><Probe /></AuthProvider></ToastProvider></I18nProvider>);

/** Advance the fake clock and let the resulting promises and renders settle. */
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

/** fetch that never answers on its own; it only rejects when its request is aborted (timeout). */
const hangingFetch = vi.fn((_url: string, init: RequestInit) => new Promise<Response>((_res, rej) => {
  init.signal?.addEventListener("abort", () => rej(new DOMException("aborted", "AbortError")));
}));

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const unauthorized = () => json({ error: { code: "AUTH_INVALID", message: "Sesi berakhir.", request_id: null } }, 401);
const CUSTOMER = { authenticated: true, name: "Penguji UX", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 };

/**
 * Fake API whose /auth/whoami behaviour can be switched during a test ("hang", "user" or "401");
 * every other path answers 404. Returns the list of whoami calls.
 */
function whoamiServer(initial: "hang" | "user" | "401") {
  const state = { mode: initial, calls: 0 };
  vi.stubGlobal("fetch", (url: string, init: RequestInit) => {
    if (!String(url).startsWith("/api/v1/auth/whoami")) return Promise.resolve(json({ error: { code: "NOT_FOUND", message: "x", request_id: null } }, 404));
    state.calls += 1;
    if (state.mode === "hang") return hangingFetch(url, init);
    return Promise.resolve(state.mode === "user" ? json(CUSTOMER) : unauthorized());
  });
  return state;
}

beforeEach(() => {
  vi.useFakeTimers();
  localStorage.setItem(SESSION_KEY, "icp_sess_stored");
  hangingFetch.mockClear();
  clearSharedGets();
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  localStorage.clear();
  sessionStorage.clear();
});

describe("auth boot with an unresponsive backend (contract C3)", () => {
  it("ends the boot state at the whoami timeout as a guest and keeps the stored session token", async () => {
    vi.stubGlobal("fetch", hangingFetch);
    renderAuth();
    expect(probe()).toHaveTextContent("booting");
    await advance(TIMEOUTS.whoami - 1);
    expect(probe()).toHaveTextContent("booting");
    await advance(1);
    expect(probe()).toHaveTextContent("ready:public");
    expect(probe()).toHaveAttribute("data-unconfirmed", "true");
    expect(localStorage.getItem(SESSION_KEY)).toBe("icp_sess_stored");
    expect(String(hangingFetch.mock.calls[0]?.[0])).toBe("/api/v1/auth/whoami");
  });

  it("a 5xx answer followed by a hanging retry still ends within the whoami timeout", async () => {
    let call = 0;
    vi.stubGlobal("fetch", (url: string, init: RequestInit) => {
      call += 1;
      if (call === 1) return Promise.resolve(json({ error: { code: "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", message: "down", request_id: null } }, 502));
      return hangingFetch(url, init);
    });
    renderAuth();
    await advance(TIMEOUTS.whoami);
    expect(probe()).toHaveTextContent("ready:public");
    expect(call).toBe(2);
    expect(localStorage.getItem(SESSION_KEY)).toBe("icp_sess_stored");
  });

  it("forgets the stored token only when the server answers 401", async () => {
    vi.stubGlobal("fetch", async () => unauthorized());
    renderAuth();
    await advance(10);
    expect(probe()).toHaveTextContent("ready:public");
    expect(probe()).toHaveAttribute("data-unconfirmed", "false");
    expect(localStorage.getItem(SESSION_KEY)).toBeNull();
  });
});

describe("background re-check of an unconfirmed credential", () => {
  it("signs the member in by itself once the server answers again", async () => {
    const api = whoamiServer("hang");
    renderAuth();
    await advance(TIMEOUTS.whoami);
    expect(probe()).toHaveTextContent("ready:public");
    api.mode = "user";
    await advance(SESSION_RECHECK_DELAYS_MS[0] - 1);
    expect(api.calls).toBe(1);
    await advance(1);
    expect(api.calls).toBe(2);
    expect(probe()).toHaveTextContent("ready:user");
    expect(probe()).toHaveAttribute("data-unconfirmed", "false");
  });

  it("backs off between attempts and stops for good after a 401", async () => {
    const api = whoamiServer("hang");
    renderAuth();
    await advance(TIMEOUTS.whoami); // boot check failed
    await advance(SESSION_RECHECK_DELAYS_MS[0] + TIMEOUTS.whoami); // first re-check failed as well
    expect(api.calls).toBe(2);
    expect(probe()).toHaveAttribute("data-unconfirmed", "true");
    api.mode = "401";
    await advance(SESSION_RECHECK_DELAYS_MS[1] - 1);
    expect(api.calls).toBe(2); // the second delay is longer than the first
    await advance(1);
    expect(probe()).toHaveTextContent("ready:public");
    expect(probe()).toHaveAttribute("data-unconfirmed", "false");
    expect(localStorage.getItem(SESSION_KEY)).toBeNull();
    const after401 = api.calls;
    await advance(5 * 60_000);
    expect(api.calls).toBe(after401);
  });

  it("waits while the tab is hidden and re-checks as soon as it is visible again", async () => {
    let visibility: DocumentVisibilityState = "visible";
    Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility });
    try {
      const api = whoamiServer("hang");
      renderAuth();
      await advance(TIMEOUTS.whoami);
      visibility = "hidden";
      api.mode = "user";
      await advance(2 * 60_000);
      expect(api.calls).toBe(1);
      expect(probe()).toHaveTextContent("ready:public");
      visibility = "visible";
      await act(async () => { document.dispatchEvent(new Event("visibilitychange")); await vi.advanceTimersByTimeAsync(0); });
      expect(api.calls).toBe(2);
      expect(probe()).toHaveTextContent("ready:user");
    } finally {
      delete (document as { visibilityState?: unknown }).visibilityState;
    }
  });
});

describe("workspace routes with an unconfirmed session", () => {
  let lastLocation = "";
  function Where() {
    const loc = useLocation();
    lastLocation = loc.pathname + loc.search;
    return null;
  }
  const mountApp = (path: string) => render(
    <MemoryRouter initialEntries={[path]}>
      <I18nProvider initial="id"><ToastProvider><AuthProvider><App /><Where /></AuthProvider></ToastProvider></I18nProvider>
    </MemoryRouter>,
  );

  it("explain the situation instead of redirecting to sign-in, and open the workspace after a successful retry", async () => {
    const api = whoamiServer("hang");
    mountApp("/ringkasan");
    await advance(TIMEOUTS.whoami);
    expect(screen.getByRole("heading", { level: 1, name: "Sesi Anda belum dapat diperiksa" })).toBeInTheDocument();
    expect(screen.getByText(/tidak menjawab dalam 8 detik/)).toBeInTheDocument();
    expect(lastLocation).toBe("/ringkasan");
    expect(localStorage.getItem(SESSION_KEY)).toBe("icp_sess_stored");
    expect(screen.queryByRole("navigation", { name: "Navigasi ruang kerja" })).toBeNull();

    api.mode = "user";
    fireEvent.click(screen.getByRole("button", { name: "Coba lagi" }));
    await advance(0);
    expect(screen.getByRole("navigation", { name: "Navigasi ruang kerja" })).toBeInTheDocument();
    expect(lastLocation).toBe("/ringkasan");
  });

  it("still send a guest without a stored credential to sign-in", async () => {
    localStorage.clear();
    whoamiServer("hang");
    mountApp("/ringkasan");
    await advance(0);
    expect(lastLocation).toBe("/masuk?next=%2Fringkasan");
  });
});

describe("re-check after a 401 on another request (AUTH_EXPIRED_EVENT)", () => {
  it("keeps the session when the re-check times out", async () => {
    const api = whoamiServer("user");
    renderAuth();
    await advance(0);
    expect(probe()).toHaveTextContent("ready:user");
    api.mode = "hang";
    await act(async () => { window.dispatchEvent(new Event(AUTH_EXPIRED_EVENT)); await vi.advanceTimersByTimeAsync(TIMEOUTS.whoami + 1); });
    expect(api.calls).toBe(2);
    expect(probe()).toHaveTextContent("ready:user");
    expect(localStorage.getItem(SESSION_KEY)).toBe("icp_sess_stored");
    expect(screen.queryByText("Sesi Anda telah berakhir. Silakan masuk kembali.")).toBeNull();
  });

  it("ends the session with a notice when the server answers 401", async () => {
    const api = whoamiServer("user");
    renderAuth();
    await advance(0);
    api.mode = "401";
    await act(async () => { window.dispatchEvent(new Event(AUTH_EXPIRED_EVENT)); await vi.advanceTimersByTimeAsync(0); });
    expect(probe()).toHaveTextContent("ready:public");
    expect(probe()).toHaveAttribute("data-unconfirmed", "false");
    expect(localStorage.getItem(SESSION_KEY)).toBeNull();
    expect(screen.getByText("Sesi Anda telah berakhir. Silakan masuk kembali.")).toBeInTheDocument();
  });
});
