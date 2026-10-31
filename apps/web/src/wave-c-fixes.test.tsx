/**
 * Wave C review fixes (FIX-WEB): sign-in help without e-mail recovery and "required" before "invalid"
 * (UX-05, UX-12), a forecast legend that names only the kinds in the result (UX-14), ranges that wrap
 * only between their two values (UX-13), a page-level error that names what is missing (UX-17), and
 * the root's scroll-padding fed by the measured header height (UX-01).
 */
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { ApiError } from "./api/client";
import type { ForecastRun } from "./api/types";
import { AuthProvider } from "./auth";
import { ForecastToolbar, historyOnlyRun } from "./components/forecast";
import { PublicLayout } from "./components/public-layout";
import { ToastProvider } from "./components/toast";
import { ErrorState } from "./components/ui";
import { fmtUsdRange } from "./format";
import { I18nProvider } from "./i18n";

const notFound = () => new Response(JSON.stringify({ error: { code: "NOT_FOUND", message: "x", request_id: null, details: {} } }), { status: 404, headers: { "content-type": "application/json" } });
const wrap = (ui: React.ReactNode, path = "/") => render(
  <MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><ToastProvider><AuthProvider>{ui}</AuthProvider></ToastProvider></I18nProvider></MemoryRouter>,
);

afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); });

describe("sign-in form (UX-05, UX-12)", () => {
  it("says an empty e-mail is required, not invalid, and names the only recovery path", async () => {
    vi.stubGlobal("fetch", async () => notFound());
    wrap(<App />, "/masuk");
    fireEvent.click(await screen.findByRole("button", { name: "Masuk" }));
    const email = screen.getByLabelText("Email");
    await waitFor(() => expect(email).toHaveAttribute("aria-invalid", "true"));
    expect(email).toHaveAccessibleDescription("Kolom ini wajib diisi.");
    expect(screen.getByLabelText("Kata sandi")).toHaveAccessibleDescription(/Kolom ini wajib diisi\. Lupa kata sandi\? Minta pemilik atau admin organisasi Anda/);
    fireEvent.change(email, { target: { value: "bukan-email" } });
    fireEvent.click(screen.getByRole("button", { name: "Masuk" }));
    await waitFor(() => expect(email).toHaveAccessibleDescription("Alamat email tidak valid."));
  });
});

describe("forecast legend (UX-14)", () => {
  const withKinds = (...kinds: string[]): ForecastRun => ({
    ...historyOnlyRun("2026-09"), id: "fc_1", created_at: "2026-10-17T00:00:00Z",
    result: { points: kinds.map((kind, i) => ({ period: `2026-1${i}`, prediction: 100, kind })) } as unknown as ForecastRun["result"],
  });
  it("names only the value kinds present in the result, with their meaning", () => {
    wrap(<ForecastToolbar run={withKinds("future_forecast", "future_forecast")} level="0.8" onLevel={() => undefined} />);
    expect(screen.getByText("bulan belum berakhir, harga acuan diasumsikan")).toBeInTheDocument();
    expect(screen.queryByText("Estimasi (menunggu publikasi)")).toBeNull();
  });
  it("names both kinds when a stored run does not say", () => {
    wrap(<ForecastToolbar run={withKinds()} level="0.8" onLevel={() => undefined} />);
    expect(screen.getByText("Estimasi (menunggu publikasi)")).toBeInTheDocument();
    expect(screen.getByText("Proyeksi")).toBeInTheDocument();
  });
});

describe("number ranges (UX-13)", () => {
  it("wrap only between the two values, never inside one", () => {
    expect(fmtUsdRange(103.32, 121.85, "id-ID")).toBe("USD 103,32 s.d. 121,85");
    expect(fmtUsdRange(103.32, 121.85, "en-US", "en")).toBe("USD 103.32 to 121.85");
  });
});

describe("page-level error (UX-17)", () => {
  it("names the parts of the page that are missing", () => {
    wrap(<ErrorState error={new ApiError(503, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "x", null)} missing={["insight", "bukti per bulan"]} onRetry={() => undefined} />);
    expect(screen.getByText("Tidak tersedia saat ini: insight dan bukti per bulan.").closest(".callout")).toHaveAttribute("role", "alert");
  });
});

describe("sticky header offset (UX-01)", () => {
  it("keeps --sticky-top on the root while a layout with a sticky header is mounted", () => {
    vi.stubGlobal("fetch", async () => notFound());
    const { unmount } = wrap(<PublicLayout><p>isi</p></PublicLayout>);
    // jsdom has no layout (height 0), but the property is set and removed with the layout.
    expect(document.documentElement.style.getPropertyValue("--sticky-top")).toBe("0px");
    unmount();
    expect(document.documentElement.style.getPropertyValue("--sticky-top")).toBe("");
  });
});
