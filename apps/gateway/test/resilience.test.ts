/**
 * Serverless backend support (docs/DEPLOYMENT.md, Vercel Hobby): warm-on-visit for page views, the site's
 * security headers on Worker-served documents, the daily scheduled refresh of the two public copies
 * (contract C2 path), and the wrangler.jsonc settings they depend on. fetch, KV, ASSETS and the clock are
 * fakes; nothing leaves the process.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import worker, {
  CACHE_STATE_HEADER,
  EDGE_CACHE_REFRESH_MS,
  SCHEDULED_REFRESH_AFTER_MS,
  SITE_SECURITY_HEADERS,
  WARM_PING_INTERVAL_MS,
  isDocumentNavigation,
  warmBackend,
  type Env,
} from "../src/index";
import siteHeadersText from "../../web/public/_headers?raw";
import wranglerText from "../wrangler.jsonc?raw";
import { parseJsonc } from "./jsonc";

const SECRET = "s".repeat(40);
const PAGE = '<!doctype html><html><head><title>ICP</title></head><body><div id="root"></div></body></html>';
const NAVIGATE = { "sec-fetch-mode": "navigate", "sec-fetch-dest": "document", accept: "text/html,application/xhtml+xml" };
const T0 = Date.parse("2026-10-28T00:00:00.000Z");
const HOUR = 60 * 60_000;
let backendSeq = 0;

/** A fresh backend origin per test: the ping throttle is kept per backend URL in the isolate. */
function backend(): string {
  backendSeq += 1;
  return `https://icp-api-staging-${backendSeq}.example`;
}

function context() {
  const pending: Promise<unknown>[] = [];
  const ctx = { waitUntil: (p: Promise<unknown>) => { pending.push(p); }, passThroughOnException: () => undefined } as unknown as ExecutionContext;
  return { ctx, pending, settle: async () => { await Promise.all(pending.splice(0)); } };
}

function assets(headers: Record<string, string> = { "content-type": "text/html; charset=utf-8" }) {
  const requests: Request[] = [];
  const fetcher = { fetch: async (input: Request) => { requests.push(input); return new Response(PAGE, { status: 200, headers }); } } as unknown as Fetcher;
  return { fetcher, requests };
}

function env(overrides: Partial<Env> = {}): Env {
  return { BACKEND_URL: backend(), GATEWAY_SHARED_SECRET: SECRET, ENVIRONMENT: "staging", UPSTREAM_TIMEOUT_MS: "10000", PUBLIC_UPSTREAM_TIMEOUT_MS: "50", ...overrides };
}

function stubFetch(handler: (req: Request, signal?: AbortSignal) => Promise<Response> | Response) {
  const calls: Request[] = [];
  vi.stubGlobal("fetch", async (input: Request | string, init?: RequestInit) => {
    const req = typeof input === "string" ? new Request(input, init) : input;
    calls.push(req);
    return handler(req, init?.signal ?? undefined);
  });
  return calls;
}

const json = (body: string) => new Response(body, { status: 200, headers: { "content-type": "application/json" } });

interface Stored { value: ArrayBuffer; metadata: unknown }
function fakeKV() {
  const store = new Map<string, Stored>();
  const writes: { key: string; metadata: unknown }[] = [];
  const kv = {
    getWithMetadata: async (key: string) => {
      const hit = store.get(key);
      return hit ? { value: hit.value.slice(0), metadata: hit.metadata } : { value: null, metadata: null };
    },
    put: async (key: string, value: ArrayBuffer, opts?: { metadata?: unknown }) => {
      writes.push({ key, metadata: opts?.metadata ?? null });
      store.set(key, { value: value.slice(0), metadata: opts?.metadata ?? null });
    },
  } as unknown as KVNamespace;
  return { kv, store, writes };
}

async function sha256(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function at(time: number) {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(time);
}

let logs: string[];
beforeEach(() => {
  logs = [];
  vi.spyOn(console, "log").mockImplementation((line: unknown) => { logs.push(String(line)); });
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("warm-on-visit", () => {
  it("serves the page at once and pings /health/live in the background", async () => {
    let release: (r: Response) => void = () => undefined;
    const calls = stubFetch(() => new Promise<Response>((resolve) => { release = resolve; }));
    const { fetcher, requests } = assets();
    const e = env({ ASSETS: fetcher });
    const { ctx, pending, settle } = context();
    const res = await worker.fetch(new Request("https://icp.example/", { headers: NAVIGATE }), e, ctx);
    // The page is complete while the ping is still waiting for the backend.
    expect(res.status).toBe(200);
    expect(await res.text()).toBe(PAGE);
    expect(requests).toHaveLength(1);
    expect(pending).toHaveLength(1);
    expect(calls.map((c) => [c.method, c.url])).toEqual([["GET", `${e.BACKEND_URL}/health/live`]]);
    expect(calls[0]!.headers.get("x-gateway-secret")).toBeNull(); // the live probe needs no secret
    release(json('{"status":"ok"}'));
    await settle();
    expect(logs.map((l) => JSON.parse(l) as { event: string; status: number })).toEqual([
      expect.objectContaining({ event: "gateway.backend_warm_ping", status: 200 }),
    ]);
  });

  it("pings each backend at most once a minute per isolate", async () => {
    const calls = stubFetch(() => json('{"status":"ok"}'));
    const e = env({ ASSETS: assets().fetcher });
    const { ctx, settle } = context();
    for (const time of [T0, T0 + 1_000, T0 + 30_000, T0 + WARM_PING_INTERVAL_MS - 1, T0 + WARM_PING_INTERVAL_MS, T0 + WARM_PING_INTERVAL_MS + 5_000]) {
      at(time);
      expect((await worker.fetch(new Request("https://icp.example/proyeksi", { headers: NAVIGATE }), e, ctx)).status).toBe(200);
    }
    await settle();
    expect(calls).toHaveLength(2);
    // warmBackend reports what it did
    const { ctx: ctx2 } = context();
    at(T0 + WARM_PING_INTERVAL_MS + 6_000);
    expect(warmBackend(e, ctx2, Date.now())).toBe("throttled");
  });

  it("never pings for files, API calls, other methods or requests that are not page navigations", async () => {
    const calls = stubFetch(() => json('{"status":"ok"}'));
    const e = env({ ASSETS: assets({ "content-type": "text/plain" }).fetcher });
    const { ctx, settle } = context();
    const requests = [
      new Request("https://icp.example/robots.txt", { headers: { "sec-fetch-mode": "no-cors", "sec-fetch-dest": "empty" } }),
      new Request("https://icp.example/manifest.webmanifest", { headers: { "sec-fetch-dest": "manifest" } }),
      new Request("https://icp.example/", { method: "HEAD", headers: NAVIGATE }),
      new Request("https://icp.example/", { method: "POST", headers: NAVIGATE, body: "x" }),
      new Request("https://icp.example/x.png", { headers: { accept: "image/avif,image/webp" } }),
      new Request("https://icp.example/api/gateway/health", { headers: NAVIGATE }),
      // An uptime monitor or script polling a page: HTML Accept, no Fetch Metadata.
      new Request("https://icp.example/", { headers: { accept: "text/html,application/xhtml+xml" } }),
    ];
    for (const r of requests) await worker.fetch(r, e, ctx);
    await settle();
    expect(calls).toHaveLength(0);
    // Changed 2026-10-28 (review B2-P2-02): an HTML Accept header without Fetch Metadata no longer counts as a
    // page view. Every current browser sends Sec-Fetch-Mode on navigations; clients without it are mostly
    // monitors, crawlers and scripts, and one polling a page would otherwise keep the backend warm.
    expect(isDocumentNavigation(new Request("https://icp.example/", { headers: { accept: "text/html" } }))).toBe(false);
    expect(isDocumentNavigation(new Request("https://icp.example/", { headers: { accept: "text/html", "sec-fetch-dest": "image" } }))).toBe(false);
    expect(isDocumentNavigation(new Request("https://icp.example/", { headers: { "sec-fetch-dest": "document" } }))).toBe(true);
  });

  it("never lets crawlers, link previews, uptime monitors or headless browsers start the backend", async () => {
    const calls = stubFetch(() => json('{"status":"ok"}'));
    const { ctx, settle } = context();
    const robots = [
      "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
      "Mozilla/5.0+(compatible; UptimeRobot/2.0; http://www.uptimerobot.com/)",
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/141.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36 Chrome-Lighthouse",
      "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
      "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)",
    ];
    for (const ua of robots) {
      const e = env({ ASSETS: assets().fetcher }); // a fresh backend each time, so the throttle cannot hide a ping
      const res = await worker.fetch(new Request("https://icp.example/", { headers: { ...NAVIGATE, "user-agent": ua } }), e, ctx);
      expect(res.status, ua).toBe(200);
    }
    await settle();
    expect(calls).toHaveLength(0);
    // A person's browser with the same navigation headers still starts it.
    const person = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36";
    await worker.fetch(new Request("https://icp.example/", { headers: { ...NAVIGATE, "user-agent": person } }), env({ ASSETS: assets().fetcher }), ctx);
    await settle();
    expect(calls).toHaveLength(1);
  });

  it("does not ping a placeholder or non-https backend in a deployed environment", async () => {
    const calls = stubFetch(() => json('{"status":"ok"}'));
    const { ctx, settle } = context();
    for (const BACKEND_URL of ["https://<vercel-production-domain>", "http://icp-api.example", undefined]) {
      const e = env({ BACKEND_URL, ASSETS: assets().fetcher });
      const res = await worker.fetch(new Request("https://icp.example/", { headers: NAVIGATE }), e, ctx);
      expect(res.status).toBe(200);
      expect(warmBackend(e, ctx, Date.now())).toBe("not_configured");
    }
    await settle();
    expect(calls).toHaveLength(0);
  });

  it("keeps the page when the ping fails", async () => {
    stubFetch(() => { throw new Error("connection refused"); });
    const e = env({ ASSETS: assets().fetcher });
    const { ctx, settle } = context();
    const res = await worker.fetch(new Request("https://icp.example/masuk", { headers: NAVIGATE }), e, ctx);
    expect(res.status).toBe(200);
    await expect(settle()).resolves.toBeUndefined();
    expect(JSON.parse(logs[0]!)).toMatchObject({ event: "gateway.backend_warm_ping", status: "no_response" });
  });
});

describe("site headers on documents served through the Worker", () => {
  it("adds the _headers security set when the asset answer lacks it", async () => {
    stubFetch(() => json('{"status":"ok"}'));
    const e = env({ ASSETS: assets().fetcher });
    const { ctx, settle } = context();
    const res = await worker.fetch(new Request("https://icp.example/", { headers: NAVIGATE }), e, ctx);
    await settle();
    for (const [name, value] of SITE_SECURITY_HEADERS) expect(res.headers.get(name), name).toBe(value);
    expect(res.headers.get("content-type")).toBe("text/html; charset=utf-8");
  });

  it("leaves an answer alone when the platform already applied _headers", async () => {
    stubFetch(() => json('{"status":"ok"}'));
    const e = env({ ASSETS: assets({ "content-type": "text/html", "content-security-policy": "default-src 'self'" }).fetcher });
    const { ctx, settle } = context();
    const res = await worker.fetch(new Request("https://icp.example/", { headers: NAVIGATE }), e, ctx);
    await settle();
    expect(res.headers.get("content-security-policy")).toBe("default-src 'self'");
    expect(res.headers.get("x-frame-options")).toBeNull();
  });

  it("uses exactly the '/*' block of apps/web/public/_headers", () => {
    const lines = siteHeadersText.split(/\r?\n/);
    const start = lines.findIndex((l) => l.trim() === "/*");
    expect(start).toBeGreaterThanOrEqual(0);
    const block: [string, string][] = [];
    for (const line of lines.slice(start + 1)) {
      if (!/^\s+\S/.test(line)) break; // the block ends at the first non-indented line
      const i = line.indexOf(":");
      block.push([line.slice(0, i).trim(), line.slice(i + 1).trim()]);
    }
    expect(block.length).toBeGreaterThan(0);
    expect(SITE_SECURITY_HEADERS.map(([n, v]) => [n.toLowerCase(), v])).toEqual(block.map(([n, v]) => [n.toLowerCase(), v]));
  });
});

describe("daily refresh of the public copies (Cron Trigger)", () => {
  const controller = { scheduledTime: T0, cron: "17 22 * * *", noRetry: () => undefined } as unknown as ScheduledController;
  const SNAPSHOT_KEY = "edge-cache/public/market-snapshot";
  const PLANS_KEY = "edge-cache/public/plans";

  it("stores both answers through the normal path with exactly two backend calls", async () => {
    const calls = stubFetch((req) => json(req.url.endsWith("/plans") ? '[{"id":"trial"}]' : '{"icp_period":"2026-09","icp_usd":112.81}'));
    const { kv, writes } = fakeKV();
    const e = env({ OBJECTS: kv });
    const { ctx, settle } = context();
    await worker.scheduled(controller, e, ctx);
    await settle();
    expect(calls.map((c) => new URL(c.url).pathname)).toEqual(["/api/v1/public/market-snapshot", "/api/v1/plans"]);
    for (const c of calls) {
      expect(c.headers.get("authorization")).toBeNull(); // anonymous, exactly like a visitor
      expect(c.headers.get("x-gateway-secret")).toBe(SECRET); // the backend only serves the gateway
    }
    expect(writes.map((w) => w.key).sort()).toEqual([PLANS_KEY, SNAPSHOT_KEY].sort());
    expect(logs.map((l) => JSON.parse(l) as { event: string; outcome: string }).map((l) => [l.event, l.outcome])).toEqual([
      ["gateway.public_refresh", "200 live"], ["gateway.public_refresh", "200 live"],
    ]);
  });

  it("waits for a cold backend that a visitor's request would give up on", async () => {
    // A cold backend: answers after 150 ms, slower than PUBLIC_UPSTREAM_TIMEOUT_MS (50 ms here).
    stubFetch((req, signal) => new Promise<Response>((resolve, reject) => {
      const timer = setTimeout(() => resolve(json(req.url.endsWith("/plans") ? '[{"id":"trial"}]' : '{"icp_usd":112.81}')), 150);
      signal?.addEventListener("abort", () => { clearTimeout(timer); reject(new Error("aborted")); });
    }));
    const { kv, writes } = fakeKV();
    const e = env({ OBJECTS: kv });
    const visitor = context();
    const res = await worker.fetch(new Request("https://icp.example/api/v1/plans"), e, visitor.ctx);
    expect(res.status).toBe(504); // no copy yet: the visitor gets the error envelope after 50 ms
    const cron = context();
    await worker.scheduled(controller, e, cron.ctx);
    await cron.settle();
    expect(writes.map((w) => w.key).sort()).toEqual([PLANS_KEY, SNAPSHOT_KEY].sort());
  });

  it("refreshes an unchanged copy from 23 hours on, where a visitor's answer waits for 24", async () => {
    expect(SCHEDULED_REFRESH_AFTER_MS).toBeLessThan(EDGE_CACHE_REFRESH_MS);
    const plans = '[{"id":"trial"}]';
    stubFetch(() => json(plans));
    const { kv, store, writes } = fakeKV();
    const bytes = new TextEncoder().encode(plans);
    const digest = await sha256(bytes);
    const storedAt = new Date(T0 - 23.5 * HOUR).toISOString();
    store.set(PLANS_KEY, { value: bytes.buffer.slice(0) as ArrayBuffer, metadata: { storedAt, sha256: digest, contentSha256: digest } });
    const e = env({ OBJECTS: kv });
    at(T0);
    const visitor = context();
    const res = await worker.fetch(new Request("https://icp.example/api/v1/plans"), e, visitor.ctx);
    expect(res.headers.get(CACHE_STATE_HEADER)).toBe("live");
    await visitor.settle();
    expect(writes).toHaveLength(0); // unchanged and younger than a day
    const cron = context();
    await worker.scheduled(controller, e, cron.ctx);
    await cron.settle();
    expect(writes.filter((w) => w.key === PLANS_KEY)).toEqual([
      { key: PLANS_KEY, metadata: { storedAt: new Date(T0).toISOString(), sha256: digest, contentSha256: digest } },
    ]);
  });

  it("logs and keeps the stored copies when the backend is down", async () => {
    stubFetch(() => { throw new Error("connection refused"); });
    const { kv, writes } = fakeKV();
    const e = env({ OBJECTS: kv });
    const { ctx, settle } = context();
    await expect(worker.scheduled(controller, e, ctx)).resolves.toBeUndefined();
    await settle();
    expect(writes).toHaveLength(0);
    expect(logs.map((l) => (JSON.parse(l) as { outcome: string }).outcome)).toEqual(["504 -", "504 -"]);
  });

  it("calls no backend while BACKEND_URL is a placeholder", async () => {
    const calls = stubFetch(() => json("{}"));
    const { ctx, settle } = context();
    await worker.scheduled(controller, env({ BACKEND_URL: "https://<vercel-production-domain>", OBJECTS: fakeKV().kv }), ctx);
    await settle();
    expect(calls).toHaveLength(0);
    expect(logs.map((l) => (JSON.parse(l) as { outcome: string }).outcome)).toEqual(["503 -", "503 -"]);
  });
});

interface WranglerEnv { vars?: Record<string, string>; triggers?: { crons?: string[] } }
interface WranglerConfig { assets?: { run_worker_first?: string[] | boolean }; triggers?: unknown; env?: Record<string, WranglerEnv> }

describe("wrangler.jsonc", () => {
  const config = parseJsonc(wranglerText) as WranglerConfig;

  it("runs the Worker for pages and the API but never for the static folders of the web app", () => {
    const patterns = config.assets?.run_worker_first;
    expect(Array.isArray(patterns)).toBe(true);
    const list = patterns as string[];
    expect(list).toContain("/*");
    // Every top-level entry of apps/web/public (copied to dist) and Vite's /assets must be served directly.
    const files = Object.keys(import.meta.glob(["../../web/public/**/*", "!../../web/public/_headers"]));
    const top = new Map<string, boolean>(); // name -> is a directory
    for (const f of files) {
      const rest = f.split("/web/public/")[1]!;
      const [first, ...more] = rest.split("/");
      top.set(first!, (top.get(first!) ?? false) || more.length > 0);
    }
    expect(top.size).toBeGreaterThan(0);
    const expected = ["!/assets/*", ...[...top].map(([name, dir]) => (dir ? `!/${name}/*` : `!/${name}`))];
    for (const pattern of expected) expect(list, pattern).toContain(pattern);
  });

  it("schedules one daily refresh for staging only", () => {
    const crons = config.env?.staging?.triggers?.crons ?? [];
    expect(crons).toHaveLength(1);
    expect(crons[0]).toMatch(/^\d{1,2} \d{1,2} \* \* \*$/); // once a day: no frequent keep-warm
    expect(config.triggers).toBeUndefined();
    expect(config.env?.production?.triggers).toBeUndefined();
  });

  it("keeps one stable backend origin for staging and the public budget below the browser's 10 s", () => {
    const vars = config.env?.staging?.vars ?? {};
    expect(vars.BACKEND_URL).toMatch(/^https:\/\//);
    expect(Number(vars.PUBLIC_UPSTREAM_TIMEOUT_MS)).toBeLessThan(10_000);
    expect(Number(vars.UPSTREAM_TIMEOUT_MS)).toBeLessThan(100_000); // Cloudflare's subrequest limit
  });
});
