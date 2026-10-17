import { afterEach, describe, expect, it, vi } from "vitest";
import worker, { type Env } from "../src/index";

const ctx = { waitUntil: () => undefined, passThroughOnException: () => undefined } as unknown as ExecutionContext;
const SECRET = "s".repeat(40);

function env(overrides: Partial<Env> = {}): Env {
  return {
    BACKEND_URL: "https://backend.example",
    GATEWAY_SHARED_SECRET: SECRET,
    ENVIRONMENT: "test",
    MAX_BODY_BYTES: "100",
    UPSTREAM_TIMEOUT_MS: "200",
    ASSETS: { fetch: async () => new Response("<html>app</html>", { headers: { "content-type": "text/html" } }) } as unknown as Fetcher,
    ...overrides,
  };
}

function mockUpstream(handler: (req: Request, signal?: AbortSignal) => Promise<Response> | Response) {
  const calls: Request[] = [];
  vi.stubGlobal("fetch", async (input: Request, init?: RequestInit) => {
    calls.push(input);
    return handler(input, init?.signal ?? undefined);
  });
  return calls;
}

afterEach(() => vi.unstubAllGlobals());

describe("API proxy", () => {
  it("forwards to the backend with secret, request id and client ip, never the client's secret", async () => {
    const calls = mockUpstream(() => new Response('{"ok":true}', {
      headers: { "content-type": "application/json", server: "uvicorn", connection: "keep-alive" },
    }));
    const res = await worker.fetch(new Request("https://icp.example/api/v1/data/summary?x=1", {
      headers: { "x-request-id": "client-req-123", "x-gateway-secret": "forged", "cf-connecting-ip": "203.0.113.5",
                 cookie: "session=abc" },
    }), env(), ctx);
    expect(res.status).toBe(200);
    expect(res.headers.get("x-request-id")).toBe("client-req-123");
    expect(res.headers.get("server")).toBeNull();
    const up = calls[0]!;
    expect(up.url).toBe("https://backend.example/api/v1/data/summary?x=1");
    expect(up.headers.get("x-gateway-secret")).toBe(SECRET);
    expect(up.headers.get("cf-connecting-ip")).toBe("203.0.113.5");
    expect(up.headers.get("cookie")).toBeNull();
  });

  it("answers 503 (not 500) while BACKEND_URL is still a placeholder", async () => {
    const calls = mockUpstream(() => new Response("never"));
    for (const BACKEND_URL of ["https://<render-staging-service>.onrender.com", "not a url", ""]) {
      const res = await worker.fetch(new Request("https://icp.example/api/v1/system/status"), env({ BACKEND_URL }), ctx);
      expect(res.status).toBe(503);
      expect(((await res.json()) as { error: { code: string } }).error.code).toBe("GATEWAY_MISCONFIGURED");
    }
    expect(calls).toHaveLength(0);
  });

  it("passes POST bodies and authorization through", async () => {
    const calls = mockUpstream(async (req) => new Response(await req.text(), { status: 201 }));
    const res = await worker.fetch(new Request("https://icp.example/api/v1/forecasts", {
      method: "POST", body: '{"horizon":2}', headers: { "content-type": "application/json", authorization: "Bearer t" },
    }), env(), ctx);
    expect(res.status).toBe(201);
    expect(await res.text()).toBe('{"horizon":2}');
    expect(calls[0]!.headers.get("authorization")).toBe("Bearer t");
  });

  it("rejects oversized bodies, bad methods and unknown paths before reaching the backend", async () => {
    const calls = mockUpstream(() => new Response("never"));
    const big = await worker.fetch(new Request("https://icp.example/api/v1/forecasts", { method: "POST", body: "x".repeat(101) }), env(), ctx);
    expect(big.status).toBe(413);
    const del = await worker.fetch(new Request("https://icp.example/api/v1/jobs/1", { method: "DELETE" }), env(), ctx);
    expect(del.status).toBe(405);
    const other = await worker.fetch(new Request("https://icp.example/api/internal"), env(), ctx);
    expect(other.status).toBe(404);
    expect(calls).toHaveLength(0);
    const body = (await big.json()) as { error: { code: string; request_id: string } };
    expect(body.error.code).toBe("PAYLOAD_TOO_LARGE");
    expect(body.error.request_id).toMatch(/^gw_/);
  });

  it("returns 503 when not configured and 504 when the backend hangs", async () => {
    const res = await worker.fetch(new Request("https://icp.example/api/v1/models"), env({ GATEWAY_SHARED_SECRET: "" }), ctx);
    expect(res.status).toBe(503);
    mockUpstream((_req, signal) => new Promise((_, reject) => signal?.addEventListener("abort", () => reject(new Error("aborted")))));
    const slow = await worker.fetch(new Request("https://icp.example/api/v1/models"), env(), ctx);
    expect(slow.status).toBe(504);
  });

  it("retries idempotent requests once on connection failure only", async () => {
    let n = 0;
    mockUpstream(() => { n++; if (n === 1) throw new TypeError("connection reset"); return new Response("ok"); });
    expect((await worker.fetch(new Request("https://icp.example/api/v1/models"), env(), ctx)).status).toBe(200);
    expect(n).toBe(2);
    n = 0;
    mockUpstream(() => { n++; throw new TypeError("connection reset"); });
    const post = await worker.fetch(new Request("https://icp.example/api/v1/forecasts", { method: "POST", body: "{}" }), env(), ctx);
    expect(post.status).toBe(504);
    expect(n).toBe(1);
  });

  it("applies the edge rate limiter with separate forecast and general buckets", async () => {
    mockUpstream(() => new Response("ok"));
    const keys: string[] = [];
    const deny = { limit: async ({ key }: { key: string }) => { keys.push(key); return { success: false }; } } as unknown as RateLimit;
    const allow = { limit: async ({ key }: { key: string }) => { keys.push(key); return { success: true }; } } as unknown as RateLimit;
    const e = env({ FORECAST_RATE_LIMITER: deny, API_RATE_LIMITER: allow });
    const fc = await worker.fetch(new Request("https://icp.example/api/v1/forecasts", { method: "POST", body: "{}",
      headers: { "cf-connecting-ip": "198.51.100.1" } }), e, ctx);
    expect(fc.status).toBe(429);
    expect(fc.headers.get("retry-after")).toBe("60");
    const get = await worker.fetch(new Request("https://icp.example/api/v1/models", { headers: { authorization: "Bearer secret-token" } }), e, ctx);
    expect(get.status).toBe(200);
    expect(keys[0]).toBe("fc:ip:198.51.100.1");
    expect(keys[1]).toMatch(/^api:auth:[0-9a-f]{24}$/);
    expect(keys.join()).not.toContain("secret-token");
  });

  it("refuses upstream redirects", async () => {
    mockUpstream(() => new Response(null, { status: 302, headers: { location: "https://elsewhere" } }));
    const res = await worker.fetch(new Request("https://icp.example/api/v1/models"), env(), ctx);
    expect(res.status).toBe(502);
  });
});

describe("non-API traffic", () => {
  it("serves the gateway health and static assets", async () => {
    const health = await worker.fetch(new Request("https://icp.example/api/gateway/health"), env(), ctx);
    expect(await health.json()).toEqual({ status: "ok", environment: "test", backend_configured: true });
    const page = await worker.fetch(new Request("https://icp.example/forecast"), env(), ctx);
    expect(await page.text()).toContain("app");
  });
});
