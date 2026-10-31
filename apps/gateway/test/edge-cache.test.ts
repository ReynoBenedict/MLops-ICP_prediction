/**
 * Contract C2: last-known-good copies of the anonymous public data in KV, and normalisation of
 * non-JSON upstream 5xx answers (Cloudflare 52x/530 HTML pages) into the standard error envelope.
 * KV, fetch and the clock are fakes; nothing leaves the process.
 */
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import worker, {
  backendConfigured,
  CACHED_AT_HEADER,
  CACHE_STATE_HEADER,
  EDGE_CACHE_REFRESH_MS,
  EDGE_CACHE_WRITE_INTERVAL_MS,
  positiveIntVar,
  publicCacheKey,
  type Env,
} from "../src/index";

const SECRET = "s".repeat(40);
const SNAPSHOT = "https://icp.example/api/v1/public/market-snapshot";
const PLANS = "https://icp.example/api/v1/plans";
const SNAPSHOT_KEY = "edge-cache/public/market-snapshot";
const PLANS_KEY = "edge-cache/public/plans";
const T0 = Date.parse("2026-10-25T08:00:00.000Z");
const MIN = 60_000;

/** ExecutionContext whose waitUntil promises can be awaited (the KV write runs there). */
function context() {
  const pending: Promise<unknown>[] = [];
  const ctx = { waitUntil: (p: Promise<unknown>) => { pending.push(p); }, passThroughOnException: () => undefined } as unknown as ExecutionContext;
  return { ctx, settle: async () => { await Promise.all(pending.splice(0)); } };
}

interface Stored { value: ArrayBuffer; metadata: unknown }

/** KV fake backed by a Map; counts every read and write. */
function fakeKV(options: { failReads?: boolean; failWrites?: boolean } = {}) {
  const store = new Map<string, Stored>();
  const reads: string[] = [];
  const writes: { key: string; metadata: unknown }[] = [];
  const kv = {
    getWithMetadata: async (key: string, type: string) => {
      reads.push(key);
      if (options.failReads) throw new Error("KV read failed");
      const hit = store.get(key);
      if (!hit) return { value: null, metadata: null };
      const copy = hit.value.slice(0);
      return { value: type === "stream" ? new Response(copy).body : copy, metadata: hit.metadata };
    },
    put: async (key: string, value: ArrayBuffer | string, opts?: { metadata?: unknown }) => {
      writes.push({ key, metadata: opts?.metadata ?? null });
      if (options.failWrites) throw new Error("KV put() limit exceeded for the day.");
      const buf = typeof value === "string" ? new TextEncoder().encode(value).buffer : value.slice(0);
      store.set(key, { value: buf as ArrayBuffer, metadata: opts?.metadata ?? null });
    },
  } as unknown as KVNamespace;
  return { kv, store, reads, writes };
}

async function sha256(bytes: ArrayBuffer | Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** Put an entry into the fake as a previous isolate would have written it. */
async function seed(store: Map<string, Stored>, key: string, body: string, storedAt: string) {
  const value = new TextEncoder().encode(body);
  store.set(key, { value: value.buffer.slice(0) as ArrayBuffer, metadata: { storedAt, sha256: await sha256(value) } });
}

function env(overrides: Partial<Env> = {}): Env {
  return {
    BACKEND_URL: "https://backend.example",
    GATEWAY_SHARED_SECRET: SECRET,
    ENVIRONMENT: "test",
    MAX_BODY_BYTES: "1000",
    UPSTREAM_TIMEOUT_MS: "10000",
    PUBLIC_UPSTREAM_TIMEOUT_MS: "80",
    ...overrides,
  };
}

const json = (body: string, status = 200) => new Response(body, { status, headers: { "content-type": "application/json" } });
const cloudflare530 = () => new Response("<!DOCTYPE html><html><head><title>Cloudflare Tunnel error | trycloudflare.com</title></head><body>Error 1033</body></html>",
  { status: 530, headers: { "content-type": "text/html; charset=UTF-8", server: "cloudflare" } });

function upstream(handler: (req: Request, signal?: AbortSignal) => Promise<Response> | Response) {
  const calls: Request[] = [];
  vi.stubGlobal("fetch", async (input: Request, init?: RequestInit) => {
    calls.push(input);
    return handler(input, init?.signal ?? undefined);
  });
  return calls;
}

const hang = (_req: Request, signal?: AbortSignal) =>
  new Promise<Response>((_, reject) => signal?.addEventListener("abort", () => reject(new Error("aborted"))));

function at(time: number) {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(time);
}

let warn: MockInstance<typeof console.warn>;
beforeEach(() => {
  // the Worker logs one JSON line per stale answer / normalised error; keep the test output clean
  warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("eligibility", () => {
  it("covers exactly anonymous GETs of the two public paths without a query string", () => {
    const key = (input: string, init?: RequestInit) => {
      const request = new Request(input, init);
      return publicCacheKey(request, new URL(request.url));
    };
    expect(key(SNAPSHOT)).toBe(SNAPSHOT_KEY);
    expect(key(PLANS)).toBe(PLANS_KEY);
    for (const [input, init] of [
      [`${SNAPSHOT}?lang=en`, undefined],
      [`${PLANS}?x=1`, undefined],
      [`${PLANS}/`, undefined],
      ["https://icp.example/api/v1/PLANS", undefined],
      ["https://icp.example/api/v1/public/market-snapshot/extra", undefined],
      ["https://icp.example/api/v1/models", undefined],
      [PLANS, { method: "HEAD" }],
      [PLANS, { method: "POST", body: "{}" }],
      [PLANS, { headers: { authorization: "Bearer icp_sess_" + "A".repeat(43) } }],
      [PLANS, { headers: { authorization: "" } }],
    ] as [string, RequestInit | undefined][]) {
      expect(key(input, init), `${init?.method ?? "GET"} ${input}`).toBeNull();
    }
  });
});

describe("live answers", () => {
  it("returns a live 200 as is, marks it live and stores it with { storedAt, sha256, contentSha256 }", async () => {
    at(T0);
    const body = '{"icp_period":"2026-09","icp_usd":112.81}';
    upstream(() => json(body));
    const { kv, store, writes } = fakeKV();
    const { ctx, settle } = context();
    const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(res.status).toBe(200);
    expect(await res.text()).toBe(body);
    expect(res.headers.get(CACHE_STATE_HEADER)).toBe("live");
    expect(res.headers.get(CACHED_AT_HEADER)).toBeNull();
    expect(res.headers.get("x-content-type-options")).toBe("nosniff");
    expect(writes.map((w) => w.key)).toEqual([SNAPSHOT_KEY]);
    const stored = store.get(SNAPSHOT_KEY)!;
    expect(new TextDecoder().decode(stored.value)).toBe(body);
    const bytesHash = await sha256(new TextEncoder().encode(body));
    // contentSha256 hashes the re-serialised JSON without generated_at; for this compact body without
    // that field it equals the byte hash
    expect(stored.metadata).toEqual({ storedAt: "2026-10-25T08:00:00.000Z", sha256: bytesHash, contentSha256: bytesHash });
  });

  it("does not rewrite a body whose hash is unchanged, even after the write interval", async () => {
    const body = '[{"id":"trial"}]';
    upstream(() => json(body));
    const { kv, writes } = fakeKV();
    const { ctx, settle } = context();
    for (const time of [T0, T0 + MIN, T0 + 6 * MIN, T0 + 3 * 60 * MIN]) {
      at(time);
      const res = await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
      expect(res.headers.get(CACHE_STATE_HEADER)).toBe("live");
      await settle();
    }
    expect(writes).toHaveLength(1);
  });

  it("writes a changed body at most once per 5 minutes per key", async () => {
    let n = 0;
    // the data itself changes on every answer (generated_at alone would not count as a change)
    upstream(() => { n++; return json(`{"icp_usd":${n},"generated_at":"2026-10-25T08:00:${String(n).padStart(2, "0")}Z"}`); });
    const { kv, writes } = fakeKV();
    const { ctx, settle } = context();
    const times = [T0, T0 + 1 * MIN, T0 + 4 * MIN, T0 + 5 * MIN, T0 + 6 * MIN, T0 + 9 * MIN, T0 + 10 * MIN];
    for (const time of times) {
      at(time);
      await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
      await settle();
    }
    expect(writes.map((w) => (w.metadata as { storedAt: string }).storedAt)).toEqual([
      new Date(T0).toISOString(), new Date(T0 + 5 * MIN).toISOString(), new Date(T0 + 10 * MIN).toISOString(),
    ]);
    // the plans key has its own budget
    upstream(() => json('[{"id":"trial"}]'));
    at(T0 + 10 * MIN);
    await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes.map((w) => w.key)).toEqual([SNAPSHOT_KEY, SNAPSHOT_KEY, SNAPSHOT_KEY, PLANS_KEY]);
  });

  it("respects a recent write by another isolate (read from KV metadata) and reads KV at most once per interval", async () => {
    const { kv, store, reads, writes } = fakeKV();
    await seed(store, SNAPSHOT_KEY, '{"old":true}', new Date(T0 - 2 * MIN).toISOString());
    let n = 0;
    upstream(() => json(`{"n":${++n}}`));
    const { ctx, settle } = context();
    for (const time of [T0, T0 + MIN, T0 + 2 * MIN]) {
      at(time);
      await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
      await settle();
    }
    expect(writes).toHaveLength(0); // stored 2 min before T0: throttled until T0 + 3 min
    expect(reads).toEqual([SNAPSHOT_KEY]); // later answers use the isolate's memo
    at(T0 + 5 * MIN);
    await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes).toHaveLength(1);
  });

  it("returns the live answer even when the KV write fails (daily write quota)", async () => {
    upstream(() => json('{"ok":true}'));
    const { kv, writes } = fakeKV({ failWrites: true });
    const { ctx, settle } = context();
    const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
    expect(writes).toHaveLength(1);
    const logged = warn.mock.calls.map((c) => String(c[0])).join("\n");
    expect(logged).toContain('"event":"gateway.edge_cache_write_failed"');
    expect(logged).toContain("limit exceeded");
  });

  it("never stores non-JSON, non-200 or oversized answers", async () => {
    const { kv, writes } = fakeKV();
    const { ctx, settle } = context();
    const answers = [
      () => new Response("<html>ok</html>", { status: 200, headers: { "content-type": "text/html" } }),
      () => json("not json at all"),
      () => json('"a string"'),
      () => json('{"error":{"code":"RATE_LIMITED"}}', 429),
      () => new Response(null, { status: 204 }),
      () => json(`{"x":"${"a".repeat(300 * 1024)}"}`),
    ];
    for (const answer of answers) {
      upstream(answer);
      await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
      await settle();
    }
    expect(writes).toHaveLength(0);
  });
});

describe("KV write budget", () => {
  const snapshotAt = (time: number, icp = 112.81) =>
    `{"icp_period":"2026-09","icp_usd":${icp},"recent":[{"period":"2026-09","icp_usd":${icp}}],"generated_at":"${new Date(time).toISOString()}"}`;

  it("ignores generated_at when deciding whether the snapshot changed: identical data is written once", async () => {
    const { kv, store, writes } = fakeKV();
    const { ctx, settle } = context();
    upstream(() => json(snapshotAt(Date.now())));
    // a visitor every few minutes for a day: every body differs in generated_at only
    for (let time = T0; time < T0 + 24 * 60 * MIN; time += 7 * MIN) {
      at(time);
      const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
      expect(res.headers.get(CACHE_STATE_HEADER)).toBe("live");
      expect(await res.text()).toBe(snapshotAt(time)); // the live answer is untouched
      await settle();
    }
    expect(writes).toHaveLength(1);
    // the byte sha256 still guards integrity; the content hash is stored next to it
    const stored = store.get(SNAPSHOT_KEY)!;
    const meta = stored.metadata as { storedAt: string; sha256: string; contentSha256: string };
    expect(meta.storedAt).toBe(new Date(T0).toISOString());
    expect(meta.sha256).toBe(await sha256(stored.value));
    expect(meta.contentSha256).toMatch(/^[0-9a-f]{64}$/);
    expect(meta.contentSha256).not.toBe(meta.sha256);
  });

  it("still writes (at most once per interval) when the data changes, whatever generated_at says", async () => {
    const { kv, store, writes } = fakeKV();
    const { ctx, settle } = context();
    let icp = 112.81;
    upstream(() => json(snapshotAt(Date.now(), icp)));
    at(T0);
    await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    icp = 113.5; // a correction arrives one minute later
    at(T0 + MIN);
    await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes).toHaveLength(1); // throttled: the stored copy is younger than 5 minutes
    at(T0 + 5 * MIN);
    await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes).toHaveLength(2);
    expect(new TextDecoder().decode(store.get(SNAPSHOT_KEY)!.value)).toContain('"icp_usd":113.5');
  });

  it("keeps working with entries stored before the content hash existed (metadata { storedAt, sha256 })", async () => {
    const { kv, store, writes } = fakeKV();
    const { ctx, settle } = context();
    const plans = '[{"id":"trial"}]';
    await seed(store, PLANS_KEY, plans, new Date(T0 - 60 * MIN).toISOString());
    await seed(store, SNAPSHOT_KEY, snapshotAt(T0 - 60 * MIN), new Date(T0 - 60 * MIN).toISOString());
    upstream(() => json(plans));
    at(T0);
    await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes).toHaveLength(0); // same bytes: not rewritten just because the format gained a field
    upstream(() => json(snapshotAt(Date.now())));
    for (const time of [T0, T0 + 10 * MIN, T0 + 20 * MIN]) {
      at(time);
      await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
      await settle();
    }
    // the old entry cannot prove the data is the same, so it is replaced once, then never again
    expect(writes.map((w) => w.key)).toEqual([SNAPSHOT_KEY]);
    // an old-format entry is still served as the stale copy
    upstream(cloudflare530);
    const stale = await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
    expect(stale.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(await stale.text()).toBe(plans);
  });

  it("serves an entry whose optional content hash is malformed (integrity rests on the byte sha256)", async () => {
    const { kv, store } = fakeKV();
    const value = new TextEncoder().encode('{"x":1}');
    store.set(SNAPSHOT_KEY, { value: value.buffer.slice(0) as ArrayBuffer,
      metadata: { storedAt: "2026-10-20T00:00:00.000Z", sha256: await sha256(value), contentSha256: "not-a-hash" } });
    upstream(cloudflare530);
    const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
    expect(res.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(await res.text()).toBe('{"x":1}');
  });

  it("remembers a failed write for one interval: an exhausted quota costs one put per key per interval, not one per answer", async () => {
    const { kv, writes } = fakeKV({ failWrites: true });
    const { ctx, settle } = context();
    let n = 0;
    upstream(() => json(`{"icp_usd":${++n}}`)); // the data changes on every answer
    for (const time of [T0, T0 + 1, T0 + MIN, T0 + 2 * MIN, T0 + 4 * MIN]) {
      at(time);
      for (let i = 0; i < 3; i++) {
        const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
        expect(res.status).toBe(200);
        expect(res.headers.get(CACHE_STATE_HEADER)).toBe("live");
      }
      await settle();
    }
    expect(writes).toHaveLength(1); // 15 live answers, one attempted put
    const failures = () => warn.mock.calls.filter((c) => String(c[0]).includes("gateway.edge_cache_write_failed")).length;
    expect(failures()).toBe(1);
    at(T0 + 5 * MIN);
    await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes).toHaveLength(2); // tried again once the interval has passed
    expect(failures()).toBe(2);
  });

  it("keeps the write back-off when the isolate re-reads KV in the meantime", async () => {
    const { kv, store, reads, writes } = fakeKV({ failWrites: true });
    await seed(store, SNAPSHOT_KEY, '{"icp_usd":1}', new Date(T0 - 60 * MIN).toISOString());
    const { ctx, settle } = context();
    const live = async (time: number, body: string) => {
      at(time);
      upstream(() => json(body));
      await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
      await settle();
    };
    await live(T0 - 4 * MIN, '{"icp_usd":1}'); // read once, unchanged
    await live(T0, '{"icp_usd":2}'); // changed: one put, which fails
    expect(writes).toHaveLength(1);
    at(T0 + 2 * MIN); // outage: the memo is 6 minutes old, so KV is read again
    upstream(cloudflare530);
    const stale = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    expect(stale.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(reads).toHaveLength(2);
    await live(T0 + 3 * MIN, '{"icp_usd":3}');
    expect(writes).toHaveLength(1); // the re-read did not reset the back-off
    await live(T0 + 5 * MIN, '{"icp_usd":3}');
    expect(writes).toHaveLength(2);
  });

  it("rewrites an unchanged copy once a day, so the stored time is never more than a day older than the last live answer", async () => {
    const { kv, store, writes } = fakeKV();
    const { ctx, settle } = context();
    const plans = '[{"id":"trial"}]';
    const DAY = 24 * 60 * MIN;
    const STEP = 7 * MIN;
    let last = T0;
    // a visitor every 7 minutes for three days; the data never changes, generated_at always does
    for (let time = T0; time < T0 + 3 * DAY; time += STEP) {
      at(time);
      upstream((req) => json(new URL(req.url).pathname === "/api/v1/plans" ? plans : snapshotAt(time)));
      await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
      await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
      await settle();
      for (const key of [SNAPSHOT_KEY, PLANS_KEY]) {
        const storedAt = Date.parse((store.get(key)!.metadata as { storedAt: string }).storedAt);
        expect(time - storedAt).toBeLessThan(DAY + STEP); // the bound the stale notice relies on
      }
      last = time;
    }
    // the first answer, then one refresh per key per day (at the first answer at least a day after the last write)
    const firstRefresh = T0 + Math.ceil(DAY / STEP) * STEP;
    const expected = [T0, firstRefresh, firstRefresh + (firstRefresh - T0)].map((t) => new Date(t).toISOString());
    const storedTimes = (key: string) =>
      writes.filter((w) => w.key === key).map((w) => (w.metadata as { storedAt: string }).storedAt);
    expect(storedTimes(SNAPSHOT_KEY)).toEqual(expected);
    expect(storedTimes(PLANS_KEY)).toEqual(expected);
    expect(writes).toHaveLength(6); // 2 a day for both keys, instead of up to 288 a day for the snapshot alone
    // an outage right after the last visit: the stale notice names a time less than a day before it
    upstream(cloudflare530);
    at(last + MIN);
    const stale = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    expect(stale.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(last - Date.parse(stale.headers.get(CACHED_AT_HEADER)!)).toBeLessThan(DAY);
  });

  it("refreshes an unchanged entry older than a day once (old metadata format included), then leaves it alone", async () => {
    const { kv, store, writes } = fakeKV();
    const { ctx, settle } = context();
    const plans = '[{"id":"trial"}]';
    await seed(store, PLANS_KEY, plans, new Date(T0 - 3 * 24 * 60 * MIN).toISOString());
    upstream(() => json(plans));
    for (const time of [T0, T0 + MIN, T0 + 10 * MIN, T0 + 23 * 60 * MIN]) {
      at(time);
      await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
      await settle();
    }
    expect(writes).toHaveLength(1);
    const meta = store.get(PLANS_KEY)!.metadata as { storedAt: string; sha256: string; contentSha256: string };
    expect(meta.storedAt).toBe(new Date(T0).toISOString());
    expect(meta.contentSha256).toMatch(/^[0-9a-f]{64}$/);
    expect(new TextDecoder().decode(store.get(PLANS_KEY)!.value)).toBe(plans);
  });

  it("a failed daily refresh is remembered like any failed write and the stored copy is still served", async () => {
    const { kv, store, writes } = fakeKV({ failWrites: true });
    const { ctx, settle } = context();
    const seededAt = new Date(T0 - 2 * 24 * 60 * MIN).toISOString();
    await seed(store, PLANS_KEY, '[{"id":"trial"}]', seededAt);
    upstream(() => json('[{"id":"trial"}]'));
    for (const time of [T0, T0 + MIN, T0 + 4 * MIN]) {
      at(time);
      await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
      await settle();
    }
    expect(writes).toHaveLength(1); // one attempted refresh, then the back-off
    upstream(cloudflare530);
    const stale = await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
    expect(stale.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(stale.headers.get(CACHED_AT_HEADER)).toBe(seededAt);
    upstream(() => json('[{"id":"trial"}]'));
    at(T0 + 5 * MIN);
    await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes).toHaveLength(2); // retried once the interval has passed
  });

  it("a failed write does not hide the copy that is already stored", async () => {
    const { kv, store, writes } = fakeKV({ failWrites: true });
    await seed(store, SNAPSHOT_KEY, '{"icp_usd":1}', new Date(T0 - 60 * MIN).toISOString());
    const { ctx, settle } = context();
    at(T0);
    upstream(() => json('{"icp_usd":2}'));
    await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes).toHaveLength(1);
    upstream(cloudflare530);
    const stale = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    expect(stale.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(await stale.text()).toBe('{"icp_usd":1}');
  });
});

describe("stale answers", () => {
  it("serves the stored copy with stale headers when the upstream answers a Cloudflare 530 HTML page", async () => {
    const { kv, store } = fakeKV();
    const body = '{"icp_period":"2026-09","icp_usd":112.81}';
    await seed(store, SNAPSHOT_KEY, body, "2026-10-24T21:15:00.000Z");
    upstream(cloudflare530);
    const res = await worker.fetch(new Request(SNAPSHOT, { headers: { "x-request-id": "req-stale-0001" } }), env({ OBJECTS: kv }), context().ctx);
    expect(res.status).toBe(200);
    expect(res.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(res.headers.get(CACHED_AT_HEADER)).toBe("2026-10-24T21:15:00.000Z");
    expect(res.headers.get("cache-control")).toBe("no-store");
    expect(res.headers.get("content-type")).toBe("application/json");
    expect(res.headers.get("x-request-id")).toBe("req-stale-0001");
    expect(res.headers.get("x-content-type-options")).toBe("nosniff");
    expect(res.headers.get("server")).toBeNull();
    expect(await res.text()).toBe(body);
  });

  it("answers the JSON error envelope (never the HTML page) for a 530 when nothing is stored", async () => {
    const { kv } = fakeKV();
    upstream(cloudflare530);
    const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
    expect(res.status).toBe(502);
    expect(res.headers.get("content-type")).toBe("application/json");
    expect(res.headers.get(CACHE_STATE_HEADER)).toBeNull();
    const text = await res.text();
    expect(text).not.toContain("<");
    expect(JSON.parse(text).error.code).toBe("UPSTREAM_TIMEOUT_OR_UNAVAILABLE");
  });

  it("serves the stored copy when the upstream times out, using the public timeout instead of the general one", async () => {
    const { kv, store } = fakeKV();
    await seed(store, PLANS_KEY, '[{"id":"trial"}]', "2026-10-20T00:00:00.000Z");
    upstream(hang);
    const started = Date.now();
    const res = await worker.fetch(new Request(PLANS), env({ OBJECTS: kv, UPSTREAM_TIMEOUT_MS: "10000", PUBLIC_UPSTREAM_TIMEOUT_MS: "60" }), context().ctx);
    expect(Date.now() - started).toBeLessThan(2000); // 60 ms public timeout, not the 10 s general one
    expect(res.status).toBe(200);
    expect(res.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(await res.json()).toEqual([{ id: "trial" }]);
  });

  it("serves the stored copy on a network error and on any upstream status >= 500, JSON or not", async () => {
    const { kv, store } = fakeKV();
    await seed(store, PLANS_KEY, '[{"id":"trial"}]', "2026-10-20T00:00:00.000Z");
    const failures: (() => Response)[] = [
      () => { throw new TypeError("connection refused"); },
      () => json('{"error":{"code":"INTERNAL_ERROR"}}', 500),
      () => json('{"error":{"code":"DATASET_UNAVAILABLE"}}', 503),
      () => new Response("Bad gateway", { status: 502 }),
      () => new Response("<html>522</html>", { status: 522, headers: { "content-type": "text/html" } }),
    ];
    for (const failure of failures) {
      upstream(failure);
      const res = await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), context().ctx);
      expect(res.status).toBe(200);
      expect(res.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    }
  });

  it("serves the cached bytes unchanged (byte-identical)", async () => {
    const body = '{\n  "source": "Kementerian ESDM — ICP resmi",\t"unit":"USD/bbl", "emoji":"\u{1F6E2}", "esc":"\\u00e9" }';
    upstream(() => json(body));
    const { kv, store } = fakeKV();
    const { ctx, settle } = context();
    const live = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    const liveBytes = new Uint8Array(await live.arrayBuffer());
    await settle();
    upstream(cloudflare530);
    const stale = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    expect(stale.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    const staleBytes = new Uint8Array(await stale.arrayBuffer());
    expect(staleBytes).toEqual(new TextEncoder().encode(body));
    expect(staleBytes).toEqual(liveBytes);
    // read back from KV by a fresh isolate (no memo): the same bytes
    const other = fakeKV();
    other.store.set(SNAPSHOT_KEY, store.get(SNAPSHOT_KEY)!);
    const fromKv = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: other.kv }), ctx);
    expect(other.reads).toEqual([SNAPSHOT_KEY]);
    expect(new Uint8Array(await fromKv.arrayBuffer())).toEqual(staleBytes);
    // a second stale answer from the isolate's memo is identical too (the memo buffer is not consumed)
    const again = new Uint8Array(await (await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx)).arrayBuffer());
    expect(again).toEqual(staleBytes);
  });

  it("never serves an entry whose bytes do not match its sha256 or whose metadata is malformed", async () => {
    upstream(cloudflare530);
    const value = new TextEncoder().encode('{"x":1}');
    const good = await sha256(value);
    for (const metadata of [
      { storedAt: "2026-10-20T00:00:00.000Z", sha256: "0".repeat(64) }, // bytes do not match
      { storedAt: "2026-10-20T00:00:00.000Z\r\nSet-Cookie: x=1", sha256: good }, // header injection attempt
      { storedAt: "yesterday", sha256: good },
      { storedAt: "2026-10-20T00:00:00.000Z", sha256: good.toUpperCase() },
      { sha256: good },
      null,
    ]) {
      const { kv, store } = fakeKV();
      store.set(SNAPSHOT_KEY, { value: value.buffer.slice(0) as ArrayBuffer, metadata });
      const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
      expect(res.status).toBe(502);
      expect(res.headers.get(CACHE_STATE_HEADER)).toBeNull();
    }
  });

  it("normalises the stored timestamp before it reaches the X-ICP-Cached-At header", async () => {
    const { kv, store } = fakeKV();
    const value = new TextEncoder().encode('{"x":1}');
    store.set(SNAPSHOT_KEY, { value: value.buffer.slice(0) as ArrayBuffer, metadata: { storedAt: "2026-10-20T07:00:00+07:00", sha256: await sha256(value) } });
    upstream(cloudflare530);
    const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
    expect(res.headers.get(CACHED_AT_HEADER)).toBe("2026-10-20T00:00:00.000Z");
  });

  it("falls back to the normal error path when KV itself fails", async () => {
    const { kv } = fakeKV({ failReads: true });
    upstream(() => { throw new TypeError("connection refused"); });
    const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
    expect(res.status).toBe(504);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("UPSTREAM_TIMEOUT_OR_UNAVAILABLE");
  });

  it("remembers that no copy exists: failing requests with an empty KV cost at most one read per key per interval", async () => {
    const { kv, reads } = fakeKV();
    upstream(cloudflare530);
    for (const time of [T0, T0 + 1, T0 + MIN, T0 + 4 * MIN]) {
      at(time);
      for (let i = 0; i < 5; i++) {
        const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
        expect(res.status).toBe(502);
      }
    }
    expect(reads).toEqual([SNAPSHOT_KEY]); // 20 failing requests, one KV read
    at(T0 + 5 * MIN);
    await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
    expect(reads).toEqual([SNAPSHOT_KEY, SNAPSHOT_KEY]); // re-checked once the interval has passed
  });

  it("re-reads a malformed or tampered entry at most once per interval as well", async () => {
    for (const metadata of [{ storedAt: "yesterday" }, { storedAt: "2026-10-20T00:00:00.000Z", sha256: "0".repeat(64) }]) {
      const { kv, store, reads } = fakeKV();
      store.set(SNAPSHOT_KEY, { value: new TextEncoder().encode('{"x":1}').buffer as ArrayBuffer, metadata });
      upstream(cloudflare530);
      at(T0);
      for (let i = 0; i < 5; i++) {
        expect((await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx)).status).toBe(502);
      }
      expect(reads, JSON.stringify(metadata)).toHaveLength(1);
    }
  });

  it("replaces a tampered entry with the next live answer without waiting for the write interval", async () => {
    const { kv, store, writes } = fakeKV();
    store.set(SNAPSHOT_KEY, { value: new TextEncoder().encode('{"x":1}').buffer as ArrayBuffer,
      metadata: { storedAt: new Date(T0 - MIN).toISOString(), sha256: "0".repeat(64) } });
    const { ctx, settle } = context();
    at(T0);
    upstream(cloudflare530);
    expect((await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx)).status).toBe(502);
    upstream(() => json('{"x":2}'));
    await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(writes).toHaveLength(1);
    upstream(cloudflare530);
    const stale = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), ctx);
    expect(stale.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(await stale.text()).toBe('{"x":2}');
  });

  it("writes the first live answer at once after an outage in which no copy existed", async () => {
    const { kv, store, reads, writes } = fakeKV();
    const { ctx, settle } = context();
    at(T0);
    upstream(cloudflare530);
    await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
    at(T0 + MIN);
    upstream(() => json('[{"id":"trial"}]'));
    const live = await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
    await settle();
    expect(live.headers.get(CACHE_STATE_HEADER)).toBe("live");
    expect(writes.map((w) => w.key)).toEqual([PLANS_KEY]);
    expect(reads).toEqual([PLANS_KEY]); // the "no copy" memo is reused, no second read
    upstream(cloudflare530);
    const stale = await worker.fetch(new Request(PLANS), env({ OBJECTS: kv }), ctx);
    expect(stale.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(store.has(PLANS_KEY)).toBe(true);
  });

  it("serves the stored copy when the upstream answers a non-JSON 4xx page (the request never reached the backend)", async () => {
    const { kv, store } = fakeKV();
    await seed(store, SNAPSHOT_KEY, '{"x":1}', "2026-10-20T00:00:00.000Z");
    upstream(() => new Response("<html>Not Found at internal-render-host.onrender.com</html>", { status: 404, headers: { "content-type": "text/html" } }));
    const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
    expect(res.status).toBe(200);
    expect(res.headers.get(CACHE_STATE_HEADER)).toBe("stale");
    expect(await res.text()).toBe('{"x":1}');
  });

  it("does not answer 4xx upstream errors from the cache", async () => {
    const { kv, store, reads } = fakeKV();
    await seed(store, SNAPSHOT_KEY, '{"x":1}', "2026-10-20T00:00:00.000Z");
    upstream(() => json('{"error":{"code":"RATE_LIMITED","message":"Slow down.","request_id":"r","details":{}}}', 429));
    const res = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: kv }), context().ctx);
    expect(res.status).toBe(429);
    expect(res.headers.get(CACHE_STATE_HEADER)).toBeNull();
    expect(reads).toHaveLength(0);
  });
});

describe("bypass", () => {
  it("never reads or writes the cache for requests with an Authorization header", async () => {
    const { kv, store, reads, writes } = fakeKV();
    await seed(store, SNAPSHOT_KEY, '{"x":1}', "2026-10-20T00:00:00.000Z");
    const { ctx, settle } = context();
    for (const authorization of ["Bearer icp_sess_" + "A".repeat(43), "Bearer junk", ""]) {
      upstream(() => json('{"member":"view"}'));
      const ok = await worker.fetch(new Request(SNAPSHOT, { headers: { authorization } }), env({ OBJECTS: kv }), ctx);
      expect(ok.headers.get(CACHE_STATE_HEADER)).toBeNull();
      upstream(cloudflare530);
      const failed = await worker.fetch(new Request(SNAPSHOT, { headers: { authorization } }), env({ OBJECTS: kv }), ctx);
      expect(failed.status).toBe(502);
      expect(failed.headers.get(CACHE_STATE_HEADER)).toBeNull();
    }
    await settle();
    expect(reads).toHaveLength(0);
    expect(writes).toHaveLength(0);
  });

  it("uses the general timeout for credentialed requests to the public paths", async () => {
    upstream(async (_req, signal) => {
      await new Promise((resolve) => setTimeout(resolve, 150));
      if (signal?.aborted) throw new Error("aborted");
      return json('{"late":true}');
    });
    const res = await worker.fetch(new Request(PLANS, { headers: { authorization: "Bearer x" } }),
      env({ OBJECTS: fakeKV().kv, PUBLIC_UPSTREAM_TIMEOUT_MS: "20" }), context().ctx);
    expect(res.status).toBe(200);
  });

  it("never caches other paths, query strings or other methods", async () => {
    const { kv, reads, writes } = fakeKV();
    const { ctx, settle } = context();
    const requests: [string, RequestInit?][] = [
      [`${SNAPSHOT}?lang=en`], [`${PLANS}?cache=bust`], [`${PLANS}/`], ["https://icp.example/api/v1/data/summary"],
      [PLANS, { method: "HEAD" }], [PLANS, { method: "POST", body: "{}" }],
    ];
    for (const [input, init] of requests) {
      upstream(() => json('{"ok":true}'));
      const ok = await worker.fetch(new Request(input, init), env({ OBJECTS: kv }), ctx);
      expect(ok.headers.get(CACHE_STATE_HEADER), input).toBeNull();
      upstream(() => { throw new TypeError("down"); });
      const down = await worker.fetch(new Request(input, init), env({ OBJECTS: kv }), ctx);
      expect(down.status, input).toBe(504);
    }
    await settle();
    expect(reads).toHaveLength(0);
    expect(writes).toHaveLength(0);
  });

  it("is inert without the KV binding: no cache headers, the normal error path", async () => {
    upstream(() => json('{"ok":true}'));
    const ok = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: undefined }), context().ctx);
    expect(ok.status).toBe(200);
    expect(ok.headers.get(CACHE_STATE_HEADER)).toBeNull();
    expect(await ok.json()).toEqual({ ok: true });
    upstream(hang);
    const down = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: undefined }), context().ctx);
    expect(down.status).toBe(504);
    upstream(cloudflare530);
    const html = await worker.fetch(new Request(SNAPSHOT), env({ OBJECTS: undefined }), context().ctx);
    expect(html.status).toBe(502);
    expect(html.headers.get("content-type")).toBe("application/json");
  });
});

describe("non-JSON upstream error normalisation (every /api/v1 route)", () => {
  it("replaces HTML error pages with the envelope: 504 for timeouts (504/522/524), 502 otherwise", async () => {
    const cases: [number, string, number][] = [
      [530, "text/html", 502], [502, "text/html", 502], [500, "text/plain", 502], [503, "", 502],
      [520, "text/html", 502], [521, "text/html", 502], [522, "text/html", 504], [524, "text/html", 504], [504, "text/html", 504],
    ];
    for (const [status, type, expected] of cases) {
      upstream(() => new Response(`<html><body>error ${status} at origin.example.internal</body></html>`,
        { status, headers: type ? { "content-type": type } : {} }));
      const res = await worker.fetch(new Request("https://icp.example/api/v1/forecasts", { method: "POST", body: "{}",
        headers: { authorization: "Bearer t", "x-request-id": "req-normalise-01" } }), env(), context().ctx);
      expect(res.status, String(status)).toBe(expected);
      expect(res.headers.get("content-type")).toBe("application/json");
      expect(res.headers.get("cache-control")).toBe("no-store");
      const text = await res.text();
      expect(text).not.toContain("origin.example.internal");
      expect(JSON.parse(text)).toEqual({ error: { code: "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", message: expect.any(String),
        request_id: "req-normalise-01", details: {} } });
    }
    // the operator log line carries the upstream status and request id, not the page or the backend URL
    expect(warn.mock.calls.map((c) => String(c[0])).join("\n")).not.toContain("origin.example.internal");
    expect(String(warn.mock.calls[0]![0])).toContain('"upstream_status":530');
  });

  it("replaces non-JSON 4xx pages (produced in front of the backend) with a 502 envelope", async () => {
    const pages: [number, Record<string, string>, string][] = [
      [404, { "content-type": "text/html" }, "<html>Not Found at internal-render-host.onrender.com</html>"],
      [429, { "content-type": "text/html; charset=UTF-8", "retry-after": "30" }, "<title>Error 1015 | abc.trycloudflare.com</title>"],
      [403, { "content-type": "text/plain" }, "error code: 1020 internal-render-host.onrender.com"],
      [401, {}, ""],
    ];
    for (const [status, headers, page] of pages) {
      upstream(() => new Response(page, { status, headers }));
      for (const request of [
        new Request("https://icp.example/api/v1/data/summary", { headers: { authorization: "Bearer t", "x-request-id": "req-normalise-4xx" } }),
        new Request(SNAPSHOT, { headers: { "x-request-id": "req-normalise-4xx" } }),
      ]) {
        const res = await worker.fetch(request, env({ OBJECTS: fakeKV().kv }), context().ctx);
        expect(res.status, `${status} ${request.url}`).toBe(502);
        expect(res.headers.get("content-type")).toBe("application/json");
        const text = await res.text();
        expect(text).not.toMatch(/onrender|trycloudflare|<html|<title/);
        expect(JSON.parse(text)).toEqual({ error: { code: "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", message: expect.any(String),
          request_id: "req-normalise-4xx", details: {} } });
      }
    }
    const logged = warn.mock.calls.map((c) => String(c[0])).join("\n");
    expect(logged).toContain('"upstream_status":404');
    expect(logged).not.toMatch(/onrender|trycloudflare/);
  });

  it("passes JSON 4xx answers and non-JSON 2xx answers (file downloads) through unchanged", async () => {
    const error = '{"error":{"code":"AUTH_REQUIRED","message":"Sign in.","request_id":"r","details":{}}}';
    upstream(() => json(error, 401));
    const denied = await worker.fetch(new Request("https://icp.example/api/v1/data/summary"), env(), context().ctx);
    expect(denied.status).toBe(401);
    expect(await denied.text()).toBe(error);
    upstream(() => new Response("period,icp_usd\n2026-09,112.81\n", { status: 200, headers: { "content-type": "text/csv" } }));
    const csv = await worker.fetch(new Request("https://icp.example/api/v1/forecasts/f1/export", { headers: { authorization: "Bearer t" } }), env(), context().ctx);
    expect(csv.status).toBe(200);
    expect(csv.headers.get("content-type")).toBe("text/csv");
    expect(await csv.text()).toBe("period,icp_usd\n2026-09,112.81\n");
  });

  it("passes JSON 5xx answers from the backend through unchanged", async () => {
    const body = '{"error":{"code":"MODEL_UNAVAILABLE","message":"No production model.","request_id":"req_1","details":{}}}';
    upstream(() => json(body, 503));
    const res = await worker.fetch(new Request("https://icp.example/api/v1/forecasts", { method: "POST", body: "{}" }), env(), context().ctx);
    expect(res.status).toBe(503);
    expect(await res.text()).toBe(body);
  });
});

describe("data plane keeps out of the edge-cache/ namespace", () => {
  it("refuses reads and writes of edge-cache/ keys without touching KV", async () => {
    const { kv, store, reads, writes } = fakeKV();
    await seed(store, SNAPSHOT_KEY, '{"x":1}', "2026-10-20T00:00:00.000Z");
    const auth = { "x-gateway-secret": SECRET };
    for (const key of [SNAPSHOT_KEY, "edge-cache%2Fpublic%2Fplans", "edge-cache/anything"]) {
      const get = await worker.fetch(new Request(`https://icp.example/api/internal/objects/${key}`, { headers: auth }), env({ OBJECTS: kv }), context().ctx);
      expect(get.status, key).toBe(400);
      const put = await worker.fetch(new Request(`https://icp.example/api/internal/objects/${key}`, {
        method: "PUT", headers: { ...auth, "content-type": "application/json" }, body: '{"poison":true}' }), env({ OBJECTS: kv }), context().ctx);
      expect(put.status, key).toBe(400);
    }
    expect(reads).toHaveLength(0);
    expect(writes).toHaveLength(0);
    // backend prefixes are unaffected
    const ok = await worker.fetch(new Request("https://icp.example/api/internal/objects/datasets/icp/abc.csv", {
      method: "PUT", headers: { ...auth, "content-type": "text/csv" }, body: "a,b" }), env({ OBJECTS: kv }), context().ctx);
    expect(ok.status).toBe(201);
  });
});

describe("configuration hardening", () => {
  it("reads numeric variables strictly and falls back to the defaults", () => {
    expect(positiveIntVar("8000", 1)).toBe(8000);
    expect(positiveIntVar(" 250 ", 1)).toBe(250);
    for (const bad of [undefined, "", "abc", "-5", "0", "1e3", "12.5", "NaN", "99999999999999999999"]) {
      expect(positiveIntVar(bad, 42), String(bad)).toBe(42);
    }
  });

  it("keeps the body limit when MAX_BODY_BYTES is malformed (it used to disable the limit)", async () => {
    const calls = upstream(() => json("{}"));
    const res = await worker.fetch(new Request("https://icp.example/api/v1/forecasts", { method: "POST", body: "x".repeat(2_000_001) }),
      env({ MAX_BODY_BYTES: "two megabytes" }), context().ctx);
    expect(res.status).toBe(413);
    expect(calls).toHaveLength(0);
  });

  it("keeps a working timeout when UPSTREAM_TIMEOUT_MS is malformed (NaN used to abort every request)", async () => {
    upstream(async (_req, signal) => {
      await new Promise((r) => setTimeout(r, 30));
      if (signal?.aborted) throw new Error("aborted");
      return json('{"ok":true}');
    });
    const res = await worker.fetch(new Request("https://icp.example/api/v1/models"), env({ UPSTREAM_TIMEOUT_MS: "" }), context().ctx);
    expect(res.status).toBe(200);
  });

  it("bounds a retry by the same deadline (one connection-failure retry never doubles the wait)", async () => {
    let n = 0;
    upstream((req, signal) => {
      n++;
      if (n === 1) return new Promise((_, reject) => setTimeout(() => reject(new TypeError("connection reset")), 120));
      return hang(req, signal);
    });
    const started = Date.now();
    const res = await worker.fetch(new Request("https://icp.example/api/v1/models"), env({ UPSTREAM_TIMEOUT_MS: "160" }), context().ctx);
    expect(res.status).toBe(504);
    expect(n).toBe(2);
    expect(Date.now() - started).toBeLessThan(245); // ≈160 ms, not 120 + 160
  });

  it("requires an https BACKEND_URL without credentials in staging and production", async () => {
    expect(backendConfigured("http://127.0.0.1:8000", "local")).toBe(true);
    expect(backendConfigured("http://127.0.0.1:8000")).toBe(true);
    expect(backendConfigured("https://abc.trycloudflare.com", "staging")).toBe(true);
    expect(backendConfigured("http://abc.trycloudflare.com", "staging")).toBe(false);
    expect(backendConfigured("http://api.example", "production")).toBe(false);
    expect(backendConfigured("https://user:pw@api.example", "local")).toBe(false);
    expect(backendConfigured("ftp://api.example", "local")).toBe(false);
    const calls = upstream(() => json("{}"));
    const res = await worker.fetch(new Request("https://icp.example/api/v1/models"),
      env({ ENVIRONMENT: "production", BACKEND_URL: "http://api.example" }), context().ctx);
    expect(res.status).toBe(503);
    expect(calls).toHaveLength(0);
  });
});

describe("write interval constant", () => {
  it("is five minutes (KV Free: 1,000 writes per day for the account)", () => {
    expect(EDGE_CACHE_WRITE_INTERVAL_MS).toBe(5 * MIN);
  });

  it("refreshes an unchanged copy once a day", () => {
    expect(EDGE_CACHE_REFRESH_MS).toBe(24 * 60 * MIN);
  });
});
