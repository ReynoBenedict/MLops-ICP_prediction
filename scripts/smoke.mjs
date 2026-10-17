// End-to-end smoke test against a running stack (read-only except for one forecast run).
//   npm run smoke                                   -> http://127.0.0.1:8787 (npm run dev:edge)
//   npm run smoke -- http://localhost:5173          -> Vite dev server
//   npm run smoke -- https://<your-gateway-host>    -> a deployed gateway (after deployment)
const base = (process.argv[2] ?? "http://127.0.0.1:8787").replace(/\/$/, "");
let failures = 0;

async function check(name, fn) {
  try {
    const detail = await fn();
    console.log(`PASS  ${name}${detail ? ` - ${detail}` : ""}`);
  } catch (e) {
    failures++;
    console.log(`FAIL  ${name} - ${e instanceof Error ? e.message : e}`);
  }
}

async function getJson(path, init) {
  const res = await fetch(`${base}${path}`, { ...init, signal: AbortSignal.timeout(30_000) });
  const body = await res.json().catch(() => null);
  return { res, body };
}

await check("web app shell", async () => {
  const res = await fetch(`${base}/`, { signal: AbortSignal.timeout(15_000) });
  const html = await res.text();
  if (!res.ok || !html.includes('id="root"')) {
    // A bare backend (no gateway / static assets) serves no shell; report it instead of failing the API checks.
    const live = await fetch(`${base}/health/live`, { signal: AbortSignal.timeout(10_000) }).catch(() => null);
    if (live?.ok) return "not served here (backend only; run against the gateway to check the web shell)";
    throw new Error(`HTTP ${res.status}`);
  }
  return res.headers.get("content-security-policy") ? "CSP present" : "no CSP header (expected only on Workers assets)";
});
await check("system status", async () => {
  const { res, body } = await getJson("/api/v1/system/status");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const bad = body.checks.filter((c) => c.critical && c.status !== "ok").map((c) => c.name);
  if (bad.length) throw new Error(`critical dependencies down: ${bad.join(", ")}`);
  return `${body.status}; request id ${res.headers.get("x-request-id")}`;
});
await check("data summary", async () => {
  const { res, body } = await getJson("/api/v1/data/summary");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const icp = body.datasets?.icp;
  if (!icp?.available) throw new Error("ICP dataset not available");
  return `ICP ${icp.period_start}..${icp.period_end}, freshness ${icp.freshness.status}; ` +
    `benchmarks: ${["wti", "brent", "dubai", "usd_idr"].filter((d) => body.datasets[d]?.available).join(", ") || "none"}`;
});
await check("production model", async () => {
  const { res, body } = await getJson("/api/v1/models");
  if (!res.ok || !body.production_version) throw new Error("no production model");
  return `v${body.production_version}`;
});
await check("forecast + export", async () => {
  const { res, body } = await getJson("/api/v1/forecasts", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ horizon: 2 }),
  });
  if (res.status !== 201) throw new Error(`HTTP ${res.status} ${body?.error?.code ?? ""}`);
  const csv = await fetch(`${base}/api/v1/forecasts/${body.id}/export.csv`, { signal: AbortSignal.timeout(15_000) });
  if (!csv.ok) throw new Error(`export HTTP ${csv.status}`);
  return body.result.points.map((p) => `${p.period}=${p.prediction}`).join(", ");
});
await check("validation errors use the envelope", async () => {
  const { res, body } = await getJson("/api/v1/forecasts", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ horizon: 99 }),
  });
  if (res.status !== 422 || body?.error?.code !== "VALIDATION_ERROR") throw new Error(`HTTP ${res.status}`);
});
await check("public market snapshot", async () => {
  const { res, body } = await getJson("/api/v1/public/market-snapshot");
  if (!res.ok || !body.icp_period) throw new Error(`HTTP ${res.status}`);
  return `ICP ${body.icp_period} = ${body.icp_usd} USD/bbl (${body.freshness.status})`;
});
await check("insights (anonymous teaser)", async () => {
  const { res, body } = await getJson("/api/v1/insights");
  if (!res.ok || !Array.isArray(body.insights)) throw new Error(`HTTP ${res.status}`);
  return `${body.insights.length} insight(s)`;
});
await check("published system forecast", async () => {
  const { res, body } = await getJson("/api/v1/forecasts/latest");
  if (res.status === 404) return "none published yet (run the refresh_forecast job)";
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return `${body.forecast_origin} -> ${body.result.points.length} month(s), issued ${body.issue_date}`;
});
await check("plans and sign-in guard", async () => {
  const { res, body } = await getJson("/api/v1/plans");
  if (!res.ok || !body.length) throw new Error(`HTTP ${res.status}`);
  const login = await getJson("/api/v1/auth/login", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ email: "nobody@example.invalid", password: "wrong-password-xx" }),
  });
  if (login.res.status !== 401) throw new Error(`login expected 401, got ${login.res.status}`);
  return `${body.length} plan(s)`;
});
await check("admin endpoints require auth", async () => {
  const { res } = await getJson("/api/v1/jobs");
  if (res.status !== 401) throw new Error(`expected 401, got ${res.status}`);
});

console.log(failures ? `\n${failures} check(s) failed against ${base}` : `\nAll smoke checks passed against ${base}`);
process.exit(failures ? 1 : 0);
