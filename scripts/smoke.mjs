// End-to-end smoke test against a running stack (read-only except for one forecast run when signed in).
//   Optional: ICP_SMOKE_TOKEN or ICP_SMOKE_EMAIL + ICP_SMOKE_PASSWORD for the signed-in checks.
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
// Access policy v4: the workspace API requires sign-in. Authenticated checks use a session from
// ICP_SMOKE_TOKEN, or ICP_SMOKE_EMAIL + ICP_SMOKE_PASSWORD (signed in, then signed out at the end);
// without them those checks are skipped. Credentials are never printed.
let session = process.env.ICP_SMOKE_TOKEN || null;
let ownSession = false;
if (!session && process.env.ICP_SMOKE_EMAIL && process.env.ICP_SMOKE_PASSWORD) {
  const { res, body } = await getJson("/api/v1/auth/login", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ email: process.env.ICP_SMOKE_EMAIL, password: process.env.ICP_SMOKE_PASSWORD }),
  });
  if (res.ok && body?.token) { session = body.token; ownSession = true; }
  else { failures++; console.log(`FAIL  sign in with ICP_SMOKE_EMAIL - HTTP ${res.status} ${body?.error?.code ?? ""}`); }
}
const auth = session ? { authorization: `Bearer ${session}` } : null;
async function member(name, fn) {
  if (!auth) { console.log(`SKIP  ${name} - set ICP_SMOKE_TOKEN or ICP_SMOKE_EMAIL/ICP_SMOKE_PASSWORD`); return; }
  await check(name, fn);
}

await check("guest: workspace API requires sign-in (401)", async () => {
  const paths = ["/api/v1/system/status", "/api/v1/data/summary", "/api/v1/data/history", "/api/v1/models",
                 "/api/v1/forecasts", "/api/v1/forecasts/latest", "/api/v1/insights", "/api/v1/datasets/active"];
  const bad = [];
  for (const p of paths) {
    const { res, body } = await getJson(p);
    if (res.status !== 401 || body?.error?.code !== "AUTH_REQUIRED" || !res.headers.get("www-authenticate")) bad.push(`${p}=${res.status}`);
  }
  const post = await getJson("/api/v1/forecasts", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ horizon: 1 }),
  });
  if (post.res.status !== 401) bad.push(`POST /api/v1/forecasts=${post.res.status}`);
  if (bad.length) throw new Error(`expected 401: ${bad.join(", ")}`);
  return `${paths.length + 1} endpoint(s)`;
});
await check("public market snapshot (no forecast values)", async () => {
  const { res, body } = await getJson("/api/v1/public/market-snapshot");
  if (!res.ok || !body.icp_period) throw new Error(`HTTP ${res.status}`);
  const fc = body.latest_forecast;
  if (fc && ("points" in fc || JSON.stringify(fc).includes("prediction"))) throw new Error("snapshot exposes forecast values");
  return `ICP ${body.icp_period} = ${body.icp_usd} USD/bbl (${body.freshness.status}); forecast teaser ${fc ? fc.issue_date : "none"}`;
});
await member("data summary", async () => {
  const { res, body } = await getJson("/api/v1/data/summary", { headers: auth });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const icp = body.datasets?.icp;
  if (!icp?.available) throw new Error("ICP dataset not available");
  return `ICP ${icp.period_start}..${icp.period_end}, freshness ${icp.freshness.status}; ` +
    `benchmarks: ${["wti", "brent", "dubai", "usd_idr"].filter((d) => body.datasets[d]?.available).join(", ") || "none"}`;
});
await member("production model", async () => {
  const { res, body } = await getJson("/api/v1/models", { headers: auth });
  if (!res.ok || !body.production_version) throw new Error("no production model");
  return `v${body.production_version}`;
});
await member("forecast + private export", async () => {
  const { res, body } = await getJson("/api/v1/forecasts", {
    method: "POST", headers: { "content-type": "application/json", ...auth }, body: JSON.stringify({ horizon: 1 }),
  });
  if (res.status !== 201) throw new Error(`HTTP ${res.status} ${body?.error?.code ?? ""}`);
  const csv = await fetch(`${base}/api/v1/forecasts/${body.id}/export.csv`, { headers: auth, signal: AbortSignal.timeout(15_000) });
  if (!csv.ok) throw new Error(`export HTTP ${csv.status}`);
  if (!(csv.headers.get("cache-control") ?? "").includes("no-store")) throw new Error("export is cacheable");
  const anon = await fetch(`${base}/api/v1/forecasts/${body.id}/export.csv`, { signal: AbortSignal.timeout(15_000) });
  if (anon.status !== 401) throw new Error(`guest export expected 401, got ${anon.status}`);
  return body.result.points.map((p) => `${p.period}=${p.prediction}`).join(", ");
});
await member("validation errors use the envelope", async () => {
  const { res, body } = await getJson("/api/v1/forecasts", {
    method: "POST", headers: { "content-type": "application/json", ...auth }, body: JSON.stringify({ horizon: 99 }),
  });
  if (res.status !== 422 || body?.error?.code !== "VALIDATION_ERROR") throw new Error(`HTTP ${res.status}`);
});
await member("insights", async () => {
  const { res, body } = await getJson("/api/v1/insights", { headers: auth });
  if (!res.ok || !Array.isArray(body.insights)) throw new Error(`HTTP ${res.status}`);
  return `${body.insights.length} insight(s)`;
});
await member("published system forecast", async () => {
  const { res, body } = await getJson("/api/v1/forecasts/latest", { headers: auth });
  if (res.status === 404) return "none published yet (run the refresh_forecast job)";
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return `${body.forecast_origin} -> ${body.result.points.length} month(s), issued ${body.issue_date}`;
});
await member("operator endpoints refuse members (403)", async () => {
  const { res, body } = await getJson("/api/v1/system/status", { headers: auth });
  if (res.status === 200 && body?.checks) return "token is an operator token (system status readable)";
  if (res.status !== 403 || body?.error?.code !== "FORBIDDEN") throw new Error(`expected 403 FORBIDDEN, got ${res.status}`);
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

if (ownSession) {
  await fetch(`${base}/api/v1/auth/logout`, { method: "POST", headers: auth, signal: AbortSignal.timeout(10_000) }).catch(() => null);
}
console.log(failures ? `\n${failures} check(s) failed against ${base}` : `\nAll smoke checks passed against ${base}`);
process.exit(failures ? 1 : 0);
