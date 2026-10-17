#!/usr/bin/env node
/**
 * Zero-cost staging backend on this machine: run the backend image in the staging configuration,
 * open a Cloudflare Quick Tunnel to it, and deploy the staging Worker pointing at the tunnel.
 *
 *   node scripts/staging-backend.mjs               # start (or reuse) the container, open a tunnel, deploy
 *   node scripts/staging-backend.mjs --no-deploy   # only print the tunnel URL
 *   node scripts/staging-backend.mjs --deploy-only # redeploy the Worker (e.g. after a web build)
 *                                                  # with the last saved tunnel URL; no container/tunnel
 *
 * Keep this process running: the tunnel (and therefore the public API) stops when it exits or the
 * machine sleeps. Quick Tunnels get a new random hostname every time. The hostname is machine-local
 * state: it is written to var/staging-backend-url.txt (gitignored) and passed to Wrangler with
 * `--var BACKEND_URL:<url>`; apps/gateway/wrangler.jsonc keeps a placeholder and is never edited.
 * Secrets come from var/secrets/staging.render.env (gitignored); nothing is printed.
 * See docs/DEPLOYMENT.md, "Self-hosted backend through a Quick Tunnel".
 */
import { spawn, spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(new URL("..", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1"));
const image = process.env.ICP_IMAGE ?? "icp-api:staging-20261017";
const container = "icp-api-staging";
const port = Number(process.env.ICP_BACKEND_PORT ?? 18000);
const envFile = resolve(root, "var/secrets/staging.render.env");
const urlFile = resolve(root, "var/staging-backend-url.txt");
const deploy = !process.argv.includes("--no-deploy");
const npx = process.platform === "win32" ? "npx.cmd" : "npx";

function deployWorker(publicUrl) {
  const dep = spawnSync(npx, ["wrangler", "deploy", "--env", "staging", "--var", `BACKEND_URL:${publicUrl}`],
    { cwd: resolve(root, "apps/gateway"), stdio: "inherit", shell: process.platform === "win32" });
  return dep.status === 0;
}

if (process.argv.includes("--deploy-only")) {
  const saved = existsSync(urlFile) ? readFileSync(urlFile, "utf8").trim() : "";
  if (!saved) {
    console.error("no saved tunnel URL in var/staging-backend-url.txt; run without --deploy-only first");
    process.exit(2);
  }
  process.exit(deployWorker(saved) ? 0 : 1);
}

if (!existsSync(envFile)) {
  console.error(`missing ${envFile} (staging variables; see docs/DEPLOYMENT.md)`);
  process.exit(2);
}

const running = spawnSync("docker", ["ps", "--filter", `name=^${container}$`, "--format", "{{.Names}}"], { encoding: "utf8" });
if (running.stdout.trim() !== container) {
  spawnSync("docker", ["rm", "-f", container], { stdio: "ignore" });
  const run = spawnSync("docker", ["run", "-d", "--name", container, "--restart", "unless-stopped", "--env-file", envFile,
    "-p", `127.0.0.1:${port}:10000`, image], { stdio: "inherit" });
  if (run.status !== 0) process.exit(run.status ?? 1);
}
for (let i = 0; i < 30; i++) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/health/live`);
    if (res.ok) break;
  } catch { /* not up yet */ }
  await new Promise((r) => setTimeout(r, 1000));
}
console.log(`backend container '${container}' listening on 127.0.0.1:${port}`);

const tunnel = spawn(npx, ["wrangler", "tunnel", "quick-start", `http://localhost:${port}`], {
  cwd: resolve(root, "apps/gateway"), stdio: ["ignore", "pipe", "pipe"], shell: process.platform === "win32",
});
let url = "";
const onData = (chunk) => {
  const text = chunk.toString();
  process.stdout.write(text);
  const m = text.match(/https:\/\/[a-z0-9-]+\.trycloudflare\.com/);
  if (m && !url) {
    url = m[0];
    void connect(url);
  }
};
tunnel.stdout.on("data", onData);
tunnel.stderr.on("data", onData);
tunnel.on("exit", (code) => { console.log(`tunnel exited (${code}); the public API is offline until restarted`); process.exit(code ?? 0); });

async function connect(publicUrl) {
  for (let i = 0; i < 20; i++) {
    try {
      const res = await fetch(`${publicUrl}/health/live`);
      if (res.ok) break;
    } catch { /* DNS/TLS not ready */ }
    await new Promise((r) => setTimeout(r, 3000));
  }
  console.log(`\nbackend reachable through ${publicUrl}`);
  writeFileSync(urlFile, publicUrl);
  if (!deploy) return;
  const ok = deployWorker(publicUrl);
  console.log(ok ? "\nstaging Worker now forwards /api/v1/* to this machine. Leave this window open." : "\nworker deploy failed; see output above");
}
