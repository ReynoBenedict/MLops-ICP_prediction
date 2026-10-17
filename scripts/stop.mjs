// Stop processes started by `npm run dev` / `npm run dev:edge` (normally Ctrl+C is enough; use this
// if the orchestrator was killed and left the API or wrangler running).
import { existsSync, readFileSync, rmSync } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { IS_WINDOWS, ROOT } from "./lib.mjs";

const PID_FILE = path.join(ROOT, "var", "dev.pids");
if (!existsSync(PID_FILE)) {
  console.log("No recorded dev processes (var/dev.pids not found).");
  process.exit(0);
}
for (const pid of readFileSync(PID_FILE, "utf8").split(/\s+/).filter(Boolean)) {
  if (IS_WINDOWS) {
    const r = spawnSync("taskkill", ["/pid", pid, "/T", "/F"], { stdio: "ignore" });
    console.log(r.status === 0 ? `stopped process tree ${pid}` : `process ${pid} not running`);
  } else {
    try {
      process.kill(-Number(pid), "SIGTERM");
    } catch {
      try { process.kill(Number(pid), "SIGTERM"); console.log(`stopped ${pid}`); } catch { console.log(`process ${pid} not running`); }
    }
  }
}
rmSync(PID_FILE, { force: true });
