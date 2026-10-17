// Start the local stack with one command; Ctrl+C stops every process.
//
//   npm run dev        FastAPI backend (http://127.0.0.1:8000, auto-reload) + Vite dev server (http://localhost:5173)
//   npm run dev:edge   FastAPI backend + production web build served by the Cloudflare Worker in
//                      `wrangler dev` (http://localhost:8787) - exercises the real gateway code path.
import { spawnSync } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { IS_WINDOWS, ROOT, npmCmd, requirePython, run, start } from "./lib.mjs";

const PID_FILE = path.join(ROOT, "var", "dev.pids");

const edge = process.argv.includes("--edge");
const py = requirePython();
const children = [];

const stopAll = (code = 0) => {
  for (const c of children) {
    if (c.exitCode === null) {
      if (IS_WINDOWS) {
        // Kill the whole process tree (npm/wrangler spawn grandchildren on Windows).
        spawnSync("taskkill", ["/pid", String(c.pid), "/T", "/F"], { stdio: "ignore" });
      } else {
        c.kill("SIGTERM");
      }
    }
  }
  rmSync(PID_FILE, { force: true });
  process.exit(code);
};
process.on("SIGINT", () => stopAll(0));
process.on("SIGTERM", () => stopAll(0));

const watch = (name, child) => {
  children.push(child);
  // Recorded so `npm run stop` can clean up if this orchestrator is killed without Ctrl+C.
  mkdirSync(path.dirname(PID_FILE), { recursive: true });
  writeFileSync(PID_FILE, children.map((c) => c.pid).join(" "));
  child.on("exit", (code) => {
    console.error(`[${name}] exited with code ${code}; stopping the stack.`);
    stopAll(code ?? 1);
  });
};

watch("api", start("api", py, ["-m", "icp_api.cli", "serve", "--reload"]));
if (edge) {
  run(npmCmd(), ["run", "build", "-w", "@icp/web"], { shell: IS_WINDOWS });
  watch("edge", start("edge", npmCmd(), ["run", "dev", "-w", "@icp/gateway"]));
  console.log("Gateway (Worker) at http://localhost:8787 - backend at http://127.0.0.1:8000. Ctrl+C to stop.");
} else {
  watch("web", start("web", npmCmd(), ["run", "dev", "-w", "@icp/web"]));
  console.log("Web at http://localhost:5173 - backend at http://127.0.0.1:8000 (docs: /docs). Ctrl+C to stop.");
}
