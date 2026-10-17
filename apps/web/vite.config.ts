import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// In development the Vite server proxies /api to the FastAPI backend, so the browser talks to one
// origin exactly as it does in production (where the Cloudflare Worker plays this role).
const env = (globalThis as { process?: { env: Record<string, string | undefined> } }).process?.env ?? {};
const backend = env.VITE_DEV_BACKEND ?? "http://127.0.0.1:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: { "/api": { target: backend, changeOrigin: false } },
  },
  build: { outDir: "dist", sourcemap: false, chunkSizeWarningLimit: 900 },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
