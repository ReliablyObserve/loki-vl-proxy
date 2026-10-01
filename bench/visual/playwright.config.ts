import { defineConfig } from "@playwright/test";

// Visual-proof capture. Pages x ranges run in parallel; the three datasources
// of one page/range run in sequence inside a test so they see the same state.
export default defineConfig({
  testDir: ".",
  testMatch: "capture.spec.ts",
  timeout: 600_000,
  workers: process.env.WORKERS ? parseInt(process.env.WORKERS) : 2,
  fullyParallel: true,
  reporter: [["list"]],
  use: {
    baseURL: process.env.GRAFANA_URL || "http://127.0.0.1:33002",
    viewport: { width: 1500, height: 1100 },
    timezoneId: "UTC",
    locale: "en-US",
  },
  projects: [{
    name: "chromium",
    use: {
      browserName: "chromium",
      // CI uses the runner's Chrome, as the e2e-ui specs do.
      launchOptions: process.env.PLAYWRIGHT_EXECUTABLE_PATH ? { executablePath: process.env.PLAYWRIGHT_EXECUTABLE_PATH } : {},
    },
  }],
});
