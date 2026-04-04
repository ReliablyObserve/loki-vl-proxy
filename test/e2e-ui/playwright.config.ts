import { defineConfig } from "@playwright/test";

const explicitExecutablePath = process.env.PLAYWRIGHT_EXECUTABLE_PATH;

export default defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  retries: 1,
  use: {
    baseURL: process.env.GRAFANA_URL || "http://localhost:3002",
    screenshot: "only-on-failure",
    trace: "on-first-retry",
    // Grafana anonymous auth — no login needed
    extraHTTPHeaders: {},
  },
  reporter: [["html", { open: "never" }], ["list"]],
  projects: [
    {
      name: "chromium",
      use: {
        browserName: "chromium",
        ...(explicitExecutablePath
          ? { launchOptions: { executablePath: explicitExecutablePath } }
          : {}),
      },
    },
  ],
});
