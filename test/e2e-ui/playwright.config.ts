import { defineConfig } from "@playwright/test";

const explicitExecutablePath = process.env.PLAYWRIGHT_EXECUTABLE_PATH;

export default defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 1 : process.env.WORKERS ? parseInt(process.env.WORKERS) : 4,
  fullyParallel: !process.env.CI,
  use: {
    baseURL: process.env.GRAFANA_URL || "http://127.0.0.1:3002",
    viewport: {
      width: 1600,
      height: 1200,
    },
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
        launchOptions: {
          ...(explicitExecutablePath ? { executablePath: explicitExecutablePath } : {}),
          headless: !process.env.HEADED,
          args: process.env.HEADED
            ? ["--disable-focus-on-load", "--window-position=1200,0"]
            : [],
        },
      },
    },
  ],
});
