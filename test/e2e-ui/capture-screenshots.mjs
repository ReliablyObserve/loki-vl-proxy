import { chromium } from "@playwright/test";
import fs from "node:fs/promises";
import path from "node:path";

const baseUrl = process.env.GRAFANA_URL || "http://127.0.0.1:3002";
const proxyQueryUrl = process.env.PROXY_QUERY_URL || "http://127.0.0.1:3100";
const vlInsertUrl =
  process.env.VL_INSERT_URL ||
  "http://127.0.0.1:9428/insert/jsonline?_stream_fields=app,service_name,level,detected_level,env,cluster,namespace";
const explicitExecutablePath = process.env.PLAYWRIGHT_EXECUTABLE_PATH;
const outDir =
  process.env.SCREENSHOT_OUT_DIR ||
  path.resolve(process.cwd(), "../../docs/images/ui");
const screenshotFrom = process.env.SCREENSHOT_FROM || "now-5m";
const screenshotTo = process.env.SCREENSHOT_TO || "now";

const datasourceNames = {
  proxy: "Loki (via VL proxy)",
  multi: "Loki (via VL proxy multi-tenant)",
};

const drilldownPath = "/a/grafana-lokiexplore-app/explore";

function buildExploreUrl(datasourceUid, expr = "") {
  const paneState = {
    A: {
      datasource: datasourceUid,
      queries: [
        {
          refId: "A",
          expr,
          queryType: "range",
          datasource: {
            type: "loki",
            uid: datasourceUid,
          },
          editorMode: expr ? "code" : "builder",
          direction: "backward",
        },
      ],
      range: {
        from: screenshotFrom,
        to: screenshotTo,
      },
      compact: false,
    },
  };
  const params = new URLSearchParams({
    schemaVersion: "1",
    panes: JSON.stringify(paneState),
    orgId: "1",
  });
  return `/explore?${params.toString()}`;
}

function buildDrilldownUrl(datasourceUid) {
  const params = new URLSearchParams({
    patterns: "[]",
    from: screenshotFrom,
    to: screenshotTo,
    timezone: "browser",
    "var-lineFormat": "",
    "var-ds": datasourceUid,
    "var-filters": "",
    "var-fields": "",
    "var-levels": "",
    "var-metadata": "",
    "var-jsonFields": "",
    "var-all-fields": "",
    "var-patterns": "",
    "var-lineFilterV2": "",
    "var-lineFilters": "",
    "var-primary_label": "service_name|=~|.+",
  });
  return `${drilldownPath}?${params.toString()}`;
}

function buildServiceDrilldownUrl(datasourceUid, serviceName) {
  const params = new URLSearchParams({
    patterns: "[]",
    from: screenshotFrom,
    to: screenshotTo,
    timezone: "browser",
    "var-lineFormat": "",
    "var-ds": datasourceUid,
    "var-filters": `service_name|=|${serviceName}`,
    "var-fields": "",
    "var-levels": "",
    "var-metadata": "",
    "var-jsonFields": "",
    "var-all-fields": "",
    "var-patterns": "",
    "var-lineFilterV2": "",
    "var-lineFilters": "",
    displayedFields: "[]",
    urlColumns: "[]",
  });
  return `${drilldownPath}/service/${encodeURIComponent(serviceName)}/logs?${params.toString()}`;
}

async function resolveDatasourceUid(request, datasourceName) {
  const response = await request.get(
    `${baseUrl}/api/datasources/name/${encodeURIComponent(datasourceName)}`
  );
  if (!response.ok()) {
    throw new Error(
      `failed to resolve datasource "${datasourceName}" (status=${response.status()})`
    );
  }
  const body = await response.json();
  if (!body.uid) {
    throw new Error(`datasource "${datasourceName}" has no uid`);
  }
  return body.uid;
}

async function waitForGrafanaReady(page) {
  await page.waitForLoadState("networkidle");
  await page.waitForTimeout(1000);
}

async function waitForLogsTable(page) {
  await page
    .locator('[data-testid="logRows"], [class*="logs-row"], [class*="LogsTable"]')
    .first()
    .waitFor({ state: "visible", timeout: 30000 });
}

async function runExploreQuery(page) {
  const runButton = page.getByRole("button", { name: /run query/i }).first();
  if (await runButton.isVisible().catch(() => false)) {
    await runButton.click();
  } else {
    const overflow = page.getByRole("button", { name: /show more items/i });
    await overflow.click();
    await page.getByRole("menuitem", { name: /run query/i }).click();
  }
}

async function ingestFreshLogs(seedApp, serviceName) {
  const now = Date.now();
  const levels = ["info", "warn", "error"];
  const lines = [];
  for (let i = 0; i < 36; i += 1) {
    const ts = new Date(now - i * 4000).toISOString();
    const lvl = levels[i % levels.length];
    lines.push(
      JSON.stringify({
        _time: ts,
        _msg: `screenshot sample log line ${i + 1}`,
        app: seedApp,
        service_name: serviceName,
        level: lvl,
        detected_level: lvl,
        env: "test",
        cluster: "local",
        namespace: "demo",
        traceID: `trace-${Math.floor(i / 3)}`,
      })
    );
  }
  const response = await fetch(vlInsertUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/stream+json",
    },
    body: `${lines.join("\n")}\n`,
  });
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`failed to ingest screenshot logs (status=${response.status}) ${body}`);
  }
}

function startBackgroundIngest(seedApp, serviceName, intervalMs = 1500) {
  let active = true;
  const timer = setInterval(async () => {
    if (!active) {
      return;
    }
    const now = new Date().toISOString();
    const level = Math.random() < 0.7 ? "info" : "error";
    const payload = JSON.stringify({
      _time: now,
      _msg: `screenshot streaming line ${now}`,
      app: seedApp,
      service_name: serviceName,
      level,
      detected_level: level,
      env: "test",
      cluster: "local",
      namespace: "demo",
      traceID: `trace-live-${Date.now()}`,
    });
    try {
      await fetch(vlInsertUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/stream+json",
        },
        body: `${payload}\n`,
      });
    } catch {
      // keep background producer best-effort
    }
  }, intervalMs);

  return () => {
    active = false;
    clearInterval(timer);
  };
}

async function waitForSeedVisible(seedApp, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const response = await fetch(
      `${proxyQueryUrl}/loki/api/v1/query_range?query=${encodeURIComponent(
        `{app="${seedApp}"}`
      )}&limit=20&start=${encodeURIComponent("now-5m")}&end=${encodeURIComponent("now")}`
    );
    if (response.ok) {
      const body = await response.json();
      const streams = body?.data?.result;
      if (Array.isArray(streams) && streams.length > 0) {
        return;
      }
    }
    await new Promise((resolve) => {
      setTimeout(resolve, 1000);
    });
  }
  throw new Error("seeded logs not visible via proxy query in time");
}

async function main() {
  await fs.mkdir(outDir, { recursive: true });

  const browser = await chromium.launch({
    headless: true,
    ...(explicitExecutablePath ? { executablePath: explicitExecutablePath } : {}),
  });
  const context = await browser.newContext({
    viewport: { width: 1600, height: 1200 },
  });
  const page = await context.newPage();
  const seedApp = `screenshot-proxy-${Date.now()}`;
  const serviceName = "screenshot-service";

  await ingestFreshLogs(seedApp, serviceName);
  const stopBackgroundIngest = startBackgroundIngest(seedApp, serviceName);
  await waitForSeedVisible(seedApp);

  const proxyUid = await resolveDatasourceUid(page.request, datasourceNames.proxy);
  await resolveDatasourceUid(page.request, datasourceNames.multi);

  await page.goto(`${baseUrl}${buildExploreUrl(proxyUid, `{app="${seedApp}"}`)}`);
  await waitForGrafanaReady(page);
  await runExploreQuery(page);
  await waitForGrafanaReady(page);
  await waitForLogsTable(page);
  await page.screenshot({
    path: path.join(outDir, "explore-main.png"),
    fullPage: true,
  });

  const firstRow = page
    .locator('[data-testid="logRows"] tr, [class*="logs-row"]')
    .first();
  if (await firstRow.isVisible().catch(() => false)) {
    await firstRow.click();
    await waitForGrafanaReady(page);
  }
  await page.screenshot({
    path: path.join(outDir, "explore-details.png"),
    fullPage: true,
  });

  await page.goto(`${baseUrl}${buildServiceDrilldownUrl(proxyUid, serviceName)}`);
  await waitForGrafanaReady(page);
  await page
    .getByRole("combobox", { name: "Filter by labels" })
    .waitFor({ state: "visible", timeout: 30000 });
  const noLogs = page.getByText("No logs found");
  const noLogsVisible = await noLogs
    .first()
    .isVisible({ timeout: 1000 })
    .catch(() => false);
  if (noLogsVisible) {
    throw new Error("drilldown screenshot capture found empty logs");
  }
  await page.screenshot({
    path: path.join(outDir, "drilldown-main.png"),
    fullPage: true,
  });

  await page.goto(`${baseUrl}${buildExploreUrl(proxyUid, `{app="${seedApp}"}`)}`);
  await waitForGrafanaReady(page);
  await clickLiveStream(page);
  await waitForGrafanaReady(page);
  await page
    .getByRole("button", { name: /pause the live stream|stop and exit the live stream/i })
    .first()
    .waitFor({ state: "visible", timeout: 20000 });
  await page.screenshot({
    path: path.join(outDir, "explore-tail-multitenant.png"),
    fullPage: true,
  });

  stopBackgroundIngest();
  await browser.close();
  // eslint-disable-next-line no-console
  console.log(`Saved screenshots to ${outDir}`);
}

async function clickLiveStream(page) {
  const liveButton = page.getByRole("button", { name: /live/i }).first();
  if (await liveButton.isVisible().catch(() => false)) {
    await liveButton.click();
    return;
  }
  const overflow = page.getByRole("button", { name: /show more items/i });
  await overflow.click();
  await page.getByRole("menuitem", { name: /live/i }).first().click();
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
