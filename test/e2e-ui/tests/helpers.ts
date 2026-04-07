import { Page, expect } from "@playwright/test";
import { buildExploreUrl, buildLogsDrilldownUrl } from "./url-state";

// Grafana datasource names matching grafana-datasources.yaml
export const PROXY_DS = "Loki (via VL proxy)";
export const PROXY_MULTI_DS = "Loki (via VL proxy multi-tenant)";
export const PROXY_TAIL_DS = "Loki (via VL proxy live tail)";
export const PROXY_TAIL_INGRESS_DS = "Loki (via ingress tail)";
export const PROXY_TAIL_NATIVE_DS = "Loki (via VL proxy live tail native)";
export const LOKI_DS = "Loki (direct)";

/**
 * Navigate to Grafana Explore with a specific datasource selected.
 */
export async function openExplore(page: Page, datasource: string) {
  const uid = await resolveDatasourceUid(page, datasource);
  await page.goto(buildExploreUrl(uid));
  await waitForGrafanaReady(page);
  await expect(page.getByRole("button", { name: /run query/i })).toBeVisible({
    timeout: 15_000,
  });
}

async function resolveDatasourceUid(page: Page, datasource: string): Promise<string> {
  const response = await page.request.get(
    `/api/datasources/name/${encodeURIComponent(datasource)}`
  );
  if (!response.ok()) {
    const listResponse = await page.request.get("/api/datasources");
    const available = listResponse.ok()
      ? (await listResponse.json()).map((ds: { name?: string }) => ds.name).filter(Boolean)
      : [];
    throw new Error(
      `failed to resolve datasource "${datasource}" (status=${response.status()}); available=${available.join(", ")}`
    );
  }
  const body = await response.json();
  return body.uid;
}

/**
 * Navigate to Grafana Logs Drilldown with a specific datasource selected.
 */
export async function openLogsDrilldown(page: Page, datasource: string) {
  const uid = await resolveDatasourceUid(page, datasource);
  await page.goto(buildLogsDrilldownUrl(uid));
  await waitForGrafanaReady(page);
  await expect(page.getByRole("combobox", { name: "Filter by labels" })).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByRole("tab", { name: "service" })).toBeVisible({
    timeout: 30_000,
  });
}

/**
 * Type a LogQL query into Grafana Explore's query editor.
 */
export async function typeQuery(page: Page, query: string) {
  // Grafana's Monaco editor for Loki queries
  const editor = page.locator('[data-testid="query-editor-rows"]').first();
  await expect(editor).toBeVisible({ timeout: 15_000 });
  await editor.click();

  // Clear existing query
  await page.keyboard.press("Meta+a");
  await page.keyboard.press("Backspace");

  // Type new query
  await page.keyboard.type(query, { delay: 10 });
}

/**
 * Click the Run Query button in Explore.
 */
export async function runQuery(page: Page) {
  const runBtn = page.getByRole("button", { name: /run query/i });
  await expect(runBtn).toBeVisible({ timeout: 15_000 });
  await runBtn.click();
  await waitForGrafanaReady(page);
  await page.waitForTimeout(1000);
}

/**
 * Check that no Grafana error alerts/toasts are visible.
 */
export async function assertNoErrors(page: Page) {
  // Check for Grafana error alert banners
  const errorAlerts = page.locator('[data-testid="data-testid Alert error"]');
  const errorCount = await errorAlerts.count();

  // Also check for error toasts
  const toasts = page.locator(".page-alert-list .alert-error, [class*='alertError']");
  const toastCount = await toasts.count();

  if (errorCount > 0 || toastCount > 0) {
    const errorText = errorCount > 0 ? await errorAlerts.first().textContent() : "";
    const toastText = toastCount > 0 ? await toasts.first().textContent() : "";
    throw new Error(
      `Grafana errors detected: alerts=${errorCount} toasts=${toastCount} ` +
        `alertText="${errorText}" toastText="${toastText}"`
    );
  }
}

/**
 * Check that log results are visible in the Explore panel.
 */
export async function assertLogsVisible(page: Page) {
  // Grafana shows logs in a table or list
  const logRows = page.locator(
    '[data-testid="logRows"], [class*="logs-row"], [class*="LogsTable"]'
  );
  await expect(logRows.first()).toBeVisible({ timeout: 10_000 });
}

/**
 * Check that metric/stats results are visible in the Explore panel.
 */
export async function assertGraphVisible(page: Page) {
  // Grafana renders graphs in uPlot or canvas
  const graph = page.locator(
    'canvas, [data-testid="graph-container"], [class*="panel-content"]'
  );
  await expect(graph.first()).toBeVisible({ timeout: 10_000 });
}

/**
 * Click on a label value in the log detail panel to drill down.
 */
export async function clickLogLabel(page: Page, labelName: string) {
  // Click on a log row first to expand details
  const logRow = page.locator('[data-testid="logRows"] tr, [class*="logs-row"]').first();
  if (await logRow.isVisible()) {
    await logRow.click();
    await page.waitForTimeout(500);
  }

  // Find and click the label
  const label = page.getByText(labelName, { exact: false }).first();
  if (await label.isVisible()) {
    await label.click();
  }
}

/**
 * Wait for Grafana to fully load (no spinners).
 */
export async function waitForGrafanaReady(page: Page) {
  await page.waitForLoadState("networkidle");
  // Wait for any loading spinners to disappear
  const spinner = page.locator('[class*="spinner"], [data-testid="Spinner"]');
  if (await spinner.isVisible({ timeout: 1000 }).catch(() => false)) {
    await spinner.waitFor({ state: "hidden", timeout: 15_000 });
  }
}

/**
 * Capture all network errors from Loki datasource requests.
 */
export function collectLokiErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on("response", (response) => {
    const url = response.url();
    if (url.includes("/loki/api/v1/") && response.status() >= 400) {
      errors.push(`${response.status()} ${response.url()}`);
    }
  });
  return errors;
}

function matchesAny(value: string, patterns: RegExp[]) {
  return patterns.some((pattern) => pattern.test(value));
}

function isRelevantGrafanaRequest(url: string) {
  return (
    url.includes("/loki/api/v1/") ||
    url.includes("/api/datasources/") ||
    url.includes("/api/ds/") ||
    url.includes("/api/live/ws") ||
    url.includes("/resources/")
  );
}

export type GrafanaGuardOptions = {
  allowedConsoleErrors?: RegExp[];
  allowedRequestFailures?: RegExp[];
  allowedResponseErrors?: RegExp[];
};

export function installGrafanaGuards(page: Page, options: GrafanaGuardOptions = {}) {
  const allowedConsoleErrors = options.allowedConsoleErrors ?? [];
  const allowedRequestFailures = options.allowedRequestFailures ?? [];
  const allowedResponseErrors = options.allowedResponseErrors ?? [];
  const consoleErrors: string[] = [];
  const requestFailures: string[] = [];
  const responseErrors: string[] = [];

  page.on("console", (message) => {
    if (message.type() !== "error") {
      return;
    }
    const text = message.text();
    if (!matchesAny(text, allowedConsoleErrors)) {
      consoleErrors.push(text);
    }
  });

  page.on("requestfailed", (request) => {
    if (!isRelevantGrafanaRequest(request.url())) {
      return;
    }
    const failure = `${request.failure()?.errorText ?? "request failed"} ${request.url()}`;
    if (!matchesAny(failure, allowedRequestFailures)) {
      requestFailures.push(failure);
    }
  });

  page.on("response", (response) => {
    if (response.status() < 400 || !isRelevantGrafanaRequest(response.url())) {
      return;
    }
    const summary = `${response.status()} ${response.url()}`;
    if (!matchesAny(summary, allowedResponseErrors)) {
      responseErrors.push(summary);
    }
  });

  return {
    async assertClean() {
      await assertNoErrors(page);
      expect(consoleErrors, "unexpected browser console errors").toEqual([]);
      expect(requestFailures, "unexpected request failures").toEqual([]);
      expect(responseErrors, "unexpected HTTP error responses").toEqual([]);
    },
  };
}
