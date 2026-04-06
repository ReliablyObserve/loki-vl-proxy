import { Page, expect } from "@playwright/test";

// Grafana datasource names matching grafana-datasources.yaml
export const PROXY_DS = "Loki (via VL proxy)";
export const PROXY_MULTI_DS = "Loki (via VL proxy multi-tenant)";
export const PROXY_TAIL_DS = "Loki (via VL proxy live tail)";
export const LOKI_DS = "Loki (direct)";

// Grafana URLs
export const EXPLORE_URL = "/explore";
export const DRILLDOWN_URL = "/a/grafana-lokiexplore-app/explore";

/**
 * Navigate to Grafana Explore with a specific datasource selected.
 */
export async function openExplore(page: Page, datasource: string) {
  // Grafana Explore URL format with datasource
  await page.goto(EXPLORE_URL);
  await page.waitForLoadState("networkidle");

  // Select datasource from picker if not already selected
  const dsSelector = page.getByTestId("data-source-picker");
  if (await dsSelector.isVisible()) {
    await dsSelector.click();
    await page.getByText(datasource, { exact: false }).first().click();
    await page.waitForTimeout(500);
  }
}

async function resolveDatasourceUid(page: Page, datasource: string): Promise<string> {
  const response = await page.request.get(
    `/api/datasources/name/${encodeURIComponent(datasource)}`
  );
  expect(response.ok()).toBeTruthy();
  const body = await response.json();
  return body.uid;
}

/**
 * Navigate to Grafana Logs Drilldown with a specific datasource selected.
 */
export async function openLogsDrilldown(page: Page, datasource: string) {
  const uid = await resolveDatasourceUid(page, datasource);
  const params = new URLSearchParams({
    patterns: "[]",
    from: "now-2h",
    to: "now",
    timezone: "browser",
    "var-lineFormat": "",
    "var-ds": uid,
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

  await page.goto(`${DRILLDOWN_URL}?${params.toString()}`);
  await page.waitForLoadState("networkidle");
}

/**
 * Type a LogQL query into Grafana Explore's query editor.
 */
export async function typeQuery(page: Page, query: string) {
  // Grafana's Monaco editor for Loki queries
  const editor = page.locator('[data-testid="query-editor-rows"]').first();
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
  await runBtn.click();
  // Wait for results to load
  await page.waitForTimeout(2000);
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
