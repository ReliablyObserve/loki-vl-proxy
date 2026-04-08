import { Page, Locator, expect } from "@playwright/test";
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
export async function openExplore(page: Page, datasource: string, expr = "") {
  const uid = await resolveDatasourceUid(page, datasource);
  await page.goto(buildExploreUrl(uid, expr));
  await waitForGrafanaReady(page);
  await expect(exploreQueryEditor(page)).toBeVisible({ timeout: 15_000 });
}

export async function resolveDatasourceUid(page: Page, datasource: string): Promise<string> {
  const deadline = Date.now() + 30_000;
  let lastStatus = "unreachable";

  while (Date.now() < deadline) {
    try {
      const response = await page.request.get(
        `/api/datasources/name/${encodeURIComponent(datasource)}`
      );
      lastStatus = String(response.status());
      if (response.ok()) {
        const body = await response.json();
        if (body.uid) {
          return body.uid;
        }
      }
    } catch {
      lastStatus = "unreachable";
    }

    await page.waitForTimeout(1_000);
  }

  let available: string[] = [];
  try {
    const listResponse = await page.request.get("/api/datasources");
    if (listResponse.ok()) {
      available = (await listResponse.json())
        .map((ds: { name?: string }) => ds.name)
        .filter(Boolean);
    }
  } catch {
    available = [];
  }

  throw new Error(
    `failed to resolve datasource "${datasource}" (lastStatus=${lastStatus}); available=${available.join(", ")}`
  );
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
}

/**
 * Type a LogQL query into Grafana Explore's query editor.
 */
export async function typeQuery(page: Page, query: string) {
  // Grafana's Monaco editor for Loki queries
  const editor = exploreQueryEditor(page);
  await expect(editor).toBeVisible({ timeout: 15_000 });
  await editor.click();

  // Clear existing query
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.press("Backspace");

  // Type new query
  await page.keyboard.type(query, { delay: 10 });
}

/**
 * Click the Run Query button in Explore.
 */
export async function runQuery(page: Page) {
  await clickExploreToolbarAction(page, /run query/i);
  await waitForGrafanaReady(page);
  await page.waitForTimeout(1000);
}

export async function clickLiveStream(page: Page) {
  await clickExploreToolbarAction(page, /live/i);
}

/**
 * Check that no Grafana error alerts/toasts are visible.
 */
export async function assertNoErrors(page: Page, allowedAlertErrors: RegExp[] = []) {
  const errorAlerts = page.locator('[data-testid="data-testid Alert error"]');
  const toasts = page.locator(".page-alert-list .alert-error, [class*='alertError']");
  const alertTexts = await unexpectedVisibleTexts(errorAlerts, allowedAlertErrors);
  const toastTexts = await unexpectedVisibleTexts(toasts, allowedAlertErrors);

  if (alertTexts.length > 0 || toastTexts.length > 0) {
    throw new Error(
      `Grafana errors detected: alerts=${alertTexts.length} toasts=${toastTexts.length} ` +
        `alertTexts=${JSON.stringify(alertTexts)} toastTexts=${JSON.stringify(toastTexts)}`
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

function exploreQueryEditor(page: Page): Locator {
  return page
    .locator('[data-testid="query-editor-rows"], [data-testid="query-editor-row"]')
    .first();
}

async function clickExploreToolbarAction(page: Page, name: RegExp) {
  const directButton = page.getByRole("button", { name }).first();
  if (await directButton.isVisible({ timeout: 2_000 }).catch(() => false)) {
    await directButton.click();
    return;
  }

  const overflowButton = page.getByRole("button", { name: /show more items/i });
  if (!(await overflowButton.isVisible({ timeout: 2_000 }).catch(() => false))) {
    throw new Error(`missing Explore toolbar action matching ${name}`);
  }

  await overflowButton.click();

  const menuAction = page.getByRole("menuitem", { name }).first();
  if (await menuAction.isVisible({ timeout: 5_000 }).catch(() => false)) {
    await menuAction.click();
    return;
  }

  const menuButton = page.getByRole("button", { name }).first();
  if (await menuButton.isVisible({ timeout: 2_000 }).catch(() => false)) {
    await menuButton.click();
    return;
  }

  throw new Error(`Explore overflow menu missing action matching ${name}`);
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
  allowedAlertErrors?: RegExp[];
  allowedConsoleErrors?: RegExp[];
  allowedRequestFailures?: RegExp[];
  allowedResponseErrors?: RegExp[];
};

export function installGrafanaGuards(page: Page, options: GrafanaGuardOptions = {}) {
  const allowedAlertErrors = options.allowedAlertErrors ?? [];
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
      await assertNoErrors(page, allowedAlertErrors);
      expect(consoleErrors, "unexpected browser console errors").toEqual([]);
      expect(requestFailures, "unexpected request failures").toEqual([]);
      expect(responseErrors, "unexpected HTTP error responses").toEqual([]);
    },
  };
}

async function unexpectedVisibleTexts(locator: Locator, allowedPatterns: RegExp[]) {
  const count = await locator.count();
  const unexpected: string[] = [];

  for (let index = 0; index < count; index++) {
    const item = locator.nth(index);
    if (!(await item.isVisible().catch(() => false))) {
      continue;
    }
    const text = (await item.textContent())?.trim() ?? "";
    if (!matchesAny(text, allowedPatterns)) {
      unexpected.push(text);
    }
  }

  return unexpected;
}
