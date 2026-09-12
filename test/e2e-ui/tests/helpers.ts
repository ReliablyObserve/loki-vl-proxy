import { Page, Locator, expect, test } from "@playwright/test";
import { buildExploreUrl, buildLogsDrilldownUrl } from "./url-state";

// Grafana datasource names matching grafana-datasources.yaml
export const PROXY_DS = "Loki (via VL proxy)";
export const PROXY_MULTI_DS = "Loki (via VL proxy multi-tenant)";
export const PROXY_TAIL_DS = "Loki (via VL proxy live tail)";
export const PROXY_TAIL_INGRESS_DS = "Loki (via ingress tail)";
export const PROXY_TAIL_NATIVE_DS = "Loki (via VL proxy live tail native)";
export const PROXY_PATTERNS_AUTODETECT_DS = "Loki (via VL proxy patterns autodetect)";
// Use native-metadata proxy for UI interaction tests — dedicated container avoids
// circuit-breaker cross-contamination from known-failing queries on other proxies.
export const PROXY_INTERACT_DS = "Loki (via VL proxy native metadata)";
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
  await expect(drilldownLabelFilter(page)).toBeVisible({ timeout: 30_000 });
}

/**
 * Logs Drilldown "Filter by labels" / "Filter by fields" comboboxes.
 *
 * Drilldown 2.0.x resolves getByRole("combobox", { name }) for them. On
 * Drilldown 2.5.x the same role query times out while getByPlaceholder resolves
 * the control (observed on 2.5.2: <input role="combobox" placeholder="Filter by
 * labels"> with no aria-label; the computed accessible name differs from the
 * placeholder there). Accepting either keeps the suite valid across the pinned
 * plugin and the latest release (verified on 2.0.4 and 2.5.2).
 */
export function drilldownLabelFilter(page: Page): Locator {
  return page
    .getByRole("combobox", { name: "Filter by labels" })
    .or(page.getByPlaceholder("Filter by labels"))
    .first();
}

export function drilldownFieldFilter(page: Page): Locator {
  return page
    .getByRole("combobox", { name: "Filter by fields" })
    .or(page.getByPlaceholder("Filter by fields"))
    .first();
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
  // Grafana renders graphs on a uPlot canvas. The query editor (Monaco) also
  // owns a hidden 0x0 canvas, so match visible canvases only; the panel wrapper
  // classes are not evidence of a rendered graph.
  const graph = page.locator('canvas:visible, [data-testid="graph-container"]');
  await expect(graph.first()).toBeVisible({ timeout: 15_000 });
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

// Grafana / Loki plugin internal errors that are not caused by the proxy.
// "Failed to load resource" console messages are already captured by the
// responseErrors listener, so suppress the browser-level duplicate.
const DEFAULT_ALLOWED_CONSOLE_ERRORS: RegExp[] = [
  /Failed to load resource/,
  /plugins\/loki/,
  /loki\/module\.js/,
  /ResizeObserver loop/,
  /Non-Error promise rejection/,
  /reading 'subscribe'/,
  /reading 'unsubscribe'/,
  /Cannot read properties of (undefined|null) reading/,
  // Grafana object-format error messages
  /\{status: [45]\d\d,/,
];

export function installGrafanaGuards(page: Page, options: GrafanaGuardOptions = {}) {
  const allowedAlertErrors = options.allowedAlertErrors ?? [];
  const allowedConsoleErrors = [
    ...DEFAULT_ALLOWED_CONSOLE_ERRORS,
    ...(options.allowedConsoleErrors ?? []),
  ];
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

/**
 * Wait until Loki's range-metric path can answer for `selector` in the window
 * the caller is about to compare: [now - lookbackSec - endOffsetSec,
 * now - endOffsetSec].
 *
 * Two fresh-stack effects make this necessary. First, the UI stack's log
 * generator starts with the stack, so a window that excludes the live edge
 * (see LIVE_EDGE_SEC in the specs) is empty on both backends for the first
 * minute. Second, Loki 3.x (TSDB) serves range metric queries through the
 * query-frontend's dynamic sharder, which sizes shards from index stats; the
 * ingester reports no bytes for a stream until its head block is cut, so
 * for the first minutes range metric queries resolve to `shards=0` and come
 * back as an empty 200 while instant and log queries already see the data
 * (verified in Loki's metrics.go log: `splits=1 shards=0
 * querier_exec_time=0s`), and streams then become visible one at a time. The
 * proxy serves the same data immediately, so a parity comparison taken
 * inside that window fails with "Loki returned 0 series". The Go suite
 * mirrors this in waitForLokiMetricData; CI runs the UI shards seconds after
 * the stack becomes ready, squarely inside the window.
 *
 * Because the generator rotates pod names, the newest streams always lag, so
 * the poll does not wait for every stream. It ends when the range path reports
 * every (level, namespace) group that Loki's own series index lists for the
 * selector and window, which is what grouped parity comparisons need. Raw
 * per-stream counts must be compared against the series index instead (see
 * lokiIndexedStreamCount).
 *
 * The current test's timeout is extended by the poll budget so the first test
 * in a worker can absorb the wait.
 */
export async function waitForLokiMetricData(
  page: Page,
  lokiUID: string,
  selector = '{app="api-gateway"}',
  opts: { timeoutMs?: number; endOffsetSec?: number; lookbackSec?: number } = {}
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 240_000;
  const endOffsetSec = opts.endOffsetSec ?? 0;
  const lookbackSec = opts.lookbackSec ?? 10 * 60;
  const groupBy = ["level", "namespace"];
  test.info().setTimeout(test.info().timeout + timeoutMs);
  const deadline = Date.now() + timeoutMs;
  const base = `/api/datasources/proxy/uid/${lokiUID}/loki/api/v1`;
  const key = (labels: Record<string, string>) =>
    groupBy.map((l) => labels[l] ?? "").join("\u0000");
  let last = "";
  while (Date.now() < deadline) {
    const end = Math.floor(Date.now() / 1000) - endOffsetSec;
    const start = end - lookbackSec;
    const rangeParams = new URLSearchParams({
      query: `sum by (${groupBy.join(", ")}) (count_over_time(${selector}[5m]))`,
      start: String(start),
      end: String(end),
      step: "60",
    });
    const seriesParams = new URLSearchParams({
      "match[]": selector,
      start: String(start),
      end: String(end),
    });
    // A refused connection or a non-JSON body while the stack settles is a
    // reason to poll again, not to give up.
    try {
      const [rangeResp, seriesResp] = await Promise.all([
        page.request.get(`${base}/query_range?${rangeParams}`),
        page.request.get(`${base}/series?${seriesParams}`),
      ]);
      if (rangeResp.ok() && seriesResp.ok()) {
        const range = (await rangeResp.json()) as {
          data?: { result?: Array<{ metric: Record<string, string> }> };
        };
        const series = (await seriesResp.json()) as {
          data?: Array<Record<string, string>>;
        };
        const visible = new Set((range.data?.result ?? []).map((r) => key(r.metric)));
        const known = new Set((series.data ?? []).map(key));
        const missing = [...known].filter((k) => !visible.has(k));
        if (known.size > 0 && missing.length === 0) return;
        last = `range path sees ${visible.size} of ${known.size} indexed ${groupBy.join("/")} groups`;
      } else {
        last = `HTTP ${rangeResp.status()} / ${seriesResp.status()}`;
      }
    } catch (err) {
      last = String(err);
    }
    await page.waitForTimeout(2_000);
  }
  throw new Error(
    `Loki range metric queries for ${selector} incomplete after ${timeoutMs} ms ` +
      `(window ends ${endOffsetSec}s ago; last: ${last}); parity results would be meaningless`
  );
}

/**
 * Number of streams Loki's series index lists for `selector` in the window.
 * Served from the ingesters' in-memory index, so unlike the range-metric path
 * it is complete on a fresh stack. Use it as the reference for raw per-stream
 * series counts (one series per stream) instead of Loki's `rate(...)` output.
 */
export async function lokiIndexedStreamCount(
  page: Page,
  lokiUID: string,
  selector: string,
  startSec: number,
  endSec: number
): Promise<number> {
  const params = new URLSearchParams({
    "match[]": selector,
    start: String(startSec),
    end: String(endSec),
  });
  const resp = await page.request.get(
    `/api/datasources/proxy/uid/${lokiUID}/loki/api/v1/series?${params}`
  );
  expect(resp.status(), "loki series status").toBe(200);
  const body = (await resp.json()) as { data?: unknown[] };
  return body.data?.length ?? 0;
}
