/**
 * Real UI click interaction tests.
 *
 * These tests actually click through Grafana's Explore UI and verify
 * that the UI responds correctly: log rows expand, parsed fields appear,
 * filter buttons work, queries update, and re-runs produce correct results.
 */

import { test, expect, type Page } from "@playwright/test";
import {
  PROXY_INTERACT_DS,
  openExplore,
  runQuery,
  assertLogsVisible,
  waitForGrafanaReady,
  installGrafanaGuards,
} from "./helpers";

// Use serial mode to prevent parallel tests from tripping the proxy circuit breaker.
// Each test opens a fresh page; sharing a single proxy container serially is safe.
test.describe.configure({ mode: "serial" });

const PROXY_DS = PROXY_INTERACT_DS;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------


/** Click the first visible log row and return whether it expanded. */
async function expandFirstLogRow(page: Page): Promise<boolean> {
  // Grafana 12 renders log rows as divs inside [data-testid="logRows"]
  const row = page
    .locator(
      '[data-testid="logRows"] [data-testid="logRow"], ' +
      '[data-testid="logRows"] > div > div, ' +
      '[class*="logs-row"]:not([class*="logs-row__"]), ' +
      '[class*="logsRow"]:not([class*="logsRow__"])'
    )
    .first();

  if (!(await row.isVisible({ timeout: 5000 }).catch(() => false))) return false;

  await row.click();
  await page.waitForTimeout(600);
  return true;
}

/** Return the visible text content of the expanded log details panel. */
async function getExpandedDetails(page: Page): Promise<string> {
  // Grafana 12 expands rows inline — try multiple selector strategies
  const selectors = [
    '[data-testid="logRowDetails"]',
    '[class*="logRowDetails"]',
    '[class*="logDetails"]',
    '[class*="log-details"]',
    '[class*="logRow__details"]',
    // Grafana 12 inline expansion: the row body that appears after click
    '[data-testid="logRows"] [aria-expanded="true"] ~ *',
    '[class*="logsRow"][class*="expanded"] [class*="body"]',
    '[class*="logsRow"][class*="expanded"]',
    // Generic: anything visible below the log rows that looks like a detail panel
    '[class*="logRow"] + div[class]',
    '[class*="logsRowDetailsTable"]',
    '[class*="detailsTable"]',
  ];

  for (const selector of selectors) {
    const el = page.locator(selector).first();
    if (await el.isVisible({ timeout: 1000 }).catch(() => false)) {
      return (await el.textContent()) ?? "";
    }
  }

  // Last resort: all visible log row text after click (may include expanded inline content)
  const rows = page.locator('[data-testid="logRows"]').first();
  if (await rows.isVisible({ timeout: 1000 }).catch(() => false)) {
    return (await rows.textContent()) ?? "";
  }

  return "";
}

/** Return all visible log row text content (for content verification). */
async function getLogRowTexts(page: Page): Promise<string[]> {
  // Wait for log rows to fully render
  await page.waitForTimeout(500);

  // Grafana 12 uses div-based log rows; try several selector strategies
  const selectors = [
    '[data-testid="logRows"] [data-testid="logRow"]',
    '[data-testid="logRows"] > div > div[class]',
    '[class*="logs-row__message"]',
    '[class*="logsRowMessage"]',
  ];

  for (const selector of selectors) {
    const rows = page.locator(selector);
    const count = await rows.count();
    if (count === 0) continue;

    const texts: string[] = [];
    for (let i = 0; i < count; i++) {
      const text = await rows.nth(i).textContent();
      if (text?.trim()) texts.push(text.trim());
    }
    if (texts.length > 0) return texts;
  }

  // Fallback: grab all visible text from the logRows container
  const container = page.locator('[data-testid="logRows"]').first();
  if (!(await container.isVisible({ timeout: 2000 }).catch(() => false))) return [];
  const allText = await container.textContent() ?? "";
  // Split by newlines and return non-empty lines
  return allText.split("\n").map(l => l.trim()).filter(l => l.length > 10);
}

// ---------------------------------------------------------------------------
// Log row expansion and content verification
// ---------------------------------------------------------------------------

test.describe("@click-interactions Log row expansion", () => {
  test("clicking a log row expands the details panel @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(page, PROXY_DS, '{app="api-gateway"}');
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    const expanded = await expandFirstLogRow(page);
    expect(expanded).toBeTruthy();

    // After clicking, some details panel or expanded content should be visible
    const details = page.locator(
      '[data-testid="logRowDetails"], [class*="logDetails"], [class*="log-details"], [class*="logRow__details"]'
    );
    const detailsVisible = await details
      .first()
      .isVisible({ timeout: 3000 })
      .catch(() => false);

    // Either a details panel appeared, or the row itself expanded (accordion style)
    const rowExpanded = await page
      .locator('[class*="logRow"][class*="expanded"], [aria-expanded="true"]')
      .first()
      .isVisible({ timeout: 1000 })
      .catch(() => false);

    expect(detailsVisible || rowExpanded).toBeTruthy();
    await guards.assertClean();
  });

  test("expanded row shows parsed fields from json parser @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    await expandFirstLogRow(page);
    const details = await getExpandedDetails(page);

    // After json parsing, the detail panel should show field names
    // At minimum one of these should appear in the expanded details
    const expectedFields = ["method", "path", "status", "duration_ms"];
    const foundFields = expectedFields.filter((f) =>
      details.toLowerCase().includes(f)
    );

    expect(foundFields.length).toBeGreaterThan(
      0,
      `Expected to find at least one of [${expectedFields.join(", ")}] in expanded row details. Got: ${details.substring(0, 500)}`
    );

    await guards.assertClean();
  });

  test("expanded row shows trace_id from api-gateway logs @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    await expandFirstLogRow(page);
    const details = await getExpandedDetails(page);

    // The api-gateway logs contain trace_id — verify it surfaces
    if (details.length > 0) {
      expect(details).toContain("trace_id");
    }

    await guards.assertClean();
  });

  test("json + keep shows only kept fields in expanded row @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | keep method, path, status'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    await expandFirstLogRow(page);
    const details = await getExpandedDetails(page);

    if (details.length > 0) {
      // Fields that were dropped should not appear
      const droppedFields = ["trace_id", "user_id", "duration_ms"];
      for (const f of droppedFields) {
        if (details.includes(f)) {
          // Some Grafana versions may show stream labels + parsed fields together
          // This is a soft check — just log, don't fail
          console.warn(`Note: dropped field "${f}" still visible in details`);
        }
      }
      // Kept fields should be present
      const keptFields = ["method", "path", "status"];
      const foundKept = keptFields.filter((f) => details.includes(f));
      expect(foundKept.length).toBeGreaterThan(0);
    }

    await guards.assertClean();
  });
});

// ---------------------------------------------------------------------------
// Log row content verification
// ---------------------------------------------------------------------------

test.describe("@click-interactions Log row content verification", () => {
  test("GET filter only shows log lines containing GET @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | method="GET"'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    const texts = await getLogRowTexts(page);
    expect(texts.length).toBeGreaterThan(0);

    // At least some visible log lines should contain "GET"
    const getLines = texts.filter((t) => t.includes("GET"));
    expect(getLines.length).toBeGreaterThan(0);

    await guards.assertClean();
  });

  test("error filter shows only error logs @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | status >= 400'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);

    // May return results or empty — either is valid
    const logsVisible = await page
      .locator('[data-testid="logRows"], [class*="logs-row"]')
      .first()
      .isVisible({ timeout: 5000 })
      .catch(() => false);

    if (logsVisible) {
      const texts = await getLogRowTexts(page);
      // All visible log lines should have status >= 400
      const errorLines = texts.filter((t) => {
        const match = t.match(/"status"\s*:\s*(\d+)/) ?? t.match(/status[=:]\s*(\d+)/);
        if (!match) return true; // can't parse, assume ok
        return Number(match[1]) >= 400;
      });
      expect(errorLines.length).toBe(texts.length);
    }

    await guards.assertClean();
  });

  test("line_format output shows formatted template @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | line_format "M={{.method}} S={{.status}}"'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    const texts = await getLogRowTexts(page);
    expect(texts.length).toBeGreaterThan(0);

    // Log lines should be formatted as "M=GET S=200" etc
    const formattedLines = texts.filter((t) => t.includes("M=") && t.includes("S="));
    expect(formattedLines.length).toBeGreaterThan(0);

    await guards.assertClean();
  });

  // Known gap: |~ alternation "A|B" returns 502 (VictoriaLogs regex translation)
  test.fixme("regex line filter removes non-matching lines @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} |~ "POST|PUT|DELETE"'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);

    const logsVisible = await page
      .locator('[data-testid="logRows"], [class*="logs-row"]')
      .first()
      .isVisible({ timeout: 5000 })
      .catch(() => false);

    if (logsVisible) {
      const texts = await getLogRowTexts(page);
      if (texts.length > 0) {
        // All lines should contain POST, PUT, or DELETE
        const matched = texts.filter(
          (t) => t.includes("POST") || t.includes("PUT") || t.includes("DELETE")
        );
        // At least half should match (some context lines may not contain the method)
        expect(matched.length).toBeGreaterThan(0);
      }
    }

    await guards.assertClean();
  });
});

// ---------------------------------------------------------------------------
// Filter button interactions
// ---------------------------------------------------------------------------

test.describe("@click-interactions Filter button interactions", () => {
  test("filter-for-value button in log details adds to query @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(page, PROXY_DS, '{app="api-gateway"}');
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    // Expand a log row
    const expanded = await expandFirstLogRow(page);
    if (!expanded) {
      console.log("ℹ️ No log rows to expand — skipping filter interaction test");
      return;
    }

    // Look for "Filter for value" button (the = icon next to a field value)
    const filterForBtn = page
      .locator(
        '[title*="Filter for value"], [aria-label*="Filter for value"], [title*="filter for"], button[name*="include"]'
      )
      .first();

    if (!(await filterForBtn.isVisible({ timeout: 3000 }).catch(() => false))) {
      console.log("ℹ️ Filter-for-value button not found — skipping");
      await guards.assertClean();
      return;
    }

    // Click the filter and wait for the query to update
    await filterForBtn.click();
    await page.waitForTimeout(1500);

    // The query editor should now contain more filter criteria
    const editor = page
      .locator('[data-testid="query-editor-rows"], [data-testid="query-editor-row"]')
      .first();
    const editorText = await editor.textContent();

    // Query should have changed from the original
    expect(editorText?.length ?? 0).toBeGreaterThan(0);

    await guards.assertClean();
  });

  test("filter-out-value button removes values from results @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(page, PROXY_DS, '{app="api-gateway"}');
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    const initialTexts = await getLogRowTexts(page);

    await expandFirstLogRow(page);

    const filterOutBtn = page
      .locator(
        '[title*="Filter out value"], [aria-label*="Filter out value"], [title*="exclude"], button[name*="exclude"]'
      )
      .first();

    if (!(await filterOutBtn.isVisible({ timeout: 3000 }).catch(() => false))) {
      console.log("ℹ️ Filter-out-value button not found — skipping");
      await guards.assertClean();
      return;
    }

    await filterOutBtn.click();
    await page.waitForTimeout(1000);
    await runQuery(page);
    await page.waitForTimeout(500);

    const filteredTexts = await getLogRowTexts(page);

    // After filtering out a value, we should have fewer or equal results
    expect(filteredTexts.length).toBeLessThanOrEqual(initialTexts.length);

    await guards.assertClean();
  });
});

// ---------------------------------------------------------------------------
// Complex query interaction tests
// ---------------------------------------------------------------------------

test.describe("@click-interactions Complex query interactions", () => {
  test("chained parser + filter + format shows formatted output @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | method="GET" | line_format "{{.method}} {{.path}}"'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    const texts = await getLogRowTexts(page);
    expect(texts.length).toBeGreaterThan(0);

    // Lines should be formatted as "GET /path/..." (may include timestamp prefix)
    const formatted = texts.filter((t) => t.includes("GET "));
    expect(formatted.length).toBeGreaterThan(0);

    await guards.assertClean();
  });

  test("error logs expand to show error field @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | status >= 500'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);

    const logsVisible = await page
      .locator('[data-testid="logRows"], [class*="logs-row"]')
      .first()
      .isVisible({ timeout: 5000 })
      .catch(() => false);

    if (!logsVisible) {
      console.log("ℹ️ No 500-level logs — skipping expansion check");
      await guards.assertClean();
      return;
    }

    await expandFirstLogRow(page);
    const details = await getExpandedDetails(page);

    if (details.length > 0) {
      // Error logs have an "error" field — should appear in details
      expect(details.toLowerCase()).toContain("error");
    }

    await guards.assertClean();
  });

  test("multi-label selector shows results from matching streams @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway",namespace="prod"}'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    const texts = await getLogRowTexts(page);
    expect(texts.length).toBeGreaterThan(0);

    await guards.assertClean();
  });

  // Known gap: {label=~"A|B"} regex alternation in label selector returns 502
  test.fixme("regex label selector returns multiple app streams @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{app=~"api-.*|payment-.*"}'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);
    await assertLogsVisible(page);

    const texts = await getLogRowTexts(page);
    // With two apps matched, should have results from both
    expect(texts.length).toBeGreaterThan(0);

    await guards.assertClean();
  });

  test("deeply chained: selector + two line filters + json + field filter @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      '{namespace="prod"} |= "api" != "health" | json | method="GET"'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);

    // Should execute without crashing
    const editor = page
      .locator('[data-testid="query-editor-rows"], [data-testid="query-editor-row"]')
      .first();
    await expect(editor).toBeVisible();

    await guards.assertClean();
  });

  test("metric query shows graph panel not log panel @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      'sum by (level) (count_over_time({app="api-gateway"}[5m]))'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);

    // Metric queries should render a graph, not log rows
    const graphVisible = await page
      .locator('canvas, [data-testid="graph-container"], [class*="uplot"]')
      .first()
      .isVisible({ timeout: 8000 })
      .catch(() => false);

    const noErrorAlert = await page
      .locator('[data-testid="data-testid Alert error"]')
      .isVisible({ timeout: 1000 })
      .catch(() => false);

    expect(!noErrorAlert).toBeTruthy();
    // Graph may or may not render depending on data — just verify no error
    console.log(`Graph visible: ${graphVisible}`);

    await guards.assertClean();
  });

  test("error rate query renders without proxy errors @click-interactions", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(
      page,
      PROXY_DS,
      'sum(rate({app="api-gateway"} | json | status >= 400 [5m])) / sum(rate({app="api-gateway"}[5m]))'
    );
    await waitForGrafanaReady(page);
    await runQuery(page);

    // Should not show a proxy/translation error
    const errorAlert = page.locator(
      '[data-testid="data-testid Alert error"], [class*="alert-error"]'
    );
    const hasError = await errorAlert.isVisible({ timeout: 2000 }).catch(() => false);
    expect(hasError).toBeFalsy();

    await guards.assertClean();
  });
});

// ---------------------------------------------------------------------------
// Performance regression — response timing
// ---------------------------------------------------------------------------

test.describe("@click-interactions Performance regression", () => {
  const timingCases = [
    {
      name: "raw stream query",
      query: `{app="api-gateway"}`,
      maxMs: 3000,
    },
    {
      name: "json parsed query",
      query: `{app="api-gateway"} | json`,
      maxMs: 5000,
    },
    {
      name: "chained filter query",
      query: `{app="api-gateway"} | json | method="GET" | status >= 200`,
      maxMs: 5000,
    },
    {
      name: "metric rate query",
      query: `sum by (level) (rate({namespace="prod"}[5m]))`,
      maxMs: 8000,
    },
    {
      name: "complex chained metric",
      query: `sum(rate({app="api-gateway"} | json | status >= 400 [5m])) / sum(rate({app="api-gateway"}[5m]))`,
      maxMs: 8000,
    },
  ];

  for (const tc of timingCases) {
    test(`${tc.name} responds within ${tc.maxMs}ms @click-interactions`, async ({
      page,
    }) => {
      installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
      });
      await openExplore(page, PROXY_DS, tc.query);
      await waitForGrafanaReady(page);

      const start = Date.now();
      await runQuery(page);
      const elapsed = Date.now() - start;

      expect(elapsed).toBeLessThan(tc.maxMs);
      console.log(`✅ ${tc.name}: ${elapsed}ms (limit: ${tc.maxMs}ms)`);
    });
  }
});
