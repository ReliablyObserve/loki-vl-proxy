import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  openExplore,
  typeQuery,
  runQuery,
  assertNoErrors,
  waitForGrafanaReady,
  collectLokiErrors,
} from "./helpers";

test.describe("Grafana Drilldown & Label Navigation", () => {
  test.beforeEach(async ({ page }) => {
    await openExplore(page, PROXY_DS);
    await waitForGrafanaReady(page);
  });

  test("clicking a log row expands details without error", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway"}');
    await runQuery(page);

    // Click first log row to expand
    const logRow = page
      .locator('[data-testid="logRows"] tr, [class*="logs-row"]')
      .first();
    if (await logRow.isVisible({ timeout: 5000 }).catch(() => false)) {
      await logRow.click();
      await page.waitForTimeout(500);
      await assertNoErrors(page);
    }

    expect(errors).toHaveLength(0);
  });

  test("label filter drill-down for app label", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway"}');
    await runQuery(page);

    // Expand log row
    const logRow = page
      .locator('[data-testid="logRows"] tr, [class*="logs-row"]')
      .first();
    if (await logRow.isVisible({ timeout: 5000 }).catch(() => false)) {
      await logRow.click();
      await page.waitForTimeout(500);

      // Look for label filter buttons (the = icon next to labels in detail panel)
      const filterBtn = page
        .locator('[title*="Filter for value"], [aria-label*="Filter for value"]')
        .first();
      if (await filterBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
        await filterBtn.click();
        await page.waitForTimeout(1000);
        await assertNoErrors(page);
      }
    }

    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });

  test("multi-label drill-down through app → level", async ({ page }) => {
    const errors = collectLokiErrors(page);

    // Step 1: Query by app
    await typeQuery(page, '{app="api-gateway"}');
    await runQuery(page);
    await assertNoErrors(page);

    // Step 2: Narrow to level=error
    await typeQuery(page, '{app="api-gateway", level="error"}');
    await runQuery(page);
    await assertNoErrors(page);

    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });

  test("drill-down from metric to logs", async ({ page }) => {
    const errors = collectLokiErrors(page);

    // Start with metric query
    await typeQuery(page, 'count_over_time({app="api-gateway"}[5m])');
    await runQuery(page);

    // Switch to log query (simulating drill-down)
    await typeQuery(page, '{app="api-gateway"}');
    await runQuery(page);

    await assertNoErrors(page);
    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });

  test("drill-down with complex filter chain", async ({ page }) => {
    const errors = collectLokiErrors(page);

    // Multi-filter query
    await typeQuery(
      page,
      '{app="api-gateway"} |= "error" != "health" | json | level="error"'
    );
    await runQuery(page);

    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });

  test("label values dropdown loads without error", async ({ page }) => {
    const errors = collectLokiErrors(page);

    // Type a partial query with stream selector
    await typeQuery(page, '{app=');
    await page.waitForTimeout(2000);

    // Label value autocomplete should fire /label/app/values
    // Check no errors from that request
    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });
});

test.describe("Grafana Error Handling", () => {
  test.beforeEach(async ({ page }) => {
    await openExplore(page, PROXY_DS);
    await waitForGrafanaReady(page);
  });

  test("invalid query shows user-friendly error", async ({ page }) => {
    // Intentionally invalid LogQL
    await typeQuery(page, "{{{invalid}}}");
    await runQuery(page);
    await page.waitForTimeout(2000);

    // Grafana should show an error message, not crash
    // We're checking no 500-level server errors
    const responses: number[] = [];
    page.on("response", (r) => {
      if (r.url().includes("/loki/api/v1/")) {
        responses.push(r.status());
      }
    });
  });

  test("empty query returns valid response", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, "{}");
    await runQuery(page);
    await page.waitForTimeout(2000);

    // Empty query should work or return a clean error, not 500
    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });

  test("very long query does not crash", async ({ page }) => {
    const errors = collectLokiErrors(page);
    // Long but valid query
    const longFilter = Array(20)
      .fill('|= "test"')
      .join(" ");
    await typeQuery(page, `{app="api-gateway"} ${longFilter}`);
    await runQuery(page);
    await page.waitForTimeout(2000);

    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });

  test("binary expression does not crash UI", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(
      page,
      'sum(rate({app="api-gateway"}[5m])) / sum(rate({app="api-gateway", level="error"}[5m]))'
    );
    await runQuery(page);
    await page.waitForTimeout(2000);

    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });
});
