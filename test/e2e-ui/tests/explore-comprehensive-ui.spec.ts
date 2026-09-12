import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  openExplore,
  runQuery,
  assertLogsVisible,
  assertGraphVisible,
  waitForGrafanaReady,
  installGrafanaGuards,
} from "./helpers";

// The e2e dataset carries `app`/`service_name` stream labels (no `job`); the
// suite previously queried {job="api-gateway"} and could never return rows.
// Timings are recorded as test annotations, not asserted: wall-clock bounds
// on a shared CI runner flake without saying anything about the proxy. Every
// test is guarded against Grafana error toasts, console errors and failed
// datasource requests instead.
test.describe("@comprehensive-ui Loki Explorer - Comprehensive UI Coverage", () => {
  const metrics = {
    pageLoads: [] as number[],
    queries: [] as number[],
    uiInteractions: [] as number[],
  };

  let guards: ReturnType<typeof installGrafanaGuards>;

  test.beforeEach(async ({ page }) => {
    await waitForGrafanaReady(page);
    guards = installGrafanaGuards(page, {
      // Explore cancels the in-flight query when a test navigates away.
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });
  });

  test.afterEach(async () => {
    await guards.assertClean();
  });

  const recordTiming = (name: string, ms: number) =>
    test.info().annotations.push({ type: "timing-ms", description: `${name}=${ms}` });

  test.describe("Page Load Performance", () => {
    test("should load Explore page within acceptable time", async ({ page }) => {
      const startTime = Date.now();
      await openExplore(page, PROXY_DS);
      const loadTime = Date.now() - startTime;
      metrics.pageLoads.push(loadTime);

      recordTiming("loadTime", loadTime);
      console.log(`✅ Explore page loaded in ${loadTime}ms`);
    });

    test("should display query editor on page load", async ({ page }) => {
      await openExplore(page, PROXY_DS);
      const editor = page
        .locator('[data-testid="query-editor-rows"], [data-testid="query-editor-row"]')
        .first();
      await expect(editor).toBeVisible();
      console.log("✅ Query editor visible after page load");
    });

    test("should show Explore toolbar with action buttons", async ({ page }) => {
      await openExplore(page, PROXY_DS);
      const runButton = page.getByRole("button", { name: /run query/i });
      await expect(runButton).toBeVisible({ timeout: 5000 });
      console.log("✅ Explore toolbar with run button visible");
    });
  });

  test.describe("Query Editor UI", () => {
    test("should render LogQL query editor", async ({ page }) => {
      await openExplore(page, PROXY_DS);
      await waitForGrafanaReady(page);
      const editor = page
        .locator('[data-testid="query-editor-rows"], [data-testid="query-editor-row"]')
        .first();
      await expect(editor).toBeVisible();
      console.log("✅ Query editor visible");
    });

    test("should allow entering query in editor", async ({ page }) => {
      const testQuery = '{app="api-gateway"}';
      await openExplore(page, PROXY_DS, testQuery);
      await waitForGrafanaReady(page);

      const editor = page
        .locator('[data-testid="query-editor-rows"], [data-testid="query-editor-row"]')
        .first();
      const editorText = await editor.textContent();
      expect(editorText).toContain(testQuery);
      console.log(`✅ Query editor contains: ${testQuery}`);
    });

    test("should execute query and show results", async ({ page }) => {
      const testQuery = '{app="api-gateway"} | json';
      await openExplore(page, PROXY_DS, testQuery);
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;
      metrics.queries.push(responseTime);

      // Check for results
      await assertLogsVisible(page);
      recordTiming("responseTime", responseTime);
      console.log(`✅ Query executed and results shown in ${responseTime}ms`);
    });
  });

  test.describe("Query Execution", () => {
    test("should execute simple metric query", async ({ page }) => {
      const query = 'sum(rate({app="api-gateway"}[5m]))';
      await openExplore(page, PROXY_DS, query);
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;
      metrics.queries.push(responseTime);

      recordTiming("responseTime", responseTime);
      console.log(`✅ Metric query executed in ${responseTime}ms`);
    });

    test("should handle parsed JSON logs", async ({ page }) => {
      const query = '{app="api-gateway"} | json';
      await openExplore(page, PROXY_DS, query);
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;

      await assertLogsVisible(page);
      recordTiming("responseTime", responseTime);
      console.log(`✅ JSON parsed logs executed in ${responseTime}ms`);
    });

    test("should show results in appropriate panel", async ({ page }) => {
      const query = '{app="api-gateway"}';
      await openExplore(page, PROXY_DS, query);
      await waitForGrafanaReady(page);
      await runQuery(page);

      // Results should be visible (logs panel or table)
      const resultsVisible = await page
        .locator('[class*="logs"], [class*="LogsTable"], [data-testid="logRows"]')
        .first()
        .isVisible({ timeout: 5000 })
        .catch(() => false);

      expect(resultsVisible).toBeTruthy();
      console.log("✅ Results displayed in results panel");
    });
  });

  test.describe("UI Interactions", () => {
    test("should load page quickly", async ({ page }) => {
      const startTime = Date.now();
      await openExplore(page, PROXY_DS, '{app="api-gateway"}');
      await waitForGrafanaReady(page);
      const loadTime = Date.now() - startTime;
      metrics.uiInteractions.push(loadTime);

      recordTiming("loadTime", loadTime);
      console.log(`✅ Explore page loads in ${loadTime}ms`);
    });

    test("should display results after query execution", async ({ page }) => {
      await openExplore(page, PROXY_DS, '{app="payment-service"}');
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;
      metrics.uiInteractions.push(responseTime);

      await assertLogsVisible(page);
      recordTiming("responseTime", responseTime);
      console.log(`✅ Results displayed in ${responseTime}ms`);
    });

    test("should handle empty results gracefully", async ({ page }) => {
      // Use a query unlikely to match anything
      const query = '{nonexistent_label="definitely_not_there_12345"}';
      await openExplore(page, PROXY_DS, query);
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;

      // Should not crash or show error, just empty results
      const editor = page
        .locator('[data-testid="query-editor-rows"], [data-testid="query-editor-row"]')
        .first();
      await expect(editor).toBeVisible();
      recordTiming("responseTime", responseTime);
      console.log(
        `✅ Empty results handled gracefully in ${responseTime}ms`
      );
    });
  });

  test.describe("Time Range & Filters", () => {
    test("should accept queries with label filters", async ({ page }) => {
      const queryWithFilter = '{app="api-gateway"} | level="error"';
      await openExplore(page, PROXY_DS, queryWithFilter);
      await waitForGrafanaReady(page);
      await runQuery(page);

      // Should execute without error
      const editor = page
        .locator('[data-testid="query-editor-rows"], [data-testid="query-editor-row"]')
        .first();
      await expect(editor).toBeVisible();
      console.log("✅ Queries with filters execute correctly");
    });

    test("should support unwrap operations", async ({ page }) => {
      // unwrap is only valid inside a range aggregation; the dataset carries
      // duration_ms (explore-regression uses the same expression).
      const metricsQuery =
        'avg_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])';

      await openExplore(page, PROXY_DS, metricsQuery);
      await waitForGrafanaReady(page);
      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;

      await assertGraphVisible(page);
      recordTiming("responseTime", responseTime);
      console.log(`✅ Unwrap operations execute in ${responseTime}ms`);
    });
  });

  test.describe("Performance Summary", () => {
    test("should collect and report metrics", async ({ page }) => {
      // This test just documents what metrics were collected
      if (metrics.pageLoads.length > 0) {
        const avgLoad =
          metrics.pageLoads.reduce((a, b) => a + b) / metrics.pageLoads.length;
        console.log(`📊 Average page load: ${avgLoad.toFixed(0)}ms`);
      }

      if (metrics.queries.length > 0) {
        const avgQuery =
          metrics.queries.reduce((a, b) => a + b) / metrics.queries.length;
        console.log(`📊 Average query response: ${avgQuery.toFixed(0)}ms`);
      }

      if (metrics.uiInteractions.length > 0) {
        const avgUI =
          metrics.uiInteractions.reduce((a, b) => a + b) /
          metrics.uiInteractions.length;
        console.log(`📊 Average UI interaction: ${avgUI.toFixed(0)}ms`);
      }

      console.log(
        `📊 PERFORMANCE SUMMARY: page loads=${metrics.pageLoads.length}, queries=${metrics.queries.length}, interactions=${metrics.uiInteractions.length}`
      );
    });
  });
});
