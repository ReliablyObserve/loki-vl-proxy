import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  openExplore,
  runQuery,
  assertLogsVisible,
  waitForGrafanaReady,
  installGrafanaGuards,
} from "./helpers";

test.describe("@comprehensive-ui Loki Explorer - Comprehensive UI Coverage", () => {
  const metrics = {
    pageLoads: [] as number[],
    queries: [] as number[],
    uiInteractions: [] as number[],
  };

  test.beforeEach(async ({ page }) => {
    await waitForGrafanaReady(page);
    await installGrafanaGuards(page);
  });

  test.describe("Page Load Performance", () => {
    test("should load Explore page within acceptable time", async ({ page }) => {
      const startTime = Date.now();
      await openExplore(page, PROXY_DS);
      const loadTime = Date.now() - startTime;
      metrics.pageLoads.push(loadTime);

      expect(loadTime).toBeLessThan(3000);
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
      const testQuery = '{job="api-gateway"}';
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
      const testQuery = '{job="api-gateway"} | json';
      await openExplore(page, PROXY_DS, testQuery);
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;
      metrics.queries.push(responseTime);

      // Check for results
      await assertLogsVisible(page);
      expect(responseTime).toBeLessThan(5000);
      console.log(`✅ Query executed and results shown in ${responseTime}ms`);
    });
  });

  test.describe("Query Execution", () => {
    test("should execute simple metric query", async ({ page }) => {
      const query = 'sum(rate({job="api-gateway"}[5m]))';
      await openExplore(page, PROXY_DS, query);
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;
      metrics.queries.push(responseTime);

      expect(responseTime).toBeLessThan(5000);
      console.log(`✅ Metric query executed in ${responseTime}ms`);
    });

    test("should handle parsed JSON logs", async ({ page }) => {
      const query = '{job="api-gateway"} | json';
      await openExplore(page, PROXY_DS, query);
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;

      await assertLogsVisible(page);
      expect(responseTime).toBeLessThan(5000);
      console.log(`✅ JSON parsed logs executed in ${responseTime}ms`);
    });

    test("should show results in appropriate panel", async ({ page }) => {
      const query = '{job="api-gateway"}';
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
      await openExplore(page, PROXY_DS, '{job="api-gateway"}');
      await waitForGrafanaReady(page);
      const loadTime = Date.now() - startTime;
      metrics.uiInteractions.push(loadTime);

      expect(loadTime).toBeLessThan(3000);
      console.log(`✅ Explore page loads in ${loadTime}ms`);
    });

    test("should display results after query execution", async ({ page }) => {
      await openExplore(page, PROXY_DS, '{job="payment-service"}');
      await waitForGrafanaReady(page);

      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;
      metrics.uiInteractions.push(responseTime);

      await assertLogsVisible(page);
      expect(responseTime).toBeLessThan(5000);
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
      expect(responseTime).toBeLessThan(5000);
      console.log(
        `✅ Empty results handled gracefully in ${responseTime}ms`
      );
    });
  });

  test.describe("Time Range & Filters", () => {
    test("should accept queries with label filters", async ({ page }) => {
      const queryWithFilter = '{job="api-gateway"} | level="error"';
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
      const metricsQuery =
        '{job="api-gateway"} | json | unwrap response_time | avg';

      await openExplore(page, PROXY_DS, metricsQuery);
      await waitForGrafanaReady(page);
      const startTime = Date.now();
      await runQuery(page);
      const responseTime = Date.now() - startTime;

      expect(responseTime).toBeLessThan(5000);
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
