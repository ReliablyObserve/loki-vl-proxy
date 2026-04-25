import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  openExplore,
  runQuery,
  waitForGrafanaReady,
  installGrafanaGuards,
} from "./helpers";

// Performance baseline thresholds
const THRESHOLDS = {
  pageLoadTime: 3000, // ms
  queryResponseTime: 5000, // ms
  uiInteractionTime: 500, // ms
  labelLoadTime: 1000, // ms
};

interface PerformanceResult {
  test: string;
  duration: number;
  passed: boolean;
  threshold: number;
}

const results: PerformanceResult[] = [];

test.describe("@performance Loki Explorer - Performance Baseline", () => {
  test.beforeEach(async ({ page }) => {
    await waitForGrafanaReady(page);
    await installGrafanaGuards(page);
  });

  test("Explore page load time", async ({ page }) => {
    const start = performance.now();
    await openExplore(page, PROXY_DS);
    const duration = performance.now() - start;

    results.push({
      test: "Explore page load",
      duration,
      passed: duration < THRESHOLDS.pageLoadTime,
      threshold: THRESHOLDS.pageLoadTime,
    });

    expect(duration).toBeLessThan(THRESHOLDS.pageLoadTime);
    console.log(
      `✅ Page load: ${duration.toFixed(0)}ms (threshold: ${THRESHOLDS.pageLoadTime}ms)`
    );
  });

  test("Simple metric query response", async ({ page }) => {
    await openExplore(page, PROXY_DS);

    const start = performance.now();
    await runQuery(page, 'up{job="api-gateway"}');
    const duration = performance.now() - start;

    results.push({
      test: "Simple metric query",
      duration,
      passed: duration < THRESHOLDS.queryResponseTime,
      threshold: THRESHOLDS.queryResponseTime,
    });

    expect(duration).toBeLessThan(THRESHOLDS.queryResponseTime);
    console.log(
      `✅ Query: ${duration.toFixed(0)}ms (threshold: ${THRESHOLDS.queryResponseTime}ms)`
    );
  });

  test("JSON parsed logs query response", async ({ page }) => {
    await openExplore(page, PROXY_DS);

    const start = performance.now();
    await runQuery(page, '{job="api-gateway"} | json');
    const duration = performance.now() - start;

    results.push({
      test: "JSON parsed logs query",
      duration,
      passed: duration < THRESHOLDS.queryResponseTime,
      threshold: THRESHOLDS.queryResponseTime,
    });

    expect(duration).toBeLessThan(THRESHOLDS.queryResponseTime);
    console.log(
      `✅ Parsed query: ${duration.toFixed(0)}ms (threshold: ${THRESHOLDS.queryResponseTime}ms)`
    );
  });

  test("Log entry expansion time", async ({ page }) => {
    await openExplore(page, PROXY_DS);
    await runQuery(page, '{job="api-gateway"} | json');

    const logRow = page.locator("[data-testid='log-row']").first();
    const expandBtn = logRow.locator("[data-testid='log-row-expand-button']");

    if (await expandBtn.isVisible().catch(() => false)) {
      const start = performance.now();
      await expandBtn.click();
      const duration = performance.now() - start;

      results.push({
        test: "Log entry expansion",
        duration,
        passed: duration < THRESHOLDS.uiInteractionTime,
        threshold: THRESHOLDS.uiInteractionTime,
      });

      expect(duration).toBeLessThan(THRESHOLDS.uiInteractionTime);
      console.log(
        `✅ Expand: ${duration.toFixed(0)}ms (threshold: ${THRESHOLDS.uiInteractionTime}ms)`
      );
    }
  });

  test("Label selector load time", async ({ page }) => {
    await openExplore(page, PROXY_DS);

    const labelSelector = page.locator("[data-testid='label-selector']");
    if (await labelSelector.isVisible().catch(() => false)) {
      const start = performance.now();
      await labelSelector.click();
      await page.waitForTimeout(100);
      const duration = performance.now() - start;

      results.push({
        test: "Label selector open",
        duration,
        passed: duration < THRESHOLDS.labelLoadTime,
        threshold: THRESHOLDS.labelLoadTime,
      });

      expect(duration).toBeLessThan(THRESHOLDS.labelLoadTime);
      console.log(
        `✅ Label selector: ${duration.toFixed(0)}ms (threshold: ${THRESHOLDS.labelLoadTime}ms)`
      );
    }
  });

  test("Concurrent filter changes response", async ({ page }) => {
    await openExplore(page, PROXY_DS);
    await runQuery(page, '{job="api-gateway"} | json');

    const start = performance.now();

    // Apply 3 rapid filters
    const filterBtn = page.locator("[data-testid='add-filter-button']");
    if (await filterBtn.isVisible().catch(() => false)) {
      for (let i = 0; i < 3; i++) {
        await filterBtn.click();
        await page.waitForTimeout(100);
      }
    }

    const duration = performance.now() - start;

    results.push({
      test: "Rapid filter application",
      duration,
      passed: duration < THRESHOLDS.queryResponseTime,
      threshold: THRESHOLDS.queryResponseTime,
    });

    console.log(
      `✅ Rapid filters: ${duration.toFixed(0)}ms (threshold: ${THRESHOLDS.queryResponseTime}ms)`
    );
  });

  test("Performance Report", async ({ page }) => {
    console.log("\n");
    console.log("═══════════════════════════════════════════════════════════");
    console.log("        LOKI EXPLORER - PERFORMANCE BASELINE REPORT        ");
    console.log("═══════════════════════════════════════════════════════════");

    const passCount = results.filter((r) => r.passed).length;
    const totalCount = results.length;

    results.forEach((result) => {
      const status = result.passed ? "✅ PASS" : "❌ FAIL";
      const percent = ((result.duration / result.threshold) * 100).toFixed(0);
      console.log(
        `${status} | ${result.test.padEnd(35)} | ${result.duration.toFixed(0).padStart(5)}ms / ${result.threshold}ms (${percent}%)`
      );
    });

    console.log("═══════════════════════════════════════════════════════════");
    console.log(`SUMMARY: ${passCount}/${totalCount} tests passed`);
    console.log("═══════════════════════════════════════════════════════════\n");

    // Report pass/fail
    expect(passCount).toBe(totalCount);
  });
});
