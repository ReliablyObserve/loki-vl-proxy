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

  test("clicking a log row expands details without error @drilldown-core", async ({ page }) => {
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

  test("label filter drill-down for app label @drilldown-core", async ({ page }) => {
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
});
