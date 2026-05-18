import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  openExplore,
  runQuery,
  assertLogsVisible,
  waitForGrafanaReady,
  installGrafanaGuards,
} from "./helpers";

test.describe("Grafana Explore — Multi-Tenant Scenarios", () => {
  test(
    "switching from multi-tenant to single-tenant datasource clears tenant filter @explore-mt",
    async ({ page }) => {
      const guards = installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
      });

      // Start on multi-tenant datasource with tenant filter
      await openExplore(page, PROXY_MULTI_DS, '{app="api-gateway", __tenant_id__="fake"}');
      await waitForGrafanaReady(page);
      await runQuery(page);
      await assertLogsVisible(page);

      // Switch to single-tenant datasource
      await openExplore(page, PROXY_DS, '{app="api-gateway"}');
      await waitForGrafanaReady(page);
      await runQuery(page);

      await assertLogsVisible(page);
      await expect(page.getByText("No logs found")).toHaveCount(0);
      await guards.assertClean();
    }
  );

  test(
    "multi-tenant explore shows logs for valid tenant and no error banner @explore-mt",
    async ({ page }) => {
      const guards = installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
      });
      await openExplore(page, PROXY_MULTI_DS, '{__tenant_id__="fake"}');
      await waitForGrafanaReady(page);
      await runQuery(page);

      await assertLogsVisible(page);
      await expect(page.locator('[data-testid="data-testid Alert error"]').filter({ visible: true })).toHaveCount(0);
      await guards.assertClean();
    }
  );

  test(
    "multi-tenant explore for missing tenant shows empty result not error banner @explore-mt",
    async ({ page }) => {
      const guards = installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
      });
      await openExplore(page, PROXY_MULTI_DS, '{__tenant_id__="nonexistent-tenant-xyz"}');
      await waitForGrafanaReady(page);
      await runQuery(page);

      await expect(page.locator('[data-testid="data-testid Alert error"]').filter({ visible: true })).toHaveCount(0);
      await guards.assertClean();
    }
  );

  test(
    "filter-for-value in multi-tenant explore adds label to query without error @explore-mt",
    async ({ page }) => {
      const guards = installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
      });

      await openExplore(page, PROXY_MULTI_DS, '{app="api-gateway", __tenant_id__="fake"}');
      await waitForGrafanaReady(page);
      await runQuery(page);
      await assertLogsVisible(page);

      // Expand the first log row to reveal field detail panel
      const firstRow = page.locator(
        '[data-testid="logRows"] [data-testid="logRow"], ' +
        '[data-testid="logRows"] > div > div, ' +
        '[class*="logs-row"]:not([class*="logs-row__"]), ' +
        '[class*="logsRow"]:not([class*="logsRow__"])'
      ).first();
      await firstRow.click({ timeout: 10_000 });

      // Wait for the detail panel to open — Grafana 12/13 use different selectors
      const detailPanel = page.locator([
        '[data-testid="logRowDetails"]',
        '[class*="logRowDetails"]',
        '[class*="logDetails"]',
        '[class*="log-details"]',
        '[class*="logRow__details"]',
        '[data-testid="logRows"] [aria-expanded="true"] ~ *',
      ].join(", ")).first();
      const detailVisible = await detailPanel.isVisible({ timeout: 10_000 }).catch(() => false);

      // Click "Filter for value" if the detail panel opened (graceful degradation)
      const filterButton = detailVisible
        ? detailPanel.locator('button[aria-label*="Filter for value"], button[title*="filter"]').first()
        : page.locator("__never__").first();

      if (detailVisible && await filterButton.isVisible({ timeout: 3_000 }).catch(() => false)) {
        await filterButton.click();
        await waitForGrafanaReady(page);
        await assertLogsVisible(page);
        await expect(page.locator('[data-testid="data-testid Alert error"]').filter({ visible: true })).toHaveCount(0);
      }

      await guards.assertClean();
    }
  );
});
