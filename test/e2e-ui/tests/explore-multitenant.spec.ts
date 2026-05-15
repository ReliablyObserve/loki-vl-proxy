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
      await expect(page.locator('[data-testid="data-testid Alert error"]')).toHaveCount(0);
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

      await expect(page.locator('[data-testid="data-testid Alert error"]')).toHaveCount(0);
      await guards.assertClean();
    }
  );
});
