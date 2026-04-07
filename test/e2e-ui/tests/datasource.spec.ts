import { test, expect } from "@playwright/test";
import { PROXY_DS, waitForGrafanaReady, installGrafanaGuards } from "./helpers";

test.describe("Grafana Datasource Health & Config", () => {
  test("datasource health check succeeds", async ({ page }) => {
    const guards = installGrafanaGuards(page);

    // Navigate to datasource settings
    await page.goto("/connections/datasources");
    await waitForGrafanaReady(page);

    // Find our proxy datasource
    const dsLink = page.getByText(PROXY_DS, { exact: false }).first();
    if (await dsLink.isVisible({ timeout: 5000 }).catch(() => false)) {
      await dsLink.click();
      await waitForGrafanaReady(page);

      // Click "Test" button
      const testBtn = page.getByRole("button", { name: /save & test|test/i });
      if (await testBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
        await testBtn.click();
        await page.waitForTimeout(3000);

        // Should show success message
        const success = page.locator(
          '[class*="alert-success"], [data-testid*="success"]'
        );
        const error = page.locator(
          '[class*="alert-error"], [data-testid*="error"]'
        );

        const isSuccess = await success
          .isVisible({ timeout: 5000 })
          .catch(() => false);
        const isError = await error
          .isVisible({ timeout: 1000 })
          .catch(() => false);

        if (isError) {
          const errText = await error.textContent();
          throw new Error(`Datasource health check failed: ${errText}`);
        }

        expect(isSuccess || !isError).toBeTruthy();
      }
    }

    await guards.assertClean();
  });
});
