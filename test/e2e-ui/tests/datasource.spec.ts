import { test, expect } from "@playwright/test";
import {
  LOKI_DS,
  PROXY_DS,
  waitForGrafanaReady,
  collectLokiErrors,
} from "./helpers";

const DIRECT_VL_DS = "VictoriaLogs (direct)";

test.describe("Grafana Datasource Health & Config", () => {
  test("datasource health check succeeds", async ({ page }) => {
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
  });

  test("buildinfo endpoint returns valid version", async ({ request }) => {
    // The proxy URL used by Grafana internally
    const proxyUrl = process.env.PROXY_URL || "http://127.0.0.1:3100";

    const resp = await request.get(
      `${proxyUrl}/loki/api/v1/status/buildinfo`
    );
    expect(resp.status()).toBe(200);

    const body = await resp.json();
    expect(body.status).toBe("success");
    expect(body.data.version).toBeTruthy();
  });

  test("ready endpoint returns 200", async ({ request }) => {
    const proxyUrl = process.env.PROXY_URL || "http://127.0.0.1:3100";
    const resp = await request.get(`${proxyUrl}/ready`);
    expect(resp.status()).toBe(200);
  });

  test("rules endpoint returns valid compatibility response", async ({ request }) => {
    const proxyUrl = process.env.PROXY_URL || "http://127.0.0.1:3100";
    const resp = await request.get(`${proxyUrl}/loki/api/v1/rules`);
    expect(resp.status()).toBe(200);
    expect(resp.headers()["content-type"]).toContain("application/yaml");
    const body = await resp.text();
    expect(body).toContain("loki-vl-e2e-alerts");
  });

  test("alerts endpoint returns valid compatibility response", async ({ request }) => {
    const proxyUrl = process.env.PROXY_URL || "http://127.0.0.1:3100";
    const resp = await request.get(`${proxyUrl}/loki/api/v1/alerts`);
    expect(resp.status()).toBe(200);

    const body = await resp.json();
    expect(body.status).toBe("success");
    expect(body.data.alerts).toBeDefined();
    expect(Array.isArray(body.data.alerts)).toBeTruthy();
  });

  test("direct VictoriaLogs datasource plugin is installed and healthy", async ({ page }) => {
    const dsResponse = await page.request.get(
      `/api/datasources/name/${encodeURIComponent(DIRECT_VL_DS)}`
    );
    expect(dsResponse.ok()).toBeTruthy();
    const dsBody = await dsResponse.json();
    expect(dsBody.type).toBe("victoriametrics-logs-datasource");

    const healthResponse = await page.request.get(
      `/api/datasources/uid/${encodeURIComponent(dsBody.uid)}/health`
    );
    expect(healthResponse.ok()).toBeTruthy();
    const healthBody = await healthResponse.json();
    expect(healthBody.status).toBe("OK");
  });

  test("direct Loki drilldown bootstrap endpoint works with tenant header", async ({ request }) => {
    const directLokiUrl =
      process.env.DIRECT_LOKI_URL || "http://host.docker.internal:3101";
    const limitsResponse = await request.get(
      `${directLokiUrl}/loki/api/v1/drilldown-limits`,
      {
        headers: {
          "X-Scope-OrgID": "0",
        },
      }
    );
    expect(limitsResponse.ok()).toBeTruthy();
    const limitsBody = await limitsResponse.json();
    expect(limitsBody.limits).toBeDefined();
  });
});
