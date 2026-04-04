import { test, expect } from "@playwright/test";
import {
  LOKI_DS,
  PROXY_DS,
  openLogsDrilldown,
  waitForGrafanaReady,
} from "./helpers";

function serviceCard(page, name: string) {
  return page.getByRole("region", { name }).first();
}

async function collectDrilldownResponses(page) {
  const responses: Record<string, unknown>[] = [];
  page.on("response", async (response) => {
    const url = response.url();
    if (
      url.includes("/resources/index/volume") ||
      url.includes("/resources/detected_fields")
    ) {
      let json: unknown = null;
      try {
        json = await response.json();
      } catch {
        json = null;
      }
      responses.push({ url, status: response.status(), json });
    }
  });
  return responses;
}

test.describe("Grafana Logs Drilldown", () => {
  test("proxy shows service buckets on landing page", async ({ page }) => {
    const responses = await collectDrilldownResponses(page);
    await openLogsDrilldown(page, PROXY_DS);
    await waitForGrafanaReady(page);

    await expect(serviceCard(page, "api-gateway").getByRole("heading", { name: "api-gateway" })).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("of 0")).toHaveCount(0);

    const volumeResponse = responses.find((r) =>
      String(r.url).includes("/resources/index/volume")
    );
    expect(volumeResponse).toBeTruthy();
    expect(volumeResponse?.status).toBe(200);
    expect(JSON.stringify(volumeResponse?.json)).toContain("api-gateway");
  });

  test("proxy drilldown shows logs and parsed fields for api-gateway", async ({
    page,
  }) => {
    const responses = await collectDrilldownResponses(page);
    await openLogsDrilldown(page, PROXY_DS);
    await waitForGrafanaReady(page);

    await serviceCard(page, "api-gateway").getByRole("link", { name: "Show logs" }).click();
    await page.waitForLoadState("networkidle");

    await expect(page.getByText("No logs found")).toHaveCount(0);
    await expect(page.getByRole("tab", { name: /Fields\d+/ })).toBeVisible();
    await page.getByRole("tab", { name: /Fields\d+/ }).click();
    await expect(page.getByText("method", { exact: true })).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("status", { exact: true })).toBeVisible();
    await expect(page.getByText("app", { exact: true })).toHaveCount(0);

    const detectedFieldsResponse = responses.find((r) =>
      String(r.url).includes("/resources/detected_fields")
    );
    expect(detectedFieldsResponse).toBeTruthy();
    expect(detectedFieldsResponse?.status).toBe(200);
    const detectedFieldsBody = JSON.stringify(detectedFieldsResponse?.json);
    expect(detectedFieldsBody).toContain("method");
    expect(detectedFieldsBody).toContain("status");
    expect(detectedFieldsBody).not.toContain('"label":"app"');
  });

  test("proxy service drilldown exposes label and field tabs", async ({ page }) => {
    await openLogsDrilldown(page, PROXY_DS);
    await waitForGrafanaReady(page);

    await serviceCard(page, "api-gateway").getByRole("link", { name: "Show logs" }).click();
    await page.waitForLoadState("networkidle");

    await expect(page.getByRole("tab", { name: /Labels\d+/ })).toBeVisible();
    await expect(page.getByRole("tab", { name: /Fields\d+/ })).toBeVisible();
    await expect(page.getByText("All levels", { exact: false })).toBeVisible();
    await page.getByRole("tab", { name: /Fields\d+/ }).click();
    await expect(page.getByText("method", { exact: true })).toBeVisible();
    await expect(page.getByText("path", { exact: true })).toBeVisible();
    await expect(page.getByText("status", { exact: true })).toBeVisible();
    await expect(page.getByText("duration_ms_extracted", { exact: true })).toHaveCount(0);
    await expect(page.getByText("path_extracted", { exact: true })).toHaveCount(0);
  });

  test("proxy and direct Loki both expose api-gateway in logs drilldown", async ({
    page,
  }) => {
    await openLogsDrilldown(page, LOKI_DS);
    await waitForGrafanaReady(page);
    await expect(serviceCard(page, "api-gateway").getByRole("heading", { name: "api-gateway" })).toBeVisible({
      timeout: 15_000,
    });

    await openLogsDrilldown(page, PROXY_DS);
    await waitForGrafanaReady(page);
    await expect(serviceCard(page, "api-gateway").getByRole("heading", { name: "api-gateway" })).toBeVisible({
      timeout: 15_000,
    });
  });

  test("proxy landing page add-label flow offers secondary labels", async ({ page }) => {
    await openLogsDrilldown(page, PROXY_DS);
    await waitForGrafanaReady(page);

    await page.getByText("Add label", { exact: true }).click();
    await expect(page.getByText("Search labels", { exact: true })).toBeVisible({
      timeout: 10_000,
    });

    const search = page.locator('input[placeholder="Search labels"]').first();
    await search.click();
    await page.getByText("cluster", { exact: true }).click();

    await expect(page.getByText("cluster", { exact: true })).toBeVisible({
      timeout: 10_000,
    });
  });

  test("proxy drilldown exposes dotted OTel structured metadata fields", async ({ page }) => {
    const responses = await collectDrilldownResponses(page);
    await openLogsDrilldown(page, PROXY_DS);
    await waitForGrafanaReady(page);

    await serviceCard(page, "otel-auth-service").getByRole("link", { name: "Show logs" }).click();
    await page.waitForLoadState("networkidle");

    await page.getByRole("tab", { name: /Fields\d+/ }).click();
    await expect(page.getByText("service.name", { exact: true })).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("service.namespace", { exact: true })).toBeVisible();
    await expect(page.getByText("k8s.pod.name", { exact: true })).toBeVisible();
    await expect(page.getByText("deployment.environment", { exact: true })).toBeVisible();

    const detectedFieldsResponse = responses.find((r) =>
      String(r.url).includes("/resources/detected_fields")
    );
    expect(detectedFieldsResponse).toBeTruthy();
    expect(JSON.stringify(detectedFieldsResponse?.json)).toContain("service.name");
    expect(JSON.stringify(detectedFieldsResponse?.json)).not.toContain('"label":"service_name"');
  });
});
