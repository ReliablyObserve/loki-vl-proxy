import { test, expect, type Page } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  installGrafanaGuards,
  openLogsDrilldown,
  resolveDatasourceUid,
  waitForGrafanaReady,
} from "./helpers";
import { buildServiceDrilldownUrl } from "./url-state";

async function waitForDrilldownLanding(page: Page) {
  await waitForGrafanaReady(page);
  await expect(page.getByRole("combobox", { name: "Filter by labels" })).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByRole("tab", { name: "service" })).toBeVisible({
    timeout: 30_000,
  });
}

async function waitForDrilldownDetails(page: Page) {
  await waitForGrafanaReady(page);
  await expect(page.getByRole("combobox", { name: "Filter by labels" })).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByRole("combobox", { name: "Filter by fields" })).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByRole("tab", { name: /Logs\d+/ })).toBeVisible({
    timeout: 30_000,
  });
}

async function expectFilterApplied(
  page: Page,
  comboName: "Filter by labels" | "Filter by fields",
  key: string,
  value?: string
) {
  const paramName = comboName === "Filter by labels" ? "var-filters" : "var-fields";
  if (value) {
    await expect
      .poll(() => new URL(page.url(), "http://localhost").searchParams.get(paramName) ?? "", {
        timeout: 15_000,
      })
      .toContain(`${key}|=|${value}`);
  }

  const chip = page.getByLabel(
    new RegExp(`(Edit|Remove) filter with key ${escapeRegex(key)}`)
  );
  if (await chip.first().isVisible({ timeout: 2_000 }).catch(() => false)) {
    return;
  }
}

function escapeRegex(text: string) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function openServiceDrilldown(
  page: Page,
  datasource: string,
  serviceName: string,
  view: "logs" | "fields" = "logs"
) {
  const uid = await resolveDatasourceUid(page, datasource);
  await page.goto(buildServiceDrilldownUrl(uid, serviceName, view));
  await waitForDrilldownDetails(page);
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
  test("proxy shows service buckets on landing page @drilldown-core", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    const responses = await collectDrilldownResponses(page);
    await openLogsDrilldown(page, PROXY_DS);
    await waitForDrilldownLanding(page);

    const volumeResponse = responses.find((r) =>
      String(r.url).includes("/resources/index/volume")
    );
    expect(volumeResponse).toBeTruthy();
    expect(volumeResponse?.status).toBe(200);
    expect(JSON.stringify(volumeResponse?.json)).toContain('"resultType":"vector"');
    await guards.assertClean();
  });

  test("service drilldown field filter survives reload from URL state @drilldown-core", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page);
    const uid = await resolveDatasourceUid(page, PROXY_DS);

    await page.goto(
      buildServiceDrilldownUrl(uid, "api-gateway", "logs", {
        "var-fields": "method|=|GET",
      })
    );
    await waitForDrilldownDetails(page);
    await expectFilterApplied(page, "Filter by fields", "method", "GET");

    await page.reload();
    await waitForDrilldownDetails(page);
    await expectFilterApplied(page, "Filter by fields", "method", "GET");
    await expect(page.getByText("No logs found")).toHaveCount(0);
    await guards.assertClean();
  });

  test("multi-tenant service drilldown loads without browser errors @drilldown-mt", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "logs");
    await expect(page.getByText("No logs found")).toHaveCount(0);
    await guards.assertClean();
  });
});
