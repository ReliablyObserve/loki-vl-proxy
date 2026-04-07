import { test, expect, type Page } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  installGrafanaGuards,
  openLogsDrilldown,
  waitForGrafanaReady,
} from "./helpers";
import { buildServiceDrilldownUrl } from "./url-state";

function serviceCard(page, name: string) {
  return page.getByRole("region", { name }).first();
}

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

async function expectFilterKeyApplied(page: Page, key: string) {
  const chip = page.getByLabel(
    new RegExp(`(Edit|Remove) filter with key ${escapeRegex(key)}`)
  );
  if (await chip.first().isVisible({ timeout: 2_000 }).catch(() => false)) {
    return;
  }

  const inlineKey = page.getByText(new RegExp(`^${escapeRegex(key)}$`, "i")).first();
  await expect(inlineKey).toBeVisible({ timeout: 15_000 });
}

async function addDrilldownFilter(
  page: Page,
  comboName: "Filter by labels" | "Filter by fields",
  key: string,
  value: string
) {
  await page.getByRole("combobox", { name: comboName }).click();
  await selectComboboxOption(page, key);
  await selectComboboxOption(page, "= Equals");
  await typeIntoActiveCombobox(page, value);
  const valueOption = page.getByRole("option", { name: value, exact: true });
  if (await valueOption.isVisible({ timeout: 3_000 }).catch(() => false)) {
    await valueOption.click();
  } else if (
    await page
      .getByRole("option", { name: /Use custom value/ })
      .isVisible({ timeout: 1_000 })
      .catch(() => false)
  ) {
    await page.getByRole("option", { name: /Use custom value/ }).click();
  } else {
    await page.keyboard.press("Enter");
  }
  await page.keyboard.press("Escape");
  await waitForGrafanaReady(page);
  await expectFilterKeyApplied(page, key);
}

function escapeRegex(text: string) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function typeIntoActiveCombobox(page: Page, value: string) {
  const input = page
    .locator('[role="combobox"][aria-expanded="true"] input, input[aria-autocomplete="list"]')
    .last();
  if (await input.isVisible({ timeout: 500 }).catch(() => false)) {
    await input.fill("");
    await input.type(value, { delay: 10 });
    return;
  }
  await page.keyboard.type(value, { delay: 10 });
}

async function selectComboboxOption(page: Page, optionLabel: string) {
  const exactOption = page.getByRole("option", { name: optionLabel, exact: true });
  if (await exactOption.isVisible({ timeout: 1_500 }).catch(() => false)) {
    await exactOption.click();
    return;
  }

  await typeIntoActiveCombobox(page, optionLabel);

  const exactPattern = new RegExp(`^${escapeRegex(optionLabel)}$`, "i");
  const searchableOption = page.getByRole("option", { name: exactPattern }).first();
  if (await searchableOption.isVisible({ timeout: 5_000 }).catch(() => false)) {
    await searchableOption.click();
    return;
  }

  const fuzzyOption = page
    .getByRole("option")
    .filter({ hasText: optionLabel })
    .first();
  await expect(fuzzyOption).toBeVisible({ timeout: 5_000 });
  await fuzzyOption.click();
}

async function openServiceDrilldown(
  page: Page,
  datasource: string,
  serviceName: string,
  view: "logs" | "fields" = "logs"
) {
  const response = await page.request.get(
    `/api/datasources/name/${encodeURIComponent(datasource)}`
  );
  expect(response.ok()).toBeTruthy();
  const body = await response.json();

  await page.goto(buildServiceDrilldownUrl(body.uid, serviceName, view));
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
    await guards.assertClean();
  });

  test("proxy landing page can add and use a cluster breakdown tab @drilldown-core", async ({ page }) => {
    const guards = installGrafanaGuards(page, {
      allowedRequestFailures: [/net::ERR_ABORTED .*\/api\/ds\/query\?ds_type=loki/i],
    });
    const responses = await collectDrilldownResponses(page);
    await openLogsDrilldown(page, PROXY_DS);
    await waitForDrilldownLanding(page);

    const clusterTab = page.getByRole("tab", { name: "cluster" });
    if (!(await clusterTab.isVisible().catch(() => false))) {
      const addLabelTab = page.getByTestId("data-testid Tab Add label");
      await addLabelTab.click();
      const searchInput = page.locator('[role="tooltip"] input');
      if (!(await searchInput.isVisible().catch(() => false))) {
        await addLabelTab.press("Enter");
      }
      await expect(searchInput).toBeVisible({ timeout: 15_000 });
      await searchInput.fill("cluster");
      await page.getByRole("option", { name: "cluster", exact: true }).click();
      await expect(clusterTab).toBeVisible({ timeout: 15_000 });
    }
    await clusterTab.click();
    await expect(clusterTab).toBeVisible({ timeout: 15_000 });

    const volumeResponse = responses
      .filter((r) => String(r.url).includes("/resources/index/volume"))
      .at(-1);
    expect(volumeResponse).toBeTruthy();
    expect(JSON.stringify(volumeResponse?.json)).toContain("us-east-1");
    await guards.assertClean();
  });

  test("proxy drilldown field filter flow works for method @drilldown-core", async ({ page }) => {
    const guards = installGrafanaGuards(page, {
      allowedRequestFailures: [/net::ERR_ABORTED .*\/api\/ds\/query\?ds_type=loki/i],
    });
    await openServiceDrilldown(page, PROXY_DS, "api-gateway", "logs");

    await addDrilldownFilter(page, "Filter by fields", "method", "GET");
    await expect(page.getByText("No logs found")).toHaveCount(0);
    await guards.assertClean();
  });

  test("service drilldown field filter survives reload from URL state @drilldown-core", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page);
    const response = await page.request.get(
      `/api/datasources/name/${encodeURIComponent(PROXY_DS)}`
    );
    expect(response.ok()).toBeTruthy();
    const body = await response.json();

    await page.goto(
      buildServiceDrilldownUrl(body.uid, "api-gateway", "logs", {
        "var-fields": "method|=|GET",
      })
    );
    await waitForDrilldownDetails(page);
    await expectFilterKeyApplied(page, "method");

    await page.reload();
    await waitForDrilldownDetails(page);
    await expectFilterKeyApplied(page, "method");
    await expect(page.getByText("No logs found")).toHaveCount(0);
    await guards.assertClean();
  });

  test("multi-tenant service drilldown keeps cluster label filter working @drilldown-mt", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "logs");

    await addDrilldownFilter(page, "Filter by labels", "cluster", "us-east-1");
    await expect(page.getByText("No logs found")).toHaveCount(0);
    await guards.assertClean();
  });
});
