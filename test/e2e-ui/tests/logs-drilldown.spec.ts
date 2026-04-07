import { test, expect, type Page } from "@playwright/test";
import {
  LOKI_DS,
  PROXY_DS,
  PROXY_MULTI_DS,
  assertNoErrors,
  collectLokiErrors,
  openLogsDrilldown,
  waitForGrafanaReady,
} from "./helpers";

function serviceCard(page, name: string) {
  return page.getByRole("region", { name }).first();
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

  const params = new URLSearchParams({
    patterns: "[]",
    from: "now-2h",
    to: "now",
    timezone: "browser",
    "var-lineFormat": "",
    "var-ds": body.uid,
    "var-filters": `service_name|=|${serviceName}`,
    "var-fields": "",
    "var-levels": "",
    "var-metadata": "",
    "var-jsonFields": "",
    "var-all-fields": "",
    "var-patterns": "",
    "var-lineFilterV2": "",
    "var-lineFilters": "",
    displayedFields: "[]",
    urlColumns: "[]",
  });

  await page.goto(
    `/a/grafana-lokiexplore-app/explore/service/${encodeURIComponent(serviceName)}/${view}?${params.toString()}`
  );
  await waitForGrafanaReady(page);
}

async function openLabelDrilldown(
  page: Page,
  datasource: string,
  label: string,
  value: string,
  levels: string[] = []
) {
  const response = await page.request.get(
    `/api/datasources/name/${encodeURIComponent(datasource)}`
  );
  expect(response.ok()).toBeTruthy();
  const body = await response.json();

  const params = new URLSearchParams({
    patterns: "[]",
    from: "now-3h",
    to: "now",
    timezone: "browser",
    "var-lineFormat": "",
    "var-ds": body.uid,
    "var-filters": `${label}|=|${value}`,
    "var-fields": "",
    "var-metadata": "",
    "var-jsonFields": "",
    "var-all-fields": "",
    "var-patterns": "",
    "var-lineFilterV2": "",
    "var-lineFilters": "",
    displayedFields: "[]",
    urlColumns: "[]",
    visualizationType: `"logs"`,
    prettifyLogMessage: "false",
    userDisplayedFields: "false",
    wrapLogMessage: "false",
    sortOrder: `"Descending"`,
  });
  for (const level of levels) {
    params.append("var-levels", `detected_level|=|${level}`);
  }

  await page.goto(
    `/a/grafana-lokiexplore-app/explore/${encodeURIComponent(label)}/${encodeURIComponent(value)}/logs?${params.toString()}`
  );
  await waitForGrafanaReady(page);
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

  test("proxy drilldown shows logs and parsed fields for api-gateway @drilldown-core", async ({
    page,
  }) => {
    const responses = await collectDrilldownResponses(page);
    await openServiceDrilldown(page, PROXY_DS, "api-gateway", "logs");

    await expect(page.getByText("No logs found")).toHaveCount(0);
    await expect(page.getByRole("tab", { name: /Fields\d+/ })).toBeVisible();
    await page.getByRole("tab", { name: /Fields\d+/ }).click();
    await waitForGrafanaReady(page);
    await expect(page.getByText("duration_ms", { exact: true })).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.getByText("path", { exact: true })).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.getByText("status", { exact: true })).toBeVisible({
      timeout: 30_000,
    });
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

  test("proxy service drilldown exposes label and field tabs @drilldown-core", async ({ page }) => {
    await openServiceDrilldown(page, PROXY_DS, "api-gateway", "logs");

    await expect(page.getByRole("tab", { name: /Labels\d+/ })).toBeVisible();
    await expect(page.getByRole("tab", { name: /Fields\d+/ })).toBeVisible();
    await expect(page.getByText("All levels", { exact: false })).toBeVisible();
    await page.getByRole("tab", { name: /Fields\d+/ }).click();
    await waitForGrafanaReady(page);
    await expect(page.getByText("duration_ms", { exact: true })).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.getByText("path", { exact: true })).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.getByText("status", { exact: true })).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.getByText("duration_ms_extracted", { exact: true })).toHaveCount(0);
    await expect(page.getByText("path_extracted", { exact: true })).toHaveCount(0);
  });

  test("proxy and direct Loki both expose api-gateway in logs drilldown @drilldown-core", async ({
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

  test("proxy landing page can add and use a cluster breakdown tab @drilldown-core", async ({ page }) => {
    const responses = await collectDrilldownResponses(page);
    await openLogsDrilldown(page, PROXY_DS);
    await waitForGrafanaReady(page);

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
    await expect(page.getByText("of 2")).toBeVisible({ timeout: 15_000 });

    const volumeResponse = responses
      .filter((r) => String(r.url).includes("/resources/index/volume"))
      .at(-1);
    expect(volumeResponse).toBeTruthy();
    expect(JSON.stringify(volumeResponse?.json)).toContain("us-east-1");
  });

  test("proxy drilldown exposes dotted OTel structured metadata fields @drilldown-core", async ({ page }) => {
    const responses = await collectDrilldownResponses(page);
    await openServiceDrilldown(page, PROXY_DS, "otel-auth-service", "fields");

    await page.getByRole("combobox", { name: "Filter by fields" }).click();
    await expect(page.getByRole("option", { name: "service.name", exact: true })).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.getByRole("option", { name: "k8s.pod.name", exact: true })).toBeVisible();
    await expect(page.getByRole("option", { name: "deployment.environment", exact: true })).toBeVisible();
    await page.keyboard.press("Escape");

    const detectedFieldsResponse = responses.find((r) =>
      String(r.url).includes("/resources/detected_fields")
    );
    expect(detectedFieldsResponse).toBeTruthy();
    const detectedFieldsBody = JSON.stringify(detectedFieldsResponse?.json);
    expect(detectedFieldsBody).toContain("service.name");
    expect(detectedFieldsBody).toContain("service.namespace");
    expect(detectedFieldsBody).toContain('"label":"service_name"');
  });

  test("proxy drilldown label filter flow works for cluster @drilldown-core", async ({ page }) => {
    await openServiceDrilldown(page, PROXY_DS, "api-gateway", "logs");

    await addDrilldownFilter(page, "Filter by labels", "cluster", "us-east-1");
    await expect(page.getByLabel("Remove filter with key cluster")).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("No logs found")).toHaveCount(0);
  });

  test("proxy drilldown field filter flow works for method @drilldown-core", async ({ page }) => {
    await openServiceDrilldown(page, PROXY_DS, "api-gateway", "logs");

    await addDrilldownFilter(page, "Filter by fields", "method", "GET");
    await expect(page.getByLabel("Remove filter with key method")).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("No logs found")).toHaveCount(0);
  });

  test("proxy drilldown cluster logs handle multiple selected levels @drilldown-mt", async ({ page }) => {
    const errors = collectLokiErrors(page);
    const datasourceErrors: string[] = [];
    page.on("response", (response) => {
      if (response.url().includes("/api/ds/query") && response.status() >= 400) {
        datasourceErrors.push(`${response.status()} ${response.url()}`);
      }
    });
    await openLabelDrilldown(page, PROXY_MULTI_DS, "cluster", "us-east-1", [
      "error",
      "info",
      "warn",
    ]);

    await assertNoErrors(page);
    await expect(page.getByText("No logs found")).toHaveCount(0);
    expect(errors).toHaveLength(0);
    expect(datasourceErrors).toHaveLength(0);
  });

  test("multi-tenant service drilldown keeps cluster label filter working @drilldown-mt", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "logs");

    await addDrilldownFilter(page, "Filter by labels", "cluster", "us-east-1");
    await expect(page.getByLabel("Remove filter with key cluster")).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("No logs found")).toHaveCount(0);
    expect(errors).toHaveLength(0);
  });

  test("multi-tenant service drilldown keeps method field filter working @drilldown-mt", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "logs");

    await addDrilldownFilter(page, "Filter by fields", "method", "GET");
    await expect(page.getByLabel("Remove filter with key method")).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("No logs found")).toHaveCount(0);
    expect(errors).toHaveLength(0);
  });

  test("multi-tenant service drilldown supports combined label and field filters @drilldown-mt", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "logs");

    await addDrilldownFilter(page, "Filter by labels", "cluster", "us-east-1");
    await addDrilldownFilter(page, "Filter by fields", "method", "GET");
    await expect(page.getByLabel("Remove filter with key cluster")).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByLabel("Remove filter with key method")).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("No logs found")).toHaveCount(0);
    expect(errors).toHaveLength(0);
  });

  test("multi-tenant landing page can add and use a cluster breakdown tab @drilldown-mt", async ({ page }) => {
    const responses = await collectDrilldownResponses(page);
    const errors = collectLokiErrors(page);
    await openLogsDrilldown(page, PROXY_MULTI_DS);
    await waitForGrafanaReady(page);

    await expect(serviceCard(page, "api-gateway").getByRole("heading", { name: "api-gateway" })).toBeVisible({
      timeout: 15_000,
    });

    await addDrilldownFilter(page, "Filter by labels", "cluster", "us-east-1");
    await expect(page.getByLabel("Remove filter with key cluster")).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("No logs found")).toHaveCount(0);
    expect(errors).toHaveLength(0);

    const volumeResponse = responses.find((r) =>
      String(r.url).includes("/resources/index/volume")
    );
    expect(volumeResponse).toBeTruthy();
    expect(JSON.stringify(volumeResponse?.json)).toContain("us-east-1");
  });

  test("multi-tenant cluster drilldown keeps multiple selected levels working @drilldown-mt", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await openLabelDrilldown(page, PROXY_MULTI_DS, "cluster", "us-east-1", [
      "error",
      "info",
      "warn",
    ]);

    await expect(page.getByText("No logs found")).toHaveCount(0);
    await expect(page.getByText(/Log volume/i)).toBeVisible({ timeout: 15_000 });
    expect(errors).toHaveLength(0);
  });

  test("multi-tenant cluster drilldown supports follow-up field filtering @drilldown-mt", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await openLabelDrilldown(page, PROXY_MULTI_DS, "cluster", "us-east-1", [
      "error",
      "info",
      "warn",
    ]);

    await addDrilldownFilter(page, "Filter by fields", "method", "GET");
    await expect(page.getByLabel("Remove filter with key method")).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByText("No logs found")).toHaveCount(0);
    expect(errors).toHaveLength(0);
  });
});
