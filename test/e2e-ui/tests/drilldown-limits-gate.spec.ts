/**
 * Logs Drilldown reads `/loki/api/v1/drilldown-limits` on every service page
 * and hides the Patterns tab when `pattern_ingester_enabled` is false
 * (ServiceScene gates on it). The proxy reports the flag from its own
 * configuration: true only when `-patterns-enabled` AND
 * `-patterns-autodetect-from-queries` are set (docs/patterns.md). These tests
 * lock the UI consequence of that contract per datasource so a change in the
 * gate, in the plugin, or in the payload shape shows up as a CI failure instead
 * of a manual finding.
 */
import { test, expect, type Page } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_PATTERNS_AUTODETECT_DS,
  LOKI_DS,
  resolveDatasourceUid,
  waitForGrafanaReady,
  installGrafanaGuards,
  drilldownLabelFilter,
} from "./helpers";
import { buildServiceDrilldownUrl } from "./url-state";

interface DrilldownLimits {
  pattern_ingester_enabled: boolean;
  version: string;
  limits: Record<string, unknown>;
  [key: string]: unknown;
}

async function drilldownLimits(page: Page, datasource: string) {
  const uid = await resolveDatasourceUid(page, datasource);
  const response = await page.request.get(
    `/api/datasources/uid/${uid}/resources/drilldown-limits`
  );
  expect(response.ok(), `${datasource}: drilldown-limits status`).toBeTruthy();
  const body = (await response.json()) as DrilldownLimits;
  return { uid, body };
}

async function openServicePage(page: Page, uid: string) {
  // The plugin decides on the Patterns tab from this response; wait for it so
  // "tab absent" means "gated", not "not fetched yet".
  const limitsFetched = page.waitForResponse(
    (r) => r.url().includes("/resources/drilldown-limits"),
    { timeout: 30_000 }
  );
  await page.goto(buildServiceDrilldownUrl(uid, "api-gateway", "logs"));
  await limitsFetched;
  await waitForGrafanaReady(page);
  await expect(drilldownLabelFilter(page)).toBeVisible({ timeout: 30_000 });
  await expect(page.getByRole("tab", { name: /^Logs/i }).first()).toBeVisible({
    timeout: 30_000,
  });
}

test.describe("Logs Drilldown — patterns tab follows drilldown-limits", () => {
  test("proxy drilldown-limits payload is a superset of Loki's @drilldown-core", async ({
    page,
  }) => {
    const [{ body: proxy }, { body: loki }] = await Promise.all([
      drilldownLimits(page, PROXY_DS),
      drilldownLimits(page, LOKI_DS),
    ]);
    expect(loki.limits, "loki limits object").toBeDefined();
    expect(proxy.limits, "proxy limits object").toBeDefined();
    for (const key of Object.keys(loki)) {
      expect(proxy, `top-level key ${key}`).toHaveProperty([key]);
    }
    for (const key of Object.keys(loki.limits)) {
      expect(proxy.limits, `limits.${key}`).toHaveProperty([key]);
    }
    expect(typeof proxy.pattern_ingester_enabled).toBe("boolean");
  });

  test("Patterns tab is hidden when the proxy reports pattern_ingester_enabled=false @drilldown-core", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });
    const { uid, body } = await drilldownLimits(page, PROXY_DS);
    expect(
      body.pattern_ingester_enabled,
      "default proxy datasource runs without -patterns-autodetect-from-queries"
    ).toBe(false);

    await openServicePage(page, uid);
    await expect(page.getByRole("tab", { name: /^Patterns/i })).toHaveCount(0);
    await guards.assertClean();
  });

  for (const datasource of [PROXY_PATTERNS_AUTODETECT_DS, LOKI_DS]) {
    test(`Patterns tab is shown when pattern_ingester_enabled=true (${datasource}) @drilldown-core`, async ({
      page,
    }) => {
      const guards = installGrafanaGuards(page, {
        allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
      });
      const { uid, body } = await drilldownLimits(page, datasource);
      expect(body.pattern_ingester_enabled, `${datasource}: flag`).toBe(true);

      await openServicePage(page, uid);
      await expect(page.getByRole("tab", { name: /^Patterns/i }).first()).toBeVisible({
        timeout: 30_000,
      });
      await guards.assertClean();
    });
  }
});
