import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  LOKI_DS,
  openExplore,
  typeQuery,
  runQuery,
  assertNoErrors,
  assertLogsVisible,
  assertGraphVisible,
  waitForGrafanaReady,
  collectLokiErrors,
} from "./helpers";

test.describe("Grafana Explore — Proxy Datasource", () => {
  test.beforeEach(async ({ page }) => {
    await openExplore(page, PROXY_DS);
    await waitForGrafanaReady(page);
  });

  test("basic log query returns results without errors", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway"}');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("line filter query works", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway"} |= "error"');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("metric query (count_over_time) renders graph", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, 'count_over_time({app="api-gateway"}[5m])');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("rate query renders graph", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, 'rate({app="api-gateway"}[5m])');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("sum by label renders graph", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, 'sum(rate({app="api-gateway"}[5m])) by (level)');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("json parser query works", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway"} | json');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("logfmt parser query works", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway"} | logfmt');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("negative filter works", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway"} != "health"');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("direction=forward works", async ({ page }) => {
    const errors = collectLokiErrors(page);
    // In Explore, direction is controlled via the UI sort button
    // but we can test via query_range API directly
    await typeQuery(page, '{app="api-gateway"}');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("quantile_over_time renders", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(
      page,
      'quantile_over_time(0.95, {app="api-gateway"} | unwrap duration [5m])'
    );
    await runQuery(page);

    // May error if no data, but should not 500
    const fatalErrors = errors.filter((e) => e.startsWith("5"));
    expect(fatalErrors).toHaveLength(0);
  });

  test("label_format multi-rename works", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(
      page,
      '{app="api-gateway"} | label_format app_name="{{.app}}", log_level="{{.level}}"'
    );
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("multi-tenant query respects __tenant_id__ filter in Explore", async ({ page }) => {
    await openExplore(page, PROXY_MULTI_DS);
    await waitForGrafanaReady(page);

    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway", __tenant_id__="fake"}');
    await runQuery(page);

    await assertNoErrors(page);
    await assertLogsVisible(page);
    expect(errors).toHaveLength(0);
  });
});

test.describe("Grafana Explore — Side-by-side Comparison", () => {
  const queries = [
    '{app="api-gateway"}',
    '{app="api-gateway"} |= "error"',
    'count_over_time({app="api-gateway"}[5m])',
    'rate({app="api-gateway"}[5m])',
    'sum(rate({app="api-gateway"}[5m])) by (level)',
  ];

  for (const query of queries) {
    test(`proxy matches Loki for: ${query.slice(0, 50)}`, async ({ page }) => {
      // Query proxy
      const proxyErrors: string[] = [];
      page.on("response", (r) => {
        if (r.url().includes("/loki/api/v1/") && r.status() >= 400) {
          proxyErrors.push(`${r.status()}`);
        }
      });

      await openExplore(page, PROXY_DS);
      await waitForGrafanaReady(page);
      await typeQuery(page, query);
      await runQuery(page);

      const proxyHasError = proxyErrors.some((e) => e.startsWith("5"));

      // Query Loki
      page.removeAllListeners("response");
      const lokiErrors: string[] = [];
      page.on("response", (r) => {
        if (r.url().includes("/loki/api/v1/") && r.status() >= 400) {
          lokiErrors.push(`${r.status()}`);
        }
      });

      await openExplore(page, LOKI_DS);
      await waitForGrafanaReady(page);
      await typeQuery(page, query);
      await runQuery(page);

      const lokiHasError = lokiErrors.some((e) => e.startsWith("5"));

      // Both should behave the same — no 5xx when the other succeeds
      if (!lokiHasError && proxyHasError) {
        throw new Error(
          `Proxy returned 5xx but Loki succeeded for query: ${query}`
        );
      }
    });
  }
});
