import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  LOKI_DS,
  openExplore,
  typeQuery,
  runQuery,
  assertLogsVisible,
  assertGraphVisible,
  installGrafanaGuards,
} from "./helpers";

test.describe("Grafana Explore — Loki Operations Parity", () => {
  test("json parser produces log results @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(page, PROXY_DS, '{app="api-gateway"} | json');
    await runQuery(page);
    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("logfmt parser produces log results @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(page, PROXY_DS, '{app="payment-service"} | logfmt');
    await runQuery(page);
    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("json parser with field filter @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(page, PROXY_DS, '{app="api-gateway"} | json | method="GET"');
    await runQuery(page);
    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("line_format renders formatted output @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | line_format "{{.method}} {{.path}} {{.status}}"'
    );
    await runQuery(page);
    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("label_format adds labels @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | label_format short_app="gateway"'
    );
    await runQuery(page);
    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("drop and keep pipeline stages @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(
      page,
      PROXY_DS,
      '{app="api-gateway"} | json | keep method, status'
    );
    await runQuery(page);
    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("metric count_over_time renders graph @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(
      page,
      PROXY_DS,
      'sum by (level) (count_over_time({app="api-gateway"}[5m]))'
    );
    await runQuery(page);
    await assertGraphVisible(page);
    await guards.assertClean();
  });

  test("metric rate renders graph @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(page, PROXY_DS, 'rate({app="api-gateway"}[5m])');
    await runQuery(page);
    await assertGraphVisible(page);
    await guards.assertClean();
  });

  test("unwrap metric renders graph @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(
      page,
      PROXY_DS,
      'avg_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])'
    );
    await runQuery(page);
    await assertGraphVisible(page);
    await guards.assertClean();
  });

  test("line filter with regex @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(page, PROXY_DS, '{app="api-gateway"} |~ "GET|POST"');
    await runQuery(page);
    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("negative line filter @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(page, PROXY_DS, '{app="api-gateway"} != "timeout"');
    await runQuery(page);
    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("topk aggregation renders graph @explore-ops", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(
      page,
      PROXY_DS,
      'topk(3, sum by (app) (count_over_time({namespace="prod"}[5m])))'
    );
    await runQuery(page);
    await assertGraphVisible(page);
    await guards.assertClean();
  });
});
