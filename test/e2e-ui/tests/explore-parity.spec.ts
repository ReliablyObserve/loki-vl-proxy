/**
 * API-level parity for the Explore split-view comparison that was verified by
 * hand (proxy vs Loki direct, same metric query, same series). Runs through
 * Grafana's datasource proxy so it exercises the same path Explore uses, but
 * compares responses instead of pixels.
 */
import { test, expect, type Page } from "@playwright/test";
import {
  PROXY_DS,
  LOKI_DS,
  resolveDatasourceUid,
  waitForLokiMetricData,
} from "./helpers";

interface MatrixResponse {
  status: string;
  data: { resultType: string; result: Array<{ metric: Record<string, string>; values: unknown[] }> };
}

// Exclude the live edge so both backends see a complete window.
const LIVE_EDGE_SEC = 60;
const WINDOW_SEC = 15 * 60;

async function queryRange(page: Page, uid: string, query: string, endSec: number) {
  const params = new URLSearchParams({
    query,
    start: String(endSec - WINDOW_SEC),
    end: String(endSec),
    step: "60",
  });
  const response = await page.request.get(
    `/api/datasources/proxy/uid/${uid}/loki/api/v1/query_range?${params}`
  );
  expect(response.status(), `${uid}: query_range status`).toBe(200);
  return { status: response.status(), body: (await response.json()) as MatrixResponse };
}

test.describe("Explore — proxy vs Loki metric parity", () => {
  test("sum by (level) rate returns the same series set on both datasources @explore-core", async ({
    page,
  }) => {
    const [proxyUID, lokiUID] = await Promise.all([
      resolveDatasourceUid(page, PROXY_DS),
      resolveDatasourceUid(page, LOKI_DS),
    ]);
    // Loki's range-metric path is blank for the first minutes of a fresh
    // stack; the proxy is not. Compare only once Loki can answer.
    await waitForLokiMetricData(page, lokiUID, '{app="api-gateway"}', {
      endOffsetSec: LIVE_EDGE_SEC,
    });
    const query = `sum by (level) (rate({app="api-gateway"}[5m]))`;
    // One shared window end: the generator creates streams continuously.
    const endSec = Math.floor(Date.now() / 1000) - LIVE_EDGE_SEC;
    const [proxy, loki] = await Promise.all([
      queryRange(page, proxyUID, query, endSec),
      queryRange(page, lokiUID, query, endSec),
    ]);

    expect(proxy.body.data.resultType).toBe("matrix");
    expect(loki.body.data.resultType).toBe("matrix");

    const levels = (r: MatrixResponse) =>
      r.data.result.map((s) => s.metric.level ?? "").sort();
    expect(levels(proxy.body), "series label set").toEqual(levels(loki.body));
    expect(levels(proxy.body).length, "at least one level series").toBeGreaterThan(0);

    for (const series of proxy.body.data.result) {
      expect(series.values.length, `proxy series ${series.metric.level} has points`).toBeGreaterThan(0);
    }
  });
});
