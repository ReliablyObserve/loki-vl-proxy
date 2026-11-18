import { test, expect, type Page } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_PATTERNS_AUTODETECT_DS,
  resolveDatasourceUid,
  waitForDrilldownApp,
  waitForGrafanaReady,
} from "./helpers";
import { buildExploreUrl, buildLogsDrilldownUrl, buildServiceDrilldownUrl } from "./url-state";

// The proxy's client detection on real Grafana traffic: every request Grafana sends from Logs Drilldown and from
// Explore reaches the proxy with its typed client profile (internal/clientprofile), read back from the proxy's
// /debug/queries callers view (the request log is sampled; the view keeps the last 256 requests whole).
//
// Captured on the wire (internal/clientprofile/testdata/captured-*.json): Drilldown tags every request, its resource
// calls included, Source=grafana-lokiexplore-app; Explore tags its query grafana-explore and its log volume query
// logvolhist, and its resource calls carry the Explore page as X-Grafana-Referer; Grafana's User-Agent names
// Grafana's version and, from 13.x, its Loki datasource plugin's (loki/<version>); no request names the Drilldown
// app's version.

const ADMIN_TOKEN = process.env.PROXY_ADMIN_TOKEN || "e2e-test-token";
const DRILLDOWN_PROXY_URL = process.env.PROXY_PATTERNS_URL || "http://127.0.0.1:13110";
const EXPLORE_PROXY_URL = process.env.PROXY_UNDERSCORE_URL || "http://127.0.0.1:13102";
const VERSION = /^\d{1,4}\.\d{1,4}\.\d{1,4}$/;

interface RecentRequest {
  time: number;
  endpoint: string;
  route: string;
  status: number;
  grafana_source?: string;
  surface?: string;
  grafana_version?: string;
  datasource_version?: string;
  drilldown_version?: string;
}

async function recentRequests(page: Page, base: string, since: number): Promise<RecentRequest[]> {
  const resp = await page.request.get(`${base}/debug/queries`, { headers: { "X-Admin-Token": ADMIN_TOKEN } });
  expect(resp.status(), `${base}/debug/queries`).toBe(200);
  const body = (await resp.json()) as { callers?: { recent_requests?: RecentRequest[] } };
  return (body.callers?.recent_requests ?? []).filter((r) => r.time >= since);
}

function expectVersions(r: RecentRequest) {
  expect(r.grafana_version ?? "", `Grafana version of ${r.route}`).toMatch(VERSION);
  if (r.datasource_version) {
    expect(r.datasource_version, `datasource version of ${r.route}`).toMatch(VERSION);
  }
  expect(r.drilldown_version, `no header names the Drilldown version (${r.route})`).toBeUndefined();
}

test.describe("client detection on Grafana traffic", () => {
  test("every Logs Drilldown request is recognised as Drilldown @drilldown-core @cov:drilldown/client-detection-captured-header-sets", async ({
    page,
  }) => {
    test.setTimeout(240_000);
    await waitForDrilldownApp(page);
    const uid = await resolveDatasourceUid(page, PROXY_PATTERNS_AUTODETECT_DS);
    const since = Date.now() - 2_000; // the proxy's clock, in the stack's VM, may lag the runner's a little
    await page.goto(buildLogsDrilldownUrl(uid));
    await waitForGrafanaReady(page);
    await page.waitForTimeout(3_000);
    for (const view of ["logs", "fields"] as const) {
      await page.goto(buildServiceDrilldownUrl(uid, "api-gateway", view));
      await waitForGrafanaReady(page);
      await page.waitForTimeout(3_000);
    }

    const recent = await recentRequests(page, DRILLDOWN_PROXY_URL, since);
    const drilldown = recent.filter((r) => r.grafana_source === "grafana-lokiexplore-app");
    const routes = new Set(drilldown.map((r) => r.route));
    // Queries and the resource calls of the landing and service pages.
    for (const route of ["/loki/api/v1/query_range", "/loki/api/v1/index/volume", "/loki/api/v1/detected_labels", "/loki/api/v1/detected_fields"]) {
      expect(routes, `Drilldown requests seen: ${[...routes].join(", ")}`).toContain(route);
    }
    for (const r of drilldown) {
      expect(r.surface, `${r.route} from Drilldown`).toBe("drilldown");
      expectVersions(r);
    }
  });

  test("Explore requests are Explore, never Drilldown @explore-core @cov:drilldown/client-detection-captured-header-sets", async ({
    page,
  }) => {
    test.setTimeout(180_000);
    const uid = await resolveDatasourceUid(page, PROXY_DS);
    const since = Date.now() - 2_000;
    await page.goto(buildExploreUrl(uid, '{service_name=~".+"}', { from: "now-1h", to: "now" }));
    await page.waitForTimeout(8_000);
    await page.goto(buildExploreUrl(uid, 'sum by (level) (count_over_time({service_name=~".+"}[1m]))', { from: "now-1h", to: "now" }));
    await page.waitForTimeout(8_000);

    const recent = await recentRequests(page, EXPLORE_PROXY_URL, since);
    const explore = recent.filter((r) => r.grafana_source === "grafana-explore" || r.grafana_source === "logvolhist");
    expect(explore.length, "Explore queries seen").toBeGreaterThan(0);
    for (const r of explore) {
      expect(r.surface, `${r.route} (${r.grafana_source}) from Explore`).toBe("explore");
      expectVersions(r);
    }
    // Explore's resource calls (labels, index/stats) carry no Source tag; the page they come from makes them Explore.
    // Other specs may use this proxy at the same time, so only the presence of such a call is asserted.
    const labels = recent.filter((r) => !r.grafana_source && r.route === "/loki/api/v1/labels");
    expect(labels.map((r) => r.surface), "Explore's label browser calls").toContain("explore");
    // No request without Drilldown's tag reads as Drilldown.
    for (const r of recent.filter((r) => r.grafana_source !== "grafana-lokiexplore-app")) {
      expect(r.surface, `${r.route} (${r.grafana_source ?? "no tag"}) must not read as Drilldown`).not.toBe("drilldown");
    }
  });
});
