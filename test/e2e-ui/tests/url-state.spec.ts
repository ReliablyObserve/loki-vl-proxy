import { test, expect } from "@playwright/test";
import {
  buildExploreUrl,
  buildLogsDrilldownUrl,
  buildServiceDrilldownUrl,
  DRILLDOWN_URL,
  EXPLORE_URL,
} from "./url-state";

test("buildExploreUrl encodes the datasource pane state @explore-core", async () => {
  const built = new URL(`http://localhost${buildExploreUrl("proxy-uid")}`);
  expect(built.pathname).toBe(EXPLORE_URL);
  expect(built.searchParams.get("schemaVersion")).toBe("1");
  const panes = JSON.parse(built.searchParams.get("panes") ?? "{}");
  expect(panes.A.datasource).toBe("proxy-uid");
  expect(panes.A.queries[0].datasource.uid).toBe("proxy-uid");
  expect(panes.A.queries[0].direction).toBe("backward");
});

test("buildLogsDrilldownUrl encodes the datasource and default drilldown state @drilldown-core", async () => {
  const built = new URL(
    `http://localhost${buildLogsDrilldownUrl("drilldown-uid", { "var-levels": "error" })}`
  );
  expect(built.pathname).toBe(DRILLDOWN_URL);
  expect(built.searchParams.get("var-ds")).toBe("drilldown-uid");
  expect(built.searchParams.get("var-primary_label")).toBe("service_name|=~|.+");
  expect(built.searchParams.get("var-levels")).toBe("error");
});

test("buildServiceDrilldownUrl preserves service filter state across reloadable URLs @drilldown-core", async () => {
  const built = new URL(
    `http://localhost${buildServiceDrilldownUrl("proxy-uid", "api-gateway", "logs", {
      "var-fields": "method|=|GET",
    })}`
  );
  expect(built.pathname).toBe(`${DRILLDOWN_URL}/service/api-gateway/logs`);
  expect(built.searchParams.get("var-ds")).toBe("proxy-uid");
  expect(built.searchParams.get("var-primary_label")).toBe("service_name|=~|.+");
  expect(built.searchParams.get("var-filters")).toBe("service_name|=|api-gateway");
  expect(built.searchParams.get("var-fields")).toBe("method|=|GET");
  expect(built.searchParams.get("var-all-fields")).toBe("method|=|GET");
});

test("buildServiceDrilldownUrl keeps dotted field triplet for event-details filters @drilldown-core", async () => {
  const built = new URL(
    `http://localhost${buildServiceDrilldownUrl("proxy-uid", "api-gateway", "logs", {
      "var-fields": "k8s.cluster.name|=|my-cluster",
    })}`
  );

  expect(built.searchParams.get("var-fields")).toBe("k8s.cluster.name|=|my-cluster");
  const [label, operator, value] = (built.searchParams.get("var-fields") ?? "").split("|");
  expect(label).toBe("k8s.cluster.name");
  expect(operator).toBe("=");
  expect(value).toBe("my-cluster");
});
