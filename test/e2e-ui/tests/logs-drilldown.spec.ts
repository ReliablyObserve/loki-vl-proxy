import { test, expect, type Page } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  PROXY_PATTERNS_AUTODETECT_DS,
  installGrafanaGuards,
  openLogsDrilldown,
  resolveDatasourceUid,
  waitForGrafanaReady,
} from "./helpers";
import { buildServiceDrilldownUrl } from "./url-state";

const allowedDrilldownMtConsoleErrors = [
  /TypeError: Cannot read properties of null \(reading 'sort'\)[\s\S]*grafana-lokiexplore-app/i,
];

async function waitForDrilldownLanding(page: Page) {
  await waitForGrafanaReady(page);
  await expect(page.getByRole("combobox", { name: "Filter by labels" })).toBeVisible({
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
  view: "logs" | "fields" = "logs",
  overrides: Record<string, string> = {}
) {
  const uid = await resolveDatasourceUid(page, datasource);
  await page.goto(buildServiceDrilldownUrl(uid, serviceName, view, overrides));
  await waitForDrilldownDetails(page);
}

function nsRangeLastDay() {
  const end = Date.now() * 1_000_000;
  const start = (Date.now() - 24 * 60 * 60 * 1000) * 1_000_000;
  return { start, end };
}

function uniqueQueries(queries: string[]) {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const query of queries) {
    const normalized = query.trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    result.push(normalized);
  }
  return result;
}

function extractExactServiceHint(query: string) {
  const serviceMatch = query.match(/\{\s*service_name="([^"]+)"/);
  if (serviceMatch?.[1]) {
    return serviceMatch[1];
  }
  const appMatch = query.match(/\{\s*app="([^"]+)"/);
  if (appMatch?.[1]) {
    return appMatch[1];
  }
  return "";
}

async function seedPatternsStream(page: Page) {
  const now = new Date();
  const lines = [
    JSON.stringify({
      _time: new Date(now.getTime() - 2000).toISOString(),
      _msg: 'time="2026-04-14T11:00:00Z" level=info msg="finished unary call with code OK" grpc.code=OK grpc.method=GetThing grpc.service=DemoService grpc.start_time="2026-04-14T11:00:00Z" grpc.time_ms=7 span.kind=server system=grpc',
      app: "pattern-test",
      service_name: "pattern-test",
      level: "info",
      cluster: "e2e",
    }),
    JSON.stringify({
      _time: now.toISOString(),
      _msg: 'time="2026-04-14T11:00:01Z" level=info msg="finished unary call with code OK" grpc.code=OK grpc.method=ListThings grpc.service=DemoService grpc.start_time="2026-04-14T11:00:01Z" grpc.time_ms=9 span.kind=server system=grpc',
      app: "pattern-test",
      service_name: "pattern-test",
      level: "info",
      cluster: "e2e",
    }),
  ].join("\n");

  // e2e-ui shards don't run ingest tests, so seed VictoriaLogs directly.
  await page.request.post(
    "http://127.0.0.1:9428/insert/jsonline?_stream_fields=app,service_name,level,cluster",
    {
      data: lines,
      headers: {
        "Content-Type": "application/stream+json",
      },
    }
  );
}

async function discoverLabelValueQueries(
  page: Page,
  uid: string,
  start: number,
  end: number
) {
  const labels = ["service_name", "app", "service", "job", "namespace", "pod", "container"];
  const discovered: string[] = [];

  for (const label of labels) {
    const params = new URLSearchParams();
    params.set("start", String(start));
    params.set("end", String(end));
    const response = await page.request.get(
      `/api/datasources/proxy/uid/${uid}/loki/api/v1/label/${encodeURIComponent(label)}/values?${params.toString()}`
    );
    if (!response.ok()) {
      continue;
    }
    const payload = (await response.json().catch(() => null)) as {
      status?: string;
      data?: unknown[];
    } | null;
    if (payload?.status !== "success" || !Array.isArray(payload.data)) {
      continue;
    }
    for (const rawValue of payload.data) {
      if (typeof rawValue !== "string" || rawValue.trim().length === 0) {
        continue;
      }
      const value = rawValue.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
      discovered.push(`{${label}="${value}"}`);
      break;
    }
  }

  return discovered;
}

async function waitForAutodetectedPatterns(
  page: Page,
  datasource: string,
  queries: string[],
  timeoutMs = 30_000
) {
  const uid = await resolveDatasourceUid(page, datasource);
  const deadline = Date.now() + timeoutMs;
  let lastPatternsPayload: unknown = null;
  let lastSeedPayload: unknown = null;
  let lastQuery = "";
  const { start, end } = nsRangeLastDay();
  await seedPatternsStream(page);
  const discoveredQueries = await discoverLabelValueQueries(page, uid, start, end);
  const fallbackQueries = uniqueQueries([
    '{app="pattern-test"}',
    '{service_name="pattern-test"}',
    ...queries,
    ...discoveredQueries,
    '{service_name=~".+"}',
    '{app=~".+"}',
  ]);
  const serviceKeys = [
    "service_name",
    "service.name",
    "service",
    "app",
    "application",
    "app_name",
  ];

  while (Date.now() < deadline) {
    for (const query of fallbackQueries) {
      lastQuery = query;
      const seedParams = new URLSearchParams();
      seedParams.set("query", query);
      seedParams.set("start", String(start));
      seedParams.set("end", String(end));
      seedParams.set("limit", "200");
      seedParams.set("direction", "backward");
      const seedResponse = await page.request.get(
        `/api/datasources/proxy/uid/${uid}/loki/api/v1/query_range?${seedParams.toString()}`
      );
      try {
        lastSeedPayload = await seedResponse.json();
      } catch {
        lastSeedPayload = null;
      }

      const patternsParams = new URLSearchParams();
      patternsParams.set("query", query);
      patternsParams.set("start", String(start));
      patternsParams.set("end", String(end));
      patternsParams.set("step", "60s");
      const patternsResponse = await page.request.get(
        `/api/datasources/uid/${uid}/resources/patterns?${patternsParams.toString()}`
      );
      try {
        lastPatternsPayload = await patternsResponse.json();
      } catch {
        lastPatternsPayload = null;
      }

      const patternsData = (lastPatternsPayload as { data?: unknown[] } | null)?.data;
      if (!Array.isArray(patternsData) || patternsData.length === 0) {
        continue;
      }

      const exactServiceHint = extractExactServiceHint(query);
      if (exactServiceHint) {
        return exactServiceHint;
      }

      if (
        !seedResponse.ok() ||
        (lastSeedPayload as { status?: string } | null)?.status !== "success"
      ) {
        continue;
      }

      const result = ((lastSeedPayload as { data?: { result?: unknown[] } } | null)?.data
        ?.result ?? []) as Array<{ stream?: Record<string, string> }>;
      for (const entry of result) {
        const stream = entry?.stream ?? {};
        for (const key of serviceKeys) {
          const value = stream[key];
          if (typeof value === "string" && value.trim().length > 0) {
            return value;
          }
        }
      }
    }

    await page.waitForTimeout(500);
  }

  throw new Error(
    `timed out waiting for autodetected patterns (query=${lastQuery}, seed=${JSON.stringify(
      lastSeedPayload
    )}, patterns=${JSON.stringify(lastPatternsPayload)})`
  );
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

  test("multi-tenant landing shows service buckets without browser errors @drilldown-mt", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedConsoleErrors: allowedDrilldownMtConsoleErrors,
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });
    const responses = await collectDrilldownResponses(page);
    await openLogsDrilldown(page, PROXY_MULTI_DS);
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
    const guards = installGrafanaGuards(page, {
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });
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
    const guards = installGrafanaGuards(page, {
      allowedConsoleErrors: allowedDrilldownMtConsoleErrors,
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });
    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "logs");
    await expect(page.getByText("No logs found")).toHaveCount(0);
    await guards.assertClean();
  });

  test("multi-tenant service field view loads detected fields without browser errors @drilldown-mt", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedConsoleErrors: allowedDrilldownMtConsoleErrors,
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });
    const responses = await collectDrilldownResponses(page);
    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "fields");

    const fieldsResponse = responses.find((r) =>
      String(r.url).includes("/resources/detected_fields")
    );
    expect(fieldsResponse).toBeTruthy();
    expect(fieldsResponse?.status).toBe(200);
    expect(JSON.stringify(fieldsResponse?.json)).toContain('"fields"');
    await guards.assertClean();
  });

  test("multi-tenant service filter survives reload from URL state @drilldown-mt", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedConsoleErrors: allowedDrilldownMtConsoleErrors,
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });
    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "logs");
    await waitForDrilldownDetails(page);
    await expectFilterApplied(page, "Filter by labels", "service_name", "api-gateway");

    await page.reload();
    await waitForDrilldownDetails(page);
    await expectFilterApplied(page, "Filter by labels", "service_name", "api-gateway");
    await expect(page.getByText("No logs found")).toHaveCount(0);
    await guards.assertClean();
  });

  test("patterns are visible in drilldown for autodetected datasource @drilldown-core", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page);
    const serviceName = await waitForAutodetectedPatterns(
      page,
      PROXY_PATTERNS_AUTODETECT_DS,
      [`{service_name="api-gateway"}`, `{app="api-gateway"}`, `{app=~".+"}`]
    );

    await openServiceDrilldown(page, PROXY_PATTERNS_AUTODETECT_DS, serviceName, "logs", {
      from: "now-24h",
      to: "now",
    });

    const patternsTab = page.getByRole("tab", { name: /^Patterns/i }).first();
    await expect(patternsTab).toBeVisible({ timeout: 10_000 });
    await patternsTab.click();

    await expect
      .poll(async () => (await patternsTab.textContent())?.trim() ?? "", { timeout: 30_000 })
      .toMatch(/^Patterns[1-9]\d*$/);

    await expect(page.getByText("No patterns match these filters.")).toHaveCount(0);

    await guards.assertClean();
  });
});
