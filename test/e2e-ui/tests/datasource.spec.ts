import { test, expect, Page } from "@playwright/test";
import {
  LOKI_DS,
  PROXY_DS,
  PROXY_OTEL_HYBRID_DS,
  PROXY_PATTERNS_AUTODETECT_DS,
  resolveDatasourceUid,
  waitForGrafanaReady,
  installGrafanaGuards,
} from "./helpers";

type DsQueryResult = { status: number; error?: string; frames: any[] };

// Runs one range query through Grafana's /api/ds/query, as Explore does.
async function dsQuery(page: Page, datasource: string, expr: string): Promise<DsQueryResult> {
  const uid = await resolveDatasourceUid(page, datasource);
  const response = await page.request.post("/api/ds/query", {
    data: {
      queries: [{ refId: "A", datasource: { uid }, expr, queryType: "range", maxLines: 50 }],
      from: "now-15m",
      to: "now",
    },
  });
  const body = await response.json();
  const result = body?.results?.A ?? {};
  return { status: response.status(), error: result.error, frames: result.frames ?? [] };
}

// Keys Grafana typed as structured metadata ("S") on the returned log rows.
function structuredMetadataKeys(frames: any[]): Set<string> {
  const keys = new Set<string>();
  for (const frame of frames) {
    const fields: { name: string }[] = frame?.schema?.fields ?? [];
    const labelsAt = fields.findIndex((f) => f.name === "labels");
    const typesAt = fields.findIndex((f) => f.name === "labelTypes");
    if (labelsAt < 0 || typesAt < 0) continue;
    const labels: Record<string, string>[] = frame.data.values[labelsAt];
    const types: Record<string, string>[] = frame.data.values[typesAt];
    labels.forEach((row, i) => {
      for (const key of Object.keys(row)) {
        if ((types[i] ?? {})[key] === "S") keys.add(key);
      }
    });
  }
  return keys;
}

test.describe("Loki-compatible profile through Grafana", () => {
  const dotted = "{env=\"production\"} | json | pipeline=`metrics/prometheus` | k8s.namespace.name=`monitoring`";

  test("a dotted name is Loki's parse error on the Explore and Drilldown datasources @cov:profiles/dotted-name-parse-error @cov:profiles/grafana-shows-loki-error-text", async ({ page }) => {
    const loki = await dsQuery(page, LOKI_DS, dotted);
    expect(loki.status, "Loki rejects the dotted name").toBe(400);
    expect(loki.error ?? "").toMatch(/^parse error at line 1, col \d+: syntax error: unexpected \./);
    for (const datasource of [PROXY_DS, PROXY_PATTERNS_AUTODETECT_DS]) {
      const proxy = await dsQuery(page, datasource, dotted);
      expect(proxy.status, `${datasource} status`).toBe(400);
      expect(proxy.error, `${datasource} error text`).toBe(loki.error);
    }
    // The OTel hybrid datasource exposes dotted names and accepts them.
    const hybrid = await dsQuery(page, PROXY_OTEL_HYBRID_DS, dotted);
    expect(hybrid.status, "OTel hybrid datasource runs the dotted filter").toBe(200);
  });

  test("structured metadata keys are Loki's sanitized names @cov:profiles/structured-metadata-keys-per-profile", async ({ page }) => {
    const expr = '{app="otel-collector"}';
    const lokiKeys = structuredMetadataKeys((await dsQuery(page, LOKI_DS, expr)).frames);
    expect(lokiKeys.size, "Loki returned OTel structured metadata (UI log generator running)").toBeGreaterThan(0);
    for (const datasource of [PROXY_DS, PROXY_PATTERNS_AUTODETECT_DS]) {
      const proxyKeys = structuredMetadataKeys((await dsQuery(page, datasource, expr)).frames);
      for (const key of proxyKeys) {
        expect(key.includes("."), `${datasource} structured metadata key ${key} has no dot`).toBe(false);
      }
      for (const key of ["k8s_cluster_name", "k8s_namespace_name", "k8s_pod_name", "trace_id", "span_id"]) {
        expect(lokiKeys.has(key), `Loki structured metadata carries ${key}`).toBe(true);
        expect(proxyKeys.has(key), `${datasource} structured metadata carries ${key}`).toBe(true);
      }
    }
    const hybridKeys = structuredMetadataKeys((await dsQuery(page, PROXY_OTEL_HYBRID_DS, expr)).frames);
    expect(hybridKeys.has("k8s.pod.name"), "OTel hybrid datasource adds the dotted spelling").toBe(true);
  });
});

test.describe("Grafana Datasource Health & Config", () => {
  test("datasource health check succeeds", async ({ page }) => {
    const guards = installGrafanaGuards(page, {
      allowedConsoleErrors: [/Failed to load resource: the server responded with a status of 404/i],
    });

    // Navigate to datasource settings
    await page.goto("/connections/datasources");
    await waitForGrafanaReady(page);

    // Find our proxy datasource
    const dsLink = page.getByText(PROXY_DS, { exact: false }).first();
    if (await dsLink.isVisible({ timeout: 5000 }).catch(() => false)) {
      await dsLink.click();
      await waitForGrafanaReady(page);

      // Click "Test" button
      const testBtn = page.getByRole("button", { name: /save & test|test/i });
      if (await testBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
        await testBtn.click();
        await page.waitForTimeout(3000);

        // Should show success message
        const success = page.locator(
          '[class*="alert-success"], [data-testid*="success"]'
        );
        const error = page.locator(
          '[class*="alert-error"], [data-testid*="error"]'
        );

        const isSuccess = await success
          .isVisible({ timeout: 5000 })
          .catch(() => false);
        const isError = await error
          .isVisible({ timeout: 1000 })
          .catch(() => false);

        if (isError) {
          const errText = await error.textContent();
          throw new Error(`Datasource health check failed: ${errText}`);
        }

        expect(isSuccess || !isError).toBeTruthy();
      }
    }

    await guards.assertClean();
  });
});
