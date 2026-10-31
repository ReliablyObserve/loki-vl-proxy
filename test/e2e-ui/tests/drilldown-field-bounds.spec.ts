import { test, expect } from "@playwright/test";
import { resolveDatasourceUid } from "./helpers";
import { buildServiceDrilldownUrl } from "./url-state";

type Frame = { schema?: { meta?: { notices?: Array<{ text?: string }> } } };
type QueryResult = { error?: string; status?: number; frames?: Frame[] };

// The vmauth variant deliberately retains Loki's stock 500-series limit.
// A query's log-line `limit` is not a metric-series limit.
test.describe("Bounded high-cardinality Fields @drilldown-core", () => {
  for (const hours of [24, 48]) {
    test(`${hours}h field cards load data with visible partial-result notices`, async ({ page }, testInfo) => {
      test.setTimeout(120_000);
      const vl = process.env.VL_URL || "http://127.0.0.1:19428";
      const loki = process.env.LOKI_URL || "http://127.0.0.1:13101";
      const service = `field-bounds-ui-${Date.now()}-${hours}`;
      const end = Math.floor(Date.now() / 60_000) * 60_000 - 600_000;
      const values: Array<[string, string]> = [];
      const rows: string[] = [];
      for (const age of [36, 12, 1]) {
        for (let i = 0; i < 510; i++) {
          const ts = end - age * 3_600_000 + i;
          const fields = { trace_id: `trace-${i.toString().padStart(4, "0")}`, total: 10_000 + i };
          const msg = JSON.stringify(fields);
          values.push([`${BigInt(ts) * 1_000_000n}`, msg]);
          rows.push(JSON.stringify({ _time: new Date(ts).toISOString(), _msg: msg,
            app: service, service_name: service, env: "fields-bounds", ...fields }));
        }
      }
      const pushed = await page.request.post(`${loki}/loki/api/v1/push`, {
        headers: { "X-Scope-OrgID": "0" },
        data: { streams: [{ stream: { app: service, service_name: service, env: "fields-bounds" }, values }] },
      });
      expect(pushed.status(), await pushed.text()).toBe(204);
      const inserted = await page.request.post(`${vl}/insert/jsonline?_stream_fields=app,service_name,env`, {
        headers: { "Content-Type": "application/stream+json" }, data: `${rows.join("\n")}\n`,
      });
      expect(inserted.ok(), await inserted.text()).toBeTruthy();
      expect((await page.request.post(`${vl}/internal/force_flush`)).ok()).toBeTruthy();

      const uid = await resolveDatasourceUid(page, "Loki (via VL proxy vmauth)");
      const failures: string[] = [];
      const seen = new Map<string, { frames: number; warnings: number }>();
      const responseWork: Promise<void>[] = [];
      page.on("pageerror", error => failures.push(String(error)));
      page.on("response", response => {
        if (!response.url().includes("/api/ds/query")) return;
        responseWork.push((async () => {
          const request = response.request().postDataJSON();
          const body = await response.json();
          if (!response.ok()) failures.push(`HTTP ${response.status()}: ${JSON.stringify(body)}`);
          for (const [ref, result] of Object.entries(body.results || {}) as Array<[string, QueryResult]>) {
            if (result.error || (result.status || 0) >= 400) failures.push(`${ref}: ${result.error || result.status}`);
            // The short query-splitting residual can legitimately be empty.
            // Require populated full-range field responses, not merely HTTP 200.
            if (!["trace_id", "total"].includes(ref) || Number(request.to) - Number(request.from) < 3_600_000) continue;
            const frames = result.frames || [];
            const warnings = frames.flatMap(f => f.schema?.meta?.notices || []).filter(n => /returning partial results/.test(n.text || "")).length;
            if (frames.length) seen.set(ref, { frames: frames.length, warnings });
          }
        })().catch(error => failures.push(String(error))));
      });
      await page.goto(buildServiceDrilldownUrl(uid, service, "fields", {
        from: String(end - hours * 3_600_000), to: String(end),
      }));
      for (const field of ["trace_id", "total"]) {
        const title = page.getByText(field, { exact: true }).first();
        await expect(title).toBeVisible({ timeout: 45_000 });
        await title.scrollIntoViewIfNeeded();
        await expect.poll(() => seen.get(field)?.frames, { timeout: 45_000 }).toBe(500);
        expect(seen.get(field)!.warnings, `${field} must visibly disclose partial results`).toBeGreaterThan(0);
        const card = page.getByTestId(`data-testid Panel header ${field}`);
        await expect(card.getByRole("button", { name: "Show all 500", exact: true })).toBeVisible();
        await expect(card.getByText("No data", { exact: true })).toHaveCount(0);
        await expect(card.locator("canvas").first()).toBeVisible();
        await card.getByRole("button", { name: "Panel status", exact: true }).click();
        await expect(page.getByText("Maximum limit of results reached. Displaying partial results.", { exact: true }).first()).toBeVisible();
        await card.getByRole("button", { name: "Panel status", exact: true }).click();
      }
      await Promise.all(responseWork);
      expect(failures).toEqual([]);
      await testInfo.attach(`fields-${hours}h-responses`, { body: JSON.stringify(Object.fromEntries(seen), null, 2), contentType: "application/json" });
      await testInfo.attach(`fields-${hours}h`, { body: await page.screenshot({ fullPage: true }), contentType: "image/png" });
    });
  }
});
