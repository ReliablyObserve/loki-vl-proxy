import { test, expect } from "@playwright/test";
import { PROXY_PATTERNS_AUTODETECT_DS, resolveDatasourceUid } from "./helpers";
import { buildServiceDrilldownUrl } from "./url-state";

type Frame = {
  schema?: { fields?: Array<{ labels?: Record<string, string> }>; meta?: { notices?: Array<{ text?: string }> } };
  data?: { values?: number[][] };
};
type QueryResult = { error?: string; status?: number; frames?: Frame[] };

// The vmauth variant deliberately keeps the proxy's built-in 1000-series limit.
// A query's log-line `limit` is not a metric-series limit.
test.describe("High-cardinality Fields @drilldown-core", () => {
  for (const { hours, partial } of [
    { hours: 24, partial: true }, { hours: 48, partial: true },
    { hours: 0.25, partial: false }, { hours: 3, partial: false },
    { hours: 24, partial: false }, { hours: 48, partial: false },
  ]) {
    test(`${hours}h field cards load ${partial ? "partial data with notices" : "every value without truncation"}`, async ({ page }, testInfo) => {
      test.setTimeout(120_000);
      const vl = process.env.VL_URL || "http://127.0.0.1:19428";
      const loki = process.env.LOKI_URL || "http://127.0.0.1:13101";
      const service = `field-bounds-ui-${Date.now()}-${hours}`;
      const end = Math.floor(Date.now() / 60_000) * 60_000 - 600_000;
      const values: Array<[string, string]> = [];
      const rows: string[] = [];
      // Stay inside Loki's out-of-order ingest window, including the settled
      // ten-minute offset above. The 30h cohort still exercises a second day.
      for (const age of [30, 12, 1, 1 / 12]) {
        for (let i = 0; i < 1010; i++) {
          const ts = end - age * 3_600_000 + i;
          const fields = { trace_id: `trace-${i.toString().padStart(4, "0")}`, batch_id: `batch-${i.toString().padStart(4, "0")}`, total: 10_000 + i };
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

      const uid = await resolveDatasourceUid(page, partial ? "Loki (via VL proxy vmauth)" : PROXY_PATTERNS_AUTODETECT_DS);
      const expectedFrames = partial ? 1000 : 1010;
      const failures: string[] = [];
      const seen = new Map<string, { frames: number; warnings: number; labels: string[]; populated: boolean }>();
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
            if (!["trace_id", "batch_id", "total"].includes(ref) || Number(request.to) - Number(request.from) < Math.min(hours * 3_600_000, 3_600_000)) continue;
            const frames = result.frames || [];
            const warnings = frames.flatMap(f => f.schema?.meta?.notices || []).filter(n => /returning partial results/.test(n.text || "")).length;
            const labels = frames.map(f => f.schema?.fields?.find(field => field.labels?.[ref])?.labels?.[ref] || "").sort();
            const populated = frames.every(f => (f.data?.values?.[1]?.length || 0) > 0
              && f.data!.values![1].every(value => Number.isFinite(value) && value > 0));
            if (frames.length) seen.set(ref, { frames: frames.length, warnings, labels, populated });
          }
        })().catch(error => failures.push(String(error))));
      });
      await page.goto(buildServiceDrilldownUrl(uid, service, "fields", {
        from: String(end - hours * 3_600_000), to: String(end),
      }));
      for (const field of ["trace_id", "batch_id", "total"]) {
        const card = page.getByTestId(`data-testid Panel header ${field}`);
        // Grafana can hide a title when the responsive header needs its space.
        // The named card, its graph and its actual data must still be visible.
        await card.scrollIntoViewIfNeeded();
        await expect(card).toBeVisible({ timeout: 45_000 });
        await expect.poll(() => seen.get(field)?.frames, { timeout: 45_000 }).toBe(expectedFrames);
        if (partial) expect(seen.get(field)!.warnings, `${field} must visibly disclose partial results`).toBeGreaterThan(0);
        else {
          expect(seen.get(field)!.warnings, `${field} must include every distinct value`).toBe(0);
          const expectedLabels = Array.from({ length: 1010 }, (_, i) => field === "total"
            ? String(10_000 + i) : `${field === "trace_id" ? "trace" : "batch"}-${i.toString().padStart(4, "0")}`).sort();
          expect(seen.get(field)!.labels, `${field} must contain the exact fixture identities, once each`).toEqual(expectedLabels);
        }
        expect(seen.get(field)!.populated, `${field} must contain finite, positive observations`).toBe(true);
        await expect(card.getByRole("button", { name: `Show all ${expectedFrames}`, exact: true })).toBeVisible();
        await expect(card.getByText("No data", { exact: true })).toHaveCount(0);
        await expect(card.locator("canvas").first()).toBeVisible();
        if (partial) {
          await card.getByRole("button", { name: "Panel status", exact: true }).click();
          await expect(page.getByText("Maximum limit of results reached. Displaying partial results.", { exact: true }).first()).toBeVisible();
          await card.getByRole("button", { name: "Panel status", exact: true }).click();
        } else {
          await expect(card.getByRole("button", { name: "Panel status", exact: true })).toHaveCount(0);
        }
      }
      await Promise.all(responseWork);
      expect(failures).toEqual([]);
      await testInfo.attach(`fields-${hours}h-responses`, { body: JSON.stringify(Object.fromEntries(seen), null, 2), contentType: "application/json" });
      await testInfo.attach(`fields-${hours}h`, { body: await page.screenshot({ fullPage: true }), contentType: "image/png" });
    });
  }
});
