import { test, expect } from "@playwright/test";
import { createHash } from "node:crypto";
import { LOKI_DS, PROXY_PATTERNS_AUTODETECT_DS, resolveDatasourceUid } from "./helpers";
import { buildServiceDrilldownUrl } from "./url-state";

type Frame = {
  schema?: { fields?: Array<{ labels?: Record<string, string> }>; meta?: { notices?: Array<{ text?: string }> } };
  data?: { values?: number[][] };
};
type QueryResult = { error?: string; status?: number; frames?: Frame[] };

// The vmauth variant deliberately keeps the proxy's built-in 1000-series limit.
// A query's log-line `limit` is not a metric-series limit.
test.describe("High-cardinality Fields @drilldown-core", () => {
  test("all Fields across services survive a 3h view, repeat and refresh", async ({ page }, testInfo) => {
    test.setTimeout(180_000);
    const vl = process.env.VL_URL || "http://127.0.0.1:19428";
    const loki = process.env.LOKI_URL || "http://127.0.0.1:13101";
    const cohort = `fields-fanout-${Date.now()}`;
    const end = Math.floor(Date.now() / 60_000) * 60_000 - 600_000;
    const bodyFields = ["request_id", "user_id", "batch_id", "session_id", "message_id", "body_trace"];
    const metadataFields = ["trace_id", "span_id"];
    const valuesPerService = 350;
    const ages = [150, 100, 50, 5];
    const streams = [];
    for (let service = 0; service < 3; service++) {
      const values = [];
      for (const age of ages) {
        for (let i = 0; i < valuesPerService; i++) {
          const id = `${service}-${i.toString().padStart(3, "0")}`;
          const fields = Object.fromEntries(bodyFields.map(field => [field, `${field}-${id}`]));
          // This field exists in the body and as metadata in different streams.
          // Drilldown selects the parser from discovery across the sample.
          if (service === 0) fields.trace_id = `body-${id}`;
          const metadata: Record<string, string> = service === 0 ? {} : { trace_id: `metadata-trace-${id}`, span_id: `metadata-span-${id}` };
          if (age === 150) metadata.historical_only = `old-${id}`;
          values.push([`${BigInt(end - age * 60_000 + i) * 1_000_000n}`, JSON.stringify(fields), metadata]);
        }
      }
      streams.push({ stream: { app: `${cohort}-${service}`, service_name: `${cohort}-${service}`, env: cohort }, values });
    }
    for (const url of [`${loki}/loki/api/v1/push`, `${vl}/insert/loki/api/v1/push?disable_message_parsing=1`]) {
      const pushed = await page.request.post(url, { headers: { "X-Scope-OrgID": "0" }, data: { streams } });
      expect(pushed.ok(), await pushed.text()).toBeTruthy();
    }
    expect((await page.request.post(`${vl}/internal/force_flush`)).ok()).toBeTruthy();
    expect((await page.request.post(`${loki}/flush`)).ok()).toBeTruthy();
    const uid = await resolveDatasourceUid(page, PROXY_PATTERNS_AUTODETECT_DS);
    const lokiUid = await resolveDatasourceUid(page, LOKI_DS);
    const failures: string[] = [];
    const pending: Promise<void>[] = [];
    const seen = new Map<string, string[]>();
    const compared = new Set<string>();
    const oracleProof: object[] = [];
    page.on("pageerror", error => failures.push(String(error)));
    page.on("response", response => {
      if (!response.url().includes("/api/ds/query")) return;
      pending.push((async () => {
        const body = await response.json();
        if (!response.ok()) failures.push(`HTTP ${response.status()}`);
        for (const [ref, result] of Object.entries(body.results || {}) as Array<[string, QueryResult]>) {
          if (result.error || (result.status || 0) >= 400) failures.push(`${ref}: ${result.error || result.status}`);
          if (![...bodyFields, ...metadataFields].includes(ref) || !result.frames?.length) continue;
          const labels = result.frames.map(frame => frame.schema?.fields?.find(field => field.labels?.[ref])?.labels?.[ref] || "").sort();
          if (labels.length > (seen.get(ref)?.length || 0)) seen.set(ref, labels);
          expect(result.frames.flatMap(frame => frame.schema?.meta?.notices || [])).toEqual([]);
          expect(result.frames.every(frame => frame.data?.values?.[1]?.some(value => Number.isFinite(value) && value > 0))).toBeTruthy();
          const request = response.request().postDataJSON();
          const queries = request.queries.filter((query: { refId: string }) => query.refId === ref);
          const key = `${ref}:${request.from}:${request.to}:${JSON.stringify(queries)}`;
          if (!compared.has(key)) {
            compared.add(key);
            const oracle = await page.request.post("/api/ds/query", { data: {
              ...request, queries: queries.map((query: object) => ({ ...query, datasource: { type: "loki", uid: lokiUid } })),
            }});
            expect(oracle.ok(), await oracle.text()).toBeTruthy();
            const answer = (await oracle.json()).results[ref];
            expect(answer.error).toBeUndefined();
            const canonical = (frames: Frame[]) => frames.map(frame => JSON.stringify({
              labels: frame.schema?.fields?.find(field => field.labels)?.labels,
              values: frame.data?.values,
            })).sort();
            const proxyFrames = canonical(result.frames);
            const lokiFrames = canonical(answer.frames || []);
            expect(proxyFrames, `${ref}: full frames must match Loki`).toEqual(lokiFrames);
            const digest = (frames: string[]) => createHash("sha256").update(JSON.stringify(frames)).digest("hex");
            oracleProof.push({ field: ref, from: request.from, to: request.to,
              queries: queries.map((query: { expr: string }) => query.expr),
              frames: proxyFrames.length, proxyDigest: digest(proxyFrames), lokiDigest: digest(lokiFrames) });
          }
        }
      })().catch(error => failures.push(String(error))));
    });
    for (const shift of [0, 0, 60_000]) {
      seen.clear();
      compared.clear();
      const params = new URLSearchParams({
        "var-ds": uid, from: String(end - 3 * 3_600_000 + shift), to: String(end + shift),
        "var-filters": `env|=|${cohort}`, "var-fieldBy": "$__all", "var-labelBy": "$__all", patterns: "[]",
      });
      await page.goto(`/a/grafana-lokiexplore-app/explore/env/${cohort}/fields?${params}`);
      for (const field of [...bodyFields, ...metadataFields]) {
        const card = page.getByTestId(`data-testid Panel header ${field}`);
        await card.scrollIntoViewIfNeeded();
        await expect(card).toBeVisible();
        const services = field === "span_id" ? [1, 2] : [0, 1, 2];
        const expected = services.flatMap(service => Array.from({ length: valuesPerService }, (_, i) => {
          const id = `${service}-${i.toString().padStart(3, "0")}`;
          return field === "trace_id" && service === 0 ? `body-${id}`
            : metadataFields.includes(field) ? `metadata-${field === "trace_id" ? "trace" : "span"}-${id}` : `${field}-${id}`;
        })).sort();
        await expect.poll(() => seen.get(field), { timeout: 45_000 }).toEqual(expected);
        await expect(card.locator("canvas").first()).toBeVisible();
        await expect(card.getByText("No data", { exact: true })).toHaveCount(0);
      }
      await Promise.all(pending);
      expect(failures).toEqual([]);
      expect([...new Set([...compared].map(key => key.split(":")[0]))].sort()).toEqual([...bodyFields, ...metadataFields].sort());
      await expect(page.getByTestId("data-testid Panel header historical_only")).toHaveCount(0);
      // Health alone may respawn a killed plugin. The CI resource guard also
      // checks cgroup OOM deltas and killed-plugin logs around the whole shard.
      for (const datasource of [uid, lokiUid]) {
        const response = await page.request.post("/api/ds/query", { data: {
          from: String(end - 3 * 3_600_000), to: String(end), queries: [{
            refId: "survival", expr: `sum(count_over_time({env="${cohort}"}[3h]))`, instant: true,
            datasource: { type: "loki", uid: datasource }, intervalMs: 60_000, maxDataPoints: 100,
          }],
        }});
        expect(response.ok(), await response.text()).toBeTruthy();
        const result = (await response.json()).results.survival;
        expect(result.error).toBeUndefined();
        expect(result.frames?.flatMap((frame: Frame) => frame.data?.values?.[1] || [])).toContain(streams.length * valuesPerService * ages.length);
      }
    }
    await testInfo.attach("all-fields-3h", { body: JSON.stringify(Object.fromEntries(seen), null, 2), contentType: "application/json" });
    await testInfo.attach("all-fields-3h-oracle", { body: JSON.stringify(oracleProof), contentType: "application/json" });
    await testInfo.attach("all-fields-3h", { body: await page.screenshot({ fullPage: true }), contentType: "image/png" });
  });

  for (const { hours, partial } of [
    { hours: 3, partial: true }, { hours: 24, partial: true }, { hours: 48, partial: true },
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
