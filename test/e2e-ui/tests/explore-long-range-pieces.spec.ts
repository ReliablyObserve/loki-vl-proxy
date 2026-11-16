/**
 * Grafana loads a metric range longer than a day in pieces: Explore sends one
 * /api/ds/query request per piece (each with its own from/to) and joins the
 * pieces' frames into one chart in the browser. This test watches it through
 * the stack's Grafana, on the proxy datasource and on the Loki datasource, over
 * the same seeded data: Grafana must ask both for the same pieces (more than
 * one), and every piece's frames must be the same on both (series labels,
 * sample times and values). Equal pieces give an equal joined chart, however
 * Grafana joins them. The data is a private app seeded into Loki and
 * VictoriaLogs with identical lines: two pods over 30 hours, inside the
 * window the stack Loki accepts pushes for (its reject-old-samples max-age).
 */
import { test, expect, type Page, type Response } from "@playwright/test";
import { PROXY_DS, LOKI_DS, openExplore, waitForExploreIdle } from "./helpers";

const LOKI = process.env.LOKI_URL || "http://127.0.0.1:13101";
const VL = process.env.VL_URL || "http://127.0.0.1:19428";
const HOUR_MS = 3_600_000;
// How far back the fixture reaches: past one day, so the range splits at a day.
const SPAN_HOURS = 30;

type Pieces = Map<string, string[]>;

async function seed(page: Page, app: string, endMs: number): Promise<number> {
  const streams = [
    { pod: "pieces-a", everyMs: 5 * 60_000 },
    { pod: "pieces-b", everyMs: 7 * 60_000 },
  ].map(({ pod, everyMs }) => {
    const values: Array<[string, string]> = [];
    for (let ts = endMs - SPAN_HOURS * HOUR_MS + 300; ts < endMs; ts += everyMs) {
      values.push([`${BigInt(ts) * 1_000_000n}`, "long range pieces fixture"]);
    }
    return { stream: { app, namespace: "prod", pod }, values };
  });
  const body = { streams };
  const lines = streams.reduce((n, s) => n + s.values.length, 0);
  const loki = await page.request.post(`${LOKI}/loki/api/v1/push`, { headers: { "X-Scope-OrgID": "0" }, data: body });
  expect(loki.status(), await loki.text()).toBe(204);
  const vl = await page.request.post(`${VL}/insert/loki/api/v1/push?disable_message_parsing=1`, { data: body });
  expect(vl.ok(), await vl.text()).toBeTruthy();
  expect((await page.request.post(`${VL}/internal/force_flush`)).ok()).toBeTruthy();
  // The lines are older than the stack Loki's query_ingesters_within (30m):
  // Loki serves them only once its ingester has flushed them to the store.
  const flushed = await page.request.post(`${LOKI}/flush`);
  expect(flushed.status(), await flushed.text()).toBeLessThan(300);
  return lines;
}

// Wait until Loki's range path counts every seeded line. Each poll filters on
// a new nonce so Loki's results cache cannot answer it with an earlier count.
async function waitForLoki(page: Page, app: string, endMs: number, lines: number) {
  test.info().setTimeout(test.info().timeout + 240_000);
  const deadline = Date.now() + 240_000;
  let last = "";
  while (Date.now() < deadline) {
    const params = new URLSearchParams({
      query: `sum(count_over_time({app="${app}"} != "nonce-${Date.now()}" [31h]))`,
      start: String(endMs / 1000),
      end: String(endMs / 1000),
      step: "60",
    });
    const response = await page.request.get(`${LOKI}/loki/api/v1/query_range?${params}`, { headers: { "X-Scope-OrgID": "0" } });
    if (response.ok()) {
      const body = await response.json();
      last = String(body?.data?.result?.[0]?.values?.at(-1)?.[1] ?? "none");
      if (last === String(lines)) return;
    } else {
      last = `HTTP ${response.status()}`;
    }
    await page.waitForTimeout(2_000);
  }
  throw new Error(`Loki's range path counts ${last} of ${lines} seeded lines`);
}

async function explorePieces(page: Page, ds: string, expr: string, marker: string, fromMs: number, toMs: number): Promise<Pieces> {
  const pieces: Pieces = new Map();
  const failures: string[] = [];
  const work: Promise<void>[] = [];
  const listener = (response: Response) => {
    if (!response.url().includes("/api/ds/query")) return;
    const request = response.request().postDataJSON();
    if (!JSON.stringify(request?.queries ?? []).includes(marker)) return;
    work.push(
      (async () => {
        const body = await response.json();
        if (!response.ok()) failures.push(`HTTP ${response.status()}: ${JSON.stringify(body)}`);
        const frames: string[] = [];
        for (const [ref, result] of Object.entries<any>(body.results ?? {})) {
          if (result.error || (result.status ?? 200) >= 400) failures.push(`${ref}: ${result.error ?? result.status}`);
          for (const frame of result.frames ?? []) {
            const fields = frame.schema?.fields ?? [];
            const values = frame.data?.values ?? [];
            frames.push(JSON.stringify({ labels: fields[1]?.labels ?? {}, times: values[0] ?? [], values: values[1] ?? [] }));
          }
        }
        frames.sort();
        pieces.set(`${request.from}..${request.to}`, frames);
      })().catch((error) => failures.push(String(error)))
    );
  };
  page.on("response", listener);
  await openExplore(page, ds, expr, { from: String(fromMs), to: String(toMs) });
  await waitForExploreIdle(page, 120_000, { clearStuckAutoRun: true });
  await Promise.all(work);
  page.off("response", listener);
  expect(failures, `${ds}: query failures`).toEqual([]);
  return pieces;
}

test.describe("Explore long metric ranges load in pieces that equal Loki's", () => {
  for (const hours of [30, 48]) {
    test(`${hours}h range @explore-core @cov:drilldown/long-range-pieces-answer-like-loki`, async ({ page }, testInfo) => {
      test.setTimeout(240_000);
      const app = `pieces-${Date.now()}-${hours}`;
      const toMs = Math.floor(Date.now() / 60_000) * 60_000 - 10 * 60_000;
      const fromMs = toMs - hours * HOUR_MS;
      const lines = await seed(page, app, toMs);
      await waitForLoki(page, app, toMs, lines);

      const expr = `sum by (pod) (count_over_time({app="${app}", namespace="prod", pod!=""} [$__auto]))`;
      const proxy = await explorePieces(page, PROXY_DS, expr, app, fromMs, toMs);
      await testInfo.attach(`pieces-${hours}h-proxy`, { body: await page.screenshot({ fullPage: true }), contentType: "image/png" });
      const loki = await explorePieces(page, LOKI_DS, expr, app, fromMs, toMs);
      await testInfo.attach(`pieces-${hours}h-loki`, { body: await page.screenshot({ fullPage: true }), contentType: "image/png" });

      expect(proxy.size, "Grafana loads the range in more than one piece").toBeGreaterThan(1);
      expect([...proxy.keys()].sort(), "the same pieces on both datasources").toEqual([...loki.keys()].sort());
      let samples = 0;
      for (const [piece, frames] of loki) {
        expect(proxy.get(piece), `piece ${piece}: frames`).toEqual(frames);
        samples += frames.reduce((n, f) => n + (JSON.parse(f).times as unknown[]).length, 0);
      }
      expect(samples, "the pieces hold the seeded data").toBeGreaterThan(0);
      await testInfo.attach(`pieces-${hours}h`, {
        body: JSON.stringify({ pieces: [...loki.keys()], samples }, null, 2),
        contentType: "application/json",
      });
    });
  }
});
