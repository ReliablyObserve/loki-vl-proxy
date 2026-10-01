/**
 * The Explore graph of a filtered `| json` metric (label filters that reject
 * the empty value: `service_version!=""`, `pipeline=~".+"`, `pipeline="x"`)
 * renders the same series, with the same points, as Loki. Compared on the panel
 * data Grafana receives (/api/ds/query frames), not on pixels: the proxy runs
 * these through the ordered JSON stats pushdown with a prefilter, which must
 * not change a single value.
 */
import { test, expect, type Page } from "@playwright/test";
import { PROXY_DS, LOKI_DS, openExplore, waitForLokiMetricData, resolveDatasourceUid } from "./helpers";

type Series = Map<string, Map<number, number>>;

// A window that ends two minutes ago so both backends have it complete. A
// stack without a live generator (bench/visual) sets EXPLORE_WINDOW_END_MS to
// the end of its seeded data instead.
const FIXED_END_MS = process.env.EXPLORE_WINDOW_END_MS ? parseInt(process.env.EXPLORE_WINDOW_END_MS) : 0;
const END_MS = () => FIXED_END_MS || Math.floor(Date.now() / 60_000) * 60_000 - 2 * 60_000;
const WINDOW_MS = 15 * 60_000;

async function panelSeries(page: Page, ds: string, expr: string, fromMs: number, toMs: number): Promise<Series> {
  const response = page.waitForResponse(
    (r) => r.url().includes("/api/ds/query") && (r.request().postData() ?? "").includes("count_over_time"),
    { timeout: 60_000 }
  );
  await openExplore(page, ds, expr, { from: String(fromMs), to: String(toMs) });
  const body = await (await response).json();
  const out: Series = new Map();
  for (const res of Object.values<any>(body.results ?? {})) {
    expect(res.error, `${ds}: query error`).toBeUndefined();
    for (const fr of res.frames ?? []) {
      const fields = fr.schema.fields;
      const labels = JSON.stringify(fields[1]?.labels ?? {});
      const points = new Map<number, number>();
      fr.data.values[0].forEach((t: number, i: number) => points.set(t, fr.data.values[1][i]));
      out.set(labels, points);
    }
  }
  return out;
}

// Loki drops empty buckets where the proxy writes zeros: compare with zeros filled in.
function same(a: Series, b: Series, what: string) {
  expect([...a.keys()].sort(), `${what}: series set`).toEqual([...b.keys()].sort());
  for (const [k, pa] of a) {
    const pb = b.get(k)!;
    for (const t of new Set([...pa.keys(), ...pb.keys()])) {
      expect(pa.get(t) ?? 0, `${what} ${k} @${t}`).toBeCloseTo(pb.get(t) ?? 0, 9);
    }
  }
}

const SHAPES: Array<[string, string, string]> = [
  ["pipeline equals", 'sum by (level) (count_over_time({env="production"} | json | pipeline=`logs/loki` [$__auto]))', "semantics/json-filter-pushdown-without-error-drop"],
  ["service_version breakdown", 'sum by (service_version) (count_over_time({env="production"} | json | drop __error__ | service_version!="" [$__auto]))', "semantics/json-filter-pushdown-underscore-label"],
  ["pipeline regexp breakdown", 'sum by (pipeline) (count_over_time({env="production"} | json | service_version!="" | pipeline=~".+" [$__auto]))', "semantics/json-filter-pushdown-underscore-label"],
];

test.describe("Explore graph of a filtered json metric equals Loki's", () => {
  for (const [name, expr, cov] of SHAPES) {
    test(`${name} @explore-core @cov:${cov}`, async ({ page }) => {
      const lokiUid = await resolveDatasourceUid(page, LOKI_DS);
      if (!FIXED_END_MS) {
        await waitForLokiMetricData(page, lokiUid, '{env="production"}', { endOffsetSec: 120 });
      }
      const toMs = END_MS();
      const fromMs = toMs - WINDOW_MS;
      const proxy = await panelSeries(page, PROXY_DS, expr, fromMs, toMs);
      const loki = await panelSeries(page, LOKI_DS, expr, fromMs, toMs);
      expect(proxy.size, "the panel has series").toBeGreaterThan(0);
      same(proxy, loki, name);
    });
  }
});
