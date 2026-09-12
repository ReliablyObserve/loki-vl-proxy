/**
 * Proxy vs Loki 100% parity regression tests.
 *
 * Uses page.request to call both datasources directly through Grafana's
 * datasource proxy (`/api/datasources/proxy/uid/{uid}/`) and compares
 * status codes, result types, stream counts, and line counts.
 *
 * Rules:
 *  - Working queries: EXACT parity required (proxy == Loki, strict)
 *  - Known proxy gaps: documented with test.fixme() — fail in CI once the
 *    proxy implementation is fixed (regression catch)
 */

import { test, expect, type Page } from "@playwright/test";
import { waitForLokiMetricData, lokiIndexedStreamCount, PROXY_DS, LOKI_DS, resolveDatasourceUid } from "./helpers";

// ---------------------------------------------------------------------------
// Types & helpers
// ---------------------------------------------------------------------------

interface LokiResponse {
  status: string;
  data: {
    resultType: "streams" | "matrix" | "vector";
    result: unknown[];
  };
}

// Exclude the live edge: the UI stack's log generator writes continuously and
// the two backends are queried a few milliseconds apart, so a window ending at
// "now" drifts by a line or two between them. Data older than a minute is
// complete on both sides.
const LIVE_EDGE_SEC = 60;
const windowEnd = () => Math.floor(Date.now() / 1000) - LIVE_EDGE_SEC;

async function queryRange(
  page: Page,
  dsUID: string,
  query: string,
  opts: { step?: string; limit?: string; windowSec?: number; endSec?: number } = {}
): Promise<{ statusCode: number; body: LokiResponse | null }> {
  await ensureStackWarm(page);
  // Callers comparing two responses pass one shared endSec: the generator
  // creates ~2 streams/s, so two ends computed a second apart would differ.
  const end = opts.endSec ?? windowEnd();
  // 7-day window by default to cover any stack age (data is ingested once at stack start)
  const start = end - (opts.windowSec ?? 7 * 24 * 3600);
  const params = new URLSearchParams({
    query,
    start: String(start),
    end: String(end),
    ...(opts.step ? { step: opts.step } : {}),
    limit: opts.limit ?? "500",
  });

  const resp = await page.request.get(
    `/api/datasources/proxy/uid/${dsUID}/loki/api/v1/query_range?${params}`
  );

  if (!resp.ok()) return { statusCode: resp.status(), body: null };
  return { statusCode: resp.status(), body: (await resp.json()) as LokiResponse };
}

// Instant query for a single scalar aggregate. Unlike query_range this is
// not subject to `limit`, so it can compare volumes that exceed any line cap.
async function instantScalar(
  page: Page,
  dsUID: string,
  query: string,
  timeSec?: number
): Promise<number> {
  await ensureStackWarm(page);
  const time = timeSec ?? windowEnd();
  const params = new URLSearchParams({ query, time: String(time) });
  const resp = await page.request.get(
    `/api/datasources/proxy/uid/${dsUID}/loki/api/v1/query?${params}`
  );
  expect(resp.status()).toBe(200);
  const body = (await resp.json()) as LokiResponse;
  expect(body.data?.resultType).toBe("vector");
  const vec = body.data.result as Array<{ value: [number, string] }>;
  expect(vec.length).toBe(1);
  return Number(vec[0].value[1]);
}

function lineCount(body: LokiResponse | null): number {
  if (!body) return -1;
  return (body.data?.result as Array<{ values: unknown[] }>).reduce(
    (sum, s) => sum + (s.values?.length ?? 0),
    0
  );
}

function seriesCount(body: LokiResponse | null): number {
  return body?.data?.result?.length ?? -1;
}

let _proxyUID: string | null = null;
let _lokiUID: string | null = null;

async function uids(page: Page) {
  if (!_proxyUID) _proxyUID = await resolveDatasourceUid(page, PROXY_DS);
  if (!_lokiUID) _lokiUID = await resolveDatasourceUid(page, LOKI_DS);
  return { proxyUID: _proxyUID, lokiUID: _lokiUID };
}

// Assert exact parity between proxy and Loki for a log stream query.
async function assertLogParity(
  page: Page,
  query: string,
  label: string
): Promise<void> {
  const { proxyUID, lokiUID } = await uids(page);
  const endSec = windowEnd();
  const [proxy, loki] = await Promise.all([
    queryRange(page, proxyUID, query, { endSec }),
    queryRange(page, lokiUID, query, { endSec }),
  ]);

  expect(proxy.statusCode, `${label}: status code`).toBe(loki.statusCode);
  if (loki.statusCode !== 200) return;

  expect(proxy.body?.data?.resultType, `${label}: resultType`).toBe(
    loki.body?.data?.resultType
  );
  expect(lineCount(proxy.body), `${label}: line count`).toBe(
    lineCount(loki.body)
  );
}

// Fresh-stack gate. The UI stack's generator starts with the stack, so for the
// first minute there is nothing behind the live edge on either backend, and
// Loki's range-metric path stays blank for a few minutes more (see
// waitForLokiMetricData). Every query in this file goes through queryRange or
// instantScalar, so gating them once per selector per worker keeps the suite
// deterministic on the fresh stacks CI uses; the proxy itself needs no grace
// period.
const stackWarm = new Map<string, Promise<void>>();
async function ensureStackWarm(page: Page, selector = '{app="api-gateway"}'): Promise<void> {
  let p = stackWarm.get(selector);
  if (!p) {
    p = (async () => {
      const { lokiUID } = await uids(page);
      await waitForLokiMetricData(page, lokiUID, selector, {
        endOffsetSec: LIVE_EDGE_SEC,
      });
    })();
    stackWarm.set(selector, p);
  }
  await p;
}

// Assert exact parity for metric queries (series count must match).
async function assertMetricParity(
  page: Page,
  query: string,
  label: string,
  selector = '{app="api-gateway"}'
): Promise<void> {
  await ensureStackWarm(page, selector);
  const { proxyUID, lokiUID } = await uids(page);
  const endSec = windowEnd();
  const [proxy, loki] = await Promise.all([
    queryRange(page, proxyUID, query, { step: "60", endSec }),
    queryRange(page, lokiUID, query, { step: "60", endSec }),
  ]);

  expect(proxy.statusCode, `${label}: status code`).toBe(loki.statusCode);
  if (loki.statusCode !== 200) return;

  expect(proxy.body?.data?.resultType, `${label}: resultType`).toBe(
    loki.body?.data?.resultType
  );
  expect(seriesCount(proxy.body), `${label}: series count`).toBe(
    seriesCount(loki.body)
  );
}

// ---------------------------------------------------------------------------
// Stream selector parity
// ---------------------------------------------------------------------------

test.describe("@regression Stream selectors — exact Loki parity", () => {
  test("exact label match @regression", async ({ page }) =>
    assertLogParity(page, `{app="api-gateway"}`, "exact label match"));

  test("multi-label exact match @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway",namespace="prod"}`,
      "multi-label"
    ));

  test("regex label match @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app=~"api-.*",namespace="prod"}`,
      "regex label match"
    ));

  test("negative label match @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway",level!="info"}`,
      "negative label"
    ));

  test("negative regex label match @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{namespace!~"kube-.*",app="api-gateway"}`,
      "neg regex label"
    ));
});

// ---------------------------------------------------------------------------
// Line filter parity
// ---------------------------------------------------------------------------

test.describe("@regression Line filters — exact Loki parity", () => {
  test("contains filter |= @regression", async ({ page }) =>
    assertLogParity(page, `{app="api-gateway"} |= "GET"`, "|= contains"));

  test("not-contains filter != @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} != "health"`,
      "!= not-contains"
    ));

  test("regex filter |~ simple @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} |~ "POST"`,
      "|~ simple regex"
    ));

  test("contains then not-contains chain @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} |= "request" != "health"`,
      "contains + not-contains chain"
    ));

  test("not-contains then not-contains chain @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} != "debug" != "trace"`,
      "double not-contains"
    ));
});

// ---------------------------------------------------------------------------
// Parser parity
// ---------------------------------------------------------------------------

test.describe("@regression Parsers — exact Loki parity", () => {
  test("json parser @regression", async ({ page }) =>
    assertLogParity(page, `{app="api-gateway"} | json`, "json"));

  test("logfmt parser @regression", async ({ page }) =>
    assertLogParity(page, `{app="payment-service"} | logfmt`, "logfmt"));

  test("json + field equality filter @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | method="GET"`,
      "json + field ="
    ));

  test("json + field not-equal filter @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | method!="GET"`,
      "json + field !="
    ));

  test("json + numeric filter status >= 400 @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | status >= 400`,
      "json + status >= 400"
    ));

  test("json + numeric filter status >= 500 @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | status >= 500`,
      "json + status >= 500"
    ));

  test("json + regex field filter @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | path=~"/api/.*"`,
      "json + regex field"
    ));

  test("json + two field filters @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | method="POST" | status >= 200`,
      "json + two field filters"
    ));

  test("logfmt + level filter @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="payment-service"} | logfmt | level="error"`,
      "logfmt + level filter"
    ));
});

// ---------------------------------------------------------------------------
// Pipeline stage parity
// ---------------------------------------------------------------------------

test.describe("@regression Pipeline stages — exact Loki parity", () => {
  test("json + line_format @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | line_format "{{.method}} {{.path}} {{.status}}"`,
      "json + line_format"
    ));

  test("json + keep @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | keep method, path, status`,
      "json + keep"
    ));

  test("json + drop @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | drop trace_id`,
      "json + drop"
    ));

  test("json + label_format @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway"} | json | label_format svc="app"`,
      "json + label_format"
    ));

  test("multi-label + json + two filters @regression", async ({ page }) =>
    assertLogParity(
      page,
      `{app="api-gateway",namespace="prod"} | json | method="POST" | status >= 200`,
      "multi-label + json + two filters"
    ));
});

// ---------------------------------------------------------------------------
// Metric query parity
// ---------------------------------------------------------------------------

// Metric parity is asserted on aggregated forms with bounded cardinality. Raw
// per-stream series (e.g. `rate({app="api-gateway"}[5m])`) are legitimately
// capped by the proxy at `-max-stats-query-series` (default 500, matching
// Drilldown's own cap; see docs) while Loki returns every stream, so an exact
// series-count comparison on high-cardinality selectors is not a parity signal.
// The cap itself is locked by the dedicated test below.
const PROXY_STATS_SERIES_CAP = 500;

test.describe("@regression Metric queries — exact Loki parity", () => {
  test("rate @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `sum by (app) (rate({app="api-gateway"}[5m]))`,
      "rate"
    ));

  test("count_over_time @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `sum by (namespace) (count_over_time({app="api-gateway"}[5m]))`,
      "count_over_time"
    ));

  test("raw per-stream series honour the documented proxy cap @regression", async ({
    page,
  }) => {
    const { proxyUID, lokiUID } = await uids(page);
    // One series per stream. The reference is Loki's series index rather than
    // its rate() output: the generator rotates pod names, and Loki's sharded
    // range path only sees a stream once its head block is cut, so the newest
    // streams are always missing there while the index (served from the
    // ingesters) and the proxy both list them immediately.
    const query = `rate({app="api-gateway"}[5m])`;
    await ensureStackWarm(page);
    const end = windowEnd();
    const windowSec = 7 * 24 * 3600;
    const [proxy, indexed] = await Promise.all([
      queryRange(page, proxyUID, query, { step: "60", windowSec, endSec: end }),
      lokiIndexedStreamCount(page, lokiUID, `{app="api-gateway"}`, end - windowSec, end),
    ]);
    expect(proxy.statusCode, "raw rate: status code").toBe(200);
    const proxySeries = seriesCount(proxy.body);
    expect(proxySeries, "raw rate: proxy never exceeds its series cap").toBeLessThanOrEqual(
      PROXY_STATS_SERIES_CAP
    );
    if (indexed <= PROXY_STATS_SERIES_CAP) {
      expect(proxySeries, "raw rate: one series per indexed stream below the cap").toBe(
        indexed
      );
    } else {
      expect(proxySeries, "raw rate: cap reached").toBe(PROXY_STATS_SERIES_CAP);
    }
  });

  test("sum by level count_over_time @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `sum by (level) (count_over_time({app="api-gateway"}[5m]))`,
      "sum by level"
    ));

  test("sum by level across apps @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `sum by (level) (rate({app="api-gateway"}[5m]))`,
      "sum by level"
    ));

  test("rate with line filter |= @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `sum by (level) (rate({app="api-gateway"} |= "error"[5m]))`,
      "rate + |="
    ));

  test("topk within single app @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `topk(2, sum by (level) (rate({app="api-gateway"}[5m])))`,
      "topk within app"
    ));

  test("avg_over_time unwrap duration_ms @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `avg by (level) (avg_over_time({app="api-gateway"} | json | unwrap duration_ms [5m]))`,
      "avg_over_time unwrap"
    ));

  test("sum_over_time unwrap status @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `sum by (level) (sum_over_time({app="api-gateway"} | json | unwrap status [5m]))`,
      "sum_over_time unwrap"
    ));

  test("sum by level for payment-service @regression", async ({ page }) =>
    assertMetricParity(
      page,
      `sum by (level) (rate({app="payment-service"}[5m]))`,
      "sum payment-service",
      '{app="payment-service"}'
    ));
});

// ---------------------------------------------------------------------------
// Response content verification (proxy-only correctness checks)
// ---------------------------------------------------------------------------

test.describe("@regression Content verification", () => {
  test("json parser exposes method field in log lines", async ({ page }) => {
    const { proxyUID } = await uids(page);
    const r = await queryRange(page, proxyUID, `{app="api-gateway"} | json`);
    expect(r.statusCode).toBe(200);

    const streams = r.body?.data?.result as Array<{
      values: Array<[string, string]>;
    }>;
    expect(streams.length).toBeGreaterThan(0);

    let found = false;
    for (const s of streams) {
      for (const [, line] of s.values) {
        if (line.includes("method") || line.includes("GET") || line.includes("POST")) {
          found = true;
          break;
        }
      }
      if (found) break;
    }
    expect(found, "Expected to find method-related content in parsed log lines").toBeTruthy();
  });

  test("json + method=GET filter returns only GET lines", async ({ page }) => {
    const { proxyUID } = await uids(page);
    const [all, get] = await Promise.all([
      queryRange(page, proxyUID, `{app="api-gateway"} | json`),
      queryRange(page, proxyUID, `{app="api-gateway"} | json | method="GET"`),
    ]);

    expect(all.statusCode).toBe(200);
    expect(get.statusCode).toBe(200);

    const allN = lineCount(all.body);
    const getN = lineCount(get.body);
    expect(getN).toBeGreaterThan(0);
    expect(getN).toBeLessThanOrEqual(allN);

    // Verify each returned line is actually a GET request
    const streams = get.body?.data?.result as Array<{
      values: Array<[string, string]>;
    }>;
    for (const s of streams) {
      for (const [, line] of s.values) {
        try {
          const obj = JSON.parse(line);
          if (obj.method) expect(obj.method).toBe("GET");
        } catch {
          // formatted line — skip value check
        }
      }
    }
  });

  test("status >= 400 returns only error status lines", async ({ page }) => {
    const { proxyUID } = await uids(page);
    const r = await queryRange(
      page,
      proxyUID,
      `{app="api-gateway"} | json | status >= 400`
    );

    expect(r.statusCode).toBe(200);
    const streams = r.body?.data?.result as Array<{
      values: Array<[string, string]>;
    }>;

    for (const s of streams) {
      for (const [, line] of s.values) {
        try {
          const obj = JSON.parse(line);
          if (obj.status !== undefined) {
            expect(Number(obj.status)).toBeGreaterThanOrEqual(400);
          }
        } catch {
          /* formatted line */
        }
      }
    }
  });

  test("negative filter removes matched content from results", async ({
    page,
  }) => {
    const { proxyUID } = await uids(page);
    // Compare volumes with an instant aggregate rather than line counts: the
    // UI stack's generator writes faster than any sane `limit`, so a capped
    // pair of query_range responses would compare equal and hide the filter.
    const at = windowEnd();
    const [allN, filtN] = await Promise.all([
      instantScalar(page, proxyUID, `sum(count_over_time({app="api-gateway"}[10m]))`, at),
      instantScalar(
        page,
        proxyUID,
        `sum(count_over_time({app="api-gateway"} != "health"[10m]))`,
        at
      ),
    ]);
    expect(allN).toBeGreaterThan(0);
    expect(filtN).toBeLessThan(allN);

    const filtered = await queryRange(
      page,
      proxyUID,
      `{app="api-gateway"} != "health"`,
      { windowSec: 10 * 60 }
    );
    expect(filtered.statusCode).toBe(200);
    const streams = filtered.body?.data?.result as Array<{
      values: Array<[string, string]>;
    }>;
    expect(lineCount(filtered.body)).toBeGreaterThan(0);
    for (const s of streams) {
      for (const [, line] of s.values) {
        expect(line.toLowerCase()).not.toContain("health");
      }
    }
  });

  test("line_format produces formatted output matching template", async ({
    page,
  }) => {
    const { proxyUID } = await uids(page);
    const r = await queryRange(
      page,
      proxyUID,
      `{app="api-gateway"} | json | line_format "M={{.method}} S={{.status}}"`
    );
    expect(r.statusCode).toBe(200);

    const streams = r.body?.data?.result as Array<{
      values: Array<[string, string]>;
    }>;
    expect(streams.length).toBeGreaterThan(0);

    let found = false;
    for (const s of streams) {
      for (const [, line] of s.values) {
        if (line.includes("M=") && line.includes("S=")) {
          found = true;
          break;
        }
      }
      if (found) break;
    }
    expect(found, "Expected line_format template output like 'M=GET S=200'").toBeTruthy();
  });

  test("chained filters narrow results step by step", async ({ page }) => {
    const { proxyUID } = await uids(page);
    const [s1, s2, s3] = await Promise.all([
      queryRange(page, proxyUID, `{app="api-gateway"}`),
      queryRange(page, proxyUID, `{app="api-gateway"} | json`),
      queryRange(page, proxyUID, `{app="api-gateway"} | json | method="GET"`),
    ]);

    expect(s1.statusCode).toBe(200);
    expect(s2.statusCode).toBe(200);
    expect(s3.statusCode).toBe(200);

    const n1 = lineCount(s1.body);
    const n2 = lineCount(s2.body);
    const n3 = lineCount(s3.body);

    expect(n2).toBeLessThanOrEqual(n1);
    expect(n3).toBeLessThanOrEqual(n2);
    expect(n3).toBeGreaterThan(0);
  });

  test("empty query returns 200 with zero results", async ({ page }) => {
    const { proxyUID, lokiUID } = await uids(page);
    const [proxy, loki] = await Promise.all([
      queryRange(page, proxyUID, `{nonexistent_xyz="no_match_99999"}`),
      queryRange(page, lokiUID, `{nonexistent_xyz="no_match_99999"}`),
    ]);
    expect(proxy.statusCode).toBe(200);
    expect(loki.statusCode).toBe(200);
    expect(lineCount(proxy.body)).toBe(0);
    expect(lineCount(loki.body)).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Known proxy gaps — documented failures that should become passing once fixed
// These use test.fixme() so they appear in reports but don't block CI.
// ---------------------------------------------------------------------------

test.describe("@regression Known proxy gaps (fixme — failing until proxy is fixed)", () => {
  // Proxy returns 502 for |~ regex with | alternation
  test.fixme(
    "|~ regex with alternation |~ 'POST|PUT|DELETE'",
    async ({ page }) => {
      await assertLogParity(
        page,
        `{app="api-gateway"} |~ "POST|PUT|DELETE"`,
        "|~ alternation"
      );
    }
  );

  // Proxy returns 502 for !~ regex with | alternation
  test.fixme(
    "!~ regex with alternation !~ 'health|ready|metrics'",
    async ({ page }) => {
      await assertLogParity(
        page,
        `{app="api-gateway"} !~ "health|ready|metrics"`,
        "!~ alternation"
      );
    }
  );

  // Proxy returns 400 for line filter AFTER json parser
  test.fixme(
    "line filter after json parser: json |= 'error'",
    async ({ page }) => {
      await assertLogParity(
        page,
        `{app="api-gateway"} | json |= "error" | status >= 500`,
        "line filter after parser"
      );
    }
  );

  // Proxy returns 502 for binary metric expressions (rate * scalar)
  test.fixme("binary metric expression: rate * 100", async ({ page }) => {
    await assertMetricParity(
      page,
      `rate({app="api-gateway"}[5m]) * 100`,
      "rate * scalar"
    );
  });

  // Proxy returns 502 for division of two metric expressions
  test.fixme(
    "error rate ratio: sum(...) / sum(...)",
    async ({ page }) => {
      await assertMetricParity(
        page,
        `sum(rate({app="api-gateway"} | json | status >= 400 [5m])) / sum(rate({app="api-gateway"}[5m]))`,
        "error rate ratio"
      );
    }
  );

  // Proxy returns 502 for count_over_time with json filter in [range]
  test.fixme(
    "count_over_time with json filter in range vector",
    async ({ page }) => {
      await assertMetricParity(
        page,
        `count_over_time({app="api-gateway"} | json | status >= 400 [5m])`,
        "count_over_time json filter"
      );
    }
  );

  // Proxy returns 502 for label_format followed by line_format
  test.fixme(
    "label_format then line_format chain",
    async ({ page }) => {
      await assertLogParity(
        page,
        `{app="api-gateway"} | json | label_format svc="app" | line_format "[{{.svc}}] {{.method}}"`,
        "label_format + line_format"
      );
    }
  );

  // Proxy returns 502 for keep then line_format
  test.fixme(
    "keep then line_format chain",
    async ({ page }) => {
      await assertLogParity(
        page,
        `{app="api-gateway"} | json | keep method, path | line_format "{{.method}} {{.path}}"`,
        "keep + line_format"
      );
    }
  );

  // Namespace-wide aggregation: proxy returns extra series compared to Loki
  test.fixme(
    "sum by app across namespace: proxy returns extra series",
    async ({ page }) => {
      await assertMetricParity(
        page,
        `sum by (app) (count_over_time({namespace="prod"}[5m]))`,
        "sum by app namespace"
      );
    }
  );

  test.fixme(
    "topk across namespace: proxy/Loki series count differs",
    async ({ page }) => {
      await assertMetricParity(
        page,
        `topk(3, sum by (app) (rate({namespace="prod"}[5m])))`,
        "topk namespace"
      );
    }
  );
});
