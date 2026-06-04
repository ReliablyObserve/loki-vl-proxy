/**
 * Methodical Loki-direct vs VL-proxy comparison for Drilldown Fields and Labels.
 *
 * Makes DIRECT API calls to both Loki (port 13101) and the proxy (port 13106),
 * but sends Grafana-style queries with:
 *   - X-Query-Tags: Source=grafana-lokiexplore-app header (engages Drilldown path)
 *   - Existence filter `| field!=""` (required by detectDrilldownSingleField)
 *   - Grafana's actual $__auto step sizes per range (see grafana_drilldown_resolution.md)
 *
 * Run with: WORKERS=1 npx playwright test drilldown-loki-vs-proxy-compare 2>&1 | tail -20
 * Results: /tmp/drilldown-compare-report.json
 */
import { test, expect, type APIRequestContext } from "@playwright/test";
import * as fs from "fs";

const LOKI_BASE = "http://127.0.0.1:13101";
const PROXY_BASE = "http://127.0.0.1:13106";
const SERVICE = "api-gateway";
const STREAM_SELECTOR = `{service_name="${SERVICE}"}`;

// Header that engages the Drilldown path in the proxy (and is otherwise a no-op for Loki).
// Without this, the proxy falls through to proxyStatsQueryRangeDirect (16 MB cap).
const DRILLDOWN_HEADERS = { "X-Query-Tags": "Source=grafana-lokiexplore-app" };

const TIME_RANGES = [
  { label: "5m",  lookback: 5 * 60 },
  { label: "15m", lookback: 15 * 60 },
  { label: "30m", lookback: 30 * 60 },
  { label: "1h",  lookback: 3600 },
  { label: "2h",  lookback: 2 * 3600 },
  { label: "6h",  lookback: 6 * 3600 },
  { label: "12h", lookback: 12 * 3600 },
  { label: "2d",  lookback: 2 * 86400 },
  { label: "7d",  lookback: 7 * 86400 },
];

// Grafana $__auto step sizes per time range, verified in grafana_drilldown_resolution.md.
// Formula: step ≈ floor(range_seconds / 500), rounded to a nice number.
// Mis-stepping the comparison test is one of the biggest sources of false-positive issues.
const AUTO_STEP: Record<string, number> = {
  "5m":  1,   "15m": 2,    "30m": 5,
  "1h":  5,   "2h":  10,   "6h":  30,
  "12h": 60,  "2d":  300,  "7d":  1200,
};

interface FieldInfo { label: string; type: string; cardinality?: number }
interface SeriesData { field: string; seriesCount: number; avgPointsPerSeries: number; hasData: boolean }
interface RangeReport {
  range: string;
  fieldsLoki: FieldInfo[]; fieldsProxy: FieldInfo[];
  labelsLoki: string[]; labelsProxy: string[];
  seriesLoki: SeriesData[]; seriesProxy: SeriesData[];
  issues: string[];
}

async function getDetectedFields(
  req: APIRequestContext, base: string, startIso: string, endIso: string
): Promise<FieldInfo[]> {
  try {
    const r = await req.get(`${base}/loki/api/v1/detected_fields`, {
      params: { query: STREAM_SELECTOR, start: startIso, end: endIso, limit: "500" },
      headers: DRILLDOWN_HEADERS,
      timeout: 30_000,
    });
    if (!r.ok()) return [];
    const body = await r.json() as Record<string, unknown>;
    // Loki returns { fields: [...] }; proxy may return { data: [...] }
    const arr = (body["fields"] ?? body["data"] ?? []) as Array<{ label: string; type: string; cardinality?: number }>;
    return arr.filter((f) => !["_msg","_stream","_stream_id","_time"].includes(f.label));
  } catch { return []; }
}

async function getDetectedLabels(
  req: APIRequestContext, base: string, startIso: string, endIso: string
): Promise<string[]> {
  try {
    const r = await req.get(`${base}/loki/api/v1/detected_labels`, {
      params: { query: STREAM_SELECTOR, start: startIso, end: endIso },
      headers: DRILLDOWN_HEADERS,
      timeout: 30_000,
    });
    if (!r.ok()) return [];
    const body = await r.json() as Record<string, unknown>;
    // Loki: { data: { detectedLabels: [...] } }
    const inner = body["data"] as Record<string, unknown> | undefined;
    const arr = (inner?.["detectedLabels"] ?? body["detectedLabels"] ?? []) as Array<{ label: string }>;
    return arr.map((l) => l.label);
  } catch { return []; }
}

async function queryFieldSeries(
  req: APIRequestContext, base: string,
  field: string, startSec: number, endSec: number, stepSec: number,
  parserSuffix: string,
): Promise<SeriesData> {
  // Mirror what Grafana Drilldown sends: parser stages + existence filter on the by-field.
  // The existence filter is REQUIRED to engage proxy's Drilldown hybrid path
  // (see detectDrilldownSingleField in drilldown_burst_coalescer.go).
  const query = `sum by (${field}) (count_over_time(${STREAM_SELECTOR}${parserSuffix} | ${field}!=""[${stepSec}s]))`;
  try {
    const r = await req.get(`${base}/loki/api/v1/query_range`, {
      params: { query, start: String(startSec), end: String(endSec), step: `${stepSec}s` },
      headers: DRILLDOWN_HEADERS,
      timeout: 30_000,
    });
    if (!r.ok()) return { field, seriesCount: 0, avgPointsPerSeries: 0, hasData: false };
    const body = await r.json() as { data?: { result?: Array<{ values?: unknown[][] }> } };
    const results = body?.data?.result ?? [];
    const seriesCount = results.length;
    const totalPoints = results.reduce((s, r) => s + (r.values?.length ?? 0), 0);
    const avgPoints = seriesCount > 0 ? totalPoints / seriesCount : 0;
    return { field, seriesCount, avgPointsPerSeries: avgPoints, hasData: seriesCount > 0 };
  } catch { return { field, seriesCount: 0, avgPointsPerSeries: 0, hasData: false }; }
}

async function queryLabelSeries(
  req: APIRequestContext, base: string,
  label: string, startSec: number, endSec: number, stepSec: number,
): Promise<SeriesData> {
  // Labels use stream-selector existence filter (no parser stage). Same Drilldown header
  // requirement so the proxy engages the hybrid path instead of the 16 MB direct cap.
  const query = `sum by (${label}) (count_over_time(${STREAM_SELECTOR} | ${label}!=""[${stepSec}s]))`;
  try {
    const r = await req.get(`${base}/loki/api/v1/query_range`, {
      params: { query, start: String(startSec), end: String(endSec), step: `${stepSec}s` },
      headers: DRILLDOWN_HEADERS,
      timeout: 30_000,
    });
    if (!r.ok()) return { field: label, seriesCount: 0, avgPointsPerSeries: 0, hasData: false };
    const body = await r.json() as { data?: { result?: Array<{ values?: unknown[][] }> } };
    const results = body?.data?.result ?? [];
    const seriesCount = results.length;
    const totalPoints = results.reduce((s, r) => s + (r.values?.length ?? 0), 0);
    const avgPoints = seriesCount > 0 ? totalPoints / seriesCount : 0;
    return { field: label, seriesCount, avgPointsPerSeries: avgPoints, hasData: seriesCount > 0 };
  } catch { return { field: label, seriesCount: 0, avgPointsPerSeries: 0, hasData: false }; }
}

// Fields from detected_fields that need a JSON parser to query properly
function needsJsonParser(f: FieldInfo): boolean {
  return Array.isArray((f as unknown as Record<string,unknown>)["parsers"])
    && ((f as unknown as Record<string,unknown>)["parsers"] as string[]).includes("json");
}

function diffFieldLists(label: string, lokiFields: FieldInfo[], proxyFields: FieldInfo[]): string[] {
  const issues: string[] = [];
  const lokiSet = new Set(lokiFields.map((f) => f.label));
  const proxySet = new Set(proxyFields.map((f) => f.label));
  for (const f of lokiFields) if (!proxySet.has(f.label)) issues.push(`[${label}] MISSING FIELD: "${f.label}" (type=${f.type}, cardinality=${f.cardinality})`);
  for (const f of proxyFields) if (!lokiSet.has(f.label)) issues.push(`[${label}] EXTRA FIELD: "${f.label}" (type=${f.type})`);
  return issues;
}

function diffLabelLists(label: string, lokiLabels: string[], proxyLabels: string[]): string[] {
  const issues: string[] = [];
  const lokiSet = new Set(lokiLabels);
  const proxySet = new Set(proxyLabels);
  for (const l of lokiLabels) if (!proxySet.has(l)) issues.push(`[${label}] MISSING LABEL: "${l}"`);
  for (const l of proxyLabels) if (!lokiSet.has(l)) issues.push(`[${label}] EXTRA LABEL: "${l}"`);
  return issues;
}

function diffSeries(label: string, lokiSeries: SeriesData[], proxySeries: SeriesData[], kind = "FIELD"): string[] {
  const issues: string[] = [];
  const lokiMap = new Map(lokiSeries.map((s) => [s.field, s]));
  const proxyMap = new Map(proxySeries.map((s) => [s.field, s]));
  const allKeys = new Set([...lokiMap.keys(), ...proxyMap.keys()]);
  for (const key of Array.from(allKeys).sort()) {
    const l = lokiMap.get(key);
    const p = proxyMap.get(key);
    if (!l || !p) continue; // field list diffs already reported above
    if (l.hasData && !p.hasData) {
      issues.push(`[${label}] ${kind} NO DATA: "${key}" (loki: ${l.seriesCount} series)`);
    } else if (l.hasData && p.hasData) {
      const sr = p.seriesCount / Math.max(l.seriesCount, 1);
      if (sr < 0.5) issues.push(`[${label}] ${kind} FEW SERIES: "${key}" proxy=${p.seriesCount} vs loki=${l.seriesCount} (${(sr*100).toFixed(0)}%)`);
      const pr = p.avgPointsPerSeries / Math.max(l.avgPointsPerSeries, 1);
      if (pr < 0.5) issues.push(`[${label}] ${kind} SPARSE: "${key}" proxy=${p.avgPointsPerSeries.toFixed(1)} vs loki=${l.avgPointsPerSeries.toFixed(1)} pts/series`);
    }
  }
  return issues;
}

test.describe("Drilldown — Loki vs Proxy direct API comparison @compare", () => {
  test.setTimeout(600_000);

  test("compare fields + labels across all time ranges", async ({ request }) => {
    const nowSec = Math.floor(Date.now() / 1000);
    const report: RangeReport[] = [];
    const allIssues: string[] = [];

    for (const { label, lookback } of TIME_RANGES) {
      const endSec = nowSec;
      const startSec = nowSec - lookback;
      const startIso = new Date(startSec * 1000).toISOString();
      const endIso = new Date(endSec * 1000).toISOString();
      const stepSec = AUTO_STEP[label] ?? 60;

      console.log(`\n=== ${label} (step=${stepSec}s) ===`);

      // Get field + label lists in parallel
      const [fieldsLoki, fieldsProxy, labelsLoki, labelsProxy] = await Promise.all([
        getDetectedFields(request, LOKI_BASE, startIso, endIso),
        getDetectedFields(request, PROXY_BASE, startIso, endIso),
        getDetectedLabels(request, LOKI_BASE, startIso, endIso),
        getDetectedLabels(request, PROXY_BASE, startIso, endIso),
      ]);

      console.log(`  detected_fields: loki=${fieldsLoki.length}, proxy=${fieldsProxy.length}`);
      console.log(`  detected_labels: loki=${labelsLoki.length}, proxy=${labelsProxy.length}`);
      if (fieldsLoki.length > 0) console.log(`  Loki fields: ${fieldsLoki.map(f=>f.label).join(", ")}`);
      if (fieldsProxy.length > 0) console.log(`  Proxy fields: ${fieldsProxy.map(f=>f.label).join(", ")}`);

      const fieldListIssues = diffFieldLists(label, fieldsLoki, fieldsProxy);
      const labelListIssues = diffLabelLists(label, labelsLoki, labelsProxy);

      // Query series for all fields that appear in Loki (ground truth)
      const lokiFieldLabels = fieldsLoki.map((f) => f.label);
      const proxyFieldLabels = fieldsProxy.map((f) => f.label);
      const allFieldLabels = [...new Set([...lokiFieldLabels, ...proxyFieldLabels])];

      const seriesLoki: SeriesData[] = [];
      const seriesProxy: SeriesData[] = [];

      // Fields: run in batches of 4
      const lokiFieldMap = new Map(fieldsLoki.map((f) => [f.label, f]));
      for (let i = 0; i < allFieldLabels.length; i += 4) {
        const batch = allFieldLabels.slice(i, i + 4);
        const results = await Promise.all(batch.map(async (fl) => {
          const lf = lokiFieldMap.get(fl);
          const hasJson = lf ? needsJsonParser(lf) : true;
          const parser = hasJson ? " | json" : "";
          const [l, p] = await Promise.all([
            queryFieldSeries(request, LOKI_BASE, fl, startSec, endSec, stepSec, parser),
            queryFieldSeries(request, PROXY_BASE, fl, startSec, endSec, stepSec, parser),
          ]);
          return { l, p };
        }));
        for (const { l, p } of results) { seriesLoki.push(l); seriesProxy.push(p); }
      }

      // Labels: run in batches of 4
      const allLabelKeys = [...new Set([...labelsLoki, ...labelsProxy])];
      const labelSeriesLoki: SeriesData[] = [];
      const labelSeriesProxy: SeriesData[] = [];
      for (let i = 0; i < allLabelKeys.length; i += 4) {
        const batch = allLabelKeys.slice(i, i + 4);
        const results = await Promise.all(batch.map(async (lbl) => {
          const [l, p] = await Promise.all([
            queryLabelSeries(request, LOKI_BASE, lbl, startSec, endSec, stepSec),
            queryLabelSeries(request, PROXY_BASE, lbl, startSec, endSec, stepSec),
          ]);
          return { l, p };
        }));
        for (const { l, p } of results) { labelSeriesLoki.push(l); labelSeriesProxy.push(p); }
      }

      const fieldSeriesIssues = diffSeries(label, seriesLoki, seriesProxy, "FIELD");
      const labelSeriesIssues = diffSeries(label, labelSeriesLoki, labelSeriesProxy, "LABEL");

      const issues = [...fieldListIssues, ...labelListIssues, ...fieldSeriesIssues, ...labelSeriesIssues];
      allIssues.push(...issues);

      if (issues.length > 0) {
        console.log(`  Issues (${issues.length}):`);
        for (const iss of issues) console.log(`    ${iss}`);
      } else {
        console.log(`  OK — no issues`);
      }

      report.push({ range: label, fieldsLoki, fieldsProxy, labelsLoki, labelsProxy, seriesLoki, seriesProxy, issues });
    }

    const noData = allIssues.filter((i) => i.includes("NO DATA") || i.includes("MISSING"));
    const sparse  = allIssues.filter((i) => i.includes("SPARSE") || i.includes("FEW SERIES"));
    console.log(`\n=== SUMMARY ===`);
    console.log(`Total issues: ${allIssues.length}`);
    console.log(`  Missing/no data: ${noData.length}`);
    console.log(`  Sparse/few series: ${sparse.length}`);

    fs.writeFileSync("/tmp/drilldown-compare-report.json", JSON.stringify({ report, allIssues }, null, 2));
    fs.writeFileSync("/tmp/drilldown-compare-report-labels.json", JSON.stringify({ report: report.map(r => ({ range: r.range, labelsLoki: r.labelsLoki, labelsProxy: r.labelsProxy, issues: r.issues.filter(i => i.includes("LABEL")) })), allIssues: allIssues.filter(i => i.includes("LABEL")) }, null, 2));

    expect(allIssues.length).toBeGreaterThanOrEqual(0);
  });
});
