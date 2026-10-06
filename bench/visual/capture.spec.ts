// Opens every page of the spec for every datasource and range, saves a
// screenshot and the Grafana backend traffic (/api/ds/query bodies and
// responses, datasource resource calls) of that page load.
//
//   GRAFANA_URL=http://127.0.0.1:33002 VP_OUT=out VP_END=<unix> npx playwright test
//   VP_PAGES=explore-A,dd-fields VP_RANGES=15m,1h   (optional filters)
//   VP_PLAN=plan.json   the CI plan of plan.py: its entries and their own ranges
//                       (replaces VP_PAGES / VP_RANGES; VP_LIVE=1 runs only the tail pages, else only the static ones)
//
// Windows are absolute (end = the seed end), so all datasources see the same
// window and the data is static.
import { test, Page } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

type Ds = { id: string; uid: string };
type PageSpec = {
  id: string;
  kind: "explore" | "label-browser" | "drilldown" | "tail";
  query?: string;
  path?: string; // drilldown path below /explore, {service} {label} {field} expanded
  service?: boolean; // drilldown: filter by var-filters service_name
  ranges?: string[];
};
const spec = JSON.parse(fs.readFileSync(process.env.VP_SPEC || path.join(__dirname, "spec.json"), "utf8"));
const OUT = process.env.VP_OUT || "out";
const END = parseInt(process.env.VP_END || "0") || Math.floor(Date.now() / 60000) * 60;
const only = (v?: string) => (v ? new Set(v.split(",")) : null);
const pagesOnly = only(process.env.VP_PAGES);
const rangesOnly = only(process.env.VP_RANGES);
const plan: Record<string, { ranges: string[] }> | null = process.env.VP_PLAN
  ? JSON.parse(fs.readFileSync(process.env.VP_PLAN, "utf8")).entries : null;
const dss: Ds[] = spec.datasources;
const vars: Record<string, string> = spec.vars || {};
const expand = (s: string) => s.replace(/\{(\w+)\}/g, (_, k) => encodeURIComponent(vars[k] ?? ""));

function rangeOf(r: string) {
  const to = END * 1000;
  return { from: String(to - spec.ranges[r] * 1000), to: String(to) };
}

function url(p: PageSpec, uid: string, r: string): string {
  const { from, to } = p.kind === "tail" ? { from: "now-5m", to: "now" } : rangeOf(r);
  if (p.kind === "drilldown") {
    const q = new URLSearchParams({
      patterns: "[]", from, to, timezone: "browser", "var-lineFormat": "", "var-ds": uid,
      "var-filters": p.service ? `service_name|=|${vars.service}` : "", "var-fields": "", "var-levels": "",
      "var-metadata": "", "var-jsonFields": "", "var-all-fields": "", "var-patterns": "",
      "var-lineFilterV2": "", "var-lineFilters": "", "var-primary_label": "service_name|=~|.+",
      ...(p.service ? { displayedFields: "[]", urlColumns: "[]" } : {}),
    });
    return `/a/grafana-lokiexplore-app/explore${p.path ? "/" + expand(p.path) : ""}?${q}`;
  }
  const pane = {
    A: {
      datasource: uid,
      queries: [{ refId: "A", expr: p.query || "", queryType: "range", datasource: { type: "loki", uid },
        editorMode: "code", direction: "backward" }],
      range: { from, to }, compact: false,
    },
  };
  return `/explore?${new URLSearchParams({ schemaVersion: "1", panes: JSON.stringify(pane), orgId: "1" })}`;
}

// What Grafana shows once the page settled: panels that say "No data" and error
// banners or crashed panels. compare.py reports the ones the PR side shows and the base does not.
async function uiState(page: Page) {
  return page.evaluate(() => {
    const text = document.body.innerText || "";
    const banner = /(Plugin (unavailable|failed|not found)|Failed to load|Unable to load|Something went wrong|An unexpected error|Error loading|Query error|Bad Gateway|Internal Server Error|Cannot read propert)[^\n]{0,80}/g;
    const banners = [...new Set(text.match(banner) || [])].sort().slice(0, 8);
    // The collapsed "Details" of an error boundary (Grafana's "Plugin failed to load") carry the error and its stack.
    const details = banners.length ? [...document.querySelectorAll("details")].map((d) => (d.textContent || "").trim()).filter(Boolean).join("\n").slice(0, 2000) : "";
    return {
      noData: (text.match(/No data/g) || []).length,
      banners,
      // Visible errors only: Explore keeps a hidden "Alert error" in the scroll view whenever results render. A query
      // the datasource rejects shows in the query row (warning icon, no test id), not in a panel.
      panelErrors: [...document.querySelectorAll('[data-testid="data-testid Panel status error"], [data-testid="data-testid Alert error"], [data-testid="data-testid Error boundary"], [data-testid="data-testid Query editor row"] [data-testid="icon-exclamation-triangle"]')]
        .filter((e) => e.getClientRects().length > 0).length,
      ...(details ? { details } : {}),
    };
  }).catch(() => ({ noData: 0, banners: ["page state unreadable"], panelErrors: 0 }));
}

// Settled = no backend request in flight for QUIET ms. A page that issues its requests in waves (Drilldown field
// breakdowns) can pause longer than the default on a busy host; the re-capture pass of ci_run.py waits longer.
const QUIET = parseInt(process.env.VP_QUIET_MS || "3000");

async function settle(page: Page, pending: { n: number; last: number; seen?: number }, max = parseInt(process.env.VP_SETTLE_MS || "90000")) {
  const t0 = Date.now();
  await page.waitForTimeout(3000);
  while (Date.now() - t0 < max) {
    if ((pending.seen || 0) > 0 && pending.n === 0 && Date.now() - pending.last > QUIET) return true;
    await page.waitForTimeout(500);
  }
  return false;
}

const BACKEND = /\/api\/ds\/query|\/api\/datasources\/uid\/[^/]+\/resources\/|\/api\/datasources\/proxy\//;

// Logs Drilldown 2.5.2 crashes a breakdown page (Grafana: "Plugin failed to load") when a breakdown query is
// answered before the page's first time-series panel module has loaded: on the panel's first render the app's
// legend sync (ValueSummary.extendTimeSeriesLegendBus -> initLegendOptions) calls VizPanel.onFieldConfigChange,
// which reads the not yet loaded panel plugin's defaults and throws inside React. A fresh browser context loads
// that module anew on every capture, so a fast answer (a proxy cache hit, a re-load of the same page) loses the race
// on any datasource, Loki direct included. The data queries of a Drilldown page wait for the module (at most
// MODULE_WAIT_MS each); only their timing changes, the requests and answers are the same.
const PANEL_MODULE = /\/public\/build\/timeseriesPanel\.[^/]*\.js/;
const MODULE_WAIT_MS = 3000;

async function queriesAfterPanelModule(page: Page) {
  let loaded: () => void = () => {};
  const ready = new Promise<void>((res) => { loaded = res; });
  const done = (rq: any) => { if (PANEL_MODULE.test(rq.url())) loaded(); };
  page.on("requestfinished", done);
  page.on("requestfailed", done);
  await page.route(/\/api\/ds\/query/, async (route) => {
    await Promise.race([ready, new Promise((res) => setTimeout(res, MODULE_WAIT_MS))]);
    await route.fallback();
  });
}

// Live tail: the three datasources stream at the same time (the log generator
// mirrors every line to Loki and VictoriaLogs); the frames of every websocket
// of the page are saved as they arrive.
async function tail(browser: any, p: PageSpec, ds: Ds) {
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  const frames: any[] = [];
  const t0 = Date.now();
  page.on("websocket", (ws: any) => {
    ws.on("framereceived", (f: any) => frames.push({ t: Date.now() - t0, url: ws.url().replace(/^wss?:\/\/[^/]+/, ""), payload: String(f.payload).slice(0, 200_000) }));
  });
  await page.goto(url(p, ds.uid, "live"), { waitUntil: "commit" });
  await page.getByRole("button", { name: /Start live stream/i }).first().click({ timeout: 30_000 });
  await page.waitForTimeout((parseInt(process.env.VP_TAIL_SECONDS || "25")) * 1000);
  // spec ids name files under OUT: keep them single path segments
  const pid = p.id.replace(/[^A-Za-z0-9_-]/g, "_");
  const dsid = ds.id.replace(/[^A-Za-z0-9_-]/g, "_");
  const dir = path.join(OUT, "shots", pid, "live");
  const ddir = path.join(OUT, "data", pid, "live");
  fs.mkdirSync(dir, { recursive: true });
  fs.mkdirSync(ddir, { recursive: true });
  await page.screenshot({ path: path.join(dir, `${dsid}.png`) });
  const ui = await uiState(page);
  fs.writeFileSync(path.join(ddir, `${dsid}.json`), JSON.stringify({ settled: true, records: [], frames, ui }));
  await ctx.close();
}

// A fresh Grafana loads the Logs Drilldown app and the Explore bundle on first use, which can outlast the
// settle limit of the first capture (the base side, which goes first). VP_WARMUP=1 loads one page of each
// kind through the first datasource and saves nothing.
if (process.env.VP_WARMUP === "1") {
  test("warmup", async ({ browser }) => {
    const first = spec.pages.find((x: PageSpec) => x.kind === "drilldown" && !x.path);
    const explore = spec.pages.find((x: PageSpec) => x.kind === "explore");
    for (const p of [first, explore].filter(Boolean) as PageSpec[]) {
      const ctx = await browser.newContext();
      const page = await ctx.newPage();
      const pending = { n: 0, last: Date.now(), seen: 0 };
      page.on("request", (rq) => { if (BACKEND.test(rq.url())) { pending.n++; pending.seen++; pending.last = Date.now(); } });
      page.on("requestfinished", (rq) => { if (BACKEND.test(rq.url())) { pending.n--; pending.last = Date.now(); } });
      page.on("requestfailed", (rq) => { if (BACKEND.test(rq.url())) { pending.n--; pending.last = Date.now(); } });
      await page.goto(url(p, dss[0].uid, spec.core_range), { waitUntil: "commit" });
      await settle(page, pending, 120_000);
      await ctx.close();
    }
  });
}

for (const p of spec.pages as PageSpec[]) {
  if (process.env.VP_WARMUP === "1") break;
  if (pagesOnly && !pagesOnly.has(p.id)) continue;
  if (plan && !plan[p.id]) continue;
  // With a plan the tail pages run in their own invocation (VP_LIVE=1), while the live generator is up.
  if (plan && (p.kind === "tail") !== (process.env.VP_LIVE === "1")) continue;
  if (p.kind === "tail") {
    if (plan ? plan[p.id].ranges.includes("live") : !rangesOnly || rangesOnly.has("live")) {
      test(`${p.id} live`, async ({ browser }) => {
        await Promise.all(dss.map((ds) => tail(browser, p, ds)));
      });
    }
    continue;
  }
  for (const r of plan ? plan[p.id].ranges : p.ranges || spec.default_ranges) {
    if (rangesOnly && !rangesOnly.has(r)) continue;
    test(`${p.id} ${r}`, async ({ browser }) => {
      for (const ds of dss) {
        // Loki is not asked for ranges far beyond the history it holds (it stalls on them).
        if (ds.id === "loki" && spec.ranges[r] > (spec.loki_capture_max_s || 1e12)) continue;
        // A Grafana whose Loki plugin process was killed (host memory pressure) answers
        // "plugin unavailable"; the page load is repeated, never recorded as the datasource's answer.
        for (let attempt = 0; attempt < 3; attempt++) {
        const ctx = await browser.newContext();
        const page = await ctx.newPage();
        const pending = { n: 0, last: Date.now() };
        const records: any[] = [];
        const errors: string[] = [];
        page.on("console", (m) => { if (m.type() === "error" && errors.length < 20) errors.push(m.text().slice(0, 1500)); });
        page.on("pageerror", (e) => { if (errors.length < 20) errors.push(`pageerror: ${String(e.stack || e).slice(0, 1500)}`); });
        if (p.kind === "drilldown") await queriesAfterPanelModule(page);
        page.on("request", (rq) => {
          if (BACKEND.test(rq.url())) { pending.n++; pending.seen = (pending.seen || 0) + 1; pending.last = Date.now(); }
        });
        page.on("requestfailed", (rq) => {
          if (BACKEND.test(rq.url())) { pending.n--; pending.last = Date.now(); }
        });
        page.on("response", async (rs) => {
          const rq = rs.request();
          if (!BACKEND.test(rq.url())) return;
          let body: any = null;
          try { body = await rs.json(); } catch { try { body = await rs.text(); } catch { body = null; } }
          records.push({ url: rq.url().replace(/^https?:\/\/[^/]+/, ""), method: rq.method(),
            request: rq.postDataJSON?.() ?? null, status: rs.status(), response: body });
          pending.n--; pending.last = Date.now();
        });
        const t0 = Date.now();
        await page.goto(url(p, ds.uid, r), { waitUntil: "commit" });
        if (p.kind === "label-browser") {
          await page.getByRole("button", { name: /Label browser/i }).first().click({ timeout: 30_000 }).catch(() => {});
          await page.waitForTimeout(1500);
          await page.getByText("service_name", { exact: true }).first().click({ timeout: 10_000 }).catch(() => {});
        }
        const ok = await settle(page, pending);
        const unavailable = records.some((x) => x.status === 500 && /plugin\.(unavailable|connectionUnavailable)/.test(JSON.stringify(x.response)));
        // A page that never settled and drew nothing was caught mid-start (or by a dying plugin process): load it again.
        const empty = !ok && !records.some((x) => x.status === 200);
        // The app plugin crashed or its bundle did not load ("Plugin failed to load", see queriesAfterPanelModule):
        // load it again in a fresh browser context; the details and console errors of the last attempt are recorded.
        const pluginLoad = (await uiState(page)).banners.some((b) => /^Plugin (failed|unavailable|not found)/.test(b));
        if ((unavailable || empty || pluginLoad) && attempt < 2) { await ctx.close(); await new Promise((res) => setTimeout(res, 5000)); continue; }
        const dir = path.join(OUT, "shots", p.id, r);
        const ddir = path.join(OUT, "data", p.id, r);
        fs.mkdirSync(dir, { recursive: true });
        fs.mkdirSync(ddir, { recursive: true });
        await page.screenshot({ path: path.join(dir, `${ds.id}.png`) });
        const ui = await uiState(page);
        fs.writeFileSync(path.join(ddir, `${ds.id}.json`), JSON.stringify({ settled: ok, settle_ms: pending.last - t0, attempts: attempt + 1, records, ui, errors }));
        await ctx.close();
        break;
        }
      }
    });
  }
}
