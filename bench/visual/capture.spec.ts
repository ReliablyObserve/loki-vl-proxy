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
import { test, Page, Request, Response } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";
import { createHash } from "crypto";

type Ds = { id: string; uid: string };
type PageSpec = {
  id: string;
  kind: "explore" | "label-browser" | "log-context" | "drilldown" | "tail";
  query?: string;
  case_id?: string; // query owned by the existing typed quality matrix
  path?: string; // drilldown path below /explore, {service} {label} {field} expanded
  service?: boolean; // drilldown: filter by var-filters service_name
  serviceName?: string; // drilldown: that service instead of vars.service
  levels?: string; // drilldown: the level pill, as var-levels (detected_level|=|error)
  fields?: string; // drilldown: a parsed field filter, as var-fields (method|=|GET)
  ranges?: string[];
};
const spec = JSON.parse(fs.readFileSync(process.env.VP_SPEC || path.join(__dirname, "spec.json"), "utf8"));
const casePages = (spec.pages as PageSpec[]).filter(p => p.case_id);
if (casePages.length) {
  const matrix = JSON.parse(fs.readFileSync(path.join(__dirname, "../../test/e2e-compat/query-semantics-matrix.json"), "utf8"));
  for (const page of casePages) {
    const c = matrix.cases.find((entry: { id: string }) => entry.id === page.case_id);
    if (page.kind !== "explore" || page.query !== undefined || !c || c.endpoint !== "query_range" || c.expectation !== "success") {
      throw new Error(`${page.case_id}: runtime scene needs an exclusive successful query_range matrix case`);
    }
    page.query = c.query;
  }
}
const OUT = process.env.VP_OUT || "out";
const END = parseInt(process.env.VP_END || "0") || Math.floor(Date.now() / 60000) * 60;
const only = (v?: string) => (v ? new Set(v.split(",")) : null);
const pagesOnly = only(process.env.VP_PAGES);
const rangesOnly = only(process.env.VP_RANGES);
const plan: Record<string, { ranges: string[] }> | null = process.env.VP_PLAN
  ? JSON.parse(fs.readFileSync(process.env.VP_PLAN, "utf8")).entries : null;
const dss: Ds[] = spec.datasources;
const vars: Record<string, string> = spec.vars || {};
const expand = (s: string, service?: string) =>
  s.replace(/\{(\w+)\}/g, (_, k) => encodeURIComponent((k === "service" && service) || (vars[k] ?? "")));

function rangeOf(r: string) {
  const to = END * 1000;
  return { from: String(to - spec.ranges[r] * 1000), to: String(to) };
}

function url(p: PageSpec, uid: string, r: string): string {
  const { from, to } = p.kind === "tail" ? { from: "now-5m", to: "now" } : rangeOf(r);
  if (p.kind === "drilldown") {
    const q = new URLSearchParams({
      patterns: "[]", from, to, timezone: "browser", "var-lineFormat": "", "var-ds": uid,
      "var-filters": p.service ? `service_name|=|${p.serviceName || vars.service}` : "", "var-fields": p.fields || "",
      "var-levels": p.levels || "",
      "var-metadata": "", "var-jsonFields": "", "var-all-fields": p.fields || "", "var-patterns": "",
      "var-lineFilterV2": "", "var-lineFilters": "", "var-primary_label": "service_name|=~|.+",
      ...(p.service ? { displayedFields: "[]", urlColumns: "[]" } : {}),
    });
    return `/a/grafana-lokiexplore-app/explore${p.path ? "/" + expand(p.path, p.serviceName) : ""}?${q}`;
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
    const banner = /(Plugin (unavailable|failed|not found)|Failed to load|Unable to load|Something went wrong|An unexpected error|Error loading|Query error|Bad Gateway|Internal Server Error|Cannot read properties of)[^\n]{0,80}/g;
    const visible = (e: Element) => e.getClientRects().length > 0;
    const alerts = '[role="alert"], [data-testid="data-testid Alert error"], [data-testid="data-testid Error boundary"]';
    const alertText = [...document.querySelectorAll(alerts)]
      .filter(visible).map(e => e.textContent || "");
    // A service's log samples can themselves say "Cannot read property" or "Plugin unavailable". They are data,
    // not error banners. Read actual alerts first (even within panels), then retain the unstructured crash-page
    // fallback only outside log rows. The native landing page uses data-log-index and log-line-body even when
    // its syntax highlighting splits a message across spans. Other panel text and crash stacks remain eligible.
    const results = alerts + ', [data-log-index], .log-line-body, .log-syntax-highlight, [data-testid*="log-row"], [data-testid*="Log row"], [data-testid*="log-line"], [data-testid*="Log line"], [class*="logs-row"], script, style';
    const outside = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT | NodeFilter.SHOW_ELEMENT);
    const fragments: string[] = [];
    let previousBlock: Element | null = null;
    const blocks = 'address, article, aside, blockquote, div, details, dialog, dl, fieldset, figure, footer, form, h1, h2, h3, h4, h5, h6, header, li, main, nav, ol, p, pre, section, summary, table, td, th, tr, ul';
    while (outside.nextNode()) {
      const node = outside.currentNode;
      if (node.nodeType === 1) {
        if ((node as Element).tagName === "BR" && visible(node as Element)) {
          fragments.push("\n");
          previousBlock = null;
        }
        continue;
      }
      const parent = node.parentElement;
      if (!parent) continue;
      if (parent.closest(results)) {
        fragments.push("\n");
        previousBlock = null;
        continue;
      }
      if (!visible(parent)) continue;
      const block = parent.closest(blocks);
      if (block !== previousBlock) fragments.push("\n");
      fragments.push(node.textContent || "");
      previousBlock = block;
    }
    // Inline markup can split a real banner ("Plugin " + <b>unavailable</b>). Keep its text together, but do not
    // join separate block elements or bridge over discarded log rows to invent a failure phrase.
    alertText.push(fragments.join(""));
    const banners = [...new Set(alertText.flatMap(s => s.match(banner) || []))].sort().slice(0, 8);
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

// Grafana's "Show context" on the newest log row of an Explore logs page (Grafana 13: the row's "Log menu", then
// "Show context"). The context dialog asks /labels for the row's instant (start == end, in milliseconds) and builds
// its context query from the row's labels that answer lists; an empty answer gives the invalid query {}.
async function openLogContext(page: Page) {
  const row = page.locator('[data-testid="logRows"] tr, [data-testid="log-line"], [class*="log-line"]').first();
  await row.hover({ timeout: 30_000 }).catch(() => {});
  await page.waitForTimeout(500);
  const menu = page.getByRole("button", { name: /log menu|log line menu/i }).first();
  if (await menu.count()) await menu.click({ timeout: 10_000 }).catch(() => {});
  await page.waitForTimeout(500);
  await page.getByRole("menuitem", { name: /show context/i }).or(page.getByRole("button", { name: /show context/i }))
    .first().click({ timeout: 10_000 }).catch(() => {});
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

// What is kept of a response body above the retention cap: it was read once, completely, and is not retained.
type BodyDigest = { sha256: string; bytes: number; head: string; tail: string };
const digestOfBody = (body: Buffer): BodyDigest => ({ sha256: createHash("sha256").update(body).digest("hex"), bytes: body.length,
  head: body.subarray(0, 16 * 1024).toString("utf8"), tail: body.subarray(-4 * 1024).toString("utf8") });

// Independent CDP buffers retain proof bytes only inside Chrome. No body cache,
// refetch, request interception or URL-only matching is used by the fallback.
function startBodyFallback(cdp: any) {
  const maxBody = 32 * 1024 * 1024, maxDigestBody = 256 * 1024 * 1024;
  const maxIdentity = 256 * 1024, maxMetadata = 4 * 1024 * 1024, maxRequests = 1024;
  type Seen = { method: string; url: string; postData: string | null; status?: number; responseURLMatches?: boolean;
    finished?: boolean; failed?: boolean; redirected?: boolean };
  const seen = new Map<string, Seen>();
  let metadata = 0, unavailable = "", disposed = false, active = 0;
  const cancellations = new Set<() => void>();
  const request = ({ requestId, request, redirectResponse }: any) => {
    if (disposed || unavailable) return;
    const previous = seen.get(requestId);
    if (previous) { previous.redirected = true; return; }
    if (!BACKEND.test(request.url)) return;
    const postData = typeof request.postData === "string" ? request.postData : null;
    if (typeof request.method !== "string" || typeof request.url !== "string" ||
        (request.hasPostData && postData === null)) { unavailable = "request identity incomplete"; return; }
    const parts = [request.method, request.url, postData || ""];
    if (parts.some(part => part.length > maxIdentity)) { unavailable = "request identity limit"; return; }
    const bytes = parts.reduce((sum, part) => sum + Buffer.byteLength(part), 0);
    if (bytes > maxIdentity || metadata + bytes > maxMetadata || seen.size >= maxRequests) {
      unavailable = "request identity limit"; return;
    }
    metadata += bytes;
    seen.set(requestId, {method: request.method, url: request.url, postData, redirected: !!redirectResponse});
  };
  const response = ({ requestId, response }: any) => {
    const item = seen.get(requestId);
    if (item && !item.redirected) { item.status = response.status; item.responseURLMatches = response.url === item.url; }
  };
  const finished = ({ requestId }: any) => { const item = seen.get(requestId); if (item) item.finished = true; };
  const failed = ({ requestId }: any) => { const item = seen.get(requestId); if (item) item.failed = true; };
  const events: [string, (...args: any[]) => void][] = [
    ["Network.requestWillBeSent", request], ["Network.responseReceived", response],
    ["Network.loadingFinished", finished], ["Network.loadingFailed", failed],
  ];
  for (const [name, handler] of events) cdp.on(name, handler);
  return {
    async read(identity: {method: string; url: string; postData: string | null; status: number})
      : Promise<{body?: Buffer; error?: string; digest?: BodyDigest}> {
      const no = (error: string, digest?: BodyDigest) => ({error, ...(digest ? {digest} : {})});
      if (disposed) return no("collector disposed");
      if (unavailable) return no(unavailable);
      const matches = [...seen.entries()].filter(([, item]) => item.method === identity.method &&
        item.url === identity.url && item.postData === identity.postData);
      if (matches.length !== 1) return no(matches.length ? "ambiguous request identity" : "request identity missing");
      const [requestId, item] = matches[0];
      if (item.redirected) return no("redirected request");
      if (item.status !== identity.status || !item.responseURLMatches) return no("response status/identity mismatch");
      if (!item.finished || item.failed) return no("response incomplete or failed");
      if (active >= 2) return no("concurrent body read limit");
      active++;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let cancelRead: () => void = () => {};
      try {
        const deadline = new Promise<never>((_, reject) => {
          cancelRead = () => reject(new Error("collector disposed"));
          cancellations.add(cancelRead);
          timer = setTimeout(() => { unavailable = "body read timed out"; reject(new Error(unavailable)); }, 2000);
        });
        const value = await Promise.race([cdp.send("Network.getResponseBody", {requestId}), deadline]);
        if (disposed) return no("collector disposed");
        if (typeof value?.body !== "string" || typeof value.base64Encoded !== "boolean") return no("body unavailable");
        const encoding = value.base64Encoded ? "base64" : "utf8";
        if (value.body.length > (value.base64Encoded ? Math.ceil(maxBody / 3) * 4 : maxBody) ||
            Buffer.byteLength(value.body, encoding) > maxBody) {
          // Over the retention cap: not kept, but proved complete by its digest and length (up to maxDigestBody).
          const size = Buffer.byteLength(value.body, encoding);
          return no("body byte limit", size <= maxDigestBody ? digestOfBody(Buffer.from(value.body, encoding)) : undefined);
        }
        const body = Buffer.from(value.body, encoding);
        if (value.base64Encoded && body.toString("base64") !== value.body) return no("invalid body encoding");
        return {body};
      } catch {
        return no(unavailable || (disposed ? "collector disposed" : "body unavailable"));
      } finally {
        clearTimeout(timer); cancellations.delete(cancelRead); active--;
      }
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      for (const cancel of cancellations) cancel();
      for (const [name, handler] of events) cdp.off(name, handler);
      seen.clear();
    },
  };
}

// Raw pageerror text and the structured DTO share the actual CDP exceptionId. Playwright's Error loses the
// identity and payload of a thrown plain object; its separate event count audits that CDP missed nothing.
async function startClientExceptionProof(page: Page, cdp: any, errors: string[]) {
  const state: any = { version: 1, complete: true, overflow: false, playwright_page_error_count: 0, events: [] };
  const pending = new Set<Promise<void>>(), cancellations = new Set<() => void>();
  const audit: string[] = [];
  let enabled = false, finalizing = false, disposed = false, count = 0;
  const bounded = async (work: Promise<any>) => {
    let cancel: () => void = () => {};
    const cancellation = new Promise((_, reject) => { cancel = () => reject(new Error("exception evidence canceled or timed out")); });
    const timer = setTimeout(cancel, 2000);
    cancellations.add(cancel);
    try { return await Promise.race([work, cancellation]); }
    finally { clearTimeout(timer); cancellations.delete(cancel); }
  };
  const onPageError = (e: Error) => {
    state.playwright_page_error_count++;
    if (audit.length < 4) audit.push(`pageerror: ${String(e.stack || e).slice(0, 1500)}`);
    else { state.overflow = true; state.complete = false; }
  };
  // A console error of a plugin carries its stack (a rejected query shows its full series labels and several frames).
  // The proof is incomplete only when a message does not fit this bound, not at the first 1500 characters: a page that
  // throws no exception must not lose its proof to a long but fully retained console message.
  const CONSOLE_TEXT_LIMIT = 8192;
  const onConsole = (message: any) => {
    if (message.type() !== "error") return;
    const text = message.text();
    if (text.length > CONSOLE_TEXT_LIMIT || errors.length >= 20) state.complete = false;
    if (errors.length < 20) errors.push(text.slice(0, CONSOLE_TEXT_LIMIT));
  };
  const onException = (event: any) => {
    const detail = event.exceptionDetails || {}, object = detail.exception || {};
    const text = `pageerror: ${String(object.description || object.value || detail.text || "unknown exception")}`;
    const index = errors.length;
    if (index < 20) errors.push(text.slice(0, 1500));
    else state.complete = false;
    count++;
    if (count > 4) { state.overflow = true; state.complete = false; return; }
    const frames = detail.stackTrace?.callFrames || [];
    const item: any = {
      event_id: `runtime:${detail.exceptionId}`, exception_id: detail.exceptionId, raw_error_index: index,
      kind: "uncaught", class_name: object.className || object.type || "unknown", own_property_names: [],
      stack: frames.slice(0, 8).map((frame: any) => ({
        url: String(frame.url || "").split("?")[0].slice(0, 2048),
        line: frame.lineNumber + 1, column: frame.columnNumber + 1,
      })),
    };
    state.events.push(item);
    if (text.length > 1500 || frames.length > 8 || !Number.isInteger(detail.exceptionId) || finalizing) state.complete = false;
    // Only inspect a small plain object with the observed DTO keys. No getters, functions, configuration or
    // headers are evaluated or collected; unknown exceptions remain raw errors and cannot match a contract.
    const preview = object.preview;
    if (object.className !== "Object" || !object.objectId || !preview || preview.overflow !== false || preview.properties?.length !== 3 ||
        JSON.stringify((preview.properties || []).map((p: any) => p.name).sort()) !== '["message","refId","status"]' || finalizing) return;
    const work = (async () => {
      try {
        const properties = await bounded(cdp.send("Runtime.getProperties", { objectId: object.objectId, ownProperties: true }));
        const own = properties.result || [];
        if (own.length !== 3 || own.some((p: any) => !p.value || p.get || p.set)) { state.complete = false; return; }
        item.own_property_names = own.map((p: any) => p.name).sort();
        if (JSON.stringify(item.own_property_names) !== '["message","refId","status"]') return;
        const values = Object.fromEntries(own.map((p: any) => [p.name, p.value.value]));
        if (typeof values.refId !== "string" || Buffer.byteLength(values.refId) > 512 ||
            typeof values.message !== "string" || Buffer.byteLength(values.message) > 4096 || !Number.isInteger(values.status)) {
          state.complete = false; return;
        }
        Object.assign(item, {kind: "uncaught-object", ref_id: values.refId, status: values.status, message: values.message});
      } catch { state.complete = false; }
    })();
    pending.add(work);
    void work.finally(() => pending.delete(work));
  };
  page.on("pageerror", onPageError);
  page.on("console", onConsole);
  cdp.on("Runtime.exceptionThrown", onException);
  try { await bounded(cdp.send("Runtime.enable")); enabled = true; }
  catch { state.complete = false; }
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    if (!finalizing) state.complete = false;
    page.off("pageerror", onPageError);
    page.off("console", onConsole);
    cdp.off("Runtime.exceptionThrown", onException);
    for (const cancel of cancellations) cancel();
  };
  return {
    dispose,
    async finish() {
      finalizing = true;
      try { await bounded(Promise.all([...pending])); }
      catch { state.complete = false; }
      finally {
        if (!enabled || count !== state.playwright_page_error_count) {
          state.complete = false;
          for (const text of audit) if (errors.length < 20) errors.push(text);
        }
        dispose();
      }
      return state;
    },
  };
}

// Collect during the existing capture, not another load. Heap maxima are
// sampled; ThreadTime is the target main thread's CPU counter, not host CPU.
async function startRuntimeProof(page: Page, began = Date.now(), errors: string[] = []) {
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Performance.enable");
  const bodyFallback = startBodyFallback(cdp);
  try {
    // Chrome keeps a response only up to maxResourceBufferSize and then answers getResponseBody "Request content was
  // evicted from inspector cache": with 32 MiB here, Logs Drilldown's trace_id and span_id breakdowns (over 40 MiB)
  // were unreadable, and their pages could not prove their answers complete. The buffer is sized for the largest
  // answer of the plan; what the capture keeps of a body is still bounded (maxBody, and a digest above it).
  await cdp.send("Network.enable", {maxTotalBufferSize: 768 * 1024 * 1024, maxResourceBufferSize: 256 * 1024 * 1024});
  } catch (error) {
    bodyFallback.dispose();
    await cdp.detach().catch(() => {});
    throw error;
  }
  const exceptions = await startClientExceptionProof(page, cdp, errors);
  const requests = new Map<string, { api: boolean; received: number }>();
  let apiRequests = 0, apiBytes = 0, pageBytes = 0, heap = 0, samples = 0;
  let abortedAPIBytes = 0, abortedAPIRequests = 0;
  let firstData: number | undefined;
  let stoppedAt = Infinity;
  let before: Record<string, number> = {}, after: Record<string, number> = {};
  cdp.on("Network.requestWillBeSent", ({ requestId, request, redirectResponse }) => {
    if (stoppedAt !== Infinity) return;
    const previous = requests.get(requestId);
    if (previous && redirectResponse) {
      const transferred = Math.max(previous.received, redirectResponse.encodedDataLength || 0);
      pageBytes += transferred;
      if (previous.api) apiBytes += transferred;
    }
    const api = BACKEND.test(request.url);
    requests.set(requestId, { api, received: 0 });
    if (api) apiRequests++;
  });
  cdp.on("Network.dataReceived", ({ requestId, encodedDataLength }) => {
    if (stoppedAt !== Infinity) return;
    const request = requests.get(requestId);
    if (request) request.received += encodedDataLength;
  });
  cdp.on("Network.loadingFinished", ({ requestId, encodedDataLength }) => {
    if (stoppedAt !== Infinity) return;
    pageBytes += encodedDataLength;
    if (requests.get(requestId)?.api) apiBytes += encodedDataLength;
    requests.delete(requestId);
  });
  cdp.on("Network.loadingFailed", ({ requestId }) => {
    if (stoppedAt !== Infinity) return;
    const request = requests.get(requestId);
    pageBytes += request?.received || 0;
    if (request?.api) {
      apiBytes += request.received;
      abortedAPIBytes += request.received;
      abortedAPIRequests++;
    }
    requests.delete(requestId);
  });
  const sample = async () => {
    if (page.isClosed()) return;
    const metrics = await cdp.send("Performance.getMetrics");
    after = Object.fromEntries(metrics.metrics.map(({ name, value }) => [name, value]));
    if (Number.isFinite(after.JSHeapUsedSize)) heap = Math.max(heap, after.JSHeapUsedSize);
    samples++;
  };
  await sample();
  before = after;
  let sampling: Promise<void> | undefined;
  let sampleError: string | undefined;
  const timer = setInterval(() => {
    if (!sampling) sampling = sample().catch((e) => { sampleError = String(e); }).finally(() => { sampling = undefined; });
  }, 250);
  let disposed = false;
  const dispose = async () => {
    if (disposed) return;
    disposed = true;
    clearInterval(timer);
    exceptions.dispose();
    bodyFallback.dispose();
    page.off("close", onClose);
    cdp.removeAllListeners();
    await cdp.detach().catch(() => {});
  };
  const onClose = () => { void dispose(); };
  page.once("close", onClose);
  return {
    dispose,
    readBody: bodyFallback.read,
    async finishClientExceptions() {
      try { return await exceptions.finish(); }
      finally { await dispose(); }
    },
    recordData(records: any[], completedAt = Date.now()) {
      if (completedAt > stoppedAt) return;
      const present = records.some((r) => {
        if (r.status !== 200 || !r.response) return false;
        const data = r.response.data ?? r.response;
        if (r.response.status === "error") return false;
        if (Array.isArray(data) && data.length) return true;
        if (["entries", "streams"].some(key => Number.isFinite(data[key]) && data[key] > 0)) return true;
        if (["result", "fields", "detectedLabels", "values", "series", "volumes"]
          .some(key => Array.isArray(data[key]) && data[key].length)) return true;
        return Object.values(r.response.results || {}).some((v: any) =>
          !v.error && v.frames?.some((f: any) => f.data?.values?.some((x: any) => Array.isArray(x) && x.length)));
      });
      if (present) firstData = Math.min(firstData ?? Infinity, completedAt - began);
    },
    async finish(boundaryMs = Date.now()) {
      stoppedAt = boundaryMs;
      clearInterval(timer);
      const boundary = sample().catch((e) => { sampleError = String(e); });
      await Promise.all([sampling, boundary]);
      const delta = (key: string) => Number.isFinite(before[key]) && Number.isFinite(after[key]) && after[key] >= before[key]
        ? after[key] - before[key] : undefined;
      const unfinished = [...requests.values()];
      const unfinishedAPIBytes = unfinished.filter(r => r.api).reduce((n, r) => n + r.received, 0);
      return {
        browser_thread_seconds: delta("ThreadTime"), browser_task_seconds: delta("TaskDuration"),
        js_heap_max_sampled_bytes: heap, metric_samples: samples, api_requests: apiRequests,
        api_encoded_bytes: apiBytes + unfinishedAPIBytes,
        page_encoded_bytes: pageBytes + unfinished.reduce((n, r) => n + r.received, 0),
        api_aborted_encoded_bytes: abortedAPIBytes, api_aborted_requests: abortedAPIRequests,
        api_unfinished_encoded_bytes: unfinishedAPIBytes,
        get first_api_data_ms() { return firstData; },
        ...(sampleError ? { sampling_error: sampleError } : {}),
      };
    },
  };
}

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
    const first = spec.pages.find((x: PageSpec) => x.kind === "drilldown" && !x.path && !x.levels);
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

// Waits for every read still running; a read that outlasts the limit is left out of the page's record, not waited on forever.
async function drain(reading: Set<Promise<void>>, limitMs = 30_000) {
  const t0 = Date.now();
  while (reading.size && Date.now() - t0 < limitMs) {
    await Promise.race([Promise.allSettled([...reading]), new Promise((res) => setTimeout(res, limitMs - (Date.now() - t0)))]);
  }
}

const cancelled = (rq: Request) => /ERR_ABORTED/.test(rq.failure()?.errorText || "");
const abortedRecord = (rq: Request) => ({ url: rq.url().replace(/^https?:\/\/[^/]+/, ""), method: rq.method(),
  request: rq.postDataJSON?.() ?? null, status: 0, response: null, aborted: rq.failure()?.errorText });

// A backend answer as the capture records it. The `response` event fires when the headers arrive; the body is
// read once the request has finished. A request the page cancelled (Chrome: net::ERR_ABORTED; Logs Drilldown does this
// to the volume panel's detected_level query on some level-pill pages) has no answer to record, but it is not
// nothing either: it is recorded as `aborted`, whether the cancel came before or after the headers, so that
// whether it left a record does not depend on which of the two happened first. compare.py reads it as "cancelled
// by the page".
async function recordAnswer(rs: Response, over: (rq: Request) => Promise<void>, readBody?: ReturnType<typeof startBodyFallback>["read"]) {
  const rq = rs.request();
  const url = rq.url().replace(/^https?:\/\/[^/]+/, "");
  await over(rq);
  if (cancelled(rq)) return abortedRecord(rq);
  let body: any = null;
  let unavailable = "";
  let source: string | undefined;
  let digest: BodyDigest | undefined;
  let bytes: Buffer | undefined;
  try { bytes = await rs.body(); } catch (error) {
    unavailable = String((error as Error)?.message ?? error).split("\n")[0].slice(0, 160) || "body unavailable";
    if (cancelled(rq)) return abortedRecord(rq);
    if (readBody && /evicted from inspector cache|No (?:resource|data) (?:found )?with given identifier/.test(unavailable)) {
      const recovered = await readBody({method: rq.method(), url: rq.url(), postData: rq.postData(), status: rs.status()});
      if (recovered.body) { bytes = recovered.body; source = "cdp-fallback"; }
      else {
        unavailable = `CDP fallback unavailable: ${recovered.error || "body missing"}; ${unavailable}`.slice(0, 160);
        digest = recovered.digest;
      }
    }
  }
  if (cancelled(rq)) return abortedRecord(rq);
  if (bytes) {
    const raw = bytes.toString("utf8");
    try { body = JSON.parse(raw); } catch { body = raw; }
  }
  const length = parseInt(rs.headers()["content-length"] || "", 10);
  return { url, method: rq.method(),
    request: rq.postDataJSON?.() ?? null, status: rs.status(), response: body,
    ...(source ? {body_source: source} : {}),
    ...(body === null ? { body_unavailable: unavailable || "empty body", ...(length ? { content_length: length } : {}),
                          ...(digest ? { body_digest: digest } : {}) } : {}) };
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
        let runtimeProof: Awaited<ReturnType<typeof startRuntimeProof>> | undefined;
        try {
        const page = await ctx.newPage();
        const pending = { n: 0, last: Date.now(), seen: 0 };
        const records: any[] = [];
        const errors: string[] = [];
        if (p.kind === "drilldown") await queriesAfterPanelModule(page);
        page.on("request", (rq) => {
          if (BACKEND.test(rq.url())) { pending.n++; pending.seen = (pending.seen || 0) + 1; pending.last = Date.now(); }
        });
        page.on("requestfailed", (rq) => {
          if (BACKEND.test(rq.url())) { pending.n--; pending.last = Date.now(); }
        });
        page.on("requestfinished", (rq) => {
          if (BACKEND.test(rq.url())) { pending.n--; pending.last = Date.now(); }
        });
        // Every answer is read by a handler of its own; the page is only recorded once all of them are done.
        // Playwright's response.finished() does not resolve for a request cancelled after its headers arrived, so the end
        // of every request (finished or failed) is taken from the page's own events.
        const ends = new Map<Request, () => void>();
        const ended = new Map<Request, Promise<void>>();
        const over = (rq: Request) => ended.get(rq) ?? Promise.resolve();
        page.on("request", (rq) => { if (BACKEND.test(rq.url())) ended.set(rq, new Promise<void>((res) => ends.set(rq, res))); });
        const completion = new WeakMap<Request, number>();
        const end = (rq: Request) => {
          completion.set(rq, Date.now());
          ends.get(rq)?.();
        };
        page.on("requestfinished", end);
        page.on("requestfailed", end);
        const reading = new Set<Promise<void>>();
        const track = (work: Promise<void>) => { const job = work.finally(() => reading.delete(job)); reading.add(job); };
        page.on("response", (rs) => {
          if (BACKEND.test(rs.request().url())) track(recordAnswer(rs, over, runtimeProof?.readBody).then(record => {
            records.push(record);
            runtimeProof?.recordData([record], completion.get(rs.request()));
          }));
        });
        // A request cancelled before its headers arrived never gets a `response` event.
        page.on("requestfailed", (rq) => {
          if (BACKEND.test(rq.url())) track(rq.response().then((rs) => { if (!rs && cancelled(rq)) records.push(abortedRecord(rq)); }));
        });
        const t0 = Date.now();
        runtimeProof = await startRuntimeProof(page, t0, errors);
        await page.goto(url(p, ds.uid, r), { waitUntil: "commit" });
        if (p.kind === "label-browser") {
          await page.getByRole("button", { name: /Label browser/i }).first().click({ timeout: 30_000 }).catch(() => {});
          await page.waitForTimeout(1500);
          await page.getByText("service_name", { exact: true }).first().click({ timeout: 10_000 }).catch(() => {});
        }
        if (p.kind === "log-context") {
          await settle(page, pending);
          await openLogContext(page);
        }
        const ok = await settle(page, pending);
        const settleMs = Date.now() - t0;
        const lastCompletionMs = pending.last - t0;
        const runtime = await runtimeProof.finish(t0 + settleMs);
        // settle() counts requests, not the reads of their bodies: wait for every read before the page is recorded.
        await drain(reading);
        const unavailable = records.some((x) => x.status === 500 && /plugin\.(unavailable|connectionUnavailable)/.test(JSON.stringify(x.response)));
        // A 200 whose body could not be read is not a successful data capture.
        // Retry that and any page with requests still in flight; another 200
        // resource response must not hide an incomplete panel query.
        const incomplete = !ok || records.some((x) => x.status === 200 && x.response === null && x.url.startsWith("/api/ds/query"));
        // The app plugin crashed or its bundle did not load ("Plugin failed to load", see queriesAfterPanelModule):
        // load it again in a fresh browser context; the details and console errors of the last attempt are recorded.
        const pluginLoad = (await uiState(page)).banners.some((b) => /^Plugin (failed|unavailable|not found)/.test(b));
        if ((unavailable || incomplete || pluginLoad) && attempt < 2) { await ctx.close(); await new Promise((res) => setTimeout(res, 5000)); continue; }
        const dir = path.join(OUT, "shots", p.id, r);
        const ddir = path.join(OUT, "data", p.id, r);
        fs.mkdirSync(dir, { recursive: true });
        fs.mkdirSync(ddir, { recursive: true });
        await page.screenshot({ path: path.join(dir, `${ds.id}.png`) });
        const ui = await uiState(page);
        const client_exceptions = await runtimeProof.finishClientExceptions();
        fs.writeFileSync(path.join(ddir, `${ds.id}.json`), JSON.stringify({ settled: ok, settle_ms: settleMs,
          last_api_completion_ms: lastCompletionMs, attempts: attempt + 1, records, ui, errors, runtime, client_exceptions }));
        await ctx.close();
        break;
        } finally {
          await runtimeProof?.dispose();
          await ctx.close();
        }
        }
      }
    });
  }
}
