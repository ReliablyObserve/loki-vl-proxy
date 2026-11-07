package proxy

import (
	"context"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The slow-query log: a client request at or over -slow-query-threshold, and one VictoriaLogs call at or over
// -slow-upstream-threshold (timed to the end of its body), are counted (loki_vl_proxy_slow_requests_total) and
// logged at WARN whatever the request-log sampling, with their queries and, for a request, its trace: every
// VictoriaLogs call it sent (the LogsQL, its window, status, bytes and time), every proxy-side operation and
// route decision, and every rewrite of its LogQL, each with its offset from the request's start.

// DefaultSlowQueryThreshold and DefaultSlowUpstreamThreshold are -slow-query-threshold and
// -slow-upstream-threshold's defaults, set from measured timings (2026-11-07): a client request is slow at 3 s,
// the 99th percentile of the daily A/B benchmark's cold first loads (3.1 s over 1,334 shape×range rows; 95th 1.0 s)
// and about the slowest 4% of the compose stack's client requests; a VictoriaLogs call is slow at 2 s, rarer than
// 1 in 50 calls on the stack: the 98th percentile of 2,511 calls over 24h of heavy Drilldown use timed to their
// headers (1.96 s), above the 99.9th of 11,458 calls timed to the end of their bodies (99.99% under 2.5 s).
const (
	DefaultSlowQueryThreshold    = 3 * time.Second
	DefaultSlowUpstreamThreshold = 2 * time.Second
)

const (
	// slowTraceMaxSteps bounds a request's trace; further steps are counted, not kept.
	slowTraceMaxSteps = 64
	// slowQueryMaxBytes caps a query's text in a slow line (a split's LogsQL runs to several KB).
	slowQueryMaxBytes = 8 << 10
	// slowLineQueryBudget caps all the query text of one slow_request line (log pipelines cut long lines; Loki's
	// max_line_size defaults to 256 KB): past it, a query is its hash.
	slowLineQueryBudget = 32 << 10
)

// slowTraceStep is one step of a request's trace.
type slowTraceStep struct {
	kind     string // upstream, operation, rewrite
	at       time.Duration
	duration time.Duration
	route    string // upstream path; operation name
	outcome  string // operation outcome; upstream body outcome
	query    string // upstream LogsQL; the rewritten LogQL
	from     string // rewrite: the LogQL before
	start    string // upstream: start, end and step as sent
	end      string
	step     string
	status   int
	bytes    int64
}

// slowTraceOn reports whether the proxy keeps request traces (either slow threshold is set).
func (p *Proxy) slowTraceOn() bool {
	return p != nil && (p.slowQueryThreshold > 0 || p.slowUpstreamThreshold > 0)
}

// traceStep adds a step to the request's trace, when the request keeps one and its log line is not written yet.
// It reports false for work that outlives its request (a background build), whose own slow lines say so.
func traceStep(ctx context.Context, step slowTraceStep) bool {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.traceOn || rt.traceClosed {
		return false
	}
	step.at = time.Since(rt.start) - step.duration
	if step.at < 0 {
		step.at = 0
	}
	if len(rt.trace) >= slowTraceMaxSteps {
		rt.traceDropped++
		return true
	}
	rt.trace = append(rt.trace, step)
	return true
}

// closeTrace ends the request's trace and returns its steps.
func closeTrace(ctx context.Context) ([]slowTraceStep, int) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return nil, 0
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.traceClosed = true
	return rt.trace, rt.traceDropped
}

// traceRewrite records a handler rewriting the request's LogQL.
func traceRewrite(ctx context.Context, from, to string) {
	if from != to {
		traceStep(ctx, slowTraceStep{kind: "rewrite", from: from, query: to})
	}
}

// slowQueryText is a query as a slow line carries it: its text, capped, or its hash with
// -slow-query-log-raw-queries=false.
func (p *Proxy) slowQueryText(q string) string {
	if !p.slowQueryLogRawQueries {
		return redactQuery(q, false)
	}
	if len(q) > slowQueryMaxBytes {
		return q[:slowQueryMaxBytes] + "...(" + strconv.Itoa(len(q)) + " bytes)"
	}
	return q
}

// slowUpstreamWindow is the start, end and step a VictoriaLogs call was sent with.
func slowUpstreamWindow(start, end, step string) string {
	var parts []string
	for _, kv := range [][2]string{{"start", start}, {"end", end}, {"step", step}} {
		if kv[1] != "" {
			parts = append(parts, kv[0]+"="+kv[1])
		}
	}
	return strings.Join(parts, " ")
}

// observeUpstreamDone records a VictoriaLogs call once its body is read (or the call failed): a step of the
// request's trace and, at or over -slow-upstream-threshold, a slow line and count.
func (p *Proxy) observeUpstreamDone(ctx context.Context, path string, params url.Values, status int, outcome string, bytes int64, total time.Duration) {
	if !p.slowTraceOn() || path == "/select/logsql/tail" { // a tail stream lasts as long as its session
		return
	}
	query := params.Get("query")
	inRequest := traceStep(ctx, slowTraceStep{kind: "upstream", route: path, query: query, start: params.Get("start"),
		end: params.Get("end"), step: params.Get("step"), status: status, outcome: outcome, bytes: bytes, duration: total})
	if p.slowUpstreamThreshold <= 0 || total < p.slowUpstreamThreshold {
		return
	}
	route := normalizeObservedRoute(path)
	p.metrics.RecordSlowRequest("vl", route)
	meta := requestRouteMetaFromContext(ctx)
	attrs := []any{
		"http.route", route,
		"loki.api.system", "vl",
		"proxy.direction", "upstream",
		"http.response.status_code", status,
		"event.duration", total.Nanoseconds(),
		"slow.threshold_ms", p.slowUpstreamThreshold.Milliseconds(),
		"loki.tenant.id", getOrgID(ctx),
		"vl.query", p.slowQueryText(query),
		"vl.window", slowUpstreamWindow(params.Get("start"), params.Get("end"), params.Get("step")),
		"vl.body_bytes", bytes,
		"vl.body_outcome", outcome,
	}
	if meta.route != "" {
		attrs = append(attrs, "http.parent_route", meta.route)
	}
	if !inRequest {
		attrs = append(attrs, "proxy.background", true)
	}
	p.log.Log(ctx, slog.LevelWarn, "slow_upstream_request", attrs...)
}

// slowRequestExempt reports a route whose requests last as long as a client keeps them open (live tail).
func slowRequestExempt(endpoint, route string) bool {
	return strings.Contains(endpoint, "tail") || strings.HasSuffix(route, "/tail")
}

// logSlowRequest writes the slow line of a client request at or over -slow-query-threshold, with its trace.
func (p *Proxy) logSlowRequest(ctx context.Context, endpoint, route, query string, status int, elapsed time.Duration, telemetry requestTelemetrySnapshot) {
	steps, dropped := closeTrace(ctx)
	if p.slowQueryThreshold <= 0 || elapsed < p.slowQueryThreshold || slowRequestExempt(endpoint, route) {
		return
	}
	p.metrics.RecordSlowRequest("loki", route)
	// Steps are added as they finish; the line lists them as they started. A query is written once (a split's
	// checks repeat its base query): a step repeating one names the step that holds it; past the line's budget a
	// query is its hash.
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].at < steps[j].at })
	budget := slowLineQueryBudget - len(query)
	seen := map[string]int{}
	text := func(step int, q string) any {
		if j, ok := seen[q]; ok {
			return map[string]int{"same_as_step": j}
		}
		seen[q] = step
		t := p.slowQueryText(q)
		if budget -= len(t); budget < 0 {
			return redactQuery(q, false)
		}
		return t
	}
	trace := make([]map[string]any, 0, len(steps))
	for i, s := range steps {
		entry := map[string]any{"kind": s.kind, "at_ms": s.at.Milliseconds()}
		switch s.kind {
		case "upstream":
			entry["route"] = s.route
			entry["query"] = text(i, s.query)
			entry["window"] = slowUpstreamWindow(s.start, s.end, s.step)
			entry["status"] = s.status
			entry["bytes"] = s.bytes
			entry["outcome"] = s.outcome
			entry["duration_ms"] = s.duration.Milliseconds()
		case "operation":
			entry["operation"] = s.route
			entry["outcome"] = s.outcome
			entry["duration_ms"] = s.duration.Milliseconds()
		case "rewrite":
			entry["from"] = text(i, s.from)
			entry["to"] = text(i, s.query)
		}
		trace = append(trace, entry)
	}
	attrs := []any{
		"http.route", route,
		"loki.request.type", endpoint,
		"loki.api.system", "loki",
		"proxy.direction", "downstream",
		"http.response.status_code", status,
		"event.duration", elapsed.Nanoseconds(),
		"slow.threshold_ms", p.slowQueryThreshold.Milliseconds(),
		"loki.tenant.id", getOrgID(ctx),
		"loki.query", p.slowQueryText(query),
		"cache.result", telemetry.cacheResult,
		"upstream.calls", telemetry.upstreamCalls,
		"upstream.duration_ms", telemetry.upstreamDuration.Milliseconds(),
		"proxy.steps", trace,
	}
	if dropped > 0 {
		attrs = append(attrs, "proxy.steps_dropped", dropped)
	}
	p.log.Log(ctx, slog.LevelWarn, "slow_request", attrs...)
}
