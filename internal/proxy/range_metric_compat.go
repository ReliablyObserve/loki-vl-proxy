package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	fj "github.com/valyala/fastjson"
)

type statsCompatSpec struct {
	BaseQuery   string
	GroupBy     []string
	OrigGroupBy []string // original Loki label names before VL translation (e.g. detected_level → level)
	ByExplicit  bool     // true when "by ()" was present — aggregate all into one series
	Func        string
	Field       string
}

type originalRangeMetricSpec struct {
	Func        string
	Window      time.Duration
	UnwrapField string
	UnwrapConv  string
	HasUnwrap   bool
	BaseQuery   string // inner stream selector + pipeline, without range window [T]
}

type rangeMetricSample struct {
	ts    int64
	value float64
}

var (
	rangeMetricUnwrapRE = regexp.MustCompile(`(?s)\|\s*unwrap\s+([^|\[]+)`)
	outerAggregationRE  = regexp.MustCompile(`^(?:sum|avg|max|min|count(?:_values)?|stddev|stdvar|sort(?:_desc)?|topk|bottomk)\s*(?:(?:by|without)\s*\([^)]*\)\s*)?`)
	outerByAfterRE      = regexp.MustCompile(`\)\s+by\s*\(([^)]+)\)\s*$`)
	outerByBeforeRE     = regexp.MustCompile(`^(?:sum|avg|min|max|count[^(]*|stddev|stdvar)\s+by\s*\(([^)]+)\)\s*\(`)
)

func parseStatsCompatSpec(logsqlQuery string) (statsCompatSpec, bool) {
	idx := strings.Index(logsqlQuery, "| stats ")
	if idx < 0 {
		return statsCompatSpec{}, false
	}

	spec := statsCompatSpec{
		BaseQuery: strings.TrimSpace(logsqlQuery[:idx]),
	}
	rest := strings.TrimSpace(logsqlQuery[idx+len("| stats "):])
	if rest == "" {
		return statsCompatSpec{}, false
	}

	if strings.HasPrefix(rest, "by (") {
		closeIdx := strings.Index(rest, ")")
		if closeIdx > len("by (") {
			labels := strings.TrimSpace(rest[len("by ("):closeIdx])
			if labels != "" {
				for _, label := range strings.Split(labels, ",") {
					label = strings.TrimSpace(label)
					if label != "" {
						spec.GroupBy = append(spec.GroupBy, label)
					}
				}
			}
			rest = strings.TrimSpace(rest[closeIdx+1:])
		} else if closeIdx == len("by (") {
			// "by ()" — explicit empty grouping: one series, no label dimensions.
			spec.ByExplicit = true
			rest = strings.TrimSpace(rest[closeIdx+1:])
		}
	}

	openIdx := strings.Index(rest, "(")
	switch {
	case openIdx < 0:
		spec.Func = strings.TrimSpace(rest)
	case strings.HasSuffix(rest, ")"):
		spec.Func = strings.TrimSpace(rest[:openIdx])
		spec.Field = strings.TrimSpace(rest[openIdx+1 : len(rest)-1])
	default:
		return statsCompatSpec{}, false
	}

	if spec.BaseQuery == "" || spec.Func == "" {
		return statsCompatSpec{}, false
	}

	return spec, true
}

func parseOriginalRangeMetricSpec(logql string) (originalRangeMetricSpec, bool) {
	logql = strings.TrimSpace(logql)
	// Strip outer aggregation like "sum by (method) (rate(...))" → "rate(...)"
	// so we parse the inner range function, not the aggregation operator.
	if loc := outerAggregationRE.FindStringIndex(logql); loc != nil && loc[0] == 0 && loc[1] < len(logql) {
		inner := strings.TrimSpace(logql[loc[1]:])
		if strings.HasPrefix(inner, "(") && strings.HasSuffix(inner, ")") {
			inner = strings.TrimSpace(inner[1 : len(inner)-1])
		}
		logql = inner
	}
	openIdx := strings.Index(logql, "(")
	closeIdx := strings.LastIndex(logql, ")")
	if openIdx <= 0 || closeIdx <= openIdx {
		return originalRangeMetricSpec{}, false
	}

	spec := originalRangeMetricSpec{
		Func: strings.TrimSpace(logql[:openIdx]),
	}
	body := strings.TrimSpace(logql[openIdx+1 : closeIdx])
	bracketOpen := strings.LastIndex(body, "[")
	bracketClose := strings.LastIndex(body, "]")
	if bracketOpen < 0 || bracketClose <= bracketOpen {
		return originalRangeMetricSpec{}, false
	}
	spec.Window = parseLokiDuration(strings.TrimSpace(body[bracketOpen+1 : bracketClose]))
	if spec.Window < 0 {
		return originalRangeMetricSpec{}, false
	}
	spec.BaseQuery = strings.TrimSpace(body[:bracketOpen])

	unwrap := rangeMetricUnwrapRE.FindStringSubmatch(body)
	if len(unwrap) == 2 {
		spec.HasUnwrap = true
		spec.UnwrapField, spec.UnwrapConv = parseUnwrapExpression(unwrap[1])
	}

	return spec, true
}

func isManualRangeStatsFunc(funcName string) bool {
	switch strings.TrimSpace(funcName) {
	case "rate", "count", "sum_len", "sum", "avg", "max", "min", "stddev", "stdvar", "quantile", "first", "last", "__rate_counter__":
		return true
	default:
		return false
	}
}

func normalizeManualMetricFunction(spec statsCompatSpec, origSpec originalRangeMetricSpec) string {
	switch strings.TrimSpace(origSpec.Func) {
	case "rate":
		return "rate"
	case "count_over_time":
		return "count_over_time"
	case "bytes_over_time":
		return "bytes_over_time"
	case "bytes_rate":
		return "bytes_rate"
	case "sum_over_time":
		return "sum"
	case "avg_over_time":
		return "avg"
	case "max_over_time":
		return "max"
	case "min_over_time":
		return "min"
	case "stddev_over_time":
		return "stddev"
	case "stdvar_over_time":
		return "stdvar"
	case "first_over_time":
		return "first"
	case "last_over_time":
		return "last"
	case "rate_counter":
		return "rate_counter"
	case "quantile_over_time":
		return "quantile"
	}

	switch strings.TrimSpace(spec.Func) {
	case "rate":
		return "rate"
	case "count":
		return "count_over_time"
	case "sum_len":
		if strings.TrimSpace(origSpec.Func) == "bytes_rate" {
			return "bytes_rate"
		}
		return "bytes_over_time"
	case "sum":
		return "sum"
	case "avg":
		return "avg"
	case "max":
		return "max"
	case "min":
		return "min"
	case "stddev":
		return "stddev"
	case "stdvar":
		return "stdvar"
	case "quantile":
		return "quantile"
	case "first":
		return "first"
	case "last":
		return "last"
	case "__rate_counter__":
		return "rate_counter"
	default:
		return ""
	}
}

func metricFuncRequiresUnwrap(funcName string) bool {
	switch strings.TrimSpace(funcName) {
	case "sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "stddev_over_time", "stdvar_over_time", "first_over_time", "last_over_time", "quantile_over_time", "rate_counter":
		return true
	default:
		return false
	}
}

func unwrapErrorFuncName(funcName string) string {
	funcName = strings.TrimSpace(funcName)
	if funcName != "" {
		return funcName
	}
	return "range_aggregation"
}

func parseStatsQuantileSpec(field string) (float64, string, bool) {
	parts := strings.SplitN(field, ",", 2)
	if len(parts) != 2 {
		return 0, "", false
	}
	phi, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, "", false
	}
	return phi, strings.TrimSpace(parts[1]), true
}

func parseUnwrapExpression(expr string) (field, conv string) {
	expr = strings.TrimSpace(expr)
	expr = strings.Trim(expr, "`\"")

	switch {
	case strings.HasPrefix(expr, "duration(") && strings.HasSuffix(expr, ")"):
		field = strings.TrimSpace(expr[len("duration(") : len(expr)-1])
		conv = "duration"
	case strings.HasPrefix(expr, "bytes(") && strings.HasSuffix(expr, ")"):
		field = strings.TrimSpace(expr[len("bytes(") : len(expr)-1])
		conv = "bytes"
	default:
		field = strings.TrimSpace(expr)
	}

	field = strings.Trim(field, "`\"")
	return field, conv
}

// parseOriginalByLabels extracts the outer by(...) label names from a LogQL
// metric query. Handles both "sum(...) by (labels)" and "sum by (labels) (...)"
// forms. Returns nil when no outer by-clause is present.
func parseOriginalByLabels(logql string) []string {
	var raw string
	if m := outerByAfterRE.FindStringSubmatch(logql); m != nil {
		raw = m[1]
	} else if m := outerByBeforeRE.FindStringSubmatch(logql); m != nil {
		raw = m[1]
	}
	if raw == "" {
		return nil
	}
	var out []string
	for _, l := range strings.Split(raw, ",") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

func (p *Proxy) handleStatsCompatRange(w http.ResponseWriter, r *http.Request, originalLogql, logsqlQuery string) bool {
	// Queries containing | math are multi-stage VL rate pipelines built by the translator
	// (e.g. sum(rate({...} | json [w]))). For non-sliding windows (range == step), VL can
	// execute them natively — no manual decomposition needed. For sliding windows (range >
	// step), fall through to the manual path which implements correct per-step accumulation.
	//
	// Exception: queries with parser stages (| json, | logfmt, etc.) without an explicit
	// "| drop __error__" opt-in must NOT use VL native stats for tumbling windows. Loki
	// excludes parse-failed lines from metric aggregation; VL counts all lines. The manual
	// path (collectRangeMetricSamples) preserves Loki's error-exclusion semantics.
	if strings.Contains(logsqlQuery, "| math ") {
		step, stepOk := parsePositiveStepDuration(r.FormValue("step"))
		origSpec, hasOrigSpec := parseOriginalRangeMetricSpec(originalLogql)
		if stepOk && hasOrigSpec && origSpec.Window > 0 && origSpec.Window <= step {
			spec, specOk := parseStatsCompatSpec(logsqlQuery)
			// Parser stages without an explicit drop-error opt-in require the manual path
			// to preserve Loki's error-exclusion semantics. Use origSpec.BaseQuery (the inner
			// LogQL stream selector + pipeline without the range window) for the drop-error
			// check — hasDropErrorOnlyPostParserStage requires the pipeline without outer
			// aggregation or range window brackets.
			if !specOk || !queryUsesParserStages(spec.BaseQuery) || hasDropErrorOnlyPostParserStage(origSpec.BaseQuery) {
				return false
			}
			// Parser stage without drop-error — fall through to the manual path below.
		}
	}
	spec, ok := parseStatsCompatSpec(logsqlQuery)
	if !ok {
		return false
	}
	if !isManualRangeStatsFunc(spec.Func) {
		return false
	}
	// Capture original Loki by-labels so we can translate VL label names back
	// in the metric response (e.g. VL "level" → Loki "detected_level").
	spec.OrigGroupBy = parseOriginalByLabels(originalLogql)
	origSpec, hasOrigSpec := parseOriginalRangeMetricSpec(originalLogql)

	manualFunc := normalizeManualMetricFunction(spec, origSpec)
	if manualFunc == "" {
		return false
	}
	step, _ := parsePositiveStepDuration(r.FormValue("step"))
	// noSlidingOverlap is true when consecutive evaluation windows don't overlap:
	// range == step (tumbling) or range < step (gap between windows). Both are
	// semantically equivalent for VL native stats — only range > step produces
	// overlapping sliding windows where VL tumbling-bucket stats diverges from LogQL.
	noSlidingOverlap := step > 0 && origSpec.Window > 0 && origSpec.Window <= step
	// For tumbling windows, an explicit "| drop __error__" in the original LogQL opts in to
	// VL's count-all semantics (parse failures counted). Use origSpec.BaseQuery — the inner
	// pipeline without outer aggregation or range brackets — so hasDropErrorOnlyPostParserStage
	// can correctly identify the drop-error clause.
	if noSlidingOverlap && queryUsesParserStages(spec.BaseQuery) && hasOrigSpec && hasDropErrorOnlyPostParserStage(origSpec.BaseQuery) {
		return false
	}
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, noSlidingOverlap) {
		return false
	}
	if !hasOrigSpec || origSpec.Window <= 0 {
		p.writeError(w, http.StatusBadRequest, "invalid range metric query")
		return true
	}
	if metricFuncRequiresUnwrap(origSpec.Func) && (!origSpec.HasUnwrap || strings.TrimSpace(origSpec.UnwrapField) == "") {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid aggregation %s without unwrap", unwrapErrorFuncName(origSpec.Func)))
		return true
	}
	return p.proxyManualRangeMetricRange(w, r, spec, origSpec, manualFunc)
}

func (p *Proxy) handleStatsCompatInstant(w http.ResponseWriter, r *http.Request, originalLogql, logsqlQuery string) bool {
	spec, ok := parseStatsCompatSpec(logsqlQuery)
	if !ok {
		return false
	}
	if !isManualRangeStatsFunc(spec.Func) {
		return false
	}
	spec.OrigGroupBy = parseOriginalByLabels(originalLogql)
	origSpec, hasOrigSpec := parseOriginalRangeMetricSpec(originalLogql)

	manualFunc := normalizeManualMetricFunction(spec, origSpec)
	if manualFunc == "" {
		return false
	}
	// Instant queries with parser stages and explicit drop-error: use native VL stats.
	// VL correctly evaluates [time-range, time] for instant queries; the drop-error opt-in
	// means parse-failed lines are intentionally excluded — count-all semantics are acceptable.
	// This matches the same fast-path gate in handleStatsCompatRange for tumbling windows.
	if queryUsesParserStages(spec.BaseQuery) && hasOrigSpec && hasDropErrorOnlyPostParserStage(origSpec.BaseQuery) {
		return false
	}
	// Instant queries have no step: the range window is the entire lookback interval,
	// not a sliding window. VL native stats correctly evaluates [time-range, time].
	// Only rate_counter still requires the manual path (counter-reset semantics).
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, true) {
		return false
	}
	if !hasOrigSpec || origSpec.Window <= 0 {
		p.writeError(w, http.StatusBadRequest, "invalid range metric query")
		return true
	}
	if metricFuncRequiresUnwrap(origSpec.Func) && (!origSpec.HasUnwrap || strings.TrimSpace(origSpec.UnwrapField) == "") {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid aggregation %s without unwrap", unwrapErrorFuncName(origSpec.Func)))
		return true
	}
	return p.proxyManualRangeMetricInstant(w, r, spec, origSpec, manualFunc)
}

// shouldUseManualRangeMetricCompat reports whether the given metric function
// must be aggregated in the proxy (manual path) rather than offloaded to
// VictoriaLogs /select/logsql/stats_query_range.
//
// rangeEqualsStep must be true when the LogQL range window equals the query
// step. When true, VL's native rate() — which buckets by the step interval —
// is semantically identical to LogQL rate()[range]. Pass false to keep the
// sliding-window manual path for cases where range != step.
func shouldUseManualRangeMetricCompat(baseQuery, manualFunc string, rangeEqualsStep bool) bool {
	manualFunc = strings.TrimSpace(manualFunc)
	if manualFunc == "rate_counter" {
		return true
	}

	// For sliding windows (range != step) VL native stats_query_range buckets by the
	// step interval (tumbling windows) while LogQL evaluates each point over [T-range, T].
	// When the data distribution is non-uniform the two diverge. Route to the manual
	// log-fetch path for correct sliding-window semantics.
	// When range == step windows are non-overlapping and native VL stats is equivalent,
	// including for queries that use parser stages (| unpack_json, | unpack_logfmt): VL stats_query_range
	// natively supports inline filter pipelines and parser stages.
	switch manualFunc {
	case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
		// Parser stages require the slow log-fetch path: Loki excludes parse-failed lines
		// from metric aggregation while VL native stats counts all lines. The drop-error
		// opt-in check (hasDropErrorOnlyPostParserStage) only works on LogQL syntax; the
		// baseQuery here is VL-translated syntax (| unpack_json etc.), so we conservatively
		// route all parser-stage queries to the manual path. Bare parser metric queries
		// (proxyBareParserMetricQueryRange) handle the drop-error fast path for the
		// ungrouped case where the original LogQL is available.
		if queryUsesParserStages(baseQuery) {
			return true
		}
		return !rangeEqualsStep
	}

	if !queryUsesParserStages(baseQuery) {
		return false
	}

	// Parser stages present — native VL stats is safe for unwrap-based aggregations
	// (parse failures self-filter via absent fields) and for non-sliding windows.
	switch manualFunc {
	case "avg", "sum", "min", "max", "quantile", "stddev", "stdvar", "first", "last":
		return false
	default:
		return true
	}
}

func (p *Proxy) proxyManualRangeMetricRange(w http.ResponseWriter, r *http.Request, spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc string) bool {
	startTS, err := parseTimestamp(r.FormValue("start"))
	if err != nil {
		p.writeError(w, http.StatusBadRequest, "invalid start timestamp: "+err.Error())
		return true
	}
	endTS, err := parseTimestamp(r.FormValue("end"))
	if err != nil {
		p.writeError(w, http.StatusBadRequest, "invalid end timestamp: "+err.Error())
		return true
	}
	step := parseLokiDuration(formatVLStep(r.FormValue("step")))
	if step <= 0 {
		step = time.Minute
	}
	field, quantile, fieldErr := p.resolveManualMetricField(spec, origSpec, manualFunc)
	if fieldErr != nil {
		p.writeError(w, http.StatusBadRequest, fieldErr.Error())
		return true
	}
	if field == "" {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid aggregation %s without unwrap", unwrapErrorFuncName(origSpec.Func)))
		return true
	}

	// Fast path: count_over_time / rate / bytes_over_time / bytes_rate with
	// explicit groupBy — use VL's stats_query_range endpoint which returns
	// pre-aggregated Prometheus buckets, avoiding reading every raw log entry.
	// Conditions: (1) field is __count__ or __bytes__, (2) groupBy has no _stream
	// sentinel (stats endpoint can group by stream labels; sentinel means caller
	// needs VL to enumerate all distinct streams — skip), (3) labels are explicit
	// (non-empty groupBy or byExplicit aggregate-all).
	var statsAggFunc string
	switch field {
	case "__count__":
		statsAggFunc = "count() as c"
	case "__bytes__":
		statsAggFunc = "sum_len(_msg) as c"
	}
	// Mirror the parser-stage guard from shouldUseManualRangeMetricCompat: if the query
	// uses parser stages, skip the stats_query_range fast path to preserve Loki's
	// parse-error exclusion semantics (Loki excludes parse-failed lines; VL counts all).
	if statsAggFunc != "" && !queryUsesParserStages(spec.BaseQuery) {
		hasStreamSentinel := false
		for _, g := range spec.GroupBy {
			if g == "_stream" {
				hasStreamSentinel = true
				break
			}
		}
		if !hasStreamSentinel && (len(spec.GroupBy) > 0 || spec.ByExplicit) {
			if series, hitsErr := p.collectRangeMetricHits(r.Context(), spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, statsAggFunc, startTS.Add(-origSpec.Window), endTS, step); hitsErr == nil {
				result := buildHitsRangeMetricMatrix(manualFunc, series, startTS, endTS, step, origSpec.Window)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(result) // nosemgrep
				return true
			}
			// Fall through to raw log path on error.
		}
	}

	series, err := p.collectRangeMetricSamples(r.Context(), spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, field, origSpec.UnwrapConv, startTS.Add(-origSpec.Window), endTS)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return true
	}

	result := buildManualRangeMetricMatrix(manualFunc, quantile, series, startTS, endTS, step, origSpec.Window)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
	return true
}

func (p *Proxy) proxyManualRangeMetricInstant(w http.ResponseWriter, r *http.Request, spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc string) bool {
	evalTS, err := parseTimestamp(r.FormValue("time"))
	if err != nil {
		evalTS = time.Now()
	}
	field, quantile, fieldErr := p.resolveManualMetricField(spec, origSpec, manualFunc)
	if fieldErr != nil {
		p.writeError(w, http.StatusBadRequest, fieldErr.Error())
		return true
	}
	if field == "" {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid aggregation %s without unwrap", unwrapErrorFuncName(origSpec.Func)))
		return true
	}

	series, err := p.collectRangeMetricSamples(r.Context(), spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, field, origSpec.UnwrapConv, evalTS.Add(-origSpec.Window), evalTS)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return true
	}

	result := buildManualRangeMetricVector(manualFunc, quantile, series, evalTS, origSpec.Window)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
	return true
}

func (p *Proxy) resolveManualMetricField(spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc string) (string, float64, error) {
	switch manualFunc {
	case "rate", "count_over_time":
		return "__count__", 0, nil
	case "bytes_over_time", "bytes_rate":
		return "__bytes__", 0, nil
	}

	if manualFunc == "quantile" {
		phi, field, ok := parseStatsQuantileSpec(spec.Field)
		if !ok {
			return "", 0, fmt.Errorf("invalid quantile query")
		}
		field = p.labelTranslator.ToVL(field)
		if strings.TrimSpace(field) == "" {
			return "", 0, fmt.Errorf("invalid aggregation %s without unwrap", unwrapErrorFuncName(origSpec.Func))
		}
		return field, phi, nil
	}

	if origSpec.UnwrapField != "" {
		return p.labelTranslator.ToVL(origSpec.UnwrapField), 0, nil
	}
	field := strings.TrimSpace(spec.Field)
	if field == "" {
		return "", 0, nil
	}
	return p.labelTranslator.ToVL(field), 0, nil
}

// collectRangeMetricHits calls VL's /select/logsql/stats_query_range endpoint and
// returns pre-bucketed samples (ts=bucket_start_ns, value=bucket_value). This
// avoids reading all raw log entries for count_over_time / rate / bytes_rate queries.
//
// statsAggFunc is the VL stats aggregation clause appended after "| stats [by (...)]",
// e.g. "count() as c" or "sum_len(_msg) as c".
//
// Only applicable when the output label set is fully determined by groupBy, i.e.
// len(groupBy) > 0 or byExplicit == true (so we don't need _stream expansion).
func (p *Proxy) collectRangeMetricHits(
	ctx context.Context,
	baseQuery string,
	groupBy, origGroupBy []string,
	byExplicit bool,
	statsAggFunc string,
	start, end time.Time,
	hitStep time.Duration,
) (map[string]manualSeriesSamples, error) {
	if hitStep <= 0 {
		hitStep = time.Minute
	}

	// Build LogsQL stats query so VL returns pre-aggregated bucket values.
	// stats_query_range understands stream labels (stored in _stream), unlike /hits.
	var statsQuery string
	if len(groupBy) > 0 && !byExplicit {
		statsQuery = baseQuery + " | stats by (" + strings.Join(groupBy, ", ") + ") " + statsAggFunc
	} else {
		statsQuery = baseQuery + " | stats " + statsAggFunc
	}

	params := url.Values{}
	params.Set("query", statsQuery)
	params.Set("start", strconv.FormatInt(start.Unix(), 10))
	params.Set("end", strconv.FormatInt(end.Unix(), 10))
	params.Set("step", strconv.FormatFloat(hitStep.Seconds(), 'f', 0, 64)+"s")

	resp, err := p.vlPost(ctx, "/select/logsql/stats_query_range", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, fmt.Errorf("stats_query_range backend %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	const maxStatsResponseBytes = 64 << 20 // 64 MB
	body, err := readBodyLimited(resp.Body, maxStatsResponseBytes)
	if err != nil {
		return nil, err
	}

	// Build VL→Loki label name mapping from groupBy↔origGroupBy.
	vlToLoki := make(map[string]string, len(groupBy))
	for i, vlName := range groupBy {
		if i < len(origGroupBy) {
			vlToLoki[vlName] = origGroupBy[i]
		} else {
			vlToLoki[vlName] = vlName
		}
	}

	// Parse Prometheus range vector response:
	// {"status":"success","data":{"result":[{"metric":{...},"values":[[ts,"v"],...]}]}}
	v, parseErr := fj.ParseBytes(body)
	if parseErr != nil {
		return nil, fmt.Errorf("parse stats_query_range response: %w", parseErr)
	}
	if status := string(v.GetStringBytes("status")); status != "success" {
		return nil, fmt.Errorf("stats_query_range non-success status: %s", status)
	}

	results := v.GetArray("data", "result")
	seriesMap := make(map[string]manualSeriesSamples, len(results))
	for _, res := range results {
		metricObj := res.GetObject("metric")
		metric := make(map[string]string)
		metricObj.Visit(func(k []byte, mv *fj.Value) {
			key := string(k)
			if key == "__name__" {
				return
			}
			lokiKey, ok := vlToLoki[key]
			if !ok {
				lokiKey = p.labelTranslator.ToLoki(key)
			}
			metric[lokiKey] = string(mv.GetStringBytes())
		})
		seriesKey := canonicalLabelsKey(metric)

		values := res.GetArray("values")
		samples := make([]rangeMetricSample, 0, len(values))
		for _, pair := range values {
			arr := pair.GetArray()
			if len(arr) < 2 {
				continue
			}
			tsUnix, tsErr := arr[0].Int64()
			if tsErr != nil {
				continue
			}
			valStr := string(arr[1].GetStringBytes())
			val, valErr := strconv.ParseFloat(valStr, 64)
			if valErr != nil {
				continue
			}
			samples = append(samples, rangeMetricSample{ts: tsUnix * int64(time.Second), value: val})
		}

		if existing, ok := seriesMap[seriesKey]; ok {
			existing.Samples = append(existing.Samples, samples...)
			sort.Slice(existing.Samples, func(i, j int) bool { return existing.Samples[i].ts < existing.Samples[j].ts })
			seriesMap[seriesKey] = existing
		} else {
			seriesMap[seriesKey] = manualSeriesSamples{Metric: metric, Samples: samples}
		}
	}
	return seriesMap, nil
}

// metricSeriesCacheEntry holds the pre-computed labels and key for a metric series.
// Cached per (_stream, level) composite within a single collectRangeMetricSamples
// call: entries sharing the same stream identity produce identical series, so the
// expensive label-copy + translation + key-build runs once per distinct stream.
type metricSeriesCacheEntry struct {
	metricLabels map[string]string // pre-translation, needed for parsed-label slow path
	translated   map[string]string // after labelTranslator
	key          string
}

func (p *Proxy) collectRangeMetricSamples(ctx context.Context, baseQuery string, groupBy, origGroupBy []string, byExplicit bool, field, unwrapConv string, start, end time.Time) (map[string]manualSeriesSamples, error) {
	params := url.Values{}
	params.Set("query", baseQuery)
	params.Set("start", formatVLTimestamp(start.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(end.UTC().Format(time.RFC3339Nano)))
	// Keep this high to avoid truncating series for compatibility stats functions.
	// Configurable via -manual-range-metric-row-limit; default 1,000,000.
	rowLimit := p.rangeMetricRowLimit
	if rowLimit <= 0 {
		rowLimit = 1_000_000
	}
	params.Set("limit", strconv.Itoa(rowLimit))

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("backend returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// seriesCache caches per (_stream + "|" + level) within this request.
	// Avoids repeated label-map allocation for the dominant case where thousands
	// of log lines share the same stream identity (same series).
	seriesCache := make(map[string]*metricSeriesCacheEntry, 32)
	seriesMap := make(map[string]manualSeriesSamples)
	includeParsedLabels := queryUsesParserStages(baseQuery)

	fjp := vlFJParserPool.Get()
	defer vlFJParserPool.Put(fjp)

	// Stream the response line by line — avoids io.ReadAll + bytes.Split which
	// would buffer the entire VL response (up to limit=1000000 lines) in memory.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		v, parseErr := fjp.ParseBytes(line)
		if parseErr != nil {
			continue
		}

		rawTS := string(v.GetStringBytes("_time"))
		if rawTS == "" {
			continue
		}
		normTS, ok := formatEntryTimestamp(rawTS)
		if !ok {
			continue
		}
		ts, err := strconv.ParseInt(normTS, 10, 64)
		if err != nil {
			continue
		}
		if ts < 1e12 {
			ts *= int64(time.Second)
		}

		sampleValue, ok := p.extractManualSampleValueFJ(v, field, unwrapConv)
		if !ok {
			continue
		}

		streamStr := string(v.GetStringBytes("_stream"))
		levelStr := strings.TrimSpace(string(v.GetStringBytes("level")))

		var seriesEntry *metricSeriesCacheEntry

		if !includeParsedLabels || len(groupBy) == 0 {
			// Hot path: series identity depends only on _stream + level.
			// Build once per unique (stream, level) pair and reuse across all matching lines.
			cacheKey := streamStr + "|" + levelStr
			seriesEntry = seriesCache[cacheKey]
			if seriesEntry == nil {
				seriesEntry = p.buildMetricSeriesEntry(streamStr, levelStr, groupBy, byExplicit, origGroupBy)
				seriesCache[cacheKey] = seriesEntry
			}
		} else {
			// Slow path: by(...) includes parsed fields extracted per entry.
			// Extend the cache key with the extracted field values so entries with
			// the same parsed-field values still hit the cache.
			cacheKey := p.buildParsedGroupByCacheKey(streamStr, levelStr, v, groupBy)
			seriesEntry = seriesCache[cacheKey]
			if seriesEntry == nil {
				base := p.buildMetricSeriesEntry(streamStr, levelStr, groupBy, byExplicit, origGroupBy)
				// Copy base metric labels and inject per-entry parsed fields.
				metricLabels := make(map[string]string, len(base.metricLabels))
				for k, val := range base.metricLabels {
					metricLabels[k] = val
				}
				// Only inject parsed labels that appear in the explicit by(...) list.
				// Adding ALL parsed fields (old behaviour) creates one series per unique
				// JSON-object combination — O(N) series for N distinct log entries.
				// Loki groups by stream labels only when no by(...) is present; parsed
				// fields become metric dimensions only when explicitly named in by(...).
				addGroupByParsedLabelsFJ(metricLabels, v, groupBy)
				translatedParsed := p.labelTranslator.TranslateLabelsMap(metricLabels)
				seriesEntry = &metricSeriesCacheEntry{
					metricLabels: metricLabels,
					translated:   translatedParsed,
					key:          canonicalLabelsKey(translatedParsed),
				}
				seriesCache[cacheKey] = seriesEntry
			}
		}

		current := seriesMap[seriesEntry.key]
		if current.Metric == nil {
			current.Metric = seriesEntry.translated
		}
		current.Samples = append(current.Samples, rangeMetricSample{ts: ts, value: sampleValue})
		seriesMap[seriesEntry.key] = current
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, fmt.Errorf("scanning VL response: %w", scanErr)
	}

	for key, series := range seriesMap {
		sort.Slice(series.Samples, func(i, j int) bool { return series.Samples[i].ts < series.Samples[j].ts })
		seriesMap[key] = series
	}

	return seriesMap, nil
}

// buildMetricSeriesEntry constructs the label maps and series key for a given
// (_stream, level) pair. Called once per distinct stream identity per request.
func (p *Proxy) buildMetricSeriesEntry(streamStr, levelStr string, groupBy []string, byExplicit bool, origGroupBy []string) *metricSeriesCacheEntry {
	rawStreamLabels := parseStreamLabels(streamStr)
	streamLabels := make(map[string]string, len(rawStreamLabels))
	for k, v := range rawStreamLabels {
		streamLabels[k] = v
	}
	if levelStr != "" {
		streamLabels["level"] = levelStr
		streamLabels["detected_level"] = levelStr
	}
	if strings.TrimSpace(streamLabels["detected_level"]) == "" {
		streamLabels["detected_level"] = "unknown"
	}
	ensureSyntheticServiceName(streamLabels)

	metricLabels := buildManualMetricLabels(streamLabels, groupBy, byExplicit)

	// Rename VL-translated groupBy keys back to their original Loki names.
	// Example: VL "level" was produced by translating Loki "detected_level";
	// the response metric must carry "detected_level" to match what Drilldown
	// requested in "sum(...) by (detected_level)".
	if len(origGroupBy) == len(groupBy) {
		for i, vlKey := range groupBy {
			if lokiKey := origGroupBy[i]; lokiKey != vlKey {
				if val, ok := metricLabels[vlKey]; ok {
					delete(metricLabels, vlKey)
					metricLabels[lokiKey] = val
				} else if val, ok := streamLabels[lokiKey]; ok {
					// VL form not found in metric (e.g. "service.name" absent for
					// Loki-push data); fall back to the underscore stream label name.
					metricLabels[lokiKey] = val
				}
			}
		}
	}

	translated := p.labelTranslator.TranslateLabelsMap(metricLabels)
	return &metricSeriesCacheEntry{
		metricLabels: metricLabels,
		translated:   translated,
		key:          canonicalLabelsKey(translated),
	}
}

// buildParsedGroupByCacheKey builds a cache key for the slow path (parsed labels).
// It extends the (_stream, level) base with the values of each groupBy field found
// in the log entry, so entries with identical parsed-field values still reuse the
// pre-built label map.
func (p *Proxy) buildParsedGroupByCacheKey(streamStr, levelStr string, v *fj.Value, groupBy []string) string {
	var b strings.Builder
	b.WriteString(streamStr)
	b.WriteByte('|')
	b.WriteString(levelStr)
	for _, key := range groupBy {
		if isVLInternalField(key) || key == "_stream_id" || key == "_stream" {
			continue
		}
		fv := v.Get(key)
		if fv == nil {
			continue
		}
		val, ok := stringifyFJValue(fv)
		if !ok {
			continue
		}
		b.WriteByte('|')
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(val)
	}
	return b.String()
}

// extractManualSampleValueFJ is the fastjson variant of extractManualSampleValue.
// Zero heap allocation for the __count__ and __bytes__ hot paths.
func (p *Proxy) extractManualSampleValueFJ(v *fj.Value, field, unwrapConv string) (float64, bool) {
	switch field {
	case "__count__":
		return 1, true
	case "__bytes__":
		return float64(len(v.GetStringBytes("_msg"))), true
	}

	raw := p.lookupFJField(v, p.manualValueCandidateFields(field))
	if raw == nil {
		return 0, false
	}

	switch unwrapConv {
	case "duration":
		s, ok := stringifyFJValue(raw)
		if !ok {
			return 0, false
		}
		return parseDuration(s)
	case "bytes":
		s, ok := stringifyFJValue(raw)
		if !ok {
			return 0, false
		}
		return parseBytes(s)
	default:
		return parseFloatValueFJ(raw)
	}
}

// lookupFJField returns the first non-nil field from v matching any key in keys.
func (p *Proxy) lookupFJField(v *fj.Value, keys []string) *fj.Value {
	for _, key := range keys {
		if fv := v.Get(key); fv != nil {
			return fv
		}
	}
	return nil
}

// parseFloatValueFJ extracts a float64 from a fastjson value without interface{} boxing.
func parseFloatValueFJ(v *fj.Value) (float64, bool) {
	switch v.Type() {
	case fj.TypeNumber:
		f, err := v.Float64()
		return f, err == nil
	case fj.TypeString:
		f, err := strconv.ParseFloat(strings.TrimSpace(string(v.GetStringBytes())), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// addGroupByParsedLabelsFJ is the fastjson variant of addGroupByParsedLabels.
func addGroupByParsedLabelsFJ(metricLabels map[string]string, v *fj.Value, groupBy []string) {
	for _, key := range groupBy {
		if isVLInternalField(key) || key == "_stream_id" {
			continue
		}
		if _, exists := metricLabels[key]; exists {
			continue
		}
		fv := v.Get(key)
		if fv == nil {
			continue
		}
		value, ok := stringifyFJValue(fv)
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if value != "" {
			metricLabels[key] = value
		}
	}
}

func queryUsesParserStages(baseQuery string) bool {
	if strings.Contains(baseQuery, "| unpack_") {
		return true
	}
	if strings.Contains(baseQuery, "| extract ") {
		return true
	}
	if strings.Contains(baseQuery, "| extract_regexp ") {
		return true
	}
	return false
}

// addGroupByParsedLabels injects only the labels named in groupBy from the
// parsed log entry. This matches Loki's behaviour: without an explicit by(...)
// clause, rate/count_over_time groups by stream labels only; parsed-field values
// only become metric dimensions when the caller explicitly names them in by(...).
func addGroupByParsedLabels(metricLabels map[string]string, entry map[string]interface{}, groupBy []string) {
	for _, key := range groupBy {
		if isVLInternalField(key) || key == "_stream_id" {
			continue
		}
		if _, exists := metricLabels[key]; exists {
			continue
		}
		if raw, ok := entry[key]; ok {
			if value, ok := stringifyEntryValue(raw); ok {
				value = strings.TrimSpace(value)
				if value != "" {
					metricLabels[key] = value
				}
			}
		}
	}
}

func addParsedEntryLabels(metricLabels map[string]string, entry map[string]interface{}, unwrapField string) {
	if metricLabels == nil {
		return
	}
	excluded := map[string]struct{}{}
	addExcludedField(excluded, unwrapField)
	addExcludedField(excluded, strings.ReplaceAll(unwrapField, ".", "_"))
	addExcludedField(excluded, strings.ReplaceAll(unwrapField, "_", "."))

	for key, raw := range entry {
		if isVLInternalField(key) || key == "_stream_id" {
			continue
		}
		if _, skip := excluded[key]; skip {
			continue
		}
		value, ok := stringifyEntryValue(raw)
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := metricLabels[key]; exists {
			continue
		}
		metricLabels[key] = value
	}
}

func addExcludedField(excluded map[string]struct{}, key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	excluded[key] = struct{}{}
}

type manualSeriesSamples struct {
	Metric  map[string]string
	Samples []rangeMetricSample
}

func buildManualMetricLabels(streamLabels map[string]string, groupBy []string, byExplicit bool) map[string]string {
	if byExplicit && len(groupBy) == 0 {
		// Explicit "by ()" — one series total, no label dimensions.
		return map[string]string{}
	}
	if len(groupBy) == 0 {
		labels := make(map[string]string, len(streamLabels))
		for k, v := range streamLabels {
			labels[k] = v
		}
		return labels
	}

	// "_stream" is a sentinel added by addStatsByStreamClause meaning "group by the
	// full stream identity". The streamLabels map already holds the expanded key/value
	// pairs from _stream, so looking up "_stream" directly always misses. Expand it
	// into all stream labels so that applyWithoutGrouping can remove specific keys
	// rather than collapsing every series into one {} bucket.
	streamExpand := false
	for _, key := range groupBy {
		if key == "_stream" {
			streamExpand = true
			break
		}
	}

	labels := make(map[string]string, len(streamLabels))
	if streamExpand {
		for k, v := range streamLabels {
			labels[k] = v
		}
	}
	for _, key := range groupBy {
		if key == "_stream" {
			continue
		}
		if value, ok := streamLabels[key]; ok {
			labels[key] = value
		}
	}
	return labels
}

func (p *Proxy) manualValueCandidateFields(field string) []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	add(field)
	add(p.labelTranslator.ToVL(field))
	add(strings.ReplaceAll(field, "_", "."))
	return out
}

func parseFloatValue(raw interface{}) (float64, bool) {
	switch value := raw.(type) {
	case float64:
		return value, true
	case json.Number:
		f, err := value.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		return f, err == nil
	default:
		f, err := strconv.ParseFloat(strings.TrimSpace(asString(raw)), 64)
		return f, err == nil
	}
}

func buildManualRangeMetricMatrix(functionName string, quantile float64, series map[string]manualSeriesSamples, start, end time.Time, step, window time.Duration) []byte {
	if end.Before(start) {
		return marshalManualMetricResponse("matrix", []map[string]interface{}{})
	}

	perSeries := make(map[string]map[string]interface{}, len(series))
	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for t := start; !t.After(end); t = t.Add(step) {
		windowStart := t.Add(-window).UnixNano()
		windowEnd := t.UnixNano()
		for _, key := range keys {
			seriesEntry := series[key]
			value, ok := aggregateManualWindow(functionName, quantile, seriesEntry.Samples, windowStart, windowEnd, window.Seconds())
			if !ok {
				continue
			}

			dst := perSeries[key]
			if dst == nil {
				dst = map[string]interface{}{
					"metric": seriesEntry.Metric,
					"values": make([][]interface{}, 0, 16),
				}
				perSeries[key] = dst
			}

			points := dst["values"].([][]interface{})
			points = append(points, []interface{}{float64(t.Unix()), strconv.FormatFloat(value, 'f', -1, 64)})
			dst["values"] = points
		}
	}

	results := make([]map[string]interface{}, 0, len(perSeries))
	for _, key := range keys {
		if seriesResult, ok := perSeries[key]; ok {
			results = append(results, seriesResult)
		}
	}
	return marshalManualMetricResponse("matrix", results)
}

// buildHitsRangeMetricMatrix builds a Prometheus matrix response from pre-bucketed
// hit counts returned by collectRangeMetricHits. Each sample is a bucket count;
// the window is applied by summing all buckets whose start falls in [T-window, T)
// for each step point T. Supports count_over_time (sum) and rate (sum/window_s).
func buildHitsRangeMetricMatrix(manualFunc string, series map[string]manualSeriesSamples, start, end time.Time, step, window time.Duration) []byte {
	if end.Before(start) {
		return marshalManualMetricResponse("matrix", []map[string]interface{}{})
	}
	windowNS := window.Nanoseconds()
	windowSec := window.Seconds()

	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	perSeries := make(map[string]map[string]interface{}, len(series))

	for t := start; !t.After(end); t = t.Add(step) {
		tNS := t.UnixNano()
		windowStartNS := tNS - windowNS

		for _, key := range keys {
			seriesEntry := series[key]
			var sum float64
			for _, s := range seriesEntry.Samples {
				if s.ts >= windowStartNS && s.ts < tNS {
					sum += s.value
				}
			}
			if sum == 0 {
				continue
			}
			var value float64
			if manualFunc == "rate" {
				value = sum / windowSec
			} else {
				value = sum
			}

			dst := perSeries[key]
			if dst == nil {
				dst = map[string]interface{}{
					"metric": seriesEntry.Metric,
					"values": make([][]interface{}, 0, 16),
				}
				perSeries[key] = dst
			}
			points := dst["values"].([][]interface{})
			points = append(points, []interface{}{float64(t.Unix()), strconv.FormatFloat(value, 'f', -1, 64)})
			dst["values"] = points
		}
	}

	results := make([]map[string]interface{}, 0, len(perSeries))
	for _, key := range keys {
		if r, ok := perSeries[key]; ok {
			results = append(results, r)
		}
	}
	return marshalManualMetricResponse("matrix", results)
}

func buildManualRangeMetricVector(functionName string, quantile float64, series map[string]manualSeriesSamples, evalTime time.Time, window time.Duration) []byte {
	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	windowStart := evalTime.Add(-window).UnixNano()
	windowEnd := evalTime.UnixNano()
	results := make([]map[string]interface{}, 0, len(series))

	for _, key := range keys {
		seriesEntry := series[key]
		value, ok := aggregateManualWindow(functionName, quantile, seriesEntry.Samples, windowStart, windowEnd, window.Seconds())
		if !ok {
			continue
		}
		results = append(results, map[string]interface{}{
			"metric": seriesEntry.Metric,
			"value":  []interface{}{float64(evalTime.Unix()), strconv.FormatFloat(value, 'f', -1, 64)},
		})
	}

	return marshalManualMetricResponse("vector", results)
}

func marshalManualMetricResponse(resultType string, result []map[string]interface{}) []byte {
	if result == nil {
		result = []map[string]interface{}{}
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": resultType,
			"result":     result,
		},
	})
	return payload
}

func aggregateManualWindow(functionName string, quantile float64, samples []rangeMetricSample, windowStart, windowEnd int64, windowSeconds float64) (float64, bool) {
	// Slice-dependent functions: build filtered slice, then aggregate.
	switch functionName {
	case "quantile", "stddev", "stdvar", "rate_counter":
		values := make([]float64, 0, len(samples))
		for _, sample := range samples {
			if sample.ts < windowStart || sample.ts > windowEnd {
				continue
			}
			values = append(values, sample.value)
		}
		if len(values) == 0 {
			return 0, false
		}
		switch functionName {
		case "quantile":
			return quantileFloat64(values, quantile), true
		case "stddev":
			return stddevFloat64(values), true
		case "stdvar":
			v := stddevFloat64(values)
			return v * v, true
		case "rate_counter":
			if windowSeconds <= 0 {
				return 0, false
			}
			if len(values) == 1 {
				return 0, true
			}
			return rateCounterWindow(values, windowSeconds), true
		}
	}

	// Inline accumulator path — no heap allocation for common aggregations.
	var (
		count    int
		sum      float64
		minVal   float64
		maxVal   float64
		firstVal float64
		lastVal  float64
		hasFirst bool
	)
	for _, sample := range samples {
		if sample.ts < windowStart || sample.ts > windowEnd {
			continue
		}
		v := sample.value
		count++
		sum += v
		if !hasFirst {
			firstVal = v
			minVal = v
			maxVal = v
			hasFirst = true
		} else {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
		lastVal = v
	}

	if count == 0 {
		return 0, false
	}

	switch functionName {
	case "count_over_time":
		return float64(count), true
	case "rate":
		if windowSeconds <= 0 {
			return 0, false
		}
		return float64(count) / windowSeconds, true
	case "bytes_over_time", "sum":
		return sum, true
	case "bytes_rate":
		if windowSeconds <= 0 {
			return 0, false
		}
		return sum / windowSeconds, true
	case "avg":
		return sum / float64(count), true
	case "min":
		return minVal, true
	case "max":
		return maxVal, true
	case "first":
		return firstVal, true
	case "last":
		return lastVal, true
	default:
		return 0, false
	}
}

func sumFloat64(values []float64) float64 {
	var out float64
	for _, value := range values {
		out += value
	}
	return out
}

func stddevFloat64(values []float64) float64 {
	mean := sumFloat64(values) / float64(len(values))
	var variance float64
	for _, value := range values {
		diff := value - mean
		variance += diff * diff
	}
	variance /= float64(len(values))
	return math.Sqrt(variance)
}

func quantileFloat64(values []float64, phi float64) float64 {
	if phi < 0 {
		return math.Inf(-1)
	}
	if phi > 1 {
		return math.Inf(1)
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	if len(ordered) == 1 {
		return ordered[0]
	}
	rank := phi * float64(len(ordered)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return ordered[lower]
	}
	weight := rank - float64(lower)
	return ordered[lower]*(1-weight) + ordered[upper]*weight
}

func rateCounterWindow(values []float64, windowSeconds float64) float64 {
	var increase float64
	prev := values[0]
	for _, current := range values[1:] {
		if current >= prev {
			increase += current - prev
		} else {
			increase += current
		}
		prev = current
	}
	return increase / windowSeconds
}
