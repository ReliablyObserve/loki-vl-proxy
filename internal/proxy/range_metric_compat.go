package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
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

// parseSingleFieldCountSpec accepts only a translated query of the exact shape
// `<base> | stats by (<field>) count()` — the shape the single-field count fast
// paths (windowed /hits, two-phase top-N, Drilldown field paths) rebuild from
// BaseQuery. parseStatsCompatSpec reads only the first stats pipe, so the rate
// translation `| stats by (f) count() as __lvp_inner | math __lvp_inner/<window>
// ...` also reports Func "count"; rebuilding it as a bare count() drops the
// per-second division and returns raw window counts where Loki returns rates.
func parseSingleFieldCountSpec(logsqlQuery string) (statsCompatSpec, bool) {
	spec, ok := parseStatsCompatSpec(logsqlQuery)
	if !ok || spec.Func != "count" || len(spec.GroupBy) != 1 {
		return statsCompatSpec{}, false
	}
	// The count() stats pipe must be the final stage.
	rest := logsqlQuery[strings.Index(logsqlQuery, "| stats ")+len("| stats "):]
	if strings.Contains(rest, "|") || !strings.HasSuffix(strings.TrimSpace(rest), "count()") {
		return statsCompatSpec{}, false
	}
	return spec, true
}

// stripOuterLabelReplace removes label_replace(v, ...) wrappers from a logql
// expression, returning the innermost wrapped expression. This allows
// parseOriginalRangeMetricSpec to reach the actual metric function even when
// the query is wrapped in one or more label_replace() calls.
func stripOuterLabelReplace(logql string) string {
	for {
		logql = strings.TrimSpace(logql)
		if !strings.HasPrefix(logql, "label_replace(") {
			break
		}
		idx := strings.Index(logql, "(")
		if idx < 0 {
			break
		}
		depth := 0
		commaAt := -1
		for i := idx + 1; i < len(logql); i++ {
			c := logql[i]
			switch {
			case c == '(':
				depth++
			case c == ')':
				if depth == 0 {
					goto done
				}
				depth--
			case c == ',' && depth == 0:
				commaAt = i
				goto done
			}
		}
	done:
		if commaAt < 0 {
			break
		}
		logql = strings.TrimSpace(logql[idx+1 : commaAt])
	}
	return logql
}

func parseOriginalRangeMetricSpec(logql string) (originalRangeMetricSpec, bool) {
	logql = strings.TrimSpace(logql)
	// Strip label_replace wrappers so the inner metric function is visible.
	logql = stripOuterLabelReplace(logql)
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

// isLogRangeWindowFunc reports the log-line range functions whose Loki window
// (t-range, t] excludes lines on its lower edge and includes the evaluation time.
func isLogRangeWindowFunc(manualFunc string) bool {
	switch manualFunc {
	case "rate", "count_over_time", "bytes_over_time", "bytes_rate":
		return true
	}
	return false
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
	// (e.g. sum(rate({...} | json [w]))). For tumbling windows (range == step), VL can
	// execute them natively — no manual decomposition needed. For any other range, fall
	// through to the manual path which evaluates each step's (T-range, T] window.
	//
	// Exception: queries with parser stages (| json, | logfmt, etc.) without an explicit
	// "| drop __error__" opt-in must NOT use VL native stats for tumbling windows. Loki
	// excludes parse-failed lines from metric aggregation; VL counts all lines. The manual
	// path (collectRangeMetricSamples) preserves Loki's error-exclusion semantics.
	if strings.Contains(logsqlQuery, "| math ") {
		step, stepOk := parsePositiveStepDuration(r.FormValue("step"))
		origSpec, hasOrigSpec := parseOriginalRangeMetricSpec(originalLogql)
		if stepOk && hasOrigSpec && origSpec.Window > 0 && p.statsRangeIsTumbling(r, origSpec.Window, step) {
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
	rangeEqualsStep := p.statsRangeIsTumbling(r, origSpec.Window, step)
	// For tumbling windows, an explicit "| drop __error__" in the original LogQL opts in to
	// VL's count-all semantics (parse failures counted). Use origSpec.BaseQuery — the inner
	// pipeline without outer aggregation or range brackets — so hasDropErrorOnlyPostParserStage
	// can correctly identify the drop-error clause.
	if manualFunc != "quantile" && rangeEqualsStep && queryUsesParserStages(spec.BaseQuery) && hasOrigSpec && hasDropErrorOnlyPostParserStage(origSpec.BaseQuery) {
		return false
	}
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, rangeEqualsStep) {
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
	// A bare outer aggregation without by() collapses all streams into one empty-label
	// series in Loki. Set ByExplicit=true so buildManualMetricLabels returns {} and
	// collectRangeMetricSamples produces a single series — not one per stream.
	if len(spec.GroupBy) == 0 && !spec.ByExplicit && hasOuterAggregationWithoutBy(originalLogql) {
		spec.ByExplicit = true
	}
	return p.proxyManualRangeMetricRange(w, r, spec, origSpec, manualFunc)
}

// statsRangeIsTumbling reports whether native step buckets answer a range
// metric: only when the range equals the step, where one VictoriaLogs bucket is
// exactly one Loki window, and the backend can place bucket edges on the
// request's windows (tumblingBucketsAligned). With range < step a bucket also
// holds the lines between windows, which Loki never counts, and with range >
// step the windows overlap; both use the anchored window evaluator.
//
// Grafana Logs Drilldown requests keep their earlier routing, where range <=
// step is native: their hits and hybrid paths build, coarsen and zero-fill a
// bucket-start axis that every panel of the page shares.
func (p *Proxy) statsRangeIsTumbling(r *http.Request, window, step time.Duration) bool {
	if step <= 0 || window <= 0 || window > step {
		return false
	}
	if isGrafanaDrilldownRequest(r) {
		return true
	}
	return window == step && p.tumblingBucketsAligned(r, window)
}

// tumblingBucketsAligned reports whether stats_query_range buckets of window
// can cover the request's evaluation windows: always when the backend honours
// the offset arg (VictoriaLogs v1.45+), otherwise only when the start is
// epoch-aligned to window, like slidingStatsBucket. An unknown version (probe
// pending or failed) counts as no offset support.
func (p *Proxy) tumblingBucketsAligned(r *http.Request, window time.Duration) bool {
	if p.supportsStatsRangeOffset() {
		return true
	}
	startNs, ok := parseLokiTimeToUnixNano(r.FormValue("start"))
	return ok && window > 0 && startNs%int64(window) == 0
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
	if manualFunc != "quantile" && queryUsesParserStages(spec.BaseQuery) && hasOrigSpec && hasDropErrorOnlyPostParserStage(origSpec.BaseQuery) {
		return false
	}
	// Instant queries have no step: the range window is the entire lookback interval,
	// not a sliding window. VL native stats correctly evaluates [time-range, time].
	// Only rate_counter still requires the manual path (counter-reset semantics).
	// Exception: parser-stage queries without explicit drop-error still use the manual path
	// so that bare outer aggregations (sum without by()) correctly collapse all streams into
	// one series via ByExplicit=true. Native VL stats returns per-stream series for such
	// queries; the manual path aggregates them into the expected single series.
	if queryUsesParserStages(spec.BaseQuery) {
		// Parser+no-drop-error → always manual for correct stream-collapse semantics.
	} else if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, true) {
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
	// A bare outer aggregation without by() collapses all streams into one empty-label
	// series in Loki. Set ByExplicit=true so buildManualMetricLabels returns {} and
	// collectRangeMetricSamples produces a single series — not one per stream.
	if len(spec.GroupBy) == 0 && !spec.ByExplicit && hasOuterAggregationWithoutBy(originalLogql) {
		spec.ByExplicit = true
	}
	return p.proxyManualRangeMetricInstant(w, r, spec, origSpec, manualFunc)
}

// hasOuterAggregationWithoutBy reports whether logql starts with a bare outer aggregation
// (sum, avg, max, min, count, …) that carries no by() or without() grouping modifier.
// Loki evaluates such expressions as a single series with no label dimensions.
func hasOuterAggregationWithoutBy(logql string) bool {
	logql = strings.TrimSpace(logql)
	logql = stripOuterLabelReplace(logql)
	loc := outerAggregationRE.FindStringIndex(logql)
	if loc == nil || loc[0] != 0 || loc[1] >= len(logql) {
		return false
	}
	// Guard against prefix collisions: outerAggregationRE matches "count" as a prefix of
	// "count_over_time". The text after the aggregation keyword (+ optional by/without
	// clause) must start with "(" to be a genuine outer aggregation operator.
	if !strings.HasPrefix(strings.TrimSpace(logql[loc[1]:]), "(") {
		return false
	}
	matched := strings.ToLower(logql[:loc[1]])
	return !strings.Contains(matched, " by") && !strings.Contains(matched, " without")
}

// parseTopKWrapper detects a top-level topk(K, expr) or bottomk(K, expr) wrapper.
// Returns k, whether descending (true=topk, false=bottomk), ok=true only when
// topk/bottomk is outermost with a valid positive integer K.
func parseTopKWrapper(logql string) (k int, descending bool, ok bool) {
	if strings.TrimSpace(logql) == "" {
		return 0, false, false
	}
	parsed, err := logqlpkg.Parse(strings.TrimSpace(logql))
	if err != nil {
		return 0, false, false
	}
	va, isVA := parsed.(*logqlpkg.VectorAggregation)
	if !isVA || !va.HasParam {
		return 0, false, false
	}
	switch va.Op {
	case logqlpkg.VectorTopK:
		if int(va.Param) <= 0 {
			return 0, false, false
		}
		return int(va.Param), true, true
	case logqlpkg.VectorBottomK:
		if int(va.Param) <= 0 {
			return 0, false, false
		}
		return int(va.Param), false, true
	}
	return 0, false, false
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
	// Loki interpolates between adjacent ranked samples. VL's quantile uses a
	// different rank selection, and its range endpoint uses tumbling buckets.
	// Use the existing exact sample evaluator for both instant and range queries.
	if manualFunc == "rate_counter" || manualFunc == "quantile" {
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

// proxyManualRangeMetricRange evaluates a range metric in the proxy. Like Loki,
// it emits a sample only for steps whose window (t-range, t] holds log lines,
// for every client: absent steps stay absent (no zero-fill), which also keeps
// absent series out of topk/bottomk ranking.
func (p *Proxy) proxyManualRangeMetricRange(w http.ResponseWriter, r *http.Request, spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc string) bool {
	// The first translated stats clause may group by stream before the outer
	// sum. For additive log metrics, combine the raw counts/bytes before window
	// evaluation and keep the native stats fast path for aggregate-all queries.
	if isSumAllLogRange(r.FormValue("query")) {
		spec.GroupBy, spec.OrigGroupBy = nil, nil
		spec.ByExplicit = true
	}
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
		// Retain empty log lines without scanning raw logs: byte sum zero
		// alone cannot distinguish an absent bucket from a present empty line.
		statsAggFunc = "sum_len(_msg) as c, count() as __sample_count"
	}
	// Bucket edges must coincide with every evaluation window edge. When no
	// bounded bucket grid exists, the exact raw-sample evaluator answers.
	// fetchStart is the left edge of the first bucket (or raw fetch), and
	// sampleShift moves bucket labels onto the window they hold.
	fetchStart, sampleShift := startTS.Add(-origSpec.Window), time.Duration(0)
	bucket, bucketsOK := p.slidingStatsBucket(startTS, step, origSpec.Window)
	if origSpec.Window < step {
		// Windows shorter than the step are disjoint. Keep only lines inside one,
		// so a bucket per step, (t-step, t], holds exactly the window (t-range, t]
		// and a raw fetch reads only window lines. gcd(step, range) buckets would
		// grow with every step that range does not divide (a 604.8s step and a
		// 5m range need 2.4s buckets: 252k per series over 7 days).
		spec.BaseQuery += windowPhaseFilter(startTS, step, origSpec.Window)
		fetchStart, sampleShift = startTS.Add(-step), step-origSpec.Window
		bucket, bucketsOK = p.slidingStatsBucket(startTS, step, step)
	}
	if !bucketsOK {
		statsAggFunc = ""
	}
	// Drilldown burst coalescer: groups ~30 per-field field-presence count_over_time
	// queries into a single fused VL conditional-stats call. Detects byExplicit
	// aggregate-all queries with a | filter field != "" pattern.
	// extractCommonBase strips | json / | logfmt + the field filter so VL uses its
	// pre-indexed column index (| json before count() if returns empty in VL).
	if p.drilldownCoalescer != nil && sampleShift == 0 && origSpec.Window >= step && statsAggFunc == "count() as c" && spec.ByExplicit && len(spec.GroupBy) == 0 {
		if base, field, ok := extractCommonBase(spec.BaseQuery); ok {
			orgID := r.Header.Get("X-Scope-OrgID")
			bKey := burstKey{
				scope:   p.contextScopeFingerprint(r.Context()),
				orgID:   orgID,
				base:    base,
				startNs: startTS.Add(-origSpec.Window).UnixNano(),
				endNs:   endTS.UnixNano(),
				stepNs:  int64(bucket),
			}
			fireFn := p.fusedFieldHits(orgID, base, startTS.Add(-origSpec.Window), endTS, bucket)
			if series, coalErr := p.drilldownCoalescer.Submit(r.Context(), bKey, field, fireFn); coalErr == nil {
				p.writeHitsRangeMetricMatrix(w, manualFunc, series, startTS, endTS, step, origSpec.Window)
				return true
			}
			// Fall through on error — coalescer failure is non-fatal.
		}
	}
	// Anchored buckets hold every (t-range, t] window whatever range and step
	// are, so per-stream series (range != step) and parser stages use them too.
	// With parser stages the stats query keeps them: VictoriaLogs groups by the
	// parsed fields and counts every line, as the raw evaluator does.
	streamBuckets := origSpec.Window != step
	if series, ok := p.collectStatsFastPathHits(r.Context(), spec, statsAggFunc, fetchStart, endTS, bucket, streamBuckets, false); ok {
		p.writeHitsRangeMetricMatrix(w, manualFunc, shiftSeriesSamples(series, sampleShift), startTS, endTS, step, origSpec.Window)
		return true
	}
	if series, ok := p.collectParserStageStatsFastPathHits(r.Context(), spec, statsAggFunc, fetchStart, endTS, bucket); ok {
		p.writeHitsRangeMetricMatrix(w, manualFunc, shiftSeriesSamples(series, sampleShift), startTS, endTS, step, origSpec.Window)
		return true
	}
	if series, ok := p.collectStatsFastPathHits(r.Context(), spec, statsAggFunc, fetchStart, endTS, bucket, streamBuckets, true); ok {
		p.writeHitsRangeMetricMatrix(w, manualFunc, shiftSeriesSamples(series, sampleShift), startTS, endTS, step, origSpec.Window)
		return true
	}

	// VL's raw-query end is exclusive; Loki includes the evaluation time. That
	// holds for every range function, the same way the lower bound is excluded
	// for every one of them, so the fetch is extended unconditionally — leaving
	// it to a subset made the window open at BOTH ends for the rest.
	fetchEnd := endTS.Add(time.Nanosecond)
	series, err := p.collectRangeMetricSamples(r.Context(), spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, field, origSpec.UnwrapConv, startTS.Add(-origSpec.Window), fetchEnd)
	if err != nil {
		p.writeError(w, badRequestStatusOr(err, http.StatusBadGateway), err.Error())
		return true
	}

	result, err := buildManualRangeMetricMatrixContext(r.Context(), manualFunc, quantile, series, startTS, endTS, step, origSpec.Window, p.resolvedMaxStatsQuerySeries())
	if err != nil {
		p.writeError(w, http.StatusServiceUnavailable, err.Error())
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
	return true
}

// maxStatsBucketResponseBytes bounds one stats_query_range bucket response of
// the window evaluator; a variable so tests can exercise the overflow paths.
var maxStatsBucketResponseBytes int64 = 64 << 20

// windowPhaseFilter returns LogsQL pipes keeping only the lines inside one of
// the evaluation windows (t-window, t], t = start+k*step, of a range metric
// whose window is shorter than its step. The phase of a line is its distance
// past the window start preceding it, modulo the step: a line is in a window
// when 0 < phase <= window. VictoriaLogs evaluates math in float64, so a line
// within a few hundred nanoseconds of a window edge may fall on either side.
func windowPhaseFilter(start time.Time, step, window time.Duration) string {
	anchor := start.Add(-window).UnixNano()
	return " | math ((_time - " + strconv.FormatInt(anchor, 10) + ") % " + strconv.FormatInt(int64(step), 10) +
		") as __lvp_window_phase | filter __lvp_window_phase:>0 __lvp_window_phase:<=" + strconv.FormatInt(int64(window), 10)
}

// shiftSeriesSamples moves every bucket label and present bucket forward by
// shift, in place: a (t-step, t] bucket of windowPhaseFilter lines is labelled
// t-step and holds the window (t-window, t], which the evaluator reads from
// labels in [t-window, t).
func shiftSeriesSamples(series map[string]manualSeriesSamples, shift time.Duration) map[string]manualSeriesSamples {
	if shift == 0 {
		return series
	}
	for key, entry := range series {
		for i := range entry.Samples {
			entry.Samples[i].ts += int64(shift)
		}
		for i := range entry.PresentBuckets {
			entry.PresentBuckets[i] += int64(shift)
		}
		series[key] = entry
	}
	return series
}

// writeHitsRangeMetricMatrix writes the bucket matrix and returns the HTTP status.
func (p *Proxy) writeHitsRangeMetricMatrix(w http.ResponseWriter, manualFunc string, series map[string]manualSeriesSamples, start, end time.Time, step, window time.Duration) int {
	result, err := buildHitsRangeMetricMatrix(manualFunc, series, start, end, step, window)
	if err != nil {
		p.writeError(w, http.StatusServiceUnavailable, err.Error())
		return http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
	return http.StatusOK
}

func isSumAllLogRange(query string) bool {
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return false
	}
	agg, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok || agg.Op != logqlpkg.VectorSum || (agg.Grouping != nil && (agg.Grouping.Without || len(agg.Grouping.Labels) != 0)) {
		return false
	}
	ra, ok := agg.Inner.(*logqlpkg.RangeAggregation)
	if !ok {
		return false
	}
	switch ra.Op {
	case logqlpkg.RangeRate, logqlpkg.RangeCountOverTime, logqlpkg.RangeBytesRate, logqlpkg.RangeBytesOverTime:
	default:
		return false
	}
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok {
		return false
	}
	for _, stage := range lq.Pipeline {
		if _, ok := stage.(*logqlpkg.UnwrapStage); ok {
			return false
		}
	}
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

	// Inclusive upper bound for every function, as above.
	fetchEnd := evalTS.Add(time.Nanosecond)
	series, err := p.collectRangeMetricSamples(r.Context(), spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, field, origSpec.UnwrapConv, evalTS.Add(-origSpec.Window), fetchEnd)
	if err != nil {
		p.writeError(w, badRequestStatusOr(err, http.StatusBadGateway), err.Error())
		return true
	}

	result, err := buildManualRangeMetricVectorContext(r.Context(), manualFunc, quantile, series, evalTS, origSpec.Window, p.resolvedMaxStatsQuerySeries())
	if err != nil {
		p.writeError(w, http.StatusServiceUnavailable, err.Error())
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
	return true
}

// collectStatsFastPathHits handles count_over_time / rate / bytes_* queries with an
// explicit groupBy — served from VL's stats_query_range endpoint. withParser selects
// queries with parser stages (kept in the stats query, so parsed by() fields group as
// in the raw evaluator) instead of queries without them. Per-stream grouping (the
// _stream sentinel of bare range metrics) is served only without parser stages and
// when streamBuckets is set.
// Returns nil, false when the fast path is not applicable or VL returns an error.
func (p *Proxy) collectStatsFastPathHits(ctx context.Context, spec statsCompatSpec, statsAggFunc string, windowStart, end time.Time, step time.Duration, streamBuckets, withParser bool) (map[string]manualSeriesSamples, bool) {
	if statsAggFunc == "" || queryUsesParserStages(spec.BaseQuery) != withParser {
		return nil, false
	}
	for _, g := range spec.GroupBy {
		if g == "_stream" && (!streamBuckets || withParser) {
			return nil, false
		}
	}
	if len(spec.GroupBy) == 0 && !spec.ByExplicit {
		return nil, false
	}
	singleField := len(spec.GroupBy) == 1 && spec.GroupBy[0] != "_stream" && !spec.ByExplicit
	var (
		series map[string]manualSeriesSamples
		err    error
	)
	if singleField && end.Sub(windowStart) >= 2*time.Hour {
		// Over hours a field such as pod churns into thousands of values, and the
		// full bucket response can take seconds only to overflow: rank first.
		series, err = p.collectTopValueRangeMetricHits(ctx, spec, statsAggFunc, windowStart, end, step)
	} else {
		series, err = p.collectRangeMetricHits(ctx, spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, statsAggFunc, windowStart, end, step)
		if errors.Is(err, errBodyTooLarge) && singleField {
			// Too many series for one bucket response: keep the busiest values.
			series, err = p.collectTopValueRangeMetricHits(ctx, spec, statsAggFunc, windowStart, end, step)
		}
	}
	if err != nil {
		return nil, false
	}
	return series, true
}

// maxTopValueFilterBytes bounds the in() filter of collectTopValueRangeMetricHits,
// leaving room for the rest of the query under VictoriaLogs' default
// -search.maxQueryLen of 16384 bytes.
const maxTopValueFilterBytes = 12 << 10

// collectTopValueRangeMetricHits answers a single-field grouped bucket query in
// two phases: one stats_query ranks the field values by line count over the
// whole fetch window, up to the operator's -max-stats-query-series, then the
// bucket query runs, restricted to those values when the ranking was cut. The
// in() filter keeps the busiest values that fit maxTopValueFilterBytes. Lines
// without the field stay in when they rank. The series of the kept values are
// exact.
func (p *Proxy) collectTopValueRangeMetricHits(ctx context.Context, spec statsCompatSpec, statsAggFunc string, windowStart, end time.Time, step time.Duration) (map[string]manualSeriesSamples, error) {
	field := quoteLogsQLIdent(spec.GroupBy[0])
	limit := p.resolvedMaxStatsQuerySeries()
	params := url.Values{}
	params.Set("query", spec.BaseQuery+" | stats by ("+field+") count() as _c | sort by (_c desc) | limit "+strconv.Itoa(limit+1))
	params.Set("start", windowStart.UTC().Format(time.RFC3339Nano))
	params.Set("end", end.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano))
	params.Set("time", end.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano))
	resp, err := p.vlPost(ctx, "/select/logsql/stats_query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("stats_query backend", resp.StatusCode, body)
	}
	body, err := readBodyLimited(resp.Body, 4<<20)
	if err != nil {
		return nil, err
	}
	values := drilldownTopValuesFromMatrix(body, spec.GroupBy[0])
	if len(values) == 0 {
		// No labelled value in the window: the unfiltered query is small.
		return p.collectRangeMetricHits(ctx, spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, statsAggFunc, windowStart, end, step)
	}
	if len(values) < limit {
		// Every value ranked (the empty group may be one of them): no filter.
		return p.collectRangeMetricHits(ctx, spec.BaseQuery, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, statsAggFunc, windowStart, end, step)
	}
	values = values[:limit]
	filter := buildVLInFilter(spec.GroupBy[0], values)
	for len(filter) > maxTopValueFilterBytes && len(values) > 1 {
		values = values[:len(values)*maxTopValueFilterBytes/len(filter)]
		filter = buildVLInFilter(spec.GroupBy[0], values)
	}
	if drilldownTopValuesHaveEmpty(body, spec.GroupBy[0]) {
		filter = "(" + filter + " or " + field + `:"")`
	}
	return p.collectRangeMetricHits(ctx, spec.BaseQuery+" | filter "+filter, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, statsAggFunc, windowStart, end, step)
}

// collectParserStageStatsFastPathHits handles parser-stage GroupBy count queries
// (e.g. Drilldown field histograms with | json | delete __error__) by stripping
// the parser stages and retrying via stats_query_range against VL's column index.
// Safe only when all remaining filters after stripping are field-existence checks.
// Returns nil, false when inapplicable, on error, or when VL returns 0 series
// (non-indexed JSON fields still need parser stages to evaluate correctly).
func (p *Proxy) collectParserStageStatsFastPathHits(ctx context.Context, spec statsCompatSpec, statsAggFunc string, windowStart, end time.Time, step time.Duration) (map[string]manualSeriesSamples, bool) {
	if statsAggFunc == "" || !queryUsesParserStages(spec.BaseQuery) || len(spec.GroupBy) == 0 {
		return nil, false
	}
	if !strings.Contains(spec.BaseQuery, "| delete __error__") {
		return nil, false
	}
	for _, g := range spec.GroupBy {
		if g == "_stream" {
			return nil, false
		}
	}
	strippedBase := strings.TrimSpace(drilldownParserPipeRE.ReplaceAllString(spec.BaseQuery, ""))
	if strippedBase == spec.BaseQuery || !allFiltersAreExistenceChecks(strippedBase) {
		return nil, false
	}
	series, err := p.collectRangeMetricHits(ctx, strippedBase, spec.GroupBy, spec.OrigGroupBy, spec.ByExplicit, statsAggFunc, windowStart, end, step)
	if err != nil || len(series) == 0 {
		return nil, false
	}
	return series, true
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

// slidingStatsBucket returns gcd(step, window): the widest bucket for which every
// evaluation window (t-window, t], t = start+k*step, is an exact union of buckets
// anchored at start-window. There is no bucket-count budget: VictoriaLogs returns
// only non-empty buckets, so a response never holds more points than the log
// lines the raw evaluator would transfer, and collectRangeMetricHits already
// bounds the response bytes (an oversized response falls back to the raw
// evaluator). ok is false below a 1ms bucket, where float response timestamps
// cannot be snapped reliably, and when the backend cannot anchor bucket edges to
// the request (stats_query_range offset, VictoriaLogs v1.45+) and the anchor is
// not epoch-aligned; callers then use the raw-sample evaluator.
func (p *Proxy) slidingStatsBucket(start time.Time, step, window time.Duration) (time.Duration, bool) {
	bucket, rest := step, window
	for rest > 0 {
		bucket, rest = rest, bucket%rest
	}
	if bucket < time.Millisecond {
		return 0, false
	}
	if !p.supportsStatsRangeOffset() && start.Add(-window).UnixNano()%int64(bucket) != 0 {
		return 0, false
	}
	return bucket, true
}

// setSlidingStatsRangeParams requests buckets of hitStep whose edges sit at
// start+k*hitStep. With offset support each bucket is (edge, edge+hitStep],
// Loki's left-open, right-closed range boundary: VictoriaLogs buckets cover
// [T, T+step) with edges at k*step-offset, so edges are shifted by 1ns. The end
// is exclusive in VictoriaLogs and inclusive in Loki, hence end+1ns.
func (p *Proxy) setSlidingStatsRangeParams(params url.Values, start, end time.Time, hitStep time.Duration) {
	params.Set("start", start.UTC().Format(time.RFC3339Nano))
	params.Set("end", end.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano))
	if hitStep%time.Second == 0 {
		params.Set("step", strconv.FormatInt(int64(hitStep/time.Second), 10)+"s")
	} else {
		params.Set("step", strconv.FormatInt(int64(hitStep), 10)+"ns")
	}
	if p.supportsStatsRangeOffset() {
		shift := ((start.UnixNano()+1)%int64(hitStep) + int64(hitStep)) % int64(hitStep)
		params.Set("offset", strconv.FormatInt(-shift, 10)+"ns")
	}
}

// snapSlidingBucketTimestamp maps a stats_query_range timestamp (float seconds)
// to the left edge of its bucket on the start+k*hitStep grid. The float form
// cannot carry the 1ns edge shift or exact nanoseconds, so the nearest edge wins.
func snapSlidingBucketTimestamp(v *fj.Value, start time.Time, hitStep time.Duration) (int64, bool) {
	sec, err := v.Float64()
	if err != nil || hitStep <= 0 {
		return 0, false
	}
	return snapSlidingBucketNanos(int64(math.Round(sec*1e9)), start, hitStep), true
}

// snapSlidingBucketNanos maps a bucket timestamp in nanoseconds, which may carry
// the 1ns edge shift, to the nearest edge of the start+k*hitStep grid.
func snapSlidingBucketNanos(ts int64, start time.Time, hitStep time.Duration) int64 {
	anchor, size := start.UnixNano(), int64(hitStep)
	offset := ts - anchor + size/2
	k := offset / size
	if offset%size < 0 {
		k--
	}
	return anchor + k*size
}

// collectRangeMetricHits calls VL's /select/logsql/stats_query_range endpoint and
// returns pre-bucketed samples (ts=bucket left edge ns on the start+k*hitStep
// grid, value=bucket_value). This avoids reading all raw log entries for
// count_over_time / rate / bytes_rate queries.
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
	p.setSlidingStatsRangeParams(params, start, end, hitStep)

	// Acquire concurrency slot. The Drilldown Fields page fires ~30 of these
	// in parallel; without a cap all 30 hit VL simultaneously, causing a CPU storm.
	// Block until a slot is free or the request is cancelled.
	if sem := p.statsQueryRangeSem; sem != nil {
		select {
		case <-sem:
			defer func() { sem <- struct{}{} }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	resp, err := p.vlPost(ctx, "/select/logsql/stats_query_range", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("stats_query_range backend", resp.StatusCode, body)
	}

	body, err := readBodyLimited(resp.Body, maxStatsBucketResponseBytes)
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
	// Cap to maxStatsQuerySeries to bound response size for high-cardinality
	// by() clauses (e.g. churn-heavy pod names, *_id fields where each value
	// appears 1-2× in the window). Default 500 matches maxDrilldownSeries
	// (the cap the Drilldown-specific paths use) and aligns with Loki's
	// default max_query_series for the same purpose. The previous default
	// of 5000 returned 10× more sparse series than Drilldown can render
	// (the plugin's "Show all N" badge is literally the returned series
	// count) and made charts look like scattered single-point spikes
	// instead of meaningful top-N curves. See memory
	// [[drilldown-high-card-fields-known-limit]] for the deep investigation.
	// Keep the busiest maxSeries by total count (not the alphabetically-first
	// maxSeries VL returns) so the chart shows signal, not the noise floor.
	withPresence := strings.Contains(statsAggFunc, ", count() as __sample_count")
	if !withPresence {
		results = capStatsResultsByTotalCount(results, p.resolvedMaxStatsQuerySeries())
	}
	// Stream-grouped series carry the labels the raw evaluator derives from
	// _stream and level.
	streamGrouped := false
	for _, g := range groupBy {
		streamGrouped = streamGrouped || g == "_stream"
	}
	seriesMap := make(map[string]manualSeriesSamples, len(results))
	for _, res := range results {
		metricObj := res.GetObject("metric")
		var metric map[string]string
		if streamGrouped {
			metric = p.buildMetricSeriesEntry(string(metricObj.Get("_stream").GetStringBytes()), strings.TrimSpace(string(metricObj.Get("level").GetStringBytes())), groupBy, byExplicit, origGroupBy).translated
		} else {
			metric = make(map[string]string)
			metricObj.Visit(func(k []byte, mv *fj.Value) {
				key := string(k)
				if key == "__name__" {
					return
				}
				// Loki never keeps an empty label value; VictoriaLogs groups an
				// absent field as "".
				value := string(mv.GetStringBytes())
				if value == "" {
					return
				}
				lokiKey, ok := vlToLoki[key]
				if !ok {
					lokiKey = p.labelTranslator.ToLoki(key)
				}
				metric[lokiKey] = value
			})
		}
		seriesKey := canonicalLabelsKey(metric)
		if withPresence && string(res.GetStringBytes("metric", "__name__")) == "__sample_count" {
			entry := seriesMap[seriesKey]
			entry.Metric = metric
			addPresentBuckets(&entry, res.GetArray("values"), start, hitStep)
			seriesMap[seriesKey] = entry
			continue
		}

		values := res.GetArray("values")
		samples := make([]rangeMetricSample, 0, len(values))
		for _, pair := range values {
			arr := pair.GetArray()
			if len(arr) < 2 {
				continue
			}
			ts, tsOK := snapSlidingBucketTimestamp(arr[0], start, hitStep)
			if !tsOK {
				continue
			}
			valStr := string(arr[1].GetStringBytes())
			val, valErr := strconv.ParseFloat(valStr, 64)
			if valErr != nil {
				continue
			}
			samples = append(samples, rangeMetricSample{ts: ts, value: val})
		}

		if existing, ok := seriesMap[seriesKey]; ok {
			existing.Samples = append(existing.Samples, samples...)
			sort.Slice(existing.Samples, func(i, j int) bool { return existing.Samples[i].ts < existing.Samples[j].ts })
			seriesMap[seriesKey] = existing
		} else {
			seriesMap[seriesKey] = manualSeriesSamples{Metric: metric, Samples: samples}
		}
	}
	if withPresence {
		// Cap complete logical series, retaining both byte values and presence.
		seriesMap = capSeriesByTotalCount(seriesMap, p.resolvedMaxStatsQuerySeries())
	}
	return seriesMap, nil
}

func addPresentBuckets(entry *manualSeriesSamples, values []*fj.Value, start time.Time, hitStep time.Duration) {
	if entry.PresentBuckets == nil {
		entry.PresentBuckets = make([]int64, 0, len(values))
	}
	for _, pair := range values {
		arr := pair.GetArray()
		if len(arr) < 2 {
			continue
		}
		ts, tsOK := snapSlidingBucketTimestamp(arr[0], start, hitStep)
		count, countErr := strconv.ParseFloat(string(arr[1].GetStringBytes()), 64)
		if tsOK && countErr == nil && count > 0 {
			entry.PresentBuckets = append(entry.PresentBuckets, ts)
		}
	}
	// VictoriaLogs returns ascending buckets; merged streams can interleave.
	present := entry.PresentBuckets
	if !sort.SliceIsSorted(present, func(i, j int) bool { return present[i] < present[j] }) {
		sort.Slice(present, func(i, j int) bool { return present[i] < present[j] })
	}
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
	params.Set("start", formatVLTimestamp(start.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(end.UTC().Format(time.RFC3339Nano)))
	// Fetch one extra row to distinguish a complete response at the configured
	// limit from truncated input. Partial samples cannot produce valid metrics.
	rowLimit, err := p.manualMetricRowBudget()
	if err != nil {
		return nil, err
	}
	// A limit query argument makes VL sort all candidates by timestamp. The
	// limit pipe streams an arbitrary subset instead; successful responses are
	// complete because overflow is rejected, and samples are sorted below.
	params.Set("query", baseQuery+" | limit "+strconv.Itoa(rowLimit+1))

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("backend returned", resp.StatusCode, body)
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
	// would buffer the entire configured row budget plus overflow probe in memory.
	limited := &io.LimitedReader{R: resp.Body, N: maxBufferedBackendBodyBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)

	rows := 0
	for scanner.Scan() {
		if err := checkManualMetricRead(ctx, limited); err != nil {
			return nil, err
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		rows++
		if rows > rowLimit {
			return nil, fmt.Errorf("manual range metric row limit exceeded (%d); narrow the query or increase -manual-range-metric-row-limit", rowLimit)
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
			if len(seriesMap) >= p.resolvedMaxStatsQuerySeries() {
				return nil, fmt.Errorf("maximum metric series exceeded (%d)", p.resolvedMaxStatsQuerySeries())
			}
			current.Metric = seriesEntry.translated
		}
		current.Samples = append(current.Samples, rangeMetricSample{ts: ts, value: sampleValue})
		seriesMap[seriesEntry.key] = current
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, fmt.Errorf("scanning VL response: %w", scanErr)
	}
	if err := checkManualMetricRead(ctx, limited); err != nil {
		return nil, err
	}

	for key, series := range seriesMap {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
	b.Grow(len(streamStr) + 2 + len(levelStr) + len(groupBy)*32)
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

	if unwrapConv == "" {
		return parseFloatValueFJ(raw)
	}
	s, ok := stringifyFJValue(raw)
	if !ok {
		return 0, false
	}
	return convertUnwrapValue(s, unwrapConv)
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
	// `| unpack_logfmt` exposes pre-parsed fields without transforming the
	// log line — VL's stats_query_range handles it natively in tens of ms.
	// Excluding it from the "parser stages" check lets queries with a
	// `detected_level="..."` filter (which the translator rewrites to
	// `... | unpack_logfmt | filter level:="..."`) reach the stats fast
	// path instead of falling back to a 5–16s client-side log scan that
	// returned 143k unaggregated series for high-cardinality groupBy.
	if strings.Contains(baseQuery, "| unpack_json") {
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
	// PresentBuckets distinguishes real zero-byte lines from absent buckets: the
	// ascending left edges of buckets holding lines. Non-nil (possibly empty) only
	// for byte sums; raw log samples retain their compact shape.
	PresentBuckets []int64
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

// resolvedMaxStatsQuerySeries returns the per-request series cap for metric
// stats queries: the configured -max-stats-query-series, or the built-in
// default of 500 (matches maxDrilldownSeries and Loki's stock max_query_series).
func (p *Proxy) resolvedMaxStatsQuerySeries() int {
	if p != nil && p.maxStatsQuerySeries > 0 {
		return p.maxStatsQuerySeries
	}
	return 500
}

// capStatsResultsByTotalCount keeps only the maxSeries VL stats results with the
// highest summed bucket value, dropping the long tail. VL's stats_query_range
// returns results in LABEL (alphabetical) order, so a plain results[:maxSeries]
// slice keeps the alphabetically-first series — which for high-cardinality
// fields (churn-heavy pod names, *_id) is the NOISE FLOOR: ~344/500 pods with
// count==1 and only a handful with a meaningful count. Ranking by total count
// instead keeps the BUSIEST series, so the Drilldown chart shows the real
// signal (continuous lines for the top contributors) rather than scattered
// single-point spikes. For count_over_time the per-bucket values are counts and
// for bytes_* they are byte sums, so total value is the genuine busy-ness
// metric; rate ranks identically (rate = count/window is monotonic in count).
// Returns the input unchanged when it already fits or maxSeries<=0. Ties on
// total break on the metric JSON (ascending) for determinism.
// See memory [[drilldown-high-card-fields-known-limit]].
func capStatsResultsByTotalCount(results []*fj.Value, maxSeries int) []*fj.Value {
	if maxSeries <= 0 || len(results) <= maxSeries {
		return results
	}
	type scored struct {
		idx   int
		total float64
		key   string
	}
	ranked := make([]scored, len(results))
	for i, res := range results {
		var total float64
		for _, pair := range res.GetArray("values") {
			arr := pair.GetArray()
			if len(arr) < 2 {
				continue
			}
			if val, err := strconv.ParseFloat(string(arr[1].GetStringBytes()), 64); err == nil {
				total += val
			}
		}
		ranked[i] = scored{idx: i, total: total, key: res.Get("metric").String()}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].total != ranked[j].total {
			return ranked[i].total > ranked[j].total
		}
		return ranked[i].key < ranked[j].key
	})
	out := make([]*fj.Value, maxSeries)
	for i := 0; i < maxSeries; i++ {
		out[i] = results[ranked[i].idx]
	}
	return out
}

// capSeriesByTotalCount is the map-based analogue of capStatsResultsByTotalCount
// for paths that have already assembled a manualSeriesSamples map (e.g. the raw
// log-scan path via collectRangeMetricSamples → buildManualRangeMetricMatrix).
// Keeps the maxSeries series with the highest total sample value. Returns the
// input unchanged when it already fits or maxSeries<=0.
func capSeriesByTotalCount(series map[string]manualSeriesSamples, maxSeries int) map[string]manualSeriesSamples {
	if maxSeries <= 0 || len(series) <= maxSeries {
		return series
	}
	type scored struct {
		key   string
		total float64
	}
	ranked := make([]scored, 0, len(series))
	for key, s := range series {
		var total float64
		for _, smp := range s.Samples {
			total += smp.value
		}
		ranked = append(ranked, scored{key: key, total: total})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].total != ranked[j].total {
			return ranked[i].total > ranked[j].total
		}
		return ranked[i].key < ranked[j].key
	})
	capped := make(map[string]manualSeriesSamples, maxSeries)
	for i := 0; i < maxSeries; i++ {
		capped[ranked[i].key] = series[ranked[i].key]
	}
	return capped
}

func buildManualRangeMetricMatrix(functionName string, quantile float64, series map[string]manualSeriesSamples, start, end time.Time, step, window time.Duration, maxSeries int) []byte {
	// Legacy callers explicitly requested busiest-series truncation. Production
	// uses the error-returning Context entrypoint and must never silently truncate.
	series = capSeriesByTotalCount(series, maxSeries)
	result, _ := buildManualRangeMetricMatrixContext(context.Background(), functionName, quantile, series, start, end, step, window, maxSeries)
	return result
}

func buildManualRangeMetricMatrixContext(ctx context.Context, functionName string, quantile float64, series map[string]manualSeriesSamples, start, end time.Time, step, window time.Duration, maxSeries int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxSeries > 0 && len(series) > maxSeries {
		return nil, fmt.Errorf("manual metric series limit exceeded (%d); narrow the query", maxSeries)
	}
	ctx = binaryEvaluationContext(ctx)
	if end.Before(start) {
		return encodeBinarySeriesContext(ctx, nil, "matrix", maxBufferedBackendBodyBytes)
	}
	if step <= 0 || end.Sub(start)/step >= 1000000 {
		return nil, fmt.Errorf("invalid or excessive manual metric evaluation points")
	}

	perSeries := make(map[string]*binaryMatchedSeries)
	keys := make([]string, 0, len(series))
	sorted := make(map[string]bool, len(series))
	for key, entry := range series {
		keys = append(keys, key)
		samples := entry.Samples
		sorted[key] = sort.SliceIsSorted(samples, func(i, j int) bool { return samples[i].ts < samples[j].ts })
	}
	sort.Strings(keys)

	for t := start; !t.After(end); t = t.Add(step) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		windowStart := t.Add(-window).UnixNano()
		windowEnd := t.UnixNano()
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			seriesEntry := series[key]
			samples := seriesEntry.Samples
			if sorted[key] {
				// Narrow time-ordered samples to [windowStart, windowEnd]; the
				// aggregator applies the exact window bounds to that slice.
				lo := sort.Search(len(samples), func(i int) bool { return samples[i].ts >= windowStart })
				hi := lo + sort.Search(len(samples)-lo, func(i int) bool { return samples[lo+i].ts > windowEnd })
				samples = samples[lo:hi]
			}
			value, ok := aggregateManualWindow(functionName, quantile, samples, windowStart, windowEnd, window.Seconds())
			if !ok {
				continue
			}
			if err := checkBinaryOutputSample(ctx); err != nil {
				return nil, err
			}

			dst := perSeries[key]
			if dst == nil {
				if err := checkBinaryOutputLabels(ctx, seriesEntry.Metric); err != nil {
					return nil, err
				}
				dst = &binaryMatchedSeries{labels: seriesEntry.Metric}
				perSeries[key] = dst
			}

			dst.points = append(dst.points, []any{float64(t.Unix()), strconv.FormatFloat(value, 'f', -1, 64)})
		}
	}

	return encodeBinarySeriesContext(ctx, perSeries, "matrix", maxBufferedBackendBodyBytes)
}

// buildHitsRangeMetricMatrix builds a Prometheus matrix response from pre-bucketed
// counts returned by collectRangeMetricHits. Buckets are labelled by their left
// edge on a grid that contains every window edge, so the window (T-window, T] of
// step point T is the sum of all buckets whose label falls in [T-window, T).
// Supports count_over_time/bytes_over_time (sum) and rate/bytes_rate (sum/window_s).
//
// Like Loki, a step whose window holds no log line is absent rather than zero.
// Series with PresentBuckets (byte sums) use them to keep windows that contain
// only empty lines. Per-series prefix sums keep each step O(log buckets), so
// finer buckets do not multiply the work by the window length.
//
// The encoded response is bounded by maxBufferedBackendBodyBytes, like the raw
// evaluator's output; an estimate above it returns an error instead.
func buildHitsRangeMetricMatrix(manualFunc string, series map[string]manualSeriesSamples, start, end time.Time, step, window time.Duration) ([]byte, error) {
	if end.Before(start) || step <= 0 {
		return marshalManualMetricResponse("matrix", []map[string]interface{}{}), nil
	}
	encodedBytes := 0
	windowNS := window.Nanoseconds()
	windowSec := window.Seconds()

	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	expectedBuckets := int(end.Sub(start)/step) + 1
	if expectedBuckets > 32768 { // pre-size cap; appends grow beyond it
		expectedBuckets = 32768
	}

	results := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		seriesEntry := series[key]
		samples := seriesEntry.Samples
		if !sort.SliceIsSorted(samples, func(i, j int) bool { return samples[i].ts < samples[j].ts }) {
			// Coalesced results are shared between requests: sort a copy.
			samples = append([]rangeMetricSample(nil), samples...)
			sort.Slice(samples, func(i, j int) bool { return samples[i].ts < samples[j].ts })
		}
		prefix := make([]float64, len(samples)+1)
		for i, sample := range samples {
			prefix[i+1] = prefix[i] + sample.value
		}
		present := seriesEntry.PresentBuckets // ascending; nil without presence data

		for k, v := range seriesEntry.Metric {
			encodedBytes += len(k) + len(v) + 6
		}
		var points [][]interface{}
		for t := start; !t.After(end); t = t.Add(step) {
			tNS := t.UnixNano()
			windowStartNS := tNS - windowNS
			lo := sort.Search(len(samples), func(i int) bool { return samples[i].ts >= windowStartNS })
			hi := sort.Search(len(samples), func(i int) bool { return samples[i].ts >= tNS })
			sum := prefix[hi] - prefix[lo]
			hasLines := sum != 0
			if present != nil {
				pLo := sort.Search(len(present), func(i int) bool { return present[i] >= windowStartNS })
				hasLines = pLo < len(present) && present[pLo] < tNS
			}
			if !hasLines {
				continue
			}
			value := sum
			if manualFunc == "rate" || manualFunc == "bytes_rate" {
				value = sum / windowSec
			}
			if points == nil {
				points = make([][]interface{}, 0, expectedBuckets)
			}
			formatted := strconv.FormatFloat(value, 'f', -1, 64)
			// `[1700000000,"<value>"],`: a timestamp of at most 20 bytes plus punctuation.
			if encodedBytes += len(formatted) + 26; encodedBytes > maxBufferedBackendBodyBytes {
				return nil, fmt.Errorf("manual metric response exceeds %d bytes", maxBufferedBackendBodyBytes)
			}
			points = append(points, []interface{}{float64(t.Unix()), formatted})
		}
		if points != nil {
			results = append(results, map[string]interface{}{
				"metric": seriesEntry.Metric,
				"values": points,
			})
		}
	}
	return marshalManualMetricResponse("matrix", results), nil
}

func buildManualRangeMetricVector(functionName string, quantile float64, series map[string]manualSeriesSamples, evalTime time.Time, window time.Duration) []byte {
	result, _ := buildManualRangeMetricVectorContext(context.Background(), functionName, quantile, series, evalTime, window)
	return result
}

func buildManualRangeMetricVectorContext(ctx context.Context, functionName string, quantile float64, series map[string]manualSeriesSamples, evalTime time.Time, window time.Duration, maxSeries ...int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(maxSeries) > 0 && maxSeries[0] > 0 && len(series) > maxSeries[0] {
		return nil, fmt.Errorf("manual metric series limit exceeded (%d); narrow the query", maxSeries[0])
	}
	ctx = binaryEvaluationContext(ctx)
	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	windowStart := evalTime.Add(-window).UnixNano()
	windowEnd := evalTime.UnixNano()
	results := make(map[string]*binaryMatchedSeries)

	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		seriesEntry := series[key]
		value, ok := aggregateManualWindow(functionName, quantile, seriesEntry.Samples, windowStart, windowEnd, window.Seconds())
		if !ok {
			continue
		}
		if err := checkBinaryOutputSample(ctx); err != nil {
			return nil, err
		}
		if err := checkBinaryOutputLabels(ctx, seriesEntry.Metric); err != nil {
			return nil, err
		}
		results[key] = &binaryMatchedSeries{labels: seriesEntry.Metric, points: [][]any{{float64(evalTime.Unix()), strconv.FormatFloat(value, 'f', -1, 64)}}}
	}

	return encodeBinarySeriesContext(ctx, results, "vector", maxBufferedBackendBodyBytes)
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

func manualWindowValues(samples []rangeMetricSample, windowStart, windowEnd int64, excludeStart bool) []float64 {
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		if sample.ts < windowStart || sample.ts > windowEnd || (excludeStart && sample.ts == windowStart) {
			continue
		}
		values = append(values, sample.value)
	}
	return values
}

func aggregateManualWindow(functionName string, quantile float64, samples []rangeMetricSample, windowStart, windowEnd int64, windowSeconds float64) (float64, bool) {
	// Slice-dependent functions: build filtered slice, then aggregate.
	switch functionName {
	case "quantile", "stddev", "stdvar", "rate_counter":
		// Loki quantile range vectors exclude the lower boundary and include the end.
		values := manualWindowValues(samples, windowStart, windowEnd, true)
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
	// A LogQL range vector is (start, end] for EVERY function: Loki's
	// batchRangeVectorIterator.load skips `sample.Timestamp <= start` and the
	// iterator is shared by unwrapped ranges too (pkg/logql/range_vector.go).
	const excludeStart = true
	for _, sample := range samples {
		if sample.ts < windowStart || sample.ts > windowEnd || (excludeStart && sample.ts == windowStart) {
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
