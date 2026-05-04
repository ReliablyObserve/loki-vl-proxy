package proxy

import (
	"bytes"
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
)

type statsCompatSpec struct {
	BaseQuery  string
	GroupBy    []string
	ByExplicit bool // true when "by ()" was present — aggregate all into one series
	Func       string
	Field      string
}

type originalRangeMetricSpec struct {
	Func        string
	Window      time.Duration
	UnwrapField string
	UnwrapConv  string
	HasUnwrap   bool
}

type rangeMetricSample struct {
	ts    int64
	value float64
}

var (
	rangeMetricUnwrapRE = regexp.MustCompile(`(?s)\|\s*unwrap\s+([^|\[]+)`)
	outerAggregationRE  = regexp.MustCompile(`^(?:sum|avg|max|min|count(?:_values)?|stddev|stdvar|sort(?:_desc)?|topk|bottomk)\s*(?:(?:by|without)\s*\([^)]*\)\s*)?`)
	// outerWithoutRE detects "sum|avg|... without (" at the start of a LogQL expression.
	outerWithoutRE = regexp.MustCompile(`^(?:sum|avg|max|min|count(?:_values)?|stddev|stdvar)\s+without\s*\(`)
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

func (p *Proxy) handleStatsCompatRange(w http.ResponseWriter, r *http.Request, originalLogql, logsqlQuery string) bool {
	spec, ok := parseStatsCompatSpec(logsqlQuery)
	if !ok {
		return false
	}
	if !isManualRangeStatsFunc(spec.Func) {
		return false
	}
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
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, noSlidingOverlap, originalLogql) {
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
	origSpec, hasOrigSpec := parseOriginalRangeMetricSpec(originalLogql)

	manualFunc := normalizeManualMetricFunction(spec, origSpec)
	if manualFunc == "" {
		return false
	}
	// Instant queries have no step: the range window is the entire lookback interval,
	// not a sliding window. VL native stats correctly evaluates [time-range, time].
	// Only rate_counter still requires the manual path (counter-reset semantics).
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, true, originalLogql) {
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
//
// originalLogql is the original LogQL query string. When it contains "__error__"
// (e.g., via `| drop __error__`), the caller has explicitly handled parse errors,
// making VL native stats semantically correct even for count-like operations.
func shouldUseManualRangeMetricCompat(baseQuery, manualFunc string, rangeEqualsStep bool, originalLogql string) bool {
	manualFunc = strings.TrimSpace(manualFunc)
	if manualFunc == "rate_counter" {
		return true
	}

	// For sliding windows (range != step) VL native stats_query_range buckets by the
	// step interval (tumbling windows) while LogQL evaluates each point over [T-range, T].
	// When the data distribution is non-uniform the two diverge. Route to the manual
	// log-fetch path for correct sliding-window semantics regardless of parser stages.
	// When range == step windows are non-overlapping and native VL stats is equivalent.
	//
	// Exception: outer without() aggregation. The manual path uses collectRangeMetricSamples
	// with groupBy=["_stream","level"] (from addStatsByStreamClause). buildManualMetricLabels
	// looks up "_stream" in the already-expanded streamLabels map — it is absent there —
	// so only "level" survives. After applyWithoutGrouping removes "level" all series
	// collapse to {} → wrong series count. Native VL stats correctly handles _stream
	// expansion before applyWithoutGrouping, preserving detected_level differentiation.
	switch manualFunc {
	case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
		if !rangeEqualsStep && outerWithoutRE.MatchString(strings.TrimSpace(originalLogql)) {
			return false
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

	series, err := p.collectRangeMetricSamples(r.Context(), spec.BaseQuery, spec.GroupBy, spec.ByExplicit, field, origSpec.UnwrapConv, startTS.Add(-origSpec.Window), endTS)
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

	series, err := p.collectRangeMetricSamples(r.Context(), spec.BaseQuery, spec.GroupBy, spec.ByExplicit, field, origSpec.UnwrapConv, evalTS.Add(-origSpec.Window), evalTS)
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

func (p *Proxy) collectRangeMetricSamples(ctx context.Context, baseQuery string, groupBy []string, byExplicit bool, field, unwrapConv string, start, end time.Time) (map[string]manualSeriesSamples, error) {
	params := url.Values{}
	params.Set("query", baseQuery)
	params.Set("start", formatVLTimestamp(start.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(end.UTC().Format(time.RFC3339Nano)))
	// Keep this high to avoid truncating series for compatibility stats functions.
	params.Set("limit", "1000000")

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("backend returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	seriesMap := make(map[string]manualSeriesSamples)
	includeParsedLabels := queryUsesParserStages(baseQuery)
	lines := bytes.Split(body, []byte{'\n'})
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		rawTS, ok := stringifyEntryValue(entry["_time"])
		if !ok || rawTS == "" {
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

		value, ok := p.extractManualSampleValue(entry, field, unwrapConv)
		if !ok {
			continue
		}

		// parseStreamLabels returns a shared cached map — copy before mutating
		// to prevent concurrent map write panics under high concurrency.
		rawStreamLabels := parseStreamLabels(asString(entry["_stream"]))
		streamLabels := make(map[string]string, len(rawStreamLabels))
		for k, v := range rawStreamLabels {
			streamLabels[k] = v
		}
		if level := strings.TrimSpace(asString(entry["level"])); level != "" {
			streamLabels["level"] = level
			streamLabels["detected_level"] = level
		}
		if strings.TrimSpace(streamLabels["detected_level"]) == "" {
			streamLabels["detected_level"] = "unknown"
		}
		ensureSyntheticServiceName(streamLabels)
		metricLabels := buildManualMetricLabels(streamLabels, groupBy, byExplicit)
		if includeParsedLabels && len(groupBy) > 0 && !byExplicit {
			// Only inject parsed labels that appear in the explicit by(...) list.
			// Adding ALL parsed fields (old behaviour) creates one series per unique
			// JSON-object combination — O(N) series for N distinct log entries.
			// Loki groups by stream labels only when no by(...) is present; parsed
			// fields become metric dimensions only when explicitly named in by(...).
			addGroupByParsedLabels(metricLabels, entry, groupBy)
		}
		translated := p.labelTranslator.TranslateLabelsMap(metricLabels)
		key := seriesKeyFromMetric(translated)

		current := seriesMap[key]
		if current.Metric == nil {
			current.Metric = translated
		}
		current.Samples = append(current.Samples, rangeMetricSample{ts: ts, value: value})
		seriesMap[key] = current
	}

	for key, series := range seriesMap {
		sort.Slice(series.Samples, func(i, j int) bool { return series.Samples[i].ts < series.Samples[j].ts })
		seriesMap[key] = series
	}

	return seriesMap, nil
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

	labels := make(map[string]string, len(groupBy))
	for _, key := range groupBy {
		if value, ok := streamLabels[key]; ok {
			labels[key] = value
		}
	}
	return labels
}

func (p *Proxy) extractManualSampleValue(entry map[string]interface{}, field, unwrapConv string) (float64, bool) {
	switch field {
	case "__count__":
		return 1, true
	case "__bytes__":
		return float64(len(asString(entry["_msg"]))), true
	}

	raw, ok := lookupEntryField(entry, p.manualValueCandidateFields(field))
	if !ok {
		return 0, false
	}

	switch unwrapConv {
	case "duration":
		value, ok := parseDuration(asString(raw))
		return value, ok
	case "bytes":
		value, ok := parseBytes(asString(raw))
		return value, ok
	default:
		value, ok := parseFloatValue(raw)
		return value, ok
	}
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

func lookupEntryField(entry map[string]interface{}, keys []string) (interface{}, bool) {
	for _, key := range keys {
		if raw, ok := entry[key]; ok {
			return raw, true
		}
	}
	return nil, false
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
	case "count_over_time":
		return float64(len(values)), true
	case "rate":
		if windowSeconds <= 0 {
			return 0, false
		}
		return float64(len(values)) / windowSeconds, true
	case "bytes_over_time", "sum":
		return sumFloat64(values), true
	case "bytes_rate":
		if windowSeconds <= 0 {
			return 0, false
		}
		return sumFloat64(values) / windowSeconds, true
	case "avg":
		return sumFloat64(values) / float64(len(values)), true
	case "min":
		return minFloat64(values), true
	case "max":
		return maxFloat64(values), true
	case "stddev":
		return stddevFloat64(values), true
	case "stdvar":
		v := stddevFloat64(values)
		return v * v, true
	case "quantile":
		return quantileFloat64(values, quantile), true
	case "first":
		return values[0], true
	case "last":
		return values[len(values)-1], true
	case "rate_counter":
		if windowSeconds <= 0 {
			return 0, false
		}
		if len(values) == 1 {
			return 0, true
		}
		return rateCounterWindow(values, windowSeconds), true
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

func minFloat64(values []float64) float64 {
	out := values[0]
	for _, value := range values[1:] {
		if value < out {
			out = value
		}
	}
	return out
}

func maxFloat64(values []float64) float64 {
	out := values[0]
	for _, value := range values[1:] {
		if value > out {
			out = value
		}
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
