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
	"unicode/utf8"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/grafana/jsonparser"
)

// defaultOrderedJSONMetricMaxBytes admits about one million raw rows (the
// -manual-range-metric-row-limit default) of roughly 1 KiB each: about a day
// of 50k lines per hour. The rows are streamed, so it bounds transfer and
// VictoriaLogs work; retained memory is bounded by the row limit.
const defaultOrderedJSONMetricMaxBytes = 1 << 30

// orderedJSONMetricMaxBytes returns -ordered-json-metric-max-bytes, the cap on
// the raw rows response read and the response built by the raw evaluator.
func (p *Proxy) orderedJSONMetricMaxBytes() int64 {
	if p.orderedJSONMaxBytes > 0 {
		return p.orderedJSONMaxBytes
	}
	return defaultOrderedJSONMetricMaxBytes
}

type orderedJSONStage struct {
	parser bool
	logfmt bool // parser is `| logfmt`; such plans only use the stats pushdown
	filter *translator.DropCondition
	line   func(string) bool
	fields map[string]bool
	match  []translator.DropCondition
	keep   bool
}

type orderedJSONMetricPlan struct {
	selector      string
	fetchQuery    string
	function      string
	window        time.Duration
	stages        []orderedJSONStage
	grouping      *logqlpkg.Grouping
	aggregated    bool
	reducesLabels bool
	noLabels      bool
	preserveError bool
	required      map[string]bool
	earlyFilters  []translator.DropCondition
	withoutJSON   string
	// unpackFields lists the keys a VictoriaLogs stats pushdown groups by and
	// unpacks (see setStatsPushdown); empty when the pushdown is not exact.
	unpackFields []string
	// pushdownFilters are the string label filters that follow the parser in
	// a pushdown plan; VictoriaLogs applies them after unpacking.
	pushdownFilters []translator.DropCondition
	// parser is "json", "logfmt" or "" (no parser). Only JSON plans have a raw
	// evaluator; the others answer through the stats pushdown or fall through.
	parser string
}

// handleOrderedJSONMetric uses the existing metric accumulators, but executes
// parser-dependent stages before reducing rows to samples. VictoriaLogs does not
// attach Loki's parser error labels, so pushing those filters into VL loses rows.
func (p *Proxy) handleOrderedJSONMetric(w http.ResponseWriter, r *http.Request, requestStart time.Time, query string, isRange bool) bool {
	plan, ok := compileOrderedJSONMetric(query)
	if !ok {
		return false
	}
	if plan.parser != "json" {
		// Drilldown keeps its dedicated coalescing and residual-chunk routes.
		return isRange && !isGrafanaDrilldownRequest(r) && p.serveLevelVolumeStatsBuckets(w, r, requestStart, query, plan)
	}
	if isRange && plan.isNativeDrilldownHistogram(r) && p.orderedJSONNativeGroupingIsExact(plan) {
		return false
	}
	if plan.withoutJSON != "" && p.orderedJSONNativeGroupingIsExact(plan) {
		// The compiled plan proves that JSON and label-only stages cannot
		// affect this aggregation. Retain line filters and reuse the
		// native metric path, avoiding a raw-log scan for broad summed rates.
		child := cloneMetricQueryRequest(r, plan.withoutJSON)
		if isRange {
			p.handleQueryRange(w, child)
		} else {
			p.handleQuery(w, child)
		}
		return true
	}
	endpoint := "query"
	if isRange {
		endpoint = "query_range"
	}
	start, end, step, err := orderedJSONMetricTimes(r, isRange)
	var body []byte
	served := false
	if err == nil && isRange {
		body, served, err = p.orderedJSONStatsBuckets(r.Context(), plan, start, end, step)
	}
	if err == nil && !served {
		var series map[string]manualSeriesSamples
		series, err = p.collectOrderedJSONMetric(r.Context(), plan, start, end, step)
		if err == nil {
			body, err = buildOrderedJSONMetric(r.Context(), plan, series, start, end, step, isRange, p.orderedJSONMetricMaxBytes())
		}
	}
	status := http.StatusOK
	if err != nil {
		status = statusFromUpstreamErr(err)
		var pipelineErr *orderedJSONPipelineError
		if errors.As(err, &pipelineErr) {
			status = http.StatusBadRequest
		}
		p.writeError(w, status, err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
	p.metrics.RecordRequest(endpoint, status, time.Since(requestStart))
	p.queryTracker.Record(endpoint, query, time.Since(requestStart), status >= 400)
	return true
}

func (p *Proxy) orderedJSONNativeGroupingIsExact(plan *orderedJSONMetricPlan) bool {
	if plan.grouping == nil || p.labelTranslator == nil {
		return true
	}
	// An alias selector may match either stored spelling, while VL group-by
	// reads only one column. Keep local grouping unless the spelling is exact.
	for _, label := range plan.grouping.Labels {
		if p.labelTranslator.ToVL(label) != label {
			return false
		}
	}
	return true
}

// Drilldown's field histogram intentionally returns the most frequent bounded
// set of values. Preserve the existing native stats/hits route for that precise
// UI query; ordinary exact metrics still use the fail-closed local evaluator.
func (plan *orderedJSONMetricPlan) isNativeDrilldownHistogram(r *http.Request) bool {
	if !isGrafanaDrilldownRequest(r) || !plan.aggregated || plan.function != "count_over_time" ||
		plan.grouping == nil || plan.grouping.Without || len(plan.grouping.Labels) != 1 {
		return false
	}
	step, ok := parsePositiveStepDuration(r.FormValue("step"))
	if !ok || step != plan.window {
		return false
	}
	field := plan.grouping.Labels[0]
	if strings.HasPrefix(field, "__") {
		return false
	}
	errorCleared, existsFilter, parsed := false, false, false
	for _, stage := range plan.stages {
		switch {
		case stage.parser:
			if parsed {
				return false
			}
			parsed = true
			errorCleared = false
		case stage.line != nil:
			continue
		case stage.filter != nil:
			if !parsed || !errorCleared || stage.filter.Field != field || stage.filter.Op != "!=" || stage.filter.Value != "" {
				return false
			}
			existsFilter = true
		case stage.keep || len(stage.match) != 0:
			return false
		default:
			for name := range stage.fields {
				if name != "__error__" && name != "__error_details__" {
					return false
				}
			}
			errorCleared = errorCleared || stage.fields["__error__"]
		}
	}
	return parsed && errorCleared && existsFilter
}

func orderedJSONMetricTimes(r *http.Request, isRange bool) (time.Time, time.Time, time.Duration, error) {
	if !isRange {
		stamp := time.Now()
		if raw := r.FormValue("time"); raw != "" {
			ns, ok := parseFlexibleUnixNanos(raw)
			if !ok {
				return time.Time{}, time.Time{}, 0, &orderedJSONPipelineError{"invalid time timestamp"}
			}
			stamp = time.Unix(0, ns)
		}
		return stamp, stamp, time.Second, nil
	}
	start, startOK := parseFlexibleUnixNanos(r.FormValue("start"))
	end, endOK := parseFlexibleUnixNanos(r.FormValue("end"))
	step, stepOK := parseStepToNanos(r.FormValue("step"))
	if !startOK || !endOK || !stepOK {
		return time.Time{}, time.Time{}, 0, &orderedJSONPipelineError{"invalid range timestamps or step"}
	}
	if _, err := metricEvalPointCount(time.Unix(0, start), time.Unix(0, end), time.Duration(step)); err != nil {
		return time.Time{}, time.Time{}, 0, &orderedJSONPipelineError{errLokiStepTooSmall}
	}
	return time.Unix(0, start), time.Unix(0, end), time.Duration(step), nil
}

func compileOrderedJSONMetric(query string) (*orderedJSONMetricPlan, bool) {
	if !strings.Contains(query, "json") && !strings.Contains(query, "detected_level") {
		return nil, false
	}
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return nil, false
	}
	plan := &orderedJSONMetricPlan{}
	if outer, ok := expr.(*logqlpkg.VectorAggregation); ok {
		if outer.Op != logqlpkg.VectorSum {
			return nil, false
		}
		plan.aggregated, plan.grouping = true, outer.Grouping
		expr = outer.Inner
	}
	rangeExpr, ok := expr.(*logqlpkg.RangeAggregation)
	if !ok || rangeExpr.Offset != "" || rangeExpr.Grouping != nil {
		return nil, false
	}
	switch rangeExpr.Op {
	case logqlpkg.RangeRate, logqlpkg.RangeCountOverTime, logqlpkg.RangeBytesRate, logqlpkg.RangeBytesOverTime:
		plan.function = string(rangeExpr.Op)
	default:
		return nil, false
	}
	plan.window, ok = parsePositiveStepDuration(rangeExpr.Range)
	if !ok {
		return nil, false
	}
	logExpr, ok := rangeExpr.Inner.(*logqlpkg.LogQuery)
	if !ok {
		return nil, false
	}
	// Re-quote selector values rather than using the AST's display rendering.
	matchers := make([]string, 0, len(logExpr.Selector.Matchers))
	for _, m := range logExpr.Selector.Matchers {
		op := []string{"=", "!=", "=~", "!~"}[int(m.Op)]
		matchers = append(matchers, m.Name+op+strconv.Quote(m.Value))
	}
	plan.selector = "{" + strings.Join(matchers, ",") + "}"
	hasJSON, hasLogfmt := false, false
	var retainedLines strings.Builder
	for _, stage := range logExpr.Pipeline {
		compiled, valid := compileOrderedJSONStage(stage)
		if !valid {
			return nil, false
		}
		hasJSON = hasJSON || (compiled.parser && !compiled.logfmt)
		hasLogfmt = hasLogfmt || compiled.logfmt
		plan.reducesLabels = plan.reducesLabels || compiled.fields != nil
		plan.stages = append(plan.stages, compiled)
		if _, ok := stage.(*logqlpkg.LineFilterStage); ok {
			retainedLines.WriteByte(' ')
			retainedLines.WriteString(stage.String())
		}
	}
	if hasJSON && hasLogfmt {
		return nil, false
	}
	if !hasJSON {
		return plan.levelVolumePlan(hasLogfmt, retainedLines.String())
	}
	plan.parser = "json"
	plan.setParserHints()
	// None of the supported stages modifies the log line, so line filters
	// commute with parsing/label mutations. Push them into VL to avoid scanning
	// discarded lines while retaining their original position in local execution.
	plan.fetchQuery = plan.selector + retainedLines.String()
	if plan.noLabels || plan.canElideJSONWithDroppedErrors(logExpr.Selector.Matchers) {
		grouping := ""
		if plan.grouping != nil {
			grouping = plan.grouping.String()
		}
		outer := "sum"
		if grouping != "" {
			outer += " " + grouping
		}
		plan.withoutJSON = outer + "(" + plan.function + "(" + plan.selector + retainedLines.String() + "[" + rangeExpr.Range + "]))"
	}
	plan.setStatsPushdown()
	return plan, true
}

// levelVolumePlan completes a plan without a JSON parser. Grafana's plain and
// `| logfmt` logs volume shapes have no raw evaluator here: they compile only
// for the stats pushdown, which must group by the detected_level alias.
func (plan *orderedJSONMetricPlan) levelVolumePlan(logfmt bool, lineFilters string) (*orderedJSONMetricPlan, bool) {
	if logfmt {
		plan.parser = "logfmt"
	}
	plan.fetchQuery = plan.selector + lineFilters
	plan.setStatsPushdown()
	if plan.grouping == nil || !containsString(plan.grouping.Labels, "detected_level") || len(plan.unpackFields) == 0 {
		return nil, false
	}
	return plan, true
}

// Grouping by guaranteed stream labels cannot observe JSON-extracted labels:
// Loki suffixes collisions with _extracted. If every possible parser error is
// subsequently dropped, parsing cannot affect this sum's value or label set.
func (plan *orderedJSONMetricPlan) canElideJSONWithDroppedErrors(matchers []logqlpkg.LabelMatcher) bool {
	if !plan.aggregated || plan.grouping == nil || plan.grouping.Without {
		return false
	}
	for _, label := range plan.grouping.Labels {
		if label == "__error__" || label == "__error_details__" {
			return false
		}
		guaranteed := false
		for _, matcher := range matchers {
			guaranteed = guaranteed || (matcher.Name == label && matcher.Op == logqlpkg.MatchEq && matcher.Value != "")
		}
		if !guaranteed {
			return false
		}
	}
	return plan.errorsDroppedAfterParser()
}

// errorsDroppedAfterParser reports whether the last parser's errors are
// dropped and every other stage is a line filter or a drop of the parser error
// labels, so each selected line contributes exactly once whether or not its
// JSON parses.
func (plan *orderedJSONMetricPlan) errorsDroppedAfterParser() bool {
	errorCleared := false
	for _, stage := range plan.stages {
		switch {
		case stage.parser:
			errorCleared = false
		case stage.line != nil:
			continue
		case stage.filter != nil || stage.keep || len(stage.match) != 0:
			return false
		default:
			for field := range stage.fields {
				if field != "__error__" && field != "__error_details__" {
					return false
				}
			}
			errorCleared = errorCleared || stage.fields["__error__"]
		}
	}
	return errorCleared
}

// orderedJSONUnpackLabelRE matches labels that Loki's JSON parser yields only
// from a key of the same spelling: nesting and sanitized characters introduce
// an underscore. Loki also skips empty parent keys; when unpack_json yields no
// value for such a line, the partial-parse check below finds the quoted key.
var orderedJSONUnpackLabelRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)

// setStatsPushdown enables VictoriaLogs stats buckets for summed log metrics
// whose parser errors are dropped: every selected line then counts once, and
// only the grouped labels depend on the JSON body. Loki suffixes a parsed key
// that collides with a stream label or structured metadata with _extracted,
// which unpack_json keep_original_fields reproduces by keeping stored values.
// detected_level is structured metadata Loki sets at ingest, so JSON never
// yields it; it is derived from level like the other metric paths.
func (plan *orderedJSONMetricPlan) setStatsPushdown() {
	if !plan.aggregated || plan.grouping == nil || plan.grouping.Without || len(plan.grouping.Labels) == 0 {
		return
	}
	filters, ok := plan.pushdownLabelFilters()
	if !ok {
		return
	}
	var fields []string
	for _, label := range plan.grouping.Labels {
		if label == "detected_level" {
			label = "level"
		} else if !orderedJSONUnpackLabelRE.MatchString(label) {
			return
		}
		if !containsString(fields, label) {
			fields = append(fields, label)
		}
	}
	plan.unpackFields = fields
	plan.pushdownFilters = filters
}

// pushdownLabelFilters reports whether the pipeline fits the stats pushdown
// and returns its label filters. Like errorsDroppedAfterParser, the last
// parser's errors must be dropped and every other stage must be a line filter
// or a drop of the error labels; string label filters (=, !=, =~, !~) on plain
// keys after the parser are also accepted. A filter only removes lines, so
// every remaining line still counts once, and a label missing from a line
// compares as the empty string on both sides.
func (plan *orderedJSONMetricPlan) pushdownLabelFilters() ([]translator.DropCondition, bool) {
	var filters []translator.DropCondition
	parsed, errorCleared := false, false
	for _, stage := range plan.stages {
		switch {
		case stage.parser:
			if parsed {
				return nil, false
			}
			parsed, errorCleared = true, false
		case stage.line != nil:
			continue
		case stage.filter != nil:
			if !parsed || !orderedJSONUnpackLabelRE.MatchString(stage.filter.Field) {
				return nil, false
			}
			filters = append(filters, *stage.filter)
		case stage.keep || len(stage.match) != 0:
			return nil, false
		default:
			for field := range stage.fields {
				if field != "__error__" && field != "__error_details__" {
					return nil, false
				}
			}
			errorCleared = errorCleared || stage.fields["__error__"]
		}
	}
	return filters, errorCleared
}

// logsQLLabelFilter renders a Loki string label filter as a LogsQL filter pipe
// with the same semantics: exact comparison, and a fully anchored RE2 regexp.
func logsQLLabelFilter(filter translator.DropCondition) string {
	field := quoteLogsQLIdent(filter.Field)
	switch filter.Op {
	case "=":
		return " | filter " + field + ":=" + logsql.QuoteValue(filter.Value)
	case "!=":
		return " | filter -" + field + ":=" + logsql.QuoteValue(filter.Value)
	case "=~":
		return " | filter " + field + ":~" + logsql.QuotePattern("^(?:"+filter.Value+")$")
	default:
		return " | filter -" + field + ":~" + logsql.QuotePattern("^(?:"+filter.Value+")$")
	}
}

// lokiNormalizedLevel mirrors the level normalization Loki applies when it
// sets detected_level at ingest; an empty level is unknown.
func lokiNormalizedLevel(level string) string {
	switch strings.ToLower(level) {
	case "":
		return "unknown"
	case "trace", "trc":
		return "trace"
	case "debug", "dbg":
		return "debug"
	case "info", "inf", "information":
		return "info"
	case "warn", "wrn", "warning":
		return "warn"
	case "error", "err":
		return "error"
	case "critical":
		return "critical"
	case "fatal":
		return "fatal"
	}
	return level
}

// orderedJSONStatsBuckets serves an eligible plan (setStatsPushdown) from
// stats_query_range buckets on the anchored sliding grid instead of raw rows.
// served is false when the raw evaluator must answer: no bucket grid, a label
// VictoriaLogs spells differently, a possible series-limit overflow, or lines
// whose JSON VictoriaLogs rejects while Loki may still extract a grouped label.
func (p *Proxy) orderedJSONStatsBuckets(ctx context.Context, plan *orderedJSONMetricPlan, start, end time.Time, step time.Duration) (body []byte, served bool, err error) {
	if len(plan.unpackFields) == 0 || p.labelTranslator == nil {
		return nil, false, nil
	}
	unpack := append([]string(nil), plan.unpackFields...)
	for _, filter := range plan.pushdownFilters {
		if !containsString(unpack, filter.Field) {
			unpack = append(unpack, filter.Field)
		}
	}
	for _, field := range unpack {
		if p.labelTranslator.ToVL(field) != field {
			return nil, false, nil
		}
	}
	bucket, ok := p.slidingStatsBucket(start, step, plan.window)
	if !ok {
		return nil, false, nil
	}
	base, err := p.translateQueryWithContext(ctx, plan.fetchQuery)
	if err != nil {
		return nil, false, err
	}
	windowStart := start.Add(-plan.window)
	// The parse-risk check and the bucket query run concurrently; the buckets
	// are discarded when the check finds a line the parsers read differently.
	riskCtx, cancelRisk := context.WithCancel(ctx)
	defer cancelRisk()
	type riskResult struct {
		risky bool
		err   error
	}
	riskDone := make(chan riskResult, 1)
	go func() {
		risky, err := p.cachedStatsPushdownRisk(riskCtx, plan.parser, base, unpack, windowStart, end)
		riskDone <- riskResult{risky, err}
	}()
	statsCtx, cancelStats := context.WithCancel(ctx)
	defer cancelStats()
	groupBy := plan.unpackFields
	if containsString(plan.grouping.Labels, "detected_level") {
		groupBy = append(append([]string(nil), groupBy...), "detected_level")
	}
	statsAggFunc := "count() as c"
	if plan.function == "bytes_rate" || plan.function == "bytes_over_time" {
		statsAggFunc = "sum_len(_msg) as c, count() as __sample_count"
	}
	query := base
	if plan.parser != "" {
		query += " | unpack_" + plan.parser + " fields (" + strings.Join(unpack, ", ") + ") keep_original_fields"
	}
	for _, filter := range plan.pushdownFilters {
		query += logsQLLabelFilter(filter)
	}
	type statsResult struct {
		series map[string]manualSeriesSamples
		err    error
	}
	statsDone := make(chan statsResult, 1)
	go func() {
		series, err := p.collectRangeMetricHits(statsCtx, query, groupBy, groupBy, false, statsAggFunc, windowStart, end, bucket)
		statsDone <- statsResult{series, err}
	}()
	risk := <-riskDone
	if risk.err != nil || risk.risky {
		cancelStats()
		return nil, false, risk.err
	}
	stats := <-statsDone
	series, err := stats.series, stats.err
	if err != nil {
		return nil, false, err
	}
	if maxSeries := p.resolvedMaxStatsQuerySeries(); len(series) >= maxSeries {
		// The bucket collector keeps the busiest series; Loki fails instead.
		return nil, false, fmt.Errorf("maximum metric series exceeded (%d)", maxSeries)
	}
	merged := make(map[string]manualSeriesSamples, len(series))
	for _, entry := range series {
		metric := make(map[string]string, len(plan.grouping.Labels))
		for _, label := range plan.grouping.Labels {
			value := entry.Metric[label]
			if label == "detected_level" {
				if value == "" {
					value = entry.Metric["level"]
				}
				value = lokiNormalizedLevel(value)
			}
			if value != "" {
				metric[label] = value
			}
		}
		key := canonicalLabelsKey(metric)
		target := merged[key]
		target.Metric = metric
		target.Samples = append(target.Samples, entry.Samples...)
		if entry.PresentBuckets != nil {
			present := append(target.PresentBuckets, entry.PresentBuckets...)
			sort.Slice(present, func(i, j int) bool { return present[i] < present[j] })
			target.PresentBuckets = present
		}
		merged[key] = target
	}
	body, err = buildHitsRangeMetricMatrix(plan.function, merged, start, end, step, plan.window)
	return body, err == nil, err
}

// serveLevelVolumeStatsBuckets answers a plain or `| logfmt` logs volume plan
// from stats buckets. It returns false, leaving the request to the other
// routes, when the pushdown cannot answer exactly.
func (p *Proxy) serveLevelVolumeStatsBuckets(w http.ResponseWriter, r *http.Request, requestStart time.Time, query string, plan *orderedJSONMetricPlan) bool {
	start, end, step, err := orderedJSONMetricTimes(r, true)
	if err != nil {
		return false
	}
	body, served, err := p.orderedJSONStatsBuckets(r.Context(), plan, start, end, step)
	if err == nil && !served {
		return false
	}
	status := http.StatusOK
	if err != nil {
		status = statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
	p.metrics.RecordRequest("query_range", status, time.Since(requestStart))
	p.queryTracker.Record("query_range", query, time.Since(requestStart), status >= 400)
	return true
}

// logfmtParseRisk reports whether a selected line lacks a grouped label as a
// stored field and holds `key=` in a shape where Loki's logfmt decoder and
// VictoriaLogs unpack_logfmt may disagree. They agree on lines made of tokens
// `key`, `key=value` or `key="value"` separated by single spaces, where values
// hold no quote, equals sign, backslash or control byte, the key appears once
// and its value is printable ASCII. Loki also splits on tabs, recovers from
// malformed tokens and keeps the first duplicate, while VictoriaLogs splits on
// spaces only and stops after a malformed quoted value.
func (p *Proxy) logfmtParseRisk(ctx context.Context, base string, fields []string, start, end time.Time) (bool, error) {
	conditions := make([]string, len(fields))
	for i, field := range fields {
		re := newLogfmtRiskPatterns(field)
		conditions[i] = "(-" + field + ":* _msg:~" + strconv.Quote(re.key) + " (-_msg:~" + strconv.Quote(re.wellFormed) +
			" or _msg:~" + strconv.Quote(re.repeated) + " or (_msg:~" + strconv.Quote(re.keyAssignment) + " -_msg:~" + strconv.Quote(re.safeValue) + ")))"
	}
	return p.statsPushdownRiskExists(ctx, base+" | filter "+strings.Join(conditions, " or ")+" | limit 1", start, end)
}

// logfmtRiskPatterns holds the regular expressions of logfmtParseRisk for one
// key. A line is a risk when it matches key and either does not match
// wellFormed, matches repeated, or matches keyAssignment without safeValue.
type logfmtRiskPatterns struct {
	key, wellFormed, repeated, keyAssignment, safeValue string
}

func newLogfmtRiskPatterns(field string) logfmtRiskPatterns {
	// A value starting with ' or a backtick is unquoted by unpack_logfmt only.
	const pairPattern = `[^\x00-\x20="]+(?:=(?:(?:[^\x00-\x20="'\x60][^\x00-\x20="]*)?|"[^"\\\x00-\x1f]*"))?`
	name := regexp.QuoteMeta(field)
	return logfmtRiskPatterns{
		key:           name + "=",
		wellFormed:    `^` + pairPattern + `(?: ` + pairPattern + `)*$`,
		repeated:      `(?:^| )` + name + `(?:=[^ ]*)?(?: .*)? ` + name + `(?:[= ]|$)`,
		keyAssignment: `(?:^| )` + name + `=`,
		safeValue:     `(?:^| )` + name + `=(?:(?:[!#-&(-<>-_a-~][!#-<>-~]*)?|"[ !#-\[\]-~]*")(?: |$)`,
	}
}

// orderedJSONPartialParseRisk reports whether a selected line lacks a grouped
// label both as a stored field and after unpack_json, yet Loki's JSON parser
// could still yield it. VictoriaLogs adds no field unless the whole line is
// one valid JSON object starting at its first byte, while Loki skips leading
// whitespace, ignores trailing bytes and keeps keys parsed before a syntax
// error. Such a line starts with a brace and holds the quoted key. Loki also
// trims spaces around keys, keeps escaped keys raw, skips arrays, keeps the
// value it extracts first for a repeated key and replaces U+FFFD, where
// unpack_json does not. One matching line keeps the exact raw evaluator.
func (p *Proxy) orderedJSONPartialParseRisk(ctx context.Context, base string, fields []string, start, end time.Time) (bool, error) {
	// escapedKey: unpack_json unescapes keys, Loki keeps them raw. replaced:
	// Loki turns U+FFFD in string values into a space.
	const escapedKey, replaced = `\\u[0-9A-Fa-f]{4}[^"]*"\s*:`, `\x{FFFD}|\\u[Ff]{3}[Dd]`
	absent := make([]string, len(fields))
	empty := make([]string, len(fields))
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = regexp.QuoteMeta(field)
		absent[i] = "-" + field + ":*"
		// A field is at risk only on a line whose body has its own key:
		// a line with another unpacked key but not this one is parsed alike.
		empty[i] = "(" + field + `:="" _msg:~` + strconv.Quote(`"\s*`+names[i]+`\s*"\s*:`) + ")"
	}
	// Loki trims spaces around a key before sanitizing it.
	key := `"\s*(?:` + strings.Join(names, "|") + `)\s*"\s*:`
	pattern := `^\s*\{(?s:.*)(?:` + key + `|` + escapedKey + `|` + replaced + `)`
	for _, name := range names {
		field := `"\s*` + name + `\s*"\s*:`
		// A repeated key: Loki keeps one value, unpack_json adds a column per
		// copy. An array: Loki skips it, unpack_json stores it as a string.
		empty = append(empty, "_msg:~"+strconv.Quote(field+`(?s:.*)`+field), "_msg:~"+strconv.Quote(field+`\s*\[`))
	}
	empty = append(empty, "_msg:~"+strconv.Quote(escapedKey), "_msg:~"+strconv.Quote(replaced))
	query := base + " | filter (" + strings.Join(absent, " or ") + ") _msg:~" + strconv.Quote(pattern) +
		" | unpack_json fields (" + strings.Join(fields, ", ") + ") keep_original_fields | filter " + strings.Join(empty, " or ") + " | limit 1"
	return p.statsPushdownRiskExists(ctx, query, start, end)
}

// statsPushdownRiskCacheTTL bounds how long a risk-free window is remembered.
// Coverage never extends past statsPushdownRiskSettle before now, so lines that
// arrive late for recent timestamps are still checked on the next request.
const (
	statsPushdownRiskCacheTTL = 5 * time.Minute
	statsPushdownRiskSettle   = 5 * time.Minute
)

// cachedStatsPushdownRisk answers the parse-risk check for [start, end],
// remembering the window already found free of risky lines per tenant, query
// and field set, so a refresh or a wider range checks only the uncovered part.
// A window with a risky line is never cached.
func (p *Proxy) cachedStatsPushdownRisk(ctx context.Context, parser, base string, fields []string, start, end time.Time) (bool, error) {
	check := func(from, to time.Time) (bool, error) {
		switch parser {
		case "json":
			return p.orderedJSONPartialParseRisk(ctx, base, fields, from, to)
		case "logfmt":
			return p.logfmtParseRisk(ctx, base, fields, from, to)
		}
		return false, nil
	}
	if p.cache == nil {
		return check(start, end)
	}
	key := "stats-pushdown-risk:v1:" + getOrgID(ctx) + ":" + parser + ":" + strings.Join(fields, ",") + ":" + base
	type window struct{ from, to time.Time }
	todo := []window{{start, end}}
	coveredFrom, coveredTo := start, end
	if raw, _, ok := p.cache.GetWithTTL(key); ok {
		var fromNs, toNs int64
		if _, err := fmt.Sscanf(string(raw), "%d %d", &fromNs, &toNs); err == nil {
			cachedFrom, cachedTo := time.Unix(0, fromNs), time.Unix(0, toNs)
			if !start.After(cachedTo) && !end.Before(cachedFrom) {
				todo = todo[:0]
				if start.Before(cachedFrom) {
					todo = append(todo, window{start, cachedFrom})
				} else {
					coveredFrom = cachedFrom
				}
				if end.After(cachedTo) {
					todo = append(todo, window{cachedTo, end})
				} else {
					coveredTo = cachedTo
				}
			}
		}
	}
	for _, w := range todo {
		risky, err := check(w.from, w.to)
		if err != nil || risky {
			return risky, err
		}
	}
	if settled := time.Now().Add(-statsPushdownRiskSettle); coveredTo.After(settled) {
		coveredTo = settled
	}
	if coveredTo.After(coveredFrom) {
		p.cache.SetLocalOnlyWithTTL(key, []byte(fmt.Sprintf("%d %d", coveredFrom.UnixNano(), coveredTo.UnixNano())), statsPushdownRiskCacheTTL)
	}
	return false, nil
}

// statsPushdownRiskExists runs a `| limit 1` risk query over [start, end] and
// reports whether it matched a line.
func (p *Proxy) statsPushdownRiskExists(ctx context.Context, query string, start, end time.Time) (bool, error) {
	params := url.Values{"query": {query}, "start": {start.UTC().Format(time.RFC3339Nano)}, "end": {end.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano)}}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return false, p.redactedBackendStatusError("backend returned", resp.StatusCode, body)
	}
	head, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(head)) != "", nil
}

// cloneMetricQueryRequest retains the admitted request's immutable tenant and
// authentication context while replacing the expression in every parsed view.
// Parent and child URL/form maps must not alias during recursive evaluation.
func cloneMetricQueryRequest(r *http.Request, query string) *http.Request {
	child := r.Clone(r.Context())
	_ = child.ParseForm()
	child.Form = cloneURLValues(child.Form)
	child.PostForm = cloneURLValues(child.PostForm)
	child.Form.Set("query", query)
	if child.Method == http.MethodPost {
		child.PostForm.Set("query", query)
	}
	params := child.URL.Query()
	params.Set("query", query)
	child.URL.RawQuery = params.Encode()
	return child
}

func compileOrderedJSONStage(stage logqlpkg.Stage) (orderedJSONStage, bool) {
	var out orderedJSONStage
	switch s := stage.(type) {
	case *logqlpkg.ParserStage:
		out.logfmt = s.Type == logqlpkg.ParserLogfmt && s.Param == ""
		out.parser = out.logfmt || (s.Type == logqlpkg.ParserJSON && s.Param == "")
		return out, out.parser
	case *logqlpkg.LabelFilterStage:
		parsed, err := logqlpkg.ParseLogQuery("{" + s.Raw + "}")
		if err != nil || len(parsed.Selector.Matchers) != 1 {
			return out, false
		}
		m := parsed.Selector.Matchers[0]
		op := []string{"=", "!=", "=~", "!~"}[int(m.Op)]
		condition, err := translator.NewDropCondition(m.Name, op, m.Value)
		out.filter = &condition
		return out, err == nil
	case *logqlpkg.LineFilterStage:
		var err error
		out.line, err = orderedJSONLineFilter(s)
		return out, err == nil
	case *logqlpkg.DropStage:
		return compileOrderedJSONFields(s.Labels, s.Matchers, false)
	case *logqlpkg.KeepStage:
		return compileOrderedJSONFields(s.Labels, s.Matchers, true)
	default:
		return out, false
	}
}

func orderedJSONLineFilter(stage *logqlpkg.LineFilterStage) (func(string) bool, error) {
	if stage.IP {
		return nil, fmt.Errorf("IP filter requires the native pipeline path")
	}
	switch stage.Op {
	case logqlpkg.LineFilterContains:
		return func(line string) bool { return strings.Contains(line, stage.Value) }, nil
	case logqlpkg.LineFilterExcludes:
		return func(line string) bool { return !strings.Contains(line, stage.Value) }, nil
	case logqlpkg.LineFilterMatchRe, logqlpkg.LineFilterExcludeRe:
		re, err := regexp.Compile("(?s)" + stage.Value)
		if err != nil {
			return nil, err
		}
		return func(line string) bool { return re.MatchString(line) == (stage.Op == logqlpkg.LineFilterMatchRe) }, nil
	default:
		return nil, fmt.Errorf("unsupported line filter")
	}
}

func compileOrderedJSONFields(labels []string, matchers []logqlpkg.DropMatcher, keep bool) (orderedJSONStage, bool) {
	out := orderedJSONStage{fields: make(map[string]bool), keep: keep}
	for _, label := range labels {
		out.fields[label] = true
	}
	for _, m := range matchers {
		condition, err := translator.NewDropCondition(m.Name, m.Op, m.Value)
		if err != nil {
			return out, false
		}
		out.match = append(out.match, condition)
	}
	return out, true
}

func (plan *orderedJSONMetricPlan) setParserHints() {
	// Match Loki's count/bytes sample extractor hints: without grouping cannot
	// determine required labels, while sum without labels may skip parsing.
	if !plan.aggregated || (plan.grouping != nil && plan.grouping.Without) {
		// Loki retains simple label-filter parser hints when all labels are
		// needed. These can reject a row during extraction, before later drops.
		for _, stage := range plan.stages {
			if stage.filter != nil {
				plan.earlyFilters = append(plan.earlyFilters, *stage.filter)
			}
		}
		return
	}
	plan.required = make(map[string]bool)
	if plan.grouping != nil {
		for _, label := range plan.grouping.Labels {
			plan.required[label] = true
		}
	}
	for _, stage := range plan.stages {
		if stage.filter != nil {
			plan.required[stage.filter.Field] = true
		}
	}
	plan.noLabels = len(plan.required) == 0
	plan.preserveError = plan.required["__error__"]
	originalRequired := make([]string, 0, len(plan.required))
	for label := range plan.required {
		originalRequired = append(originalRequired, label)
	}
	for _, label := range originalRequired {
		if strings.HasSuffix(label, "_extracted") {
			plan.required[strings.TrimSuffix(label, "_extracted")] = true
		}
	}
}

type orderedJSONPipelineError struct{ message string }

func (e *orderedJSONPipelineError) Error() string { return e.message }

func (plan *orderedJSONMetricPlan) process(line string, base map[string]string) (map[string]string, bool, error) {
	return plan.processWithStreamLabels(line, base, base)
}

func (plan *orderedJSONMetricPlan) processWithStreamLabels(line string, base, streamLabels map[string]string) (map[string]string, bool, error) {
	return plan.processWithContext(context.Background(), line, base, streamLabels)
}

func (plan *orderedJSONMetricPlan) processWithContext(ctx context.Context, line string, base, streamLabels map[string]string) (map[string]string, bool, error) {
	labels := cloneStringMap(base)
	metadata := make(map[string]bool)
	for name := range base {
		if _, isStream := streamLabels[name]; !isStream {
			metadata[name] = true
		}
	}
	extracted := make(map[string]bool)
	for _, stage := range plan.stages {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		switch {
		case stage.parser:
			if !plan.noLabels && (plan.required == nil || len(extracted) != len(plan.required)) {
				collisions := cloneStringMap(streamLabels)
				for name := range metadata {
					collisions[name] = labels[name]
				}
				if err := plan.parseJSON(line, collisions, labels, extracted); err != nil {
					if errors.Is(err, errOrderedJSONFilterRejected) {
						return nil, false, nil
					}
					return nil, false, err
				}
			}
		case stage.filter != nil:
			if !stage.filter.Matches(labels[stage.filter.Field]) {
				return nil, false, nil
			}
		case stage.line != nil:
			if !stage.line(line) {
				return nil, false, nil
			}
		default:
			applyOrderedJSONFields(stage, labels)
			for name := range metadata {
				if _, exists := labels[name]; !exists {
					delete(metadata, name)
				}
			}
		}
	}
	return labels, true, nil
}

func applyOrderedJSONFields(stage orderedJSONStage, labels map[string]string) {
	if stage.keep && len(stage.fields) == 0 && len(stage.match) == 0 {
		return
	}
	for name, value := range labels {
		if stage.keep && (name == "__error__" || name == "__error_details__" || name == "__preserve_error__") {
			continue
		}
		matched := stage.fields[name]
		for _, condition := range stage.match {
			matched = matched || (condition.Field == name && condition.Matches(value))
		}
		if matched != stage.keep {
			delete(labels, name)
		}
	}
}

var errOrderedJSONHintsComplete = errors.New("required JSON labels extracted")

var errOrderedJSONFilterRejected = errors.New("JSON label filter rejected row")

var errOrderedJSONComplexity = errors.New("JSON metric parser depth or field limit exceeded")

func (plan *orderedJSONMetricPlan) parseJSON(line string, base, labels map[string]string, extracted map[string]bool) error {
	if len(line) > 1<<20 {
		return errOrderedJSONComplexity
	}
	fields := 0
	var walk func([]byte, string, int) error
	walk = func(data []byte, prefix string, depth int) error {
		if depth > 64 {
			return errOrderedJSONComplexity
		}
		return jsonparser.ObjectEach(data, func(key, value []byte, kind jsonparser.ValueType, _ int) error {
			fields++
			if fields > 1024 {
				return errOrderedJSONComplexity
			}
			name := orderedJSONLabelName(string(key), prefix == "")
			if name == "" && prefix == "" && kind != jsonparser.Object {
				return nil
			}
			if name == "" {
				name = prefix
			} else if prefix != "" {
				name = prefix + "_" + name
			}
			if len(name) > 1024 || len(value) > 64<<10 && kind != jsonparser.Object {
				return errOrderedJSONComplexity
			}
			if kind == jsonparser.Object {
				if plan.required != nil {
					wanted := false
					for label := range plan.required {
						wanted = wanted || strings.HasPrefix(label, name)
					}
					if !wanted {
						return nil
					}
				}
				return walk(value, name, depth+1)
			}
			if kind != jsonparser.String && kind != jsonparser.Number && kind != jsonparser.Boolean {
				return nil
			}
			if _, exists := base[name]; exists {
				name += "_extracted"
			}
			if (plan.required != nil && !plan.required[name]) || extracted[name] {
				return nil
			}
			decoded := orderedJSONScalar(value, kind)
			labels[name], extracted[name] = decoded, true
			if !plan.acceptExtractedLabel(name, decoded) {
				return errOrderedJSONFilterRejected
			}
			if plan.required != nil && len(extracted) == len(plan.required) {
				return errOrderedJSONHintsComplete
			}
			return nil
		})
	}
	return plan.recordJSONError(labels, walk([]byte(line), "", 0))
}

func (plan *orderedJSONMetricPlan) recordJSONError(labels map[string]string, err error) error {
	if err == nil || errors.Is(err, errOrderedJSONHintsComplete) {
		return nil
	}
	if errors.Is(err, errOrderedJSONComplexity) || errors.Is(err, errOrderedJSONFilterRejected) {
		return err
	}
	labels["__error__"] = "JSONParserErr"
	labels["__error_details__"] = err.Error()
	if plan.preserveError {
		labels["__preserve_error__"] = "true"
	}
	return nil
}

func orderedJSONScalar(value []byte, kind jsonparser.ValueType) string {
	if kind != jsonparser.String {
		return string(value)
	}
	decoded, err := jsonparser.Unescape(value, nil)
	if err != nil {
		return ""
	}
	// Match Loki's unescapeJSONString, including explicit U+FFFD escapes.
	return strings.Map(func(r rune) rune {
		if r == utf8.RuneError {
			return ' '
		}
		return r
	}, string(decoded))
}

func (plan *orderedJSONMetricPlan) acceptExtractedLabel(name, value string) bool {
	for _, filter := range plan.earlyFilters {
		if filter.Field == name {
			return filter.Matches(value)
		}
	}
	return true
}

func orderedJSONLabelName(name string, first bool) string {
	name = strings.TrimSpace(name)
	var out strings.Builder
	if first && len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		out.WriteByte('_')
	}
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (c >= '0' && c <= '9') {
			out.WriteRune(c)
		} else {
			out.WriteByte('_')
		}
	}
	return out.String()
}

func (plan *orderedJSONMetricPlan) groupLabels(labels map[string]string) map[string]string {
	if !plan.aggregated {
		// Loki merges label-reducing pipelines with a vector aggregation,
		// whose label builder omits empty values from the resulting series.
		if plan.reducesLabels {
			for name, value := range labels {
				if value == "" {
					delete(labels, name)
				}
			}
		}
		return labels
	}
	group := make(map[string]string)
	if plan.grouping == nil {
		return group
	}
	for name, value := range labels {
		included := false
		for _, wanted := range plan.grouping.Labels {
			included = included || name == wanted
		}
		if included != plan.grouping.Without && name != "__name__" && value != "" {
			group[name] = value
		}
	}
	return group
}

func (p *Proxy) collectOrderedJSONMetric(ctx context.Context, plan *orderedJSONMetricPlan, start, end time.Time, step time.Duration) (map[string]manualSeriesSamples, error) {
	query, err := p.translateQueryWithContext(ctx, plan.fetchQuery)
	if err != nil {
		return nil, err
	}
	limit := p.rangeMetricRowLimit
	if limit <= 0 {
		limit = 1_000_000
	}
	if limit == math.MaxInt {
		return nil, fmt.Errorf("manual range metric row limit is too large")
	}
	// The query argument limit asks VL to sort by timestamp. A final limit pipe
	// bounds raw collection without that sort; local window evaluation sorts
	// the complete accepted rows and rejects overflow instead of truncating.
	query += " | limit " + strconv.Itoa(limit+1)
	// VL's end is exclusive; Loki evaluation includes a line exactly at end.
	params := url.Values{"query": {query}, "start": {start.Add(-plan.window).UTC().Format(time.RFC3339Nano)}, "end": {end.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano)}}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("backend returned", resp.StatusCode, body)
	}
	maxBytes := p.orderedJSONMetricMaxBytes()
	readLimit := maxBytes
	if readLimit < math.MaxInt64 {
		readLimit++ // one byte past the cap detects overflow
	}
	limited := &io.LimitedReader{R: resp.Body, N: readLimit}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	series := make(map[string]manualSeriesSamples)
	streamLabels := make(map[string]map[string]string)
	descriptors := make(map[string]cachedLogQueryStreamDescriptor)
	exposures := make(map[string][]metadataFieldExposure)
	rows := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if limited.N <= 0 {
			return nil, orderedJSONResponseLimitError(maxBytes)
		}
		rows++
		if rows > limit {
			return nil, fmt.Errorf("manual range metric row limit exceeded (%d)", limit)
		}
		var row map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, fmt.Errorf("invalid backend metric row: %w", err)
		}
		ts, ok := parseFlexibleUnixNanos(row["_time"])
		if !ok {
			return nil, fmt.Errorf("invalid backend metric timestamp")
		}
		if !orderedJSONSampleVisible(ts, start.UnixNano(), end.UnixNano(), int64(step), int64(plan.window)) {
			continue
		}
		desc := p.logQueryStreamDescriptor(row["_stream"], row["level"], streamLabels, descriptors)
		base, err := p.orderedJSONBaseLabels(row, desc, exposures)
		if err != nil {
			return nil, err
		}
		labels, keep, err := plan.processWithContext(ctx, row["_msg"], base, desc.translatedLabels)
		if err != nil {
			return nil, err
		}
		if !keep {
			continue
		}
		if labels["__error__"] != "" && labels["__preserve_error__"] != "true" {
			return nil, &orderedJSONPipelineError{fmt.Sprintf("pipeline error: %q; filter errors with __error__=\"\" or explicitly drop __error__", labels["__error__"])}
		}
		labels = plan.groupLabels(labels)
		key := canonicalLabelsKey(labels)
		entry, exists := series[key]
		if !exists {
			if len(series) >= p.resolvedMaxStatsQuerySeries() {
				return nil, fmt.Errorf("maximum metric series exceeded (%d)", p.resolvedMaxStatsQuerySeries())
			}
			entry.Metric = labels
		}
		value := 1.0
		if plan.function == "bytes_rate" || plan.function == "bytes_over_time" {
			value = float64(len(row["_msg"]))
		}
		entry.Samples = append(entry.Samples, rangeMetricSample{ts: ts, value: value})
		series[key] = entry
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if limited.N <= 0 {
		return nil, orderedJSONResponseLimitError(maxBytes)
	}
	return series, ctx.Err()
}

// Raw VL rows have not executed unpack_json: regular fields outside _stream
// therefore represent structured metadata under the documented data model.
// Keep them available to filters and suffix parser collisions with _extracted.
func (p *Proxy) orderedJSONBaseLabels(row map[string]string, desc cachedLogQueryStreamDescriptor, exposures map[string][]metadataFieldExposure) (map[string]string, error) {
	base := cloneStringMap(desc.translatedLabels)
	for key, value := range row {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" || value == "" {
			continue
		}
		if _, exists := desc.rawLabels[key]; exists {
			continue
		}
		if len(key) > 1024 || len(value) > 64<<10 {
			return nil, errOrderedJSONComplexity
		}
		for _, exposure := range p.metadataFieldExposuresCached(key, exposures) {
			if _, exists := base[exposure.name]; !exists {
				base[exposure.name] = value
			}
		}
		if len(base) > 1024 {
			return nil, errOrderedJSONComplexity
		}
	}
	return base, nil
}

// The first evaluation at or after a row decides whether any window includes it;
// rows in gaps when step > window must not generate samples or pipeline errors.
func orderedJSONSampleVisible(ts, start, end, step, window int64) bool {
	eval := start
	if ts > start {
		delta := ts - start
		eval += (delta / step) * step
		if delta%step != 0 {
			eval += step
		}
	}
	return eval <= end && ts > eval-window && ts <= eval
}

// orderedJSONResponseLimitError names the flag that bounds the raw rows read.
func orderedJSONResponseLimitError(maxBytes int64) error {
	return fmt.Errorf("ordered JSON metric response exceeds %d bytes; narrow the query or increase -ordered-json-metric-max-bytes", maxBytes)
}

func buildOrderedJSONMetric(ctx context.Context, plan *orderedJSONMetricPlan, series map[string]manualSeriesSamples, start, end time.Time, step time.Duration, isRange bool, maxBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if step <= 0 || end.Before(start) {
		return nil, fmt.Errorf("invalid ordered JSON metric evaluation bounds")
	}
	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]map[string]interface{}, 0, len(keys))
	totalPoints := 0
	for _, key := range keys {
		entry := series[key]
		sort.Slice(entry.Samples, func(i, j int) bool { return entry.Samples[i].ts < entry.Samples[j].ts })
		left, right := 0, 0
		total := 0.0
		points := make([][]interface{}, 0)
		for eval := start; !eval.After(end); eval = eval.Add(step) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for right < len(entry.Samples) && entry.Samples[right].ts <= eval.UnixNano() {
				total += entry.Samples[right].value
				right++
			}
			for left < right && entry.Samples[left].ts <= eval.Add(-plan.window).UnixNano() {
				total -= entry.Samples[left].value
				left++
			}
			if right > left {
				totalPoints++
				if totalPoints > maxMetricEvalSamples {
					return nil, fmt.Errorf("ordered JSON metric output exceeds %d samples", maxMetricEvalSamples)
				}
				value := metricWindowValue(plan.function, total, plan.window)
				points = append(points, []interface{}{float64(eval.UnixNano()) / float64(time.Second), formatMetricSampleValue(value)})
			}
		}
		if len(points) == 0 {
			continue
		}
		item := map[string]interface{}{"metric": entry.Metric}
		if isRange {
			item["values"] = points
		} else {
			item["value"] = points[0]
		}
		result = append(result, item)
	}
	resultType := "vector"
	if isRange {
		resultType = "matrix"
	}
	body := marshalManualMetricResponse(resultType, result)
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("ordered JSON metric output exceeds %d bytes; narrow the query or increase -ordered-json-metric-max-bytes", maxBytes)
	}
	return body, nil
}
