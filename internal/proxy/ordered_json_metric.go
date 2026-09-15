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
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/grafana/jsonparser"
)

const orderedJSONMetricMaxBytes = 64 << 20

type orderedJSONStage struct {
	parser bool
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
}

// handleOrderedJSONMetric uses the existing metric accumulators, but executes
// parser-dependent stages before reducing rows to samples. VictoriaLogs does not
// attach Loki's parser error labels, so pushing those filters into VL loses rows.
func (p *Proxy) handleOrderedJSONMetric(w http.ResponseWriter, r *http.Request, requestStart time.Time, query string, isRange bool) bool {
	plan, ok := compileOrderedJSONMetric(query)
	if !ok {
		return false
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
	if err == nil {
		var series map[string]manualSeriesSamples
		series, err = p.collectOrderedJSONMetric(r.Context(), plan, start, end, step)
		if err == nil {
			body, err = buildOrderedJSONMetric(r.Context(), plan, series, start, end, step, isRange)
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
		return time.Time{}, time.Time{}, 0, &orderedJSONPipelineError{"invalid range timestamps or step; maximum 10000 evaluation points"}
	}
	return time.Unix(0, start), time.Unix(0, end), time.Duration(step), nil
}

func compileOrderedJSONMetric(query string) (*orderedJSONMetricPlan, bool) {
	if !strings.Contains(query, "json") {
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
	hasJSON := false
	var retainedLines strings.Builder
	for _, stage := range logExpr.Pipeline {
		compiled, valid := compileOrderedJSONStage(stage)
		if !valid {
			return nil, false
		}
		hasJSON = hasJSON || compiled.parser
		plan.reducesLabels = plan.reducesLabels || compiled.fields != nil
		plan.stages = append(plan.stages, compiled)
		if _, ok := stage.(*logqlpkg.LineFilterStage); ok {
			retainedLines.WriteByte(' ')
			retainedLines.WriteString(stage.String())
		}
	}
	if !hasJSON {
		return nil, false
	}
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
		out.parser = s.Type == logqlpkg.ParserJSON && s.Param == ""
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
	limited := &io.LimitedReader{R: resp.Body, N: orderedJSONMetricMaxBytes + 1}
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
			return nil, fmt.Errorf("ordered JSON metric response exceeds %d bytes", orderedJSONMetricMaxBytes)
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
		return nil, fmt.Errorf("ordered JSON metric response exceeds %d bytes", orderedJSONMetricMaxBytes)
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

func buildOrderedJSONMetric(ctx context.Context, plan *orderedJSONMetricPlan, series map[string]manualSeriesSamples, start, end time.Time, step time.Duration, isRange bool) ([]byte, error) {
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
	if len(body) > orderedJSONMetricMaxBytes {
		return nil, fmt.Errorf("ordered JSON metric output exceeds %d bytes", orderedJSONMetricMaxBytes)
	}
	return body, nil
}
