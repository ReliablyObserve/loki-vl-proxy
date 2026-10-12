package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// absent_over_time (Loki v3.7.7).
//
// pkg/logql/evaluator.go:647-664 builds an AbsentRangeVectorEvaluator, and
// :754-778 (Next) answers a step whose window (t-range, t] holds no sample with
// one series valued 1 and a step that holds one with nothing. The series
// carries the labels absentLabels (:1320-1345) takes from the stream selector:
// a name with exactly one equality matcher; a name any other matcher also
// names, a regexp or inequality matcher, and a value of "" give none. The
// sample extractor keeps no labels (syntax/extractor.go:44-48), so every line
// the pipeline keeps is one sample whatever the parser extracted, and an
// unwrapped label that is empty or absent makes no sample
// (log/metrics_extraction.go:214-231). The steps are start, start+step, ...:
// Loki aligns a query to its step only with -querier.align-querier-with-step,
// off by default.
//
// The proxy asks the metric path for the presence of samples instead: the
// ungrouped `sum(count_over_time(...))`, or `sum(rate(...))` for an unwrapped
// label (Loki has no count_over_time with unwrap), which that path answers from
// VictoriaLogs stats as one series with a point at each step whose window
// holds a line, on Loki's window bounds and step grid
// (semantics/unwrap-rate-ungrouped-sum-per-stream). The answer is those steps
// inverted, on Loki's grid. The proxy reads no rows of its own; a label filter
// on a JSON-parsed field goes through the existing ordered-JSON metric route
// (handleOrderedJSONMetric), which pushes the parse down to stats and reads
// bounded rows only where VictoriaLogs' unpack cannot give Loki's parse.

// absentOverTimeCompatSpec is an absent_over_time, possibly under vector
// aggregations (outermost first).
type absentOverTimeCompatSpec struct {
	baseQuery      string
	rangeWindow    time.Duration
	rangeWindowStr string
	aggs           []*logqlpkg.VectorAggregation
	// labelReplaces are the label_replace calls around it, outermost first.
	labelReplaces []translator.LabelReplaceSpec
	// err is Loki's parse error for a form it rejects; the handlers answer 400.
	err string
}

// errAbsentGrouping is Loki's rejection of `absent_over_time(...) by (...)`
// (pkg/logql/syntax/ast.go:1439-1447 RangeAggregationExpr.validate, wrapped as a
// ParseError at :1409).
const errAbsentGrouping = "parse error : grouping not allowed for absent_over_time aggregation"

// parseAbsentOverTimeCompatSpec reads absent_over_time(<log query>[range]),
// alone or under vector aggregations and label_replace calls.
func parseAbsentOverTimeCompatSpec(logql string) (absentOverTimeCompatSpec, bool) {
	query := strings.TrimSpace(logql)
	var replaces []translator.LabelReplaceSpec
	for {
		inner, spec, ok := translator.SplitLabelReplace(query)
		if !ok {
			break
		}
		if _, err := regexp.Compile("^(?:" + spec.Regex + ")$"); err != nil {
			return absentOverTimeCompatSpec{}, false // the label_replace handlers report it
		}
		replaces = append(replaces, spec)
		query = strings.TrimSpace(inner)
	}
	var aggs []*logqlpkg.VectorAggregation
	var absent *logqlpkg.RangeAggregation
	if expr, err := logqlpkg.Parse(query); err == nil {
		node := expr
		for {
			agg, ok := node.(*logqlpkg.VectorAggregation)
			if !ok {
				break
			}
			aggs = append(aggs, agg)
			node = agg.Inner
		}
		ra, ok := node.(*logqlpkg.RangeAggregation)
		if !ok || ra.Op != logqlpkg.RangeAbsentOverTime {
			return absentOverTimeCompatSpec{}, false
		}
		if ra.Grouping != nil {
			return absentOverTimeCompatSpec{err: errAbsentGrouping}, true
		}
		absent = ra
		query = ra.String()
	} else if !absentOverTimeCompatRE.MatchString(query) {
		return absentOverTimeCompatSpec{}, false
	}
	matches := absentOverTimeCompatRE.FindStringSubmatch(query)
	if len(matches) != 3 || (absent == nil && len(aggs) > 0) {
		return absentOverTimeCompatSpec{}, false
	}
	window, ok := parsePositiveStepDuration(matches[2])
	if !ok {
		return absentOverTimeCompatSpec{}, false
	}
	baseQuery := strings.TrimSpace(matches[1])
	if baseQuery == "" {
		return absentOverTimeCompatSpec{}, false
	}
	return absentOverTimeCompatSpec{baseQuery: baseQuery, rangeWindow: window, rangeWindowStr: matches[2], aggs: aggs, labelReplaces: replaces}, true
}

// invalid returns the message of the 400 a spec the handlers cannot answer gets.
func (s absentOverTimeCompatSpec) invalid() string {
	switch {
	case s.err != "":
		return s.err
	case s.rangeWindowStr == "":
		return "absent_over_time: missing range window"
	}
	return ""
}

// unwrapped reports whether the log query unwraps a label.
func (s absentOverTimeCompatSpec) unwrapped() bool {
	return bareParserUnwrapFieldRE.MatchString(s.baseQuery)
}

// presenceQuery is the LogQL whose points are the steps that hold a sample.
func (s absentOverTimeCompatSpec) presenceQuery() string {
	fn := "count_over_time"
	if s.unwrapped() {
		fn = "rate"
	}
	return fmt.Sprintf("sum(%s(%s[%s]))", fn, s.baseQuery, s.rangeWindowStr)
}

// absentOverTimeLabels is absentLabels (pkg/logql/evaluator.go:1320-1345),
// matcher by matcher, with the label builder's rule that an empty value
// deletes the label.
func absentOverTimeLabels(baseQuery string) map[string]string {
	labels := map[string]string{}
	lq, err := logqlpkg.ParseLogQuery(strings.TrimSpace(baseQuery))
	if err != nil {
		return labels
	}
	var other []string
	for _, m := range lq.Selector.Matchers {
		if m.Name == "__name__" {
			continue
		}
		if _, has := labels[m.Name]; m.Op == logqlpkg.MatchEq && !has {
			if m.Value != "" {
				labels[m.Name] = m.Value
			}
		} else {
			other = append(other, m.Name)
		}
	}
	for _, name := range other {
		delete(labels, name)
	}
	return labels
}

// series folds the one absent series through the vector aggregations, as
// VectorAggEvaluator folds a single series (pkg/logql/evaluator.go:435-529):
// the grouping keeps or drops labels, count is 1, stddev and stdvar are 0, sum,
// avg, min and max keep the value, topk, bottomk and sort keep the series whole
// (none for k < 1), and label_replace then rewrites its labels. ok is false when
// no series is left.
func (s absentOverTimeCompatSpec) series() (labels map[string]string, value string, ok bool) {
	labels, value = absentOverTimeLabels(s.baseQuery), "1"
	for i := len(s.aggs) - 1; i >= 0; i-- {
		agg := s.aggs[i]
		switch agg.Op {
		case logqlpkg.VectorSort, logqlpkg.VectorSortDesc:
			continue // a sort keeps the series whole (evaluator.go:506-516)
		case logqlpkg.VectorTopK, logqlpkg.VectorBottomK:
			if int(agg.Param) < 1 {
				return nil, "", false
			}
			continue
		case logqlpkg.VectorCount:
			value = "1"
		case logqlpkg.VectorStddev, logqlpkg.VectorStdvar:
			value = "0"
		}
		kept := map[string]string{}
		switch g := agg.Grouping; {
		case g == nil:
		case g.Without:
			for k, v := range labels {
				kept[k] = v
			}
			for _, name := range g.Labels {
				delete(kept, name)
			}
		default:
			for _, name := range g.Labels {
				if v, has := labels[name]; has {
					kept[name] = v
				}
			}
		}
		labels = kept
	}
	for i := len(s.labelReplaces) - 1; i >= 0; i-- {
		spec := s.labelReplaces[i]
		replaceLabel(labels, regexp.MustCompile("^(?:"+spec.Regex+")$"), spec)
	}
	return labels, value, true
}

// absentPresenceDegraded reports a presence answer that is a stand-in for a
// failed or suppressed one: the partial-result reply (headers), the split
// residual suppression, and the series-limit partial whose warning is in the
// body (the body is authoritative, http_utils.go writeDrilldownPartialFromUpstream).
// Inverting it would claim data is absent.
func absentPresenceDegraded(h http.Header, body []byte) bool {
	return h.Get("Warning") != "" || h.Get("X-Proxy-Drilldown-Path") != "" || h.Get("X-Proxy-Upstream-Status") != "" || hasTopLevelWarnings(body)
}

// presenceAnswer runs the presence query through the metric path (handle is
// handleQueryRange or handleQuery) and returns its body. When the answer is not
// a clean 200 it is written to w as it is and ok is false.
func (p *Proxy) presenceAnswer(w http.ResponseWriter, r *http.Request, spec absentOverTimeCompatSpec, handle func(http.ResponseWriter, *http.Request)) (body []byte, ok bool) {
	bw := &bufferedResponseWriter{header: make(http.Header)}
	sc := &statusCapture{ResponseWriter: bw, code: http.StatusOK}
	handle(sc, cloneMetricQueryRequest(r, spec.presenceQuery()))
	if sc.code != http.StatusOK || absentPresenceDegraded(bw.header, bw.body) {
		copyHeaders(w.Header(), bw.header)
		w.WriteHeader(sc.code)
		_, _ = w.Write(bw.body)
		return nil, false
	}
	return bw.body, true
}

// presenceSteps returns the timestamps (ns) of the points of a presence answer.
// A point valued 0 is no sample for a count; a rate of unwrapped values may be 0 and is one.
func presenceSteps(body []byte, unwrapped bool) map[int64]bool {
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
				Value  []any   `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	present := map[int64]bool{}
	if json.Unmarshal(body, &resp) != nil {
		return present
	}
	for _, series := range resp.Data.Result {
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			if len(point) < 2 {
				continue
			}
			ts, _ := point[0].(float64)
			raw, _ := point[1].(string)
			if v, err := strconv.ParseFloat(raw, 64); !unwrapped && err == nil && v == 0 {
				continue
			}
			present[int64(math.Round(ts*1e9))] = true
		}
	}
	return present
}

// finishAbsent writes the answer. The request itself was counted by the
// presence query's handler, which also tracked the presence query; the query as
// the client wrote it is tracked here.
func (p *Proxy) finishAbsent(w http.ResponseWriter, endpoint, originalQuery string, start time.Time, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
	p.queryTracker.Record(endpoint, originalQuery, time.Since(start), false)
}

// proxyAbsentOverTimeQuery answers absent_over_time at /query: the series at
// the evaluation time when no sample is in (time-range, time], nothing otherwise.
func (p *Proxy) proxyAbsentOverTimeQuery(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec absentOverTimeCompatSpec) {
	if msg := spec.invalid(); msg != "" {
		p.writeError(w, http.StatusBadRequest, msg)
		p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
		return
	}
	evalNs, ok := parseFlexibleUnixNanos(r.FormValue("time"))
	if !ok {
		evalNs = time.Now().UnixNano()
	}
	r = cloneMetricQueryRequest(r, r.FormValue("query"))
	r.Form.Set("time", strconv.FormatInt(evalNs, 10))
	body, ok := p.presenceAnswer(w, r, spec, p.handleQuery)
	if !ok {
		p.queryTracker.Record("query", originalQuery, time.Since(start), true)
		return
	}
	result := []lokiVectorResult{}
	if labels, value, has := spec.series(); has && len(presenceSteps(body, spec.unwrapped())) == 0 {
		result = append(result, lokiVectorResult{Metric: labels, Value: []any{float64(evalNs) / float64(time.Second), value}})
	}
	out, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	p.finishAbsent(w, "query", originalQuery, start, out)
}

// proxyAbsentOverTimeQueryRange answers absent_over_time at /query_range: one
// series valued 1 at each step start, start+step, ... end whose window holds no
// sample, no series when every window holds one.
func (p *Proxy) proxyAbsentOverTimeQueryRange(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec absentOverTimeCompatSpec) {
	if msg := spec.invalid(); msg != "" {
		p.writeError(w, http.StatusBadRequest, msg)
		p.metrics.RecordRequest("query_range", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
		return
	}
	body, ok := p.presenceAnswer(w, r, spec, p.handleQueryRange)
	if !ok {
		p.queryTracker.Record("query_range", originalQuery, time.Since(start), true)
		return
	}
	startNs, okS := parseLokiTimeToUnixNano(r.FormValue("start"))
	endNs, okE := parseLokiTimeToUnixNano(r.FormValue("end"))
	step, okStep := parsePositiveStepDuration(r.FormValue("step"))
	if !okS || !okE || !okStep {
		p.writeError(w, http.StatusBadRequest, "invalid start, end or step")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	p.finishAbsent(w, "query_range", originalQuery, start, buildAbsentOverTimeMatrix(spec, presenceSteps(body, spec.unwrapped()), startNs, endNs, step.Nanoseconds()))
}

// buildAbsentOverTimeMatrix is the matrix of the steps start, start+step, ...
// end that are not in present. A presence point is matched to its step by
// index, not by its exact nanosecond: it reaches here as float seconds, which at
// today's epoch resolve only a few hundred nanoseconds.
func buildAbsentOverTimeMatrix(spec absentOverTimeCompatSpec, present map[int64]bool, startNs, endNs, stepNs int64) []byte {
	result := []lokiMatrixResult{}
	if labels, value, ok := spec.series(); ok && stepNs > 0 {
		has := make(map[int64]bool, len(present))
		for ts := range present {
			has[int64(math.Round(float64(ts-startNs)/float64(stepNs)))] = true
		}
		var values [][]any
		for i, ts := int64(0), startNs; ts <= endNs; i, ts = i+1, ts+stepNs {
			if !has[i] {
				values = append(values, []any{float64(ts) / float64(time.Second), value})
			}
		}
		if len(values) > 0 {
			result = append(result, lokiMatrixResult{Metric: labels, Values: values})
		}
	}
	out, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}})
	return out
}

// isAbsentOverTimeQuery reports whether the query is one the absent handlers
// answer, offset or not.
func isAbsentOverTimeQuery(query string) bool {
	_, stripped, err := extractLogQLOffset(query)
	if err != nil {
		return false
	}
	spec, ok := parseAbsentOverTimeCompatSpec(stripped)
	return ok && spec.err == ""
}

// mergeAbsentMultiTenant merges the per-tenant answers of an absent_over_time.
// Loki's multi-tenant querier merges the tenants' samples into one iterator
// before the evaluator runs (pkg/querier/multi_tenant_querier.go SelectSamples),
// so a step is absent only when no tenant has a sample, and the series carries
// the selector's labels alone, never __tenant_id__ (evaluator.go absentLabels).
// A tenant's answer holds a step exactly when it has no sample there, so the
// merged answer is the steps every tenant holds, per series. Samples are
// compared by label set and value, which the tenants share.
func mergeAbsentMultiTenant(recorders []*httptest.ResponseRecorder) ([]byte, error) {
	type point struct {
		ts    string
		value string
	}
	var resultType string
	var order []string
	metrics := map[string]map[string]string{}
	counts := map[string]map[point]int{} // series key -> point -> tenants holding it
	seen := map[string]int{}             // series key -> tenants holding the series
	points := map[string][][]any{}       // series key -> samples in first-seen order
	for _, rec := range recorders {
		var resp struct {
			Data struct {
				ResultType string            `json:"resultType"`
				Result     []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, err
		}
		resultType = resp.Data.ResultType
		for _, raw := range resp.Data.Result {
			var item struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			}
			if err := json.Unmarshal(raw, &item); err != nil {
				return nil, err
			}
			key, _ := json.Marshal(item.Metric)
			k := string(key)
			if _, ok := metrics[k]; !ok {
				metrics[k] = item.Metric
				counts[k] = map[point]int{}
				order = append(order, k)
			}
			seen[k]++
			vs := item.Values
			if item.Value != nil {
				vs = [][]any{item.Value}
			}
			for _, v := range vs {
				if len(v) < 2 {
					continue
				}
				pt := point{fmt.Sprint(v[0]), fmt.Sprint(v[1])}
				if counts[k][pt] == 0 {
					points[k] = append(points[k], v)
				}
				counts[k][pt]++
			}
		}
	}
	matrix, vector := []lokiMatrixResult{}, []lokiVectorResult{}
	for _, k := range order {
		if seen[k] != len(recorders) {
			continue
		}
		var held [][]any
		for _, v := range points[k] {
			if counts[k][point{fmt.Sprint(v[0]), fmt.Sprint(v[1])}] == len(recorders) {
				held = append(held, v)
			}
		}
		if len(held) == 0 {
			continue
		}
		if resultType == "vector" {
			vector = append(vector, lokiVectorResult{Metric: metrics[k], Value: held[0]})
		} else {
			matrix = append(matrix, lokiMatrixResult{Metric: metrics[k], Values: held})
		}
	}
	var result any = matrix
	if resultType == "vector" {
		result = vector
	}
	return json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": result}})
}
