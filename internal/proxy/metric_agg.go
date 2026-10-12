package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

var (
	vectorLiteralRE = regexp.MustCompile(`^\s*vector\(\s*([^)]+?)\s*\)\s*$`)
	vectorBinaryRE  = regexp.MustCompile(`^\s*vector\(\s*([^)]+?)\s*\)\s*([+\-*/])\s*vector\(\s*([^)]+?)\s*\)\s*$`)
)

// unwrapEmptySumWithout preserves every series for an explicit sum without().
// Its grouping keeps all labels except __name__. An absent grouping or by()
// instead reduces all labels. Evaluate the operand once, retaining the number
// of wrappers because removing empty labels can merge groups on a later pass.
func unwrapEmptySumWithout(query string) (logqlpkg.Expr, int) {
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return nil, 0
	}
	count := 0
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok || agg.Op != logqlpkg.VectorSum || agg.Grouping == nil || !agg.Grouping.Without || len(agg.Grouping.Labels) != 0 {
			break
		}
		expr = agg.Inner
		count++
	}
	if count == 0 {
		return nil, 0
	}
	return expr, count
}

func evaluateConstantInstantVectorQuery(expr, timeParam string) ([]byte, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}

	if matches := vectorLiteralRE.FindStringSubmatch(expr); len(matches) == 2 {
		value, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
		if err != nil {
			return nil, false
		}
		return buildConstantVectorResponse(timeParam, value), true
	}

	if matches := vectorBinaryRE.FindStringSubmatch(expr); len(matches) == 4 {
		left, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
		if err != nil {
			return nil, false
		}
		right, err := strconv.ParseFloat(strings.TrimSpace(matches[3]), 64)
		if err != nil {
			return nil, false
		}
		value, ok := applyConstantBinaryOp(left, right, matches[2])
		if !ok {
			return nil, false
		}
		return buildConstantVectorResponse(timeParam, value), true
	}

	return nil, false
}

func buildConstantVectorResponse(timeParam string, value float64) []byte {
	ts := parseInstantVectorTime(timeParam)
	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result": []map[string]interface{}{
				{
					"metric": map[string]string{},
					"value":  []interface{}{ts, strconv.FormatFloat(value, 'f', -1, 64)},
				},
			},
			"stats": map[string]interface{}{},
		},
	})
	return result
}

func parseInstantVectorTime(timeParam string) int64 {
	if timeParam == "" {
		return time.Now().UnixNano()
	}
	if t, err := time.Parse(time.RFC3339Nano, timeParam); err == nil {
		return t.UnixNano()
	}
	if i, err := strconv.ParseInt(timeParam, 10, 64); err == nil {
		if i < 1_000_000_000_000 {
			return i * int64(time.Second)
		}
		return i
	}
	if f, err := strconv.ParseFloat(timeParam, 64); err == nil {
		if f < 1_000_000_000_000 {
			return int64(f * float64(time.Second))
		}
		return int64(f)
	}
	return time.Now().UnixNano()
}

type instantMetricPostAgg struct {
	name  string
	inner string
	k     int
	// grouping is the by/without clause of a stddev or stdvar; nil aggregates
	// every series into one.
	grouping *logqlpkg.Grouping
}

// parseStddevStdvarQuery recognises stddev and stdvar over a vector with a
// `by`/`without` clause in either position, which Loki folds across the series
// of each group (pkg/logql/evaluator.go VectorAggEvaluator.Next). The operand
// is sliced from the query as written, the way the translator reads it.
func parseStddevStdvarQuery(logql string) (instantMetricPostAgg, bool) {
	expr, err := logqlpkg.Parse(logql)
	if err != nil {
		return instantMetricPostAgg{}, false
	}
	agg, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok || (agg.Op != logqlpkg.VectorStddev && agg.Op != logqlpkg.VectorStdvar) {
		return instantMetricPostAgg{}, false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(logql, string(agg.Op)))
	if !strings.HasPrefix(rest, "(") {
		// A leading grouping clause: skip it to the operand.
		open := strings.IndexByte(rest, '(')
		if open < 0 {
			return instantMetricPostAgg{}, false
		}
		rest = strings.TrimSpace(rest[open+1+strings.IndexByte(rest[open+1:], ')')+1:])
	}
	end := closingParenIndex(rest)
	if end < 0 {
		return instantMetricPostAgg{}, false
	}
	return instantMetricPostAgg{name: string(agg.Op), inner: strings.TrimSpace(rest[1:end]), grouping: agg.Grouping}, true
}

var postAggParserStageRE = regexp.MustCompile(`\|\s*(?:json|logfmt|pattern|regexp|unpack)\b`)

// translatePostAggOperand translates the operand of a post-aggregation. The
// operand of a stddev or stdvar `by` a parsed label is translated as the
// series-level form the vector aggregations count, min, max and avg use, whose
// first stats pipe groups by the listed labels and the series identity; the
// final stats pipe is dropped, as the fold across the series is the proxy's,
// and so is the alias of a function no math pipe reads (the shape of a bare
// range aggregation, which the manual range evaluator knows).
func (p *Proxy) translatePostAggOperand(ctx context.Context, postAgg instantMetricPostAgg) (string, error) {
	if g := postAgg.grouping; g != nil && !g.Without && len(g.Labels) > 0 && postAggParserStageRE.MatchString(postAgg.inner) {
		translated, err := p.translateQueryWithContext(ctx, "max by ("+strings.Join(g.Labels, ", ")+") ("+postAgg.inner+")")
		if i := strings.LastIndex(translated, " | stats "); err == nil && i > 0 && strings.Count(translated, "| stats ") > 1 {
			operand := translated[:i]
			if !strings.Contains(operand[strings.Index(operand, "| stats "):], "| math ") {
				operand = strings.TrimSuffix(operand, " as __lvp_inner")
			}
			return operand, nil
		}
	}
	return p.translateQueryWithContext(ctx, postAgg.inner)
}

// closingParenIndex returns the index of the parenthesis closing the one s
// opens, skipping quoted strings, or -1.
func closingParenIndex(s string) int {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"', '`':
			for i++; i < len(s) && s[i] != c; i++ {
				if c == '"' && s[i] == '\\' {
					i++
				}
			}
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

func parseInstantMetricPostAggQuery(logql string) (instantMetricPostAgg, bool) {
	logql = strings.TrimSpace(logql)
	for _, name := range []string{"sort_desc", "sort"} {
		prefix := name + "("
		if !strings.HasPrefix(logql, prefix) || !strings.HasSuffix(logql, ")") {
			continue
		}
		inner := strings.TrimSpace(logql[len(prefix) : len(logql)-1])
		if inner == "" {
			return instantMetricPostAgg{}, false
		}
		return instantMetricPostAgg{name: name, inner: inner}, true
	}
	// stddev/stdvar as outer aggregations over a metric
	// (e.g., stddev by (app) (count_over_time({...}[5m]))). The values to fold
	// are the inner query's, one per Loki series, which VictoriaLogs computes
	// with native stats; it has no stdvar, and its stddev (v1.49+) would leave
	// the sliding-window evaluator, which reads the first stats pipe, without
	// the fold. The proxy executes the inner query then applies the aggregation
	// across the returned series of each group.
	if postAgg, ok := parseStddevStdvarQuery(logql); ok {
		return postAgg, true
	}
	for _, name := range []string{"stddev", "stdvar"} {
		prefix := name + "("
		if !strings.HasPrefix(logql, prefix) || !strings.HasSuffix(logql, ")") {
			continue
		}
		inner := strings.TrimSpace(logql[len(prefix) : len(logql)-1])
		if inner == "" {
			return instantMetricPostAgg{}, false
		}
		return instantMetricPostAgg{name: name, inner: inner}, true
	}
	for _, name := range []string{"topk", "bottomk"} {
		prefix := name + "("
		if !strings.HasPrefix(logql, prefix) || !strings.HasSuffix(logql, ")") {
			continue
		}
		args := strings.TrimSpace(logql[len(prefix) : len(logql)-1])
		comma := topLevelCommaIndex(args)
		if comma <= 0 {
			return instantMetricPostAgg{}, false
		}
		k, err := strconv.Atoi(strings.TrimSpace(args[:comma]))
		if err != nil || k <= 0 {
			return instantMetricPostAgg{}, false
		}
		inner := strings.TrimSpace(args[comma+1:])
		if inner == "" {
			return instantMetricPostAgg{}, false
		}
		return instantMetricPostAgg{name: name, inner: inner, k: k}, true
	}
	return instantMetricPostAgg{}, false
}

func topLevelCommaIndex(s string) int {
	depth := 0
	inQuote := false
	for i, r := range s {
		switch r {
		case '"':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote && depth > 0 {
				depth--
			}
		case ',':
			if !inQuote && depth == 0 {
				return i
			}
		}
	}
	return -1
}

func preserveMetricStreamIdentity(originalLogQL, translatedLogsQL string, withoutLabels []string) string {
	if !isStatsQuery(translatedLogsQL) {
		return translatedLogsQL
	}
	if strings.Contains(translatedLogsQL, "| stats by (") {
		return translatedLogsQL
	}
	if len(withoutLabels) > 0 || isBareMetricFunctionQuery(strings.TrimSpace(originalLogQL)) {
		return addStatsByStreamClause(translatedLogsQL, logqlpkg.ParserCaptureLabels(originalLogQL))
	}
	return translatedLogsQL
}

func isBareMetricFunctionQuery(logql string) bool {
	for _, prefix := range []string{
		"rate(",
		"rate_counter(",
		"count_over_time(",
		"bytes_over_time(",
		"bytes_rate(",
		"sum_over_time(",
		"avg_over_time(",
		"max_over_time(",
		"min_over_time(",
		"first_over_time(",
		"last_over_time(",
		"stddev_over_time(",
		"stdvar_over_time(",
		"absent_over_time(",
		"quantile_over_time(",
	} {
		if strings.HasPrefix(logql, prefix) {
			return true
		}
	}
	return false
}

// addStatsByStreamClause groups a bare metric query by the series identity
// Loki gives it: the stream, its level, and every label a `| regexp` or
// `| pattern` stage extracted (captureLabels), which Loki counts as part of
// the series.
func addStatsByStreamClause(logsqlQuery string, captureLabels []string) string {
	idx := strings.Index(logsqlQuery, "| stats ")
	if idx < 0 {
		return logsqlQuery
	}
	statsStart := idx + len("| stats ")
	group := "_stream, level"
	for _, label := range captureLabels {
		group += ", " + quoteLogsQLIdent(label)
	}
	return logsqlQuery[:statsStart] + "by (" + group + ") " + logsqlQuery[statsStart:]
}

func (p *Proxy) handleInstantMetricPostAggregation(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, postAgg instantMetricPostAgg) {
	// Parse the inner LogQL expression to drive routing via typed AST.
	parsedInner, _ := logqlpkg.Parse(postAgg.inner)

	translatedInner, err := p.translatePostAggOperand(r.Context(), postAgg)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
		return
	}
	translatedInner, withoutLabels := translator.ParseWithoutMarker(translatedInner)
	translatedInner = preserveMetricStreamIdentity(postAgg.inner, translatedInner, withoutLabels)

	r = p.withRequestScope(r)

	bw := &bufferedResponseWriter{header: make(http.Header)}
	sc := &statusCapture{ResponseWriter: bw, code: 200}

	var dispatched bool
	if binOp, ok := parsedInner.(*logqlpkg.BinOpExpr); ok {
		leftLogsql, leftErr := p.translateQueryWithContext(r.Context(), binOp.Left.String())
		rightLogsql, rightErr := p.translateQueryWithContext(r.Context(), binOp.Right.String())
		if leftErr != nil {
			p.writeError(w, http.StatusBadRequest, leftErr.Error())
			p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
			return
		}
		if rightErr != nil {
			p.writeError(w, http.StatusBadRequest, rightErr.Error())
			p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
			return
		}
		p.proxyBinaryMetricQueryVM(sc, r, binOp.Op, leftLogsql, rightLogsql, binOpExprToVMInfo(binOp))
		dispatched = true
	}

	if !dispatched {
		if isStatsQuery(translatedInner) {
			p.proxyStatsQuery(sc, r, translatedInner)
		} else {
			p.writeError(w, http.StatusBadRequest, "unsupported instant aggregation target")
			p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
			return
		}
	}

	if len(withoutLabels) > 0 {
		bw.body = applyWithoutGrouping(bw.body, withoutLabels, withoutAggregationOp(originalQuery))
	}

	if sc.code >= http.StatusBadRequest {
		copyHeaders(w.Header(), bw.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(sc.code)
		_, _ = w.Write(bw.body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), sc.code), elapsed)
		p.queryTracker.Record("query", originalQuery, elapsed, true)
		return
	}

	result := applyInstantVectorPostAggregation(bw.body, postAgg)
	copyHeaders(w.Header(), bw.Header())
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	_, _ = w.Write(result)
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusOK), elapsed)
	p.queryTracker.Record("query", originalQuery, elapsed, false)
}

// handleRangeMetricPostAggregation handles topk/bottomk/sort at /query_range by
// fetching the full matrix from VL and ranking at each evaluation timestamp.
func (p *Proxy) handleRangeMetricPostAggregation(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, postAgg instantMetricPostAgg) {
	translatedInner, err := p.translatePostAggOperand(r.Context(), postAgg)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
		return
	}
	translatedInner, withoutLabels := translator.ParseWithoutMarker(translatedInner)
	translatedInner = preserveMetricStreamIdentity(postAgg.inner, translatedInner, withoutLabels)

	r = p.withRequestScope(r)

	// proxyStatsQueryRange reads r.FormValue("query") as originalLogql for the stats
	// compat layer. If the outer sort/topk wrapper is still in r.Form, parseOriginalRangeMetricSpec
	// extracts "sort" as the function name and normalizeManualMetricFunction returns "",
	// causing handleStatsCompatRange to fall through to the direct path with an empty result.
	// Clone the request and replace "query" with the unwrapped inner expression so the
	// stats compat layer can correctly identify the inner metric function.
	innerR := r.Clone(r.Context())
	_ = innerR.ParseForm()
	innerR.Form.Set("query", postAgg.inner)

	bw := &bufferedResponseWriter{header: make(http.Header)}
	sc := &statusCapture{ResponseWriter: bw, code: 200}
	// Ranking needs the complete trailing window at each evaluation point.
	// Native VL buckets are timestamped at their left edge; ranking those
	// directly can select the next window's winner and omit the first point.
	// The manual range adapter still uses VL pre-aggregation when available.
	handled := false
	if postAgg.name == "topk" || postAgg.name == "bottomk" {
		spec, ok := parseStatsCompatSpec(translatedInner)
		orig, hasOrig := parseOriginalRangeMetricSpec(postAgg.inner)
		if ok && hasOrig && orig.Window > 0 {
			fn := normalizeManualMetricFunction(spec, orig)
			switch fn {
			case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
				spec.OrigGroupBy = parseOriginalByLabels(postAgg.inner)
				// Rank every series per step from VictoriaLogs window stats
				// instead of truncating to the busiest series first.
				rankedR := innerR.WithContext(withRangeTopK(innerR.Context(), postAgg.k, postAgg.name == "topk"))
				handled = p.proxyManualRangeMetricRange(sc, rankedR, spec, orig, fn)
			}
		}
	}
	if !handled {
		p.proxyStatsQueryRange(sc, innerR, translatedInner)
	}

	if len(withoutLabels) > 0 {
		bw.body = applyWithoutGrouping(bw.body, withoutLabels, withoutAggregationOp(originalQuery))
	}

	if sc.code >= http.StatusBadRequest {
		copyHeaders(w.Header(), bw.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(sc.code)
		_, _ = w.Write(bw.body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query_range", unwrapRecordedStatus(r.Context(), sc.code), elapsed)
		p.queryTracker.Record("query_range", originalQuery, elapsed, true)
		return
	}

	result := applyMatrixPostAggregation(bw.body, postAgg)
	copyHeaders(w.Header(), bw.Header())
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	_, _ = w.Write(result)
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", unwrapRecordedStatus(r.Context(), http.StatusOK), elapsed)
	p.queryTracker.Record("query_range", originalQuery, elapsed, false)
}

// applyMatrixPostAggregation applies topk/bottomk/sort/stddev/stdvar to a matrix result.
func applyMatrixPostAggregation(body []byte, postAgg instantMetricPostAgg) []byte {
	if postAgg.name == "stddev" || postAgg.name == "stdvar" {
		return applyMatrixStddevAgg(body, postAgg.name, postAgg.grouping)
	}
	return applyMatrixSortTopkAgg(body, postAgg)
}

// stddevAccumulator folds the values of one group the way Loki's
// VectorAggEvaluator does (pkg/logql/evaluator.go): seeded with the first
// value, then Welford's update; the population variance is m2/n, the standard
// deviation its square root, 0 for a group of one value.
type stddevAccumulator struct{ mean, m2, n float64 }

func (a *stddevAccumulator) add(v float64) {
	a.n++
	if a.n == 1 {
		a.mean = v
		return
	}
	delta := v - a.mean
	a.mean += delta / a.n
	a.m2 += delta * (v - a.mean)
}

func (a *stddevAccumulator) result(funcName string) float64 {
	if funcName == "stdvar" {
		return a.m2 / a.n
	}
	return math.Sqrt(a.m2 / a.n)
}

// stddevGroupLabels returns the labels of the group a series falls into: `by`
// keeps the listed labels the series has, `without` drops them, no grouping
// keeps none (__name__ is never kept, as in VectorAggEvaluator).
func stddevGroupLabels(metric map[string]string, g *logqlpkg.Grouping) map[string]string {
	out := map[string]string{}
	if g == nil {
		return out
	}
	if !g.Without {
		for _, name := range g.Labels {
			if v, ok := metric[name]; ok && v != "" {
				out[name] = v
			}
		}
		return out
	}
	for name, v := range metric {
		if name != "__name__" && v != "" {
			out[name] = v
		}
	}
	for _, name := range g.Labels {
		delete(out, name)
	}
	return out
}

// stddevGroups holds the accumulators of the groups at each time, in the
// order of the groups' label sets.
type stddevGroups struct {
	grouping *logqlpkg.Grouping
	labels   map[string]map[string]string
	acc      map[string]map[float64]*stddevAccumulator
}

func newStddevGroups(g *logqlpkg.Grouping) *stddevGroups {
	return &stddevGroups{grouping: g, labels: map[string]map[string]string{}, acc: map[string]map[float64]*stddevAccumulator{}}
}

func (g *stddevGroups) add(metric map[string]string, ts, v float64) {
	labels := stddevGroupLabels(metric, g.grouping)
	key := metricKeyStr(labels)
	if g.acc[key] == nil {
		g.labels[key] = labels
		g.acc[key] = map[float64]*stddevAccumulator{}
	}
	if g.acc[key][ts] == nil {
		g.acc[key][ts] = &stddevAccumulator{}
	}
	g.acc[key][ts].add(v)
}

type stddevSeries struct {
	labels map[string]string
	points [][]any // (time, value) in time order
}

// series returns the groups ordered by label set.
func (g *stddevGroups) series(funcName string) []stddevSeries {
	keys := make([]string, 0, len(g.acc))
	for key := range g.acc {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]stddevSeries, 0, len(keys))
	for _, key := range keys {
		times := make([]float64, 0, len(g.acc[key]))
		for ts := range g.acc[key] {
			times = append(times, ts)
		}
		sort.Float64s(times)
		points := make([][]any, 0, len(times))
		for _, ts := range times {
			points = append(points, []any{ts, strconv.FormatFloat(g.acc[key][ts].result(funcName), 'f', -1, 64)})
		}
		out = append(out, stddevSeries{g.labels[key], points})
	}
	return out
}

// applyMatrixStddevAgg computes population stddev/stdvar across the series of
// each group at each timestamp, one series per group (Loki answers a group only
// at the steps where one of its series has a point). An answer with no
// resultType holds no rows and is an empty matrix.
func applyMatrixStddevAgg(body []byte, funcName string, grouping *logqlpkg.Grouping) []byte {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || (resp.Data.ResultType != "matrix" && resp.Data.ResultType != "") {
		return body
	}
	groups := newStddevGroups(grouping)
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			if len(v) < 2 {
				continue
			}
			ts, errT := parseFloat(v[0])
			val, errV := parseFloat(v[1])
			if errT == nil && errV == nil {
				groups.add(s.Metric, ts, val)
			}
		}
	}
	result := make([]map[string]any, 0)
	for _, g := range groups.series(funcName) {
		result = append(result, map[string]any{"metric": g.labels, "values": g.points})
	}
	out, err := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}})
	if err != nil {
		return body
	}
	return out
}

// applyMatrixSortTopkAgg applies topk/bottomk/sort to a matrix (query_range) result.
// topk/bottomk use per-step selection; sort orders series by their last value.
func applyMatrixSortTopkAgg(body []byte, postAgg instantMetricPostAgg) []byte {
	if postAgg.name == "topk" || postAgg.name == "bottomk" {
		return applyTopKToMatrix(body, postAgg.k, postAgg.name == "topk")
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]interface{} `json:"metric"`
				Values [][]interface{}        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || resp.Data.ResultType != "matrix" {
		return body
	}

	// Rank by last value of each series
	type ranked struct {
		idx  int
		last float64
	}
	ranks := make([]ranked, len(resp.Data.Result))
	for i, s := range resp.Data.Result {
		ranks[i].idx = i
		if len(s.Values) > 0 {
			last := s.Values[len(s.Values)-1]
			if len(last) >= 2 {
				if v, err := parseFloat(last[1]); err == nil {
					ranks[i].last = v
				}
			}
		}
	}

	sort.SliceStable(ranks, func(i, j int) bool {
		li, lj := ranks[i].last, ranks[j].last
		switch postAgg.name {
		case "sort", "bottomk": // ascending
			if li == lj {
				return ranks[i].idx < ranks[j].idx
			}
			return li < lj
		default: // topk, sort_desc — descending
			if li == lj {
				return ranks[i].idx < ranks[j].idx
			}
			return li > lj
		}
	})

	// sort/sort_desc return all series reordered.
	resultCount := len(ranks)

	// Pre-allocate with safe maximum size to avoid CodeQL taint analysis issues
	// with user-provided allocation sizes. Use a fixed-size allocation and populate
	// only the needed elements.
	const preallocSize = 10000
	selected := make([]struct {
		Metric map[string]interface{} `json:"metric"`
		Values [][]interface{}        `json:"values"`
	}, preallocSize)

	if resultCount > len(selected) {
		resultCount = len(selected)
	}
	for i := 0; i < resultCount; i++ {
		selected[i] = resp.Data.Result[ranks[i].idx]
	}
	resp.Data.Result = selected[:resultCount]

	out, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return out
}

func parseFloat(v interface{}) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case string:
		return strconv.ParseFloat(t, 64)
	default:
		return 0, fmt.Errorf("not a number")
	}
}

func applyInstantVectorPostAggregation(body []byte, postAgg instantMetricPostAgg) []byte {
	if postAgg.name == "stddev" || postAgg.name == "stdvar" {
		return applyInstantStddevAgg(body, postAgg.name, postAgg.grouping)
	}
	return applyInstantSortTopkAgg(body, postAgg)
}

// applyInstantStddevAgg computes population stddev/stdvar across the series of
// each group in an instant vector, one sample per group. An answer with no
// resultType holds no rows and is an empty vector.
func applyInstantStddevAgg(body []byte, funcName string, grouping *logqlpkg.Grouping) []byte {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || (resp.Data.ResultType != "vector" && resp.Data.ResultType != "") {
		return body
	}
	groups := newStddevGroups(grouping)
	for _, s := range resp.Data.Result {
		if len(s.Value) < 2 {
			continue
		}
		ts, errT := parseFloat(s.Value[0])
		val, errV := parseFloat(s.Value[1])
		if errT == nil && errV == nil {
			groups.add(s.Metric, ts, val)
		}
	}
	// Loki aggregates an empty input vector into an empty vector.
	result := make([]map[string]any, 0)
	for _, g := range groups.series(funcName) {
		result = append(result, map[string]any{"metric": g.labels, "value": g.points[0]})
	}
	out, err := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	if err != nil {
		return body
	}
	return out
}

func applyInstantSortTopkAgg(body []byte, postAgg instantMetricPostAgg) []byte {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]interface{} `json:"metric"`
				Value  []interface{}          `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || resp.Data.ResultType != "vector" {
		return body
	}

	sort.SliceStable(resp.Data.Result, func(i, j int) bool {
		left := vectorPointValue(resp.Data.Result[i].Value)
		right := vectorPointValue(resp.Data.Result[j].Value)
		switch postAgg.name {
		case "sort", "bottomk":
			if left == right {
				return metricKey(resp.Data.Result[i].Metric) < metricKey(resp.Data.Result[j].Metric)
			}
			return left < right
		default:
			if left == right {
				return metricKey(resp.Data.Result[i].Metric) < metricKey(resp.Data.Result[j].Metric)
			}
			return left > right
		}
	})

	if (postAgg.name == "topk" || postAgg.name == "bottomk") && postAgg.k < len(resp.Data.Result) {
		resp.Data.Result = resp.Data.Result[:postAgg.k]
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return out
}

func vectorPointValue(value []interface{}) float64 {
	if len(value) < 2 {
		return 0
	}
	switch raw := value[1].(type) {
	case string:
		parsed, _ := strconv.ParseFloat(raw, 64)
		return parsed
	case float64:
		return raw
	default:
		return 0
	}
}

func applyConstantBinaryOp(left, right float64, op string) (float64, bool) {
	switch op {
	case "+":
		return left + right, true
	case "-":
		return left - right, true
	case "*":
		return left * right, true
	case "/":
		if right == 0 {
			return 0, false
		}
		return left / right, true
	default:
		return 0, false
	}
}
