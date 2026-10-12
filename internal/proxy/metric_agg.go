package proxy

import (
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

// evaluateConstantInstantQuery answers an instant query of constants, which has
// no data behind it. vector(n), and a binary operation of two of them, is a
// vector of one sample with no labels (Loki pkg/logql/engine.go evalVector); a
// literal, or a binary operation of literals, is a scalar (evalLiteral), whose
// sample time is integer milliseconds (see constantInstantMillis).
func evaluateConstantInstantQuery(expr, timeParam string) ([]byte, bool) {
	parsed, err := logqlpkg.Parse(strings.TrimSpace(expr))
	if err != nil {
		return nil, false
	}
	value, isVector, ok := binaryConstantValue(parsed, 0)
	if !ok {
		return nil, false
	}
	if isVector {
		return buildConstantVectorResponse(timeParam, value), true
	}
	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "scalar",
			"result":     []interface{}{constantInstantMillis(timeParam), strconv.FormatFloat(value, 'f', -1, 64)},
		},
	})
	return result, true
}

// constantInstantSeconds is the evaluation time as a vector sample time: seconds
// with millisecond precision (Loki pkg/querier/queryrange/prometheus.go
// marshalVector, model.Time(ms)), not the nanoseconds the request carries.
func constantInstantSeconds(timeParam string) float64 {
	return float64(constantInstantMillis(timeParam)) / 1e3
}

// constantInstantMillis is the evaluation time as a scalar sample time. Loki
// answers it as integer milliseconds, not seconds: marshalScalar
// (pkg/querier/queryrange/prometheus.go) hands the millisecond timestamp of
// promql.Scalar.T (pkg/logql/engine.go evalLiteral) to model.TimeFromUnix,
// which reads seconds, so model.Time.String prints the millisecond count.
func constantInstantMillis(timeParam string) int64 {
	return parseInstantVectorTime(timeParam) / int64(time.Millisecond)
}

func buildConstantVectorResponse(timeParam string, value float64) []byte {
	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result": []map[string]interface{}{
				{
					"metric": map[string]string{},
					"value":  []interface{}{constantInstantSeconds(timeParam), strconv.FormatFloat(value, 'f', -1, 64)},
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
	// group is the by()/without() clause of topk and bottomk: k series are kept
	// per group (nil: one group).
	group *logqlpkg.Grouping
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
	// stddev/stdvar as outer aggregations over an already-aggregated metric
	// (e.g., stddev(sum by(app)(count_over_time({...}[5m])))).
	// VL has no direct equivalent; the proxy executes the inner query then
	// applies the aggregation across the returned series.
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
	if postAgg, ok := parseTopKPostAgg(logql); ok {
		return postAgg, true
	}
	// A query the typed parser reads is not topk here (a binary expression
	// around it, say); only one it rejects keeps the textual split, and only
	// when the name's own parenthesis is the last character.
	if _, err := logqlpkg.Parse(logql); err == nil {
		return instantMetricPostAgg{}, false
	}
	for _, name := range []string{"topk", "bottomk"} {
		prefix := name + "("
		if !strings.HasPrefix(logql, prefix) || closingParen(logql, len(prefix)-1) != len(logql)-1 {
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

// parseTopKPostAgg splits topk and bottomk in every spelling Loki's grammar
// accepts (pkg/logql/syntax/syntax.y:182-184): `topk(k, expr)`, `topk (k,
// expr)`, `topk by (l) (k, expr)`, `topk without (l) (k, expr)` and
// `topk(k, expr) by (l)`. The by()/without() clause ranks k series per group
// of its labels (pkg/logql/evaluator.go VectorAggEvaluator.Next keeps one heap
// per grouping key). The inner expression is the source text, not a
// reserialised AST.
func parseTopKPostAgg(logql string) (instantMetricPostAgg, bool) {
	rest := strings.TrimSpace(logql)
	name := ""
	for _, candidate := range []string{"topk", "bottomk"} {
		if strings.HasPrefix(rest, candidate) {
			name, rest = candidate, strings.TrimSpace(rest[len(candidate):])
			break
		}
	}
	if name == "" {
		return instantMetricPostAgg{}, false
	}
	leading := false
	if i := strings.IndexByte(rest, '('); i > 0 {
		// `by (l) (k, expr)`: the label list comes before the arguments.
		close := closingParen(rest, i)
		if close < 0 {
			return instantMetricPostAgg{}, false
		}
		rest, leading = strings.TrimSpace(rest[close+1:]), true
	}
	if !strings.HasPrefix(rest, "(") {
		return instantMetricPostAgg{}, false
	}
	close := closingParen(rest, 0)
	if close < 0 || (leading && strings.TrimSpace(rest[close+1:]) != "") {
		return instantMetricPostAgg{}, false
	}
	args := rest[1:close]
	comma := topLevelCommaIndex(args)
	parsed, err := logqlpkg.Parse(strings.TrimSpace(logql))
	va, ok := parsed.(*logqlpkg.VectorAggregation)
	if err != nil || !ok || comma <= 0 || !va.HasParam || int(va.Param) <= 0 {
		return instantMetricPostAgg{}, false
	}
	inner := strings.TrimSpace(args[comma+1:])
	if inner == "" {
		return instantMetricPostAgg{}, false
	}
	return instantMetricPostAgg{name: name, inner: inner, k: int(va.Param), group: va.Grouping}, true
}

// closingParen returns the index of the ")" that closes the "(" at s[open],
// skipping quoted strings and raw strings, or -1.
func closingParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '"', '`':
			quote := s[i]
			for i++; i < len(s) && s[i] != quote; i++ {
				if quote == '"' && s[i] == '\\' {
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

// topKCompare orders two sample values for topk, bottomk, sort and sort_desc:
// negative when a ranks before b, positive after, zero when they tie. NaN ranks
// last in both directions, as in Loki's heaps (pkg/logql/vector.go: a NaN is
// the lowest of vectorByValueHeap and of vectorByReverseValueHeap, and a full
// heap lets any number displace a NaN root), so it never blocks a number.
func topKCompare(a, b float64, descending bool) int {
	switch aNaN, bNaN := math.IsNaN(a), math.IsNaN(b); {
	case aNaN && bNaN, a == b:
		return 0
	case aNaN:
		return 1
	case bNaN:
		return -1
	case (a > b) == descending:
		return -1
	}
	return 1
}

// topKGroupKey identifies the group a series is ranked in: the values of the
// by() labels, or every label but the without() ones and __name__. Without a
// clause all series are one group.
func topKGroupKey(group *logqlpkg.Grouping, metric map[string]string) string {
	if group == nil || (!group.Without && len(group.Labels) == 0) {
		return ""
	}
	kept := make(map[string]string, len(metric))
	if group.Without {
		for name, value := range metric {
			kept[name] = value
		}
		delete(kept, "__name__")
		for _, name := range group.Labels {
			delete(kept, name)
		}
	} else {
		for _, name := range group.Labels {
			kept[name] = metric[name]
		}
	}
	return canonicalLabelsKey(kept)
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

	translatedInner, err := p.translateQueryWithContext(r.Context(), postAgg.inner)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
		return
	}
	translatedInner, markerOp, withoutLabels := translator.ParseWithoutMarker(translatedInner)
	withoutOp, foldWithout := withoutFoldOp(markerOp, originalQuery)
	translatedInner, transforms, regexErr := parseLabelTransformMarkers(translatedInner)
	if regexErr != "" {
		p.writeError(w, http.StatusBadRequest, regexErr)
		p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
		return
	}
	translatedInner = preserveMetricStreamIdentity(postAgg.inner, translatedInner, withoutLabels)

	r = p.withRequestScope(r)
	_, innerIsBinary := parsedInner.(*logqlpkg.BinOpExpr)
	if innerIsBinary {
		// The operands of a binary expression fold and are limited on their own.
		withoutLabels = nil
	}
	foldWithout = foldWithout && len(withoutLabels) > 0
	if foldWithout {
		r = p.withWithoutFold(r)
	}

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
			statsR := r
			if foldWithout {
				// The ranking wraps the folded series: the stats query must not apply
				// it to the per-stream rows, which hold a different set than the fold.
				statsR = cloneMetricQueryRequest(r, postAgg.inner)
			}
			p.proxyStatsQuery(sc, statsR, translatedInner)
		} else {
			p.writeError(w, http.StatusBadRequest, "unsupported instant aggregation target")
			p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
			return
		}
	}

	if len(withoutLabels) > 0 {
		bw.body = applyWithoutChain(bw.body, withoutLabels, originalQuery, withoutOp)
	}
	if sc.code < http.StatusBadRequest {
		bw.body = transforms.apply(bw.body)
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
	if foldWithout {
		var capErr error
		if result, capErr = p.capWithoutFold(r.Context(), result); capErr != nil {
			p.writeError(w, http.StatusBadRequest, capErr.Error())
			p.metrics.RecordRequest("query", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
			return
		}
	}
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
	translatedInner, err := p.translateQueryWithContext(r.Context(), postAgg.inner)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
		return
	}
	translatedInner, markerOp, withoutLabels := translator.ParseWithoutMarker(translatedInner)
	withoutOp, foldWithout := withoutFoldOp(markerOp, originalQuery)
	translatedInner, transforms, regexErr := parseLabelTransformMarkers(translatedInner)
	if regexErr != "" {
		p.writeError(w, http.StatusBadRequest, regexErr)
		p.metrics.RecordRequest("query_range", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
		return
	}
	translatedInner = preserveMetricStreamIdentity(postAgg.inner, translatedInner, withoutLabels)

	r = p.withRequestScope(r)
	if parsedInner, _ := logqlpkg.Parse(postAgg.inner); parsedInner != nil {
		if _, binary := parsedInner.(*logqlpkg.BinOpExpr); binary {
			// The operands of a binary expression fold on their own.
			withoutLabels = nil
		}
	}
	foldWithout = foldWithout && len(withoutLabels) > 0
	if foldWithout {
		r = p.withWithoutFold(r)
	}

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
				rankedR := innerR.WithContext(withRangeTopK(innerR.Context(), postAgg.k, postAgg.name == "topk", postAgg.group))
				handled = p.proxyManualRangeMetricRange(sc, rankedR, spec, orig, fn)
			}
		}
	}
	if !handled {
		p.proxyStatsQueryRange(sc, innerR, translatedInner)
	}

	if len(withoutLabels) > 0 {
		bw.body = applyWithoutChain(bw.body, withoutLabels, originalQuery, withoutOp)
	}
	if sc.code < http.StatusBadRequest {
		bw.body = transforms.apply(bw.body)
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
	if foldWithout {
		var capErr error
		if result, capErr = p.capWithoutFold(r.Context(), result); capErr != nil {
			p.writeError(w, http.StatusBadRequest, capErr.Error())
			p.metrics.RecordRequest("query_range", unwrapRecordedStatus(r.Context(), http.StatusBadRequest), time.Since(start))
			return
		}
	}
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
		return applyMatrixStddevAgg(body, postAgg.name)
	}
	return applyMatrixSortTopkAgg(body, postAgg)
}

// applyMatrixStddevAgg computes population stddev/stdvar across all series
// at each timestamp and returns a single unlabelled series.
func applyMatrixStddevAgg(body []byte, funcName string) []byte {
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

	// Collect values per timestamp across all series.
	tsValues := make(map[float64][]float64)
	var tsOrder []float64
	seen := make(map[float64]struct{})
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			if len(v) < 2 {
				continue
			}
			ts, err := parseFloat(v[0])
			if err != nil {
				continue
			}
			val, err := parseFloat(v[1])
			if err != nil {
				continue
			}
			tsValues[ts] = append(tsValues[ts], val)
			if _, ok := seen[ts]; !ok {
				seen[ts] = struct{}{}
				tsOrder = append(tsOrder, ts)
			}
		}
	}
	sort.Slice(tsOrder, func(i, j int) bool { return tsOrder[i] < tsOrder[j] })

	values := make([][]interface{}, 0, len(tsOrder))
	for _, ts := range tsOrder {
		var agg float64
		if funcName == "stdvar" {
			agg = populationVariance(tsValues[ts])
		} else {
			agg = populationStddev(tsValues[ts])
		}
		values = append(values, []interface{}{ts, strconv.FormatFloat(agg, 'f', -1, 64)})
	}

	out, err := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result": []map[string]interface{}{
				{"metric": map[string]string{}, "values": values},
			},
		},
	})
	if err != nil {
		return body
	}
	return out
}

// applyMatrixSortTopkAgg applies topk/bottomk/sort to a matrix (query_range) result.
// topk/bottomk use per-step selection; sort orders series by their last value.
func applyMatrixSortTopkAgg(body []byte, postAgg instantMetricPostAgg) []byte {
	if postAgg.name == "topk" || postAgg.name == "bottomk" {
		return applyTopKToMatrix(body, postAgg.k, postAgg.name == "topk", postAgg.group)
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
		return applyInstantStddevAgg(body, postAgg.name)
	}
	return applyInstantSortTopkAgg(body, postAgg)
}

// applyInstantStddevAgg computes population stddev/stdvar across all series
// in an instant vector and returns a single unlabelled scalar.
func applyInstantStddevAgg(body []byte, funcName string) []byte {
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

	// Collect all series values and the timestamp from the first series.
	var vals []float64
	var ts float64
	for i, s := range resp.Data.Result {
		if len(s.Value) < 2 {
			continue
		}
		if i == 0 {
			ts, _ = parseFloat(s.Value[0])
		}
		if v, err := parseFloat(s.Value[1]); err == nil {
			vals = append(vals, v)
		}
	}

	if len(vals) == 0 {
		// Loki aggregates an empty input vector into an empty vector.
		return []byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}
	var agg float64
	if funcName == "stdvar" {
		agg = populationVariance(vals)
	} else {
		agg = populationStddev(vals)
	}

	out, err := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result": []map[string]interface{}{
				{"metric": map[string]string{}, "value": []interface{}{ts, strconv.FormatFloat(agg, 'f', -1, 64)}},
			},
		},
	})
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

	descending := postAgg.name != "sort" && postAgg.name != "bottomk"
	sort.SliceStable(resp.Data.Result, func(i, j int) bool {
		if c := topKCompare(vectorPointValue(resp.Data.Result[i].Value), vectorPointValue(resp.Data.Result[j].Value), descending); c != 0 {
			return c < 0
		}
		return metricKey(resp.Data.Result[i].Metric) < metricKey(resp.Data.Result[j].Metric)
	})

	if postAgg.name == "topk" || postAgg.name == "bottomk" {
		// The sort above ranks the whole vector; keep k of every group.
		kept, seen := resp.Data.Result[:0], map[string]int{}
		for _, sample := range resp.Data.Result {
			metric := make(map[string]string, len(sample.Metric))
			for name, value := range sample.Metric {
				metric[name] = fmt.Sprint(value)
			}
			group := topKGroupKey(postAgg.group, metric)
			if seen[group]++; seen[group] <= postAgg.k {
				kept = append(kept, sample)
			}
		}
		resp.Data.Result = kept
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

// populationStddev computes the population standard deviation of vals.
func populationStddev(vals []float64) float64 {
	n := float64(len(vals))
	if n == 0 {
		return 0
	}
	mean := 0.0
	for _, v := range vals {
		mean += v
	}
	mean /= n
	variance := 0.0
	for _, v := range vals {
		d := v - mean
		variance += d * d
	}
	return math.Sqrt(variance / n)
}

// populationVariance computes the population variance of vals.
func populationVariance(vals []float64) float64 {
	d := populationStddev(vals)
	return d * d
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
