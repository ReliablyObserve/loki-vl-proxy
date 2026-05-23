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
	vectorBinaryRE  = regexp.MustCompile(`^\s*vector\(\s*([^)]+?)\s*\)\s*([+\-*/])\s*vector\(\s*([^)]+?)\s*\)\s*$`)
)

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
		return addStatsByStreamClause(translatedLogsQL)
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

func addStatsByStreamClause(logsqlQuery string) string {
	idx := strings.Index(logsqlQuery, "| stats ")
	if idx < 0 {
		return logsqlQuery
	}
	statsStart := idx + len("| stats ")
	return logsqlQuery[:statsStart] + "by (_stream, level) " + logsqlQuery[statsStart:]
}

func (p *Proxy) handleInstantMetricPostAggregation(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, postAgg instantMetricPostAgg) {
	// Parse the inner LogQL expression to drive routing via typed AST.
	parsedInner, _ := logqlpkg.Parse(postAgg.inner)

	translatedInner, err := p.translateQueryWithContext(r.Context(), postAgg.inner)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}
	translatedInner, withoutLabels := translator.ParseWithoutMarker(translatedInner)
	translatedInner = preserveMetricStreamIdentity(postAgg.inner, translatedInner, withoutLabels)

	r = withOrgID(r)

	bw := &bufferedResponseWriter{header: make(http.Header)}
	sc := &statusCapture{ResponseWriter: bw, code: 200}

	var dispatched bool
	if ra, ok := parsedInner.(*logqlpkg.RangeAggregation); ok && ra.Step != "" {
		innerLogsql, innerErr := p.translateQueryWithContext(r.Context(), ra.Inner.String())
		if innerErr != nil {
			p.writeError(w, http.StatusBadRequest, innerErr.Error())
			p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
			return
		}
		p.proxySubquery(sc, r, string(ra.Op), innerLogsql, ra.Range, ra.Step)
		dispatched = true
	} else if binOp, ok := parsedInner.(*logqlpkg.BinOpExpr); ok {
		leftLogsql, leftErr := p.translateQueryWithContext(r.Context(), binOp.Left.String())
		rightLogsql, rightErr := p.translateQueryWithContext(r.Context(), binOp.Right.String())
		if leftErr != nil {
			p.writeError(w, http.StatusBadRequest, leftErr.Error())
			p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
			return
		}
		if rightErr != nil {
			p.writeError(w, http.StatusBadRequest, rightErr.Error())
			p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
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
			p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
			return
		}
	}

	if len(withoutLabels) > 0 {
		bw.body = applyWithoutGrouping(bw.body, withoutLabels)
	}

	if sc.code >= http.StatusBadRequest {
		copyHeaders(w.Header(), bw.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(sc.code)
		_, _ = w.Write(bw.body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query", sc.code, elapsed)
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
	p.metrics.RecordRequest("query", http.StatusOK, elapsed)
	p.queryTracker.Record("query", originalQuery, elapsed, false)
}

// handleRangeMetricPostAggregation handles topk/bottomk/sort at /query_range by
// fetching the full matrix from VL and then trimming to the requested K series.
func (p *Proxy) handleRangeMetricPostAggregation(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, postAgg instantMetricPostAgg) {
	translatedInner, err := p.translateQueryWithContext(r.Context(), postAgg.inner)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	translatedInner, withoutLabels := translator.ParseWithoutMarker(translatedInner)
	translatedInner = preserveMetricStreamIdentity(postAgg.inner, translatedInner, withoutLabels)

	r = withOrgID(r)

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
	p.proxyStatsQueryRange(sc, innerR, translatedInner)

	if len(withoutLabels) > 0 {
		bw.body = applyWithoutGrouping(bw.body, withoutLabels)
	}

	if sc.code >= http.StatusBadRequest {
		copyHeaders(w.Header(), bw.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(sc.code)
		_, _ = w.Write(bw.body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query_range", sc.code, elapsed)
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
	p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
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
// It ranks series by their last value and trims to the requested K.
func applyMatrixSortTopkAgg(body []byte, postAgg instantMetricPostAgg) []byte {
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

	// sort/sort_desc return all series reordered; only topk/bottomk trim to k.
	resultCount := len(ranks)
	if (postAgg.name == "topk" || postAgg.name == "bottomk") && postAgg.k > 0 {
		// Ensure topk size is safe: bounded by min(requested, max constant, available)
		const maxTopK = 10000
		safeSize := postAgg.k
		if safeSize > maxTopK {
			safeSize = maxTopK
		}
		if safeSize < resultCount {
			resultCount = safeSize
		}
	}

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
