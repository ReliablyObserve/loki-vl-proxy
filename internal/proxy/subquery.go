package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// proxySubqueryRange evaluates a subquery for query_range requests.
// It runs the inner metric query at each sub-step within the subquery range,
// then aggregates the results using the outer function.
func (p *Proxy) proxySubqueryRange(w http.ResponseWriter, r *http.Request, outerFunc, innerQuery, rng, step string) {
	reqStart := r.FormValue("start")
	reqEnd := r.FormValue("end")
	reqStep := r.FormValue("step")

	endTS, err := parseTimestamp(reqEnd)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, "invalid end timestamp: "+err.Error())
		return
	}
	startTS, err := parseTimestamp(reqStart)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, "invalid start timestamp: "+err.Error())
		return
	}

	subRange := parseLokiDuration(rng)
	subStep := parseLokiDuration(step)
	if subRange == 0 || subStep == 0 {
		p.writeError(w, http.StatusBadRequest, "invalid subquery range or step")
		return
	}

	// For each point in [startTS, endTS] at the request step, evaluate the subquery
	outerStep := parseLokiDuration(formatVLStep(reqStep))
	if outerStep == 0 {
		outerStep = subStep // fallback
	}

	var resultSeries []subquerySeriesResult

	// Walk through the outer time range
	for t := startTS; !t.After(endTS); t = t.Add(outerStep) {
		// For this time point, evaluate inner query over [t-subRange, t] at subStep intervals
		values, seriesKey, err := p.evaluateSubqueryWindow(r.Context(), innerQuery, t.Add(-subRange), t, subStep, reqStep)
		if err != nil {
			p.log.Debug("subquery inner query error", "error", err)
			continue
		}

		// Apply outer aggregation
		for key, vals := range values {
			aggValue := subqueryAggregate(outerFunc, vals)
			resultSeries = appendSubquerySeries(resultSeries, key, seriesKey[key], t, aggValue)
		}
	}

	// Format as Loki matrix response
	result := formatSubqueryMatrixResult(resultSeries)
	w.Header().Set("Content-Type", "application/json")
	w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
}

// proxySubquery evaluates a subquery for instant query requests.
func (p *Proxy) proxySubquery(w http.ResponseWriter, r *http.Request, outerFunc, innerQuery, rng, step string) {
	reqTime := r.FormValue("time")
	endTS, err := parseTimestamp(reqTime)
	if err != nil {
		endTS = time.Now()
	}

	subRange := parseLokiDuration(rng)
	subStep := parseLokiDuration(step)
	if subRange == 0 || subStep == 0 {
		p.writeError(w, http.StatusBadRequest, "invalid subquery range or step")
		return
	}

	startTS := endTS.Add(-subRange)

	values, seriesKey, err := p.evaluateSubqueryWindow(r.Context(), innerQuery, startTS, endTS, subStep, step)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	// Build instant (vector) result
	var vectorResult []map[string]interface{}
	for key, vals := range values {
		aggValue := subqueryAggregate(outerFunc, vals)
		vectorResult = append(vectorResult, map[string]interface{}{
			"metric": seriesKey[key],
			"value":  []interface{}{float64(endTS.Unix()), strconv.FormatFloat(aggValue, 'f', -1, 64)},
		})
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     vectorResult,
		},
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
}

// evaluateSubqueryWindow runs the inner query at each sub-step within [start, end].
// Returns a map from series key → collected values, and series key → metric labels.
func (p *Proxy) evaluateSubqueryWindow(ctx context.Context, innerQuery string, start, end time.Time, subStep time.Duration, stepStr string) (map[string][]float64, map[string]map[string]string, error) {
	type subStepResult struct {
		ts   time.Time
		body []byte
		err  error
	}

	// Calculate sub-step time points
	var timePoints []time.Time
	for t := start; !t.After(end); t = t.Add(subStep) {
		timePoints = append(timePoints, t)
	}

	// Execute all sub-step queries concurrently (bounded)
	maxConcurrency := 10
	sem := make(chan struct{}, maxConcurrency)
	results := make([]subStepResult, len(timePoints))
	var wg sync.WaitGroup

	for i, tp := range timePoints {
		wg.Add(1)
		go func(idx int, t time.Time) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			body, err := p.executeSubqueryStepQuery(ctx, innerQuery, t)
			if err != nil {
				results[idx] = subStepResult{ts: t, err: err}
				return
			}
			results[idx] = subStepResult{ts: t, body: body}
		}(i, tp)
	}
	wg.Wait()

	// Collect values per series
	seriesValues := make(map[string][]float64)
	seriesLabels := make(map[string]map[string]string)

	for _, res := range results {
		if res.err != nil {
			continue
		}
		extractValuesFromStatsResult(res.body, seriesValues, seriesLabels)
	}

	return seriesValues, seriesLabels, nil
}

func (p *Proxy) executeSubqueryStepQuery(ctx context.Context, query string, ts time.Time) ([]byte, error) {
	// Support translated binary expressions inside subqueries
	// (for example rate()/bytes_rate() normalization wrappers).
	if op, leftQL, rightQL, _, ok := translator.ParseBinaryMetricExprFull(query); ok {
		leftScalar := translator.IsScalar(leftQL)
		rightScalar := translator.IsScalar(rightQL)

		fetchMetric := func(q string) ([]byte, error) {
			params := url.Values{}
			params.Set("query", q)
			params.Set("time", strconv.FormatInt(ts.Unix(), 10))
			resp, err := p.vlPost(ctx, "/select/logsql/stats_query", params)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			return body, nil
		}

		var (
			leftBody  []byte
			rightBody []byte
			leftErr   error
			rightErr  error
		)

		if !leftScalar && !rightScalar {
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				leftBody, leftErr = fetchMetric(leftQL)
			}()
			go func() {
				defer wg.Done()
				rightBody, rightErr = fetchMetric(rightQL)
			}()
			wg.Wait()
			if leftErr != nil {
				return nil, leftErr
			}
			if rightErr != nil {
				return nil, rightErr
			}
		} else {
			if !leftScalar {
				leftBody, leftErr = fetchMetric(leftQL)
				if leftErr != nil {
					return nil, leftErr
				}
			} else {
				leftBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + leftQL + `"]}}`)
			}

			if !rightScalar {
				rightBody, rightErr = fetchMetric(rightQL)
				if rightErr != nil {
					return nil, rightErr
				}
			} else {
				rightBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + rightQL + `"]}}`)
			}
		}

		return combineBinaryMetricResults(leftBody, rightBody, op, "vector", leftScalar, rightScalar, leftQL, rightQL), nil
	}

	params := url.Values{}
	params.Set("query", query)
	params.Set("time", strconv.FormatInt(ts.Unix(), 10))

	resp, err := p.vlPost(ctx, "/select/logsql/stats_query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// extractValuesFromStatsResult parses a VL stats_query response and adds values to the maps.
func extractValuesFromStatsResult(body []byte, values map[string][]float64, labels map[string]map[string]string) {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return
	}

	for _, series := range resp.Data.Result {
		key := seriesKeyFromMetric(series.Metric)
		if _, ok := labels[key]; !ok {
			labels[key] = series.Metric
		}

		if len(series.Value) >= 2 {
			val := parseValueToFloat(series.Value[1])
			values[key] = append(values[key], val)
		}
	}
}

func seriesKeyFromMetric(metric map[string]string) string {
	if len(metric) == 0 {
		return "{}"
	}
	// Build a deterministic key
	var parts []string
	for k, v := range metric {
		parts = append(parts, k+"="+v)
	}
	// Simple sort for determinism
	for i := 0; i < len(parts); i++ {
		for j := i + 1; j < len(parts); j++ {
			if parts[i] > parts[j] {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func parseValueToFloat(v interface{}) float64 {
	switch val := v.(type) {
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	case float64:
		return val
	case json.Number:
		f, _ := val.Float64()
		return f
	default:
		return 0
	}
}

// subqueryAggregate applies the outer aggregation function to collected values.
func subqueryAggregate(fn string, values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	switch fn {
	case "max_over_time":
		m := values[0]
		for _, v := range values[1:] {
			if v > m {
				m = v
			}
		}
		return m
	case "min_over_time":
		m := values[0]
		for _, v := range values[1:] {
			if v < m {
				m = v
			}
		}
		return m
	case "sum_over_time":
		var s float64
		for _, v := range values {
			s += v
		}
		return s
	case "avg_over_time":
		var s float64
		for _, v := range values {
			s += v
		}
		return s / float64(len(values))
	case "count_over_time":
		return float64(len(values))
	case "stddev_over_time":
		return stddev(values)
	case "stdvar_over_time":
		return variance(values)
	case "first_over_time":
		return values[0]
	case "last_over_time":
		return values[len(values)-1]
	default:
		return values[0]
	}
}

func variance(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	var sumSq float64
	for _, v := range values {
		d := v - mean
		sumSq += d * d
	}
	return sumSq / float64(len(values))
}

func stddev(values []float64) float64 {
	return math.Sqrt(variance(values))
}

type subquerySeriesResult struct {
	key    string
	metric map[string]string
	points [][]interface{} // [[timestamp, value], ...]
}

func appendSubquerySeries(series []subquerySeriesResult, key string, metric map[string]string, t time.Time, value float64) []subquerySeriesResult {
	for i := range series {
		if series[i].key == key {
			series[i].points = append(series[i].points, []interface{}{
				float64(t.Unix()), strconv.FormatFloat(value, 'f', -1, 64),
			})
			return series
		}
	}
	return append(series, subquerySeriesResult{
		key:    key,
		metric: metric,
		points: [][]interface{}{
			{float64(t.Unix()), strconv.FormatFloat(value, 'f', -1, 64)},
		},
	})
}

func formatSubqueryMatrixResult(series []subquerySeriesResult) []byte {
	var result []map[string]interface{}
	for _, s := range series {
		result = append(result, map[string]interface{}{
			"metric": s.metric,
			"values": s.points,
		})
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	resp, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	})
	return resp
}

// parseLokiDuration parses Loki/Prometheus-style duration strings like "5m", "1h", "30s", "1d".
func parseLokiDuration(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	// Try Go's time.ParseDuration first (handles "5m", "1h30m", "30s", etc.)
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}

	// Fall back to Prometheus/Loki units (d/w/y) and mixed-unit forms.
	if d, ok := parsePrometheusStyleDuration(s); ok {
		return d
	}

	// Handle "d" suffix (days) — not supported by Go
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(s[:len(s)-1])
		if err == nil {
			return time.Duration(n) * 24 * time.Hour
		}
	}

	// Try as seconds (numeric string)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second))
	}

	return 0
}

// parseTimestamp parses a Loki timestamp (Unix seconds, nanoseconds, or RFC3339).
func parseTimestamp(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Now(), nil
	}

	// Try as float (Unix seconds, possibly with fractional part)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		// If the number is very large, it's nanoseconds
		if sec > 1e15 {
			return time.Unix(0, int64(f)), nil
		}
		return time.Unix(sec, nsec), nil
	}

	// Try RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("unparseable timestamp: %q", s)
}
