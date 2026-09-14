package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	if subRange <= 0 || subStep <= 0 {
		p.writeError(w, http.StatusBadRequest, "invalid subquery range or step")
		return
	}

	// For each point in [startTS, endTS] at the request step, evaluate the subquery
	outerStep := parseLokiDuration(formatVLStep(reqStep))
	if outerStep <= 0 {
		outerStep = subStep // fallback
	}

	outerPoints, err := subqueryPointCount(startTS, endTS, outerStep)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	innerPoints, err := subqueryPointCount(endTS.Add(-subRange), endTS, subStep)
	if err != nil || outerPoints > maxSubqueryEvaluations/innerPoints {
		p.writeError(w, http.StatusBadRequest, "subquery exceeds total evaluation limit (10000)")
		return
	}
	var resultSamples int
	var resultSeries []subquerySeriesResult
	ctx := context.WithValue(r.Context(), subqueryBudgetKey{}, &subqueryBudget{})

	// Walk through the outer time range
	for t := startTS; !t.After(endTS); t = t.Add(outerStep) {
		// Stop if the client disconnected or the request context was cancelled.
		select {
		case <-ctx.Done():
			return
		default:
		}

		// For this time point, evaluate inner query over [t-subRange, t] at subStep intervals
		values, seriesKey, err := p.evaluateSubqueryWindow(ctx, innerQuery, t.Add(-subRange), t, subStep, reqStep)
		if err != nil {
			p.writeSubqueryError(w, err)
			return
		}

		// Apply outer aggregation
		resultSamples += len(values)
		if resultSamples > maxSubquerySamples {
			p.writeError(w, http.StatusBadRequest, "subquery exceeds sample limit")
			return
		}
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
	if subRange <= 0 || subStep <= 0 {
		p.writeError(w, http.StatusBadRequest, "invalid subquery range or step")
		return
	}

	startTS := endTS.Add(-subRange)

	values, seriesKey, err := p.evaluateSubqueryWindow(r.Context(), innerQuery, startTS, endTS, subStep, step)
	if err != nil {
		p.writeSubqueryError(w, err)
		return
	}

	// Build instant (vector) result
	vectorResult := make([]map[string]interface{}, 0, len(values))
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
	points, err := subqueryPointCount(start, end, subStep)
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	type stepResult struct {
		values map[string][]float64
		labels map[string]map[string]string
	}
	results := make([]stepResult, points)
	var next atomic.Int64
	budget, _ := ctx.Value(subqueryBudgetKey{}).(*subqueryBudget)
	if budget == nil {
		budget = &subqueryBudget{}
	}
	group, callCtx := errgroup.WithContext(ctx)
	for worker := 0; worker < min(points, 10); worker++ {
		group.Go(func() error {
			for {
				if err := callCtx.Err(); err != nil {
					return err
				}
				i := int(next.Add(1) - 1)
				if i >= points {
					return nil
				}
				body, err := p.executeSubqueryStepQuery(callCtx, innerQuery, start.Add(time.Duration(i)*subStep))
				if err != nil {
					return err
				}
				if budget.bytes.Add(int64(len(body))) > 64<<20 {
					return errSubqueryBudget
				}
				values, labels := make(map[string][]float64), make(map[string]map[string]string)
				if err := extractValuesFromStatsResult(body, values, labels); err != nil {
					return err
				}
				var count int64
				for _, samples := range values {
					count += int64(len(samples))
				}
				if budget.samples.Add(count) > maxSubquerySamples {
					return errSubqueryBudget
				}
				results[i] = stepResult{values, labels}
			}
		})
	}
	if err := group.Wait(); err != nil {
		return nil, nil, err
	}
	values, labels := make(map[string][]float64), make(map[string]map[string]string)
	for _, result := range results {
		for key, samples := range result.values {
			values[key] = append(values[key], samples...)
			labels[key] = result.labels[key]
		}
	}
	return values, labels, nil
}

const maxSubqueryEvaluations = 10000
const maxSubquerySamples = 1000000

type subqueryBudgetKey struct{}
type subqueryBudget struct{ bytes, samples atomic.Int64 }

var errSubqueryBudget = errors.New("subquery exceeds evaluation, sample or decoded-byte limit")

func subqueryPointCount(start, end time.Time, step time.Duration) (int, error) {
	if step <= 0 || end.Before(start) {
		return 0, fmt.Errorf("invalid subquery bounds or step")
	}
	distance := end.Sub(start)
	if distance == time.Duration(1<<63-1) || distance/step >= maxSubqueryEvaluations {
		return 0, errSubqueryBudget
	}
	return int(distance/step) + 1, nil
}
func (p *Proxy) writeSubqueryError(w http.ResponseWriter, err error) {
	status := statusFromUpstreamErr(err)
	if errors.Is(err, errSubqueryBudget) {
		status = http.StatusBadRequest
	}
	p.writeError(w, status, err.Error())
}

func (p *Proxy) readSubqueryResponse(resp *http.Response) ([]byte, error) {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("subquery", resp.StatusCode, body)
	}
	const limit = 8 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if len(body) > limit {
		return nil, errSubqueryBudget
	}
	return body, err
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
			body, err := p.readSubqueryResponse(resp)
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
	body, err := p.readSubqueryResponse(resp)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// extractValuesFromStatsResult parses a VL stats_query response and adds values to the maps.
func extractValuesFromStatsResult(body []byte, values map[string][]float64, labels map[string]map[string]string) error {
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
		return fmt.Errorf("invalid subquery response: %w", err)
	}
	if resp.Status != "success" || resp.Data.Result == nil {
		return fmt.Errorf("invalid subquery result status or array")
	}

	for _, series := range resp.Data.Result {
		key := seriesKeyFromMetric(series.Metric)
		if _, ok := labels[key]; !ok {
			labels[key] = series.Metric
		}

		if len(series.Value) != 2 {
			return fmt.Errorf("invalid subquery sample tuple")
		}
		if len(series.Value) == 2 {
			raw, ok := series.Value[1].(string)
			if !ok {
				return fmt.Errorf("invalid subquery sample value")
			}
			val, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return fmt.Errorf("invalid subquery sample number")
			}
			values[key] = append(values[key], val)
		}
	}
	return nil
}

func seriesKeyFromMetric(metric map[string]string) string {
	if len(metric) == 0 {
		return "{}"
	}
	data, _ := json.Marshal(metric)
	return string(data)
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
	if nanos, ok := parseFlexibleUnixNanos(s); ok {
		return time.Unix(0, nanos), nil
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp: %q", s)
}
