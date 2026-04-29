//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var rangeMetricCompatOnce sync.Once

func containsUnwrapErrorText(body string) bool {
	body = strings.ToLower(strings.TrimSpace(body))
	return strings.Contains(body, "without unwrap") ||
		strings.Contains(body, "requires `| unwrap") ||
		strings.Contains(body, "requires | unwrap")
}

func ensureRangeMetricCompatData(t *testing.T) {
	t.Helper()
	ensureDataIngested(t)
	rangeMetricCompatOnce.Do(func() {
		now := time.Now()
		pushStream(t, now, streamDef{
			Labels: map[string]string{
				"app":       "range-metric-counter",
				"namespace": "prod",
				"cluster":   "us-east-1",
				"env":       "production",
			},
			Lines: []string{
				`{"counter":100,"latency":1}`,
				`{"counter":130,"latency":2}`,
				`{"counter":10,"latency":3}`,
				`{"counter":30,"latency":4}`,
			},
		})
		waitForLokiMetricDataSelector(t, `{app="range-metric-counter"}`)
	})
}

func TestRangeMetricCompatibilityMatrix(t *testing.T) {
	ensureRangeMetricCompatData(t)

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	end := now.Add(15 * time.Minute).Format(time.RFC3339Nano)

	tests := []struct {
		name      string
		query     string
		tolerance float64
	}{
		{name: "rate", query: `rate({app="api-gateway"}[5m])`, tolerance: 1e-9},
		{name: "count_over_time", query: `count_over_time({app="api-gateway"}[5m])`, tolerance: 1e-9},
		{name: "bytes_over_time", query: `bytes_over_time({app="api-gateway"}[5m])`, tolerance: 1e-9},
		{name: "bytes_rate", query: `bytes_rate({app="api-gateway"}[5m])`, tolerance: 1e-9},
		{name: "sum_over_time_unwrap", query: `sum_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "avg_over_time_unwrap", query: `avg_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "max_over_time_unwrap", query: `max_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "min_over_time_unwrap", query: `min_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "first_over_time_unwrap", query: `first_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "last_over_time_unwrap", query: `last_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "stddev_over_time_unwrap", query: `stddev_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "stdvar_over_time_unwrap", query: `stdvar_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "quantile_over_time_unwrap", query: `quantile_over_time(0.95, {app="api-gateway"} | json | unwrap duration_ms [5m])`, tolerance: 1e-9},
		{name: "rate_counter_unwrap", query: `rate_counter({app="range-metric-counter"} | json | unwrap counter [5m])`, tolerance: 1e-9},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, tc.query, start, end, "60")
			proxyResult := queryRangeResult(t, proxyURL, tc.query, start, end, "60")

			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status code mismatch loki=%d proxy=%d query=%s lokiBody=%s proxyBody=%s", lokiResult.StatusCode, proxyResult.StatusCode, tc.query, lokiResult.Body, proxyResult.Body)
			}
			if lokiResult.StatusCode != http.StatusOK {
				t.Fatalf("expected success from both backends for %s, loki=%s proxy=%s", tc.name, lokiResult.Body, proxyResult.Body)
			}
			lokiResp := lokiResult.JSON
			proxyResp := proxyResult.JSON

			lokiData, _ := lokiResp["data"].(map[string]interface{})
			proxyData, _ := proxyResp["data"].(map[string]interface{})
			if lokiData["resultType"] != proxyData["resultType"] {
				t.Fatalf("resultType mismatch loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"])
			}

			lokiFlat := flattenMatrixResult(t, lokiData["result"])
			proxyFlat := flattenMatrixResult(t, proxyData["result"])
			if len(lokiFlat) != len(proxyFlat) {
				t.Fatalf("series point count mismatch loki=%d proxy=%d", len(lokiFlat), len(proxyFlat))
			}

			for key, lokiValue := range lokiFlat {
				proxyValue, ok := proxyFlat[key]
				if !ok {
					t.Fatalf("proxy missing series point %s", key)
				}
				if math.Abs(lokiValue-proxyValue) > tc.tolerance {
					t.Fatalf("value mismatch at %s: loki=%v proxy=%v tolerance=%v", key, lokiValue, proxyValue, tc.tolerance)
				}
			}
		})
	}
}

func TestRangeMetricCompatibilityMissingUnwrapErrors(t *testing.T) {
	ensureRangeMetricCompatData(t)
	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	end := now.Add(15 * time.Minute).Format(time.RFC3339Nano)

	queries := []string{
		`sum_over_time({app="api-gateway"}[5m])`,
		`avg_over_time({app="api-gateway"}[5m])`,
		`max_over_time({app="api-gateway"}[5m])`,
		`min_over_time({app="api-gateway"}[5m])`,
		`stddev_over_time({app="api-gateway"}[5m])`,
		`stdvar_over_time({app="api-gateway"}[5m])`,
		`quantile_over_time(0.95, {app="api-gateway"}[5m])`,
		`rate_counter({app="range-metric-counter"}[5m])`,
		`first_over_time({app="api-gateway"}[5m])`,
		`last_over_time({app="api-gateway"}[5m])`,
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, query, start, end, "60")
			proxyResult := queryRangeResult(t, proxyURL, query, start, end, "60")

			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status code mismatch loki=%d proxy=%d query=%s loki=%s proxy=%s", lokiResult.StatusCode, proxyResult.StatusCode, query, lokiResult.Body, proxyResult.Body)
			}
			if proxyResult.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400 for missing unwrap query=%s proxy=%s", query, proxyResult.Body)
			}
			if !containsUnwrapErrorText(lokiResult.Body) {
				t.Fatalf("expected Loki unwrap error query=%s body=%s", query, lokiResult.Body)
			}
			if !containsUnwrapErrorText(proxyResult.Body) {
				t.Fatalf("expected proxy unwrap error query=%s body=%s", query, proxyResult.Body)
			}
		})
	}
}

type rangeQueryResult struct {
	StatusCode int
	Body       string
	JSON       map[string]interface{}
}

func queryRangeResult(t *testing.T, baseURL, query, start, end, step string) rangeQueryResult {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", start)
	params.Set("end", end)
	params.Set("step", step)
	resp, err := http.Get(baseURL + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		t.Fatalf("query_range request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var payload map[string]interface{}
	_ = json.Unmarshal(body, &payload)
	return rangeQueryResult{
		StatusCode: resp.StatusCode,
		Body:       string(body),
		JSON:       payload,
	}
}

func flattenMatrixResult(t *testing.T, raw interface{}) map[string]float64 {
	t.Helper()
	result, ok := raw.([]interface{})
	if !ok {
		t.Fatalf("expected matrix result array, got %T", raw)
	}

	flat := make(map[string]float64)
	for _, item := range result {
		series, ok := item.(map[string]interface{})
		if !ok {
			t.Fatalf("expected series object, got %T", item)
		}
		metricMap, _ := series["metric"].(map[string]interface{})
		metricKey := canonicalMetricKey(metricMap)
		values, _ := series["values"].([]interface{})
		for _, value := range values {
			pair, ok := value.([]interface{})
			if !ok || len(pair) < 2 {
				continue
			}
			ts := toInt64(pair[0])
			parsed, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprintf("%v", pair[1])), 64)
			if err != nil {
				t.Fatalf("failed to parse matrix value %v: %v", pair[1], err)
			}
			flat[fmt.Sprintf("%s@%d", metricKey, ts)] = parsed
		}
	}
	return flat
}

func canonicalMetricKey(metric map[string]interface{}) string {
	if len(metric) == 0 {
		return "{}"
	}
	parts := make([]string, 0, len(metric))
	for key, value := range metric {
		parts = append(parts, fmt.Sprintf("%s=%v", key, value))
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

func toInt64(value interface{}) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed
		}
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return parsed.Unix()
		}
		parsed, _ := strconv.ParseInt(v, 10, 64)
		return parsed
	default:
		parsed, _ := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
		return parsed
	}
}
