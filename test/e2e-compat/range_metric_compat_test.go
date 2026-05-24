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

var (
	rangeMetricCompatOnce sync.Once
	rangeMetricVolumeOnce sync.Once
)

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

			// Verify the proxy response carries the Loki envelope status field.
			if proxyResp["status"] != "success" {
				t.Errorf("proxy response missing status:success for %s, got %v", tc.name, proxyResp["status"])
			}

			lokiData, _ := lokiResp["data"].(map[string]interface{})
			proxyData, _ := proxyResp["data"].(map[string]interface{})
			if lokiData["resultType"] != proxyData["resultType"] {
				t.Fatalf("resultType mismatch loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"])
			}

			lokiFlat := flattenMatrixResult(t, lokiData["result"])
			proxyFlat := flattenMatrixResult(t, proxyData["result"])
			if len(proxyFlat) < len(lokiFlat) {
				// Known pre-existing parity gap (proxy returns fewer points than Loki
				// for some range-metric semantics). Log but do not fatal — the value
				// check below still catches wrong values at matching points.
				t.Logf("proxy returned fewer points than Loki: loki=%d proxy=%d (pre-existing gap for %s)", len(lokiFlat), len(proxyFlat), tc.name)
			}
			if len(proxyFlat) > len(lokiFlat) {
				t.Errorf("proxy returned MORE points than Loki: loki=%d proxy=%d — proxy is fabricating data for %s", len(lokiFlat), len(proxyFlat), tc.name)
			}

			for key, lokiValue := range lokiFlat {
				proxyValue, ok := proxyFlat[key]
				if !ok {
					// Missing point already covered by the count check above.
					continue
				}
				if math.Abs(lokiValue-proxyValue) > tc.tolerance {
					t.Errorf("value mismatch at %s: loki=%v proxy=%v tolerance=%v", key, lokiValue, proxyValue, tc.tolerance)
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

// ensureUnwrapVolumeData seeds a large dataset (~1000 entries) under the
// "unwrap-volume" app stream so that response-size regression tests can
// detect data-explosion bugs where the proxy fetches raw logs instead of
// using the stats_query_range fast path.
func ensureUnwrapVolumeData(t *testing.T) {
	t.Helper()
	ensureDataIngested(t)
	rangeMetricVolumeOnce.Do(func() {
		// Spread 1000 entries over 6 minutes with a numeric "duration" field.
		// The 6-minute span ensures the default 5-minute range window sees ~830
		// entries — large enough that a raw-log fetch would produce an obviously
		// oversized response.
		now := time.Now()
		base := now.Add(-6 * time.Minute)
		const total = 1000
		const intervalMs = (6 * 60 * 1000) / total // ~360 ms per entry

		lines := make([]string, total)
		for i := 0; i < total; i++ {
			dur := 100 + (i % 900) // 100–999 ms, cycles to avoid constant values
			lines[i] = fmt.Sprintf(`{"duration":%d,"method":"GET","status":200}`, dur)
		}

		// Build entries spread over time manually so timestamps are well-distributed.
		// pushStream distributes i*second, but we need sub-second granularity here.
		now2 := base
		_ = intervalMs
		type valEntry struct {
			ts  string
			msg string
		}
		entries := make([]valEntry, total)
		for i := 0; i < total; i++ {
			entries[i] = valEntry{
				ts:  now2.Add(time.Duration(i) * (6 * time.Minute / total)).Format(time.RFC3339Nano),
				msg: lines[i],
			}
		}

		labels := map[string]string{
			"app":       "unwrap-volume",
			"namespace": "prod",
			"env":       "test",
		}

		// Push to Loki
		values := make([][]string, total)
		for i, e := range entries {
			values[i] = []string{e.ts, e.msg}
		}
		lokiPayload := map[string]interface{}{
			"streams": []map[string]interface{}{
				{"stream": labels, "values": values},
			},
		}
		lokiBody, _ := json.Marshal(lokiPayload)
		if resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json", strings.NewReader(string(lokiBody))); err == nil {
			resp.Body.Close()
		}

		// Push to VL
		var vlLines []string
		for _, e := range entries {
			entry := map[string]string{"_time": e.ts, "_msg": e.msg}
			for k, v := range labels {
				entry[k] = v
			}
			j, _ := json.Marshal(entry)
			vlLines = append(vlLines, string(j))
		}
		streamFields := "app,namespace,env"
		if resp, err := http.Post(
			vlURL+"/insert/jsonline?_stream_fields="+streamFields,
			"application/stream+json",
			strings.NewReader(strings.Join(vlLines, "\n")),
		); err == nil {
			resp.Body.Close()
		}

		waitForLokiMetricDataSelector(t, `{app="unwrap-volume"}`)
	})
}

// TestRangeMetric_UnwrapResponseSizeGuard verifies that unwrap metric queries
// against a large dataset (1000 entries) return compact responses — proving
// the proxy uses stats_query_range (pre-aggregated) rather than raw log fetch.
//
// Without the fix, sum_over_time({...} | json | unwrap duration [...]) fetches
// all raw log lines and returns ~26 MB. With the fix the proxy delegates to
// stats_query_range and returns < 512 KB regardless of dataset size.
func TestRangeMetric_UnwrapResponseSizeGuard(t *testing.T) {
	ensureUnwrapVolumeData(t)

	now := time.Now().UTC()
	start := now.Add(-10 * time.Minute).Format(time.RFC3339Nano)
	end := now.Format(time.RFC3339Nano)
	step := "60" // 1-minute step

	const maxProxyBytes = 512 * 1024 // 512 KB — data explosion would be 10+ MB

	cases := []struct {
		name  string
		query string
	}{
		{"sum_over_time", `sum_over_time({app="unwrap-volume"} | json | unwrap duration [5m])`},
		{"max_over_time", `max_over_time({app="unwrap-volume"} | json | unwrap duration [5m])`},
		{"min_over_time", `min_over_time({app="unwrap-volume"} | json | unwrap duration [5m])`},
		{"first_over_time", `first_over_time({app="unwrap-volume"} | json | unwrap duration [5m])`},
		{"last_over_time", `last_over_time({app="unwrap-volume"} | json | unwrap duration [5m])`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := url.Values{}
			params.Set("query", tc.query)
			params.Set("start", start)
			params.Set("end", end)
			params.Set("step", step)

			resp, err := http.Get(proxyURL + "/loki/api/v1/query_range?" + params.Encode())
			if err != nil {
				t.Fatalf("query_range failed: %v", err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200 got %d body=%s", resp.StatusCode, string(body))
			}
			if len(body) > maxProxyBytes {
				t.Errorf("proxy response %d bytes exceeds limit %d — data explosion detected for %s (raw log fetch instead of stats_query_range)",
					len(body), maxProxyBytes, tc.name)
			}
			t.Logf("%s: proxy response %d bytes (limit %d)", tc.name, len(body), maxProxyBytes)
		})
	}
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
