//go:build e2e

package e2e_compat

import (
	"math"
	"net/http"
	"sync"
	"testing"
	"time"
)

var missingOpsDataOnce sync.Once

func ensureMissingOpsData(t *testing.T) {
	t.Helper()
	ensureDataIngested(t)
	missingOpsDataOnce.Do(func() {
		// The new test data streams (duration-bytes-test, pattern-filter-test,
		// unpack-test) are already ingested by ingestRichTestData. No additional
		// data needed here.
		t.Log("Missing ops test data verified")
	})
}

// ---------------------------------------------------------------------------
// offset directive
// ---------------------------------------------------------------------------

func TestMissingOps_OffsetDirective_Range(t *testing.T) {
	ensureMissingOpsData(t)

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-30 * time.Minute).Format(time.RFC3339Nano)
	end := now.Format(time.RFC3339Nano)

	tests := []struct {
		name  string
		query string
	}{
		{name: "rate_offset_1h", query: `rate({app="api-gateway"}[5m] offset 1h)`},
		{name: "count_over_time_offset_30m", query: `count_over_time({app="api-gateway"}[5m] offset 30m)`},
		{name: "sum_by_offset", query: `sum by (level) (count_over_time({app="api-gateway"}[5m] offset 1h))`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, tc.query, start, end, "60")
			proxyResult := queryRangeResult(t, proxyURL, tc.query, start, end, "60")

			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status code mismatch loki=%d proxy=%d query=%s\nloki=%s\nproxy=%s",
					lokiResult.StatusCode, proxyResult.StatusCode, tc.query, lokiResult.Body, proxyResult.Body)
			}

			// If Loki accepts offset, verify proxy does too and results are structurally similar
			if lokiResult.StatusCode == http.StatusOK {
				lokiData, _ := lokiResult.JSON["data"].(map[string]interface{})
				proxyData, _ := proxyResult.JSON["data"].(map[string]interface{})
				if lokiData["resultType"] != proxyData["resultType"] {
					t.Fatalf("resultType mismatch loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"])
				}
			}
		})
	}
}

func TestMissingOps_OffsetDirective_Instant(t *testing.T) {
	ensureMissingOpsData(t)

	at := time.Now().UTC()
	tests := []struct {
		name  string
		query string
	}{
		{name: "sum_count_offset", query: `sum(count_over_time({app="api-gateway"}[5m] offset 1h))`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiStatus, lokiBody, lokiResp := queryInstantGET(t, lokiURL, tc.query, at)
			proxyStatus, proxyBody, proxyResp := queryInstantGET(t, proxyURL, tc.query, at)

			if lokiStatus != proxyStatus {
				t.Fatalf("status mismatch loki=%d proxy=%d query=%s\nloki=%s\nproxy=%s",
					lokiStatus, proxyStatus, tc.query, lokiBody, proxyBody)
			}

			if lokiStatus == http.StatusOK && lokiResp != nil && proxyResp != nil {
				lokiData, _ := lokiResp["data"].(map[string]interface{})
				proxyData, _ := proxyResp["data"].(map[string]interface{})
				if lokiData["resultType"] != proxyData["resultType"] {
					t.Fatalf("resultType mismatch loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"])
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// unpack parser
// ---------------------------------------------------------------------------

func TestMissingOps_UnpackParser(t *testing.T) {
	ensureMissingOpsData(t)

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	end := now.Add(15 * time.Minute).Format(time.RFC3339Nano)

	tests := []struct {
		name  string
		query string
	}{
		{name: "unpack_filter", query: `{app="unpack-test"} | unpack | method="GET"`},
		{name: "unpack_status_filter", query: `{app="unpack-test"} | unpack | status >= 400`},
		{name: "unpack_no_filter", query: `{app="unpack-test"} | unpack`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, tc.query, start, end, "")
			proxyResult := queryRangeResult(t, proxyURL, tc.query, start, end, "")

			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status mismatch loki=%d proxy=%d query=%s\nloki=%s\nproxy=%s",
					lokiResult.StatusCode, proxyResult.StatusCode, tc.query, lokiResult.Body, proxyResult.Body)
			}

			if lokiResult.StatusCode == http.StatusOK {
				lokiLines := countResultLogLines(lokiResult.JSON)
				proxyLines := countResultLogLines(proxyResult.JSON)
				if lokiLines == 0 {
					t.Fatalf("expected Loki to return log lines for query=%s body=%s", tc.query, lokiResult.Body)
				}
				if lokiLines != proxyLines {
					t.Fatalf("line count mismatch loki=%d proxy=%d query=%s", lokiLines, proxyLines, tc.query)
				}
			}
		})
	}
}

func TestMissingOps_UnpackMetricQuery(t *testing.T) {
	ensureMissingOpsData(t)

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	end := now.Add(15 * time.Minute).Format(time.RFC3339Nano)

	tests := []struct {
		name  string
		query string
	}{
		{name: "count_over_time_unpack", query: `count_over_time({app="unpack-test"} | unpack | status >= 400 [5m])`},
		{name: "rate_unpack", query: `rate({app="unpack-test"} | unpack [5m])`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, tc.query, start, end, "60")
			proxyResult := queryRangeResult(t, proxyURL, tc.query, start, end, "60")

			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status mismatch loki=%d proxy=%d query=%s\nloki=%s\nproxy=%s",
					lokiResult.StatusCode, proxyResult.StatusCode, tc.query, lokiResult.Body, proxyResult.Body)
			}

			if lokiResult.StatusCode == http.StatusOK {
				lokiData, _ := lokiResult.JSON["data"].(map[string]interface{})
				proxyData, _ := proxyResult.JSON["data"].(map[string]interface{})
				if lokiData["resultType"] != proxyData["resultType"] {
					t.Fatalf("resultType mismatch loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"])
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// |> pattern match line filter
// ---------------------------------------------------------------------------

func TestMissingOps_PatternMatchLineFilter(t *testing.T) {
	ensureMissingOpsData(t)

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	end := now.Add(15 * time.Minute).Format(time.RFC3339Nano)

	tests := []struct {
		name  string
		query string
	}{
		{name: "include_pattern", query: `{app="pattern-filter-test"} |> "user=<_> action=login"`},
		{name: "exclude_pattern", query: `{app="pattern-filter-test"} !> "result=failure"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, tc.query, start, end, "")
			proxyResult := queryRangeResult(t, proxyURL, tc.query, start, end, "")

			// Both should agree on status — if Loki 3.7.1 doesn't support |>,
			// both should error; if it does, both should succeed.
			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status mismatch loki=%d proxy=%d query=%s\nloki=%s\nproxy=%s",
					lokiResult.StatusCode, proxyResult.StatusCode, tc.query, lokiResult.Body, proxyResult.Body)
			}

			if lokiResult.StatusCode == http.StatusOK {
				lokiLines := countResultLogLines(lokiResult.JSON)
				proxyLines := countResultLogLines(proxyResult.JSON)
				if lokiLines != proxyLines {
					t.Fatalf("line count mismatch loki=%d proxy=%d query=%s", lokiLines, proxyLines, tc.query)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// unwrap duration() / unwrap bytes()
// ---------------------------------------------------------------------------

func TestMissingOps_UnwrapDurationModifier(t *testing.T) {
	ensureMissingOpsData(t)

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	end := now.Add(15 * time.Minute).Format(time.RFC3339Nano)

	tests := []struct {
		name      string
		query     string
		tolerance float64
	}{
		{name: "avg_unwrap_duration", query: `avg_over_time({app="duration-bytes-test"} | json | unwrap duration(response_time) [5m])`, tolerance: 1e-6},
		{name: "max_unwrap_duration", query: `max_over_time({app="duration-bytes-test"} | json | unwrap duration(response_time) [5m])`, tolerance: 1e-6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, tc.query, start, end, "60")
			proxyResult := queryRangeResult(t, proxyURL, tc.query, start, end, "60")

			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status mismatch loki=%d proxy=%d query=%s\nloki=%s\nproxy=%s",
					lokiResult.StatusCode, proxyResult.StatusCode, tc.query, lokiResult.Body, proxyResult.Body)
			}

			if lokiResult.StatusCode == http.StatusOK {
				lokiData, _ := lokiResult.JSON["data"].(map[string]interface{})
				proxyData, _ := proxyResult.JSON["data"].(map[string]interface{})
				if lokiData["resultType"] != proxyData["resultType"] {
					t.Fatalf("resultType mismatch loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"])
				}
			}
		})
	}
}

func TestMissingOps_UnwrapBytesModifier(t *testing.T) {
	ensureMissingOpsData(t)

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	end := now.Add(15 * time.Minute).Format(time.RFC3339Nano)

	tests := []struct {
		name  string
		query string
	}{
		{name: "sum_unwrap_bytes", query: `sum_over_time({app="duration-bytes-test"} | json | unwrap bytes(body_size) [5m])`},
		{name: "avg_unwrap_bytes", query: `avg_over_time({app="duration-bytes-test"} | json | unwrap bytes(body_size) [5m])`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, tc.query, start, end, "60")
			proxyResult := queryRangeResult(t, proxyURL, tc.query, start, end, "60")

			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status mismatch loki=%d proxy=%d query=%s\nloki=%s\nproxy=%s",
					lokiResult.StatusCode, proxyResult.StatusCode, tc.query, lokiResult.Body, proxyResult.Body)
			}

			if lokiResult.StatusCode == http.StatusOK {
				lokiData, _ := lokiResult.JSON["data"].(map[string]interface{})
				proxyData, _ := proxyResult.JSON["data"].(map[string]interface{})
				if lokiData["resultType"] != proxyData["resultType"] {
					t.Fatalf("resultType mismatch loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"])
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// label_replace()
// ---------------------------------------------------------------------------

func TestMissingOps_LabelReplace(t *testing.T) {
	ensureMissingOpsData(t)

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	end := now.Add(15 * time.Minute).Format(time.RFC3339Nano)

	tests := []struct {
		name  string
		query string
	}{
		{name: "label_replace_basic", query: `label_replace(rate({app="api-gateway"}[5m]), "app_short", "$1", "app", "(.*)-.*")`},
		{name: "label_replace_sum", query: `sum by (app_short) (label_replace(rate({app="api-gateway"}[5m]), "app_short", "$1", "app", "(.*)-.*"))`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := queryRangeResult(t, lokiURL, tc.query, start, end, "60")
			proxyResult := queryRangeResult(t, proxyURL, tc.query, start, end, "60")

			if lokiResult.StatusCode != proxyResult.StatusCode {
				t.Fatalf("status mismatch loki=%d proxy=%d query=%s\nloki=%s\nproxy=%s",
					lokiResult.StatusCode, proxyResult.StatusCode, tc.query, lokiResult.Body, proxyResult.Body)
			}

			// If both succeed, verify structural parity
			if lokiResult.StatusCode == http.StatusOK {
				lokiData, _ := lokiResult.JSON["data"].(map[string]interface{})
				proxyData, _ := proxyResult.JSON["data"].(map[string]interface{})
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
					if math.Abs(lokiValue-proxyValue) > 1e-9 {
						t.Fatalf("value mismatch at %s: loki=%v proxy=%v", key, lokiValue, proxyValue)
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func countResultLogLines(resp map[string]interface{}) int {
	data, _ := resp["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	total := 0
	for _, item := range result {
		stream, _ := item.(map[string]interface{})
		values, _ := stream["values"].([]interface{})
		total += len(values)
	}
	return total
}

// Shared helpers reused from same package:
// - queryRangeResult() from range_metric_compat_test.go
// - flattenMatrixResult() from range_metric_compat_test.go
// - queryInstantGET() from operations_matrix_compat_test.go
