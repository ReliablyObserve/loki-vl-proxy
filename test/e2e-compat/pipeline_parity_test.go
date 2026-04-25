//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestPipeline_ErrorPropagation verifies that the proxy returns the same
// errors as Loki for invalid LogQL queries, rather than silently succeeding.
func TestPipeline_ErrorPropagation(t *testing.T) {
	ensureDataIngested(t)

	invalidQueries := []struct {
		name  string
		query string
	}{
		{"binary_op_on_pipeline", `{cluster="us-east-1"} | json <= 2`},
		{"missing_range_for_rate", `rate({app="api-gateway"})`},
		{"malformed_selector", `{app=}`},
		{"invalid_regex", `{app=~"[invalid"}`},
		{"metric_on_log_query", `sum({app="api-gateway"})`},
	}

	for _, tc := range invalidQueries {
		t.Run(tc.name, func(t *testing.T) {
			lokiStatus, lokiBody := pipelineQueryRaw(t, lokiURL, tc.query)
			proxyStatus, proxyBody := pipelineQueryRaw(t, proxyURL, tc.query)

			// Both should return error (4xx status or error in body)
			lokiIsError := lokiStatus >= 400 || strings.Contains(lokiBody, "error")
			proxyIsError := proxyStatus >= 400 || strings.Contains(proxyBody, "error")

			if lokiIsError && !proxyIsError {
				t.Errorf("Loki returns error (status=%d) but proxy succeeds (status=%d) for query %q\nLoki: %s\nProxy: %s",
					lokiStatus, proxyStatus, tc.query, pipelineTruncate(lokiBody, 200), pipelineTruncate(proxyBody, 200))
			}
		})
	}
}

// TestPipeline_ParserStages tests every parser stage for Loki vs proxy parity.
func TestPipeline_ParserStages(t *testing.T) {
	ensureDataIngested(t)

	parserTests := []struct {
		name  string
		query string
	}{
		// JSON parser
		{"json_basic", `{app="api-gateway",env="production"} | json`},
		{"json_with_field_filter", `{app="api-gateway",env="production"} | json | method="GET"`},
		{"json_with_numeric_filter", `{app="api-gateway",env="production"} | json | status>=400`},
		{"json_with_regex_filter", `{app="api-gateway",env="production"} | json | path=~"/api/v1/.*"`},

		// Logfmt parser
		{"logfmt_basic", `{app="payment-service",env="production"} | logfmt`},
		{"logfmt_with_field_filter", `{app="payment-service",env="production"} | logfmt | level="error"`},

		// Line format
		{"line_format_basic", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}} {{.status}}"`},

		// Label format
		{"label_format_basic", `{app="api-gateway",env="production"} | json | label_format method_upper="{{.method}}"`},

		// Drop / Keep
		{"drop_labels", `{app="api-gateway",env="production"} | json | drop trace_id, user_id`},
		{"keep_labels", `{app="api-gateway",env="production"} | json | keep method, path, status`},

		// Decolorize
		{"decolorize", `{app="api-gateway",env="production"} | decolorize`},

		// Unpack
		{"unpack_basic", `{app="api-gateway",env="production"} | unpack`},
	}

	for _, tc := range parserTests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := pipelineQueryLoki(t, tc.query)
			proxyResult := pipelineQueryProxy(t, tc.query)

			lokiOK := pipelineCheckStatus(lokiResult)
			proxyOK := pipelineCheckStatus(proxyResult)

			if lokiOK != proxyOK {
				t.Errorf("status mismatch: loki_ok=%v proxy_ok=%v for %q", lokiOK, proxyOK, tc.query)
				return
			}

			if !lokiOK {
				return // both errored, which is fine
			}

			lokiLines := pipelineCountLogLines(lokiResult)
			proxyLines := pipelineCountLogLines(proxyResult)

			if lokiLines == 0 && proxyLines == 0 {
				return // both empty, which is fine
			}

			// Allow some tolerance for line counts (VL indexing timing)
			if lokiLines > 0 && proxyLines == 0 {
				t.Errorf("loki returned %d lines but proxy returned 0 for %q", lokiLines, tc.query)
			}
			if proxyLines > 0 && lokiLines == 0 {
				t.Errorf("proxy returned %d lines but loki returned 0 for %q", proxyLines, tc.query)
			}
		})
	}
}

// TestPipeline_LineFilters tests all line filter operations.
func TestPipeline_LineFilters(t *testing.T) {
	ensureDataIngested(t)

	filterTests := []struct {
		name  string
		query string
	}{
		// Contains
		{"contains", `{app="api-gateway",env="production"} |= "GET"`},
		{"not_contains", `{app="api-gateway",env="production"} != "health"`},

		// Regex
		{"regex_match", `{app="api-gateway",env="production"} |~ "GET|POST"`},
		{"regex_not_match", `{app="api-gateway",env="production"} !~ "health|ready|metrics"`},

		// Chained filters
		{"chained_contains", `{app="api-gateway",env="production"} |= "api" |= "GET"`},
		{"chained_mixed", `{app="api-gateway",env="production"} |= "api" |~ "GET|POST" != "health"`},
		{"chained_regex", `{app="api-gateway",env="production"} |~ "status.*[45][0-9][0-9]"`},
	}

	for _, tc := range filterTests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := pipelineQueryLoki(t, tc.query)
			proxyResult := pipelineQueryProxy(t, tc.query)

			lokiOK := pipelineCheckStatus(lokiResult)
			proxyOK := pipelineCheckStatus(proxyResult)

			if lokiOK != proxyOK {
				t.Errorf("status mismatch: loki_ok=%v proxy_ok=%v for %q", lokiOK, proxyOK, tc.query)
				return
			}

			lokiLines := pipelineCountLogLines(lokiResult)
			proxyLines := pipelineCountLogLines(proxyResult)

			if lokiLines > 0 && proxyLines == 0 {
				t.Errorf("loki returned %d lines but proxy returned 0 for %q", lokiLines, tc.query)
			}
		})
	}
}

// TestPipeline_MultiStage tests multi-stage pipeline combinations.
func TestPipeline_MultiStage(t *testing.T) {
	ensureDataIngested(t)

	multiStageTests := []struct {
		name  string
		query string
	}{
		{"json_then_line_filter", `{app="api-gateway",env="production"} | json | method="GET" |= "users"`},
		{"json_then_line_format", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}}"`},
		{"json_then_label_format", `{app="api-gateway",env="production"} | json | label_format req="{{.method}} {{.path}}"`},
		{"logfmt_then_line_filter", `{app="payment-service",env="production"} | logfmt | level="error" |= "database"`},
		{"json_drop_then_format", `{app="api-gateway",env="production"} | json | drop trace_id | line_format "{{.method}} {{.status}}"`},
		{"json_keep_then_format", `{app="api-gateway",env="production"} | json | keep method, status | line_format "{{.method}} {{.status}}"`},
		{"parser_then_drop_error", `{app="api-gateway",env="production"} | json | drop __error__, __error_details__`},
	}

	for _, tc := range multiStageTests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := pipelineQueryLoki(t, tc.query)
			proxyResult := pipelineQueryProxy(t, tc.query)

			lokiOK := pipelineCheckStatus(lokiResult)
			proxyOK := pipelineCheckStatus(proxyResult)

			if lokiOK != proxyOK {
				t.Errorf("status mismatch: loki_ok=%v proxy_ok=%v for %q", lokiOK, proxyOK, tc.query)
			}
		})
	}
}

// TestPipeline_MetricQueries tests metric aggregation queries for parity.
func TestPipeline_MetricQueries(t *testing.T) {
	ensureDataIngested(t)

	metricTests := []struct {
		name  string
		query string
	}{
		{"count_over_time", `count_over_time({app="api-gateway",env="production"}[5m])`},
		{"rate", `rate({app="api-gateway",env="production"}[5m])`},
		{"bytes_over_time", `bytes_over_time({app="api-gateway",env="production"}[5m])`},
		{"bytes_rate", `bytes_rate({app="api-gateway",env="production"}[5m])`},
		{"sum_by_level", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`},
		{"rate_with_filter", `rate({app="api-gateway",env="production"} |= "GET"[5m])`},
		{"topk", `topk(3, sum by (app) (count_over_time({env="production"}[5m])))`},
		{"bottomk", `bottomk(3, sum by (app) (count_over_time({env="production"}[5m])))`},
	}

	for _, tc := range metricTests {
		t.Run(tc.name, func(t *testing.T) {
			lokiResult := pipelineQueryLoki(t, tc.query)
			proxyResult := pipelineQueryProxy(t, tc.query)

			lokiOK := pipelineCheckStatus(lokiResult)
			proxyOK := pipelineCheckStatus(proxyResult)

			if lokiOK != proxyOK {
				t.Errorf("status mismatch: loki_ok=%v proxy_ok=%v for %q", lokiOK, proxyOK, tc.query)
			}
		})
	}
}

// ─── HELPERS ────────────────────────────────────────────────────────────────

func pipelineQueryRaw(t *testing.T, baseURL, query string) (int, string) {
	t.Helper()
	now := time.Now()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", fmt.Sprintf("%d", now.Add(-2*time.Hour).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")

	resp, err := http.Get(baseURL + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer resp.Body.Close()

	var body strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			body.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	return resp.StatusCode, body.String()
}

func pipelineQueryLoki(t *testing.T, query string) map[string]interface{} {
	t.Helper()
	_, body := pipelineQueryRaw(t, lokiURL, query)
	var result map[string]interface{}
	json.Unmarshal([]byte(body), &result)
	return result
}

func pipelineQueryProxy(t *testing.T, query string) map[string]interface{} {
	t.Helper()
	_, body := pipelineQueryRaw(t, proxyURL, query)
	var result map[string]interface{}
	json.Unmarshal([]byte(body), &result)
	return result
}

func pipelineCheckStatus(result map[string]interface{}) bool {
	status, _ := result["status"].(string)
	return status == "success"
}

func pipelineCountLogLines(result map[string]interface{}) int {
	data, _ := result["data"].(map[string]interface{})
	streams, _ := data["result"].([]interface{})
	count := 0
	for _, s := range streams {
		stream, _ := s.(map[string]interface{})
		values, _ := stream["values"].([]interface{})
		count += len(values)
	}
	return count
}

func pipelineTruncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
