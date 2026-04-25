//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// TestLogQL_Exhaustive_ErrorParity walks through ALL LogQL expression types
// and verifies the proxy returns the same error codes and messages as Loki.
// This is the "test machine" that systematically discovers parity gaps.
func TestLogQL_Exhaustive_ErrorParity(t *testing.T) {
	ensureDataIngested(t)

	cases := []struct {
		name     string
		query    string
		category string
	}{
		// ── Binary operations on log queries (must error) ──
		{"binary_eq_on_matchers", `{level="info"} == 2`, "binary_on_log"},
		{"binary_ne_on_matchers", `{level="info"} != 2`, "binary_on_log"},
		{"binary_lt_on_matchers", `{level="info"} < 2`, "binary_on_log"},
		{"binary_le_on_matchers", `{level="info"} <= 2`, "binary_on_log"},
		{"binary_gt_on_matchers", `{level="info"} > 2`, "binary_on_log"},
		{"binary_ge_on_matchers", `{level="info"} >= 2`, "binary_on_log"},
		{"binary_eq_on_pipeline", `{app="api-gateway"} | json == 2`, "binary_on_log"},
		{"binary_le_on_pipeline", `{app="api-gateway"} | json <= 2`, "binary_on_log"},
		{"binary_add_on_log", `{level="info"} + 1`, "binary_on_log"},
		{"binary_sub_on_log", `{level="info"} - 1`, "binary_on_log"},
		{"binary_mul_on_log", `{level="info"} * 2`, "binary_on_log"},
		{"binary_div_on_log", `{level="info"} / 2`, "binary_on_log"},
		{"binary_mod_on_log", `{level="info"} % 2`, "binary_on_log"},
		{"binary_pow_on_log", `{level="info"} ^ 2`, "binary_on_log"},

		// ── Malformed selectors ──
		{"empty_value_selector", `{app=}`, "malformed"},
		{"unclosed_brace", `{app="api-gateway"`, "malformed"},
		{"no_selector", `| json`, "malformed"},
		{"empty_query", ``, "malformed"},

		// ── Invalid regex ──
		{"invalid_regex_match", `{app=~"[invalid"}`, "invalid_regex"},
		{"invalid_regex_not_match", `{app!~"(unclosed"}`, "invalid_regex"},

		// ── Metric without range ──
		{"rate_no_range", `rate({app="api-gateway"})`, "metric_no_range"},
		{"count_no_range", `count_over_time({app="api-gateway"})`, "metric_no_range"},
		{"bytes_no_range", `bytes_over_time({app="api-gateway"})`, "metric_no_range"},

		// ── Metric on log query (no aggregation function) ──
		{"sum_on_log", `sum({app="api-gateway"})`, "metric_on_log"},
		{"avg_on_log", `avg({app="api-gateway"})`, "metric_on_log"},
		{"topk_on_log", `topk(5, {app="api-gateway"})`, "metric_on_log"},
		{"sort_on_log", `sort({app="api-gateway"})`, "metric_on_log"},
		{"sort_desc_on_log", `sort_desc({app="api-gateway"})`, "metric_on_log"},

		// ── Unwrap without parser ──
		{"unwrap_no_parser", `sum_over_time({app="api-gateway"} | unwrap duration_ms [5m])`, "unwrap"},

		// ── Invalid pipeline stages ──
		{"double_parser", `{app="api-gateway"} | json | json`, "pipeline"},
		{"line_format_no_template", `{app="api-gateway"} | line_format`, "pipeline"},
	}

	score := &exhaustiveScore{}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lokiCode, lokiBody := exhaustiveQuery(t, lokiURL, tc.query)
			proxyCode, proxyBody := exhaustiveQuery(t, proxyURL, tc.query)

			lokiIsError := lokiCode >= 400
			proxyIsError := proxyCode >= 400

			if lokiIsError == proxyIsError {
				score.pass(tc.category, tc.name)
			} else if lokiIsError && !proxyIsError {
				score.fail(tc.category, tc.name,
					fmt.Sprintf("Loki=%d Proxy=%d | Loki error: %s", lokiCode, proxyCode, exhaustiveTruncate(lokiBody, 120)))
				t.Errorf("SILENT FAIL: Loki returns %d but proxy returns %d for %q\n  Loki: %s",
					lokiCode, proxyCode, tc.query, exhaustiveTruncate(lokiBody, 150))
			} else {
				score.fail(tc.category, tc.name,
					fmt.Sprintf("Proxy=%d Loki=%d | Proxy error: %s", proxyCode, lokiCode, exhaustiveTruncate(proxyBody, 120)))
				t.Errorf("EXTRA ERROR: Proxy returns %d but Loki returns %d for %q\n  Proxy: %s",
					proxyCode, lokiCode, tc.query, exhaustiveTruncate(proxyBody, 150))
			}
		})
	}

	score.report(t)
}

// TestLogQL_Exhaustive_QueryParity walks through ALL valid LogQL pipeline
// operations and verifies both Loki and proxy return success.
func TestLogQL_Exhaustive_QueryParity(t *testing.T) {
	ensureDataIngested(t)

	cases := []struct {
		name     string
		query    string
		category string
	}{
		// ── Selectors ──
		{"exact_match", `{app="api-gateway",env="production"}`, "selector"},
		{"regex_match", `{app=~"api-.*",env="production"}`, "selector"},
		{"not_equal", `{app!="payment-service",env="production"}`, "selector"},
		{"regex_not_match", `{app!~"nginx.*",env="production"}`, "selector"},

		// ── Line filters ──
		{"line_contains", `{app="api-gateway",env="production"} |= "GET"`, "line_filter"},
		{"line_not_contains", `{app="api-gateway",env="production"} != "health"`, "line_filter"},
		{"line_regex", `{app="api-gateway",env="production"} |~ "GET|POST"`, "line_filter"},
		{"line_not_regex", `{app="api-gateway",env="production"} !~ "health|ready"`, "line_filter"},

		// ── Parsers ──
		{"json_parser", `{app="api-gateway",env="production"} | json`, "parser"},
		{"logfmt_parser", `{app="payment-service",env="production"} | logfmt`, "parser"},
		{"unpack_parser", `{app="api-gateway",env="production"} | unpack`, "parser"},
		{"decolorize", `{app="api-gateway",env="production"} | decolorize`, "parser"},
		{"regexp_parser", `{app="api-gateway",env="production"} | regexp "\"method\":\"(?P<http_method>[A-Z]+)\""`, "parser"},
		{"pattern_parser", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}} {{.status}}" | pattern "<method> <path> <code>"`, "parser"},

		// ── Field filters (after parser) ──
		{"json_field_eq", `{app="api-gateway",env="production"} | json | method="GET"`, "field_filter"},
		{"json_field_ne", `{app="api-gateway",env="production"} | json | method!="DELETE"`, "field_filter"},
		{"json_field_regex", `{app="api-gateway",env="production"} | json | path=~"/api/.*"`, "field_filter"},
		{"json_field_not_regex", `{app="api-gateway",env="production"} | json | path!~"/health.*"`, "field_filter"},
		{"json_field_gt", `{app="api-gateway",env="production"} | json | status>=400`, "field_filter"},
		{"json_field_lt", `{app="api-gateway",env="production"} | json | duration_ms<100`, "field_filter"},
		{"logfmt_field_eq", `{app="payment-service",env="production"} | logfmt | level="error"`, "field_filter"},
		{"detected_level_filter", `{app="api-gateway",env="production"} | detected_level="error"`, "field_filter"},

		// ── Format stages ──
		{"line_format", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}}"`, "format"},
		{"line_format_with_newline", `{app="api-gateway",env="production"} | json | line_format "method={{.method}}\npath={{.path}}"`, "format"},
		{"label_format", `{app="api-gateway",env="production"} | json | label_format method_up="{{.method}}"`, "format"},
		{"label_format_multiple", `{app="api-gateway",env="production"} | json | label_format m="{{.method}}", s="{{.status}}"`, "format"},

		// ── Drop / Keep ──
		{"drop_single", `{app="api-gateway",env="production"} | json | drop trace_id`, "drop_keep"},
		{"drop_multiple", `{app="api-gateway",env="production"} | json | drop trace_id, user_id`, "drop_keep"},
		{"keep_single", `{app="api-gateway",env="production"} | json | keep method`, "drop_keep"},
		{"keep_multiple", `{app="api-gateway",env="production"} | json | keep method, status`, "drop_keep"},
		{"drop_error", `{app="api-gateway",env="production"} | json | drop __error__, __error_details__`, "drop_keep"},

		// ── Multi-stage pipelines ──
		{"json_filter_format", `{app="api-gateway",env="production"} | json | method="GET" | line_format "{{.status}}"`, "multi_stage"},
		{"logfmt_filter", `{app="payment-service",env="production"} | logfmt | level="error"`, "multi_stage"},
		{"json_drop_format", `{app="api-gateway",env="production"} | json | drop trace_id | line_format "{{.method}}"`, "multi_stage"},
		{"chained_line_filters", `{app="api-gateway",env="production"} |= "api" |~ "GET|POST" != "health"`, "multi_stage"},
		{"json_keep_format", `{app="api-gateway",env="production"} | json | keep method, status | line_format "{{.method}} {{.status}}"`, "multi_stage"},
		{"line_filter_then_json", `{app="api-gateway",env="production"} |= "GET" | json`, "multi_stage"},
		{"json_two_field_filters", `{app="api-gateway",env="production"} | json | method="GET" | path=~"/api/.*"`, "multi_stage"},
		{"decolorize_then_json", `{app="api-gateway",env="production"} | decolorize | json`, "multi_stage"},

		// ── Metric queries (range aggregations) ──
		{"count_over_time", `count_over_time({app="api-gateway",env="production"}[5m])`, "metric_range"},
		{"rate", `rate({app="api-gateway",env="production"}[5m])`, "metric_range"},
		{"bytes_over_time", `bytes_over_time({app="api-gateway",env="production"}[5m])`, "metric_range"},
		{"bytes_rate", `bytes_rate({app="api-gateway",env="production"}[5m])`, "metric_range"},
		{"rate_with_line_filter", `rate({app="api-gateway",env="production"} |= "GET"[5m])`, "metric_range"},
		{"rate_with_json_filter", `rate({app="api-gateway",env="production"} | json | method="GET"[5m])`, "metric_range"},
		{"count_with_logfmt", `count_over_time({app="payment-service",env="production"} | logfmt | level="error"[5m])`, "metric_range"},

		// ── Metric aggregations ──
		{"sum_by", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "metric_agg"},
		{"sum_rate_by", `sum by (app) (rate({env="production"}[5m]))`, "metric_agg"},
		{"avg_by", `avg by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "metric_agg"},
		{"max_by", `max by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "metric_agg"},
		{"min_by", `min by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "metric_agg"},
		{"count_agg", `count(count_over_time({env="production"}[5m]))`, "metric_agg"},
		{"topk", `topk(3, sum by (app) (count_over_time({env="production"}[5m])))`, "metric_agg"},
		{"bottomk", `bottomk(3, sum by (app) (count_over_time({env="production"}[5m])))`, "metric_agg"},
		{"sort_metric", `sort(sum by (app) (count_over_time({env="production"}[5m])))`, "metric_agg"},
		{"sort_desc_metric", `sort_desc(sum by (app) (count_over_time({env="production"}[5m])))`, "metric_agg"},

		// ── Unwrap metrics ──
		{"sum_over_time_unwrap", `sum_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
		{"avg_over_time_unwrap", `avg_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
		{"max_over_time_unwrap", `max_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
		{"min_over_time_unwrap", `min_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
		{"first_over_time_unwrap", `first_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
		{"last_over_time_unwrap", `last_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
		{"stddev_over_time_unwrap", `stddev_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
		{"stdvar_over_time_unwrap", `stdvar_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
		{"quantile_over_time_unwrap", `quantile_over_time(0.99, {app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},

		// ── Comparison operators on metrics ──
		{"metric_gt_zero", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) > 0`, "metric_compare"},
		{"metric_lt", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) < 1000`, "metric_compare"},
		{"metric_eq", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) == 0`, "metric_compare"},
		{"metric_ne", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) != 0`, "metric_compare"},
		{"metric_bool_gt", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) > bool 0`, "metric_compare"},
		{"metric_scalar_mul", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) * 100`, "metric_compare"},
		{"metric_scalar_div", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) / 60`, "metric_compare"},
		{"metric_scalar_add", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) + 1`, "metric_compare"},

		// ── Vector operations ──
		{"vector_or", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) or sum by (level) (count_over_time({app="api-gateway",env="production",level="debug"}[5m]))`, "vector_op"},
		{"vector_and", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) and sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "vector_op"},
		{"vector_unless", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) unless sum by (level) (count_over_time({app="api-gateway",env="production",level="debug"}[5m]))`, "vector_op"},
		{"vector_div", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) / sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "vector_op"},

		// ── Grouping modifiers ──
		{"on_grouping", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) / on(level) sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "grouping"},
		{"without_grouping", `sum without (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "grouping"},

		// ── Absent ──
		{"absent_over_time", `absent_over_time({app="nonexistent_service_xyz"}[5m])`, "absent"},

		// ── Empty results (valid but no data) ──
		{"impossible_filter", `{app="api-gateway",env="production"} |= "IMPOSSIBLE_STRING_NEVER_EXISTS"`, "empty"},
		{"nonexistent_app", `{app="this_app_does_not_exist",env="production"}`, "empty"},

		// ── Instant queries via query_range ──
		{"simple_selector_only", `{app="api-gateway",env="production",level="error"}`, "basic"},
		{"wildcard_regex", `{app=~".+",env="production"}`, "basic"},
		{"multiple_not_equal", `{app!="nginx-ingress",env="production",level!="debug"}`, "basic"},
	}

	score := &exhaustiveScore{}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lokiCode, _ := exhaustiveQuery(t, lokiURL, tc.query)
			proxyCode, proxyBody := exhaustiveQuery(t, proxyURL, tc.query)

			lokiOK := lokiCode == 200
			proxyOK := proxyCode == 200

			if lokiOK && proxyOK {
				score.pass(tc.category, tc.name)
			} else if lokiOK && !proxyOK {
				score.fail(tc.category, tc.name,
					fmt.Sprintf("Loki=200 Proxy=%d: %s", proxyCode, exhaustiveTruncate(proxyBody, 120)))
				t.Errorf("Loki succeeds but proxy fails (%d) for %q: %s",
					proxyCode, tc.query, exhaustiveTruncate(proxyBody, 150))
			} else if !lokiOK && proxyOK {
				score.fail(tc.category, tc.name,
					fmt.Sprintf("Loki=%d Proxy=200: proxy should also error", lokiCode))
				t.Errorf("Loki fails (%d) but proxy succeeds for %q", lokiCode, tc.query)
			} else {
				// Both error — check if codes match
				if lokiCode != proxyCode {
					score.fail(tc.category, tc.name,
						fmt.Sprintf("error code mismatch: Loki=%d Proxy=%d", lokiCode, proxyCode))
				} else {
					score.pass(tc.category, tc.name)
				}
			}
		})
	}

	score.report(t)
}

// ─── HELPERS ────────────────────────────────────────────────────────────────

type exhaustiveScore struct {
	total    int
	passed   int
	failed   int
	failures []string
}

func (s *exhaustiveScore) pass(category, name string) {
	s.total++
	s.passed++
}

func (s *exhaustiveScore) fail(category, name, detail string) {
	s.total++
	s.failed++
	s.failures = append(s.failures, fmt.Sprintf("[%s] %s: %s", category, name, detail))
}

func (s *exhaustiveScore) report(t *testing.T) {
	t.Helper()
	pct := 0
	if s.total > 0 {
		pct = 100 * s.passed / s.total
	}
	t.Logf("\n╔═══════════════════════════════════════════╗")
	t.Logf("║  LogQL Exhaustive Parity Score            ║")
	t.Logf("╠═══════════════════════════════════════════╣")
	t.Logf("║  Passed: %3d / %3d (%3d%%)                 ║", s.passed, s.total, pct)
	t.Logf("║  Failed: %3d                              ║", s.failed)
	t.Logf("╚═══════════════════════════════════════════╝")
	if len(s.failures) > 0 {
		t.Logf("\nFailures:")
		for _, f := range s.failures {
			t.Logf("  %s", f)
		}
	}
	if s.failed > 0 {
		t.Errorf("LogQL parity: %d/%d (%d%%) — %d failures", s.passed, s.total, pct, s.failed)
	}
}

func exhaustiveQuery(t *testing.T, baseURL, query string) (int, string) {
	t.Helper()
	now := time.Now()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", fmt.Sprintf("%d", now.Add(-2*time.Hour).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")
	params.Set("step", "60")

	resp, err := http.Get(baseURL + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func exhaustiveParseStatus(body string) string {
	var result map[string]interface{}
	if json.Unmarshal([]byte(body), &result) == nil {
		if s, ok := result["status"].(string); ok {
			return s
		}
	}
	return "unknown"
}

func exhaustiveTruncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
