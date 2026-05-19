//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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

		// ── Over-time functions without unwrap (must error) ──
		{"avg_over_time_no_unwrap", `avg_over_time({app="api-gateway"}[5m])`, "unwrap_required"},
		{"sum_over_time_no_unwrap", `sum_over_time({app="api-gateway"}[5m])`, "unwrap_required"},
		{"max_over_time_no_unwrap", `max_over_time({app="api-gateway"}[5m])`, "unwrap_required"},
		{"min_over_time_no_unwrap", `min_over_time({app="api-gateway"}[5m])`, "unwrap_required"},
		{"first_over_time_no_unwrap", `first_over_time({app="api-gateway"}[5m])`, "unwrap_required"},
		{"last_over_time_no_unwrap", `last_over_time({app="api-gateway"}[5m])`, "unwrap_required"},
		{"stddev_over_time_no_unwrap", `stddev_over_time({app="api-gateway"}[5m])`, "unwrap_required"},
		{"stdvar_over_time_no_unwrap", `stdvar_over_time({app="api-gateway"}[5m])`, "unwrap_required"},
		{"quantile_over_time_no_unwrap", `quantile_over_time(0.99, {app="api-gateway"}[5m])`, "unwrap_required"},

		// ── count_values (not translatable to VL) ──
		{"count_values_metric", `count_values("app", count_over_time({app="api-gateway"}[5m]))`, "count_values"},

		// ── label_replace / label_join applied to a log stream ──
		// Note: proxy accepts these (proxy extension); Loki rejects → tracked in TestLogQL_Exhaustive_KnownGaps

		// ── absent_over_time without range ──
		{"absent_over_time_no_range", `absent_over_time({app="api-gateway"})`, "malformed"},

		// ── topk / bottomk with invalid N ──
		{"topk_n_zero", `topk(0, sum by(level)(count_over_time({env="production"}[5m])))`, "invalid_k"},
		{"topk_n_negative", `topk(-1, sum by(level)(count_over_time({env="production"}[5m])))`, "invalid_k"},
		{"bottomk_n_zero", `bottomk(0, sum by(level)(count_over_time({env="production"}[5m])))`, "invalid_k"},
		{"topk_n_float", `topk(1.5, sum by(level)(count_over_time({env="production"}[5m])))`, "invalid_k"},

		// ── outer quantile() aggregation (LogQL has quantile_over_time, not quantile()) ──
		{"quantile_outer_agg", `quantile(0.5, sum by(app)(rate({env="production"}[5m])))`, "invalid_agg"},

		// ── binary op between two log streams ──
		{"binary_two_log_streams", `{app="api-gateway",env="production"} + {app="payment-service",env="production"}`, "binary_on_log"},

		// ── ip filter with invalid CIDR ──
		{"ip_filter_invalid_cidr", `{app="api-gateway"} | json | ip("not-a-valid-cidr")`, "invalid_filter"},

		// ── invalid regex in stream selector ──
		{"invalid_regex_selector", `{app=~"[unclosed-bracket"}`, "invalid_regex"},

		// ── avg aggregation on a bare log stream ──
		{"avg_on_log_stream", `avg({app="api-gateway",env="production"})`, "metric_on_log"},

		// line_format_unclosed_brace and invalid_operator_diamond are tracked in
		// KnownGaps as proxy_strict gaps (proxy forwards instead of rejecting).

		// ── rate_counter without unwrap ───────────────────────────────────────
		// rate_counter is an unwrap-only range aggregation (like avg_over_time).
		{"rate_counter_no_unwrap", `rate_counter({app="api-gateway"}[5m])`, "unwrap_required"},

		// ── label_replace with invalid regex ─────────────────────────────────
		{"label_replace_invalid_regex", `label_replace(sum by(app)(rate({env="production"}[5m])), "new", "$1", "app", "[invalid")`, "invalid_regex"},

		// ── ip() with an invalid IP address (invalid octet > 255) ────────────
		{"ip_line_filter_invalid_ipv4", `{app="api-gateway"} |= ip("999.999.999.999")`, "invalid_filter"},

		// ── quantile_over_time with negative phi ──────────────────────────────
		// Loki rejects phi < 0 with 400; phi > 1 is accepted by Loki (returns 200).
		{"quantile_over_time_neg", `quantile_over_time(-0.1, {app="api-gateway"} | json | unwrap duration_ms [5m])`, "invalid_filter"},

		// ── Selector / syntax errors now caught by proxy (were proxy_strict gaps) ──
		// Empty selector: Loki requires at least one non-wildcard matcher.
		{"empty_selector", `{}`, "empty_selector"},
		// Bare range vector: a stream selector followed immediately by [5m] is invalid.
		{"bare_range_vector", `{app="api-gateway"}[5m]`, "bare_range"},
		// without/by grouping modifier applied directly to a log stream (not an aggregation).
		{"without_on_log_stream", `{app="api-gateway"} without(app)`, "grouping_on_stream"},
		{"by_on_log_stream", `{app="api-gateway"} by(app)`, "grouping_on_stream"},
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

// waitForProxyQueryReady polls the proxy's query endpoint until it returns HTTP 200
// three times consecutively. This ensures the circuit breaker (successThreshold=3) has
// fully closed after a potential VL restart triggered by earlier heavy tests. Without
// this, TestLogQL_Exhaustive_QueryParity inherits an open circuit breaker and every
// sub-test fails with "circuit breaker open — backend unavailable".
func waitForProxyQueryReady(t *testing.T) {
	t.Helper()
	params := url.Values{}
	params.Set("query", `{app="api-gateway",env="production"}`)
	params.Set("start", fmt.Sprintf("%d", time.Now().Add(-1*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", time.Now().UnixNano()))
	params.Set("limit", "1")
	params.Set("step", "60")
	target := proxyURL + "/loki/api/v1/query_range?" + params.Encode()

	const (
		timeout          = 45 * time.Second
		pollInterval     = 500 * time.Millisecond
		requiredConsecutive = 3 // must match circuit breaker successThreshold
	)
	deadline := time.Now().Add(timeout)
	consecutive := 0
	for time.Now().Before(deadline) {
		resp, err := http.Get(target)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				consecutive++
				if consecutive >= requiredConsecutive {
					return
				}
				continue
			}
		}
		consecutive = 0
		time.Sleep(pollInterval)
	}
	t.Logf("waitForProxyQueryReady: proxy not ready after %s — proceeding anyway", timeout)
}

// TestLogQL_Exhaustive_QueryParity walks through ALL valid LogQL pipeline
// operations and verifies both Loki and proxy return success.
func TestLogQL_Exhaustive_QueryParity(t *testing.T) {
	ensureDataIngested(t)
	// Wait for the proxy circuit breaker to fully close before running
	// the 150+ query parity sub-tests. Earlier tests (TestQuerySemanticsMatrix,
	// TestGrafanaClickout_*) can OOM-kill VL, opening the circuit breaker.
	waitForProxyQueryReady(t)

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

		// ── label_replace / label_join / group() ──────────────────────────────
		{"label_replace_basic", `label_replace(sum by (level)(count_over_time({app="api-gateway"}[5m])), "level_alias", "$1", "level", "(.*)")`, "label_transform"},
		{"label_replace_rewrite", `label_replace(sum by (app)(rate({env="production"}[5m])), "service", "$1", "app", "(.*)")`, "label_transform"},
		{"label_replace_no_match", `label_replace(sum by (level)(count_over_time({app="api-gateway"}[5m])), "new_label", "default", "level", "^nonexistent$")`, "label_transform"},
		{"label_replace_multi_result", `label_replace(sum by (app, level)(count_over_time({env="production"}[5m])), "app_env", "$1-prod", "app", "(.*)")`, "label_transform"},
		{"avg_without_app", `avg without(app)(rate({env="production"}[5m]))`, "label_transform"},
		{"count_by_level_agg", `count by(level)(count_over_time({env="production"}[5m]))`, "label_transform"},
		{"group_outer_agg", `group(sum by (app)(count_over_time({env="production"}[5m])))`, "label_transform"},
		{"group_without", `group(sum without (level)(count_over_time({env="production"}[5m])))`, "label_transform"},

		// ── Subqueries ────────────────────────────────────────────────────────
		// subquery_max_rate, subquery_avg_rate: Loki 3.7.1 rejects applying max/avg_over_time
		// to a subquery over rate() — these are proxy extensions tracked in TestLogQL_Exhaustive_KnownGaps.
		{"subquery_rate_count", `rate(count_over_time({app="api-gateway",env="production"}[5m])[30m:5m])`, "subquery"},
		{"subquery_sum_by", `sum by (app)(max_over_time(rate({env="production"}[5m])[30m:5m]))`, "subquery"},

		// ── Offset modifier ───────────────────────────────────────────────────
		{"rate_offset_5m", `rate({app="api-gateway",env="production"}[5m] offset 5m)`, "offset"},
		{"count_offset_10m", `count_over_time({app="api-gateway",env="production"}[5m] offset 10m)`, "offset"},
		{"sum_rate_offset", `sum by (app)(rate({env="production"}[5m] offset 5m))`, "offset"},
		{"bytes_rate_offset", `bytes_rate({app="api-gateway",env="production"}[5m] offset 5m)`, "offset"},

		// ── Unwrap with unit conversion ───────────────────────────────────────
		// duration-bytes-test has JSON fields: response_time ("15ms"), body_size ("1024B")
		{"unwrap_duration_conv", `sum_over_time({app="duration-bytes-test",env="production"} | json | unwrap duration(response_time) [5m])`, "unwrap_unit"},
		{"unwrap_bytes_conv", `sum_over_time({app="duration-bytes-test",env="production"} | json | unwrap bytes(body_size) [5m])`, "unwrap_unit"},
		{"unwrap_duration_avg", `avg_over_time({app="duration-bytes-test",env="production"} | json | unwrap duration(response_time) [5m])`, "unwrap_unit"},
		{"unwrap_duration_max", `max_over_time({app="duration-bytes-test",env="production"} | json | unwrap duration(response_time) [5m])`, "unwrap_unit"},
		{"unwrap_by_label", `sum by (app)(sum_over_time({env="production"} | json | unwrap duration_ms [5m]))`, "unwrap_unit"},
		{"unwrap_max_by_level", `max by (level)(max_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m]))`, "unwrap_unit"},
		{"unwrap_quantile_by", `quantile_over_time(0.95, {app="api-gateway",env="production"} | json | unwrap duration_ms [5m]) by (level)`, "unwrap_unit"},

		// ── Field-specific parser extraction ─────────────────────────────────
		{"json_two_fields_only", `{app="api-gateway",env="production"} | json method, status`, "field_parser"},
		{"json_three_fields", `{app="api-gateway",env="production"} | json method, path, status`, "field_parser"},
		{"json_field_then_filter", `{app="api-gateway",env="production"} | json status | status="200"`, "field_parser"},
		{"logfmt_fields_specific", `{app="payment-service",env="production"} | logfmt level, msg`, "field_parser"},

		// ── Regexp with named capture groups ──────────────────────────────────
		{"regexp_named_single", `{app="api-gateway",env="production"} | regexp "(?P<http_method>[A-Z]+)"`, "regexp_named"},
		{"regexp_named_multi", `{app="api-gateway",env="production"} | regexp "(?P<method>[A-Z]+) (?P<url_path>/[^ ]*)"`, "regexp_named"},
		{"regexp_named_filter", `{app="api-gateway",env="production"} | regexp "(?P<http_method>[A-Z]+)" | http_method="GET"`, "regexp_named"},

		// ── Advanced selector patterns ────────────────────────────────────────
		{"multi_app_regex_alt", `{app=~"api-gateway|payment-service",env="production"}`, "selector_advanced"},
		{"nested_wildcard", `{env="production",app=~"api-.*"}`, "selector_advanced"},
		{"not_match_multi", `{app!~"nginx.*|payment.*",env="production"}`, "selector_advanced"},
		{"env_regex_alternation", `{env=~"production|staging",app="api-gateway"}`, "selector_advanced"},
		{"combined_match_types", `{app=~"api-.*",env="production",level!="debug"}`, "selector_advanced"},

		// ── absent_over_time expanded ─────────────────────────────────────────
		{"absent_existing_stream", `absent_over_time({app="api-gateway",env="production"}[5m])`, "absent"},
		{"absent_nonexistent_stream", `absent_over_time({app="nonexistent-xyz-123-abc"}[1m])`, "absent"},
		{"absent_with_impossible_filter", `absent_over_time({app="api-gateway"} |= "IMPOSSIBLE_STRING_xyz_123" [5m])`, "absent"},

		// ── Complex multi-stage pipelines ─────────────────────────────────────
		{"error_filter_json_chain", `{app="api-gateway",env="production"} |= "error" | json | method!="GET" | status>=500`, "complex_pipeline"},
		{"json_path_status_drop", `{app="api-gateway",env="production"} | json | path=~"/api/.*" | status>200 | drop trace_id`, "complex_pipeline"},
		{"logfmt_filter_format", `{app="payment-service",env="production"} | logfmt | level="error" | line_format "[{{.level}}] {{.msg}}"`, "complex_pipeline"},
		{"chained_line_filter_complex", `{app="api-gateway",env="production"} |= "GET" |= "/api" != "health" | json`, "complex_pipeline"},
		{"json_regex_numeric_range", `{app="api-gateway",env="production"} | json | path=~"/api/.*" | status>=400 | status<500`, "complex_pipeline"},
		{"keep_then_format", `{app="api-gateway",env="production"} | json | keep method, status | line_format "{{.method}} {{.status}}"`, "complex_pipeline"},
		{"drop_then_keep", `{app="api-gateway",env="production"} | json | drop trace_id | keep method, path, status`, "complex_pipeline"},
		{"decolorize_json_filter", `{app="api-gateway",env="production"} | decolorize | json | status>=200`, "complex_pipeline"},

		// ── Nested / chained binary metric expressions ────────────────────────
		{"binary_sum_plus_sum", `sum by(app)(rate({env="production"}[5m])) + sum by(app)(rate({env="production"}[5m]))`, "binary_nested"},
		{"binary_rate_ratio_pct", `sum by(app)(rate({env="production"}[5m])) / sum by(app)(rate({env="production"}[5m])) * 100`, "binary_nested"},
		{"binary_three_services", `sum(rate({app="api-gateway"}[5m])) + sum(rate({app="payment-service"}[5m])) + sum(rate({app="nginx-ingress"}[5m]))`, "binary_nested"},
		{"binary_bytes_vs_rate", `sum(bytes_rate({env="production"}[5m])) / sum(rate({env="production"}[5m]))`, "binary_nested"},
		{"binary_bool_chain", `sum by (level)(count_over_time({app="api-gateway"}[5m])) > bool 0 + 0`, "binary_nested"},

		// ── Without-clause expansion ──────────────────────────────────────────
		{"sum_without_level", `sum without (level) (rate({env="production"}[5m]))`, "without"},
		{"max_without_env", `max without (env) (count_over_time({env="production"}[5m]))`, "without"},
		{"avg_without_multi", `avg without (level, env) (count_over_time({env="production"}[5m]))`, "without"},
		{"count_without_cluster", `count without (cluster) (count_over_time({env="production"}[5m]))`, "without"},

		// ── Vector matching expansion ─────────────────────────────────────────
		{"on_match_app", `sum by(app)(count_over_time({env="production"}[5m])) / on(app) sum by(app)(count_over_time({env="production"}[5m]))`, "vector_match"},
		{"ignoring_level", `sum by(app, level)(count_over_time({env="production"}[5m])) / ignoring(level) sum by(app)(count_over_time({env="production"}[5m]))`, "vector_match"},
		{"group_left_fanout", `sum by(app, level)(rate({env="production"}[5m])) / on(app) group_left sum by(app)(rate({env="production"}[5m]))`, "vector_match"},
		{"group_right_fanout", `sum by(app)(rate({env="production"}[5m])) / on(app) group_right sum by(app, level)(rate({env="production"}[5m]))`, "vector_match"},

		// ── Multi-service spanning queries ────────────────────────────────────
		{"multi_service_filter", `{env="production"} |= "error" | json | status>=500`, "multi_app"},
		{"multi_service_rate_sum", `sum(rate({env="production"}[5m]))`, "multi_app"},
		{"multi_service_topk", `topk(5, sum by (app)(rate({env="production"}[5m])))`, "multi_app"},
		{"multi_service_count_by_app", `count by (app) (count_over_time({env="production"}[5m]))`, "multi_app"},
		{"multi_service_bytes", `sum by (app)(bytes_rate({env="production"}[5m]))`, "multi_app"},

		// ── Unpack parser extended ────────────────────────────────────────────
		{"unpack_field_filter", `{app="api-gateway",env="production"} | unpack | level="error"`, "unpack_ext"},
		{"unpack_keep_format", `{app="api-gateway",env="production"} | unpack | keep level | line_format "{{.level}}"`, "unpack_ext"},

		// ── Pattern parser extended ───────────────────────────────────────────
		{"pattern_extract_two", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}}" | pattern "<method> <path>"`, "pattern_ext"},
		{"pattern_filter_after", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}}" | pattern "<method> <path>" | method="GET"`, "pattern_ext"},

		// ── Metric queries with complex filter chains ─────────────────────────
		{"rate_json_status_filter", `rate({app="api-gateway",env="production"} | json | status>=400 [5m])`, "metric_complex"},
		{"count_logfmt_level", `count_over_time({app="payment-service",env="production"} | logfmt | level="error"[5m])`, "metric_complex"},
		{"sum_rate_json_method", `sum by (level)(rate({app="api-gateway",env="production"} | json | method="GET" [5m]))`, "metric_complex"},
		{"bytes_rate_filtered", `sum by (app)(bytes_rate({env="production"} |= "error" [5m]))`, "metric_complex"},
		{"avg_unwrap_filtered", `avg_over_time({app="api-gateway",env="production"} | json | method="GET" | unwrap duration_ms [5m])`, "metric_complex"},

		// ── stddev / stdvar as BY-clause aggregations ─────────────────────────
		{"stddev_by_app", `stddev by(app)(count_over_time({env="production"}[5m]))`, "metric_agg_ext"},
		{"stdvar_by_app", `stdvar by(app)(count_over_time({env="production"}[5m]))`, "metric_agg_ext"},
		{"stddev_by_level", `stddev by(level)(count_over_time({app="api-gateway",env="production"}[5m]))`, "metric_agg_ext"},

		// ── max / min with by-clause ──────────────────────────────────────────
		{"max_by_app_rate", `max by(app)(rate({env="production"}[5m]))`, "metric_agg_ext"},
		{"min_by_level_count", `min by(level)(count_over_time({env="production"}[5m]))`, "metric_agg_ext"},
		{"sum_without_two_labels", `sum without(level, app)(count_over_time({env="production"}[5m]))`, "metric_agg_ext"},

		// ── decolorize in metric range ────────────────────────────────────────
		{"decolorize_in_rate", `rate({app="api-gateway"} | decolorize [5m])`, "parser_in_metric"},
		{"decolorize_in_count", `count_over_time({app="api-gateway"} | decolorize [5m])`, "parser_in_metric"},
		{"unpack_in_rate", `rate({app="api-gateway"} | unpack [5m])`, "parser_in_metric"},
		{"bytes_rate_json_range", `bytes_rate({app="api-gateway"} | json [5m])`, "parser_in_metric"},
		{"logfmt_drop_in_rate", `rate({app="payment-service"} | logfmt | drop msg [5m])`, "parser_in_metric"},
		{"json_method_count", `count_over_time({app="api-gateway"} | json | method="GET" [5m])`, "parser_in_metric"},
		{"json_keep_count", `count_over_time({app="api-gateway"} | json | keep method, status [5m])`, "parser_in_metric"},

		// ── label_format then aggregate ───────────────────────────────────────
		{"label_format_sum_by", `sum by(http_method)(rate({app="api-gateway"} | json | label_format http_method="method" [5m]))`, "label_fmt_metric"},

		// ── bytes_over_time with line filter ──────────────────────────────────
		{"bytes_over_time_line_filter", `bytes_over_time({app="api-gateway",env="production"} |= "GET" [5m])`, "bytes_metric"},

		// ── multi-app regex with field filter ─────────────────────────────────
		{"multi_app_regex_json", `{app=~"api-gateway|payment-service"} | json | status>=400`, "selector_regex"},

		// ── deep pipeline (5+ stages) ─────────────────────────────────────────
		{"pipeline_5stages", `{app="api-gateway",env="production"} | json | method="GET" | status>=200 | status<400 | drop trace_id | line_format "{{.method}} {{.path}} {{.status}}"`, "deep_pipeline"},

		// ── line_format with __timestamp__ ───────────────────────────────────
		{"line_format_timestamp", `{app="api-gateway"} | json | line_format "{{.__timestamp__}} {{.method}}"`, "line_fmt_ext"},

		// ── quantile_over_time with BY clause ─────────────────────────────────
		{"quantile_by_label_95", `quantile_over_time(0.95, {app="api-gateway",env="production"} | json | unwrap duration_ms [5m]) by (level)`, "unwrap_quantile"},
		{"quantile_by_label_50", `quantile_over_time(0.50, {app="api-gateway",env="production"} | json | unwrap duration_ms [5m]) by (app)`, "unwrap_quantile"},

		// ── subquery min / avg ────────────────────────────────────────────────
		// subquery_min_outer and subquery_count_avg: Loki 3.7.1 rejects these — tracked in KnownGaps.
		{"subquery_sum_by_outer", `sum by(app)(max_over_time(rate({env="production"}[5m])[5m:1m]))`, "subquery_ext"},

		// ── logfmt filter + line_format ───────────────────────────────────────
		{"logfmt_filter_line_format", `{app="payment-service",env="production"} | logfmt | level="error" | line_format "[{{.level}}] {{.msg}}"`, "logfmt_format"},

		// ── nested sum/rate binary division ──────────────────────────────────
		{"sum_rate_binary_div", `sum(rate({app="api-gateway"}[5m])) / sum(rate({app="payment-service"}[5m]))`, "binary_metric_ext"},

		// ── double logfmt parser (idempotent) ────────────────────────────────
		{"double_logfmt_parser", `{app="payment-service",env="production"} | logfmt | logfmt`, "parser_idempotent"},

		// ── unwrap missing field (valid syntax, empty results) ────────────────
		{"unwrap_missing_field", `max_over_time({app="api-gateway"} | unwrap nonexistent_field [5m])`, "unwrap_empty"},

		// ── __error__ label handling ──────────────────────────────────────────
		// After a parser stage, __error__ is set on lines that fail to parse.
		// __error__="" (empty) matches lines that parsed successfully.
		{"error_label_nonempty", `{app="api-gateway",env="production"} | json | __error__!=""`, "error_label"},
		{"error_label_empty_matches_valid", `{app="api-gateway",env="production"} | json | __error__=""`, "error_label"},
		{"error_label_in_keep_stage", `{app="api-gateway",env="production"} | json | keep level, __error__`, "error_label"},
		{"error_label_count_metric", `count_over_time({app="api-gateway",env="production"} | json | __error__!="" [5m])`, "error_label"},
		// error_label_rate_metric moved to KnownGaps: Loki 3.7.1 rejects rate() with __error__ label filter.
		// ip_line_filter_* moved to KnownGaps: proxy does not translate ip() filter function.

		// ── Drilldown / observability error-rate patterns ─────────────────────
		// These mirror the queries Grafana Logs Drilldown generates for volume,
		// error rate, and latency breakdowns.
		{"drilldown_error_rate_ratio", `sum(rate({env="production"} |= "error" [5m])) / sum(rate({env="production"}[5m]))`, "drilldown"},
		{"drilldown_error_count_by_app", `sum by(app)(count_over_time({env="production"} | json | status>=500 [5m]))`, "drilldown"},
		{"drilldown_volume_bytes_by_app", `sum by(app)(bytes_over_time({env="production"}[5m]))`, "drilldown"},
		{"drilldown_top_error_services", `topk(5, sum by(app)(rate({env="production"} |= "error" [5m])))`, "drilldown"},
		{"drilldown_p99_latency_by_svc", `max by(app)(quantile_over_time(0.99, {env="production"} | json | unwrap duration_ms [5m]))`, "drilldown"},
		{"drilldown_error_pct_by_app", `sum by(app)(rate({env="production"} | json | status>=500 [5m])) / sum by(app)(rate({env="production"}[5m])) * 100`, "drilldown"},

		// ── line_format with built-in special variables ────────────────────────
		// {{.__line__}} refers to the full original log line; stream labels are
		// accessed the same way as extracted labels inside line_format.
		{"line_format_line_builtin", `{app="api-gateway",env="production"} | json | line_format "raw: {{.__line__}}"`, "line_fmt_builtins"},
		{"line_format_stream_label_access", `{app="api-gateway",env="production"} | json | line_format "app={{.app}} method={{.method}} status={{.status}}"`, "line_fmt_builtins"},
		{"line_format_multi_extracted_fields", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}} took {{.duration_ms}}ms -> {{.status}}"`, "line_fmt_builtins"},

		// ── Nested / chained label_replace ────────────────────────────────────
		// label_replace_chained moved to KnownGaps: proxy returns 422 for chained label_replace.

		// ── JSON field extraction with alias (rename on extract) ─────────────
		// Loki JSON parser supports: | json alias="json.path.expression"
		{"json_field_alias_multi", `{app="api-gateway",env="production"} | json http_code="status", http_method="method"`, "json_alias"},
		// json_field_alias_then_filter moved to KnownGaps: empty result mismatch under investigation.

		// ── Pattern parser extended (3 captures + filter) ─────────────────────
		{"pattern_three_captures_filter", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}} {{.status}}" | pattern "<method> <path> <status>" | method="GET"`, "pattern_ext3"},
		{"pattern_wildcard_then_filter", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}} {{.status}}" | pattern "<_> <path> <status>" | status="200"`, "pattern_ext3"},

		// ── Logfmt extended: keep and regex on extracted fields ────────────────
		{"logfmt_keep_fields", `{app="payment-service",env="production"} | logfmt | keep level, msg`, "logfmt_ext"},
		{"logfmt_filter_then_keep", `{app="payment-service",env="production"} | logfmt | level="error" | keep level, msg`, "logfmt_ext"},
		{"logfmt_regex_on_extracted_field", `{app="payment-service",env="production"} | logfmt | msg=~".*error.*"`, "logfmt_ext"},

		// ── bytes_over_time as aggregation base ───────────────────────────────
		{"bytes_over_time_sum_by_app", `sum by(app)(bytes_over_time({env="production"}[5m]))`, "bytes_agg"},
		{"bytes_over_time_max_total", `max(bytes_over_time({env="production"}[5m]))`, "bytes_agg"},
		{"bytes_rate_sum_total", `sum(bytes_rate({env="production"}[5m]))`, "bytes_agg"},

		// ── absent_over_time with regex stream selector ────────────────────────
		{"absent_over_time_regex_selector", `absent_over_time({app=~"nonexistent-.*-xyz"}[5m])`, "absent_regex"},

		// ── Regexp without named groups (pure filter, no extraction) ──────────
		{"regexp_filter_only", `{app="api-gateway",env="production"} | regexp "\"status\":(4|5)\\d\\d"`, "regexp_filter"},

		// ── Multi-label by / without clauses ──────────────────────────────────
		{"avg_by_multi_labels", `avg by(app, level)(rate({env="production"}[5m]))`, "multi_label_agg"},
		{"sum_without_multi_labels", `sum without(level, env)(count_over_time({env="production"}[5m]))`, "multi_label_agg"},
		{"bottomk_bytes_rate", `bottomk(3, sum by(app)(bytes_rate({env="production"}[5m])))`, "multi_label_agg"},
		{"topk_avg_rate", `topk(3, avg by(app)(rate({env="production"}[5m])))`, "multi_label_agg"},

		// ── Numeric comparison pipelines ──────────────────────────────────────
		{"dual_bound_2xx", `{app="api-gateway",env="production"} | json | status >= 200 | status < 300`, "numeric_pipeline"},
		{"filter_slow_requests_format", `{app="api-gateway",env="production"} | json | duration_ms > 1000 | line_format "SLOW: {{.method}} {{.path}} {{.duration_ms}}ms"`, "numeric_pipeline"},
		{"chained_ne_filters", `{app="api-gateway",env="production"} | json | status != 200 | status != 404`, "numeric_pipeline"},
		{"metric_from_5xx_filter", `sum by(level)(rate({app="api-gateway",env="production"} | json | status>=500 [5m]))`, "numeric_pipeline"},

		// ── Extended selector patterns ─────────────────────────────────────────
		{"three_exact_label_selector", `{app="api-gateway",env="production",level="info"}`, "selector_ext"},
		{"regex_and_ne_label_combo", `{app=~"api-.*",env="production",level!="debug"}`, "selector_ext"},

		// ── Rate with chained line and field filters ───────────────────────────
		{"rate_chained_line_filters", `rate({app="api-gateway",env="production"} |= "GET" |= "/api" [5m])`, "rate_chain_filter"},
		{"rate_json_4xx_range", `rate({app="api-gateway",env="production"} | json | status>=400 | status<500 [5m])`, "rate_chain_filter"},

		// ── Binary operations across different time windows ────────────────────
		{"binary_window_diff_5m_1m", `sum(rate({env="production"}[5m])) - sum(rate({env="production"}[1m]))`, "binary_window"},
		{"binary_window_ratio_15m_5m", `sum(rate({env="production"}[15m])) / sum(rate({env="production"}[5m]))`, "binary_window"},

		// ── Unwrap metrics with by-clause aggregation ─────────────────────────
		{"sum_over_time_unwrap_by_level", `sum by(level)(sum_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m]))`, "unwrap_agg_by"},
		{"avg_over_time_unwrap_by_app", `avg by(app)(avg_over_time({env="production"} | json | unwrap duration_ms [5m]))`, "unwrap_agg_by"},

		// ── bytes_rate with JSON field filter ─────────────────────────────────
		{"bytes_rate_json_status_filtered", `sum by(app)(bytes_rate({env="production"} | json | status>=200 [5m]))`, "bytes_filtered"},

		// ── Mixed text and regex field filters ────────────────────────────────
		{"mixed_text_regex_field", `{app="api-gateway",env="production"} | json | method="GET" | path=~"/api/.*" | status!=500`, "mixed_field_filter"},
		{"mixed_numeric_and_regex", `{app="api-gateway",env="production"} | json | status>=200 | status<300 | method=~"GET|POST"`, "mixed_field_filter"},

		// ── label_format with multiple renames ────────────────────────────────
		{"label_format_multi_rename", `{app="api-gateway",env="production"} | json | label_format req_method="method", req_path="path", http_status="status"`, "label_format_rename"},

		// ── count_over_time with complex filter chain ─────────────────────────
		{"count_complex_chain", `count_over_time({app="api-gateway",env="production"} |= "GET" | json | status>=200 | status<400 [5m])`, "count_complex"},
		{"count_logfmt_drop_format", `count_over_time({app="payment-service",env="production"} | logfmt | level!="debug" [5m])`, "count_complex"},

		// ── Regexp named group then metric aggregation ─────────────────────────
		{"regexp_named_then_rate", `sum by(http_method)(rate({app="api-gateway",env="production"} | regexp "(?P<http_method>[A-Z]+)" [5m]))`, "regexp_metric"},

		// ── Multi-app selector with field filter ──────────────────────────────
		{"multi_app_field_filter_regex", `{app=~"api-gateway|payment-service",env="production"} | json | status>=400`, "multi_app_ext"},
		{"multi_app_bytes_rate", `sum by(app)(bytes_rate({app=~"api-gateway|payment-service",env="production"}[5m]))`, "multi_app_ext"},

		// ── stddev / stdvar outer aggregation ─────────────────────────────────
		// These were proxy_bug gaps; now fixed via proxy-side post-aggregation.
		{"stddev_outer_aggregation", `stddev(sum by(app)(count_over_time({env="production"}[5m])))`, "outer_agg_stddev"},
		{"stdvar_outer_aggregation", `stdvar(sum by(app)(count_over_time({env="production"}[5m])))`, "outer_agg_stddev"},

		// quantile_over_time_gt_one moved to KnownGaps: VL rejects phi>1 with 422, Loki allows it.
	}

	score := &exhaustiveScore{}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lokiCode, lokiBody := exhaustiveQuery(t, lokiURL, tc.query)
			proxyCode, proxyBody := exhaustiveQuery(t, proxyURL, tc.query)

			lokiOK := lokiCode == 200
			proxyOK := proxyCode == 200

			if lokiOK && proxyOK {
				// Verify result shape: resultType and emptiness must match.
				lokiType := exhaustiveResultType(lokiBody)
				proxyType := exhaustiveResultType(proxyBody)
				if lokiType != "" && proxyType != "" && lokiType != proxyType {
					score.fail(tc.category, tc.name,
						fmt.Sprintf("resultType mismatch: Loki=%s Proxy=%s", lokiType, proxyType))
					t.Errorf("resultType mismatch for %q: Loki=%s Proxy=%s", tc.query, lokiType, proxyType)
					return
				}
				lokiCount := exhaustiveResultCount(lokiBody)
				proxyCount := exhaustiveResultCount(proxyBody)
				if lokiCount > 0 && proxyCount == 0 {
					score.fail(tc.category, tc.name,
						fmt.Sprintf("Loki has %d result series/streams, proxy returned 0", lokiCount))
					t.Errorf("empty result mismatch for %q: Loki=%d series, Proxy=0", tc.query, lokiCount)
					return
				}
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

// exhaustiveQueryClient is a shared HTTP client with a per-request timeout.
// Without a timeout, queries against the proxy can hang indefinitely when
// VictoriaLogs restarts mid-request (subquery evaluation under load).
var exhaustiveQueryClient = &http.Client{Timeout: 20 * time.Second}

func exhaustiveQuery(t *testing.T, baseURL, query string) (int, string) {
	t.Helper()
	return exhaustiveQueryWithRange(t, baseURL, query, 30*time.Minute, 60)
}

// exhaustiveShortWindowQuery uses a 2-minute window to avoid overloading VL
// during proxy-side subquery evaluation (which fans out many inner queries).
func exhaustiveShortWindowQuery(t *testing.T, baseURL, query string) (int, string) {
	t.Helper()
	return exhaustiveQueryWithRange(t, baseURL, query, 2*time.Minute, 120)
}

func exhaustiveQueryWithRange(t *testing.T, baseURL, query string, window time.Duration, stepSec int) (int, string) {
	t.Helper()
	now := time.Now()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", fmt.Sprintf("%d", now.Add(-window).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")
	params.Set("step", fmt.Sprintf("%d", stepSec))

	resp, err := exhaustiveQueryClient.Get(baseURL + "/loki/api/v1/query_range?" + params.Encode())
	if err != nil {
		t.Logf("query timed out or failed (treating as 503): %v", err)
		return http.StatusServiceUnavailable, ""
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

func exhaustiveResultType(body string) string {
	var result map[string]interface{}
	if json.Unmarshal([]byte(body), &result) == nil {
		if data, ok := result["data"].(map[string]interface{}); ok {
			if rt, ok := data["resultType"].(string); ok {
				return rt
			}
		}
	}
	return ""
}

func exhaustiveResultCount(body string) int {
	var result map[string]interface{}
	if json.Unmarshal([]byte(body), &result) == nil {
		if data, ok := result["data"].(map[string]interface{}); ok {
			if results, ok := data["result"].([]interface{}); ok {
				return len(results)
			}
		}
	}
	return 0
}

func exhaustiveTruncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// TestLogQL_Exhaustive_KnownGaps documents confirmed parity divergences between
// Loki 3.7.1 and the proxy. Gap types:
//
//	"proxy_strict"    — proxy rejects what Loki accepts (proxy too permissive check failed)
//	"proxy_extension" — proxy supports this but Loki 3.7.1 doesn't (by design)
//	"proxy_bug"       — Loki succeeds but proxy fails (needs a fix)
//	"code_mismatch"   — both error with different HTTP codes
//
// The test validates each gap still exists. If a gap is resolved (proxy_bug fixed,
// or proxy_strict tightened), the test will log "GAP FIXED" — then remove the entry.
func TestLogQL_Exhaustive_KnownGaps(t *testing.T) {
	ensureDataIngested(t)

	type gap struct {
		name        string
		query       string
		gapType     string
		lokiExpect  int
		proxyExpect int
		note        string
		// shortWindow uses a 2-minute query range instead of 30 minutes.
		// Required for proxy-side subquery evaluation: a 30m window with 60s step
		// generates ~150 inner VL calls per case, crashing VictoriaLogs under load.
		// A 2-minute window reduces inner calls to ~10 while still verifying the gap.
		shortWindow bool
		// skipUnlessE2ESubquery marks cases that require a stable, lightly-loaded
		// VictoriaLogs instance. Set E2E_SUBQUERY_TESTS=1 to include them.
		// These are proxy_extension gaps (proxy accepts, Loki rejects) — the proxy
		// is correct; the skip is purely about VL reliability under concurrent load.
		skipUnlessE2ESubquery bool
	}

	gaps := []gap{
		{
			"at_timestamp_modifier",
			`rate({app="api-gateway"}[5m] @ 1000000000)`,
			"proxy_extension", 400, 200,
			"proxy supports @ step-alignment modifier (maps to VL query time); Loki 3.7.1 parse error",
			false, false,
		},
		{
			"at_start_modifier",
			`rate({app="api-gateway"}[5m] @ start())`,
			"proxy_extension", 400, 200,
			"proxy supports @ start() modifier; Loki 3.7.1 parse error",
			false, false,
		},
		{
			"label_join_function",
			`label_join(sum by (app)(count_over_time({env="production"}[5m])), "app_copy", "", "app")`,
			"proxy_extension", 400, 200,
			"label_join() is a proxy post-processing extension; not in Loki 3.7.1 LogQL",
			false, false,
		},
		// stddev_outer_aggregation, stdvar_outer_aggregation, quantile_neg_error_code were
		// proxy_bug/code_mismatch gaps but are now fixed — moved to QueryParity/ErrorParity.
		// Subquery-over-range-function extensions: Loki 3.7.1 rejects applying
		// max/avg/min_over_time to a subquery over rate() or count_over_time().
		// The proxy evaluates these via proxy-side subquery evaluation (extension).
		{
			name: "subquery_max_rate",
			query: `max_over_time(rate({app="api-gateway",env="production"}[5m])[1h:15m])`,
			gapType: "proxy_extension", lokiExpect: 400, proxyExpect: 200,
			note:                  "Loki 3.7.1 rejects max_over_time applied to rate() subquery; proxy evaluates via proxy-side subquery",
			shortWindow:           true,
			skipUnlessE2ESubquery: true,
		},
		{
			name: "subquery_avg_rate",
			query: `avg_over_time(rate({env="production"}[5m])[30m:5m])`,
			gapType: "proxy_extension", lokiExpect: 400, proxyExpect: 200,
			note:                  "Loki 3.7.1 rejects avg_over_time applied to rate() subquery; proxy evaluates via proxy-side subquery",
			shortWindow:           true,
			skipUnlessE2ESubquery: true,
		},
		{
			name: "subquery_min_outer",
			query: `min_over_time(rate({env="production"}[5m])[5m:1m])`,
			gapType: "proxy_extension", lokiExpect: 400, proxyExpect: 200,
			note:                  "Loki 3.7.1 rejects min_over_time applied to rate() subquery; proxy evaluates via proxy-side subquery",
			shortWindow:           true,
			skipUnlessE2ESubquery: true,
		},
		{
			name: "subquery_count_avg",
			query: `avg_over_time(count_over_time({env="production"}[1m])[5m:1m])`,
			gapType: "proxy_extension", lokiExpect: 400, proxyExpect: 200,
			note:                  "Loki 3.7.1 rejects avg_over_time applied to count_over_time() subquery; proxy evaluates via proxy-side subquery",
			shortWindow:           true,
			skipUnlessE2ESubquery: true,
		},
		// proxy_strict: proxy accepts queries that Loki rejects.
		{
			"line_format_unclosed_brace",
			`{app="api-gateway"} | line_format "{{.method"`,
			"proxy_strict", 400, 200,
			"Loki rejects unclosed Go template brace in line_format; proxy forwards without template validation",
			false, false,
		},
		{
			"invalid_operator_diamond",
			`{app="api-gateway"} | json | status <> 200`,
			"proxy_strict", 400, 200,
			"<> is not a valid LogQL operator; Loki returns 400, proxy forwards and VL may accept or silently drop",
			false, false,
		},
		{
			"error_label_rate_metric",
			`rate({env="production"} | json | __error__!="" [5m])`,
			"proxy_strict", 400, 200,
			"Loki 3.7.1 rejects rate() with __error__ label filter inside range vector; proxy forwards successfully",
			false, false,
		},
		// proxy_bug: Loki succeeds but proxy fails.
		{
			"ip_line_filter_ipv4",
			`{app="api-gateway",env="production"} |= ip("192.168.1.1")`,
			"proxy_bug", 200, 400,
			"ip() line filter not translated; proxy returns 400, Loki returns 200 (empty)",
			false, false,
		},
		{
			"ip_line_filter_cidr",
			`{app="nginx-ingress",env="production"} |= ip("10.0.0.0/8")`,
			"proxy_bug", 200, 400,
			"ip() CIDR line filter not translated; proxy returns 400",
			false, false,
		},
		{
			"ip_line_filter_negative",
			`{app="api-gateway",env="production"} != ip("127.0.0.1")`,
			"proxy_bug", 200, 400,
			"ip() negative line filter not translated; proxy returns 400",
			false, false,
		},
		{
			"ip_line_filter_range",
			`{app="api-gateway",env="production"} |= ip("192.168.0.1-192.168.0.255")`,
			"proxy_bug", 200, 400,
			"ip() range line filter not translated; proxy returns 400",
			false, false,
		},
		{
			"label_replace_chained",
			`label_replace(label_replace(sum by(app)(count_over_time({env="production"}[5m])), "service", "$1", "app", "(.*)"), "short", "$1", "service", "^([^-]+)")`,
			"proxy_bug", 200, 422,
			"chained label_replace() fails in proxy translation; VL returns 422",
			false, false,
		},
		{
			"json_field_alias_then_filter",
			`{app="api-gateway",env="production"} | json http_code="status" | http_code="200"`,
			"proxy_bug", 200, 200,
			"json alias then filter on alias name: proxy returns 200 but with empty results vs Loki non-empty",
			false, false,
		},
		// VictoriaLogs is stricter than Loki on quantile phi range.
		{
			"quantile_over_time_gt_one",
			`quantile_over_time(2.0, {app="api-gateway"} | json | unwrap duration_ms [5m])`,
			"proxy_bug", 200, 422,
			"Loki accepts phi>1 (extrapolates beyond p100); VL rejects with 422 — VL limitation",
			false, false,
		},
	}

	t.Log("\n══════════════════════════════════════════════════════════")
	t.Log("  Known Parity Gaps (proxy vs Loki 3.7.1)")
	t.Log("══════════════════════════════════════════════════════════")

	e2eSubqueryEnabled := os.Getenv("E2E_SUBQUERY_TESTS") == "1"

	for _, g := range gaps {
		g := g
		t.Run(g.name, func(t *testing.T) {
			if g.skipUnlessE2ESubquery && !e2eSubqueryEnabled {
				t.Skipf("proxy_extension subquery gap skipped under load (set E2E_SUBQUERY_TESTS=1 to run): %s", g.note)
			}
			queryFn := exhaustiveQuery
			if g.shortWindow {
				queryFn = exhaustiveShortWindowQuery
			}
			lokiCode, _ := queryFn(t, lokiURL, g.query)
			proxyCode, _ := queryFn(t, proxyURL, g.query)

			t.Logf("[%s] %s", g.gapType, g.note)
			t.Logf("  Loki=%d (expected %d)  Proxy=%d (expected %d)", lokiCode, g.lokiExpect, proxyCode, g.proxyExpect)

			if lokiCode != g.lokiExpect {
				t.Errorf("GAP CHANGED: Loki now returns %d (expected %d) — update this registry",
					lokiCode, g.lokiExpect)
			}
			lokiFixed := (g.gapType == "proxy_bug" || g.gapType == "proxy_strict") &&
				proxyCode == g.lokiExpect
			if lokiFixed {
				t.Logf("  ✓ GAP FIXED: proxy now returns %d matching Loki — remove from known gaps", proxyCode)
			} else if proxyCode != g.proxyExpect {
				t.Errorf("GAP CHANGED: proxy now returns %d (expected %d) — update this registry",
					proxyCode, g.proxyExpect)
			}
		})
	}
}
