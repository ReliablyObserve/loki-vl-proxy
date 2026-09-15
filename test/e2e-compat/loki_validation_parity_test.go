//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCompat_LokiValidationParity pins queries Loki rejects with HTTP 400 while
// parsing or validating them, before any storage work: subqueries (LogQL has
// no [range:step] grammar), non-selector expressions on metadata endpoints,
// selectors whose every matcher matches the empty string, topk/bottomk with
// k <= 0, label_format/json/logfmt/pattern/regexp stages Loki cannot build, and
// endpoints that require a query. The proxy must answer each with the same
// status and Loki's message in the bad_data envelope. Near misses Loki accepts
// must stay 200 on both sides, including Drilldown-style {label=~".+"}
// selectors.
func TestCompat_LokiValidationParity(t *testing.T) {
	end := time.Now()
	start := end.Add(-time.Hour)
	withWindow := func(params url.Values) url.Values {
		out := cloneQueryValues(params)
		out.Set("start", strconv.FormatInt(start.UnixNano(), 10))
		out.Set("end", strconv.FormatInt(end.UnixNano(), 10))
		return out
	}
	instant := func(query string) url.Values {
		return url.Values{"query": {query}, "time": {strconv.FormatInt(end.UnixNano(), 10)}}
	}

	// Both sides must serve real data for a valid selector first; otherwise a
	// 400-vs-400 or 200-vs-200 comparison proves nothing.
	for name, base := range map[string]string{"loki": lokiURL, "proxy": proxyURL} {
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/labels", withWindow(url.Values{"query": {`{app=~".+"}`}}), "0", nil)
		var labels struct {
			Data []string `json:"data"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &labels) != nil || len(labels.Data) == 0 {
			t.Fatalf("%s is not serving data for a valid labels query: %d %s", name, status, body)
		}
	}

	const selector = `{app=~".+"}`
	rejected := []struct {
		name   string
		path   string
		params url.Values
	}{
		// Subqueries.
		{"subquery_sum_rate", "/loki/api/v1/query", instant(`max_over_time(sum(rate({app=~".+"}[1m]))[30m:5m])`)},
		{"subquery_rate", "/loki/api/v1/query", instant(`max_over_time(rate({app=~".+"}[1m])[30m:5m])`)},
		{"subquery_rate_range", "/loki/api/v1/query_range", url.Values{"query": {`max_over_time(rate({app=~".+"}[1m])[30m:5m])`}, "step": {"60"}}},
		{"subquery_count_log_range", "/loki/api/v1/query_range", url.Values{"query": {`count_over_time({app=~".+"}[30m:5m])`}, "step": {"60"}}},
		// loghttp.ParseRangeQuery checks the 11,000-point resolution limit
		// before the query is parsed: the resolution error wins.
		{"resolution_limit_precedes_subquery", "/loki/api/v1/query_range", url.Values{"query": {`max_over_time(rate({app=~".+"}[1m])[30m:5m])`}, "step": {"0.1"}}},
		{"resolution_limit_precedes_empty_selector", "/loki/api/v1/query_range", url.Values{"query": {`{app=""}`}, "step": {"0.1"}}},

		// Non-selector query/match[] on metadata endpoints.
		{"labels_line_filter", "/loki/api/v1/labels", url.Values{"query": {selector + ` |= "foo"`}}},
		{"labels_parser", "/loki/api/v1/labels", url.Values{"query": {selector + ` | logfmt`}}},
		{"series_line_filter", "/loki/api/v1/series", url.Values{"match[]": {selector + ` |= "foo"`}}},
		{"label_values_line_filter", "/loki/api/v1/label/app/values", url.Values{"query": {selector + ` |= "foo"`}}},
		{"index_stats_parser", "/loki/api/v1/index/stats", url.Values{"query": {selector + ` | logfmt`}}},
		{"volume_line_filter", "/loki/api/v1/index/volume", url.Values{"query": {selector + ` |= "foo"`}}},
		{"patterns_line_filter", "/loki/api/v1/patterns", url.Values{"query": {selector + ` |= "foo"`}, "step": {"60"}}},
		{"detected_labels_parser", "/loki/api/v1/detected_labels", url.Values{"query": {selector + ` | logfmt`}}},
		{"detected_fields_metric", "/loki/api/v1/detected_fields", url.Values{"query": {`count_over_time({app=~".+"}[5m])`}}},

		// Empty-compatible selectors.
		{"query_range_empty_eq", "/loki/api/v1/query_range", url.Values{"query": {`{app=""}`}, "limit": {"10"}}},
		{"query_range_empty_regex", "/loki/api/v1/query_range", url.Values{"query": {`{app=~""}`}, "limit": {"10"}}},
		{"query_range_match_all_regex", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".*"}`}, "limit": {"10"}}},
		{"query_range_not_equal_only", "/loki/api/v1/query_range", url.Values{"query": {`{app!="x"}`}, "limit": {"10"}}},
		{"metric_empty_selector", "/loki/api/v1/query", instant(`sum(count_over_time({app=""}[5m]))`)},
		{"labels_empty_eq", "/loki/api/v1/labels", url.Values{"query": {`{app=""}`}}},
		{"series_empty_regex", "/loki/api/v1/series", url.Values{"match[]": {`{app=~""}`}}},
		{"label_values_not_regex", "/loki/api/v1/label/app/values", url.Values{"query": {`{app!~"x"}`}}},
		{"index_stats_empty_braces", "/loki/api/v1/index/stats", url.Values{"query": {`{}`}}},
		{"volume_empty_eq", "/loki/api/v1/index/volume", url.Values{"query": {`{app=""}`}}},
		{"volume_range_match_all", "/loki/api/v1/index/volume_range", url.Values{"query": {`{app=~".*"}`}, "step": {"60"}}},
		{"patterns_empty_eq", "/loki/api/v1/patterns", url.Values{"query": {`{app=""}`}, "step": {"60"}}},
		{"detected_labels_empty_regex", "/loki/api/v1/detected_labels", url.Values{"query": {`{app=~""}`}}},
		{"detected_fields_match_all", "/loki/api/v1/detected_fields", url.Values{"query": {`{app=~".*"}`}}},
		{"detected_field_values_empty_eq", "/loki/api/v1/detected_field/app/values", url.Values{"query": {`{app=""}`}}},
		{"tail_empty_eq", "/loki/api/v1/tail", url.Values{"query": {`{app=""}`}}},

		// Missing query where Loki requires one.
		{"index_stats_no_query", "/loki/api/v1/index/stats", url.Values{}},
		{"volume_no_query", "/loki/api/v1/index/volume", url.Values{}},
		{"patterns_no_query", "/loki/api/v1/patterns", url.Values{"step": {"60"}}},
		{"detected_fields_no_query", "/loki/api/v1/detected_fields", url.Values{}},

		// topk / bottomk k <= 0.
		{"topk_zero", "/loki/api/v1/query", instant(`topk(0, count_over_time({app=~".+"}[5m]))`)},
		{"bottomk_zero", "/loki/api/v1/query", instant(`bottomk(0, count_over_time({app=~".+"}[5m]))`)},
		{"topk_zero_range", "/loki/api/v1/query_range", url.Values{"query": {`topk(0, count_over_time({app=~".+"}[5m]))`}, "step": {"300"}}},

		// Pipeline stages Loki cannot build.
		{"label_format_unclosed", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | label_format x="{{ .app"`}, "limit": {"10"}}},
		{"label_format_unknown_func", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | label_format x="{{ nofunc .app }}"`}, "limit": {"10"}}},
		{"label_format_duplicate", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | label_format x="a", x="b"`}, "limit": {"10"}}},
		{"json_unterminated_bracket", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | json foo="bar["`}, "limit": {"10"}}},
		{"json_unbalanced_index", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | json foo="a.b[0"`}, "limit": {"10"}}},
		{"json_double_dot_metric", "/loki/api/v1/query", instant(`count_over_time({app=~".+"} | json foo="a..b" [5m])`)},
		{"logfmt_bad_expression", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | logfmt foo="a.b"`}, "limit": {"10"}}},
		{"pattern_duplicate_capture", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | pattern "<a> <a>"`}, "limit": {"10"}}},
		{"regexp_duplicate_name", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | regexp "(?P<a>x)(?P<a>y)"`}, "limit": {"10"}}},
		// Loki keeps line_format in a count/rate range when a parser or line
		// filter follows it, so the invalid template is reported there.
		{"line_format_before_parser_metric", "/loki/api/v1/query", instant(`count_over_time({app=~".+"} | line_format "{{bad" | json [5m])`)},
		{"line_format_before_line_filter_metric", "/loki/api/v1/query", instant(`count_over_time({app=~".+"} | line_format "{{bad" |= "x" [5m])`)},

		// /series reads match as well as match[]; {} is "all series" only alone.
		{"series_match_param_empty_eq", "/loki/api/v1/series", url.Values{"match": {`{app=""}`}}},
		{"series_match_param_line_filter", "/loki/api/v1/series", url.Values{"match": {selector + ` |= "foo"`}}},
		{"series_empty_braces_with_group", "/loki/api/v1/series", url.Values{"match[]": {`{ }`, selector}}},

		// Loki's syntax.maxInputSize (128 KiB).
		{"labels_input_too_long", "/loki/api/v1/labels", url.Values{"query": {`{app="` + strings.Repeat("x", 131072) + `"}`}}},
	}
	for _, tc := range rejected {
		t.Run("rejects/"+tc.name, func(t *testing.T) {
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, tc.path, withWindow(tc.params), "0", nil)
			lokiMsg := strings.TrimSpace(string(lokiBody))
			if lokiStatus != http.StatusBadRequest {
				t.Fatalf("Loki fixture drifted: want 400, got %d %s", lokiStatus, lokiBody)
			}
			proxyStatus, proxyBody := rejectedQueryGet(t, proxyURL, tc.path, withWindow(tc.params), "0", nil)
			if proxyStatus != lokiStatus {
				t.Fatalf("status: proxy %d, Loki %d (%s); proxy body %s", proxyStatus, lokiStatus, lokiMsg, proxyBody)
			}
			var envelope struct {
				Status    string `json:"status"`
				ErrorType string `json:"errorType"`
				Error     string `json:"error"`
			}
			if err := json.Unmarshal(proxyBody, &envelope); err != nil {
				t.Fatalf("proxy error body is not JSON: %v %s", err, proxyBody)
			}
			if envelope.Status != "error" || envelope.ErrorType != "bad_data" || envelope.Error != lokiMsg {
				t.Fatalf("proxy error = %+v, want bad_data with Loki's message %q", envelope, lokiMsg)
			}
		})
	}

	accepted := []struct {
		name   string
		path   string
		params url.Values
	}{
		{"query_range_non_empty_regex", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+"}`}, "limit": {"10"}}},
		{"query_range_empty_plus_selective", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+", missing_label=""}`}, "limit": {"10"}}},
		{"query_range_match_all_plus_selective", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+", pod=~".*"}`}, "limit": {"10"}}},
		{"topk_one", "/loki/api/v1/query", instant(`topk(1, sum by (app) (count_over_time({app=~".+"}[5m])))`)},
		{"bottomk_two_range", "/loki/api/v1/query_range", url.Values{"query": {`bottomk(2, sum by (app) (count_over_time({app=~".+"}[5m])))`}, "step": {"300"}}},
		{"label_format_template", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | label_format svc="{{ .app | ToUpper }}"`}, "limit": {"10"}}},
		{"label_format_rename", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | label_format svc=app`}, "limit": {"10"}}},
		{"label_format_sprig", "/loki/api/v1/query_range", url.Values{"query": {selector + " | label_format short=`{{ .app | trunc 3 }}`"}, "limit": {"10"}}},
		{"json_valid_expressions", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | json first="a.b[0]", method, quoted="a[\"b\"]"`}, "limit": {"10"}}},
		{"logfmt_valid_expressions", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | logfmt lvl="level", msg`}, "limit": {"10"}}},
		{"labels_non_empty_regex", "/loki/api/v1/labels", url.Values{"query": {`{app=~".+"}`}}},
		{"series_empty_braces", "/loki/api/v1/series", url.Values{"match[]": {`{}`}}},
		{"series_duplicate_empty_braces", "/loki/api/v1/series", url.Values{"match[]": {`{}`, `{}`}}},
		{"series_match_and_match_brackets", "/loki/api/v1/series", url.Values{"match[]": {selector}, "match": {`{app=~".+", pod!=""}`}}},
		{"line_format_log_query", "/loki/api/v1/query_range", url.Values{"query": {selector + ` | line_format "{{ .app }}"`}, "limit": {"10"}}},
		{"line_format_dropped_from_count", "/loki/api/v1/query", instant(`count_over_time({app=~".+"} | line_format "{{bad" [5m])`)},
		{"volume_match_any", "/loki/api/v1/index/volume", url.Values{"query": {`{}`}}},
		{"detected_labels_drilldown_selector", "/loki/api/v1/detected_labels", url.Values{"query": {`{service_name=~".+"}`}}},
		{"detected_fields_pipeline", "/loki/api/v1/detected_fields", url.Values{"query": {selector + ` | logfmt`}}},
	}
	for _, tc := range accepted {
		t.Run("accepts/"+tc.name, func(t *testing.T) {
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, tc.path, withWindow(tc.params), "0", nil)
			if lokiStatus != http.StatusOK {
				t.Fatalf("Loki fixture drifted: want 200, got %d %s", lokiStatus, lokiBody)
			}
			proxyStatus, proxyBody := rejectedQueryGet(t, proxyURL, tc.path, withWindow(tc.params), "0", nil)
			if proxyStatus != http.StatusOK {
				t.Fatalf("proxy rejected a query Loki accepts: %d %s", proxyStatus, proxyBody)
			}
		})
	}
}
