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

// TestCompat_CountValues pins that count_values is not LogQL: Loki v3.7.7 has
// no such vector aggregation (pkg/logql/syntax/lex.go:114-125, syntax.y:477-490
// and ast.go:1243-1254 list none), so its parser answers HTTP 400 "parse error at line L, col C:
// syntax error: unexpected IDENTIFIER" at the position of the name, on the
// instant and the range endpoint, whatever the arguments or grouping and
// wherever in an expression the name appears. The proxy must answer the same
// status and Loki's message in the bad_data envelope, and must not evaluate
// anything. The test fails, not skips, if Loki ever starts to answer 200: that
// means Loki added the function and the proxy has to implement it.
// conformance: semantics/count-values-unsupported
func TestCompat_CountValues(t *testing.T) {
	ensureDataIngested(t)
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
	ranged := func(query string) url.Values {
		return url.Values{"query": {query}, "step": {"60"}}
	}

	const selector = `{app=~".+"}`
	inner := `sum by (level) (count_over_time(` + selector + `[5m]))`

	// Both sides must serve real, non-empty data for the inner vector first;
	// otherwise a 400-vs-400 comparison proves nothing about the function.
	for name, base := range map[string]string{"loki": lokiURL, "proxy": proxyURL} {
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", withWindow(ranged(inner)), "0", nil)
		var matrix struct {
			Data struct {
				Result []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &matrix) != nil || len(matrix.Data.Result) == 0 {
			t.Fatalf("%s does not answer the inner vector with samples: %d %s", name, status, body)
		}
	}

	cases := []struct {
		name   string
		path   string
		params url.Values
	}{
		{"range", "/loki/api/v1/query_range", ranged(`count_values("v", ` + inner + `)`)},
		{"instant", "/loki/api/v1/query", instant(`count_values("v", ` + inner + `)`)},
		{"range_log_range_inner", "/loki/api/v1/query_range", ranged(`count_values("v", count_over_time(` + selector + `[5m]))`)},
		{"range_by_before_args", "/loki/api/v1/query_range", ranged(`count_values by (level) ("v", ` + inner + `)`)},
		{"range_without_before_args", "/loki/api/v1/query_range", ranged(`count_values without (level) ("v", ` + inner + `)`)},
		{"range_by_after_args", "/loki/api/v1/query_range", ranged(`count_values("v", ` + inner + `) by (level)`)},
		{"range_invalid_label_name", "/loki/api/v1/query_range", ranged(`count_values("1 bad", ` + inner + `)`)},
		{"range_no_arguments", "/loki/api/v1/query_range", ranged(`count_values()`)},
		{"range_nested_in_sum", "/loki/api/v1/query_range", ranged(`sum(count_values("v", ` + inner + `))`)},
		{"range_nested_in_topk", "/loki/api/v1/query_range", ranged(`topk(1, count_values("v", ` + inner + `))`)},
		{"range_binary_operand", "/loki/api/v1/query_range", ranged(inner + ` + count_values("v", ` + inner + `)`)},
		{"range_parenthesised", "/loki/api/v1/query_range", ranged(`(count_values("v", ` + inner + `))`)},
		{"range_second_line", "/loki/api/v1/query_range", ranged("sum(\n  count_values(\"v\", " + inner + "))")},
		{"instant_nested_in_sum", "/loki/api/v1/query", instant(`sum(count_values("v", ` + inner + `))`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, tc.path, withWindow(tc.params), "0", nil)
			lokiMsg := strings.TrimSpace(string(lokiBody))
			if lokiStatus == http.StatusOK {
				t.Fatalf("Loki now answers count_values (200 %s): the proxy has to implement it", lokiBody)
			}
			if lokiStatus != http.StatusBadRequest || !strings.HasPrefix(lokiMsg, "parse error") {
				t.Fatalf("Loki fixture drifted: want a 400 parse error, got %d %s", lokiStatus, lokiBody)
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

	// count_values stays an ordinary label name where Loki accepts it.
	for _, tc := range []struct {
		name   string
		params url.Values
	}{
		{"label_filter", url.Values{"query": {selector + ` | count_values="x"`}, "limit": {"10"}}},
		{"grouping_label", ranged(`sum by (count_values) (count_over_time(` + selector + `[5m]))`)},
	} {
		t.Run("accepts/"+tc.name, func(t *testing.T) {
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/query_range", withWindow(tc.params), "0", nil)
			if lokiStatus != http.StatusOK {
				t.Fatalf("Loki fixture drifted: want 200, got %d %s", lokiStatus, lokiBody)
			}
			proxyStatus, proxyBody := rejectedQueryGet(t, proxyURL, "/loki/api/v1/query_range", withWindow(tc.params), "0", nil)
			if proxyStatus != http.StatusOK {
				t.Fatalf("proxy rejected a query Loki accepts: %d %s", proxyStatus, proxyBody)
			}
		})
	}
}
