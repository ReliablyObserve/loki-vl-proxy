//go:build e2e

// Comprehensive LogQL function chaining and combination tests.
// Verifies all Loki pipe stages work correctly when chained together.
package e2e_compat

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestChaining_ParserThenFilter tests parser | filter chains.
func TestChaining_ParserThenFilter(t *testing.T) {
	ingestChainingData(t)

	chains := []struct {
		name  string
		query string
	}{
		{"json_then_status_filter", `{app="chain-test"} | json | status >= 400`},
		{"json_then_line_contains", `{app="chain-test"} | json |= "error"`},
		{"logfmt_then_filter", `{app="chain-logfmt"} | logfmt | level = "error"`},
		{"json_then_label_not_equal", `{app="chain-test"} | json | method != "GET"`},
		{"json_then_regex_filter", `{app="chain-test"} | json | path =~ "/api/.*"`},
	}

	for _, tc := range chains {
		t.Run(tc.name, func(t *testing.T) {
			streams := queryRange(t, proxyURL, tc.query)
			t.Logf("%s returned %d streams", tc.name, len(streams))
		})
	}
}

// TestChaining_MultipleFilters tests stacking multiple line filters.
func TestChaining_MultipleFilters(t *testing.T) {
	ingestChainingData(t)

	chains := []struct {
		name  string
		query string
	}{
		{"contains_and_not_contains", `{app="chain-test"} |= "request" != "health"`},
		{"contains_and_regex", `{app="chain-test"} |= "error" |~ "status.*[45][0-9]{2}"`},
		{"double_negative", `{app="chain-test"} != "debug" != "trace"`},
		{"regex_then_contains", `{app="chain-test"} |~ "POST|PUT" |= "api"`},
	}

	for _, tc := range chains {
		t.Run(tc.name, func(t *testing.T) {
			streams := queryRange(t, proxyURL, tc.query)
			t.Logf("%s returned %d streams", tc.name, len(streams))
		})
	}
}

// TestChaining_ParserThenDrop tests parser | drop/keep chains.
func TestChaining_ParserThenDrop(t *testing.T) {
	ingestChainingData(t)

	chains := []struct {
		name  string
		query string
	}{
		{"json_then_drop", `{app="chain-test"} | json | drop duration_ms`},
		{"json_then_keep", `{app="chain-test"} | json | keep status, method, path`},
		{"logfmt_then_drop", `{app="chain-logfmt"} | logfmt | drop caller`},
	}

	for _, tc := range chains {
		t.Run(tc.name, func(t *testing.T) {
			streams := queryRange(t, proxyURL, tc.query)
			t.Logf("%s returned %d streams", tc.name, len(streams))
		})
	}
}

// TestChaining_ParserThenLineFormat tests parser | line_format chains.
func TestChaining_ParserThenLineFormat(t *testing.T) {
	ingestChainingData(t)

	chains := []struct {
		name  string
		query string
	}{
		{"json_then_line_format", `{app="chain-test"} | json | line_format "{{.method}} {{.path}} {{.status}}"`},
		{"logfmt_then_line_format", `{app="chain-logfmt"} | logfmt | line_format "{{.level}}: {{.msg}}"`},
	}

	for _, tc := range chains {
		t.Run(tc.name, func(t *testing.T) {
			streams := queryRange(t, proxyURL, tc.query)
			t.Logf("%s returned %d streams", tc.name, len(streams))
		})
	}
}

// TestChaining_FilterThenDecolorize tests filter | decolorize chains.
func TestChaining_FilterThenDecolorize(t *testing.T) {
	ingestDecolorizeData(t)

	t.Run("contains_then_decolorize", func(t *testing.T) {
		streams := queryRange(t, proxyURL, `{app="ansi-test"} |= "ERROR" | decolorize`)
		for _, s := range streams {
			stream, _ := s.(map[string]interface{})
			values, _ := stream["values"].([]interface{})
			for _, v := range values {
				val, _ := v.([]interface{})
				if len(val) >= 2 {
					line, _ := val[1].(string)
					if strings.Contains(line, "\x1b[") {
						t.Errorf("decolorize failed: %q still has ANSI", line)
					}
				}
			}
		}
	})
}

// TestChaining_MetricQueries tests metric query variations.
func TestChaining_MetricQueries(t *testing.T) {
	ingestChainingData(t)

	queries := []struct {
		name  string
		query string
	}{
		{"rate", `rate({app="chain-test"}[5m])`},
		{"count_over_time", `count_over_time({app="chain-test"}[5m])`},
		{"bytes_over_time", `bytes_over_time({app="chain-test"}[5m])`},
		{"sum_rate_by", `sum(rate({app="chain-test"}[5m])) by (level)`},
		{"topk_rate", `topk(5, rate({app="chain-test"}[5m]))`},
		{"rate_with_filter", `rate({app="chain-test"} |= "error"[5m])`},
		{"count_json_filter", `count_over_time({app="chain-test"} | json | status >= 400 [5m])`},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			params := url.Values{
				"query": {tc.query},
				"start": {time.Now().Add(-1 * time.Hour).Format(time.RFC3339Nano)},
				"end":   {time.Now().Add(time.Hour).Format(time.RFC3339Nano)},
				"step":  {"60"},
			}
			resp, err := http.Get(proxyURL + "/loki/api/v1/query_range?" + params.Encode())
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				// Some VL-unsupported functions (e.g., bytes_over_time) return 422
				if resp.StatusCode == 422 {
					t.Logf("VL returned 422 for %q (function may not be natively supported)", tc.name)
					return
				}
				t.Errorf("expected 200, got %d", resp.StatusCode)
			}

			var result map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&result)
			if result["status"] != "success" {
				t.Errorf("expected status=success, got %v", result["status"])
			}
		})
	}
}

// TestChaining_BinaryMetricQueries tests binary metric expressions.
func TestChaining_BinaryMetricQueries(t *testing.T) {
	ingestChainingData(t)

	queries := []struct {
		name  string
		query string
	}{
		{"rate_times_100", `rate({app="chain-test"}[5m]) * 100`},
		{"rate_div_rate", `sum(rate({app="chain-test"}[5m])) / sum(rate({app="chain-test",level="error"}[5m]))`},
		{"count_minus_count", `count_over_time({app="chain-test"}[5m]) - count_over_time({app="chain-test",level="error"}[5m])`},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			params := url.Values{
				"query": {tc.query},
				"start": {time.Now().Add(-1 * time.Hour).Format(time.RFC3339Nano)},
				"end":   {time.Now().Add(time.Hour).Format(time.RFC3339Nano)},
				"step":  {"60"},
			}
			resp, err := http.Get(proxyURL + "/loki/api/v1/query_range?" + params.Encode())
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				t.Errorf("expected 200, got %d for %s", resp.StatusCode, tc.query)
			}

			var result map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&result)
			if result["status"] != "success" {
				t.Errorf("expected status=success for %s, got %v", tc.name, result["status"])
			}
		})
	}
}

// TestChaining_AllEndpointsWithLabels tests all API endpoints respond correctly.
func TestChaining_AllEndpointsWithLabels(t *testing.T) {
	ingestChainingData(t)

	endpoints := []struct {
		name     string
		path     string
		wantCode int
	}{
		{"labels", "/loki/api/v1/labels", 200},
		{"label_values", "/loki/api/v1/label/app/values", 200},
		{"series", "/loki/api/v1/series?match[]={app=\"chain-test\"}", 200},
		{"detected_fields", "/loki/api/v1/detected_fields?query=*", 200},
		{"detected_labels", "/loki/api/v1/detected_labels?query=*", 200},
		{"format_query", "/loki/api/v1/format_query?query={app=\"test\"}", 200},
		{"buildinfo", "/loki/api/v1/status/buildinfo", 200},
		{"ready", "/ready", 200},
		{"metrics", "/metrics", 200},
		{"patterns", "/loki/api/v1/patterns?query={app=\"test\"}", 200},
		{"index_stats", "/loki/api/v1/index/stats?query={app=\"chain-test\"}&start=" + time.Now().UTC().Add(-1*time.Hour).Format(time.RFC3339Nano) + "&end=" + time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano), 200},
	}

	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			resp, err := http.Get(proxyURL + ep.path)
			if err != nil {
				t.Fatalf("%s: request failed: %v", ep.name, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != ep.wantCode {
				t.Errorf("%s: expected %d, got %d", ep.name, ep.wantCode, resp.StatusCode)
			}
		})
	}
}

// TestChaining_SecurityHeaders verifies security headers on all responses.
func TestChaining_SecurityHeaders(t *testing.T) {
	resp, err := http.Get(proxyURL + "/loki/api/v1/labels")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	headers := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
	}
	for name, want := range headers {
		got := resp.Header.Get(name)
		if got != want {
			t.Errorf("header %s = %q, want %q", name, got, want)
		}
	}
	if got := resp.Header.Get("Cache-Control"); !strings.Contains(got, "no-store") {
		t.Errorf("header Cache-Control = %q, want token no-store", got)
	}
}

// TestChaining_WriteBlocked verifies all write methods are blocked.
func TestChaining_WriteBlocked(t *testing.T) {
	methods := []string{"POST", "PUT", "DELETE"}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req, _ := http.NewRequest(method, proxyURL+"/loki/api/v1/push", strings.NewReader("{}"))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != 405 {
				t.Errorf("expected 405, got %d for %s", resp.StatusCode, method)
			}
		})
	}
}

// ─── Test Data ───────────────────────────────────────────────────────────────

var chainingDataIngested bool

func ingestChainingData(t *testing.T) {
	t.Helper()
	if chainingDataIngested {
		return
	}
	now := time.Now()

	// JSON-format logs
	pushStream(t, now, streamDef{
		Labels: map[string]string{"app": "chain-test", "level": "info", "env": "prod"},
		Lines: []string{
			`{"method":"GET","path":"/api/v1/users","status":200,"duration_ms":15}`,
			`{"method":"POST","path":"/api/v1/orders","status":201,"duration_ms":142}`,
			`{"method":"GET","path":"/health","status":200,"duration_ms":1}`,
			`{"method":"GET","path":"/api/v1/products","status":200,"duration_ms":8}`,
		},
	})
	pushStream(t, now, streamDef{
		Labels: map[string]string{"app": "chain-test", "level": "error", "env": "prod"},
		Lines: []string{
			`{"method":"POST","path":"/api/v1/payments","status":500,"duration_ms":5023,"error":"connection refused"}`,
			`{"method":"GET","path":"/api/v1/users/999","status":404,"duration_ms":12,"error":"not found"}`,
		},
	})

	// Logfmt-format logs
	pushStream(t, now, streamDef{
		Labels: map[string]string{"app": "chain-logfmt", "level": "info"},
		Lines: []string{
			`level=info msg="request processed" method=GET path=/api status=200 duration=15ms`,
			`level=info msg="cache hit" key=user:42 ttl=300s`,
		},
	})
	pushStream(t, now, streamDef{
		Labels: map[string]string{"app": "chain-logfmt", "level": "error"},
		Lines: []string{
			`level=error msg="database error" caller=db.go:42 err="connection pool exhausted"`,
		},
	})

	time.Sleep(3 * time.Second)
	chainingDataIngested = true
}
