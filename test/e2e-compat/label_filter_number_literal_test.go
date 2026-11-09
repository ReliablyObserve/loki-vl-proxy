//go:build e2e

package e2e_compat

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A label filter reads its number, duration and bytes literals as Loki's lexer does (a number with Go's text/scanner
// rules, then a duration or a bytes size when the whole text is one): `0x`, `1e`, `1b`, `0kb`, a vertical tab inside
// a literal and a `0b1` after white space (Loki's lexer reads it as a decimal number without digits) are Loki's 400 with its text, and the literals Loki reads (`0x1`, `1e3`,
// `0KB`, `1h30m`, `-5m`) are accepted. Both sides answer the same status and, for a rejection, the same bytes.
// conformance: semantics/label-filter-number-literal-scan, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_LabelFilterNumberLiteralsLikeLoki(t *testing.T) {
	end := time.Now()
	start := end.Add(-5 * time.Minute)
	for _, tc := range []struct {
		query  string
		status int
	}{
		{`{app="numlit-none"} | json | latency < 0x1`, http.StatusOK},
		{`{app="numlit-none"} | json | latency < 1e3`, http.StatusOK},
		{`{app="numlit-none"} | json | size >= 1KB`, http.StatusOK}, // Loki 3.7 fails `0KB` here with a 500 after 38s (failed to enqueue request)
		{`{app="numlit-none"} | json | latency > 1h30m`, http.StatusOK},
		{`{app="numlit-none"} | json | latency > -5m`, http.StatusOK},
		{`{app="numlit-none"} | json | latency < 0x`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency < 1e`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | size > 1b`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | size > 0kb`, http.StatusBadRequest},
		{"{app=\"numlit-none\"} | json | latency < 0B\v", http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency<0b1`, http.StatusOK},
		{`{app="numlit-none"} | json | latency<0b1 and size>1_0`, http.StatusOK},
		{`{app="numlit-none"} | json | latency < 0b1`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency < 1__0`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency<0b1 and size>0o8`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency < 5x`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency < 1h30`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency < 5 + 3`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency < 5 +`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | by="x"`, http.StatusBadRequest},
		{`{app="numlit-none"} | json | latency < 5 x-f`, http.StatusBadRequest},
		{`sum(count_over_time({app="numlit-none"}[5m])) > 1e3`, http.StatusOK},
		{`sum(count_over_time({app="numlit-none"}[5m])) > .5`, http.StatusOK},
		{`sum(count_over_time({app="numlit-none"}[5m])) > 0x`, http.StatusBadRequest},
		{`sum(count_over_time({app="numlit-none"}[5m])) > 0b1`, http.StatusBadRequest},
		{`{app="numlit-none"} |= 0x`, http.StatusBadRequest},
	} {
		for _, route := range []string{"query_range", "query"} {
			if route == "query" && tc.status == http.StatusOK && !strings.HasPrefix(tc.query, "sum(") {
				continue // an instant query is a metric query; Loki refuses a log query there
			}
			t.Run(route+" "+strconv.Quote(tc.query), func(t *testing.T) {
				params := url.Values{"query": {tc.query}}
				if route == "query" {
					params.Set("time", strconv.FormatInt(end.UnixNano(), 10))
				} else {
					params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
					params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
					params.Set("step", "60")
				}
				lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/"+route, cloneQueryValues(params), "0", nil)
				if lokiStatus != tc.status {
					t.Fatalf("Loki fixture drifted: want %d, got %d %s", tc.status, lokiStatus, lokiBody)
				}
				proxyStatus, proxyBody := rejectedQueryGet(t, proxyURL, "/loki/api/v1/"+route, cloneQueryValues(params), "0", nil)
				if proxyStatus != lokiStatus {
					t.Fatalf("proxy %d %q, Loki %d %q", proxyStatus, proxyBody, lokiStatus, lokiBody)
				}
				if tc.status == http.StatusBadRequest && strings.TrimSpace(string(proxyBody)) != strings.TrimSpace(string(lokiBody)) {
					t.Fatalf("proxy %q, Loki %q", proxyBody, lokiBody)
				}
			})
		}
	}
}
