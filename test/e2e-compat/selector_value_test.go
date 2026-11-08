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

// TestCompat_SelectorValueMustBeString: a stream-selector matcher value is a
// string in Loki, and nothing else. ip("...") belongs to line and label
// filters, so `{a=ip("1.2.3.4")}` is a 400 parse error that expects STRING, in
// the first and a later matcher position and for a bare word or a number. The
// proxy answers the same status and bytes on query_range and query; it read
// ip(...) as a string value and answered 200 with no streams.
// conformance: semantics/selector-values-are-strings, status-400, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_SelectorValueMustBeString(t *testing.T) {
	end := time.Now()
	start := end.Add(-time.Hour)
	for _, query := range []string{
		`{a=ip("1.2.3.4")}`,
		`{a="1", addr!=ip("1.1.1.1")}`,
		`{a=~ip("1.2.3.4")}`,
		`{a=foo}`,
		`{a="1", b=1}`,
		`count_over_time({a="1", addr!=ip("1.1.1.1")}[5m])`,
	} {
		for _, route := range []string{"query_range", "query"} {
			t.Run(route+" "+query, func(t *testing.T) {
				params := url.Values{"query": {query}}
				if route == "query" {
					params.Set("time", strconv.FormatInt(end.UnixNano(), 10))
				} else {
					params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
					params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
					params.Set("step", "60")
				}
				lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/"+route, cloneQueryValues(params), "0", nil)
				if lokiStatus != http.StatusBadRequest || !strings.HasSuffix(string(lokiBody), ", expecting STRING") {
					t.Fatalf("Loki fixture drifted: want a 400 parse error expecting STRING, got %d %s", lokiStatus, lokiBody)
				}
				proxyStatus, proxyBody := rejectedQueryGet(t, proxyURL, "/loki/api/v1/"+route, cloneQueryValues(params), "0", nil)
				if proxyStatus != lokiStatus || strings.TrimSpace(string(proxyBody)) != strings.TrimSpace(string(lokiBody)) {
					t.Fatalf("proxy %d %q, Loki %d %q", proxyStatus, proxyBody, lokiStatus, lokiBody)
				}
			})
		}
	}
}
