//go:build e2e

package e2e_compat

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// A pattern line filter with two captures in a row or a capture other than `<_>` is Loki's 400 with Loki's text,
// for log and metric queries, query_range and query alike; valid patterns and the `| pattern` parser's named
// captures still answer 200.
//
// conformance: semantics/pattern-line-filter-invalid-captures, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_PatternLineFilterInvalidCaptures(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	now := time.Now()
	rangeParams := func(q string) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(now.Add(-15*time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(now.UnixNano(), 10)}, "limit": {"10"}}
	}
	ask := func(base, path string, params url.Values) (int, string) {
		status, body := hardeningRequest(t, http.MethodGet, base+path+"?"+params.Encode(), "", headers)
		return status, string(body)
	}
	for _, tc := range []struct {
		path, query string
		want        int
	}{
		{"/loki/api/v1/query_range", `{app="pattern-captures"} |> "<_><_>"`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="pattern-captures"} |> "<foo>"`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="pattern-captures"} |> "<_> <foo>"`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="pattern-captures"} !> "ok" or "<a>" or "<_><_>"`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `sum(count_over_time({app="pattern-captures"} |> "<a><b>" [5m]))`, http.StatusBadRequest},
		{"/loki/api/v1/query", `sum(count_over_time({app="pattern-captures"} |> "<a> x" [5m]))`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="pattern-captures"} |> "<_> x <_>"`, http.StatusOK},
		{"/loki/api/v1/query_range", `{app="pattern-captures"} | pattern "<a> <b>"`, http.StatusOK},
	} {
		params := rangeParams(tc.query)
		if tc.path == "/loki/api/v1/query" {
			params = url.Values{"query": {tc.query}, "time": {strconv.FormatInt(now.UnixNano(), 10)}}
		}
		ls, lb := ask(lokiURL, tc.path, params)
		ps, pb := ask(proxyURL, tc.path, params)
		if ls != tc.want {
			t.Fatalf("%s %s: Loki answered %d %s, the test pins %d", tc.path, tc.query, ls, lb, tc.want)
		}
		if ps != ls || (ls != http.StatusOK && pb != lb) {
			t.Errorf("%s %s:\n proxy %d %q\n Loki  %d %q", tc.path, tc.query, ps, pb, ls, lb)
		}
	}
}
