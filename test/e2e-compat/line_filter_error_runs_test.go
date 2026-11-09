//go:build e2e

package e2e_compat

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// An invalid line filter is Loki's 400 with Loki's text: the error quotes the run of line filters Loki builds
// together (adjacent ones, and those after a parser, up to a label filter, line_format or unpack) and names the
// filter Loki names, for log and metric queries, query_range and query alike.
//
// conformance: semantics/line-filter-error-quotes-the-run, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_LineFilterErrorRunsLikeLoki(t *testing.T) {
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
		{"/loki/api/v1/query_range", `{app="line-filter-runs"} |= "x" |~ "("`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="line-filter-runs"} |~ "(" |~ "["`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="line-filter-runs"} |~ "(" | json |~ "["`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="line-filter-runs"} | logfmt | x="1" |~ "(" |= "k"`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="line-filter-runs"} |= "x" != "y" or ip("z")`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="line-filter-runs"} | json x="a[" |~ "("`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="line-filter-runs"} |> "<a>" | line_format "{{.x}"`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `sum(count_over_time({app="line-filter-runs"} |= "x" | logfmt |~ "(" [5m]))`, http.StatusBadRequest},
		{"/loki/api/v1/query", `sum(count_over_time({app="line-filter-runs"} != ip("z") |~ "(" [5m]))`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="line-filter-runs"} |= ip("z") or "x"`, http.StatusOK},
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
