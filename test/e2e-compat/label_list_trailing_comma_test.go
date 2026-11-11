//go:build e2e

package e2e_compat

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// A label list that ends in a comma (`by (app,)`, `| drop a,`) is Loki's 400 with Loki's text, on query_range and
// query alike; the same lists without the comma still answer 200. No data is needed: the query is rejected before
// anything is read.
//
// conformance: semantics/label-list-trailing-comma, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_LabelListTrailingCommaLikeLoki(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	now := time.Now()
	ask := func(base, path string, params url.Values) (int, string) {
		status, body := hardeningRequest(t, http.MethodGet, base+path+"?"+params.Encode(), "", headers)
		return status, string(body)
	}
	for _, tc := range []struct {
		path, query string
		want        int
	}{
		{"/loki/api/v1/query_range", `sum by (app,) (count_over_time({app="label-lists"}[5m]))`, http.StatusBadRequest},
		{"/loki/api/v1/query", `sum(count_over_time({app="label-lists"} | drop level, [5m]))`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `sum by (app) (count_over_time({app="label-lists"}[5m]))`, http.StatusOK},
		{"/loki/api/v1/query", `sum(count_over_time({app="label-lists"} | drop level [5m]))`, http.StatusOK},
	} {
		params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(now.Add(-15*time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(now.UnixNano(), 10)}, "step": {"60"}, "limit": {"10"}}
		if tc.path == "/loki/api/v1/query" {
			params = url.Values{"query": {tc.query}, "time": {strconv.FormatInt(now.UnixNano(), 10)}, "limit": {"10"}}
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
