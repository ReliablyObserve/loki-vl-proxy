//go:build e2e

package e2e_compat

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// An ip() label filter with an invalid pattern is Loki's 400 with Loki's text, for log and metric queries,
// query_range and query; a valid pattern and a stage that joins several filters still answer 200.
//
// conformance: semantics/ip-label-filter-invalid-pattern, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_IPLabelFilterInvalidPatternLikeLoki(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	now := time.Now()
	for _, tc := range []struct {
		path, query string
		want        int
	}{
		{"/loki/api/v1/query_range", `{app="ip-invalid"} | addr=ip("zzz")`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="ip-invalid"} | addr = IP ( "" )`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="ip-invalid"} | addr!=ip("10.0.0.5-10.0.0.1")`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="ip-invalid"} | json | addr=ip("1.2.3.4/33")`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `sum(count_over_time({app="ip-invalid"} | json | addr=ip("zzz") [5m]))`, http.StatusBadRequest},
		{"/loki/api/v1/query", `sum(count_over_time({app="ip-invalid"} | addr!=ip("a\"b") [5m]))`, http.StatusBadRequest},
		{"/loki/api/v1/query_range", `{app="ip-invalid"} | addr=ip("10.0.0.0/8")`, http.StatusOK},
		{"/loki/api/v1/query_range", `{app="ip-invalid"} | a="1", addr=ip("zzz")`, http.StatusOK},
	} {
		params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(now.Add(-15*time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(now.UnixNano(), 10)}, "limit": {"10"}}
		if tc.path == "/loki/api/v1/query" {
			params = url.Values{"query": {tc.query}, "time": {strconv.FormatInt(now.UnixNano(), 10)}}
		}
		ls, lb := hardeningRequest(t, http.MethodGet, lokiURL+tc.path+"?"+params.Encode(), "", headers)
		ps, pb := hardeningRequest(t, http.MethodGet, proxyURL+tc.path+"?"+params.Encode(), "", headers)
		if ls != tc.want {
			t.Fatalf("%s %s: Loki answered %d %s, the test pins %d", tc.path, tc.query, ls, lb, tc.want)
		}
		if ps != ls || (ls != http.StatusOK && string(pb) != string(lb)) {
			t.Errorf("%s %s:\n proxy %d %q\n Loki  %d %q", tc.path, tc.query, ps, pb, ls, lb)
		}
	}
}
