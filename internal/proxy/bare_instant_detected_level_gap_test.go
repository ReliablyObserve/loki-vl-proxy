package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// A bare range function over a stream with no level label carries
// detected_level="unknown" on both query_range and instant answers, including
// under label_replace, as Loki labels it.
// conformance: semantics/instant-bare-range-detected-level-unknown
func TestInstantBareRangeHasDetectedLevelUnknown(t *testing.T) {
	const stream = `{\"app\":\"api-gateway\",\"env\":\"prod-eu\",\"probe\":\"a1\"}`
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query" || r.URL.Path == "/select/logsql/stats_query_range" {
			if !strings.Contains(r.FormValue("query"), "| stats by (_stream, level, detected_level)") {
				t.Errorf("expected stream grouping to derive detected_level: %s", r.FormValue("query"))
			}
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"_stream":"%s","detected_level":"unknown"},"value":[1704067260,"0.5"]}]}}`, stream)
		case "/select/logsql/stats_query_range":
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"_stream":"%s","detected_level":"unknown"},"values":[[1704067200,"1"]]}]}}`, stream)
		default:
			fmt.Fprint(w, `{"status":"success","data":{"result":[]}}`)
		}
	}))
	t.Cleanup(vl.Close)
	p := newTestProxy(t, vl.URL)
	for _, query := range []string{
		`rate({app="api-gateway"}[5m])`,
		`label_replace(rate({app="api-gateway"}[5m]), "app_short", "$1", "app", "(.*)-.*")`,
	} {
		for _, endpoint := range []string{"query", "query_range"} {
			v := url.Values{"query": {query}, "start": {"1704067200"}, "end": {"1704067260"}, "step": {"60"}, "time": {"1704067260"}}
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+endpoint+"?"+v.Encode(), nil)
			req.Header.Set("X-Scope-OrgID", "0")
			rec := httptest.NewRecorder()
			if endpoint == "query" {
				p.handleQuery(rec, req)
			} else {
				p.handleQueryRange(rec, req)
			}
			a := decodeLR(t, rec.Code, rec.Body.Bytes())
			if a.code != http.StatusOK || len(a.series) != 1 {
				t.Fatalf("%s %s: HTTP %d, %d series\n%s", endpoint, query, a.code, len(a.series), a.body)
			}
			level, has := a.series[0].Metric["detected_level"]
			if !has || level != "unknown" {
				t.Errorf("%s %s: detected_level=%q (present %v), want unknown\n%s", endpoint, query, level, has, a.body)
			}
		}
	}
}
