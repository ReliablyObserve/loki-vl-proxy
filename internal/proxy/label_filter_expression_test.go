package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// labelFilterBackend records the requests that reach VictoriaLogs.
type labelFilterBackend struct {
	mu    sync.Mutex
	paths []string
	query []string
}

func (b *labelFilterBackend) handler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	b.mu.Lock()
	b.paths = append(b.paths, r.URL.Path)
	b.query = append(b.query, r.FormValue("query"))
	b.mu.Unlock()
	if strings.HasSuffix(r.URL.Path, "/stats_query_range") || strings.HasSuffix(r.URL.Path, "/stats_query") {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"{\"status\":503,\"method\":\"GET\"}","app":"a"}` + "\n"))
}

// conformance: semantics/label-filter-expression-syntax, loki_api_v1_query_range
func TestQueryRange_LabelFilterExpressionsReachVictoriaLogsAsOneFilter(t *testing.T) {
	cases := []struct {
		name, query string
		wantFilter  string // what must follow `| unpack_json ` in the VictoriaLogs query
	}{
		{"group then comma", `{app="a"} | json | (status>=500 or latency_ms>1000), method="GET"`, `| filter ((status:>=500 or (latency_ms:>1000 OR "latency.ms":>1000 OR "latency-ms":>1000)) and method:="GET")`},
		{"comma", `{app="a"} | json | status>=500,latency<250ms`, `| filter (status:>=500 and latency:<250ms)`},
		{"group with and, or", `{app="a"} | json | (status < 500 and status > 200) or method = "GET"`, `| filter ((status:<500 and status:>200) or method:="GET")`},
		{"comma binds the rest of the group", `{app="a"} | json | status>=500, method="GET" or method="PUT"`, `| filter (status:>=500 and (method:="GET" or method:="PUT"))`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &labelFilterBackend{}
			vl := httptest.NewServer(http.HandlerFunc(backend.handler))
			defer vl.Close()
			p := newTestProxy(t, vl.URL)

			w := doCompatProxyRequest(p, "/loki/api/v1/query_range?start=1609459200&end=1609462800&limit=100&query="+url.QueryEscape(tc.query), map[string]string{"X-Scope-OrgID": "0"})
			if w.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if len(backend.query) == 0 || !strings.Contains(backend.query[0], "| unpack_json "+tc.wantFilter) {
				t.Fatalf("VictoriaLogs query %q does not carry %q", backend.query, tc.wantFilter)
			}
		})
	}
}

// conformance: semantics/label-filter-expression-syntax, loki_api_v1_query_range
func TestQueryRange_LabelFilterExpressionMetricsTakeTheRouteOfASingleFilter(t *testing.T) {
	// A grouped or comma-joined filter must not change how a metric is
	// answered: the requests that reach VictoriaLogs go to the same endpoints as
	// the ones of the same metric with one filter, and the pushed-down ones are
	// stats queries that run the filter before | stats.
	run := func(t *testing.T, query string) *labelFilterBackend {
		backend := &labelFilterBackend{}
		vl := httptest.NewServer(http.HandlerFunc(backend.handler))
		defer vl.Close()
		p := newTestProxy(t, vl.URL)
		w := doCompatProxyRequest(p, "/loki/api/v1/query_range?start=1609459200&end=1609462800&step=60&query="+url.QueryEscape(query), map[string]string{"X-Scope-OrgID": "0"})
		if w.Code != http.StatusOK {
			t.Fatalf("%s: status=%d body=%s", query, w.Code, w.Body)
		}
		if len(backend.paths) == 0 {
			t.Fatalf("%s never reached VictoriaLogs", query)
		}
		return backend
	}
	cases := []struct {
		query, control string
		statsPushdown  bool
	}{
		{`sum by (app) (count_over_time({app="a"} | json | (status>=500 or latency>1000), method="GET" [5m]))`, `sum by (app) (count_over_time({app="a"} | json | status>=500 [5m]))`, true},
		{`sum by (app) (bytes_over_time({app="a"} | logfmt | status>=500, (method="GET" or method="PUT") [5m]))`, `sum by (app) (bytes_over_time({app="a"} | logfmt | status>=500 [5m]))`, true},
		{`sum by (app) (rate({app="a"} | json | (status>=500 or x>1), method="GET" | unwrap latency [5m]))`, `sum by (app) (rate({app="a"} | json | status>=500 | unwrap latency [5m]))`, true},
		{`sum without (app) (rate({app="a"} | json | (status>=500 or latency>1000), method="GET" [5m]))`, `sum without (app) (rate({app="a"} | json | status>=500 [5m]))`, false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			got, control := run(t, tc.query), run(t, tc.control)
			if strings.Join(got.paths, ",") != strings.Join(control.paths, ",") {
				t.Fatalf("requests went to %v, the single-filter metric's to %v", got.paths, control.paths)
			}
			for i, query := range got.query {
				if !strings.Contains(query, "| filter ") {
					t.Fatalf("request %d lost the filter: %s", i, query)
				}
				if tc.statsPushdown && strings.Index(query, "| stats ") < strings.Index(query, "| filter ") {
					t.Fatalf("request %d: the filter must run before | stats in VictoriaLogs: %s", i, query)
				}
			}
			if tc.statsPushdown {
				for _, path := range got.paths {
					if !strings.Contains(path, "stats_query") {
						t.Fatalf("a metric read raw rows from %s", path)
					}
				}
			}
		})
	}
}

// conformance: semantics/label-filter-expression-syntax, loki_api_v1_query_range
func TestQueryRange_MalformedLabelFilterExpressionsAreRejectedLikeLoki(t *testing.T) {
	// The messages are Loki v3.7.9's for the same query, with the columns of the
	// {app="a"} | json | <expression> text.
	cases := []struct{ expression, want string }{
		{`()`, "parse error at line 1, col 21: syntax error: unexpected ), expecting IDENTIFIER or ("},
		{`(a="1"`, "parse error at line 1, col 26: syntax error: unexpected $end"},
		{`a="1")`, "parse error at line 1, col 25: syntax error: unexpected )"},
		{`a="1" or`, "parse error at line 1, col 28: syntax error: unexpected $end, expecting IDENTIFIER or ("},
		{`a="1" and and b="2"`, "parse error at line 1, col 30: syntax error: unexpected and, expecting IDENTIFIER or ("},
		{`(a="1" or b="2"),`, "parse error at line 1, col 37: syntax error: unexpected $end, expecting IDENTIFIER or ("},
		{`, a="1"`, "parse error at line 1, col 20: syntax error: unexpected ,"},
		// A single filter whose value has the wrong kind.
		{`duration > "1s"`, "parse error at line 1, col 31: syntax error: unexpected STRING"},
		{`ok=true`, "parse error at line 1, col 23: syntax error: unexpected IDENTIFIER"},
		{`a =~ 5`, "parse error at line 1, col 25: syntax error: unexpected NUMBER, expecting STRING"},
		{`a = "x" 5`, "parse error at line 1, col 28: syntax error: unexpected NUMBER"},
		{`(a="1" or b="2" |= "x"`, "parse error at line 0, col 36: syntax error: unexpected |="},
	}
	for _, tc := range cases {
		t.Run(tc.expression, func(t *testing.T) {
			backend := &labelFilterBackend{}
			vl := httptest.NewServer(http.HandlerFunc(backend.handler))
			defer vl.Close()
			p := newTestProxy(t, vl.URL)

			query := `{app="a"} | json | ` + tc.expression
			w := doCompatProxyRequest(p, "/loki/api/v1/query_range?start=1609459200&end=1609462800&query="+url.QueryEscape(query), map[string]string{"X-Scope-OrgID": "0"})
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			// Loki's body is the message itself, as text.
			if got := readErrorRec(t, w).Error; got != tc.want {
				t.Fatalf("body=%q, want %q", got, tc.want)
			}
			if len(backend.paths) != 0 {
				t.Fatalf("a rejected query reached VictoriaLogs: %v", backend.paths)
			}
		})
	}
}
