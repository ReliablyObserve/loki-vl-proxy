package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// An empty pattern line filter reaches VictoriaLogs as the filter for the empty
// line on every read path: Loki accepts only an empty line under `|> ""` and
// every other line under `!> ""` (pkg/logql/log/pattern ParseLineFilter, Test).
// conformance: semantics/pattern-line-filter
func TestEmptyPatternLineFilterReachesVictoriaLogs(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		seen = append(seen, r.URL.Path+" "+r.FormValue("query"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer vl.Close()

	for _, tc := range []struct {
		name, path, query, want string
	}{
		{"log keeps empty lines", "/loki/api/v1/query_range", `{namespace="prod"} |> ""`, `~"^$"`},
		{"log drops empty lines", "/loki/api/v1/query_range", `{namespace="prod"} !> ""`, `NOT ~"^$"`},
		{"metric", "/loki/api/v1/query_range", `count_over_time({namespace="prod"} |> "" [5m])`, `~"^$"`},
		{"instant metric", "/loki/api/v1/query", `sum(count_over_time({namespace="prod"} !> "" [5m]))`, `NOT ~"^$"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			seen = nil
			mu.Unlock()
			p := newGapTestProxy(t, vl.URL)
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			params := url.Values{"query": {tc.query}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}, "time": {"1700000600"}}
			req := httptest.NewRequest(http.MethodGet, tc.path+"?"+params.Encode(), nil)
			rec := httptest.NewRecorder()
			if strings.HasSuffix(tc.path, "/query") {
				p.handleQuery(rec, req)
			} else {
				p.handleQueryRange(rec, req)
			}
			mu.Lock()
			defer mu.Unlock()
			var found bool
			for _, q := range seen {
				if strings.Contains(q, tc.want) {
					found = true
				}
				if strings.Contains(q, `~".*"`) {
					t.Errorf("empty pattern still translated to match-all: %s", q)
				}
			}
			if !found {
				t.Errorf("no VictoriaLogs request carries %s (status %d): %q", tc.want, rec.Code, seen)
			}
		})
	}
}

// The Drilldown patterns endpoint scopes the query to its stream selector
// (patternScopeQuery), as Loki's pattern ingester does, so a pattern filter,
// empty or not, never reaches VictoriaLogs from it.
// conformance: semantics/pattern-line-filter
func TestPatternLineFilterIsNotPartOfPatternsQuery(t *testing.T) {
	p := newGapTestProxy(t, "http://127.0.0.1:1")
	for _, query := range []string{`{namespace="prod"} |> ""`, `{namespace="prod"} !> "" != "x"`, `{namespace="prod"} |> "a <_>"`} {
		got, err := p.translatePatternQuery(context.Background(), query)
		if err != nil || got != `namespace:="prod"` {
			t.Errorf("%s: translated to %q (err %v), want the stream selector alone", query, got, err)
		}
	}
}
