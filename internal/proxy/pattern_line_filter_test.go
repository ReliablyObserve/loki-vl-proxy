package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
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

// A non-empty pattern line filter reaches VictoriaLogs as Loki's exact matcher
// on every read path: the extract pipe searches each literal at its first
// occurrence like pattern.Matcher.Test (pkg/logql/log/pattern/pattern.go:124-165)
// and the filter on its fields keeps non-empty captures and lines that end on a
// trailing literal. VictoriaLogs applies the log query's limit after that
// filter, and metric queries keep the filter inside the stats query, so the
// proxy neither refetches nor reads raw rows.
// conformance: semantics/pattern-line-filter-anchoring
func TestPatternLineFilterAnchoringReachesVictoriaLogs(t *testing.T) {
	var mu sync.Mutex
	type call struct{ path, query, limit string }
	var seen []call
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		seen = append(seen, call{r.URL.Path, r.FormValue("query"), r.FormValue("limit")})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer vl.Close()

	exclude := `| extract "<plain:__lvp_pf0_1> GET <plain:__lvp_pf0_2> 200<plain:__lvp_pf0_r>" from _msg | filter NOT (__lvp_pf0_1:* __lvp_pf0_2:* __lvp_pf0_r:"") | delete __lvp_pf*`
	include := `~"(?s)^.+ GET .+ 200$" | extract "<plain:__lvp_pf0_1> GET <plain:__lvp_pf0_2> 200<plain:__lvp_pf0_r>" from _msg | filter __lvp_pf0_1:* __lvp_pf0_2:* __lvp_pf0_r:"" | delete __lvp_pf*`
	for _, tc := range []struct {
		name, path, query, want, wantPath string
	}{
		{"log include", "/loki/api/v1/query_range", `{namespace="prod"} |> "<_> GET <_> 200"`, include, "/select/logsql/query"},
		{"log exclude (Drilldown pattern exclude)", "/loki/api/v1/query_range", `{namespace="prod"} !> "<_> GET <_> 200"`, exclude, "/select/logsql/query"},
		{"metric include", "/loki/api/v1/query_range", `count_over_time({namespace="prod"} |> "<_> GET <_> 200" [5m])`, include, "/select/logsql/stats_query_range"},
		{"Drilldown volume with pattern exclude", "/loki/api/v1/query_range", `sum by (detected_level) (count_over_time({namespace="prod"} !> "<_> GET <_> 200" [5m]))`, exclude, "/select/logsql/stats_query_range"},
		{"instant metric", "/loki/api/v1/query", `sum(count_over_time({namespace="prod"} !> "<_> GET <_> 200" [5m]))`, exclude, "/select/logsql/stats_query"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			seen = nil
			mu.Unlock()
			p := newGapTestProxy(t, vl.URL)
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			params := url.Values{"query": {tc.query}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}, "time": {"1700000600"}, "limit": {"37"}}
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
			for _, c := range seen {
				if strings.Contains(c.query, `.* GET .*`) {
					t.Errorf("unanchored pattern regexp still reaches VictoriaLogs: %s %s", c.path, c.query)
				}
				if !strings.Contains(c.query, tc.want) {
					continue
				}
				found = true
				if c.path != tc.wantPath {
					t.Errorf("pattern filter sent to %s, want %s", c.path, tc.wantPath)
				}
				if c.path == "/select/logsql/query" && c.limit != "37" {
					t.Errorf("log query limit %q, want the client's 37 (VictoriaLogs applies it after the exact filter)", c.limit)
				}
				if c.path != "/select/logsql/query" && !strings.Contains(c.query[strings.Index(c.query, tc.want):], "| stats ") {
					t.Errorf("pattern filter is not inside the stats query: %s", c.query)
				}
			}
			if !found {
				t.Errorf("no VictoriaLogs request carries %s (status %d): %+v", tc.want, rec.Code, seen)
			}
		})
	}
}

// A pattern line filter parses nothing in Loki: its extract pipe must not route
// a metric to the parser paths, which add parsed labels or read raw rows. A
// pattern parser stage still does.
// conformance: semantics/pattern-line-filter-anchoring
func TestPatternLineFilterIsNotAParserStage(t *testing.T) {
	for _, tc := range []struct {
		logql  string
		parser bool
	}{
		{`{app="x"} |> "<_> GET <_> 200"`, false},
		{`{app="x"} !> "a<_>b" or "<_>c"`, false},
		{`{app="x"} |> "foo" |> "<_> bar"`, false},
		{`{app="x"} |> "<_> GET <_>" | pattern "<ip> <_>"`, true},
		{`{app="x"} |> "<_> GET <_>" | json`, true},
		{`{app="x"} !> "<_> GET <_>" | logfmt`, false},
	} {
		q, err := translator.TranslateLogQL(tc.logql)
		if err != nil {
			t.Fatalf("%s: %v", tc.logql, err)
		}
		if got := queryUsesParserStages(q); got != tc.parser {
			t.Errorf("%s: queryUsesParserStages(%s) = %v, want %v", tc.logql, q, got, tc.parser)
		}
	}
}
