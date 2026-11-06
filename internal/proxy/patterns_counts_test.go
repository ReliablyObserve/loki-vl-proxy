package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// Loki's /patterns step: the request's step, or the range over 250 points in whole seconds, and never under the
// 10 s resolution it keeps pattern samples at (measured against Loki 3.7.9 on the compose stack).
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternCountsStepIsLokis(t *testing.T) {
	for _, tc := range []struct {
		span time.Duration
		step string
		want time.Duration
		ok   bool
	}{
		{5 * time.Minute, "", 10 * time.Second, true},
		{30 * time.Minute, "", 10 * time.Second, true},
		{time.Hour, "", 14 * time.Second, true},
		{6 * time.Hour, "", 86 * time.Second, true},
		{24 * time.Hour, "", 345 * time.Second, true},
		{10 * time.Minute, "1s", 10 * time.Second, true},
		{10 * time.Minute, "5s", 10 * time.Second, true},
		{10 * time.Minute, "15s", 15 * time.Second, true},
		{10 * time.Minute, "25", 25 * time.Second, true},
		{10 * time.Minute, "1m", time.Minute, true},
		{10 * time.Minute, "nope", 0, false},
	} {
		got, ok := patternCountsStep(0, int64(tc.span), tc.step)
		if got != tc.want || ok != tc.ok {
			t.Errorf("step for %s %q = %s %v, want %s %v", tc.span, tc.step, got, ok, tc.want, tc.ok)
		}
	}
}

// The counts queries are LogsQL nodes all the way down: no raw-text node, rendered once by the printer.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternCountsQueriesAreTyped(t *testing.T) {
	lq, err := logqlpkg.ParseLogQuery(`{app="api", env!="dev"}`)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := logqlpkg.SelectorFilter(lq.Selector, logqlpkg.TranslateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	groups := patternGroupsQuery(filter, 9)
	timeline := patternTimelineQuery(filter, 14*time.Second, 9)
	for _, q := range []*logsql.Query{groups, timeline} {
		assertNoRawLogsQL(t, q.Filter)
		for _, pipe := range q.Pipes {
			if stats, ok := pipe.(logsql.PipeStats); ok {
				for _, f := range stats.Funcs {
					if _, raw := f.Func.(logsql.DeferredExpr); raw {
						t.Fatalf("stats function %q is raw text", f.Func)
					}
				}
			}
		}
	}
	by := `detected_level, level, LEVEL, Level, "log.level", severity, SEVERITY, Severity, SeverityText, lvl, LVL, Lvl, severity_text, Severity_Text, SEVERITY_TEXT, severity_number, _lvp_collapsed`
	prefix := `app:="api" -env:="dev" | copy _msg as _lvp_collapsed | collapse_nums at _lvp_collapsed | stats by (`
	if want := prefix + by + `) count() as c, min(_msg) as _msg, max(_msg) as _lvp_max | limit 10`; groups.String() != want {
		t.Errorf("groups query\n got %s\nwant %s", groups, want)
	}
	if want := prefix + `_time:14s, ` + by + `) count() as c | limit 10`; timeline.String() != want {
		t.Errorf("timeline query\n got %s\nwant %s", timeline, want)
	}
}

func assertNoRawLogsQL(t *testing.T, f logsql.FilterExpr) {
	t.Helper()
	switch n := f.(type) {
	case logsql.DeferredExpr:
		t.Fatalf("filter %q is raw text", n)
	case logsql.AllOf:
		for _, x := range n {
			assertNoRawLogsQL(t, x)
		}
	case logsql.AnyOf:
		for _, x := range n {
			assertNoRawLogsQL(t, x)
		}
	case logsql.Group:
		assertNoRawLogsQL(t, n.Expr)
	case logsql.Exclude:
		assertNoRawLogsQL(t, n.Expr)
	case logsql.AndExpr:
		assertNoRawLogsQL(t, n.Left)
		assertNoRawLogsQL(t, n.Right)
	case logsql.OrExpr:
		assertNoRawLogsQL(t, n.Left)
		assertNoRawLogsQL(t, n.Right)
	case logsql.NotExpr:
		assertNoRawLogsQL(t, n.Expr)
	}
}

// fakePatternCounts is a VictoriaLogs that answers the two counts queries: groups is the groups query's rows,
// timeline the timeline query's; any other query gets other.
type fakePatternCounts struct {
	groups, timeline, other string
	calls                   atomic.Int32
}

func (f *fakePatternCounts) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		_ = r.ParseForm()
		q := r.Form.Get("query")
		w.Header().Set("Content-Type", "application/x-ndjson")
		switch {
		case strings.Contains(q, "| collapse_nums at _lvp_collapsed") && strings.Contains(q, "stats by (_time:"):
			_, _ = w.Write([]byte(f.timeline))
		case strings.Contains(q, "| collapse_nums at _lvp_collapsed"):
			_, _ = w.Write([]byte(f.groups))
		default:
			_, _ = w.Write([]byte(f.other))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newPatternCountsProxy(t *testing.T, backendURL string, maxRows int) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:      backendURL,
		Cache:           cache.New(60*time.Second, 1000),
		LogLevel:        "error",
		PatternsCounts:  true,
		ExecutionLimits: ExecutionLimitsConfig{PatternsMaxCountsRows: maxRows},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func patternsRequest(t *testing.T, p *Proxy, query, step string) patternsResponse {
	t.Helper()
	q := url.Values{"query": {query}, "start": {"1793988900"}, "end": {"1793990100"}}
	if step != "" {
		q.Set("step", step)
	}
	w := httptest.NewRecorder()
	p.handlePatterns(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns?"+q.Encode(), nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var resp patternsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

// The counts flow answers every line of the range, per step, with one entry per level and pattern: a number that
// is the same in every line of a group stays in the pattern, one that varies is <_>.
//
// conformance: drilldown/patterns-full-range-counts, drilldown/patterns-one-series-per-level, loki_api_v1_patterns
func TestPatternsFromCountsCountEveryLine(t *testing.T) {
	fake := &fakePatternCounts{
		groups: `{"level":"info","_lvp_collapsed":"GET /api status=<N> took <N>ms user=alice","c":"5","_msg":"GET /api status=200 took 5ms user=alice","_lvp_max":"GET /api status=200 took 9ms user=alice"}
{"level":"info","_lvp_collapsed":"GET /api status=<N> took <N>ms user=bob","c":"2","_msg":"GET /api status=200 took 7ms user=bob","_lvp_max":"GET /api status=200 took 8ms user=bob"}
{"level":"error","_lvp_collapsed":"job failed code=<N> retry later now","c":"3","_msg":"job failed code=7 retry later now","_lvp_max":"job failed code=7 retry later now"}
`,
		timeline: `{"_time":"2026-11-06T18:20:00Z","level":"info","_lvp_collapsed":"GET /api status=<N> took <N>ms user=alice","c":"3"}
{"_time":"2026-11-06T18:29:50Z","level":"info","_lvp_collapsed":"GET /api status=<N> took <N>ms user=alice","c":"2"}
{"_time":"2026-11-06T18:20:00Z","level":"info","_lvp_collapsed":"GET /api status=<N> took <N>ms user=bob","c":"2"}
{"_time":"2026-11-06T18:25:00Z","level":"error","_lvp_collapsed":"job failed code=<N> retry later now","c":"3"}
`,
	}
	p := newPatternCountsProxy(t, fake.server(t).URL, 0)
	resp := patternsRequest(t, p, `{app="api"}`, "")
	got := map[string]map[int64]int{}
	for _, e := range resp.Data {
		key := e.Level + " " + e.Pattern
		if got[key] != nil {
			t.Fatalf("two entries for %q", key)
		}
		got[key] = map[int64]int{}
		for _, s := range e.Samples {
			ts, _ := numberToInt64(s[0])
			n, _ := numberToInt(s[1])
			if n == 0 {
				t.Fatalf("%q has a zero sample", key)
			}
			got[key][ts] = n
		}
	}
	want := map[string]map[int64]int{
		"info GET /api status=200 took <_> user=<_>": {1793989200: 5, 1793989790: 2},
		"error job failed code=7 retry later now":    {1793989500: 3},
	}
	if len(got) != len(want) {
		t.Fatalf("patterns %v, want %v", got, want)
	}
	for key, samples := range want {
		for ts, n := range samples {
			if got[key][ts] != n {
				t.Errorf("%q at %d = %d, want %d (%v)", key, ts, got[key][ts], n, got)
			}
		}
	}
	if calls := fake.calls.Load(); calls != 2 {
		t.Errorf("VictoriaLogs calls = %d, want the two counts queries", calls)
	}
}

// Anything the counts flow does not answer falls back to sampling lines.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternsFromCountsFallsBackToSampling(t *testing.T) {
	sample := `{"_time":"2026-11-06T18:20:00Z","_msg":"sampled line one two three","app":"api","level":"info"}` + "\n"
	group := `{"level":"info","_lvp_collapsed":"a b c d","c":"1","_msg":"a b c d","_lvp_max":"a b c d"}` + "\n"
	row := `{"_time":"2026-11-06T18:20:00Z","level":"info","_lvp_collapsed":"a b c d","c":"1"}` + "\n"
	for name, tc := range map[string]struct {
		groups, timeline, query string
		maxRows                 int
	}{
		"a group row without a count": {`{"_msg":"a b c d"}` + "\n", row, `{app="api"}`, 0},
		"a row without a message":     {`{"level":"info","_lvp_collapsed":"m","c":"1","_msg":"` + vlDefaultMsgValue + `"}` + "\n", row, `{app="api"}`, 0},
		"more rows than the cap":      {group + group, row, `{app="api"}`, 1},
		"no groups":                   {"", "", `{app="api"}`, 0},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakePatternCounts{groups: tc.groups, timeline: tc.timeline, other: sample}
			p := newPatternCountsProxy(t, fake.server(t).URL, tc.maxRows)
			resp := patternsRequest(t, p, tc.query, "60s")
			if len(resp.Data) != 1 || !strings.HasPrefix(resp.Data[0].Pattern, "sampled line") {
				t.Fatalf("not answered by sampling: %+v", resp.Data)
			}
		})
	}
}

// The handler scopes a query to its stream selector before either path (patternScopeQuery), so a query with a
// pipeline is counted for its selector, as sampling mines it. Loki answers such a query 400 (pkg/pattern
// ParseMatchers): registry case loki/errors/patterns-non-selector-query, open.
func TestPatternsCountsScopeAQueryToItsSelector(t *testing.T) {
	fake := &fakePatternCounts{
		groups:   `{"level":"info","_lvp_collapsed":"a b c d","c":"1","_msg":"a b c d","_lvp_max":"a b c d"}` + "\n",
		timeline: `{"_time":"2026-11-06T18:20:00Z","level":"info","_lvp_collapsed":"a b c d","c":"1"}` + "\n",
	}
	p := newPatternCountsProxy(t, fake.server(t).URL, 0)
	resp := patternsRequest(t, p, `{app="api"} |= "x"`, "60s")
	if len(resp.Data) != 1 || resp.Data[0].Pattern != "a b c d" {
		t.Fatalf("not counted for the selector: %+v", resp.Data)
	}
}

// Without -patterns-counts the proxy samples lines.
func TestPatternsCountsOffSamples(t *testing.T) {
	fake := &fakePatternCounts{other: `{"_time":"2026-11-06T18:20:00Z","_msg":"sampled line one two three","app":"api","level":"info"}` + "\n"}
	p := newPatternCountsProxy(t, fake.server(t).URL, 0)
	p.patternsCounts = false
	resp := patternsRequest(t, p, `{app="api"}`, "60s")
	if len(resp.Data) != 1 || !strings.HasPrefix(resp.Data[0].Pattern, "sampled line") {
		t.Fatalf("not answered by sampling: %+v", resp.Data)
	}
}

// A request without a step gets Loki's derived step, and no step answers under the 10 s sample resolution.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternStepsFollowLoki(t *testing.T) {
	for _, tc := range []struct{ start, end, want string }{
		{"0", "300", "1s"}, {"0", "3600", "14s"}, {"0", "86400", "345s"}, {"0", "100", "1s"}, {"10", "5", ""},
	} {
		if got := derivePatternStep(tc.start, tc.end); got != tc.want {
			t.Errorf("derivePatternStep(%s, %s) = %q, want %q", tc.start, tc.end, got, tc.want)
		}
	}
	for step, want := range map[string]string{"1s": "10s", "9s": "10s", "10s": "10s", "14s": "14s", "1m": "1m", "": ""} {
		if got := patternSampleStep(step); got != want {
			t.Errorf("patternSampleStep(%q) = %q, want %q", step, got, want)
		}
	}
}

// Loki answers only the steps a pattern saw lines in: no zero samples between them.
//
// conformance: drilldown/patterns-full-range-counts, loki_api_v1_patterns
func TestPatternSamplesHaveNoZeros(t *testing.T) {
	entries := fillPatternSamplesAcrossRequestedRange([]patternResultEntry{{
		Pattern: "a b c d",
		Samples: [][]interface{}{{int64(100), 2}, {int64(130), 1}},
	}}, "100", "200", "10s")
	if got := entries[0].Samples; len(got) != 2 {
		t.Fatalf("samples %v, want the two steps with lines", got)
	}
}

func FuzzPatternCountsStep(f *testing.F) {
	for _, seed := range []string{"", "1s", "15s", "1m", "250ms", "0", "-5s", "1e9"} {
		f.Add(int64(3600e9), seed)
	}
	f.Fuzz(func(t *testing.T, span int64, step string) {
		if span <= 0 {
			return
		}
		got, ok := patternCountsStep(0, span, step)
		if !ok {
			return
		}
		if got < patternCountsMinStep || got%time.Second != 0 {
			t.Fatalf("step %s for %d %q is under 10s or not whole seconds", got, span, step)
		}
	})
}
