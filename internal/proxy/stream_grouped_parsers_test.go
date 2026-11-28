package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// streamGroupedBackend answers the stats of a cut query with the series given, the stream label check (before the
// read) with labelled and the stored value check (after it) with stored, and records every request.
type streamGroupedBackend struct {
	mu   sync.Mutex
	sent []string
	// series are the metric labels of the stats answer, as JSON objects.
	series []string
	// labelled and stored answer the two checks: "" no row, "row" one row, "fail" a 500.
	labelled, stored string
}

func (b *streamGroupedBackend) serve(t *testing.T, ts int64) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		b.mu.Lock()
		b.sent = append(b.sent, r.URL.Path+" "+query)
		b.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		answerCheck := func(how string) {
			switch how {
			case "row":
				fmt.Fprintf(w, `{"_time":"%s"}`+"\n", time.Unix(ts, 0).UTC().Format(time.RFC3339Nano))
			case "fail":
				w.WriteHeader(http.StatusInternalServerError)
			}
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/stats_query_range"):
			parts := make([]string, len(b.series))
			for i, m := range b.series {
				parts[i] = fmt.Sprintf(`{"metric":%s,"values":[[%d,"2"],[%d,"3"]]}`, m, ts, ts+60)
			}
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[%s]}}`, strings.Join(parts, ","))
		case strings.HasSuffix(r.URL.Path, "/stats_query"):
			parts := make([]string, len(b.series))
			for i, m := range b.series {
				parts[i] = fmt.Sprintf(`{"metric":%s,"value":[%d,"2"]}`, m, ts)
			}
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[%s]}}`, strings.Join(parts, ","))
		case strings.HasSuffix(r.URL.Path, "/stream_field_names"):
			_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1},{"value":"level","hits":1},{"value":"pod","hits":1}]}`))
		case strings.HasSuffix(query, "| fields _time | limit 1") && strings.Contains(query, `=~".+"}`):
			answerCheck(b.labelled)
		case strings.HasSuffix(query, "| fields _time | limit 1"):
			answerCheck(b.stored)
		case strings.HasSuffix(r.URL.Path, "/select/logsql/query"):
			// A raw-row read of the evaluator: no line.
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (b *streamGroupedBackend) requests() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.sent...)
}

func streamGroupedTestProxy(t *testing.T, url string, lowering bool) *Proxy {
	t.Helper()
	p, err := New(Config{BackendURL: url, Cache: cache.New(time.Minute, 100), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	p.queryPlanLowering = lowering
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// TestStreamGroupedParsersRoute holds the route of a sum grouped by a stream label over json and logfmt parsers
// (semantics/stream-grouped-sum-skips-parsers): a stream label check, the stats of the query with no parser and a
// stored value check, served when every check holds; every other outcome goes on to the ordered JSON evaluator,
// which reads raw rows, as before, and a grouping by a parsed key or by a label some stream lacks spends no stats
// read.
//
// conformance: semantics/stream-grouped-sum-skips-parsers
func TestStreamGroupedParsersRoute(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	ts := now.Add(-30 * time.Minute).Unix()
	const ps5 = `sum by (level) (count_over_time({app="api"} | json | logfmt | drop __error__, __error_details__ [1m]))`
	const labelled = `(app:="api") AND (NOT {level=~".+"}) | fields _time | limit 1`
	const stored = `(app:="api") AND (NOT (({level="error"} AND level:="error") OR ({level="info"} AND level:="info"))) | fields _time | limit 1`
	both := []string{`{"level":"info"}`, `{"level":"error"}`}
	for _, c := range []struct {
		name             string
		query            string
		series           []string
		labelled, stored string
		lowering         bool
		served           bool
		// checks are the checks the route must send, in order; stats whether it reads the cut query.
		checks []string
		stats  bool
	}{
		{name: "served", query: ps5, series: both, lowering: true, served: true, checks: []string{labelled, stored}, stats: true},
		{name: "a stream lacks the label", query: ps5, series: both, labelled: "row", lowering: true, checks: []string{labelled}},
		{name: "the label check fails", query: ps5, series: both, labelled: "fail", lowering: true, checks: []string{labelled}},
		{name: "a stored field differs", query: ps5, series: both, stored: "row", lowering: true, checks: []string{labelled, stored}, stats: true},
		{name: "the stored check fails", query: ps5, series: both, stored: "fail", lowering: true, checks: []string{labelled, stored}, stats: true},
		{name: "a series without the label", query: ps5, series: []string{`{"level":"info"}`, `{}`}, lowering: true, checks: []string{labelled}, stats: true},
		{name: "a series with an empty label", query: ps5, series: []string{`{"level":""}`}, lowering: true, checks: []string{labelled}, stats: true},
		{name: "lowering off", query: ps5, series: both},
		{name: "a parsed key no stream holds", lowering: true,
			query:  `sum by (route) (count_over_time({app="api"} | json | logfmt | drop __error__ [1m]))`,
			series: []string{`{"route":"/a"}`}},
		{name: "a label the stream value restore reads, pinned", lowering: true, served: true, stats: true,
			query:  `sum by (app) (count_over_time({app="api"} | logfmt | json | drop __error__ [1m]))`,
			series: []string{`{"app":"api"}`}},
		{name: "two labels", lowering: true, served: true, stats: true,
			query:  `sum by (level, pod) (count_over_time({app="api"} | json | logfmt | drop __error__ [1m]))`,
			series: []string{`{"level":"info","pod":"a"}`, `{"level":"info","pod":"b"}`},
			checks: []string{
				`(app:="api") AND (NOT {level=~".+"} OR NOT {pod=~".+"}) | fields _time | limit 1`,
				`(app:="api") AND (NOT (({level="info"} AND level:="info"))) | fields _time | limit 1`,
			}},
		{name: "ranked", query: `topk(1, ` + ps5 + `)`, series: both, lowering: true, served: true, checks: []string{labelled, stored}, stats: true},
	} {
		for _, isRange := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/range=%v", c.name, isRange), func(t *testing.T) {
				backend := &streamGroupedBackend{series: c.series, labelled: c.labelled, stored: c.stored}
				srv := backend.serve(t, ts)
				p := streamGroupedTestProxy(t, srv.URL, c.lowering)
				form := url.Values{"query": {c.query}}
				path := "/loki/api/v1/query"
				if isRange {
					path = "/loki/api/v1/query_range"
					form.Set("start", fmt.Sprint(now.Add(-time.Hour).Unix()))
					form.Set("end", fmt.Sprint(now.Unix()))
					form.Set("step", "60")
				} else {
					form.Set("time", fmt.Sprint(now.Unix()))
				}
				got := doCompatProxyRequest(p, path+"?"+form.Encode(), nil)
				if got.Code != http.StatusOK {
					t.Fatalf("status %d: %s", got.Code, got.Body.String())
				}
				var checks []string
				raw, stats := 0, false
				for _, s := range backend.requests() {
					endpoint, query, _ := strings.Cut(s, " ")
					switch {
					case strings.HasSuffix(query, "| fields _time | limit 1"):
						checks = append(checks, query)
					case strings.HasSuffix(endpoint, "/select/logsql/query") && !strings.Contains(query, "| stats "):
						raw++
					case strings.Contains(endpoint, "stats_query") || strings.Contains(query, "| stats "):
						if strings.Contains(query, "unpack_") {
							t.Errorf("the cut query parses: %s", query)
						}
						stats = stats || strings.Contains(query, "stats by (")
					}
				}
				if strings.Join(checks, "\n") != strings.Join(c.checks, "\n") {
					t.Errorf("checks %q, want %q", checks, c.checks)
				}
				if stats != c.stats {
					t.Errorf("read the cut query %v, want %v: %v", stats, c.stats, backend.requests())
				}
				if served := raw == 0; served != c.served {
					t.Errorf("served %v (%d raw reads), want %v: %v", served, raw, c.served, backend.requests())
				}
				// A ranking over a range reads its stats per step from the query endpoint, which this backend leaves empty.
				if c.served && (!isRange || !strings.HasPrefix(c.query, "topk")) && !strings.Contains(got.Body.String(), `"level":"info"`) && !strings.Contains(got.Body.String(), `"app":"api"`) {
					t.Errorf("answer %s", got.Body.String())
				}
			})
		}
	}
}

// TestStreamGroupedAnswerValues holds the answer check: every series holds every grouping label with a value.
//
// conformance: semantics/stream-grouped-sum-skips-parsers
func TestStreamGroupedAnswerValues(t *testing.T) {
	for _, c := range []struct {
		body     string
		complete bool
		values   string
	}{
		{`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"b","pod":"x"},"values":[]},{"metric":{"level":"a","pod":"x"}}]}}`, true, "level=a,b pod=x"},
		{`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"level":"a","pod":"y"},"value":[1,"1"]}]}}`, true, "level=a pod=y"},
		{`{"status":"success","data":{"resultType":"matrix","result":[]}}`, true, "level= pod="},
		{`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"a"}}]}}`, false, ""},
		{`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"a","pod":""}}]}}`, false, ""},
		{`{"status":"success","data":{"resultType":"streams","result":[]}}`, false, ""},
		{`{"status":"error","data":{"resultType":"matrix","result":[]}}`, false, ""},
		{`not json`, false, ""},
	} {
		values, complete := streamGroupedAnswerValues([]byte(c.body), []string{"level", "pod"})
		if complete != c.complete {
			t.Errorf("%s: complete %v", c.body, complete)
			continue
		}
		if complete {
			if got := "level=" + strings.Join(values["level"], ",") + " pod=" + strings.Join(values["pod"], ","); got != c.values {
				t.Errorf("%s: %s, want %s", c.body, got, c.values)
			}
		}
	}
}

// TestStreamGroupedCutOf holds the cut query: no stage from the first parser on, the range, offset, grouping and a
// ranking kept, written in the canonical spelling.
//
// conformance: semantics/stream-grouped-sum-skips-parsers
func TestStreamGroupedCutOf(t *testing.T) {
	for _, c := range []struct {
		q, cut, ranked string
		at             int
		window, offset time.Duration
	}{
		{q: `sum by (level) (count_over_time({app="a"} |= "x" | json | logfmt | drop __error__ [5m] offset 1h))`, at: 1,
			cut: `sum by (level) (count_over_time({app="a"} |= "x"[5m] offset 1h))`, window: 5 * time.Minute, offset: time.Hour},
		{q: `topk(2, sum by (level, pod) (rate({app="a"} | logfmt | json | drop __error__ [1m])))`, at: 0,
			cut: `sum by (level, pod) (rate({app="a"}[1m]))`, ranked: `topk(2,sum by (level, pod) (rate({app="a"}[1m])))`, window: time.Minute},
	} {
		expr, err := logqlpkg.Parse(c.q)
		if err != nil {
			t.Fatal(err)
		}
		cut, ok := streamGroupedCutOf(expr, c.at)
		if !ok || cut.query != c.cut || cut.ranked != c.ranked || cut.window != c.window || cut.offset != c.offset || cut.selector.String() != `{app="a"}` {
			t.Errorf("%s: %+v %v", c.q, cut, ok)
		}
		// The cut keeps the request's own tree as it was.
		if again, _ := logqlpkg.Parse(c.q); expr.String() != again.String() {
			t.Errorf("%s: the cut changed the tree: %s", c.q, expr)
		}
	}
	if _, ok := streamGroupedCutOf(&logqlpkg.LogQuery{}, 0); ok {
		t.Error("a log query has a cut")
	}
}

// TestAwaitTenantStreamLabelNames holds the bounded wait of a reader that needs the tenant's stream label names: the
// first read waits for the refresh it starts, a later one reads the names without a call, and a failed listing answers
// no name (the stream grouped rule then runs its checks).
//
// conformance: semantics/stream-grouped-sum-skips-parsers
func TestAwaitTenantStreamLabelNames(t *testing.T) {
	for _, fail := range []bool{false, true} {
		var mu sync.Mutex
		listings := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/stream_field_names") {
				mu.Lock()
				listings++
				mu.Unlock()
				if fail {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				time.Sleep(20 * time.Millisecond)
				_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1},{"value":"level","hits":1}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"values":[]}`))
		}))
		t.Cleanup(srv.Close)
		p := streamGroupedTestProxy(t, srv.URL, true)
		ctx := context.Background()
		first := p.awaitTenantStreamLabelNames(ctx)
		mu.Lock()
		afterFirst := listings
		mu.Unlock()
		second := p.awaitTenantStreamLabelNames(ctx)
		mu.Lock()
		afterSecond := listings
		mu.Unlock()
		switch {
		case fail && (len(first) != 0 || len(second) != 0):
			t.Errorf("a failed listing answered names: %v %v", first, second)
		case !fail && (strings.Join(first, ",") != "app,level" || strings.Join(second, ",") != "app,level"):
			t.Errorf("names %v then %v", first, second)
		case !fail && (afterFirst == 0 || afterSecond != afterFirst):
			// The listing is one refresh, sent in the backend's time chunks; the second read sends none.
			t.Errorf("listing calls %d then %d", afterFirst, afterSecond)
		}
	}
}
