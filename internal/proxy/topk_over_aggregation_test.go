package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range, loki_api_v1_query
func TestParseAggregationOverRanking(t *testing.T) {
	by := func(labels ...string) *logqlpkg.Grouping { return &logqlpkg.Grouping{Labels: labels} }
	without := func(labels ...string) *logqlpkg.Grouping { return &logqlpkg.Grouping{Without: true, Labels: labels} }
	topk3 := `topk(3, ` + topKInner + `)`
	type layer struct {
		name  string
		k     int
		group *logqlpkg.Grouping
	}
	for _, tc := range []struct {
		name, query string
		ranking     string // the innermost ranking layer's name
		inner, text string
		outer       []layer // innermost first
		ok          bool
	}{
		{"sum", `sum(` + topk3 + `)`, "topk", topKInner, topk3, []layer{{"sum", 0, nil}}, true},
		{"sum by", `sum by (app) (` + topk3 + `)`, "topk", topKInner, topk3, []layer{{"sum", 0, by("app")}}, true},
		{"sum trailing by", `sum(` + topk3 + `) by (app)`, "topk", topKInner, topk3, []layer{{"sum", 0, by("app")}}, true},
		{"spaces", `sum  by (app)  (  ` + topk3 + `  )`, "topk", topKInner, topk3, []layer{{"sum", 0, by("app")}}, true},
		{"parenthesised operand", `sum((` + topk3 + `))`, "topk", topKInner, topk3, []layer{{"sum", 0, nil}}, true},
		{"count of bottomk without", `count without (env) (bottomk(2, ` + topKInner + `))`, "bottomk", topKInner, `bottomk(2, ` + topKInner + `)`, []layer{{"count", 0, without("env")}}, true},
		{"topk of topk", `topk(2, ` + topk3 + `)`, "topk", topKInner, topk3, []layer{{"topk", 2, nil}}, true},
		{"grouped topk of grouped bottomk", `topk by (env) (1, bottomk by (app) (2, ` + topKInner + `))`, "bottomk", topKInner, `bottomk by (app) (2, ` + topKInner + `)`, []layer{{"topk", 1, by("env")}}, true},
		{"three layers", `avg(sum by (app) (` + topk3 + `))`, "topk", topKInner, topk3, []layer{{"sum", 0, by("app")}, {"avg", 0, nil}}, true},
		{"over sort", `topk(2, sort(` + topKInner + `))`, "sort", topKInner, `sort(` + topKInner + `)`, []layer{{"topk", 2, nil}}, true},
		{"sort_desc, not sort", `sum(sort_desc(` + topKInner + `))`, "sort_desc", topKInner, `sort_desc(` + topKInner + `)`, []layer{{"sum", 0, nil}}, true},
		{"sort over sum over topk", `sort(sum by (app) (` + topk3 + `))`, "topk", topKInner, topk3, []layer{{"sum", 0, by("app")}, {"sort", 0, nil}}, true},
		{"the aggregation below the ranking stays its operand", `sum(topk(3, max by (env) (` + topKInner + `)))`, "topk", `max by (env) (` + topKInner + `)`, `topk(3, max by (env) (` + topKInner + `))`, []layer{{"sum", 0, nil}}, true},
		{"quoted parenthesis in the selector", `sum(topk(3, sum by (env) (count_over_time({a=")("}[5m]))))`, "topk", `sum by (env) (count_over_time({a=")("}[5m]))`, `topk(3, sum by (env) (count_over_time({a=")("}[5m])))`, []layer{{"sum", 0, nil}}, true},

		{"ranking is outermost: its own route", topk3, "", "", "", nil, false},
		{"sort is outermost: its own route", `sort(` + topKInner + `)`, "", "", "", nil, false},
		{"no ranking: sums keep their own route", `sum(` + topKInner + `)`, "", "", "", nil, false},
		{"range function is not an aggregation", `sum(count_over_time({a="b"}[5m]))`, "", "", "", nil, false},
		{"binary expression around the fold", `sum(` + topk3 + `) / 2`, "", "", "", nil, false},
		{"binary expression under the fold", `sum(` + topk3 + ` + 1)`, "", "", "", nil, false},
		{"stddev keeps its own route", `stddev(` + topk3 + `)`, "", "", "", nil, false},
		{"stdvar keeps its own route", `stdvar by (app) (` + topk3 + `)`, "", "", "", nil, false},
		{"k is zero below the fold", `sum(topk(0, ` + topKInner + `))`, "", "", "", nil, false},
		{"a log query", `{app="x"} |= "topk(3, sum("`, "", "", "", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseAggregationOverRanking(tc.query)
			if ok != tc.ok {
				t.Fatalf("%s: ok=%v (%+v), want %v", tc.query, ok, got, tc.ok)
			}
			if !ok {
				return
			}
			if got.name != tc.ranking || got.inner != tc.inner || got.text != tc.text {
				t.Fatalf("%s\n got %s %q (text %q)\nwant %s %q (text %q)", tc.query, got.name, got.inner, got.text, tc.ranking, tc.inner, tc.text)
			}
			var outer []layer
			for l := got.outer; l != nil; l = l.outer {
				outer = append(outer, layer{l.name, l.k, l.group})
			}
			if !reflect.DeepEqual(outer, tc.outer) {
				t.Fatalf("%s: outer %+v, want %+v", tc.query, outer, tc.outer)
			}
		})
	}
}

// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range
func TestFoldExcludeLabels(t *testing.T) {
	body := matrixOf(t, map[string][]string{"a/dev": {"1"}, "b/prod": {"2"}})
	for _, tc := range []struct {
		name  string
		group *logqlpkg.Grouping
		want  []string
	}{
		{"no clause drops every label", nil, []string{withoutAllLabels}},
		{"by () drops every label", &logqlpkg.Grouping{}, []string{withoutAllLabels}},
		{"by keeps the listed labels", &logqlpkg.Grouping{Labels: []string{"app"}}, []string{"env"}},
		{"by a label no series has", &logqlpkg.Grouping{Labels: []string{"nope"}}, []string{"app", "env"}},
		{"by every label", &logqlpkg.Grouping{Labels: []string{"app", "env"}}, nil},
		{"without drops the listed labels and the name", &logqlpkg.Grouping{Without: true, Labels: []string{"env"}}, []string{"env", "__name__"}},
		{"without () drops only the name", &logqlpkg.Grouping{Without: true}, []string{"__name__"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := foldExcludeLabels(body, tc.group)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			have := map[string]bool{}
			for _, name := range got {
				have[name] = true
			}
			want := map[string]bool{}
			for _, name := range tc.want {
				want[name] = true
			}
			if !reflect.DeepEqual(have, want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// The fold of one step is the fold of the series that have a sample at that
// step: a series that is absent from a step adds nothing to it.
// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range
func TestOuterFoldMatrixEdges(t *testing.T) {
	body := matrixOf(t, map[string][]string{
		"a/dev":  {"4", "", "1"},
		"a/prod": {"2", "6", ""},
		"b/dev":  {"", "3", "5"},
	})
	for _, tc := range []struct {
		name  string
		op    string
		group *logqlpkg.Grouping
		want  map[string]map[int]string // series labels -> step -> value
	}{
		{"sum", "sum", nil, map[string]map[int]string{"": {0: "6", 1: "9", 2: "6"}}},
		{"avg", "avg", nil, map[string]map[int]string{"": {0: "3", 1: "4.5", 2: "3"}}},
		{"min", "min", nil, map[string]map[int]string{"": {0: "2", 1: "3", 2: "1"}}},
		{"max", "max", nil, map[string]map[int]string{"": {0: "4", 1: "6", 2: "5"}}},
		{"count", "count", nil, map[string]map[int]string{"": {0: "2", 1: "2", 2: "2"}}},
		{"sum by app", "sum", &logqlpkg.Grouping{Labels: []string{"app"}}, map[string]map[int]string{"a": {0: "6", 1: "6", 2: "1"}, "b": {1: "3", 2: "5"}}},
		{"max by env", "max", &logqlpkg.Grouping{Labels: []string{"env"}}, map[string]map[int]string{"dev": {0: "4", 1: "3", 2: "5"}, "prod": {0: "2", 1: "6"}}},
		{"count without env", "count", &logqlpkg.Grouping{Without: true, Labels: []string{"env"}}, map[string]map[int]string{"a": {0: "2", 1: "1", 2: "1"}, "b": {1: "1", 2: "1"}}},
		{"sum without (): nothing merges", "sum", &logqlpkg.Grouping{Without: true}, map[string]map[int]string{"a/dev": {0: "4", 2: "1"}, "a/prod": {0: "2", 1: "6"}, "b/dev": {1: "3", 2: "5"}}},
		{"sum by a missing label folds all", "sum", &logqlpkg.Grouping{Labels: []string{"nope"}}, map[string]map[int]string{"": {0: "6", 1: "9", 2: "6"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			folded := applyWithoutGrouping(body, foldExcludeLabels(body, tc.group), tc.op)
			if !sameFoldedSeries(t, folded, tc.want) {
				t.Fatalf("got %s, want %v", folded, tc.want)
			}
		})
	}
}

// sameFoldedSeries compares by the full label set, so a series with only an
// env label is told from one with only an app.
func sameFoldedSeries(t *testing.T, body []byte, want map[string]map[int]string) bool {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string
				Values [][]any
			}
		}
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	got := map[string]map[int]string{}
	for _, s := range resp.Data.Result {
		var name string
		switch {
		case s.Metric["app"] != "" && s.Metric["env"] != "":
			name = s.Metric["app"] + "/" + s.Metric["env"]
		default:
			name = s.Metric["app"] + s.Metric["env"]
		}
		got[name] = map[int]string{}
		for _, v := range s.Values {
			got[name][int((v[0].(float64)-100)/60)] = v[1].(string)
		}
	}
	return reflect.DeepEqual(got, want)
}

// A fold over an empty vector is empty, and the ranking under it is a real
// answer even when its operand holds no series.
// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query
func TestOuterFoldEmptyAndInstantHandlers(t *testing.T) {
	for _, tc := range []struct {
		name, vector string
		query        string
		want         []string // "labels=value", any order
	}{
		{"empty operand", ``, `sum(topk(3, ` + topKInner + `))`, nil},
		{"sum of the top 2", `{"metric":{"app":"a","env":"dev"},"value":[1700000000,"1"]},{"metric":{"app":"b","env":"dev"},"value":[1700000000,"5"]},{"metric":{"app":"c","env":"dev"},"value":[1700000000,"4"]}`,
			`sum(topk(2, ` + topKInner + `))`, []string{"=9"}},
		{"count of the bottom 2 by env", `{"metric":{"app":"a","env":"dev"},"value":[1700000000,"1"]},{"metric":{"app":"b","env":"dev"},"value":[1700000000,"5"]},{"metric":{"app":"c","env":"prod"},"value":[1700000000,"4"]}`,
			`count by (env) (bottomk(2, ` + topKInner + `))`, []string{"env=dev,=1", "env=prod,=1"}},
		{"top 1 of the top 2", `{"metric":{"app":"a","env":"dev"},"value":[1700000000,"1"]},{"metric":{"app":"b","env":"dev"},"value":[1700000000,"5"]},{"metric":{"app":"c","env":"dev"},"value":[1700000000,"4"]}`,
			`topk(1, topk(2, ` + topKInner + `))`, []string{"app=b,env=dev,=5"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` + tc.vector + `]}}`))
			}))
			defer backend.Close()
			p := newGapTestProxy(t, backend.URL)
			rec := httptest.NewRecorder()
			p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {tc.query}, "time": {"1700000000"}}.Encode(), nil))
			var resp struct {
				Data struct {
					ResultType string
					Result     []struct {
						Metric map[string]string
						Value  []any
					}
				}
			}
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil || resp.Data.ResultType != "vector" {
				t.Fatalf("%d %.300s", rec.Code, rec.Body)
			}
			var got []string
			for _, s := range resp.Data.Result {
				got = append(got, refLabelKey(s.Metric)+"="+s.Value[1].(string))
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for _, want := range tc.want {
				found := false
				for _, have := range got {
					found = found || have == want
				}
				if !found {
					t.Errorf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// The inner aggregation below a ranking merges with its own operator, not with
// the outermost one: `max(topk(2, sum without (env) (...)))` sums the envs of an
// app before it ranks the apps and takes the largest.
// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query
func TestOuterFoldKeepsInnerWithoutMerge(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"app":"a","env":"dev"},"value":[1700000000,"3"]},{"metric":{"app":"a","env":"prod"},"value":[1700000000,"4"]},` +
			`{"metric":{"app":"b","env":"dev"},"value":[1700000000,"5"]},{"metric":{"app":"b","env":"prod"},"value":[1700000000,"1"]}]}}`))
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	rec := httptest.NewRecorder()
	query := `max(topk(2, sum without (env) (count_over_time({a="b"}[5m]))))`
	p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {query}, "time": {"1700000000"}}.Encode(), nil))
	got := parseOverAggResponse(t, query, rec.Code, rec.Body.Bytes(), "vector")
	if v, ok := got[""][1700000000]; len(got) != 1 || !ok || v != 7 {
		t.Fatalf("got %v, want one series of 7 (a: 3+4)", got)
	}
}

// Loki counts max_query_series on the answer of the whole query
// (pkg/logql/engine.go JoinSampleVector): the series the ranking keeps over all
// steps are not an answer, so a fold of them is not limited by their number.
// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range, loki_api_v1_query
func TestOuterFoldSeriesLimitCountsTheFinalAnswer(t *testing.T) {
	base := time.Unix(1699999200, 0).UTC()
	start, end := base.Add(10*time.Minute), base.Add(30*time.Minute)
	backend := newTopKTestBackend(t, topKFixture(1, base, 60))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	p.maxStatsQuerySeries = 2
	rangeQuery := func(query string) (int, string, int) {
		params := url.Values{"query": {query}, "start": {fmt.Sprint(start.UnixNano())}, "end": {fmt.Sprint(end.UnixNano())}, "step": {"60"}}
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
		var resp struct {
			Data struct{ Result []json.RawMessage }
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		return rec.Code, rec.Body.String(), len(resp.Data.Result)
	}
	if code, body, n := rangeQuery(`sum(topk(3, ` + topKInner + `))`); code != http.StatusOK || n != 1 {
		t.Errorf("sum(topk) on a range under limit 2: %d, %d series: %.300s", code, n, body)
	}
	// A fold that keeps more series than the limit is Loki's error.
	if code, body, _ := rangeQuery(`sum by (app, env) (topk(3, ` + topKInner + `))`); code == http.StatusOK || !strings.Contains(body, "max") {
		t.Errorf("sum by (app, env) (topk) over limit 2: %d %.300s", code, body)
	}
	// The other range routes do not count the series below the fold either: a
	// bare range function, a window equal to the step, a parser stage and a
	// backend before v1.45, whose windows the raw evaluator reads.
	for version, inner := range map[string]string{
		"v1.50.0": `count_over_time({app=~".+"}[5m])`,
	} {
		p.storeBackendVersion(version, version)
		if code, body, n := rangeQuery(`sum(topk(3, ` + inner + `))`); code != http.StatusOK || n != 1 {
			t.Errorf("%s sum(topk(%s)) under limit 2: %d, %d series: %.300s", version, inner, code, n, body)
		}
	}
	for _, inner := range []string{topKInner, `sum by (app, env) (count_over_time({app=~".+"}[1m]))`, `sum by (app, env) (count_over_time({app=~".+"} | logfmt [5m]))`} {
		for _, version := range []string{"v1.44.0", "v1.50.0"} {
			p.storeBackendVersion(version, version)
			if code, body, n := rangeQuery(`sum(topk(3, ` + inner + `))`); code != http.StatusOK || n != 1 {
				t.Errorf("%s sum(topk(%s)) under limit 2: %d, %d series: %.300s", version, inner, code, n, body)
			}
		}
	}
	p.storeBackendVersion("v1.50.0", "v1.50.0")

	var rows []string
	for i := 0; i < 8; i++ {
		rows = append(rows, fmt.Sprintf(`{"metric":{"app":"a%d","env":"dev"},"value":[1700000000,"%d"]}`, i, i+1))
	}
	instant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(rows, ",") + `]}}`))
	}))
	defer instant.Close()
	pi := newGapTestProxy(t, instant.URL)
	pi.maxStatsQuerySeries = 2
	for query, wantOK := range map[string]bool{
		`sum(topk(5, ` + topKInner + `))`:                    true,
		`sum by (env) (bottomk(5, ` + topKInner + `))`:       true,
		`topk(1, topk(5, ` + topKInner + `))`:                true,
		`sum by (app) (topk(5, ` + topKInner + `))`:          false,
		`topk(5, topk(6, ` + topKInner + `))`:                false,
		`count by (app) (bottomk(3, ` + topKInner + `))`:     false,
		`max by (env) (sort_desc(` + topKInner + `))`:        true,
		`sum by (app) (sort_desc(` + topKInner + `))`:        false,
		`topk(2, sum by (app) (topk(6, ` + topKInner + `)))`: true,
	} {
		rec := httptest.NewRecorder()
		pi.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {query}, "time": {"1700000000"}}.Encode(), nil))
		if gotOK := rec.Code == http.StatusOK; gotOK != wantOK || (!wantOK && !strings.Contains(rec.Body.String(), "max")) {
			t.Errorf("%s under limit 2: %d %.200s, want ok=%v", query, rec.Code, rec.Body, wantOK)
		}
	}
}

// A tumbling window under sort is answered by VictoriaLogs' stats_query_range
// (a bucket T holds [T, T+step), so Loki's step T+step); the layers above the
// sort fold its matrix.
// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range
func TestSortOverTumblingRange(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
			`{"metric":{"app":"a"},"values":[[1699999980,"1"],[1700000040,"4"]]},` +
			`{"metric":{"app":"b"},"values":[[1699999980,"2"],[1700000040,"3"]]},` +
			`{"metric":{"app":"c"},"values":[[1699999980,"9"]]}]}}`))
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	query := `sum(sort(sum by (app) (count_over_time({a="b"}[1m]))))`
	params := url.Values{"query": {query}, "start": {"1700000040"}, "end": {"1700000100"}, "step": {"60"}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	got := parseOverAggResponse(t, query, rec.Code, rec.Body.Bytes(), "matrix")
	want := overAggPoints{"": {1700000040: 12, 1700000100: 7}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// Queries that were routed before keep their route: a sum over a range function
// is not a ranking, and a ranking that is the outermost layer is its own route.
// conformance: semantics/topk-over-vector-aggregation
func TestAggregationOverRankingLeavesOtherQueriesAlone(t *testing.T) {
	for _, query := range []string{
		`sum(count_over_time({a="b"}[5m]))`,
		`sum by (app) (rate({a="b"}[5m]))`,
		`topk(3, ` + topKInner + `)`,
		`sort(` + topKInner + `)`,
		`stddev(` + topKInner + `)`,
		`sum(topk(3, ` + topKInner + `)) / 2`,
		`{a="b"} |= "topk(3, x)"`,
	} {
		if got, ok := parseAggregationOverRanking(query); ok {
			t.Errorf("%s was taken for an aggregation over a ranking: %+v", query, got)
		}
	}
}

// Logs Drilldown gets a folded answer over the limit cut to the limit with
// Loki's partial-result warning on query_range, where every other client gets
// the error.
// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range
func TestOuterFoldSeriesLimitPartialResultForDrilldown(t *testing.T) {
	base := time.Unix(1699999200, 0).UTC()
	start, end := base.Add(10*time.Minute), base.Add(30*time.Minute)
	backend := newTopKTestBackend(t, topKFixture(1, base, 60))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	p.maxStatsQuerySeries = 2
	query := `sum by (app, env) (topk(3, ` + topKInner + `))`
	params := url.Values{"query": {query}, "start": {fmt.Sprint(start.UnixNano())}, "end": {fmt.Sprint(end.UnixNano())}, "step": {"60"}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	var resp struct {
		Warnings []string
		Data     struct{ Result []json.RawMessage }
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil || len(resp.Data.Result) != 2 || len(resp.Warnings) == 0 {
		t.Fatalf("%d %.400s", rec.Code, rec.Body)
	}
}
