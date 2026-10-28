package proxy

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

const topKInner = `sum by (app, env) (count_over_time({a="b"}[5m]))`

// conformance: semantics/topk-grouped-spellings, loki_api_v1_query_range, loki_api_v1_query
func TestParseTopKPostAgg(t *testing.T) {
	by := func(labels ...string) *logqlpkg.Grouping { return &logqlpkg.Grouping{Labels: labels} }
	without := func(labels ...string) *logqlpkg.Grouping { return &logqlpkg.Grouping{Without: true, Labels: labels} }
	for _, tc := range []struct {
		name, query string
		want        instantMetricPostAgg
		ok          bool
	}{
		{"plain", `topk(3, ` + topKInner + `)`, instantMetricPostAgg{name: "topk", inner: topKInner, k: 3}, true},
		{"bottomk", `bottomk(2, ` + topKInner + `)`, instantMetricPostAgg{name: "bottomk", inner: topKInner, k: 2}, true},
		{"space before arguments", `topk (3, ` + topKInner + `)`, instantMetricPostAgg{name: "topk", inner: topKInner, k: 3}, true},
		{"by before the arguments", `topk by (env) (3, ` + topKInner + `)`, instantMetricPostAgg{name: "topk", inner: topKInner, k: 3, group: by("env")}, true},
		{"without before the arguments", `bottomk without (app, env) (3, ` + topKInner + `)`, instantMetricPostAgg{name: "bottomk", inner: topKInner, k: 3, group: without("app", "env")}, true},
		{"by after the arguments", `topk(3, ` + topKInner + `) by (env)`, instantMetricPostAgg{name: "topk", inner: topKInner, k: 3, group: by("env")}, true},
		{"without after the arguments", `topk(3, ` + topKInner + `) without (app)`, instantMetricPostAgg{name: "topk", inner: topKInner, k: 3, group: without("app")}, true},
		{"empty by is one group", `topk by () (3, ` + topKInner + `)`, instantMetricPostAgg{name: "topk", inner: topKInner, k: 3, group: by()}, true},
		{"quoted parenthesis in the selector", `topk by (env) (1, sum by (env) (count_over_time({a=")("}[5m])))`, instantMetricPostAgg{name: "topk", inner: `sum by (env) (count_over_time({a=")("}[5m]))`, k: 1, group: by("env")}, true},
		{"k is zero: a parse error in Loki", `topk by (env) (0, ` + topKInner + `)`, instantMetricPostAgg{}, false},
		{"k is not an integer", `topk(1.5, ` + topKInner + `)`, instantMetricPostAgg{}, false},
		{"grouping twice", `topk by (env) (3, ` + topKInner + `) by (app)`, instantMetricPostAgg{}, false},
		{"binary expression around it", `topk by (env) (3, ` + topKInner + `) * 2`, instantMetricPostAgg{}, false},
		{"not topk", `sum by (env) (` + topKInner + `)`, instantMetricPostAgg{}, false},
		{"topk inside a binary expression, added", `topk(2, ` + topKInner + `) + sum(` + topKInner + `)`, instantMetricPostAgg{}, false},
		{"topk and bottomk, set operation", `topk(2, ` + topKInner + `) and bottomk(2, ` + topKInner + `)`, instantMetricPostAgg{}, false},
		{"topk by topk, scalar on the left", `2 * topk by (env) (2, ` + topKInner + `)`, instantMetricPostAgg{}, false},
		{"two topk without a space", `topk(2,` + topKInner + `)+topk(2,` + topKInner + `)`, instantMetricPostAgg{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseInstantMetricPostAggQuery(tc.query)
			if ok != tc.ok || (ok && !reflect.DeepEqual(got, tc.want)) {
				t.Fatalf("%s\n got %+v ok=%v\nwant %+v ok=%v", tc.query, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings
func TestTopKGroupKey(t *testing.T) {
	m := map[string]string{"app": "a", "env": "prod", "__name__": "x"}
	for _, tc := range []struct {
		name  string
		group *logqlpkg.Grouping
		a, b  map[string]string
		same  bool
	}{
		{"no clause", nil, m, map[string]string{"app": "z"}, true},
		{"by shares the listed labels", &logqlpkg.Grouping{Labels: []string{"env"}}, m, map[string]string{"app": "z", "env": "prod"}, true},
		{"by separates other values", &logqlpkg.Grouping{Labels: []string{"env"}}, m, map[string]string{"app": "a", "env": "dev"}, false},
		{"by treats a missing label as empty", &logqlpkg.Grouping{Labels: []string{"env"}}, map[string]string{"app": "a"}, map[string]string{"app": "b", "env": ""}, true},
		{"without drops the listed labels and __name__", &logqlpkg.Grouping{Without: true, Labels: []string{"app"}}, m, map[string]string{"env": "prod"}, true},
		{"without separates other labels", &logqlpkg.Grouping{Without: true, Labels: []string{"app"}}, m, map[string]string{"app": "a", "env": "dev"}, false},
		{"without nothing: every label set is a group", &logqlpkg.Grouping{Without: true}, m, map[string]string{"app": "b", "env": "prod"}, false},
	} {
		if got := topKGroupKey(tc.group, tc.a) == topKGroupKey(tc.group, tc.b); got != tc.same {
			t.Errorf("%s: same group = %v, want %v", tc.name, got, tc.same)
		}
	}
}

// matrixOf builds a Loki matrix body from series labelled "app/env" with the
// value at each step ("" skips the step).
func matrixOf(t *testing.T, series map[string][]string) []byte {
	t.Helper()
	names := make([]string, 0, len(series))
	for name := range series {
		names = append(names, name)
	}
	sort.Strings(names)
	var result []map[string]any
	for _, name := range names {
		metric := map[string]string{"app": name[:1]}
		if len(name) > 2 {
			metric["env"] = name[2:]
		}
		var values [][]any
		for step, v := range series[name] {
			if v != "" {
				values = append(values, []any{float64(100 + step*60), v})
			}
		}
		result = append(result, map[string]any{"metric": metric, "values": values})
	}
	body, err := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// ranked decodes a matrix into "app/env" -> step index -> value.
func ranked(t *testing.T, body []byte) map[string]map[int]string {
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
	out := map[string]map[int]string{}
	for _, s := range resp.Data.Result {
		name := s.Metric["app"]
		if env, ok := s.Metric["env"]; ok {
			name += "/" + env
		}
		out[name] = map[int]string{}
		for _, v := range s.Values {
			out[name][int((v[0].(float64)-100)/60)] = v[1].(string)
		}
	}
	return out
}

// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, loki_api_v1_query_range
func TestTopKMatrixEdges(t *testing.T) {
	by := func(labels ...string) *logqlpkg.Grouping { return &logqlpkg.Grouping{Labels: labels} }
	nan, inf := strconv.FormatFloat(math.NaN(), 'f', -1, 64), "+Inf"
	for _, tc := range []struct {
		name   string
		series map[string][]string
		k      int
		desc   bool
		group  *logqlpkg.Grouping
		want   map[string]map[int]string
	}{
		{
			name:   "the best series changes between steps and a series keeps only its winning samples",
			series: map[string][]string{"a": {"5", "1", "3"}, "b": {"4", "6", "2"}, "c": {"1", "2", "9"}},
			k:      1, desc: true,
			want: map[string]map[int]string{"a": {0: "5"}, "b": {1: "6"}, "c": {2: "9"}},
		},
		{
			name:   "bottomk keeps the lowest of every step",
			series: map[string][]string{"a": {"5", "1", "3"}, "b": {"4", "6", "2"}, "c": {"1", "2", "9"}},
			k:      2, desc: false,
			want: map[string]map[int]string{"a": {1: "1", 2: "3"}, "b": {0: "4", 2: "2"}, "c": {0: "1", 1: "2"}},
		},
		{
			name:   "ties at the boundary take the first label",
			series: map[string][]string{"c": {"7"}, "a": {"7"}, "b": {"7"}},
			k:      2, desc: true,
			want: map[string]map[int]string{"a": {0: "7"}, "b": {0: "7"}},
		},
		{
			name:   "ties below the boundary do not change the winner",
			series: map[string][]string{"a": {"3"}, "b": {"3"}, "c": {"9"}},
			k:      1, desc: true,
			want: map[string]map[int]string{"c": {0: "9"}},
		},
		{
			name:   "NaN loses to every number in topk",
			series: map[string][]string{"a": {nan}, "b": {"-1"}, "c": {"-5"}},
			k:      2, desc: true,
			want: map[string]map[int]string{"b": {0: "-1"}, "c": {0: "-5"}},
		},
		{
			name:   "NaN loses to every number in bottomk",
			series: map[string][]string{"a": {nan}, "b": {"4"}, "c": {"5"}},
			k:      2, desc: false,
			want: map[string]map[int]string{"b": {0: "4"}, "c": {0: "5"}},
		},
		{
			name:   "NaN fills the places numbers leave free",
			series: map[string][]string{"a": {nan}, "b": {"4"}},
			k:      2, desc: true,
			want: map[string]map[int]string{"a": {0: nan}, "b": {0: "4"}},
		},
		{
			name:   "infinities rank by sign",
			series: map[string][]string{"a": {inf}, "b": {"4"}, "c": {"-Inf"}},
			k:      1, desc: false,
			want: map[string]map[int]string{"c": {0: "-Inf"}},
		},
		{
			name:   "k larger than the series count keeps everything",
			series: map[string][]string{"a": {"1", ""}, "b": {"2", "3"}},
			k:      10, desc: true,
			want: map[string]map[int]string{"a": {0: "1"}, "b": {0: "2", 1: "3"}},
		},
		{
			name:   "a series present at some steps only ranks where it is present",
			series: map[string][]string{"a": {"9", "", ""}, "b": {"1", "1", ""}, "c": {"", "5", "2"}},
			k:      1, desc: true,
			want: map[string]map[int]string{"a": {0: "9"}, "c": {1: "5", 2: "2"}},
		},
		{
			name:   "k of zero keeps nothing",
			series: map[string][]string{"a": {"1"}, "b": {"2"}},
			k:      0, desc: true,
			want: map[string]map[int]string{},
		},
		{
			name:   "by ranks inside each group",
			series: map[string][]string{"a/dev": {"1", "9"}, "b/dev": {"2", "3"}, "a/prod": {"7", "1"}, "b/prod": {"8", "2"}},
			k:      1, desc: true, group: by("env"),
			want: map[string]map[int]string{"b/dev": {0: "2"}, "a/dev": {1: "9"}, "b/prod": {0: "8", 1: "2"}},
		},
		{
			name:   "by a label the series lack is one group",
			series: map[string][]string{"a": {"1"}, "b": {"2"}},
			k:      1, desc: true, group: by("env"),
			want: map[string]map[int]string{"b": {0: "2"}},
		},
		{
			name:   "a tie across the k-th place inside a without group takes the first labels",
			series: map[string][]string{"a/dev": {"5"}, "b/dev": {"5"}, "c/dev": {"5"}, "a/prod": {"1"}, "b/prod": {"2"}},
			k:      2, desc: true, group: &logqlpkg.Grouping{Without: true, Labels: []string{"app"}},
			want: map[string]map[int]string{"a/dev": {0: "5"}, "b/dev": {0: "5"}, "a/prod": {0: "1"}, "b/prod": {0: "2"}},
		},
		{
			name:   "NaN inside a by group loses to a number and fills a group of NaN only",
			series: map[string][]string{"a/dev": {nan}, "b/dev": {"3"}, "a/prod": {nan}, "b/prod": {nan}, "c/prod": {"-1"}, "a/stage": {nan}},
			k:      1, desc: true, group: by("env"),
			want: map[string]map[int]string{"b/dev": {0: "3"}, "c/prod": {0: "-1"}, "a/stage": {0: nan}},
		},
		{
			name:   "k larger than one group but smaller than the total cuts only the larger group",
			series: map[string][]string{"a/dev": {"9"}, "a/prod": {"1"}, "b/prod": {"5"}, "c/prod": {"3"}},
			k:      2, desc: true, group: by("env"),
			want: map[string]map[int]string{"a/dev": {0: "9"}, "b/prod": {0: "5"}, "c/prod": {0: "3"}},
		},
		{
			name:   "a group ranks on its own at a step where another group has no sample",
			series: map[string][]string{"a/dev": {"1", ""}, "b/dev": {"2", ""}, "a/prod": {"", "4"}, "b/prod": {"", "3"}},
			k:      1, desc: false, group: by("env"),
			want: map[string]map[int]string{"a/dev": {0: "1"}, "b/prod": {1: "3"}},
		},
		{
			name:   "without drops the listed labels from the group key",
			series: map[string][]string{"a/dev": {"1"}, "b/dev": {"2"}, "a/prod": {"7"}, "b/prod": {"8"}},
			k:      1, desc: false, group: &logqlpkg.Grouping{Without: true, Labels: []string{"app"}},
			want: map[string]map[int]string{"a/dev": {0: "1"}, "a/prod": {0: "7"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ranked(t, applyTopKToMatrix(matrixOf(t, tc.series), tc.k, tc.desc, tc.group))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, loki_api_v1_query
func TestInstantTopKPerGroup(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{"app":"a","env":"dev"},"value":[1,"1"]},{"metric":{"app":"b","env":"dev"},"value":[1,"2"]},` +
		`{"metric":{"app":"a","env":"prod"},"value":[1,"7"]},{"metric":{"app":"b","env":"prod"},"value":[1,"8"]}]}}`)
	names := func(out []byte) []string {
		var resp struct {
			Data struct {
				Result []struct{ Metric map[string]string }
			}
		}
		if err := json.Unmarshal(out, &resp); err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, s := range resp.Data.Result {
			got = append(got, s.Metric["app"]+"/"+s.Metric["env"])
		}
		return got
	}
	for _, tc := range []struct {
		agg  instantMetricPostAgg
		want []string
		body []byte // overrides the vector above
	}{
		{instantMetricPostAgg{name: "topk", k: 2}, []string{"b/prod", "a/prod"}, nil},
		{instantMetricPostAgg{name: "topk", k: 1, group: &logqlpkg.Grouping{Labels: []string{"env"}}}, []string{"b/prod", "b/dev"}, nil},
		{instantMetricPostAgg{name: "bottomk", k: 1, group: &logqlpkg.Grouping{Labels: []string{"env"}}}, []string{"a/dev", "a/prod"}, nil},
		{instantMetricPostAgg{name: "topk", k: 1, group: &logqlpkg.Grouping{Without: true, Labels: []string{"app"}}}, []string{"b/prod", "b/dev"}, nil},
		{instantMetricPostAgg{name: "topk", k: 9}, []string{"b/prod", "a/prod", "b/dev", "a/dev"}, nil},
		// k larger than every group but smaller than the total keeps all four.
		{instantMetricPostAgg{name: "topk", k: 2, group: &logqlpkg.Grouping{Labels: []string{"env"}}}, []string{"b/prod", "a/prod", "b/dev", "a/dev"}, nil},
		// A tie across the k-th place inside a without group takes the first labels,
		// and a NaN loses to every number in its group but fills a place a number leaves; equal
		// NaN go by label order (the second case: b/dev, not c/dev).
		{instantMetricPostAgg{name: "topk", k: 2, group: &logqlpkg.Grouping{Without: true, Labels: []string{"app"}}}, []string{"a/dev", "b/dev", "b/prod", "a/prod"},
			[]byte(`{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{"app":"c","env":"dev"},"value":[1,"5"]},{"metric":{"app":"a","env":"dev"},"value":[1,"5"]},{"metric":{"app":"b","env":"dev"},"value":[1,"5"]},` +
				`{"metric":{"app":"a","env":"prod"},"value":[1,"NaN"]},{"metric":{"app":"b","env":"prod"},"value":[1,"2"]}]}}`)},
		{instantMetricPostAgg{name: "bottomk", k: 1, group: &logqlpkg.Grouping{Labels: []string{"env"}}}, []string{"b/prod", "b/dev"},
			[]byte(`{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{"app":"c","env":"dev"},"value":[1,"NaN"]},{"metric":{"app":"b","env":"dev"},"value":[1,"NaN"]},` +
				`{"metric":{"app":"a","env":"prod"},"value":[1,"NaN"]},{"metric":{"app":"b","env":"prod"},"value":[1,"2"]}]}}`)},
	} {
		in := body
		if tc.body != nil {
			in = tc.body
		}
		if got := names(applyInstantSortTopkAgg(in, tc.agg)); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%+v: got %v, want %v", tc.agg, got, tc.want)
		}
	}
}

// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, loki_api_v1_query
// The instant handler ranks per group too: `topk by (env) (1, ...)` was a 400
// ("log queries are not supported as an instant query type") before.
func TestInstantTopKByHandler(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"app":"a","env":"dev"},"value":[1700000000,"1"]},{"metric":{"app":"b","env":"dev"},"value":[1700000000,"2"]},` +
			`{"metric":{"app":"a","env":"prod"},"value":[1700000000,"7"]},{"metric":{"app":"b","env":"prod"},"value":[1700000000,"8"]}]}}`))
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	for query, want := range map[string]int{
		`topk by (env) (1, ` + topKInner + `)`:            2,
		`topk(1, ` + topKInner + `) by (env)`:             2,
		`topk(3, ` + topKInner + `)`:                      3,
		`topk by (env) (1, count_over_time({a="b"}[5m]))`: 2,
		`bottomk without (app) (1, ` + topKInner + `)`:    2,
	} {
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {query}, "time": {"1700000000"}}.Encode(), nil)
		rec := httptest.NewRecorder()
		p.handleQuery(rec, req)
		var resp struct {
			Data struct {
				ResultType string
				Result     []json.RawMessage
			}
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil || resp.Data.ResultType != "vector" || len(resp.Data.Result) != want {
			t.Errorf("%s: %d, want %d series: %.300s", query, rec.Code, want, rec.Body)
		}
	}
}

// Loki's parser rejects a k below 1 (pkg/logql/syntax/ast.go mustNewVectorAggregation:
// "invalid parameter (must be greater than 0) topk(0"), in every spelling and on
// both endpoints, so no ranking is ever asked for zero series.
// conformance: semantics/topk-grouped-spellings, loki_api_v1_query, loki_api_v1_query_range
func TestTopKRejectsAKBelowOne(t *testing.T) {
	var reached bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	for _, query := range []string{
		`topk(0, ` + topKInner + `)`,
		`bottomk(0, ` + topKInner + `)`,
		`topk by (env) (0, ` + topKInner + `)`,
		`topk without (app) (0, ` + topKInner + `)`,
		`bottomk(0, ` + topKInner + `) by (env)`,
		`topk(-1, ` + topKInner + `)`,
	} {
		for _, endpoint := range []string{"query", "query_range"} {
			params := url.Values{"query": {query}, "time": {"1700000000"}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}}
			rec := doCompatProxyRequest(p, "/loki/api/v1/"+endpoint+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "parse error") {
				t.Errorf("%s %s: %d %.200s, want a 400 parse error", endpoint, query, rec.Code, rec.Body)
			}
		}
	}
	if reached {
		t.Error("a query with a k below 1 reached VictoriaLogs")
	}
}

// The wrapper the stats handlers read carries the clause, so a grouped topk is
// not cut to k series in total before it is ranked per group.
func TestParseTopKWrapperGrouping(t *testing.T) {
	for query, want := range map[string]*logqlpkg.Grouping{
		`topk(3, ` + topKInner + `)`:                  nil,
		`topk by (env) (3, ` + topKInner + `)`:        {Labels: []string{"env"}},
		`bottomk(3, ` + topKInner + `) without (app)`: {Without: true, Labels: []string{"app"}},
	} {
		k, _, group, ok := parseTopKWrapper(query)
		if !ok || k != 3 || !reflect.DeepEqual(group, want) {
			t.Errorf("%s: k=%d group=%+v ok=%v, want %+v", query, k, group, ok, want)
		}
	}
}

// A topk inside a binary expression is never taken for the topk the post
// aggregation handlers rank: the inner text would be `X) + sum(X` and the
// answer a matrix at an instant.
// conformance: semantics/topk-grouped-spellings, loki_api_v1_query
func TestInstantTopKInsideBinaryIsNotRanked(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"a","env":"dev"},"value":[1700000000,"1"]},{"metric":{"app":"b","env":"dev"},"value":[1700000000,"2"]}]}}`))
	}))
	defer backend.Close()
	p := newGapTestProxy(t, backend.URL)
	for _, query := range []string{
		`topk(2, ` + topKInner + `) + sum(` + topKInner + `)`,
		`topk(2, ` + topKInner + `) and bottomk(2, ` + topKInner + `)`,
	} {
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {query}, "time": {"1700000000"}}.Encode(), nil)
		rec := httptest.NewRecorder()
		p.handleQuery(rec, req)
		var resp struct{ Data struct{ ResultType string } }
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Data.ResultType == "matrix" {
			t.Errorf("%s: an instant query answered a matrix: %.300s", query, rec.Body)
		}
	}
}

func TestTopKCompare(t *testing.T) {
	nan, inf := math.NaN(), math.Inf(1)
	for _, tc := range []struct {
		a, b       float64
		descending bool
		want       int
	}{
		{7, 5, true, -1}, {5, 7, true, 1}, {7, 5, false, 1}, {5, 7, false, -1}, {5, 5, true, 0},
		{nan, 5, true, 1}, {5, nan, true, -1}, {nan, 5, false, 1}, {5, nan, false, -1},
		{nan, nan, true, 0}, {nan, -inf, true, 1}, {inf, 5, true, -1},
	} {
		if got := topKCompare(tc.a, tc.b, tc.descending); got != tc.want {
			t.Errorf("topKCompare(%v, %v, desc=%v) = %d, want %d", tc.a, tc.b, tc.descending, got, tc.want)
		}
	}
}

// NaN never blocks a later number, in the heap of the window-stats route, the
// vector filter and the instant sort alike: topk(2) over [NaN 5 7] is {7, 5}.
// conformance: semantics/topk-grouped-spellings, semantics/topk-bottomk-range-series
func TestTopKNaNRanksLastEverywhere(t *testing.T) {
	nan := math.NaN()
	// The heap: three series with one sample at step 0, in this order.
	stats := &slidingStats{windowGrid: true, ids: map[string]int32{}, lokiNames: []string{"app"}, keys: []string{"a", "b", "c"}}
	for i, v := range []float64{nan, 5, 7} {
		stats.records = append(stats.records, slidingRecord{series: int32(i), grid: slidingGridA, index: 0, count: v})
	}
	for _, tc := range []struct {
		descending bool
		want       []string
	}{{true, []string{"b", "c"}}, {false, []string{"b", "c"}}} {
		body, err := stats.encodeTopK(context.Background(), false, 1, 0, 0, rangeTopK{k: 2, descending: tc.descending}, time.Unix(1700000000, 0), time.Minute, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for app := range ranked(t, body) {
			got = append(got, app)
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("heap descending=%v kept %v, want %v", tc.descending, got, tc.want)
		}
	}
	// k=3 keeps the NaN: it fills a place no number wants.
	body, err := stats.encodeTopK(context.Background(), false, 1, 0, 0, rangeTopK{k: 3, descending: true}, time.Unix(1700000000, 0), time.Minute, 1<<20)
	if err != nil || len(ranked(t, body)) != 3 {
		t.Errorf("k=3 over [NaN 5 7]: %v %s", err, body)
	}

	// The vector filter and the instant sort.
	vector := []byte(`{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{"app":"a"},"value":[1,"NaN"]},{"metric":{"app":"b"},"value":[1,"5"]},{"metric":{"app":"c"},"value":[1,"7"]}]}}`)
	apps := func(body []byte) []string {
		var resp struct {
			Data struct {
				Result []struct{ Metric map[string]string }
			}
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, s := range resp.Data.Result {
			out = append(out, s.Metric["app"])
		}
		return out
	}
	for _, descending := range []bool{true, false} {
		if got := apps(applyTopKToVector(vector, 2, descending, nil)); !reflect.DeepEqual(got, []string{"b", "c"}) {
			t.Errorf("vector descending=%v kept %v", descending, got)
		}
	}
	for name, want := range map[string][]string{"topk": {"c", "b"}, "bottomk": {"b", "c"}} {
		if got := apps(applyInstantSortTopkAgg(vector, instantMetricPostAgg{name: name, k: 2})); !reflect.DeepEqual(got, want) {
			t.Errorf("instant %s kept %v, want %v", name, got, want)
		}
	}
	for name, want := range map[string][]string{"sort_desc": {"c", "b", "a"}, "sort": {"b", "c", "a"}} {
		if got := apps(applyInstantSortTopkAgg(vector, instantMetricPostAgg{name: name})); !reflect.DeepEqual(got, want) {
			t.Errorf("instant %s ordered %v, want %v", name, got, want)
		}
	}
}
