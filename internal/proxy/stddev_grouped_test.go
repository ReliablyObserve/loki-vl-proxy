package proxy

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The grouping and the operand of stddev and stdvar are read from the query as
// Loki reads them, in either clause position; stddev_over_time, which works
// over the samples of one window, is not this aggregation.
//
// conformance: semantics/stddev-stdvar-grouped-values
func TestParseStddevStdvarPostAgg(t *testing.T) {
	const op = `count_over_time({env="production"}[5m])`
	for _, tc := range []struct {
		query     string
		name      string
		inner     string
		by        []string
		without   bool
		ungrouped bool
		ok        bool
	}{
		{query: `stddev by (app) (` + op + `)`, name: "stddev", inner: op, by: []string{"app"}, ok: true},
		{query: `stddev by(app)(` + op + `)`, name: "stddev", inner: op, by: []string{"app"}, ok: true},
		{query: `stdvar by (app, level) (` + op + `)`, name: "stdvar", inner: op, by: []string{"app", "level"}, ok: true},
		{query: `stddev (` + op + `) by (app)`, name: "stddev", inner: op, by: []string{"app"}, ok: true},
		{query: `stdvar without (app) (` + op + `)`, name: "stdvar", inner: op, by: []string{"app"}, without: true, ok: true},
		{query: `stdvar(` + op + `) without (app)`, name: "stdvar", inner: op, by: []string{"app"}, without: true, ok: true},
		{query: `stddev by () (` + op + `)`, name: "stddev", inner: op, ok: true},
		{query: `stddev without () (` + op + `)`, name: "stddev", inner: op, without: true, ok: true},
		{query: `stddev(` + op + `)`, name: "stddev", inner: op, ungrouped: true, ok: true},
		// The operand is taken as written, a parenthesised range and a `)` inside a quoted filter included.
		{query: `stdvar without (app) (count_over_time(({namespace="prod"} |~".+bar")[1m]))`, name: "stdvar", inner: `count_over_time(({namespace="prod"} |~".+bar")[1m])`, by: []string{"app"}, without: true, ok: true},
		{query: `stddev by (app) (count_over_time({env="x"} |= "a)b" [5m]))`, name: "stddev", inner: `count_over_time({env="x"} |= "a)b" [5m])`, by: []string{"app"}, ok: true},
		{query: "stddev by (app) (count_over_time({env=\"x\"} |= `a)b` [5m]))", name: "stddev", inner: "count_over_time({env=\"x\"} |= `a)b` [5m])", by: []string{"app"}, ok: true},
		{query: `stddev by (app) (sum by (app, pod) (rate({env="x"}[5m])))`, name: "stddev", inner: `sum by (app, pod) (rate({env="x"}[5m]))`, by: []string{"app"}, ok: true},
		// Not this aggregation.
		{query: `stddev_over_time({env="x"} | unwrap n [5m])`},
		{query: `sum by (app) (` + op + `)`},
		{query: `stddev by (app) (` + op + `) / 2`},
		{query: `stddev(` + op + `) / sum(` + op + `)`},
		{query: `stdvar(` + op + `) + stddev(` + op + `)`},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, ok := parseInstantMetricPostAggQuery(tc.query)
			if tc.name == "" {
				if ok {
					t.Fatalf("parsed as %+v, want a query this route does not take", got)
				}
				return
			}
			if !ok || got.name != tc.name || got.inner != tc.inner {
				t.Fatalf("got %+v ok=%v, want %s over %q", got, ok, tc.name, tc.inner)
			}
			if tc.ungrouped {
				if g := got.grouping; g != nil && (g.Without || len(g.Labels) != 0) {
					t.Fatalf("grouping %+v, want none", g)
				}
				return
			}
			if got.grouping == nil || got.grouping.Without != tc.without || !reflect.DeepEqual(append([]string(nil), got.grouping.Labels...), append([]string(nil), tc.by...)) {
				t.Fatalf("grouping %+v, want without=%v %v", got.grouping, tc.without, tc.by)
			}
		})
	}
}

func foldMatrixBody(series ...string) []byte {
	return []byte(`{"status":"success","data":{"resultType":"matrix","result":[` + strings.Join(series, ",") + `]}}`)
}

func foldVectorBody(series ...string) []byte {
	return []byte(`{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(series, ",") + `]}}`)
}

type foldPoint struct {
	metric map[string]string
	values [][2]string // (time, value) rendered
}

func decodeFold(t *testing.T, body []byte) (resultType string, series []foldPoint) {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || resp.Data.Result == nil {
		t.Fatalf("not a success answer with a result (%v): %s", err, body)
	}
	for _, s := range resp.Data.Result {
		points := s.Values
		if s.Value != nil {
			points = [][]any{s.Value}
		}
		fp := foldPoint{metric: s.Metric}
		for _, p := range points {
			fp.values = append(fp.values, [2]string{fmt.Sprint(p[0]), fmt.Sprint(p[1])})
		}
		series = append(series, fp)
	}
	return resp.Data.ResultType, series
}

func groupingOf(kind string, labels ...string) *logqlpkg.Grouping {
	switch kind {
	case "by":
		return &logqlpkg.Grouping{Labels: labels}
	case "without":
		return &logqlpkg.Grouping{Without: true, Labels: labels}
	}
	return nil
}

// The edge cases of the fold, on instant and range answers: the groups, labels
// and values Loki's VectorAggEvaluator makes (pkg/logql/evaluator.go).
//
// conformance: semantics/stddev-stdvar-grouped-values
func TestStddevStdvarFoldEdgeCases(t *testing.T) {
	sample := func(labels string, v ...string) string {
		vals := make([]string, len(v))
		for i, x := range v {
			vals[i] = fmt.Sprintf(`[%d,%q]`, 100+30*i, x)
		}
		return fmt.Sprintf(`{"metric":{%s},"values":[%s]}`, labels, strings.Join(vals, ","))
	}
	inst := func(labels, v string) string {
		return fmt.Sprintf(`{"metric":{%s},"value":[100,%q]}`, labels, v)
	}
	for _, tc := range []struct {
		name     string
		grouping *logqlpkg.Grouping
		op       string
		series   []string // matrix series, one value per step
		want     []foldPoint
	}{
		{
			name: "single-series group is 0", op: "stddev", grouping: groupingOf("by", "app"),
			series: []string{sample(`"app":"a","pod":"p0"`, "7", "9")},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "0"}, {"130", "0"}}}},
		},
		{
			name: "equal values are 0", op: "stdvar", grouping: groupingOf("by", "app"),
			series: []string{sample(`"app":"a","pod":"p0"`, "5"), sample(`"app":"a","pod":"p1"`, "5")},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "0"}}}},
		},
		{
			name: "population stdvar and stddev of 2,6", op: "stdvar", grouping: groupingOf("by", "app"),
			series: []string{sample(`"app":"a","pod":"p0"`, "2"), sample(`"app":"a","pod":"p1"`, "6")},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "4"}}}},
		},
		{
			name: "stddev of 2,6", op: "stddev", grouping: groupingOf("by", "app"),
			series: []string{sample(`"app":"a","pod":"p0"`, "2"), sample(`"app":"a","pod":"p1"`, "6")},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "2"}}}},
		},
		{
			name: "NaN in a group is NaN", op: "stddev", grouping: groupingOf("by", "app"),
			series: []string{sample(`"app":"a","pod":"p0"`, "NaN"), sample(`"app":"a","pod":"p1"`, "3"), sample(`"app":"b","pod":"p2"`, "3"), sample(`"app":"b","pod":"p3"`, "5")},
			want: []foldPoint{
				{map[string]string{"app": "a"}, [][2]string{{"100", "NaN"}}},
				{map[string]string{"app": "b"}, [][2]string{{"100", "1"}}},
			},
		},
		{
			name: "infinity is NaN", op: "stdvar", grouping: groupingOf("by", "app"),
			series: []string{sample(`"app":"a","pod":"p0"`, "+Inf"), sample(`"app":"a","pod":"p1"`, "3")},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "NaN"}}}},
		},
		{
			name: "a series with no point at a step is left out of it", op: "stdvar", grouping: groupingOf("by", "app"),
			series: []string{sample(`"app":"a","pod":"p0"`, "1", "1"), `{"metric":{"app":"a","pod":"p1"},"values":[[100,"3"]]}`},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "1"}, {"130", "0"}}}},
		},
		{
			name: "a group with no points at a step has no point there", op: "stddev", grouping: groupingOf("by", "app"),
			series: []string{`{"metric":{"app":"a","pod":"p0"},"values":[[100,"1"]]}`, `{"metric":{"app":"b","pod":"p1"},"values":[[130,"1"]]}`},
			want: []foldPoint{
				{map[string]string{"app": "a"}, [][2]string{{"100", "0"}}},
				{map[string]string{"app": "b"}, [][2]string{{"130", "0"}}},
			},
		},
		{
			name: "by () equals no grouping", op: "stddev", grouping: groupingOf("by"),
			series: []string{sample(`"app":"a"`, "2"), sample(`"app":"b"`, "6")},
			want:   []foldPoint{{map[string]string{}, [][2]string{{"100", "2"}}}},
		},
		{
			name: "no grouping folds every series", op: "stddev", grouping: nil,
			series: []string{sample(`"app":"a"`, "2"), sample(`"app":"b"`, "6"), sample(`"app":"c"`, "4")},
			want:   []foldPoint{{map[string]string{}, [][2]string{{"100", "1.632993161855452"}}}},
		},
		{
			name: "without () keeps every label, so every series is a group of one", op: "stddev", grouping: groupingOf("without"),
			series: []string{sample(`"app":"a"`, "2"), sample(`"app":"b"`, "6")},
			want: []foldPoint{
				{map[string]string{"app": "a"}, [][2]string{{"100", "0"}}},
				{map[string]string{"app": "b"}, [][2]string{{"100", "0"}}},
			},
		},
		{
			name: "without drops the listed labels and __name__", op: "stdvar", grouping: groupingOf("without", "pod"),
			series: []string{sample(`"__name__":"c","app":"a","pod":"p0"`, "2"), sample(`"app":"a","pod":"p1"`, "6")},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "4"}}}},
		},
		{
			name: "by a label no series has is one group with no labels", op: "stddev", grouping: groupingOf("by", "missing"),
			series: []string{sample(`"app":"a"`, "2"), sample(`"app":"b"`, "6")},
			want:   []foldPoint{{map[string]string{}, [][2]string{{"100", "2"}}}},
		},
		{
			name: "an empty label value is no label", op: "stddev", grouping: groupingOf("by", "app", "pod"),
			series: []string{sample(`"app":"a","pod":""`, "2"), sample(`"app":"a"`, "6")},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "2"}}}},
		},
		{
			// Welford's update keeps the variance of large, close values; a
			// sum of squares loses it.
			name: "large close values", op: "stdvar", grouping: groupingOf("by", "app"),
			series: []string{sample(`"app":"a","pod":"p0"`, "1000000004"), sample(`"app":"a","pod":"p1"`, "1000000007"), sample(`"app":"a","pod":"p2"`, "1000000013"), sample(`"app":"a","pod":"p3"`, "1000000016")},
			want:   []foldPoint{{map[string]string{"app": "a"}, [][2]string{{"100", "22.5"}}}},
		},
		{
			name: "no series is no result", op: "stddev", grouping: groupingOf("by", "app"),
			series: nil, want: nil,
		},
	} {
		t.Run("matrix/"+tc.name, func(t *testing.T) {
			rt, got := decodeFold(t, applyMatrixStddevAgg(foldMatrixBody(tc.series...), tc.op, tc.grouping))
			if rt != "matrix" {
				t.Fatalf("resultType %q", rt)
			}
			compareFold(t, got, tc.want)
		})
	}

	// The instant form takes the first step of each case.
	t.Run("instant", func(t *testing.T) {
		body := foldVectorBody(inst(`"app":"a","pod":"p0"`, "2"), inst(`"app":"a","pod":"p1"`, "6"), inst(`"app":"b","pod":"p2"`, "9"))
		rt, got := decodeFold(t, applyInstantStddevAgg(body, "stdvar", groupingOf("by", "app")))
		if rt != "vector" {
			t.Fatalf("resultType %q", rt)
		}
		compareFold(t, got, []foldPoint{
			{map[string]string{"app": "a"}, [][2]string{{"100", "4"}}},
			{map[string]string{"app": "b"}, [][2]string{{"100", "0"}}},
		})
	})
	t.Run("instant empty vector", func(t *testing.T) {
		rt, got := decodeFold(t, applyInstantStddevAgg(foldVectorBody(), "stddev", groupingOf("without", "app")))
		if rt != "vector" || len(got) != 0 {
			t.Fatalf("%q %v, want an empty vector", rt, got)
		}
	})
	t.Run("an answer with no rows and no resultType is an empty vector", func(t *testing.T) {
		rt, got := decodeFold(t, applyInstantStddevAgg([]byte(`{"status":"success","data":{"result":[]}}`), "stdvar", groupingOf("without", "app")))
		if rt != "vector" || len(got) != 0 {
			t.Fatalf("%q %v, want an empty vector", rt, got)
		}
	})
	t.Run("an error answer is passed on", func(t *testing.T) {
		body := []byte(`{"status":"error","errorType":"bad_data","error":"x"}`)
		if got := applyInstantStddevAgg(body, "stddev", groupingOf("by", "app")); string(got) != string(body) {
			t.Fatalf("got %s", got)
		}
		if got := applyMatrixStddevAgg(body, "stddev", groupingOf("by", "app")); string(got) != string(body) {
			t.Fatalf("got %s", got)
		}
	})
}

func compareFold(t *testing.T, got, want []foldPoint) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d series %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i := range want {
		if !reflect.DeepEqual(got[i].metric, want[i].metric) {
			t.Fatalf("series %d labels %v, want %v", i, got[i].metric, want[i].metric)
		}
		if len(got[i].values) != len(want[i].values) {
			t.Fatalf("series %d values %v, want %v", i, got[i].values, want[i].values)
		}
		for j := range want[i].values {
			if got[i].values[j][0] != want[i].values[j][0] || !closeFold(got[i].values[j][1], want[i].values[j][1]) {
				t.Fatalf("series %d point %d is %v, want %v", i, j, got[i].values[j], want[i].values[j])
			}
		}
	}
}

func closeFold(a, b string) bool {
	if a == b {
		return true
	}
	var x, y float64
	if _, err := fmt.Sscan(a, &x); err != nil {
		return false
	}
	if _, err := fmt.Sscan(b, &y); err != nil {
		return false
	}
	d := x - y
	if d < 0 {
		d = -d
	}
	return d <= 1e-9*max(1, y)
}

// The query of registry cluster ce65447e, an instant stdvar without over a
// parenthesised, filtered log range, answers a vector with a resultType whether
// the window holds lines or not; each of its series is its own group (the only
// label it keeps apart from app is the pod), so the variance is 0.
//
// conformance: semantics/stddev-stdvar-grouped-values
func TestStdvarWithoutInstantAlwaysAnswersAVector(t *testing.T) {
	const query = `stdvar without (app) (count_over_time(({namespace="prod"} |~".+bar")[1m]))`
	for _, lines := range []int{0, 5} {
		t.Run(fmt.Sprintf("lines=%d", lines), func(t *testing.T) {
			var rows []emuRow
			for i := 0; i < lines; i++ {
				rows = append(rows, emuRow{ts: 1_800_000_000_000_000_000 - int64(i)*1e9, stream: stdStreamText(stdStreams[i%len(stdStreams)]), value: 1, msg: "bar"})
			}
			emu := &vlEmulator{t: t, rows: rows, field: "amount"}
			backend := httptest.NewServer(emu.handler())
			defer backend.Close()
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			res := doCompatProxyRequest(p, "/loki/api/v1/query?"+url.Values{"query": {query}, "time": {"1800000000"}}.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			var resp struct {
				Status string `json:"status"`
				Data   struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []any             `json:"value"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil || res.Code != 200 || resp.Status != "success" || resp.Data.ResultType != "vector" {
				t.Fatalf("%d (%v): %s", res.Code, err, res.Body)
			}
			if len(resp.Data.Result) != min(lines, 5) {
				t.Fatalf("%d series for %d lines: %s", len(resp.Data.Result), lines, res.Body)
			}
			for _, s := range resp.Data.Result {
				if s.Metric["pod"] == "" || s.Metric["app"] != "" || s.Value[1] != "0" {
					t.Fatalf("series %v = %v, want a pod series of variance 0 without app", s.Metric, s.Value)
				}
			}
		})
	}
}

func foldedStdRequest(t *testing.T, p *Proxy, query string, ranged bool) (int, []byte) {
	t.Helper()
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query?"
	if ranged {
		target = "/loki/api/v1/query_range?"
		params.Set("start", "1800000000")
		params.Set("end", "1800000300")
		params.Set("step", "60")
	} else {
		params.Set("time", "1800000300")
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	return res.Code, res.Body.Bytes()
}

func foldedStdProxy(t *testing.T, rows []emuRow) *Proxy {
	t.Helper()
	emu := &vlEmulator{t: t, rows: rows, field: "amount"}
	backend := httptest.NewServer(emu.handler())
	t.Cleanup(backend.Close)
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Loki counts max_query_series on the answer of the aggregation
// (pkg/logql/engine.go), so 600 streams in 3 apps are 3 series, while 600 groups
// are over the limit. The operand's own series are not what the limit counts.
//
// conformance: semantics/stddev-stdvar-grouped-values
func TestStddevStdvarCountsSeriesLimitOnTheGroups(t *testing.T) {
	var rows []emuRow
	for i := 0; i < 600; i++ {
		rows = append(rows, emuRow{ts: 1_800_000_000_000_000_000 + int64(i%5)*1e9 + int64(i), stream: fmt.Sprintf(`{app=%q,pod="p%d"}`, []string{"a", "b", "c"}[i%3], i), value: 1, msg: "line"})
	}
	p := foldedStdProxy(t, rows)
	for _, ranged := range []bool{false, true} {
		for _, op := range []string{"stddev", "stdvar"} {
			t.Run(fmt.Sprintf("%s/ranged=%v", op, ranged), func(t *testing.T) {
				code, body := foldedStdRequest(t, p, op+` by (app) (count_over_time({env="production"}[5m]))`, ranged)
				var resp struct {
					Data struct {
						Result []json.RawMessage `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(body, &resp); err != nil || code != 200 || len(resp.Data.Result) != 3 {
					t.Fatalf("%d (%v): %d series, want 3 groups: %s", code, err, len(resp.Data.Result), body)
				}
				code, body = foldedStdRequest(t, p, op+` by (pod) (count_over_time({env="production"}[5m]))`, ranged)
				if code != 400 || !strings.Contains(string(body), "max_query_series") && !strings.Contains(string(body), "series") {
					t.Fatalf("%d: 600 groups want the series limit error: %s", code, body)
				}
			})
		}
	}
}

// An outer aggregation over a `without` operand merges the operand's series with
// the operand's own aggregation (max here), not with the outer one's: Loki's
// stddev(max without (pod) (x)) is the deviation of the per-app maxima.
//
// conformance: semantics/stddev-stdvar-grouped-values
func TestPostAggregationMergesWithoutByTheOperandsOperator(t *testing.T) {
	// pods p0..p2 of app a hold 1, 2 and 6 lines, p4 and p5 of app c 3 and 5.
	var rows []emuRow
	for pod, n := range map[int]int{0: 1, 1: 2, 2: 6, 4: 3, 5: 5} {
		for i := 0; i < n; i++ {
			rows = append(rows, emuRow{ts: 1_800_000_000_000_000_000 + int64(10+pod*10+i)*1e9, stream: stdStreamText(stdStreams[pod]), value: 1, msg: "line"})
		}
	}
	p := foldedStdProxy(t, rows)
	for _, tc := range []struct {
		query string
		want  []float64 // values in the answer's order
	}{
		// The per-app maxima are 6 and 5 (sums would be 9 and 8, minima 1 and 3).
		{`stddev(max without (pod) (count_over_time({env="production"}[5m])))`, []float64{0.5}},
		{`stdvar(min without (pod) (count_over_time({env="production"}[5m])))`, []float64{1}},
		{`sort_desc(max without (pod) (count_over_time({env="production"}[5m])))`, []float64{6, 5}},
		{`topk(1, max without (pod) (count_over_time({env="production"}[5m])))`, []float64{6}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			code, body := foldedStdRequest(t, p, tc.query, false)
			var resp struct {
				Data struct {
					Result []struct {
						Value []any `json:"value"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &resp); err != nil || code != 200 || len(resp.Data.Result) != len(tc.want) {
				t.Fatalf("%d (%v): %s", code, err, body)
			}
			for i, w := range tc.want {
				if got := fmt.Sprint(resp.Data.Result[i].Value[1]); !closeFold(got, strconv.FormatFloat(w, 'f', -1, 64)) {
					t.Fatalf("result %d = %s, want %v: %s", i, got, w, body)
				}
			}
		})
	}
}
