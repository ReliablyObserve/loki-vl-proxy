package verify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

func ok(body string) QueryResult { return QueryResult{StatusCode: 200, Body: []byte(body)} }

func TestParseShape(t *testing.T) {
	cases := []struct {
		name string
		body string
		want Shape
	}{
		{"streams", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"a":"1"},"values":[["1","x"],["2","y"]]},{"stream":{"a":"2"},"values":[["3","z"]]}]}}`,
			Shape{Status: "success", Kind: "streams", Series: 2, Points: 3}},
		{"matrix", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1,"1"],[2,"2"]]}]}}`,
			Shape{Status: "success", Kind: "matrix", Series: 1, Points: 2}},
		{"vector", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"a":"1"},"value":[1,"1"]},{"metric":{"a":"2"},"value":[1,"2"]}]}}`,
			Shape{Status: "success", Kind: "vector", Series: 2, Points: 2}},
		{"scalar", `{"status":"success","data":{"resultType":"scalar","result":[1,"2"]}}`,
			Shape{Status: "success", Kind: "scalar", Series: 1, Points: 1}},
		{"empty matrix", `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			Shape{Status: "success", Kind: "empty"}},
		{"label values", `{"status":"success","data":["a","b","c"]}`,
			Shape{Status: "success", Kind: "strings", Series: 3}},
		{"loki label values without data", `{"status":"success"}`,
			Shape{Status: "success", Kind: "empty"}},
		{"series", `{"status":"success","data":[{"app":"a"},{"app":"b"}]}`,
			Shape{Status: "success", Kind: "series", Series: 2}},
		{"loki detected_fields", `{"fields":[{"label":"a","type":"string","cardinality":1,"parsers":["json"]}],"limit":1000}`,
			Shape{Kind: "detected_fields", Series: 1}},
		{"proxy detected_fields", `{"data":[{"label":"a","parsers":["json"]},{"label":"b","parsers":null}],"status":"success"}`,
			Shape{Status: "success", Kind: "detected_fields", Series: 2}},
		{"patterns", `{"status":"success","data":[{"pattern":"<_> x","samples":[[1,2],[3,4]]}]}`,
			Shape{Status: "success", Kind: "patterns", Series: 1, Points: 2}},
	}
	for _, tc := range cases {
		got, err := ParseShape([]byte(tc.body))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got.Stats = nil
		if got.Status != tc.want.Status || got.Kind != tc.want.Kind || got.Series != tc.want.Series || got.Points != tc.want.Points {
			t.Errorf("%s: got %+v want %+v", tc.name, got, tc.want)
		}
	}

	s, err := ParseShape([]byte(`{"streams":3,"chunks":9,"bytes":100,"entries":10}`))
	if err != nil || s.Kind != "index_stats" || s.Stats["entries"] != 10 || s.Stats["streams"] != 3 {
		t.Fatalf("index stats shape: %+v %v", s, err)
	}
	if _, err := ParseShape([]byte(`not json`)); err == nil {
		t.Fatal("expected parse error")
	}
}

func fields(diffs []Diff) string {
	var out []string
	for _, d := range diffs {
		out = append(out, d.Field)
	}
	return strings.Join(out, ",")
}

func TestCompareResponsesStrict(t *testing.T) {
	matrix := func(series, points int) string {
		var b strings.Builder
		b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		for i := 0; i < series; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(`{"metric":{},"values":[`)
			for j := 0; j < points; j++ {
				if j > 0 {
					b.WriteString(",")
				}
				b.WriteString(`[1,"1"]`)
			}
			b.WriteString(`]}`)
		}
		b.WriteString(`]}}`)
		return b.String()
	}
	strict := workload.Tolerance{}
	cases := []struct {
		name        string
		loki, proxy QueryResult
		tol         workload.Tolerance
		want        string
	}{
		{"identical", ok(matrix(4, 61)), ok(matrix(4, 61)), strict, ""},
		{"series differ", ok(matrix(4, 61)), ok(matrix(5, 61)), strict, "series_count,point_count"},
		{"points differ", ok(matrix(4, 61)), ok(matrix(4, 60)), strict, "point_count"},
		{"points within tolerance", ok(matrix(4, 61)), ok(matrix(4, 60)), workload.Tolerance{PointsPct: 0.05}, ""},
		{"kind differs", ok(matrix(1, 1)), ok(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}`), strict, "kind"},
		{"one side empty", ok(matrix(1, 5)), ok(`{"status":"success","data":{"resultType":"matrix","result":[]}}`), strict, "kind"},
		{"both empty", ok(`{"status":"success","data":{"resultType":"streams","result":[]}}`), ok(`{"status":"success","data":{"resultType":"matrix","result":[]}}`), strict, "empty_result"},
		{"both empty allowed", ok(`{"status":"success"}`), ok(`{"status":"success","data":[]}`), workload.Tolerance{AllowEmpty: true}, ""},
		{"http status differs", ok(matrix(1, 1)), QueryResult{StatusCode: 400, Body: []byte(`{"status":"error"}`)}, strict, "http_status"},
		{"both failed", QueryResult{StatusCode: 500, Body: []byte(`{}`)}, QueryResult{StatusCode: 500, Body: []byte(`{}`)}, strict, "http_status"},
		{"skip ignores shape", ok(`{"status":"success","data":[{"pattern":"a","samples":[]}]}`), ok(`{"status":"success","data":[{"pattern":"a","samples":[]},{"pattern":"b","samples":[]}]}`), workload.Tolerance{Skip: "backend-specific"}, ""},
		{"skip still requires success", ok(`{"status":"success","data":[]}`), QueryResult{StatusCode: 502, Body: []byte(`{}`)}, workload.Tolerance{Skip: "x"}, "http_status"},
		{"labels count", ok(`{"status":"success","data":["a","b"]}`), ok(`{"status":"success","data":["a","b","c"]}`), strict, "series_count"},
		{"index stats ignores chunks", ok(`{"streams":2,"chunks":10,"bytes":100,"entries":5}`), ok(`{"streams":2,"chunks":0,"bytes":100,"entries":5}`), strict, ""},
		{"index stats entries", ok(`{"streams":2,"chunks":10,"bytes":100,"entries":5}`), ok(`{"streams":2,"chunks":0,"bytes":100,"entries":50}`), strict, "index_stats.entries"},
		{"detected fields across encodings", ok(`{"fields":[{"label":"a","parsers":["json"]}]}`), ok(`{"status":"success","data":[{"label":"a","parsers":["json"]}]}`), strict, ""},
	}
	for _, tc := range cases {
		diffs, _, _ := CompareResponses(tc.loki, tc.proxy, tc.tol)
		if got := fields(diffs); got != tc.want {
			t.Errorf("%s: diffs %q, want %q (%+v)", tc.name, got, tc.want, diffs)
		}
	}
}

func TestRunSkipsExcludedAndReportsMismatches(t *testing.T) {
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") == "bad" {
			w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","a"],["2","b"]]}]}}`))
			return
		}
		w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","a"]]}]}}`))
	}))
	defer loki.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "volume") {
			t.Errorf("excluded query was requested")
		}
		w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","a"]]}]}}`))
	}))
	defer proxy.Close()

	queries := []workload.Query{
		{Name: "good", Path: "/loki/api/v1/query_range", Params: url.Values{"query": {"good"}}},
		{Name: "bad", Path: "/loki/api/v1/query_range", Params: url.Values{"query": {"bad"}}},
		{Name: "volume", Path: "/loki/api/v1/index/volume", Excluded: "not comparable"},
	}
	results := Run(context.Background(), loki.URL, []Target{{Name: "proxy", URL: proxy.URL}}, queries, 5*time.Second)
	if len(results) != 3 || !results[0].Passed || results[1].Passed || !results[2].Passed || results[2].Skipped == "" {
		t.Fatalf("unexpected results: %+v", results)
	}
	summary := Summary("small", results)
	if !strings.Contains(summary, "small/bad") || !strings.Contains(summary, "point_count") || strings.Contains(summary, "small/good") {
		t.Fatalf("summary:\n%s", summary)
	}
	if Summary("small", results[:1]) != "" {
		t.Fatal("summary of passing results must be empty")
	}
}

func TestCompareResponsesFailsDegradedAnswers(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a"},"values":[[1,"1"]]}]}}`
	withHeader := func(name string) QueryResult {
		r := ok(body)
		r.Header = http.Header{}
		r.Header.Set(name, "1")
		return r
	}
	for _, name := range []string{"X-Proxy-Stale-Response", "X-Proxy-Drilldown-Hits-Fallback", "X-Proxy-Upstream-Error", "X-Proxy-Upstream-Status", "Warning", "X-Loki-VL-Partial-Response", "X-Multi-Tenant-Partial-Failures"} {
		diffs, _, _ := CompareResponses(ok(body), withHeader(name), workload.Tolerance{})
		if got := fields(diffs); got != "degraded" || !strings.Contains(diffs[0].Proxy, name) {
			t.Errorf("%s: diffs %+v", name, diffs)
		}
	}
	warned := ok(`{"status":"success","warnings":["maximum query bounds exceeded; returning partial results"],"data":{"resultType":"matrix","result":[{"metric":{"app":"a"},"values":[[1,"1"]]}]}}`)
	if got := fields(first(CompareResponses(ok(body), warned, workload.Tolerance{}))); got != "degraded" {
		t.Errorf("proxy warnings: %q", got)
	}
	// Loki's own partial answers fail verification too.
	if got := fields(first(CompareResponses(warned, ok(body), workload.Tolerance{}))); got != "degraded" {
		t.Errorf("loki warnings: %q", got)
	}
	// A skipped shape still requires an answer that is not degraded.
	if got := fields(first(CompareResponses(ok(body), withHeader("X-Proxy-Stale-Response"), workload.Tolerance{Skip: "x"}))); got != "degraded" {
		t.Errorf("skip: %q", got)
	}
}

func first(d []Diff, _, _ Shape) []Diff { return d }

func TestCompareResponsesComparesContentWithEqualCounts(t *testing.T) {
	strict := workload.Tolerance{}
	streams := func(labels, line string) string {
		return `{"status":"success","data":{"resultType":"streams","result":[{"stream":` + labels + `,"values":[["1789000000000000002","b"],["1789000000000000001","` + line + `"]]}]}}`
	}
	cases := []struct {
		name, loki, proxy, want string
	}{
		{"label names", `{"status":"success","data":["app","cluster"]}`, `{"status":"success","data":["app","service_name"]}`, "values"},
		{"label names in another order", `{"status":"success","data":["app","cluster"]}`, `{"status":"success","data":["cluster","app"]}`, ""},
		{"series label sets", `{"status":"success","data":[{"app":"a","cluster":"x"}]}`, `{"status":"success","data":[{"app":"a","cluster":"y"}]}`, "label_sets"},
		{"detected field names", `{"fields":[{"label":"status","parsers":["json"]}]}`, `{"status":"success","data":[{"label":"latency_ms","parsers":["json"]}]}`, "field_names"},
		{"matrix label sets", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a"},"values":[[1,"1"]]}]}}`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"b"},"values":[[1,"1"]]}]}}`, "label_sets"},
		{"vector label sets", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"detected_level":"info"},"value":[1,"1"]}]}}`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"level":"info"},"value":[1,"1"]}]}}`, "label_sets"},
		{"stream label sets", streams(`{"app":"a"}`, "a"), streams(`{"app":"b"}`, "a"), "label_sets"},
		{"log line content", streams(`{"app":"a"}`, "a"), streams(`{"app":"a"}`, "z"), "entry_content_sample"},
		{"metadata tuple ignored", streams(`{"app":"a"}`, "a"), `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"a"},"values":[["1789000000000000001","a",{"level":"info"}],["1789000000000000002","b",{}]]}]}}`, ""},
		{"patterns keep the skip", `{"status":"success","data":[{"pattern":"a","samples":[[1,2]]}]}`, `{"status":"success","data":[{"pattern":"b","samples":[[1,2]]}]}`, ""},
	}
	for _, tc := range cases {
		tol := strict
		if strings.HasPrefix(tc.name, "patterns") {
			tol = workload.Tolerance{Skip: "backend-specific"}
		}
		diffs, _, _ := CompareResponses(ok(tc.loki), ok(tc.proxy), tol)
		if got := fields(diffs); got != tc.want {
			t.Errorf("%s: diffs %q, want %q (%+v)", tc.name, got, tc.want, diffs)
		}
	}
}

func TestParseShapeSampleOrdersByTimestamp(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"streams","result":[`)
	for s := 0; s < 2; s++ {
		if s > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"stream":{"s":"` + strconv.Itoa(s) + `"},"values":[`)
		for i := 30; i > 0; i-- {
			if i < 30 {
				b.WriteString(",")
			}
			ts := int64(1789000000000000000) + int64(i*2+s)
			b.WriteString(`["` + strconv.FormatInt(ts, 10) + `","line"]`)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`]}}`)
	shape, err := ParseShape([]byte(b.String()))
	if err != nil {
		t.Fatal(err)
	}
	if len(shape.Sample) != SampleSize || shape.Sample[0] != "1789000000000000002 line" || shape.Sample[1] != "1789000000000000003 line" {
		t.Fatalf("sample %v", shape.Sample)
	}
}

func TestRunVerifiesEveryTarget(t *testing.T) {
	const body = `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"a"},"values":[["1","x"]]}]}}`
	var lokiCalls atomic.Int32
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lokiCalls.Add(1)
		_, _ = w.Write([]byte(body))
	}))
	defer loki.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
	defer good.Close()
	stale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proxy-Stale-Response", "true")
		_, _ = w.Write([]byte(body))
	}))
	defer stale.Close()
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"a"},"values":[["1","y"]]}]}}`))
	}))
	defer other.Close()

	targets := []Target{{"proxy", good.URL}, {"proxy_nocache", stale.URL}, {"proxy_partial", other.URL}}
	queries := []workload.Query{{Name: "q", Path: "/loki/api/v1/query_range", Params: url.Values{"query": {"x"}}}}
	results := Run(context.Background(), loki.URL, targets, queries, 5*time.Second)
	if len(results) != 3 || lokiCalls.Load() != 1 {
		t.Fatalf("results=%d loki calls=%d", len(results), lokiCalls.Load())
	}
	want := map[string]string{"proxy": "", "proxy_nocache": "degraded", "proxy_partial": "entry_content_sample"}
	for _, r := range results {
		if got := fields(r.Diffs); got != want[r.Target] || r.Passed != (want[r.Target] == "") {
			t.Errorf("%s: passed=%v diffs %q", r.Target, r.Passed, got)
		}
	}
	summary := Summary("small", results)
	if !strings.Contains(summary, "small/q (loki vs proxy_nocache)") || !strings.Contains(summary, "small/q (loki vs proxy_partial)") || strings.Contains(summary, "vs proxy)") {
		t.Fatalf("summary:\n%s", summary)
	}
}
