package proxy

import (
	"encoding/json"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Loki's absentLabels (pkg/logql/evaluator.go:1320-1345): the labels of the
// selector's equality matchers; a name that any other matcher also names, and a
// value of "", have none.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimeLabelsEdgeTable(t *testing.T) {
	for _, tc := range []struct {
		name, selector string
		want           map[string]string
	}{
		{"one equality", `{app="a"}`, map[string]string{"app": "a"}},
		{"two names", `{app="a",env="prod"}`, map[string]string{"app": "a", "env": "prod"}},
		{"regexp matcher gives none", `{app="a",cluster=~"ops-.*"}`, map[string]string{"app": "a"}},
		{"only regexp", `{app=~"a.*"}`, map[string]string{}},
		{"negative regexp", `{app="a",env!~"dev.*"}`, map[string]string{"app": "a"}},
		{"inequality", `{app="a",env!="dev"}`, map[string]string{"app": "a"}},
		{"equality and regexp on the same name", `{app="a",app=~".+"}`, map[string]string{}},
		{"equality and inequality on the same name", `{app="a",app!="b"}`, map[string]string{}},
		{"two equalities on the same name", `{app="a",app="b"}`, map[string]string{}},
		{"empty equality", `{app="",env="prod"}`, map[string]string{"env": "prod"}},
		{"empty then equality on the same name", `{app="",app="a"}`, map[string]string{"app": "a"}},
		{"line filter is not a matcher", `{app="a"} |= "x"`, map[string]string{"app": "a"}},
		{"parser and label filter are not matchers", `{app="a"} | json | env="prod"`, map[string]string{"app": "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := absentOverTimeLabels(tc.selector)
			if !mapsEqualString(got, tc.want) {
				t.Fatalf("labels of %s: got %v, want %v", tc.selector, got, tc.want)
			}
		})
	}
}

// The forms the absent handler takes: the log query, its range, and the vector
// aggregations around it; everything else is left to the other paths.
//
// conformance: semantics/absent-over-time-semantics
func TestParseAbsentOverTimeCompatSpecEdgeTable(t *testing.T) {
	for _, tc := range []struct {
		name, query  string
		ok           bool
		base, window string
		aggs         int
		presence     string
	}{
		{"selector", `absent_over_time({app="a"}[5m])`, true, `{app="a"}`, "5m", 0, `sum(count_over_time({app="a"}[5m]))`},
		{"line filter", `absent_over_time({app="a"} |= "x" [1m])`, true, `{app="a"} |= "x"`, "1m", 0, `sum(count_over_time({app="a"} |= "x"[1m]))`},
		{"parser", `absent_over_time({app="a"} | json [1m])`, true, `{app="a"} | json`, "1m", 0, `sum(count_over_time({app="a"} | json[1m]))`},
		{"parser and label filter", `absent_over_time({app="a"} | json | nonexistant_field="foo" [1m])`, true, `{app="a"} | json | nonexistant_field="foo"`, "1m", 0, ``},
		{"unwrap", `absent_over_time({app="a"} | logfmt | unwrap amount [1m])`, true, `{app="a"} | logfmt | unwrap amount`, "1m", 0, `sum(rate({app="a"} | logfmt | unwrap amount[1m]))`},
		{"unwrap with conversion", `absent_over_time({app="a"} | logfmt | unwrap duration(latency) [1m])`, true, ``, "1m", 0, `sum(rate(`},
		{"sum", `sum(absent_over_time({app="a"}[1m]))`, true, `{app="a"}`, "1m", 1, ``},
		{"sum by", `sum by (app) (absent_over_time({app="a"}[1m]))`, true, `{app="a"}`, "1m", 1, ``},
		{"nested", `max(sum without (pod) (absent_over_time({app="a"}[1m])))`, true, `{app="a"}`, "1m", 2, ``},
		{"topk", `topk(1, absent_over_time({app="a"}[1m]))`, true, `{app="a"}`, "1m", 1, ``},
		{"sort", `sort(absent_over_time({app="a"}[1m]))`, true, `{app="a"}`, "1m", 1, ``},
		{"sort_desc", `sort_desc(absent_over_time({app="a"}[1m]))`, true, `{app="a"}`, "1m", 1, ``},
		{"label_replace", `label_replace(absent_over_time({app="a"}[1m]), "dst", "$1", "app", "(.*)")`, true, `{app="a"}`, "1m", 0, ``},
		{"label_replace around sum by", `label_replace(sum by (app) (absent_over_time({app="a"}[1m])), "dst", "$1", "app", "(.*)")`, true, `{app="a"}`, "1m", 1, ``},
		{"binary expression is left alone", `absent_over_time({app="a"}[1m]) + absent_over_time({app="b"}[1m])`, false, ``, ``, 0, ``},
		{"another range function", `count_over_time({app="a"}[1m])`, false, ``, ``, 0, ``},
		{"no range", `absent_over_time({app="a"})`, false, ``, ``, 0, ``},
		{"bad range", `absent_over_time({app="a"}[bad])`, false, ``, ``, 0, ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, ok := parseAbsentOverTimeCompatSpec(tc.query)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (%+v)", ok, tc.ok, spec)
			}
			if !ok {
				return
			}
			if tc.base != "" && spec.baseQuery != tc.base {
				t.Fatalf("base = %q, want %q", spec.baseQuery, tc.base)
			}
			if spec.rangeWindowStr != tc.window || len(spec.aggs) != tc.aggs {
				t.Fatalf("window = %q, %d aggregations; want %q, %d", spec.rangeWindowStr, len(spec.aggs), tc.window, tc.aggs)
			}
			if tc.presence != "" && !strings.HasPrefix(spec.presenceQuery(), tc.presence) {
				t.Fatalf("presence query = %q, want prefix %q", spec.presenceQuery(), tc.presence)
			}
		})
	}
}

// The one absent series folded by vector aggregations, as Loki's
// VectorAggEvaluator folds a single series.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimeSeriesFoldEdgeTable(t *testing.T) {
	t.Run("topk of zero is empty", func(t *testing.T) {
		spec, _ := parseAbsentOverTimeCompatSpec(`topk(1, absent_over_time({app="a"}[1m]))`)
		spec.aggs[0].Param = 0
		if _, _, has := spec.series(); has {
			t.Fatal("topk(0) keeps no series")
		}
	})
	for _, tc := range []struct {
		name, query string
		labels      map[string]string
		value       string
		ok          bool
	}{
		{"bare", `absent_over_time({app="a",env="p"}[1m])`, map[string]string{"app": "a", "env": "p"}, "1", true},
		{"sum", `sum(absent_over_time({app="a",env="p"}[1m]))`, map[string]string{}, "1", true},
		{"sum by kept", `sum by (env) (absent_over_time({app="a",env="p"}[1m]))`, map[string]string{"env": "p"}, "1", true},
		{"sum by missing label", `sum by (zone) (absent_over_time({app="a",env="p"}[1m]))`, map[string]string{}, "1", true},
		{"sum without", `sum without (env) (absent_over_time({app="a",env="p"}[1m]))`, map[string]string{"app": "a"}, "1", true},
		{"count", `count(absent_over_time({app="a"}[1m]))`, map[string]string{}, "1", true},
		{"stddev", `stddev(absent_over_time({app="a"}[1m]))`, map[string]string{}, "0", true},
		{"stdvar by", `stdvar by (app) (absent_over_time({app="a"}[1m]))`, map[string]string{"app": "a"}, "0", true},
		{"count of stddev", `count(stddev(absent_over_time({app="a"}[1m])))`, map[string]string{}, "1", true},
		{"sum of stddev", `sum(stddev(absent_over_time({app="a"}[1m])))`, map[string]string{}, "0", true},
		{"sort keeps the series whole", `sort(absent_over_time({app="a",env="p"}[1m]))`, map[string]string{"app": "a", "env": "p"}, "1", true},
		{"sort_desc", `sort_desc(absent_over_time({app="a"}[1m]))`, map[string]string{"app": "a"}, "1", true},
		{"sort under sum by", `sort(sum by (env) (absent_over_time({app="a",env="p"}[1m])))`, map[string]string{"env": "p"}, "1", true},
		{"label_replace sets a label", `label_replace(absent_over_time({app="a"}[1m]), "dst", "x-$1", "app", "(.*)")`, map[string]string{"app": "a", "dst": "x-a"}, "1", true},
		{"label_replace that does not match", `label_replace(absent_over_time({app="a"}[1m]), "dst", "$1", "app", "zzz")`, map[string]string{"app": "a"}, "1", true},
		{"label_replace of a label the series lacks", `label_replace(absent_over_time({app=~"a"}[1m]), "dst", "v", "nope", "")`, map[string]string{"dst": "v"}, "1", true},
		{"label_replace after sum by", `label_replace(sum by (app) (absent_over_time({app="a",env="p"}[1m])), "dst", "$1", "app", "(.*)")`, map[string]string{"app": "a", "dst": "a"}, "1", true},
		{"nested label_replace, inner first", `label_replace(label_replace(absent_over_time({app="a"}[1m]), "b", "$1", "app", "(.*)"), "c", "$1", "b", "(.*)")`, map[string]string{"app": "a", "b": "a", "c": "a"}, "1", true},
		{"topk keeps the series whole", `topk(2, absent_over_time({app="a",env="p"}[1m]))`, map[string]string{"app": "a", "env": "p"}, "1", true},
		{"bottomk", `bottomk(1, absent_over_time({app="a"}[1m]))`, map[string]string{"app": "a"}, "1", true},
		{"regexp-only selector", `sum by (app) (absent_over_time({app=~"a.*"}[1m]))`, map[string]string{}, "1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, ok := parseAbsentOverTimeCompatSpec(tc.query)
			if !ok {
				t.Fatalf("not recognised: %s", tc.query)
			}
			labels, value, has := spec.series()
			if has != tc.ok {
				t.Fatalf("has series = %v, want %v", has, tc.ok)
			}
			if has && (value != tc.value || !mapsEqualString(labels, tc.labels)) {
				t.Fatalf("got %v = %s, want %v = %s", labels, value, tc.labels, tc.value)
			}
		})
	}
}

// vlRecorder answers every stats call with answer and records the queries.
type vlRecorder struct {
	mu      sync.Mutex
	paths   []string
	queries []string
	answer  string
	status  int
}

func (v *vlRecorder) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		v.mu.Lock()
		v.paths = append(v.paths, r.URL.Path)
		if strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query") {
			v.queries = append(v.queries, r.FormValue("query"))
		}
		v.mu.Unlock()
		if !strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query") {
			return // no rows, no labels
		}
		if v.status != 0 {
			http.Error(w, "boom", v.status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(v.answer))
	})
}

func absentRangeRequest(t *testing.T, vl *vlRecorder, query string, headers map[string]string) (*httptest.ResponseRecorder, []absentSeriesOut) {
	t.Helper()
	backend := httptest.NewServer(vl.handler())
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	params := url.Values{"query": {query}, "start": {"1800000000"}, "end": {"1800000240"}, "step": {"60"}}
	hdr := map[string]string{"X-Scope-OrgID": "0"}
	for k, v := range headers {
		hdr[k] = v
	}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), hdr)
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	_ = json.Unmarshal(res.Body.Bytes(), &resp)
	var out []absentSeriesOut
	for _, s := range resp.Data.Result {
		o := absentSeriesOut{metric: s.Metric, points: map[int64]string{}}
		for _, pt := range s.Values {
			o.points[int64(pt[0].(float64))*1e9] = pt[1].(string)
		}
		out = append(out, o)
	}
	return res, out
}

// The registry row: with a parser stage the answer used to be inverted, a
// series per stream with 0 where data exists and nothing where no line matches.
// Data present is no series; a label filter no line passes is the series at
// every step, labelled by the selector's equality matchers; the label filter
// reaches VictoriaLogs, where the presence is counted.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimeParserStageEdgeTable(t *testing.T) {
	empty := `{"status":"success","data":{"resultType":"matrix","result":[]}}`
	data := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c"},"values":[[1799999940,"2"],[1800000000,"1"],[1800000060,"4"],[1800000120,"3"],[1800000180,"1"]]}]}}`
	every := []int64{1800000000e9, 1800000060e9, 1800000120e9, 1800000180e9, 1800000240e9}
	for _, tc := range []struct {
		name, query string
		answer      string
		wantSeries  int
		wantSteps   []int64
		mustSend    string // a fragment of the VictoriaLogs query
		rawRows     bool   // the existing JSON route may read rows to apply Loki's parse
	}{
		{"json, data present", `absent_over_time({service_name="api-gateway"} | json [1m])`, data, 0, nil, "stats", false},
		{"json then a label filter no line passes", `absent_over_time({service_name="api-gateway"} | json | nonexistant_field="foo" [1m])`, empty, 1, every, "nonexistant_field", true},
		{"json, no data at all", `absent_over_time({service_name="api-gateway"} | json [1m])`, empty, 1, every, "service_name", false},
		{"logfmt then a label filter, data present", `absent_over_time({service_name="api-gateway"} | logfmt | level="error" [1m])`, data, 0, nil, "level", false},
		{"line filter, data present", `absent_over_time({service_name="api-gateway"} |= "x" [1m])`, data, 0, nil, `"x"`, false},
		{"selector, data present", `absent_over_time({service_name="api-gateway"}[1m])`, data, 0, nil, "service_name", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vl := &vlRecorder{answer: tc.answer}
			res, got := absentRangeRequest(t, vl, tc.query, nil)
			if res.Code != http.StatusOK {
				t.Fatalf("%d %s", res.Code, res.Body)
			}
			if len(got) != tc.wantSeries {
				t.Fatalf("%d series, want %d: %s", len(got), tc.wantSeries, res.Body)
			}
			if tc.wantSeries == 1 {
				if !mapsEqualString(got[0].metric, map[string]string{"service_name": "api-gateway"}) {
					t.Fatalf("labels %v", got[0].metric)
				}
				if len(got[0].points) != len(tc.wantSteps) {
					t.Fatalf("points %v, want steps %v", got[0].points, tc.wantSteps)
				}
				for _, ts := range tc.wantSteps {
					if got[0].points[ts] != "1" {
						t.Fatalf("no 1 at %d: %v", ts/1e9, got[0].points)
					}
				}
			}
			if !tc.rawRows {
				for _, path := range vl.paths {
					if path == "/select/logsql/query" {
						t.Fatalf("raw rows were read: %v", vl.paths)
					}
				}
			}
			joined := strings.Join(vl.queries, "\n")
			if !strings.Contains(joined, tc.mustSend) {
				t.Fatalf("VictoriaLogs never saw %q:\n%s", tc.mustSend, joined)
			}
		})
	}
}

// An answer that is not the presence of samples must not be read as their
// absence: a failed VictoriaLogs call stays a sanitized error for API and
// Grafana traffic alike, never a series of ones firing every absence alert.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimeDoesNotInvertFailedPresence(t *testing.T) {
	q := `absent_over_time({app="a"} | json [1m])`
	for _, client := range []struct {
		name    string
		headers map[string]string
	}{
		{"api", nil},
		{"grafana", map[string]string{"User-Agent": "Grafana/11.0"}},
	} {
		t.Run(client.name, func(t *testing.T) {
			res, got := absentRangeRequest(t, &vlRecorder{status: http.StatusBadGateway}, q, client.headers)
			if res.Code != http.StatusInternalServerError || len(got) != 0 {
				t.Fatalf("backend failure was inverted or misclassified: %d %s samples=%v", res.Code, res.Body, got)
			}
			var body struct {
				Status, Error string
				Warnings      []string
			}
			if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil || body.Status != "error" || body.Error != lokiErrInternal || len(body.Warnings) != 0 {
				t.Fatalf("expected sanitized Loki error, got %s", res.Body)
			}
			if res.Header().Get("Warning") != "" || res.Header().Get(drilldownPartialUpstreamStatusHeader) != "" {
				t.Fatalf("backend failure was labelled a bounds partial: %v", res.Header())
			}
		})
	}
}

// The instant form: a vector with the series when the window (time-range, time]
// holds no sample, an empty vector otherwise, and a window ending before older
// data is still empty of it.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimeInstantEdgeTable(t *testing.T) {
	for _, tc := range []struct {
		name, query, answer string
		want                int
	}{
		{"parser, no data", `absent_over_time({app="a"} | json [1m])`, `{"status":"success","data":{"resultType":"vector","result":[]}}`, 1},
		{"parser, data", `absent_over_time({app="a"} | json [1m])`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1800000000,"3"]}]}}`, 0},
		{"a count of zero is no data", `absent_over_time({app="a"}[1m])`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1800000000,"0"]}]}}`, 1},
		{"unwrapped sum of zero is data", `absent_over_time({app="a"} | logfmt | unwrap n [1m])`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1800000000,"0"]}]}}`, 0},
		{"under sum", `sum(absent_over_time({app="a"}[1m]))`, `{"status":"success","data":{"resultType":"vector","result":[]}}`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vl := &vlRecorder{answer: tc.answer}
			backend := httptest.NewServer(vl.handler())
			defer backend.Close()
			p := newTestProxy(t, backend.URL)
			params := url.Values{"query": {tc.query}, "time": {"1800000000"}}
			res := doCompatProxyRequest(p, "/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			var resp struct {
				Data struct {
					Result []lokiVectorResult `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil || res.Code != http.StatusOK {
				t.Fatalf("%d %s", res.Code, res.Body)
			}
			if len(resp.Data.Result) != tc.want {
				t.Fatalf("%d series, want %d: %s", len(resp.Data.Result), tc.want, res.Body)
			}
			if tc.want == 1 {
				if v := resp.Data.Result[0].Value; v[1] != "1" || v[0].(float64) != 1.8e9 {
					t.Fatalf("sample %v", v)
				}
			}
		})
	}
}

func TestAbsentOverTimeMissingRangeWindowIsBadRequest(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	for _, instant := range []bool{false, true} {
		req := httptest.NewRequest("GET", "/loki/api/v1/query?query=x&time=1800000000", nil)
		resp := httptest.NewRecorder()
		spec := absentOverTimeCompatSpec{baseQuery: `{app="a"}`, rangeWindow: time.Minute}
		if instant {
			p.proxyAbsentOverTimeQuery(resp, req, time.Now(), `absent_over_time({app="a"}[1m])`, spec)
		} else {
			p.proxyAbsentOverTimeQueryRange(resp, req, time.Now(), `absent_over_time({app="a"}[1m])`, spec)
		}
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("instant=%v: %d %s", instant, resp.Code, resp.Body)
		}
	}
}

// The log query inside absent_over_time reaches the presence query intact
// whatever stages it holds, and the presence query is itself valid LogQL.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimePresenceQueryKeepsEveryStage(t *testing.T) {
	for _, inner := range []string{
		`{app="a"} |= "e" != "x" |~ "a.*b" !~ "zz"`,
		`{app="a"} | json a="b", c="d.e"`,
		`{app="a"} | logfmt | level=~"err.*"`,
		`{app="a"} | pattern "<_> <lvl> <msg>" | lvl="info"`,
		`{app="a"} | regexp "(?P<x>\\d+)" | x > 5`,
		`{app="a"} | json | line_format "{{.msg}}" | label_format dst=src`,
		`{app="a"} | json | drop level | keep app, msg`,
		`{app="a"} | json | latency > 250ms and status >= 500`,
		`{app="a"} | logfmt | unwrap bytes(size)`,
		`{app="a"} | logfmt | unwrap duration(latency) | __error__=""`,
		`{app="a"} | unpack | decolorize`,
	} {
		query := `absent_over_time(` + inner + ` [5m])`
		spec, ok := parseAbsentOverTimeCompatSpec(query)
		if !ok {
			t.Fatalf("not recognised: %s", query)
		}
		presence := spec.presenceQuery()
		if _, err := logqlpkg.Parse(presence); err != nil {
			t.Fatalf("%s: the presence query %s does not parse: %v", query, presence, err)
		}
		if !strings.Contains(presence, "[5m]") {
			t.Fatalf("%s: lost the range: %s", query, presence)
		}
	}
}

// `absent_over_time(...) by (...)` is not LogQL: Loki's validation rejects a
// grouping on absent_over_time (pkg/logql/syntax/ast.go:1439-1447, wrapped as
// a parse error at :1409), whatever is around it.
//
// conformance: semantics/absent-over-time-semantics, status-400
func TestAbsentOverTimeGroupingIsLokisParseError(t *testing.T) {
	const want = "parse error : grouping not allowed for absent_over_time aggregation"
	for _, q := range []string{
		`absent_over_time({app="a"}[1m]) by (app)`,
		`absent_over_time({app="a"} | json [1m]) without (app)`,
		`sum(absent_over_time({app="a"}[1m]) by (app))`,
	} {
		for _, path := range []string{"/loki/api/v1/query_range?start=1800000000&end=1800000240&step=60&", "/loki/api/v1/query?time=1800000000&"} {
			vl := &vlRecorder{answer: `{"status":"success","data":{"resultType":"vector","result":[]}}`}
			backend := httptest.NewServer(vl.handler())
			p := newTestProxy(t, backend.URL)
			res := doCompatProxyRequest(p, path+"query="+url.QueryEscape(q), map[string]string{"X-Scope-OrgID": "0"})
			backend.Close()
			if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), want) {
				t.Fatalf("%s %s: %d %s", path, q, res.Code, res.Body)
			}
			if len(vl.paths) != 0 {
				t.Fatalf("%s: VictoriaLogs was asked: %v", q, vl.paths)
			}
		}
	}
}

// The Drilldown series-limit partial is a 200 whose body, not its headers,
// carries the warning (legacy partial-failure envelopes, addSeriesLimitWarning);
// the presence of samples read from it is incomplete, so it is passed on.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimePassesOnWarningInBody(t *testing.T) {
	warned := `{"status":"success","data":{"resultType":"matrix","result":[]},"warnings":["Warning: maximum series limit reached"]}`
	if !absentPresenceDegraded(http.Header{}, []byte(warned)) {
		t.Fatal("a body warning is a degraded answer")
	}
	if absentPresenceDegraded(http.Header{}, []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)) {
		t.Fatal("a clean body is not degraded")
	}
	p := newTestProxy(t, "http://unused")
	spec, _ := parseAbsentOverTimeCompatSpec(`absent_over_time({app="a"}[1m])`)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?query=x&start=1800000000&end=1800000240&step=60", nil)
	rec := httptest.NewRecorder()
	body, ok := p.presenceAnswer(rec, req, spec, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(warned))
	})
	if ok || body != nil || rec.Code != http.StatusOK || rec.Body.String() != warned {
		t.Fatalf("the warned answer was not passed on as it is: ok=%v %d %s", ok, rec.Code, rec.Body)
	}
}

// Loki evaluates a multi-tenant absent_over_time once over the tenants' merged
// samples (pkg/querier/multi_tenant_querier.go MultiTenantQuerier.SelectSamples
// merges one sample iterator per tenant before AbsentRangeVectorEvaluator runs),
// so a step is absent only when no tenant has a line there, and the series is
// labelled by the selector alone: absentLabels (evaluator.go:1320-1345) never
// names __tenant_id__. Tenant b is empty: it must not make the step absent.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimeMultiTenant(t *testing.T) {
	// Tenant a (AccountID 10) holds lines while aHasData is set; tenant b (20)
	// never does.
	var aHasData atomic.Bool
	aHasData.Store(true)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		data := r.Header.Get("AccountID") == "10" && aHasData.Load()
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			if data {
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c"},"values":[[1799999940,"2"],[1800000000,"1"]]}]}}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		default:
			if data {
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"c"},"value":[1800000000,"3"]}]}}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		}
	}))
	defer backend.Close()
	_, _, mux := newTwoTenantProxy(t, backend.URL)
	get := func(target string) (int, []absentSeriesOut, string) {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Values [][]any           `json:"values"`
					Value  []any             `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		var out []absentSeriesOut
		for _, s := range resp.Data.Result {
			o := absentSeriesOut{metric: s.Metric, points: map[int64]string{}}
			vs := s.Values
			if s.Value != nil {
				vs = [][]any{s.Value}
			}
			for _, v := range vs {
				o.points[int64(v[0].(float64))*1e9] = v[1].(string)
			}
			out = append(out, o)
		}
		return rec.Code, out, rec.Body.String()
	}
	rng := "/loki/api/v1/query_range?start=1800000000&end=1800000240&step=60&query="
	inst := "/loki/api/v1/query?time=1800000000&query="
	sel := `absent_over_time({app="a"}[1m])`

	t.Run("data in one tenant: its steps are not absent for the pair", func(t *testing.T) {
		// Tenant a's presence covers 1800000000 and 1800000060 (a tumbling bucket
		// is relabelled one step on); the steps after are empty in both tenants.
		code, got, body := get(rng + url.QueryEscape(sel))
		if code != http.StatusOK || len(got) != 1 {
			t.Fatalf("%d %s", code, body)
		}
		if !mapsEqualString(got[0].metric, map[string]string{"app": "a"}) {
			t.Fatalf("labels %v: want the selector's equality matchers alone, no __tenant_id__", got[0].metric)
		}
		for _, ts := range []int64{1800000000e9, 1800000060e9} {
			if _, present := got[0].points[ts]; present {
				t.Fatalf("step %d is absent for the pair although tenant a has data: %v", ts/1e9, got[0].points)
			}
		}
		if got[0].points[1800000120e9] != "1" || got[0].points[1800000240e9] != "1" {
			t.Fatalf("steps without data in either tenant must be 1: %v", got[0].points)
		}
	})
	t.Run("instant, data in one tenant: nothing", func(t *testing.T) {
		code, got, body := get(inst + url.QueryEscape(sel))
		if code != http.StatusOK || len(got) != 0 {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("under a vector aggregation", func(t *testing.T) {
		code, got, body := get(rng + url.QueryEscape(`sum by (app) (`+sel+`)`))
		if code != http.StatusOK || len(got) != 1 || !mapsEqualString(got[0].metric, map[string]string{"app": "a"}) || len(got[0].points) != 3 {
			t.Fatalf("%d %s", code, body)
		}
	})
	aHasData.Store(false)
	sel = `absent_over_time({app="a"}[2m])` // another window: the answers above are cached
	t.Run("no data in either tenant: one series without __tenant_id__ at every step", func(t *testing.T) {
		code, got, body := get(rng + url.QueryEscape(sel))
		if code != http.StatusOK || len(got) != 1 {
			t.Fatalf("%d %s", code, body)
		}
		if !mapsEqualString(got[0].metric, map[string]string{"app": "a"}) || len(got[0].points) != 5 {
			t.Fatalf("labels %v, %d steps; want {app=a} at 5 steps", got[0].metric, len(got[0].points))
		}
	})
	t.Run("instant, no data in either tenant: one sample without __tenant_id__", func(t *testing.T) {
		code, got, body := get(inst + url.QueryEscape(sel))
		if code != http.StatusOK || len(got) != 1 || !mapsEqualString(got[0].metric, map[string]string{"app": "a"}) {
			t.Fatalf("%d %s", code, body)
		}
	})
}
