package proxy

import (
	"encoding/json"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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
		{"sort is left alone", `sort(absent_over_time({app="a"}[1m]))`, false, ``, ``, 0, ``},
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
// absence: a failed VictoriaLogs call is its error, and the partial-result
// reply Grafana-sourced traffic gets for one is passed on, not inverted into a
// series of ones that would fire every absence alert.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentOverTimeDoesNotInvertFailedPresence(t *testing.T) {
	q := `absent_over_time({app="a"} | json [1m])`
	t.Run("backend error", func(t *testing.T) {
		res, got := absentRangeRequest(t, &vlRecorder{status: http.StatusBadGateway}, q, nil)
		if res.Code < 400 || len(got) != 0 {
			t.Fatalf("%d %s", res.Code, res.Body)
		}
	})
	t.Run("grafana partial result", func(t *testing.T) {
		res, got := absentRangeRequest(t, &vlRecorder{status: http.StatusBadGateway}, q, map[string]string{"User-Agent": "Grafana/11.0"})
		if res.Header().Get("Warning") == "" {
			t.Fatalf("the partial-result warning was dropped: %d %v %s", res.Code, res.Header(), res.Body)
		}
		if len(got) != 0 {
			t.Fatalf("a degraded answer was inverted into %v", got)
		}
	})
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
