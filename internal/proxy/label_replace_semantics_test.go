package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// label_replace adds, rewrites or removes one label of every series its
// expression yields. Loki v3.7.7:
//
//   - pkg/logql/syntax/ast.go mustNewLabelReplaceExpr compiles the regexp as
//     `^(?:regex)$`, so it must match the whole source value; an invalid one is
//     the parse error "invalid regex in label_replace: ...";
//   - pkg/logql/evaluator.go LabelReplaceEvaluator.Next reads the source label
//     (empty when absent), leaves the series as it is when the regexp does not
//     match, otherwise expands the replacement with Regexp.ExpandString ($1,
//     ${1}, $name, ${name}, $$) and deletes the destination label before
//     setting it when the expansion is not empty. It checks no label name and
//     raises no error for two series that end with one label set;
//   - pkg/logql/engine.go JoinSampleVector keeps those series apart on an
//     instant query (one sample each) and, on a range query, files their
//     points under one series (vectorsToSeries keys by label hash, appending
//     the points of each step in turn).
//
// The proxy applies the transform to the series VictoriaLogs aggregated, so an
// instant vector and a range matrix go through the same function.

// lrSeries is one series of a decoded metric answer: labels and points, a
// matrix series' values or an instant vector's single value.
type lrSeries struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value,omitempty"`
	Values [][]any           `json:"values,omitempty"`
}

type lrAnswer struct {
	code       int
	resultType string
	series     []lrSeries
	body       string
}

func decodeLR(t *testing.T, code int, body []byte) lrAnswer {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string     `json:"resultType"`
			Result     []lrSeries `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("answer is not JSON (HTTP %d): %v\n%s", code, err, body)
	}
	return lrAnswer{code: code, resultType: resp.Data.ResultType, series: resp.Data.Result, body: string(body)}
}

// lrKey is a canonical rendering of a label set.
func lrKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k + "=" + strconv.Quote(m[k]))
	}
	b.WriteByte('}')
	return b.String()
}

// lrBackend is a VictoriaLogs that answers every stats query with series
// carrying exactly the given label sets, two points each on a range query.
func lrBackend(t *testing.T, sets []map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if q := r.FormValue("query"); strings.Contains(q, "__lvp_lr:") || strings.Contains(q, "__lvp_lj:") || strings.Contains(q, "__binary__") || strings.Contains(q, "|||") {
			t.Errorf("an internal marker reached VictoriaLogs: %s", q)
		}
		var out []string
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			for i, m := range sets {
				mb, _ := json.Marshal(m)
				out = append(out, fmt.Sprintf(`{"metric":%s,"values":[[1704067200,"%d"],[1704067260,"%d"]]}`, mb, i+1, i+2))
			}
			_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[%s]}}`, strings.Join(out, ","))
		case "/select/logsql/stats_query":
			for i, m := range sets {
				mb, _ := json.Marshal(m)
				out = append(out, fmt.Sprintf(`{"metric":%s,"value":[1704067200,"%d"]}`, mb, i+1))
			}
			_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[%s]}}`, strings.Join(out, ","))
		default:
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func lrAsk(t *testing.T, p *Proxy, endpoint, q string) lrAnswer {
	t.Helper()
	return lrAskAs(t, p, endpoint, q, nil)
}

func lrAskAs(t *testing.T, p *Proxy, endpoint, q string, headers map[string]string) lrAnswer {
	t.Helper()
	v := url.Values{"query": {q}, "start": {"1704067200"}, "end": {"1704067260"}, "step": {"60"}, "time": {"1704067260"}}
	req := httptest.NewRequest("GET", "/loki/api/v1/"+endpoint+"?"+v.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	for k, val := range headers {
		req.Header.Set(k, val)
	}
	rec := httptest.NewRecorder()
	if endpoint == "query" {
		p.handleQuery(rec, req)
	} else {
		p.handleQueryRange(rec, req)
	}
	return decodeLR(t, rec.Code, rec.Body.Bytes())
}

// lrReference is Loki's label_replace written apart from the proxy: the
// evaluator step (LabelReplaceEvaluator.Next) on one label set.
func lrReference(in map[string]string, dst, repl, src, regex string) map[string]string {
	re := regexp.MustCompile("^(?:" + regex + ")$")
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	s := in[src]
	idx := re.FindStringSubmatchIndex(s)
	if idx == nil {
		return out
	}
	res := re.ExpandString(nil, repl, s, idx)
	delete(out, dst)
	if len(res) > 0 {
		out[dst] = string(res)
	}
	return out
}

// lrReferenceJoin is JoinSampleVector for the series of a transformed answer:
// an instant vector keeps every sample, a range matrix files the points of the
// series that now share a label set under one series, step by step.
func lrReferenceJoin(base []lrSeries, instant bool, f func(map[string]string) map[string]string) map[string][]string {
	out := map[string][]string{}
	if instant {
		for _, s := range base {
			k := lrKey(f(s.Metric))
			out[k] = append(out[k], fmt.Sprint(s.Value))
		}
		for k := range out {
			sort.Strings(out[k])
		}
		return out
	}
	steps := map[string]bool{}
	for _, s := range base {
		for _, v := range s.Values {
			steps[fmt.Sprint(v[0])] = true
		}
	}
	order := make([]string, 0, len(steps))
	for ts := range steps {
		order = append(order, ts)
	}
	sort.Slice(order, func(i, j int) bool {
		a, _ := strconv.ParseFloat(order[i], 64)
		b, _ := strconv.ParseFloat(order[j], 64)
		return a < b
	})
	for _, ts := range order {
		for _, s := range base {
			for _, v := range s.Values {
				if fmt.Sprint(v[0]) == ts {
					k := lrKey(f(s.Metric))
					out[k] = append(out[k], fmt.Sprint(v))
				}
			}
		}
	}
	return out
}

func lrGot(a lrAnswer, instant bool) map[string][]string {
	out := map[string][]string{}
	for _, s := range a.series {
		k := lrKey(s.Metric)
		if instant {
			out[k] = append(out[k], fmt.Sprint(s.Value))
			continue
		}
		for _, v := range s.Values {
			out[k] = append(out[k], fmt.Sprint(v))
		}
	}
	if instant {
		for k := range out {
			sort.Strings(out[k])
		}
	}
	return out
}

// The edge table: every rule of the evaluator step, on both answer shapes.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceEdgeTable(t *testing.T) {
	type tc struct {
		name              string
		in                map[string]string
		dst, repl, src, r string
		want              map[string]string
	}
	cases := []tc{
		{"adds the destination", map[string]string{"app": "api-gateway"}, "app_short", "$1", "app", "(.*)-.*", map[string]string{"app": "api-gateway", "app_short": "api"}},
		{"regexp is anchored: a prefix does not match", map[string]string{"app": "api-gateway"}, "d", "x", "app", "api", map[string]string{"app": "api-gateway"}},
		{"regexp is anchored: a suffix does not match", map[string]string{"app": "api-gateway"}, "d", "x", "app", "gateway", map[string]string{"app": "api-gateway"}},
		{"regexp is anchored: the whole value matches", map[string]string{"app": "api"}, "d", "x", "app", "api", map[string]string{"app": "api", "d": "x"}},
		{"alternation is anchored as a whole", map[string]string{"app": "bc"}, "d", "x", "app", "a|bc", map[string]string{"app": "bc", "d": "x"}},
		{"alternation does not leak the anchors", map[string]string{"app": "abc"}, "d", "x", "app", "a|bc", map[string]string{"app": "abc"}},
		{"no match keeps the series and an existing destination", map[string]string{"app": "web", "d": "old"}, "d", "$1", "app", "(.*)-.*", map[string]string{"app": "web", "d": "old"}},
		{"empty replacement removes the destination", map[string]string{"app": "web", "d": "old"}, "d", "", "app", ".*", map[string]string{"app": "web"}},
		{"empty replacement without a destination changes nothing", map[string]string{"app": "web"}, "d", "", "app", ".*", map[string]string{"app": "web"}},
		{"an empty expansion removes the destination", map[string]string{"app": "web", "d": "old"}, "d", "$1", "app", "web()", map[string]string{"app": "web"}},
		{"braced group", map[string]string{"app": "api-gateway"}, "d", "${1}_x", "app", "(.*)-.*", map[string]string{"app": "api-gateway", "d": "api_x"}},
		{"bare group name swallows the suffix", map[string]string{"app": "api-gateway"}, "d", "$1x", "app", "(.*)-.*", map[string]string{"app": "api-gateway"}},
		{"named group", map[string]string{"app": "api-gateway"}, "d", "${head}", "app", "(?P<head>[a-z]+)-.*", map[string]string{"app": "api-gateway", "d": "api"}},
		{"bare named group", map[string]string{"app": "api-gateway"}, "d", "$head.", "app", "(?P<head>[a-z]+)-.*", map[string]string{"app": "api-gateway", "d": "api."}},
		{"two groups", map[string]string{"app": "api-gateway"}, "d", "$2/$1", "app", "([a-z]+)-([a-z]+)", map[string]string{"app": "api-gateway", "d": "gateway/api"}},
		{"unknown group expands to nothing", map[string]string{"app": "api", "d": "old"}, "d", "$2", "app", "(.*)", map[string]string{"app": "api"}},
		{"dollar escape", map[string]string{"app": "api"}, "d", "$$1", "app", "(.*)", map[string]string{"app": "api", "d": "$1"}},
		{"constant replacement", map[string]string{"app": "api"}, "d", "const", "app", ".*", map[string]string{"app": "api", "d": "const"}},
		{"missing source is empty: a regexp that matches empty applies", map[string]string{"app": "api"}, "d", "dflt", "nope", "(.*)", map[string]string{"app": "api", "d": "dflt"}},
		{"missing source is empty: a regexp that needs a character does not", map[string]string{"app": "api"}, "d", "dflt", "nope", ".+", map[string]string{"app": "api"}},
		{"empty regexp matches an empty source", map[string]string{"app": "api"}, "d", "dflt", "nope", "", map[string]string{"app": "api", "d": "dflt"}},
		{"destination equal to the source", map[string]string{"app": "api-gateway"}, "app", "$1", "app", "(.*)-.*", map[string]string{"app": "api"}},
		{"destination equal to the source, removed", map[string]string{"app": "api", "b": "1"}, "app", "", "app", ".*", map[string]string{"b": "1"}},
		{"dot does not match a newline", map[string]string{"app": "a\nb"}, "d", "x", "app", ".*", map[string]string{"app": "a\nb"}},
		{"flag lets dot match a newline", map[string]string{"app": "a\nb"}, "d", "x", "app", "(?s).*", map[string]string{"app": "a\nb", "d": "x"}},
		{"label names are not validated", map[string]string{"app": "api"}, "not a label", "x", "app", ".*", map[string]string{"app": "api", "not a label": "x"}},
		{"dotted destination", map[string]string{"app": "api"}, "service.name", "$0", "app", ".*", map[string]string{"app": "api", "service.name": "api"}},
		{"group zero is the whole match", map[string]string{"app": "api"}, "d", "<$0>", "app", "a(p)i", map[string]string{"app": "api", "d": "<api>"}},
		{"unicode value", map[string]string{"app": "żółć-1"}, "d", "$1", "app", "(.*)-.*", map[string]string{"app": "żółć-1", "d": "żółć"}},
	}
	for _, c := range cases {
		spec := translator.LabelReplaceSpec{DstLabel: c.dst, Replacement: c.repl, SrcLabel: c.src, Regex: c.r}
		for _, shape := range []string{"matrix", "vector"} {
			t.Run(c.name+"/"+shape, func(t *testing.T) {
				mb, _ := json.Marshal(c.in)
				var body string
				if shape == "matrix" {
					body = fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":%s,"values":[[1704067200,"1"]]}],"stats":{"x":1}}}`, mb)
				} else {
					body = fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":[{"metric":%s,"value":[1704067200,"1"]}],"stats":{"x":1}}}`, mb)
				}
				out := applyLabelReplace([]byte(body), spec)
				a := decodeLR(t, 200, out)
				if len(a.series) != 1 || lrKey(a.series[0].Metric) != lrKey(c.want) {
					t.Fatalf("labels %s, want %s\n%s", lrKey(a.series[0].Metric), lrKey(c.want), out)
				}
				if a.resultType != shape {
					t.Errorf("resultType %q, want %q", a.resultType, shape)
				}
				if !strings.Contains(string(out), `"stats":{"x":1}`) {
					t.Errorf("the rest of the answer is not kept: %s", out)
				}
				if ref := lrReference(c.in, c.dst, c.repl, c.src, c.r); lrKey(ref) != lrKey(c.want) {
					t.Fatalf("test table disagrees with the reference: %s vs %s", lrKey(ref), lrKey(c.want))
				}
			})
		}
	}
}

// Two series that end with one label set: no error (the evaluator checks
// nothing); an instant vector holds both samples, a range matrix one series
// with the points of each step in turn.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceDuplicateLabelSets(t *testing.T) {
	spec := translator.LabelReplaceSpec{DstLabel: "app", Replacement: "all", SrcLabel: "app", Regex: ".*"}
	matrix := `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"app":"a"},"values":[[1,"1"],[2,"2"]]},{"metric":{"app":"b"},"values":[[1,"10"],[2,"20"]]},{"metric":{"app":"c","k":"v"},"values":[[1,"7"]]}]}}`
	got := decodeLR(t, 200, applyLabelReplace([]byte(matrix), spec))
	if len(got.series) != 2 {
		t.Fatalf("range: %d series, want the two duplicates filed under one\n%s", len(got.series), got.body)
	}
	for _, s := range got.series {
		if s.Metric["app"] == "all" && len(s.Metric) == 1 {
			if v := fmt.Sprint(s.Values); v != "[[1 1] [1 10] [2 2] [2 20]]" {
				t.Errorf("merged points %s, want the points of each step in turn", v)
			}
		}
	}
	vector := `{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{"app":"a"},"value":[1,"1"]},{"metric":{"app":"b"},"value":[1,"10"]}]}}`
	got = decodeLR(t, 200, applyLabelReplace([]byte(vector), spec))
	if len(got.series) != 2 || lrKey(got.series[0].Metric) != lrKey(got.series[1].Metric) {
		t.Fatalf("instant: want both samples under the shared label set\n%s", got.body)
	}
}

// The registry shape and the shapes around it, on both endpoints.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceOnInstantAndRange(t *testing.T) {
	sets := []map[string]string{{"app": "api-gateway"}, {"app": "web"}}
	p := newTestProxy(t, lrBackend(t, sets).URL)
	count := `count_over_time({app="api-gateway"}[5m])`
	agg := `sum by (app) (` + count + `)`
	lr := func(inner, dst, repl, src, re string) string {
		return fmt.Sprintf(`label_replace(%s, %q, %q, %q, %q)`, inner, dst, repl, src, re)
	}
	cases := []struct {
		name  string
		query string
		want  []map[string]string
	}{
		{"over an aggregation", lr(agg, "app_short", "$1", "app", "(.*)-.*"), []map[string]string{{"app": "api-gateway", "app_short": "api"}, {"app": "web"}}},
		{"nested", lr(lr(agg, "a1", "$1", "app", "(.*)-.*"), "a2", "x$1", "a1", "(.*)"), []map[string]string{{"app": "api-gateway", "a1": "api", "a2": "xapi"}, {"app": "web", "a2": "x"}}},
		{"nested, the outer reads what the inner removed", lr(lr(agg, "app", "", "app", "web"), "w", "gone", "app", ""), []map[string]string{{"app": "api-gateway"}, {"w": "gone"}}},
		{"escaped regexp", lr(agg, "d", "$1", "app", `([a-z]+)\-(\w+)`), []map[string]string{{"app": "api-gateway", "d": "api"}, {"app": "web"}}},
		{"escaped dot", lr(agg, "d", "$1", "app", `(\w+)\.(\w+)`), []map[string]string{{"app": "api-gateway"}, {"app": "web"}}},
		{"over a binary expression", lr(agg+` * 2`, "app_short", "$1", "app", "(.*)-.*"), []map[string]string{{"app": "api-gateway", "app_short": "api"}, {"app": "web"}}},
		{"over a binary of two aggregations", lr(agg+` / `+agg, "app_short", "$1", "app", "(.*)-.*"), []map[string]string{{"app": "api-gateway", "app_short": "api"}, {"app": "web"}}},
	}
	for _, c := range cases {
		for _, endpoint := range []string{"query_range", "query"} {
			t.Run(c.name+"/"+endpoint, func(t *testing.T) {
				a := lrAsk(t, p, endpoint, c.query)
				if a.code != 200 {
					t.Fatalf("HTTP %d: %s", a.code, a.body)
				}
				want := "matrix"
				if endpoint == "query" {
					want = "vector"
				}
				if a.resultType != want {
					t.Fatalf("resultType %q, want %q\n%s", a.resultType, want, a.body)
				}
				got, exp := map[string]bool{}, map[string]bool{}
				for _, s := range a.series {
					got[lrKey(s.Metric)] = true
				}
				for _, m := range c.want {
					exp[lrKey(m)] = true
				}
				if fmt.Sprint(got) != fmt.Sprint(exp) {
					t.Fatalf("label sets %v, want %v\n%s", got, exp, a.body)
				}
			})
		}
	}
}

// topk, bottomk, sort and stddev rank or fold the series label_replace
// produced: the transform runs on what VictoriaLogs answered, then the
// ranking. The translator's marker never reaches VictoriaLogs.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceUnderPostAggregation(t *testing.T) {
	sets := []map[string]string{{"app": "api-gateway"}, {"app": "web"}}
	p := newTestProxy(t, lrBackend(t, sets).URL)
	lr := `label_replace(sum by (app) (count_over_time({app="x"}[5m])), "app_short", "$1", "app", "(.*)-.*")`
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`topk(2, ` + lr + `)`, []string{`{app="api-gateway",app_short="api"}`, `{app="web"}`}},
		{`bottomk(1, ` + lr + `)`, []string{`{app="api-gateway",app_short="api"}`}},
		{`sort(` + lr + `)`, []string{`{app="api-gateway",app_short="api"}`, `{app="web"}`}},
		{`sort_desc(` + lr + `)`, []string{`{app="api-gateway",app_short="api"}`, `{app="web"}`}},
	} {
		for _, endpoint := range []string{"query_range", "query"} {
			if endpoint == "query_range" && (strings.HasPrefix(tc.query, "topk") || strings.HasPrefix(tc.query, "bottomk")) {
				continue // ranks per step from window statistics, which this backend does not emulate
			}
			a := lrAsk(t, p, endpoint, tc.query)
			got := map[string]bool{}
			for _, s := range a.series {
				got[lrKey(s.Metric)] = true
			}
			if a.code != 200 || len(got) != len(tc.want) {
				t.Errorf("%s %s: HTTP %d, series %v, want %v\n%s", endpoint, tc.query, a.code, got, tc.want, a.body)
				continue
			}
			for _, w := range tc.want {
				if !got[w] {
					t.Errorf("%s %s: series %v, want %v\n%s", endpoint, tc.query, got, tc.want, a.body)
				}
			}
		}
	}
	for _, endpoint := range []string{"query_range", "query"} {
		a := lrAsk(t, p, endpoint, `sort(label_replace(sum by (app) (count_over_time({app="x"}[5m])), "d", "$1", "app", "(unclosed"))`)
		if a.code != http.StatusBadRequest || !strings.Contains(a.body, "parse error : invalid regex in label_replace:") {
			t.Errorf("%s: HTTP %d, want Loki's parse error\n%s", endpoint, a.code, a.body)
		}
	}
}

// The registry row's query, as curl, Grafana Explore and Logs Drilldown send it
// (Drilldown range answers pass through the series-limit rewriter after the
// transform), with Loki's categorize-labels encoding flag.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceRowQueryForEveryClient(t *testing.T) {
	p := newTestProxy(t, lrBackend(t, []map[string]string{{"app": "api-gateway"}, {"app": "web"}}).URL)
	q := `label_replace(sum by (app) (rate({app="api-gateway"}[5m])), "app_short", "$1", "app", "(.*)-.*")`
	for client, headers := range instantClients {
		for _, endpoint := range []string{"query_range", "query"} {
			a := lrAskAs(t, p, endpoint, q, headers)
			if a.code != 200 || len(a.series) != 2 {
				t.Errorf("%s %s: HTTP %d, %d series, want 2\n%s", client, endpoint, a.code, len(a.series), a.body)
				continue
			}
			for _, s := range a.series {
				if (s.Metric["app"] == "api-gateway") != (s.Metric["app_short"] == "api") {
					t.Errorf("%s %s: app_short=%q for app=%q\n%s", client, endpoint, s.Metric["app_short"], s.Metric["app"], a.body)
				}
			}
		}
	}
}

// A bare range aggregation carries the stream identity; the new label rides
// along with it.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceOverBareRangeAggregation(t *testing.T) {
	p := newTestProxy(t, lrBackend(t, []map[string]string{{"app": "api-gateway", "_stream": `{app="api-gateway"}`, "level": "info"}}).URL)
	for _, endpoint := range []string{"query_range", "query"} {
		a := lrAsk(t, p, endpoint, `label_replace(rate({app="api-gateway"}[5m]), "app_short", "$1", "app", "(.*)-.*")`)
		if a.code != 200 || len(a.series) != 1 || a.series[0].Metric["app_short"] != "api" || a.series[0].Metric["app"] != "api-gateway" {
			t.Errorf("%s: HTTP %d, want app_short=api next to app\n%s", endpoint, a.code, a.body)
		}
	}
}

// An invalid regexp is Loki's parse error on both endpoints, nested or not.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceInvalidRegexp(t *testing.T) {
	p := newTestProxy(t, lrBackend(t, []map[string]string{{"app": "a"}}).URL)
	agg := `sum by (app) (count_over_time({app="a"}[5m]))`
	for _, q := range []string{
		`label_replace(` + agg + `, "d", "$1", "app", "[invalid")`,
		`label_replace(label_replace(` + agg + `, "d", "$1", "app", "(.*)"), "e", "$1", "d", "(unclosed")`,
		`label_replace(` + agg + ` * 2, "d", "$1", "app", "*bad")`,
	} {
		for _, endpoint := range []string{"query_range", "query"} {
			a := lrAsk(t, p, endpoint, q)
			if a.code != http.StatusBadRequest || !strings.Contains(a.body, "parse error : invalid regex in label_replace: error parsing regexp:") {
				t.Errorf("%s %s: HTTP %d, want Loki's 400 parse error\n%s", endpoint, q, a.code, a.body)
			}
		}
	}
}

// A call that does not hold the four string arguments of Loki's grammar, or a
// string whose escapes are invalid, is a 400 parse error on both endpoints: it
// is never taken for a log query or answered with untransformed series.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceMalformedArguments(t *testing.T) {
	p := newTestProxy(t, lrBackend(t, []map[string]string{{"app": "a"}}).URL)
	agg := `sum by (app) (count_over_time({app="a"}[5m]))`
	// The messages are the ones Loki v3.7.7's parser raises for these queries.
	for _, tc := range []struct{ query, want string }{
		{`label_replace(` + agg + `, "d", "$1", "app", "(.+):\d+")`, "parse error at line 1, col 80: invalid char escape"},
		{`label_replace(` + agg + `, "d", "$1", "app")`, "parse error at line 1, col 78: syntax error: unexpected ), expecting ,"},
		{`label_replace(` + agg + `, "d", "$1")`, "parse error at line 1, col 71: syntax error: unexpected ), expecting ,"},
		{`label_replace(` + agg + `, "d", "$1", "app", "x", "y")`, "parse error at line 1, col 83: syntax error: unexpected ,, expecting )"},
		{`label_replace(` + agg + `, d, "$1", "app", "x")`, "parse error at line 1, col 62: syntax error: unexpected IDENTIFIER, expecting STRING"},
		{`label_replace(` + agg + `, "d", "$1", "app", 5)`, "parse error at line 1, col 80: syntax error: unexpected NUMBER, expecting STRING"},
		{`label_replace(` + agg + ` "d", "$1", "app", "x")`, "parse error at line 1, col 61: syntax error: unexpected STRING"},
	} {
		for _, endpoint := range []string{"query_range", "query"} {
			a := lrAsk(t, p, endpoint, tc.query)
			if a.code != http.StatusBadRequest || !strings.Contains(a.body, `"message":"`+tc.want+`"`) {
				t.Errorf("%s %s: HTTP %d, want a 400 with %q\n%s", endpoint, tc.query, a.code, tc.want, a.body)
			}
		}
	}
}

// Loki sorts the result after the transform (pkg/logql/engine.go JoinSampleVector:
// sort.Sort(result) for a range, sort.Slice(vec, labels.Compare) for an instant
// query), so the order of the backend's series does not survive it.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceSortsTheResult(t *testing.T) {
	agg := `sum by (app) (count_over_time({app="x"}[5m]))`
	lr := func(inner, dst, repl, src, re string) string {
		return fmt.Sprintf(`label_replace(%s, %q, %q, %q, %q)`, inner, dst, repl, src, re)
	}
	for _, tc := range []struct {
		name  string
		sets  []map[string]string
		query string
		want  []string
	}{
		{"input in reverse order", []map[string]string{{"app": "web"}, {"app": "api-gateway"}}, lr(agg, "app_short", "$1", "app", "(.*)-.*"),
			[]string{`{app="api-gateway",app_short="api"}`, `{app="web"}`}},
		{"the transform reorders the label sets", []map[string]string{{"app": "a"}, {"app": "b"}},
			lr(lr(agg, "a_new", "2", "app", "a"), "a_new", "1", "app", "b"),
			[]string{`{a_new="1",app="b"}`, `{a_new="2",app="a"}`}},
		{"a shorter prefix first", []map[string]string{{"app": "b", "k": "v"}, {"app": "a"}, {"app": "b"}}, lr(agg, "none", "", "app", "zz"),
			[]string{`{app="a"}`, `{app="b"}`, `{app="b",k="v"}`}},
	} {
		p := newTestProxy(t, lrBackend(t, tc.sets).URL)
		for _, endpoint := range []string{"query_range", "query"} {
			a := lrAsk(t, p, endpoint, tc.query)
			var got []string
			for _, s := range a.series {
				got = append(got, lrKey(s.Metric))
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("%s %s: order %v, want %v\n%s", tc.name, endpoint, got, tc.want, a.body)
			}
		}
	}
	// sort and sort_desc order by value: that order is kept
	if !queryIsSortable(`label_replace(sort(sum(x)), "a", "b", "c", "d")`) || queryIsSortable(`label_replace(sum by (resort) (x), "a", "b", "c", "d")`) {
		t.Error("queryIsSortable does not tell sort(...) from a name that ends in it")
	}
}

// compareLabelSets is Prometheus' labels.Compare.
func TestCompareLabelSets(t *testing.T) {
	for _, tc := range []struct {
		a, b map[string]string
		want int
	}{
		{map[string]string{"a": "1"}, map[string]string{"a": "1"}, 0},
		{map[string]string{"a": "1"}, map[string]string{"a": "2"}, -1},
		{map[string]string{"a": "1"}, map[string]string{"a": "1", "b": "2"}, -1},
		{map[string]string{"a": "1", "c": "1"}, map[string]string{"a": "1", "b": "2"}, 1},
		{map[string]string{"b": "1"}, map[string]string{"a": "9"}, 1},
		{map[string]string{}, map[string]string{"a": "9"}, -1},
	} {
		if got := compareLabelSets(tc.a, tc.b); (got > 0) != (tc.want > 0) || (got < 0) != (tc.want < 0) {
			t.Errorf("compare(%v, %v) = %d, want sign %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// label_replace over `sum without`: the without marker no longer swallows the
// label_replace marker that follows it.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceOverSumWithout(t *testing.T) {
	p := newTestProxy(t, lrBackend(t, []map[string]string{
		{"_stream": `{app="api-gateway"}`, "level": "info"}, {"_stream": `{app="web"}`, "level": "info"},
	}).URL)
	for _, endpoint := range []string{"query_range", "query"} {
		a := lrAsk(t, p, endpoint, `label_replace(sum without (level) (count_over_time({app=~".+"}[5m])), "app_short", "$1", "app", "(.*)-.*")`)
		if a.code != 200 || len(a.series) != 2 {
			t.Fatalf("%s: HTTP %d, %d series, want 2\n%s", endpoint, a.code, len(a.series), a.body)
		}
		for _, s := range a.series {
			if _, kept := s.Metric["level"]; kept {
				t.Errorf("%s: level kept by without: %v", endpoint, s.Metric)
			}
			if (s.Metric["app"] == "api-gateway") != (s.Metric["app_short"] == "api") {
				t.Errorf("%s: app_short=%q for app=%q, want api for api-gateway only", endpoint, s.Metric["app_short"], s.Metric["app"])
			}
		}
	}
}

// The differential test: generated label sets and generated transforms, the
// proxy's answer on both endpoints against the reference applied to the
// untransformed answer of the same backend.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range, loki_api_v1_query
func TestLabelReplaceDifferential(t *testing.T) {
	values := []string{"api-gateway", "web", "x", "api-1.2", "", "żółć-9", "a|b", "10.20", "ab"}
	regexes := []string{`(.*)-.*`, `(.*)`, `.*`, `.+`, `api`, `(?P<head>[a-z]+)-(?P<tail>.*)`, `([^-]+)`, `a|b`, `(\d+)\.(\d+)`, `.+-(.*)`, ``, `(a)(b)?`, `web|x`, `(?i)API-.*`, `[a-z]+`, `(.)(.)`}
	repls := []string{`$1`, `${1}`, `$2`, `${head}`, `$1-$2`, ``, `static`, `$$1`, `$1x`, `${1}x`, `$none`, `$0`, `<${tail}>`}
	names := []string{"a", "b", "c", "new", "d.e"}
	rng := rand.New(rand.NewSource(7))
	for round := 0; round < 40; round++ {
		var sets []map[string]string
		seen := map[string]bool{}
		for i, n := 0, 1+rng.Intn(5); i < n; i++ {
			m := map[string]string{}
			for _, k := range []string{"a", "b", "c"} {
				if rng.Intn(4) > 0 {
					m[k] = values[rng.Intn(len(values))]
				}
			}
			if key := lrKey(m); !seen[key] {
				seen[key] = true
				sets = append(sets, m)
			}
		}
		p := newTestProxy(t, lrBackend(t, sets).URL)
		type step struct{ dst, repl, src, re string }
		steps := make([]step, 1+rng.Intn(2))
		for i := range steps {
			steps[i] = step{names[rng.Intn(len(names))], repls[rng.Intn(len(repls))], names[rng.Intn(len(names))], regexes[rng.Intn(len(regexes))]}
		}
		base := `sum by (a, b, c) (count_over_time({app="x"}[5m]))`
		q := base
		for _, s := range steps {
			q = fmt.Sprintf(`label_replace(%s, %s, %s, %s, %s)`, q, strconv.Quote(s.dst), strconv.Quote(s.repl), strconv.Quote(s.src), strconv.Quote(s.re))
		}
		for _, endpoint := range []string{"query_range", "query"} {
			instant := endpoint == "query"
			baseAns := lrAsk(t, p, endpoint, base)
			if baseAns.code != 200 || len(baseAns.series) != len(sets) {
				t.Fatalf("round %d %s: the untransformed answer has %d series (HTTP %d), want %d\n%s", round, endpoint, len(baseAns.series), baseAns.code, len(sets), baseAns.body)
			}
			want := lrReferenceJoin(baseAns.series, instant, func(m map[string]string) map[string]string {
				for _, s := range steps {
					m = lrReference(m, s.dst, s.repl, s.src, s.re)
				}
				return m
			})
			got := lrAsk(t, p, endpoint, q)
			if got.code != 200 {
				t.Fatalf("round %d %s %s: HTTP %d\n%s", round, endpoint, q, got.code, got.body)
			}
			if g, w := lrGot(got, instant), want; fmt.Sprint(g) != fmt.Sprint(w) {
				t.Fatalf("round %d %s %s over %v\n got %v\nwant %v", round, endpoint, q, sets, g, w)
			}
		}
	}
}
