package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// The post-aggregation route reads label_replace calls at the root around a ranking from the query's plan: the
// ranking the route takes for the query under the calls, and the calls innermost first. Calls around anything else,
// and around a ranking nested with a stddev or stdvar (semantics/stddev-stdvar-nested-or-binary), keep their routes.
//
// conformance: semantics/label-replace-around-ranking-and-vector
func TestLabelReplaceAroundRankingRoute(t *testing.T) {
	const agg = `sum by (app) (count_over_time({env="production"}[5m]))`
	a := physical.LabelReplace{DstLabel: "a", Replacement: "$1", SrcLabel: "app", Regex: "(.*)"}
	b := physical.LabelReplace{DstLabel: "b", Replacement: "x$1", SrcLabel: "a", Regex: "(.*)"}
	for _, tc := range []struct {
		q, name, inner string
		relabel        []physical.LabelReplace
		outer          string
	}{
		{q: `label_replace(topk(2, ` + agg + `), "a", "$1", "app", "(.*)")`, name: "topk", inner: agg, relabel: []physical.LabelReplace{a}},
		{q: `label_replace(bottomk by (app) (1, ` + agg + `), "a", "$1", "app", "(.*)")`, name: "bottomk", inner: agg, relabel: []physical.LabelReplace{a}},
		{q: `label_replace(sort_desc(` + agg + `), "a", "$1", "app", "(.*)")`, name: "sort_desc", inner: agg, relabel: []physical.LabelReplace{a}},
		{q: `label_replace(label_replace(sort(` + agg + `), "a", "$1", "app", "(.*)"), "b", "x$1", "a", "(.*)")`, name: "sort", inner: agg, relabel: []physical.LabelReplace{a, b}},
		{q: `label_replace(sum(topk(2, ` + agg + `)), "a", "$1", "app", "(.*)")`, name: "topk", inner: agg, relabel: []physical.LabelReplace{a}, outer: "sum"},
		{q: `label_replace(topk(2, ` + agg + ` / 2), "a", "$1", "app", "(.*)")`, name: "topk", inner: agg + ` / 2`, relabel: []physical.LabelReplace{a}},
	} {
		got, ok := parseInstantMetricPostAggQuery(tc.q)
		specs, relabelled := rankingUnderLabelReplace(tc.q)
		if !ok || !relabelled {
			t.Errorf("%s: not routed (%v, %v)", tc.q, ok, relabelled)
			continue
		}
		outer := ""
		if got.outer != nil {
			outer = got.outer.name
		}
		if got.name != tc.name || got.inner != tc.inner || outer != tc.outer || !reflect.DeepEqual(specs, tc.relabel) {
			t.Errorf("%s: %s over %q (outer %q), calls %+v; want %s over %q (outer %q), calls %+v", tc.q, got.name, got.inner, outer, specs, tc.name, tc.inner, tc.outer, tc.relabel)
		}
	}
	for _, q := range []string{
		`label_replace(` + agg + `, "a", "$1", "app", "(.*)")`,
		`label_replace(stddev by (app) (count_over_time({env="production"}[5m])), "a", "$1", "app", "(.*)")`,
		`label_replace(topk(2, stddev by (app) (count_over_time({env="production"}[5m]))), "a", "$1", "app", "(.*)")`,
		`label_replace(stddev(topk(2, ` + agg + `)), "a", "$1", "app", "(.*)")`,
		`label_replace(topk(2, ` + agg + `) / 2, "a", "$1", "app", "(.*)")`,
		`label_replace(vector(1), "a", "$1", "app", "(.*)")`,
		`label_replace(sum by (sorted) (rate({app="a"}[5m])), "a", "$1", "sorted", "(.*)")`,
		`topk(2, label_replace(` + agg + `, "a", "$1", "app", "(.*)"))`,
	} {
		got, ok := parseInstantMetricPostAggQuery(q)
		if _, relabelled := rankingUnderLabelReplace(q); relabelled {
			t.Errorf("%s: routed as a ranking under label_replace: %+v", q, got)
		}
		if strings.HasPrefix(q, "label_replace") && ok {
			t.Errorf("%s: routed: %+v", q, got)
		}
	}
	// The physical reference of both shadows holds the calls over the ranking.
	op, _, routed, err := PostAggregationPhysical(`label_replace(topk(2, `+agg+`), "a", "$1", "app", "(.*)")`, func(string) (string, error) {
		return `env:="production" | stats by (app) count()`, nil
	})
	lm, isMap := op.(physical.LabelMap)
	if err != nil || !routed || !isMap || !reflect.DeepEqual(lm.Replace, []physical.LabelReplace{a}) {
		t.Fatalf("PostAggregationPhysical = %#v, %v, %v", op, routed, err)
	}
	if rs, ok := lm.In.(physical.RankStep); !ok || rs.Op != "topk" || rs.K != 2 {
		t.Fatalf("under the calls: %#v", lm.In)
	}
}

// The samples of a merged series at one timestamp are written in the order the ranking at the top answers them
// (the highest first for topk and sort_desc, the lowest first for bottomk and sort, NaN last); a series with one sample
// per timestamp, an answer under an aggregation and an instant answer are left as they are.
//
// conformance: semantics/label-replace-around-ranking-and-vector
func TestRelabelEvaluatedOrdersSamplesOfOneLabelSet(t *testing.T) {
	const matrix = `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"app":"a"},"values":[[60,"1"],[120,"NaN"]]},` +
		`{"metric":{"app":"b"},"values":[[60,"5"],[120,"2"]]},` +
		`{"metric":{"app":"c"},"values":[[60,"3"]]}]}}`
	removeApp, _ := newLabelTransforms([]physical.LabelReplace{{DstLabel: "app", Replacement: "", SrcLabel: "", Regex: ""}})
	values := func(body []byte) string {
		var doc struct {
			Data struct {
				Result []struct {
					Values [][2]any `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &doc); err != nil || len(doc.Data.Result) != 1 {
			t.Fatalf("%s: %v", body, err)
		}
		var out []string
		for _, p := range doc.Data.Result[0].Values {
			out = append(out, fmt.Sprint(p[0])+"="+fmt.Sprint(p[1]))
		}
		return strings.Join(out, " ")
	}
	for name, tc := range map[string]struct {
		postAgg instantMetricPostAgg
		want    string
	}{
		"topk":      {instantMetricPostAgg{name: "topk", k: 3}, "60=5 60=3 60=1 120=2 120=NaN"},
		"sort_desc": {instantMetricPostAgg{name: "sort_desc"}, "60=5 60=3 60=1 120=2 120=NaN"},
		"bottomk":   {instantMetricPostAgg{name: "bottomk", k: 3}, "60=1 60=3 60=5 120=2 120=NaN"},
		"sort":      {instantMetricPostAgg{name: "sort"}, "60=1 60=3 60=5 120=2 120=NaN"},
		"sum over a ranking": {instantMetricPostAgg{name: "topk", k: 3, outer: &instantMetricPostAgg{name: "sum"}},
			"60=1 60=5 60=3 120=NaN 120=2"},
		"topk under a ranking": {instantMetricPostAgg{name: "topk", k: 3, outer: &instantMetricPostAgg{name: "sort"}},
			"60=1 60=3 60=5 120=2 120=NaN"},
	} {
		got := values(relabelEvaluated([]byte(matrix), removeApp, false, tc.postAgg))
		if got != tc.want {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
	// No calls, one sample per timestamp, an instant answer: as they are.
	if got := relabelEvaluated([]byte(matrix), labelTransforms{}, false, instantMetricPostAgg{name: "topk"}); string(got) != matrix {
		t.Errorf("no calls: %s", got)
	}
	const distinct = `{"data":{"resultType":"matrix","result":[{"metric":{"a":"x"},"values":[[60,"1"],[120,"2"]]}]}}`
	if got := orderSamplesAtEachStep([]byte(distinct), true); string(got) != distinct {
		t.Errorf("one sample per timestamp: %s", got)
	}
	const vector = `{"data":{"resultType":"vector","result":[{"metric":{},"value":[60,"1"]},{"metric":{},"value":[60,"5"]}]}}`
	if got := orderSamplesAtEachStep([]byte(vector), true); string(got) != vector {
		t.Errorf("instant answer: %s", got)
	}
	if got := orderSamplesAtEachStep([]byte(`not json`), true); string(got) != `not json` {
		t.Errorf("not an answer: %s", got)
	}
}

// orderSamplesAtEachStep against a reference written apart: for random series of duplicate timestamps, each
// timestamp's samples are its input samples sorted by the ranking's order (topKCompare), the timestamps in order.
//
// conformance: semantics/label-replace-around-ranking-and-vector
func TestOrderSamplesAtEachStepMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for round := 0; round < 300; round++ {
		descending := rng.Intn(2) == 0
		type point struct {
			ts int
			v  float64
		}
		var points []point
		for i := 0; i < 1+rng.Intn(12); i++ {
			v := float64(rng.Intn(5))
			if rng.Intn(9) == 0 {
				v = math.NaN()
			}
			points = append(points, point{60 * (1 + rng.Intn(4)), v})
		}
		sort.SliceStable(points, func(i, j int) bool { return points[i].ts < points[j].ts })
		var b strings.Builder
		b.WriteString(`{"data":{"resultType":"matrix","result":[{"metric":{},"values":[`)
		for i, p := range points {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString("[" + strconv.Itoa(p.ts) + `,"` + strconv.FormatFloat(p.v, 'f', -1, 64) + `"]`)
		}
		b.WriteString(`]}]}}`)
		want := append([]point(nil), points...)
		sort.SliceStable(want, func(i, j int) bool {
			if want[i].ts != want[j].ts {
				return want[i].ts < want[j].ts
			}
			a, c := want[i].v, want[j].v
			switch {
			case math.IsNaN(a):
				return false
			case math.IsNaN(c):
				return true
			case descending:
				return a > c
			}
			return a < c
		})
		var wantText []string
		for _, p := range want {
			wantText = append(wantText, strconv.Itoa(p.ts)+"="+strconv.FormatFloat(p.v, 'f', -1, 64))
		}
		var doc struct {
			Data struct {
				Result []struct {
					Values [][2]any `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(orderSamplesAtEachStep([]byte(b.String()), descending), &doc); err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, p := range doc.Data.Result[0].Values {
			got = append(got, fmt.Sprint(p[0])+"="+fmt.Sprint(p[1]))
		}
		if strings.Join(got, " ") != strings.Join(wantText, " ") {
			t.Fatalf("descending %v: %v, want %v", descending, got, wantText)
		}
	}
}

// A label_replace regexp that does not compile answers Loki's 400 before any VictoriaLogs request, on query and
// query_range; an instant ranking under valid calls answers the ranked series relabelled.
//
// conformance: semantics/label-replace-around-ranking-and-vector
func TestLabelReplaceAroundRankingHandlers(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		kind, sample := "vector", `"value":[1704067260,"%s"]`
		if r.URL.Path != "/select/logsql/stats_query" {
			kind, sample = "matrix", `"values":[[1704067260,"%s"]]`
		}
		series := func(app, v string) string {
			return `{"metric":{"app":"` + app + `"},` + strings.Replace(sample, "%s", v, 1) + `}`
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"` + kind + `","result":[` + series("api-gateway", "9") + `,` + series("web", "4") + `,` + series("db-main", "2") + `]}}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, endpoint := range []string{"query", "query_range"} {
		calls.Store(0)
		a := lrAsk(t, p, endpoint, `label_replace(topk(2, sum by (app) (count_over_time({env="production"}[1m]))), "a", "$1", "app", "(")`)
		if a.code != http.StatusBadRequest || !strings.Contains(a.body, "invalid regex in label_replace") || calls.Load() != 0 {
			t.Errorf("%s: HTTP %d after %d VictoriaLogs calls: %s", endpoint, a.code, calls.Load(), a.body)
		}
		if endpoint != "query" {
			// The range route ranks rows this stand-in does not answer: TestLabelReplaceAroundRankingMatchesLoki
			// (internal/querycorpus) holds it to Loki.
			continue
		}
		a = lrAsk(t, p, endpoint, `label_replace(topk(2, sum by (app) (count_over_time({env="production"}[1m]))), "short", "$1", "app", "([a-z]+)-.*")`)
		if a.code != http.StatusOK || len(a.series) != 2 {
			t.Fatalf("%s: HTTP %d, %d series: %s", endpoint, a.code, len(a.series), a.body)
		}
		for _, s := range a.series {
			if want := map[string]string{"api-gateway": "api", "web": ""}[s.Metric["app"]]; s.Metric["short"] != want || s.Metric["app"] == "db-main" {
				t.Errorf("%s: %v, want short=%q", endpoint, s.Metric, want)
			}
		}
	}
}

// label_replace around a ranking keeps the ranking's caches: an exact repeat asks VictoriaLogs nothing and answers
// the same bytes, a window moved back a minute answers what a proxy without the extent cache answers for it, and the
// answer is the ranking's own answer relabelled, a call that gives every ranked series one label set included (its
// samples of one step kept in the ranking's order).
//
// conformance: semantics/label-replace-around-ranking-and-vector, quality/query-range-extent-cache
func TestLabelReplaceAroundRankingKeepsTheRankingCaches(t *testing.T) {
	end := time.Unix(rankedTopKEnd, 0).UTC()
	lines := rankedTopKLines(5, end, 3*time.Hour)
	step := 15 * time.Second
	ranking := `topk(3, ` + fmt.Sprintf(rankedTopKInner, "5m") + `)`
	for _, tc := range []struct {
		name, query string
		replace     physical.LabelReplace
	}{
		{"a new label", `label_replace(` + ranking + `, "app_short", "$1", "app", "(.*)-.*")`, physical.LabelReplace{DstLabel: "app_short", Replacement: "$1", SrcLabel: "app", Regex: "(.*)-.*"}},
		{"one label set", `label_replace(` + ranking + `, "app", "", "", "")`, physical.LabelReplace{DstLabel: "app", Replacement: "", SrcLabel: "", Regex: ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			on := newRankedTopKStack(t, lines, true, nil)
			off := newRankedTopKStack(t, lines, false, nil)
			start := end.Add(-time.Hour)
			code, cold := on.ask(t, tc.query, start, end, step, nil)
			if code != http.StatusOK {
				t.Fatalf("cold: HTTP %d %s", code, cold)
			}
			b, g, r := on.vlCalls()
			code, repeat := on.ask(t, tc.query, start, end, step, nil)
			if b2, g2, r2 := on.vlCalls(); code != http.StatusOK || b2+g2+r2 != b+g+r || string(repeat) != string(cold) {
				t.Fatalf("exact repeat: HTTP %d, %d more VictoriaLogs calls, same answer %v", code, b2+g2+r2-b-g-r, string(repeat) == string(cold))
			}
			// The ranking's own answer, relabelled, is the answer.
			_, plain := off.ask(t, ranking, start, end, step, nil)
			relabel, _ := newLabelTransforms([]physical.LabelReplace{tc.replace})
			if want := relabelEvaluated(plain, relabel, false, instantMetricPostAgg{name: "topk", k: 3}); !sameAnswer(t, cold, want) {
				t.Fatalf("not the ranking relabelled:\n got %s\nwant %s", cold, want)
			}
			moved := func(p *rankedTopKStack) []byte {
				code, body := p.ask(t, tc.query, start.Add(-time.Minute), end.Add(-time.Minute), step, nil)
				if code != http.StatusOK {
					t.Fatalf("moved: HTTP %d %s", code, body)
				}
				return body
			}
			if a, b := moved(on), moved(off); !sameAnswer(t, a, b) {
				t.Fatalf("moved window, cached and uncached differ:\n%s\n%s", a, b)
			}
		})
	}
}

// sameAnswer compares the data of two answers (the stats and headers aside).
func sameAnswer(t *testing.T, a, b []byte) bool {
	t.Helper()
	read := func(body []byte) string {
		var doc struct {
			Data struct {
				ResultType string            `json:"resultType"`
				Result     []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatalf("%v: %s", err, body)
		}
		out := doc.Data.ResultType
		for _, r := range doc.Data.Result {
			out += "\n" + string(r)
		}
		return out
	}
	return read(a) == read(b)
}
