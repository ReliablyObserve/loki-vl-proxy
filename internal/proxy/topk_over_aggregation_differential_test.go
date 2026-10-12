package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This file pins an aggregation over a topk, bottomk or sort to Loki's: the
// inner vector is ranked at every step on its own, and the outer aggregation
// folds what survived at that step (Loki v3.7.7 pkg/logql/evaluator.go
// VectorAggEvaluator.Next evaluates its operand to a vector per step, so
// `sum(topk(3, x))` is the sum of the 3 best samples of each step). The
// reference below is written apart from the proxy and evaluates every layer on
// the vector of one step.

// refSample is one sample of the reference vector.
type refSample struct {
	labels map[string]string
	value  float64
}

// refLayer is one aggregation of the reference: op is the LogQL operator, by
// or without the clause (neither: one group).
type refLayer struct {
	op      string
	k       int
	by      []string
	without []string
	hasBy   bool
	hasWith bool
}

func (l refLayer) groupLabels(in map[string]string) map[string]string {
	out := map[string]string{}
	switch {
	case l.hasWith:
		for name, value := range in {
			out[name] = value
		}
		delete(out, "__name__")
		for _, name := range l.without {
			delete(out, name)
		}
	case l.hasBy:
		for _, name := range l.by {
			if value, ok := in[name]; ok {
				out[name] = value
			}
		}
	}
	return out
}

func refLabelKey(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		b.WriteString(name + "=" + labels[name] + ",")
	}
	return b.String()
}

// refEval evaluates one layer on the vector of one step. tie reports a tie
// across the k-th place of a topk or bottomk group, where Loki's choice follows
// map order and the series that survive are not reproducible.
func refEval(vec []refSample, l refLayer) (out []refSample, tie bool) {
	groups := map[string][]refSample{}
	var order []string
	for _, s := range vec {
		key := refLabelKey(l.groupLabels(s.labels))
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], s)
	}
	for _, key := range order {
		members := groups[key]
		switch l.op {
		case "topk", "bottomk":
			sort.SliceStable(members, func(i, j int) bool {
				if l.op == "topk" {
					return members[i].value > members[j].value
				}
				return members[i].value < members[j].value
			})
			if len(members) > l.k && l.k > 0 && members[l.k-1].value == members[l.k].value {
				tie = true
			}
			if len(members) > l.k {
				members = members[:l.k]
			}
			out = append(out, members...)
		case "sort", "sort_desc":
			out = append(out, members...)
		default:
			agg := members[0].value
			sum := 0.0
			for _, m := range members {
				sum += m.value
				switch l.op {
				case "min":
					agg = math.Min(agg, m.value)
				case "max":
					agg = math.Max(agg, m.value)
				}
			}
			switch l.op {
			case "sum":
				agg = sum
			case "avg":
				agg = sum / float64(len(members))
			case "count":
				agg = float64(len(members))
			}
			out = append(out, refSample{l.groupLabels(members[0].labels), agg})
		}
	}
	return out, tie
}

// overAggLayer is a layer of a query: how it is written around its operand and
// how the reference evaluates it.
type overAggLayer struct {
	wrap func(inner string) string
	ref  refLayer
}

func foldLayer(op, clause string, by, without []string, hasBy, hasWith bool) overAggLayer {
	return overAggLayer{func(inner string) string { return fmt.Sprintf("%s%s (%s)", op, clause, inner) }, refLayer{op: op, by: by, without: without, hasBy: hasBy, hasWith: hasWith}}
}

func fold(op string) overAggLayer { return foldLayer(op, "", nil, nil, false, false) }
func foldBy(op string, labels ...string) overAggLayer {
	return foldLayer(op, " by ("+strings.Join(labels, ", ")+")", labels, nil, true, false)
}
func foldWithout(op string, labels ...string) overAggLayer {
	return foldLayer(op, " without ("+strings.Join(labels, ", ")+")", nil, labels, false, true)
}
func foldTrailing(op string, labels ...string) overAggLayer {
	return overAggLayer{func(inner string) string {
		return fmt.Sprintf("%s (%s) by (%s)", op, inner, strings.Join(labels, ", "))
	}, refLayer{op: op, by: labels, hasBy: true}}
}

func rank(op string, k int) overAggLayer {
	return overAggLayer{func(inner string) string { return fmt.Sprintf("%s(%d, %s)", op, k, inner) }, refLayer{op: op, k: k}}
}
func rankBy(op string, k int, labels ...string) overAggLayer {
	return overAggLayer{func(inner string) string {
		return fmt.Sprintf("%s by (%s) (%d, %s)", op, strings.Join(labels, ", "), k, inner)
	}, refLayer{op: op, k: k, by: labels, hasBy: true}}
}
func rankWithout(op string, k int, labels ...string) overAggLayer {
	return overAggLayer{func(inner string) string {
		return fmt.Sprintf("%s without (%s) (%d, %s)", op, strings.Join(labels, ", "), k, inner)
	}, refLayer{op: op, k: k, without: labels, hasWith: true}}
}
func order(op string) overAggLayer {
	return overAggLayer{func(inner string) string { return op + "(" + inner + ")" }, refLayer{op: op}}
}

// overAggChains are the queries under test, innermost layer first.
var overAggChains = map[string][]overAggLayer{
	"sum(topk)":                      {rank("topk", 3), fold("sum")},
	"sum by (app) (topk)":            {rank("topk", 3), foldBy("sum", "app")},
	"sum by (app, env) (topk)":       {rank("topk", 3), foldBy("sum", "app", "env")},
	"sum(topk) by (env) trailing":    {rank("topk", 3), foldTrailing("sum", "env")},
	"sum without (app) (topk)":       {rank("topk", 4), foldWithout("sum", "app")},
	"sum without () (topk)":          {rank("topk", 4), foldWithout("sum")},
	"avg(bottomk)":                   {rank("bottomk", 5), fold("avg")},
	"max(topk)":                      {rank("topk", 2), fold("max")},
	"min by (env) (topk)":            {rank("topk", 5), foldBy("min", "env")},
	"count(bottomk)":                 {rank("bottomk", 3), fold("count")},
	"count by (app) (topk)":          {rank("topk", 6), foldBy("count", "app")},
	"sum by (env) (topk by (env))":   {rankBy("topk", 2, "env"), foldBy("sum", "env")},
	"sum(bottomk without (app))":     {rankWithout("bottomk", 2, "app"), fold("sum")},
	"k above the series count":       {rank("topk", 100), fold("sum")},
	"topk(topk)":                     {rank("topk", 3), rank("topk", 2)},
	"bottomk(topk)":                  {rank("topk", 5), rank("bottomk", 2)},
	"topk by (env) (topk)":           {rank("topk", 6), rankBy("topk", 1, "env")},
	"topk(bottomk by (env))":         {rankBy("bottomk", 2, "env"), rank("topk", 3)},
	"topk(sum by (app) (topk))":      {rank("topk", 6), foldBy("sum", "app"), rank("topk", 2)},
	"sum(max by (env) (topk))":       {rank("topk", 8), foldBy("max", "env"), fold("sum")},
	"avg(sum by (app) (bottomk))":    {rank("bottomk", 7), foldBy("sum", "app"), fold("avg")},
	"sum(topk(topk))":                {rank("topk", 6), rank("topk", 4), fold("sum")},
	"sum(sort)":                      {order("sort"), fold("sum")},
	"sum by (env) (sort_desc)":       {order("sort_desc"), foldBy("sum", "env")},
	"topk(sort)":                     {order("sort"), rank("topk", 3)},
	"bottomk(sort_desc)":             {order("sort_desc"), rank("bottomk", 3)},
	"sum(topk(sort))":                {order("sort_desc"), rank("topk", 4), fold("sum")},
	"max(sort(topk))":                {rank("topk", 4), order("sort"), fold("max")},
	"count(topk by (app, env))":      {rankBy("topk", 1, "app", "env"), fold("count")},
	"sum by (nope) (topk)":           {rank("topk", 3), foldBy("sum", "nope")},
	"topk without (app,env)(then s)": {rankWithout("topk", 2, "app", "env"), fold("sum")},
}

// overAggQuery writes the chain around inner.
func overAggQuery(chain []overAggLayer, inner string) string {
	query := inner
	for _, layer := range chain {
		query = layer.wrap(query)
	}
	return query
}

// overAggReference evaluates the chain on one vector. skip reports a step
// where a tie across a ranking layer's k-th place is followed by anything but
// an ungrouped fold (or by nothing): Loki breaks the tie by map order, and
// which series survives then changes the labels or the groups downstream.
func overAggReference(vec []refSample, chain []overAggLayer) (out []refSample, skip bool) {
	out = vec
	tied, lastTie := false, false
	for _, layer := range chain {
		if tied {
			ref := layer.ref
			ungroupedFold := ref.op != "topk" && ref.op != "bottomk" && ref.op != "sort" && ref.op != "sort_desc" && !ref.hasBy && !ref.hasWith
			skip = skip || !ungroupedFold
		}
		var tie bool
		out, tie = refEval(out, layer.ref)
		tied, lastTie = tied || tie, tie
	}
	return out, skip || lastTie
}

func refVectorOf(vec []topKSeries) []refSample {
	out := make([]refSample, 0, len(vec))
	for _, s := range vec {
		out = append(out, refSample{map[string]string{"app": s.app, "env": s.env}, s.value})
	}
	return out
}

// overAggPoints is a response by series labels and unix second.
type overAggPoints map[string]map[int64]float64

func parseOverAggResponse(t *testing.T, query string, code int, body []byte, wantType string) overAggPoints {
	t.Helper()
	var resp struct {
		Data struct {
			ResultType string
			Result     []struct {
				Metric map[string]string
				Values [][]any
				Value  []any
			}
		}
	}
	if code != http.StatusOK || json.Unmarshal(body, &resp) != nil || resp.Data.ResultType != wantType {
		t.Fatalf("%s: %d %.300s", query, code, body)
	}
	got := overAggPoints{}
	for _, series := range resp.Data.Result {
		key := refLabelKey(series.Metric)
		if got[key] != nil {
			t.Fatalf("%s: duplicate series %s", query, key)
		}
		got[key] = map[int64]float64{}
		pairs := series.Values
		if series.Value != nil {
			pairs = [][]any{series.Value}
		}
		for _, pair := range pairs {
			v, _ := strconv.ParseFloat(pair[1].(string), 64)
			got[key][int64(pair[0].(float64))] = v
		}
	}
	return got
}

// assertOverAggStep compares the series of one step.
func assertOverAggStep(t *testing.T, query string, ts int64, want []refSample, got overAggPoints) {
	t.Helper()
	wantKeys := map[string]float64{}
	for _, s := range want {
		wantKeys[refLabelKey(s.labels)] = s.value
	}
	for key, value := range wantKeys {
		have, ok := got[key][ts]
		if !ok {
			t.Errorf("%s @%d: series {%s} missing, want %v", query, ts, key, value)
			continue
		}
		if math.Abs(have-value) > 1e-9*math.Max(1, math.Abs(value)) {
			t.Errorf("%s @%d: {%s} = %v, want %v", query, ts, key, have, value)
		}
	}
	for key, points := range got {
		if _, ok := points[ts]; ok {
			if _, want := wantKeys[key]; !want {
				t.Errorf("%s @%d: unexpected series {%s} = %v", query, ts, key, points[ts])
			}
		}
	}
}

func overAggRangeDifferential(t *testing.T, version string, seeds []int64, shapes []topKShape, start, end time.Time) {
	base := time.Unix(1699999200, 0).UTC()
	compared := 0
	for _, seed := range seeds {
		lines := topKFixture(seed, base, 60)
		backend := newTopKTestBackend(t, lines)
		p := newGapTestProxy(t, backend.URL)
		p.storeBackendVersion(version, version)
		for _, shape := range shapes {
			inner := fmt.Sprintf(`sum by (app, env) (%s({app=~".+"}[%s]))`, shape.fn, formatPromDuration(shape.window))
			for name, chain := range overAggChains {
				if shape.window == shape.step && strings.Contains(name, "sort") {
					// A tumbling window under sort is answered by VictoriaLogs'
					// stats_query_range, which this backend does not emulate;
					// TestSortOverTumblingRange covers that route.
					continue
				}
				query := overAggQuery(chain, inner)
				t.Run(fmt.Sprintf("seed%d/%s/%s/%s", seed, shape.fn, name, formatPromDuration(shape.step)), func(t *testing.T) {
					params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.FormatFloat(shape.step.Seconds(), 'f', -1, 64)}}
					rec := httptest.NewRecorder()
					p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
					got := parseOverAggResponse(t, query, rec.Code, rec.Body.Bytes(), "matrix")
					for ts := start; !ts.After(end); ts = ts.Add(shape.step) {
						want, skip := overAggReference(refVectorOf(topKReferenceVector(lines, shape.fn, ts, shape.window)), chain)
						if skip {
							continue
						}
						compared++
						assertOverAggStep(t, query, ts.Unix(), want, got)
					}
				})
			}
		}
		backend.Close()
	}
	if compared == 0 {
		t.Fatal("no step was compared")
	}
}

// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range
func TestTopKOverVectorAggregationRangeDifferential(t *testing.T) {
	base := time.Unix(1699999200, 0).UTC()
	// An unaligned start: Grafana's now-1h is never on a step boundary.
	start, end := base.Add(10*time.Minute+7300*time.Millisecond), base.Add(40*time.Minute+7300*time.Millisecond)
	overAggRangeDifferential(t, "v1.50.0", []int64{1, 2}, []topKShape{
		{"count_over_time", time.Minute, 5 * time.Minute},  // sliding
		{"count_over_time", time.Minute, time.Minute},      // tumbling
		{"rate", 90 * time.Second, 5 * time.Minute},        // window not a multiple of the step
		{"bytes_over_time", 45 * time.Second, time.Minute}, // window longer than the step, uneven
	}, start, end)
}

// A backend that cannot anchor buckets (before v1.45) sends the window to the
// raw evaluator, whose matrix the proxy folds itself.
// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query_range
func TestTopKOverVectorAggregationRangeDifferentialRawEvaluator(t *testing.T) {
	base := time.Unix(1699999200, 0).UTC()
	start, end := base.Add(10*time.Minute+30*time.Second), base.Add(30*time.Minute+30*time.Second)
	overAggRangeDifferential(t, "v1.44.0", []int64{4}, []topKShape{{"count_over_time", time.Minute, 5 * time.Minute}}, start, end)
}

// randomInstantVector is a vector of 12 app x env series whose values come
// from a small set, so that ties are common.
func randomInstantVector(rng *rand.Rand) []topKSeries {
	var vec []topKSeries
	for _, app := range []string{"a1", "a2", "a3", "a4"} {
		for _, env := range []string{"dev", "prod", "stage"} {
			if rng.Intn(5) == 0 {
				continue
			}
			vec = append(vec, topKSeries{app: app, env: env, value: float64(1 + rng.Intn(12))})
		}
	}
	return vec
}

// conformance: semantics/topk-over-vector-aggregation, loki_api_v1_query
func TestTopKOverVectorAggregationInstantDifferential(t *testing.T) {
	const at = 1700000000
	compared := 0
	for seed := int64(1); seed <= 12; seed++ {
		vec := randomInstantVector(rand.New(rand.NewSource(seed)))
		var rows []string
		for _, s := range vec {
			rows = append(rows, fmt.Sprintf(`{"metric":{"app":%q,"env":%q},"value":[%d,%q]}`, s.app, s.env, at, strconv.FormatFloat(s.value, 'f', -1, 64)))
		}
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(rows, ",") + `]}}`))
		}))
		p := newGapTestProxy(t, backend.URL)
		for name, chain := range overAggChains {
			query := overAggQuery(chain, topKInner)
			t.Run(fmt.Sprintf("seed%d/%s", seed, name), func(t *testing.T) {
				rec := httptest.NewRecorder()
				p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {query}, "time": {strconv.Itoa(at)}}.Encode(), nil))
				got := parseOverAggResponse(t, query, rec.Code, rec.Body.Bytes(), "vector")
				want, skip := overAggReference(refVectorOf(vec), chain)
				if skip {
					return
				}
				compared++
				assertOverAggStep(t, query, at, want, got)
			})
		}
		backend.Close()
	}
	if compared == 0 {
		t.Fatal("no vector was compared")
	}
}
