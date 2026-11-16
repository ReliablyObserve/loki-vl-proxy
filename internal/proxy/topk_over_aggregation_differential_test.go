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

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// This file pins an aggregation over a topk, bottomk or sort to Loki's: the
// inner vector is ranked at every step on its own, and the outer aggregation
// folds what survived at that step, so `sum(topk(3, x))` is the sum of the 3
// best samples of each step. The proxy is compared with Loki's answers,
// recorded with Loki's engine (test/oracle/loki engine; LVP_RUN_ORACLES=1
// records them again). Where a ranking ties across its k-th place Loki may keep
// either series, so the comparison needs to know where a tie sits: aggLayer.eval
// evaluates each layer on one step's vector, from LogQL's documented meaning of
// the operators, and says when a ranking was tied; it is itself held to every
// untied recorded answer.

// topKFoldRefSample is one sample of an instant vector.
type topKFoldRefSample struct {
	labels map[string]string
	value  float64
}

// overAggLayer is a layer of a query: how it is written around its operand and
// what it does.
type overAggLayer struct {
	wrap func(inner string) string
	ref  aggLayer
}

// aggLayer is one vector aggregation: op (sum, avg, min, max, count, topk,
// bottomk, sort, sort_desc), k for a ranking, and its by() or without() labels.
type aggLayer struct {
	op             string
	k              int
	by, without    []string
	hasBy, hasWith bool
}

// group is the group key of a series and, for a fold, the labels its result
// keeps: the by() labels the series has, or every label but the without()
// ones; no clause is one group with no labels.
func (l aggLayer) group(labels map[string]string) (string, map[string]string) {
	kept := map[string]string{}
	switch {
	case l.hasBy:
		for _, name := range l.by {
			if v, ok := labels[name]; ok && v != "" {
				kept[name] = v
			}
		}
	case l.hasWith:
		drop := map[string]bool{}
		for _, name := range l.without {
			drop[name] = true
		}
		for name, v := range labels {
			if !drop[name] {
				kept[name] = v
			}
		}
	}
	return overAggLabelKey(kept), kept
}

// eval applies the layer to one step's vector. tied is set when a ranking had
// to choose between series of equal value across its k-th place.
func (l aggLayer) eval(vec []topKFoldRefSample) (out []topKFoldRefSample, tied bool) {
	switch l.op {
	case "sort", "sort_desc":
		return vec, false
	case "topk", "bottomk":
		members := map[string][]topKFoldRefSample{}
		var order []string
		for _, s := range vec {
			key, _ := l.group(s.labels)
			if _, ok := members[key]; !ok {
				order = append(order, key)
			}
			members[key] = append(members[key], s)
		}
		for _, key := range order {
			m := members[key]
			sort.SliceStable(m, func(i, j int) bool { return ranksBefore(m[i].value, m[j].value, l.op == "topk") })
			if len(m) > l.k {
				last := m[l.k-1].value
				if last == m[l.k].value || (math.IsNaN(last) && math.IsNaN(m[l.k].value)) {
					tied = true
				}
				m = m[:l.k]
			}
			out = append(out, m...)
		}
		return out, tied
	}
	type acc struct {
		labels   map[string]string
		sum, n   float64
		first    bool
		min, max float64
	}
	groups := map[string]*acc{}
	var order []string
	for _, s := range vec {
		key, kept := l.group(s.labels)
		a := groups[key]
		if a == nil {
			a = &acc{labels: kept, min: s.value, max: s.value}
			groups[key] = a
			order = append(order, key)
		}
		a.sum += s.value
		a.n++
		a.min = math.Min(a.min, s.value)
		a.max = math.Max(a.max, s.value)
	}
	for _, key := range order {
		a := groups[key]
		v := a.sum
		switch l.op {
		case "avg":
			v = a.sum / a.n
		case "min":
			v = a.min
		case "max":
			v = a.max
		case "count":
			v = a.n
		}
		out = append(out, topKFoldRefSample{a.labels, v})
	}
	return out, false
}

// evalChain applies the layers innermost first; tied is set when any ranking
// was tied.
func evalChain(vec []topKFoldRefSample, chain []overAggLayer) (out []topKFoldRefSample, tied bool) {
	out = vec
	for _, layer := range chain {
		var t bool
		out, t = layer.ref.eval(out)
		tied = tied || t
	}
	return out, tied
}

// overAggLabelKey is a label set as name=value, pairs, names sorted.
func overAggLabelKey(labels map[string]string) string {
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

func foldLayer(op, clause string, by, without []string, hasBy, hasWith bool) overAggLayer {
	return overAggLayer{func(inner string) string { return fmt.Sprintf("%s%s (%s)", op, clause, inner) }, aggLayer{op: op, by: by, without: without, hasBy: hasBy, hasWith: hasWith}}
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
	}, aggLayer{op: op, by: labels, hasBy: true}}
}

func rank(op string, k int) overAggLayer {
	return overAggLayer{func(inner string) string { return fmt.Sprintf("%s(%d, %s)", op, k, inner) }, aggLayer{op: op, k: k}}
}
func rankBy(op string, k int, labels ...string) overAggLayer {
	return overAggLayer{func(inner string) string {
		return fmt.Sprintf("%s by (%s) (%d, %s)", op, strings.Join(labels, ", "), k, inner)
	}, aggLayer{op: op, k: k, by: labels, hasBy: true}}
}
func rankWithout(op string, k int, labels ...string) overAggLayer {
	return overAggLayer{func(inner string) string {
		return fmt.Sprintf("%s without (%s) (%d, %s)", op, strings.Join(labels, ", "), k, inner)
	}, aggLayer{op: op, k: k, without: labels, hasWith: true}}
}
func order(op string) overAggLayer {
	return overAggLayer{func(inner string) string { return op + "(" + inner + ")" }, aggLayer{op: op}}
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

func refVectorOf(vec []topKSeries) []topKFoldRefSample {
	out := make([]topKFoldRefSample, 0, len(vec))
	for _, s := range vec {
		out = append(out, topKFoldRefSample{map[string]string{"app": s.app, "env": s.env}, s.value})
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
		key := overAggLabelKey(series.Metric)
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
func assertOverAggStep(t *testing.T, query string, ts int64, want []topKFoldRefSample, got overAggPoints) {
	t.Helper()
	wantKeys := map[string]float64{}
	for _, s := range want {
		wantKeys[overAggLabelKey(s.labels)] = s.value
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

// overAggLokiVector is a recorded Loki answer's samples at one unix second (the step's time with its fraction cut,
// as the proxy's points are read).
func overAggLokiVector(t *testing.T, id string, a lokiEngineAnswer, ts int64) []topKFoldRefSample {
	t.Helper()
	if a.Status != http.StatusOK {
		t.Fatalf("%s: Loki answered %d %s", id, a.Status, a.Error)
	}
	s, err := decodeLokiSeries(a.Body)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	var out []topKFoldRefSample
	for key, points := range s.Points {
		for ms, text := range points {
			if ms/1000 == ts {
				v, _ := strconv.ParseFloat(text, 64)
				out = append(out, topKFoldRefSample{s.Labels[key], v})
			}
		}
	}
	return out
}

// overAggChainNames are the chains in a fixed order.
func overAggChainNames() []string {
	names := make([]string, 0, len(overAggChains))
	for name := range overAggChains {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// overAggCompare compares, at one step, the proxy with Loki's recorded answer, and the layer evaluation with it,
// unless a ranking of the chain was tied over Loki's inner vector (then Loki may keep either series).
func overAggCompare(t *testing.T, query string, ts int64, inner, loki []topKFoldRefSample, chain []overAggLayer, got overAggPoints) bool {
	t.Helper()
	eval, tied := evalChain(inner, chain)
	if tied {
		return false
	}
	assertOverAggStep(t, query, ts, loki, got)
	evalPoints := overAggPoints{}
	for _, s := range eval {
		evalPoints[overAggLabelKey(s.labels)] = map[int64]float64{ts: s.value}
	}
	assertOverAggStep(t, query+" (layer evaluation)", ts, loki, evalPoints)
	return true
}

func overAggRangeDifferential(t *testing.T, name, version string, seeds []int64, shapes []topKShape, start, end time.Time) {
	base := time.Unix(1699999200, 0).UTC()
	id := func(seed int64, shape topKShape, query string) string {
		return fmt.Sprintf("%d|%s|%s", seed, shape.step, query)
	}
	skipped := func(shape topKShape, name string) bool {
		// A tumbling window under sort is answered by VictoriaLogs' stats_query_range, which this backend does
		// not emulate; TestSortOverTumblingRange covers that route.
		return shape.window == shape.step && strings.Contains(name, "sort")
	}
	var sets []oraclerun.EngineDataSet
	for _, seed := range seeds {
		set := oraclerun.EngineDataSet{Streams: topKLineStreams(topKFixture(seed, base, 60))}
		for _, shape := range shapes {
			inner := fmt.Sprintf(`sum by (app, env) (%s({app=~".+"}[%s]))`, shape.fn, formatPromDuration(shape.window))
			set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: id(seed, shape, inner), Unaligned: true, Form: topKRangeForm(inner, start, end, shape.step)})
			for _, chainName := range overAggChainNames() {
				if skipped(shape, chainName) {
					continue
				}
				query := overAggQuery(overAggChains[chainName], inner)
				set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: id(seed, shape, query), Unaligned: true, Form: topKRangeForm(query, start, end, shape.step)})
			}
		}
		sets = append(sets, set)
	}
	answers := lokiEngineAnswers(t, name, sets)
	compared := 0
	for _, seed := range seeds {
		backend := newTopKTestBackend(t, topKFixture(seed, base, 60))
		p := newGapTestProxy(t, backend.URL)
		p.storeBackendVersion(version, version)
		for _, shape := range shapes {
			inner := fmt.Sprintf(`sum by (app, env) (%s({app=~".+"}[%s]))`, shape.fn, formatPromDuration(shape.window))
			for _, chainName := range overAggChainNames() {
				if skipped(shape, chainName) {
					continue
				}
				chain := overAggChains[chainName]
				query := overAggQuery(chain, inner)
				t.Run(fmt.Sprintf("seed%d/%s/%s/%s", seed, shape.fn, chainName, formatPromDuration(shape.step)), func(t *testing.T) {
					params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.FormatFloat(shape.step.Seconds(), 'f', -1, 64)}}
					rec := httptest.NewRecorder()
					p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
					got := parseOverAggResponse(t, query, rec.Code, rec.Body.Bytes(), "matrix")
					for ts := start; !ts.After(end); ts = ts.Add(shape.step) {
						vec := overAggLokiVector(t, id(seed, shape, inner), answers[id(seed, shape, inner)], ts.Unix())
						loki := overAggLokiVector(t, id(seed, shape, query), answers[id(seed, shape, query)], ts.Unix())
						if overAggCompare(t, query, ts.Unix(), vec, loki, chain, got) {
							compared++
						}
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
	overAggRangeDifferential(t, "topk_over_aggregation_range", "v1.50.0", []int64{1, 2}, []topKShape{
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
	overAggRangeDifferential(t, "topk_over_aggregation_range_raw", "v1.44.0", []int64{4}, []topKShape{{"count_over_time", time.Minute, 5 * time.Minute}}, start, end)
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
	lokiInner := `sum by (app, env) (last_over_time({app=~".+"} | logfmt | unwrap v [1m]))`
	vectors := map[int64][]topKSeries{}
	var sets []oraclerun.EngineDataSet
	for seed := int64(1); seed <= 12; seed++ {
		vec := randomInstantVector(rand.New(rand.NewSource(seed)))
		vectors[seed] = vec
		set := oraclerun.EngineDataSet{}
		for _, s := range vec {
			set.Streams = append(set.Streams, oraclerun.EngineStream{Labels: map[string]string{"app": s.app, "env": s.env}, Entries: []oraclerun.EngineEntry{{TS: (at - 30) * 1e9, Line: "v=" + strconv.FormatFloat(s.value, 'f', -1, 64)}}})
		}
		for _, name := range overAggChainNames() {
			set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: fmt.Sprintf("%d|%s", seed, name), Instant: true, Form: url.Values{"query": {overAggQuery(overAggChains[name], lokiInner)}, "time": {strconv.Itoa(at)}}.Encode()})
		}
		sets = append(sets, set)
	}
	answers := lokiEngineAnswers(t, "topk_over_aggregation_instant", sets)
	compared := 0
	for seed := int64(1); seed <= 12; seed++ {
		vec := vectors[seed]
		var rows []string
		var inner []topKFoldRefSample
		for _, s := range vec {
			rows = append(rows, fmt.Sprintf(`{"metric":{"app":%q,"env":%q},"value":[%d,%q]}`, s.app, s.env, at, strconv.FormatFloat(s.value, 'f', -1, 64)))
			inner = append(inner, topKFoldRefSample{map[string]string{"app": s.app, "env": s.env}, s.value})
		}
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(rows, ",") + `]}}`))
		}))
		p := newGapTestProxy(t, backend.URL)
		for _, name := range overAggChainNames() {
			chain := overAggChains[name]
			query := overAggQuery(chain, topKInner)
			t.Run(fmt.Sprintf("seed%d/%s", seed, name), func(t *testing.T) {
				rec := httptest.NewRecorder()
				p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {query}, "time": {strconv.Itoa(at)}}.Encode(), nil))
				got := parseOverAggResponse(t, query, rec.Code, rec.Body.Bytes(), "vector")
				lokiID := fmt.Sprintf("%d|%s", seed, name)
				loki := overAggLokiVector(t, lokiID, answers[lokiID], at)
				if overAggCompare(t, query, at, inner, loki, chain, got) {
					compared++
				}
			})
		}
		backend.Close()
	}
	if compared == 0 {
		t.Fatal("no vector was compared")
	}
}
