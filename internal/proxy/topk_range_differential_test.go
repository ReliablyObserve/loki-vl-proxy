package proxy

import (
	"container/heap"
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

// This file pins topk and bottomk on query_range to Loki's: every step is
// ranked on its own, within each group of the by()/without() labels, and the
// result holds the union of the series that ranked at some step with their
// samples at those steps only. The reference is written apart from the proxy:
// it evaluates every window from the lines (Loki v3.7.7 pkg/logql/range_vector.go,
// the window (t-range, t]) and ranks with the heap of pkg/logql/evaluator.go
// VectorAggEvaluator.Next and pkg/logql/vector.go (vectorByValueHeap).
//
// The proxy under test is the real handler over a simulated VictoriaLogs that
// answers the stats pipes of the window evaluator.

type topKLine struct {
	ts       int64
	app, env string
	msg      string
}

// topKSeries is one series of the reference inner vector.
type topKSeries struct {
	app, env string
	value    float64
}

// lokiTopK ranks one step's vector like VectorAggEvaluator.Next. group maps a
// series to its grouping key.
func lokiTopK(vec []topKSeries, k int, descending bool, group func(topKSeries) string) []topKSeries {
	heaps := map[string]*refHeap{}
	order := []string{}
	for _, s := range vec {
		key := group(s)
		h, ok := heaps[key]
		if !ok {
			h = &refHeap{descending: descending}
			heaps[key] = h
			order = append(order, key)
			heap.Push(h, s)
			continue
		}
		root := h.items[0].value
		better := root < s.value
		if !descending {
			better = root > s.value
		}
		if len(h.items) < k || better || math.IsNaN(root) {
			if len(h.items) == k {
				heap.Pop(h)
			}
			heap.Push(h, s)
		}
	}
	var out []topKSeries
	for _, key := range order {
		out = append(out, heaps[key].items...)
	}
	return out
}

// refHeap is vectorByValueHeap (topk) and vectorByReverseValueHeap (bottomk).
type refHeap struct {
	items      []topKSeries
	descending bool
}

func (h *refHeap) Len() int { return len(h.items) }
func (h *refHeap) Less(i, j int) bool {
	if math.IsNaN(h.items[i].value) {
		return true
	}
	if h.descending {
		return h.items[i].value < h.items[j].value
	}
	return h.items[i].value > h.items[j].value
}
func (h *refHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *refHeap) Push(x any)    { h.items = append(h.items, x.(topKSeries)) }
func (h *refHeap) Pop() any {
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return last
}

// topKFixture generates lines for app x env series with gaps and changing
// leaders: a series is silent for stretches of minutes, so it enters and
// leaves the vector.
func topKFixture(seed int64, base time.Time, minutes int) []topKLine {
	rng := rand.New(rand.NewSource(seed))
	var lines []topKLine
	for _, app := range []string{"a1", "a2", "a3", "a4"} {
		for _, env := range []string{"dev", "prod", "stage"} {
			weight := 1 + rng.Intn(5)
			for m := 0; m < minutes; m++ {
				if rng.Intn(6) == 0 {
					continue
				}
				for i, n := 0, rng.Intn(weight*2+1); i < n; i++ {
					ts := base.Add(time.Duration(m)*time.Minute + time.Duration(rng.Int63n(int64(time.Minute))))
					lines = append(lines, topKLine{ts: ts.UnixNano(), app: app, env: env, msg: strings.Repeat("x", 3+rng.Intn(20))})
				}
			}
		}
	}
	return lines
}

// topKReferenceVector is the inner vector at step t: one sample per series
// whose window (t-window, t] holds a line.
func topKReferenceVector(lines []topKLine, fn string, t time.Time, window time.Duration) []topKSeries {
	type id struct{ app, env string }
	count, size := map[id]float64{}, map[id]float64{}
	for _, l := range lines {
		if l.ts <= t.Add(-window).UnixNano() || l.ts > t.UnixNano() {
			continue
		}
		count[id{l.app, l.env}]++
		size[id{l.app, l.env}] += float64(len(l.msg))
	}
	var vec []topKSeries
	for k, c := range count {
		v := c
		switch fn {
		case "rate":
			v = c / window.Seconds()
		case "bytes_over_time":
			v = size[k]
		}
		vec = append(vec, topKSeries{k.app, k.env, v})
	}
	sort.Slice(vec, func(i, j int) bool { return vec[i].app+vec[i].env < vec[j].app+vec[j].env })
	return vec
}

func newTopKTestBackend(t *testing.T, lines []topKLine) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		body, _, ok := emulateVLStatsPipe(r.FormValue("query"), parseFakeVLTime(t, r.FormValue("start")), parseFakeVLTime(t, r.FormValue("end")), func(yield func(int64, map[string]string, string)) {
			for _, l := range lines {
				yield(l.ts, map[string]string{"app": l.app, "env": l.env}, l.msg)
			}
		})
		switch {
		case r.URL.Path == "/select/logsql/query" && ok:
			_, _ = w.Write(body)
		case r.URL.Path == "/select/logsql/query":
			// The raw evaluator of a backend without anchored buckets.
			start, end := parseFakeVLTime(t, r.FormValue("start")), parseFakeVLTime(t, r.FormValue("end"))
			for _, l := range lines {
				if l.ts < start || l.ts >= end || !fakeWindowPhaseKeep(t, r.FormValue("query"), l.ts) {
					continue
				}
				row, _ := json.Marshal(map[string]string{"_time": time.Unix(0, l.ts).UTC().Format(time.RFC3339Nano), "_msg": l.msg, "_stream": fmt.Sprintf(`{app=%q,env=%q}`, l.app, l.env), "app": l.app, "env": l.env})
				_, _ = w.Write(append(row, '\n'))
			}
		default:
			http.NotFound(w, r)
		}
	}))
}

type topKPoints map[string]map[int64]float64 // series labels -> unix second -> value

func queryTopKRange(t *testing.T, p *Proxy, query string, start, end time.Time, step time.Duration) topKPoints {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	var resp struct {
		Data struct {
			ResultType string
			Result     []struct {
				Metric map[string]string
				Values [][]any
			}
		}
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil || resp.Data.ResultType != "matrix" {
		t.Fatalf("%s: %d %.300s", query, rec.Code, rec.Body)
	}
	got := topKPoints{}
	for _, series := range resp.Data.Result {
		key := fmt.Sprintf("app=%s,env=%s", series.Metric["app"], series.Metric["env"])
		if got[key] != nil {
			t.Fatalf("%s: duplicate series %s", query, key)
		}
		got[key] = map[int64]float64{}
		for _, pair := range series.Values {
			v, _ := strconv.ParseFloat(pair[1].(string), 64)
			got[key][int64(pair[0].(float64))] = v
		}
	}
	return got
}

type topKGrouping struct {
	clause func(op string, k int, inner string) string
	key    func(topKSeries) string
}

func topKSpelling(format string, key func(topKSeries) string) topKGrouping {
	return topKGrouping{func(op string, k int, inner string) string { return fmt.Sprintf(format, op, k, inner) }, key}
}

// topKGroupings are the spellings of Loki's grammar (syntax.y:182-184) with
// the key the reference groups by.
var topKGroupings = map[string]topKGrouping{
	"ungrouped":       topKSpelling("%s(%d, %s)", func(topKSeries) string { return "" }),
	"spaced":          topKSpelling("%s (%d, %s)", func(topKSeries) string { return "" }),
	"by env":          topKSpelling("%s by (env) (%d, %s)", func(s topKSeries) string { return s.env }),
	"by env trailing": topKSpelling("%s(%d, %s) by (env)", func(s topKSeries) string { return s.env }),
	"by app,env":      topKSpelling("%s by (app, env) (%d, %s)", func(s topKSeries) string { return s.app + "/" + s.env }),
	"without app":     topKSpelling("%s without (app) (%d, %s)", func(s topKSeries) string { return s.env }),
	"without env":     topKSpelling("%s(%d, %s) without (env)", func(s topKSeries) string { return s.app }),
}

type topKShape struct {
	fn     string
	step   time.Duration
	window time.Duration
}

// runTopKDifferential compares the proxy with the per-step reference over
// generated data: the union of ranked series, the samples they keep, and the
// values selected at every step and group. Ties at a boundary are broken by an
// unordered map in Loki, so there the selected values are compared and the
// series are not.
func runTopKDifferential(t *testing.T, start, end time.Time, version string, seeds []int64, ks []int, shapes []topKShape) {
	base := time.Unix(1699999200, 0).UTC()
	for _, seed := range seeds {
		lines := topKFixture(seed, base, 60)
		backend := newTopKTestBackend(t, lines)
		p := newGapTestProxy(t, backend.URL)
		p.storeBackendVersion(version, version)
		for _, shape := range shapes {
			inner := fmt.Sprintf(`sum by (app, env) (%s({app=~".+"}[%s]))`, shape.fn, formatPromDuration(shape.window))
			for _, op := range []string{"topk", "bottomk"} {
				for _, k := range ks {
					for name, g := range topKGroupings {
						query := g.clause(op, k, inner)
						t.Run(fmt.Sprintf("seed%d/%s/%s", seed, name, query), func(t *testing.T) {
							got := queryTopKRange(t, p, query, start, end, shape.step)
							want, tied := topKPoints{}, map[string]bool{}
							rng := rand.New(rand.NewSource(seed))
							for ts := start; !ts.After(end); ts = ts.Add(shape.step) {
								vec := topKReferenceVector(lines, shape.fn, ts, shape.window)
								// Loki meets the series in map order: a tie at the k-th
								// place keeps whichever came first, not the first label.
								shuffled := append([]topKSeries(nil), vec...)
								rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
								picked := lokiTopK(shuffled, k, op == "topk", g.key)
								for _, s := range picked {
									key := fmt.Sprintf("app=%s,env=%s", s.app, s.env)
									if want[key] == nil {
										want[key] = map[int64]float64{}
									}
									want[key][ts.Unix()] = s.value
								}
								for group := range tiedGroups(vec, g.key, k, op == "topk") {
									tied[fmt.Sprintf("%d|%s", ts.Unix(), group)] = true
								}
								assertTopKStep(t, query, ts, vec, picked, got, g.key, op == "topk")
							}
							for key, pts := range got {
								for ts := range pts {
									var app, env string
									_, _ = fmt.Sscanf(key, "app=%2s,env=%s", &app, &env)
									if _, ok := want[key][ts]; !ok && !tied[fmt.Sprintf("%d|%s", ts, g.key(topKSeries{app: app, env: env}))] {
										t.Errorf("%s: unexpected sample %s @%d", query, key, ts)
									}
								}
							}
						})
					}
				}
			}
		}
		backend.Close()
	}
}

// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, limits/top-n-ranking-not-silent, loki_api_v1_query_range
func TestTopKRangeDifferential(t *testing.T) {
	base := time.Unix(1699999200, 0).UTC()
	// An unaligned start: Grafana's now-1h is never on a step boundary.
	start, end := base.Add(10*time.Minute+7300*time.Millisecond), base.Add(50*time.Minute+7300*time.Millisecond)
	runTopKDifferential(t, start, end, "v1.50.0", []int64{1, 2, 3}, []int{1, 2, 5, 100}, []topKShape{
		{"count_over_time", time.Minute, 5 * time.Minute},  // sliding
		{"count_over_time", time.Minute, time.Minute},      // tumbling
		{"rate", 90 * time.Second, 5 * time.Minute},        // window not a multiple of the step
		{"bytes_over_time", time.Minute, 3 * time.Minute},  // sliding bytes
		{"count_over_time", 45 * time.Second, time.Minute}, // window longer than the step, uneven
	})
}

// A backend that cannot anchor buckets (before v1.45) and an unaligned start
// send the window to the raw evaluator, whose matrix the proxy ranks itself.
// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, loki_api_v1_query_range
func TestTopKRangeDifferentialRawEvaluator(t *testing.T) {
	base := time.Unix(1699999200, 0).UTC()
	start, end := base.Add(10*time.Minute+30*time.Second), base.Add(40*time.Minute+30*time.Second)
	runTopKDifferential(t, start, end, "v1.44.0", []int64{4}, []int{2}, []topKShape{
		{"count_over_time", time.Minute, 5 * time.Minute},
		{"bytes_over_time", time.Minute, 2 * time.Minute},
	})
}

func formatPromDuration(d time.Duration) string {
	if d%time.Minute == 0 {
		return strconv.Itoa(int(d/time.Minute)) + "m"
	}
	return strconv.Itoa(int(d/time.Second)) + "s"
}

// assertTopKStep checks the samples the proxy holds at one step: per group the
// selected values equal the reference's, and where no tie sits on the
// boundary the selected series equal too.
func assertTopKStep(t *testing.T, query string, ts time.Time, vec, picked []topKSeries, got topKPoints, group func(topKSeries) string, descending bool) {
	t.Helper()
	byGroup := func(in []topKSeries) map[string][]float64 {
		out := map[string][]float64{}
		for _, s := range in {
			out[group(s)] = append(out[group(s)], s.value)
		}
		for _, v := range out {
			sort.Float64s(v)
		}
		return out
	}
	gotVec := []topKSeries{}
	for _, s := range vec {
		if v, ok := got[fmt.Sprintf("app=%s,env=%s", s.app, s.env)][ts.Unix()]; ok {
			s.value = v
			gotVec = append(gotVec, s)
		}
	}
	want, have := byGroup(picked), byGroup(gotVec)
	if fmt.Sprint(want) != fmt.Sprint(have) {
		t.Errorf("%s @%d: selected values %v, want %v", query, ts.Unix(), have, want)
		return
	}
	// Series are compared per group when its boundary holds no tie.
	for g, values := range want {
		boundary := values[len(values)-1] // bottomk: the largest kept
		if descending {
			boundary = values[0] // topk: the smallest kept
		}
		ties := 0
		for _, s := range vec {
			if group(s) == g && s.value == boundary {
				ties++
			}
		}
		if ties > 1 {
			continue
		}
		for _, s := range picked {
			if group(s) == g {
				if _, ok := got[fmt.Sprintf("app=%s,env=%s", s.app, s.env)][ts.Unix()]; !ok {
					t.Errorf("%s @%d: series app=%s env=%s missing", query, ts.Unix(), s.app, s.env)
				}
			}
		}
	}
}

// tiedGroups returns the groups of vec with a tie across the k-th place, where
// Loki's choice depends on map order.
func tiedGroups(vec []topKSeries, group func(topKSeries) string, k int, descending bool) map[string]bool {
	byGroup := map[string][]float64{}
	for _, s := range vec {
		byGroup[group(s)] = append(byGroup[group(s)], s.value)
	}
	tied := map[string]bool{}
	for g, values := range byGroup {
		sort.Float64s(values)
		if descending {
			sort.Sort(sort.Reverse(sort.Float64Slice(values)))
		}
		if len(values) > k && values[k-1] == values[k] {
			tied[g] = true
		}
	}
	return tied
}
