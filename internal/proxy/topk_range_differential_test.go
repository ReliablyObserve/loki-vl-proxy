package proxy

import (
	"encoding/json"
	"fmt"
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

// This file pins topk and bottomk on query_range to Loki's: every step is
// ranked on its own, within each group of the by()/without() labels, and the
// result holds the union of the series that ranked at some step with their
// samples at those steps only. Loki's answers are recorded with Loki's engine
// over the same lines (test/oracle/loki engine; LVP_RUN_ORACLES=1 records them
// again).
//
// The proxy under test is the real handler over a simulated VictoriaLogs that
// answers the stats pipes of the window evaluator.

type topKLine struct {
	ts       int64
	app, env string
	msg      string
}

// topKSeries is one series of an instant vector: its labels and value.
type topKSeries struct {
	app, env string
	value    float64
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

// topKGroupings are the grouping spellings LogQL accepts on topk and bottomk
// (before and after the parameters) with the key a series groups by.
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

// topKLineStreams are the fixture lines as Loki streams: one stream per app x env, the line the message.
func topKLineStreams(lines []topKLine) []oraclerun.EngineStream {
	var out []oraclerun.EngineStream
	index := map[string]int{}
	for _, l := range lines {
		key := l.app + "/" + l.env
		i, ok := index[key]
		if !ok {
			i = len(out)
			index[key] = i
			out = append(out, oraclerun.EngineStream{Labels: map[string]string{"app": l.app, "env": l.env}})
		}
		out[i].Entries = append(out[i].Entries, oraclerun.EngineEntry{TS: l.ts, Line: l.msg})
	}
	return out
}

// topKRangeForm is a query_range request form over the differential's window.
func topKRangeForm(query string, start, end time.Time, step time.Duration) string {
	return url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}.Encode()
}

// topKLokiPoints reads a recorded Loki answer as series -> unix second -> value.
func topKLokiPoints(t *testing.T, id string, a lokiEngineAnswer) topKPoints {
	t.Helper()
	if a.Status != http.StatusOK {
		t.Fatalf("%s: Loki answered %d %s", id, a.Status, a.Error)
	}
	s, err := decodeLokiSeries(a.Body)
	if err != nil || s.ResultType != "matrix" {
		t.Fatalf("%s: %v %s", id, err, s.ResultType)
	}
	out := topKPoints{}
	for key, points := range s.Points {
		name := fmt.Sprintf("app=%s,env=%s", s.Labels[key]["app"], s.Labels[key]["env"])
		out[name] = map[int64]float64{}
		for ms, text := range points {
			v, _ := strconv.ParseFloat(text, 64)
			out[name][ms/1000] = v
		}
	}
	return out
}

// runTopKDifferential compares the proxy with Loki's recorded answers (test/oracle/loki engine, Loki's engine at
// the request's own, unaligned bounds) over generated data: the union of ranked series, the samples they keep,
// and the values selected at every step and group. Ties at a boundary are broken by the order Loki meets the
// series, so there the selected values are compared and the series are not; the ties are read from Loki's answer
// for the inner aggregation.
func runTopKDifferential(t *testing.T, name string, start, end time.Time, version string, seeds []int64, ks []int, shapes []topKShape) {
	base := time.Unix(1699999200, 0).UTC()
	type request struct {
		seed         int64
		query, inner string
		grouping     string
		op           string
		step         time.Duration
	}
	var sets []oraclerun.EngineDataSet
	var requests []request
	groupingNames := make([]string, 0, len(topKGroupings))
	for g := range topKGroupings {
		groupingNames = append(groupingNames, g)
	}
	sort.Strings(groupingNames)
	for _, seed := range seeds {
		set := oraclerun.EngineDataSet{Streams: topKLineStreams(topKFixture(seed, base, 60))}
		for _, shape := range shapes {
			inner := fmt.Sprintf(`sum by (app, env) (%s({app=~".+"}[%s]))`, shape.fn, formatPromDuration(shape.window))
			set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: fmt.Sprintf("%d|%s|%s", seed, shape.step, inner), Unaligned: true, Form: topKRangeForm(inner, start, end, shape.step)})
			for _, op := range []string{"topk", "bottomk"} {
				for _, k := range ks {
					for _, g := range groupingNames {
						query := topKGroupings[g].clause(op, k, inner)
						set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: fmt.Sprintf("%d|%s|%s", seed, shape.step, query), Unaligned: true, Form: topKRangeForm(query, start, end, shape.step)})
						requests = append(requests, request{seed: seed, query: query, inner: inner, grouping: g, op: op, step: shape.step})
					}
				}
			}
		}
		sets = append(sets, set)
	}
	answers := lokiEngineAnswers(t, name, sets)
	proxies := map[int64]*Proxy{}
	for _, seed := range seeds {
		backend := newTopKTestBackend(t, topKFixture(seed, base, 60))
		t.Cleanup(backend.Close)
		p := newGapTestProxy(t, backend.URL)
		p.storeBackendVersion(version, version)
		proxies[seed] = p
	}
	for _, rq := range requests {
		t.Run(fmt.Sprintf("seed%d/%s/%s", rq.seed, rq.grouping, rq.query), func(t *testing.T) {
			g := topKGroupings[rq.grouping]
			got := queryTopKRange(t, proxies[rq.seed], rq.query, start, end, rq.step)
			lokiID := fmt.Sprintf("%d|%s|%s", rq.seed, rq.step, rq.query)
			innerID := fmt.Sprintf("%d|%s|%s", rq.seed, rq.step, rq.inner)
			loki := topKLokiPoints(t, lokiID, answers[lokiID])
			vectors := topKLokiPoints(t, innerID, answers[innerID])
			tied := map[string]bool{}
			for ts := start; !ts.After(end); ts = ts.Add(rq.step) {
				var vec, picked []topKSeries
				for key, pts := range vectors {
					if v, ok := pts[ts.Unix()]; ok {
						s := topKSeriesOf(key, v)
						vec = append(vec, s)
						if lv, ok := loki[key][ts.Unix()]; ok {
							s.value = lv
							picked = append(picked, s)
						}
					}
				}
				for group := range topKTiedGroups(vec, picked, g.key) {
					tied[fmt.Sprintf("%d|%s", ts.Unix(), group)] = true
				}
				assertTopKStep(t, rq.query, ts, vec, picked, got, g.key, rq.op == "topk")
			}
			for key, pts := range got {
				for ts := range pts {
					if _, ok := loki[key][ts]; !ok && !tied[fmt.Sprintf("%d|%s", ts, g.key(topKSeriesOf(key, 0)))] {
						t.Errorf("%s: unexpected sample %s @%d", rq.query, key, ts)
					}
				}
			}
		})
	}
}

// topKSeriesOf reads a series name app=<a>,env=<e>.
func topKSeriesOf(key string, value float64) topKSeries {
	var app, env string
	if i := strings.Index(key, ",env="); i > 0 {
		app, env = strings.TrimPrefix(key[:i], "app="), key[i+len(",env="):]
	}
	return topKSeries{app: app, env: env, value: value}
}

// topKTiedGroups are the groups of a step whose kept samples tie across the k-th place: some series not kept has
// the value of the last kept one, so Loki could have kept either.
func topKTiedGroups(vec, picked []topKSeries, group func(topKSeries) string) map[string]bool {
	kept := map[string]map[float64]int{}
	for _, s := range picked {
		g := group(s)
		if kept[g] == nil {
			kept[g] = map[float64]int{}
		}
		kept[g][s.value]++
	}
	all := map[string]map[float64]int{}
	for _, s := range vec {
		g := group(s)
		if all[g] == nil {
			all[g] = map[float64]int{}
		}
		all[g][s.value]++
	}
	out := map[string]bool{}
	for g, values := range kept {
		for v, n := range values {
			if all[g][v] > n {
				out[g] = true
			}
		}
	}
	return out
}

// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings, limits/top-n-ranking-not-silent, loki_api_v1_query_range
func TestTopKRangeDifferential(t *testing.T) {
	base := time.Unix(1699999200, 0).UTC()
	// An unaligned start: Grafana's now-1h is never on a step boundary.
	start, end := base.Add(10*time.Minute+7300*time.Millisecond), base.Add(50*time.Minute+7300*time.Millisecond)
	runTopKDifferential(t, "topk_range", start, end, "v1.50.0", []int64{1, 2, 3}, []int{1, 2, 5, 100}, []topKShape{
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
	runTopKDifferential(t, "topk_range_raw", start, end, "v1.44.0", []int64{4}, []int{2}, []topKShape{
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
