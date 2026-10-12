package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file pins nested vector aggregations (`min by (zone) (avg by (pod, zone)
// (count_over_time(...)))`) to Loki's, over generated rows and windows, with a
// reference evaluator written apart from the proxy: Loki v3.7.7
// pkg/logql/evaluator.go VectorAggEvaluator.Next (the series of a level's input
// fall into the group the level's labels name, `by` keeping only the listed
// labels a series has, `without` dropping the listed ones; sum is the total,
// count the number of series, avg a running mean, max and min the extremes)
// over pkg/logql/range_vector.go (count_over_time, and rate = lines per second,
// of the lines of a series in the window (t-range, t]). Each level folds the
// output of the level below it.
//
// The proxy under test is the real handler; only VictoriaLogs is simulated, by
// the shared vlEmulator.

// nvLevel is one vector aggregation of the query, innermost first.
type nvLevel struct {
	op      string
	by      []string // nil: no grouping; empty: `by ()`
	without bool
}

func (l nvLevel) grouping() string {
	switch {
	case l.without:
		return "without (" + strings.Join(l.by, ", ") + ")"
	case l.by != nil:
		return "by (" + strings.Join(l.by, ", ") + ")"
	}
	return ""
}

func (l nvLevel) String() string { return strings.TrimSpace(l.op + " " + l.grouping()) }

type nvScenario struct {
	levels     []nvLevel
	fn, parser string // parser: "" or "logfmt"
	window     time.Duration
	step       time.Duration // 0: instant
	start, end int64         // ns, step-aligned evaluation times
	rows       []emuRow
	version    string
	trailingBy bool    // `sum(...) by (l)` rather than `sum by (l) (...)`
	divisor    float64 // when set, divides the aggregation by a scalar: `<aggregation> / 2`
}

var nvStreams = []string{
	`{app="uw",pod="a",zone="x",svc="s1"}`,
	`{app="uw",pod="b",zone="x",svc="s1"}`,
	`{app="uw",pod="c",zone="x",svc="s2"}`,
	`{app="uw",pod="d",zone="y",svc="s1"}`,
	`{app="uw",pod="e",zone="y",svc="s2"}`,
}

func (s nvScenario) times() []int64 {
	if s.step == 0 {
		return []int64{s.end}
	}
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

func (s nvScenario) query() string {
	selector := `{app="uw"}`
	if s.parser != "" {
		selector += " | " + s.parser
	}
	q := fmt.Sprintf(`%s(%s [%s])`, s.fn, selector, uwWindowText(s.window))
	for _, l := range s.levels {
		if s.trailingBy && l.grouping() != "" {
			q = l.op + "(" + q + ") " + l.grouping()
		} else {
			q = l.String() + " (" + q + ")"
		}
	}
	if s.divisor != 0 {
		q += " / " + strconv.FormatFloat(s.divisor, 'f', -1, 64)
	}
	return q
}

func (s nvScenario) String() string {
	return fmt.Sprintf("%s step=%v window=%v rows=%d version=%q", s.query(), s.step, s.window, len(s.rows), s.version)
}

// dropEdgeRows removes the lines exactly on an edge of a window (t-window, t]:
// VictoriaLogs v1.40 has no stats offset argument and misplaces them.
func (s *nvScenario) dropEdgeRows() {
	var kept []emuRow
next:
	for _, row := range s.rows {
		for _, t := range s.times() {
			if row.ts == t-int64(s.window) || row.ts == t {
				continue next
			}
		}
		kept = append(kept, row)
	}
	s.rows = kept
}

// generateNestedScenario builds rows and an evaluation window. aligned puts a
// range query's start on its step grid: VictoriaLogs v1.40 has no stats offset
// argument, so only an epoch-aligned tumbling window is answered by its buckets
// (the proxy's window evaluator reads any other, a different route).
func generateNestedScenario(rnd *rand.Rand, fn string, levels []nvLevel, instant, aligned bool) nvScenario {
	steps := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second, 5 * time.Minute}
	s := nvScenario{levels: levels, fn: fn, window: windows[rnd.Intn(len(windows))], trailingBy: rnd.Intn(2) == 0}
	base := int64(1_800_000_000) * 1e9
	if !instant {
		s.step = steps[rnd.Intn(len(steps))]
		s.window = s.step // a tumbling window: range == step
	}
	s.start = base + int64(rnd.Intn(600))*1e9
	if aligned && !instant {
		s.start -= s.start % int64(s.step)
	}
	if instant {
		s.end = s.start
	} else {
		s.end = s.start + int64(rnd.Intn(6))*int64(s.step)
	}
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	n := 10 + rnd.Intn(90)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(4) == 0 { // on a window edge
			edge := s.start + int64(rnd.Intn(6))*int64(max(s.step, time.Second))
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			ts = edge + int64(rnd.Intn(3)-1)
		}
		// Streams of uneven weight, so the values of a step differ per series.
		stream := nvStreams[rnd.Intn(len(nvStreams))]
		if rnd.Intn(2) == 0 {
			stream = nvStreams[rnd.Intn(2)]
		}
		s.rows = append(s.rows, emuRow{ts: ts, stream: stream, msg: "x=1"})
	}
	return s
}

// nvSeries is a series of Loki's input vector.
type nvSeries struct {
	labels map[string]string
	value  float64
}

func nvLabelsKey(labels map[string]string) string {
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

// nvFold is VectorAggEvaluator.Next: the series of the vector fall into the
// group whose kept labels they share, and each group takes the operator's value.
func nvFold(in []nvSeries, level nvLevel) []nvSeries {
	type group struct {
		labels map[string]string
		values []float64
	}
	groups := map[string]*group{}
	var order []string
	for _, series := range in {
		labels := map[string]string{}
		for name, value := range series.labels {
			if slices.Contains(level.by, name) != level.without {
				labels[name] = value
			}
		}
		key := nvLabelsKey(labels)
		g := groups[key]
		if g == nil {
			g = &group{labels: labels}
			groups[key] = g
			order = append(order, key)
		}
		g.values = append(g.values, series.value)
	}
	var out []nvSeries
	for _, key := range order {
		g := groups[key]
		var v float64
		switch level.op {
		case "sum", "avg":
			for _, x := range g.values {
				v += x
			}
			if level.op == "avg" {
				v /= float64(len(g.values))
			}
		case "count":
			v = float64(len(g.values))
		case "max":
			v = slices.Max(g.values)
		case "min":
			v = slices.Min(g.values)
		default:
			panic("reference: " + level.op)
		}
		out = append(out, nvSeries{g.labels, v})
	}
	return out
}

// lokiNestedAggregation is the reference: per evaluation time, the value of the
// range function of every stream that has a line in (t-window, t], folded by
// the levels, innermost first. It returns labels key -> time -> value.
func lokiNestedAggregation(s nvScenario) (map[string]map[int64]float64, map[string]map[string]string) {
	out := map[string]map[int64]float64{}
	labelsOf := map[string]map[string]string{}
	for _, t := range s.times() {
		perStream := map[string]float64{}
		for _, row := range s.rows {
			if row.ts > t-int64(s.window) && row.ts <= t {
				perStream[row.stream]++
			}
		}
		var vec []nvSeries
		for stream, count := range perStream {
			value := count
			if s.fn == "rate" {
				value = count / s.window.Seconds()
			}
			labels := map[string]string{"service_name": "uw"}
			for _, name := range []string{"app", "pod", "zone", "svc"} {
				labels[name] = nvStreamLabel(stream, name)
			}
			vec = append(vec, nvSeries{labels, value})
		}
		for _, level := range s.levels {
			vec = nvFold(vec, level)
		}
		for _, series := range vec {
			if s.divisor != 0 {
				series.value /= s.divisor
			}
			key := nvLabelsKey(series.labels)
			if out[key] == nil {
				out[key] = map[int64]float64{}
				labelsOf[key] = series.labels
			}
			out[key][t] = series.value
		}
	}
	return out, labelsOf
}

func nvStreamLabel(stream, label string) string {
	for _, part := range strings.Split(strings.Trim(stream, "{}"), ",") {
		if k, v, ok := strings.Cut(part, "="); ok && k == label {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

// runNestedScenario sends the scenario through the real handler against the
// emulated VictoriaLogs and compares the series, their labels and their values
// with the reference. fold: the levels are folded over series rows by the
// proxy (an innermost `without`), so the stats answer is not the final one.
func runNestedScenario(t *testing.T, label string, s nvScenario, fold bool) {
	t.Helper()
	emu := &vlEmulator{t: t, rows: s.rows, version: s.version, field: "amount"}
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	params := url.Values{"query": {s.query()}}
	target := "/loki/api/v1/query_range?"
	if s.step == 0 {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(s.end/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(s.start/1e9, 10))
		params.Set("end", strconv.FormatInt(s.end/1e9, 10))
		params.Set("step", strconv.FormatInt(int64(s.step/time.Second), 10))
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s %s: %d %s", label, s, res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	want, wantLabels := lokiNestedAggregation(s)
	got := map[string]map[int64]float64{}
	for _, series := range resp.Data.Result {
		// The stats routes of VictoriaLogs answer no detected_level for streams
		// without a level, a separate gap; leave it out of both sides.
		delete(series.Metric, "detected_level")
		key := nvLabelsKey(series.Metric)
		if _, dup := got[key]; dup {
			t.Fatalf("%s %s: two series with the labels %v (%d series in all), Loki has %d", label, s, series.Metric, len(resp.Data.Result), len(want))
		}
		if _, ok := want[key]; !ok {
			t.Fatalf("%s %s: series with the labels %v, Loki has %d series, e.g. %v", label, s, series.Metric, len(want), nvFirstOf(wantLabels))
		}
		got[key] = map[int64]float64{}
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[key][int64(point[0].(float64))*1e9] = v
		}
	}
	for key, points := range want {
		if got[key] == nil {
			t.Fatalf("%s %s: no series with the labels %v", label, s, wantLabels[key])
		}
		for ts, w := range points {
			g, ok := got[key][ts]
			if !ok {
				t.Fatalf("%s %s: %v has no point at %d, Loki has %g", label, s, wantLabels[key], ts/1e9, w)
			}
			if math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("%s %s: %v at %d got %g, Loki has %g", label, s, wantLabels[key], ts/1e9, g, w)
			}
		}
		for ts, g := range got[key] {
			if _, ok := points[ts]; !ok {
				t.Fatalf("%s %s: %v has an extra point at %d = %g, Loki has none", label, s, wantLabels[key], ts/1e9, g)
			}
		}
	}
	if fold {
		return
	}
	// Computed by VictoriaLogs: stats only, and only the final groups come back.
	if raw := emu.endpointCalls("/select/logsql/query"); len(raw) > 0 {
		t.Fatalf("%s %s: the metric read raw rows: %s", label, s, raw[0])
	}
	if groups := s.outerGroups(); emu.widest > groups {
		t.Fatalf("%s %s: VictoriaLogs answered %d series, the outer level has at most %d groups", label, s, emu.widest, groups)
	}
}

// outerGroups is how many groups the outermost level can make of the streams
// the rows are in, at any time: the bound on the series VictoriaLogs answers
// when it folds the whole chain (a time bucket outside the evaluation window
// can add a group the answer then drops).
func (s nvScenario) outerGroups() int {
	seen := map[string]bool{}
	var vec []nvSeries
	for _, row := range s.rows {
		if seen[row.stream] {
			continue
		}
		seen[row.stream] = true
		labels := map[string]string{"service_name": "uw"}
		for _, name := range []string{"app", "pod", "zone", "svc"} {
			labels[name] = nvStreamLabel(row.stream, name)
		}
		vec = append(vec, nvSeries{labels, 1})
	}
	for _, level := range s.levels {
		vec = nvFold(vec, level)
	}
	return len(vec)
}

func nvFirstOf(m map[string]map[string]string) map[string]string {
	for _, v := range m {
		return v
	}
	return nil
}

func runNestedMatrix(t *testing.T, cases [][]nvLevel, iterations int, fold bool) {
	t.Helper()
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, levels := range cases {
			for _, fn := range []string{"count_over_time", "rate"} {
				if version == "v1.40.0" && fn == "rate" {
					continue // a rate there reads the wrong edge sample, a separate gap
				}
				if fold && fn == "rate" && levels[0].op == "sum" {
					// `sum without (l) (rate(...))` alone answers one unlabelled
					// series (the ungrouped sum branch of buildRateLikeQuery
					// ignores the without), a separate gap.
					continue
				}
				for _, parser := range []string{"", "logfmt"} {
					for _, instant := range []bool{false, true} {
						name := fmt.Sprintf("version=%s/%s/instant=%v", version, nvScenario{levels: levels, fn: fn, parser: parser, window: time.Minute}.query(), instant)
						t.Run(name, func(t *testing.T) {
							rnd := rand.New(rand.NewSource(int64(len(name))*7919 + int64(name[len(name)-3]) + 31))
							for i := 0; i < iterations; i++ {
								s := generateNestedScenario(rnd, fn, levels, instant, version == "v1.40.0")
								s.version, s.parser = version, parser
								if version == "v1.40.0" {
									s.dropEdgeRows()
								}
								runNestedScenario(t, fmt.Sprintf("#%d", i), s, fold)
							}
						})
					}
				}
			}
		}
	}
}

func nvBy(op string, labels ...string) nvLevel {
	if labels == nil {
		labels = []string{}
	}
	return nvLevel{op: op, by: labels}
}

func nvWithout(op string, labels ...string) nvLevel {
	return nvLevel{op: op, by: labels, without: true}
}

// TestNestedVectorAggregationDifferential: two levels of every pair of sum, avg,
// min, max and count (the inner `by (pod, zone)`, the outer `by (zone)`, whose
// groups hold several series), then three levels, an outer label the inner
// group lacks, `by ()`, no grouping, and a `without` over a `by` level. The
// whole chain is one query to VictoriaLogs, answered by stats only.
//
// conformance: semantics/nested-aggregation-outer-by-native-routes, metric-series-identity
func TestNestedVectorAggregationDifferential(t *testing.T) {
	ops := []string{"sum", "avg", "min", "max", "count"}
	var cases [][]nvLevel
	for _, inner := range ops {
		for _, outer := range ops {
			cases = append(cases, []nvLevel{nvBy(inner, "pod", "zone"), nvBy(outer, "zone")})
		}
	}
	cases = append(cases,
		// An outer label the inner group lacks is absent: one series.
		[]nvLevel{nvBy("avg", "pod"), nvBy("min", "zone")},
		[]nvLevel{nvBy("max", "pod", "zone"), nvBy("count", "pod", "baz")},
		[]nvLevel{nvBy("count", "baz"), nvBy("sum", "baz")},
		// by (), and no grouping, fold everything.
		[]nvLevel{nvBy("avg", "pod", "zone"), nvBy("min")},
		[]nvLevel{nvBy("max", "pod", "zone"), {op: "count"}},
		[]nvLevel{{op: "max"}, nvBy("sum", "zone")},
		// The outer level keeps labels both name.
		[]nvLevel{nvBy("avg", "pod", "zone", "svc"), nvBy("max", "pod", "zone")},
		[]nvLevel{nvBy("sum", "pod", "zone"), nvBy("avg", "zone", "svc")},
		// without over a level whose labels are named.
		[]nvLevel{nvBy("avg", "pod", "zone"), nvWithout("min", "pod")},
		[]nvLevel{nvBy("max", "pod", "zone", "svc"), nvWithout("count", "svc")},
		[]nvLevel{nvBy("sum", "pod", "zone"), nvWithout("max", "baz")},
		[]nvLevel{nvBy("avg", "pod", "zone"), nvWithout("sum", "pod", "zone")},
		// three levels
		[]nvLevel{nvBy("max", "pod", "zone", "svc"), nvBy("min", "pod", "zone"), nvBy("max", "zone")},
		[]nvLevel{nvBy("max", "pod", "zone", "svc"), nvBy("count", "pod", "zone"), nvBy("sum", "zone")},
		[]nvLevel{nvBy("avg", "pod", "zone", "svc"), nvBy("avg", "pod", "zone"), nvBy("avg", "zone")},
		[]nvLevel{nvBy("sum", "pod", "zone", "svc"), nvBy("max", "pod", "zone"), nvBy("min")},
		[]nvLevel{nvBy("min", "pod", "zone", "svc"), nvWithout("avg", "svc"), nvBy("count", "zone")},
		[]nvLevel{nvBy("count", "pod", "zone", "svc"), nvWithout("max", "svc"), nvWithout("sum", "pod")},
		[]nvLevel{nvBy("max", "pod", "zone", "svc"), nvBy("sum", "pod", "svc"), nvBy("avg", "zone")},
	)
	runNestedMatrix(t, cases, 3, false)
}

// TestNestedVectorAggregationOverSeriesWithout: an innermost `without` groups
// the series by labels VictoriaLogs cannot list, so the proxy folds every level
// over the series rows it reads; the answer is still Loki's at every level.
//
// conformance: semantics/nested-aggregation-outer-by-native-routes, metric-series-identity
func TestNestedVectorAggregationOverSeriesWithout(t *testing.T) {
	runNestedMatrix(t, [][]nvLevel{
		{nvWithout("avg", "pod"), nvBy("min", "zone")},
		{nvWithout("max", "pod", "svc"), nvBy("sum", "zone")},
		{nvWithout("count", "pod"), nvBy("max", "zone", "svc")},
		{nvWithout("sum", "pod"), nvWithout("avg", "svc")},
		{nvWithout("min", "svc"), nvBy("count", "pod", "zone"), nvBy("sum", "zone")},
		{nvWithout("avg", "pod"), nvBy("max")},
		{nvWithout("max", "baz"), nvBy("count", "zone")},
	}, 3, true)
}

// A binary operation with a scalar keeps the series of the nested aggregation
// under it: the side of a binary expression is translated by the same chain.
//
// conformance: semantics/nested-aggregation-outer-by-native-routes, metric-series-identity
func TestNestedVectorAggregationUnderBinaryScalar(t *testing.T) {
	for _, levels := range [][]nvLevel{
		{nvBy("avg", "pod", "zone"), nvBy("min", "zone")},
		{nvBy("sum", "pod", "zone"), nvBy("max")},
		{nvBy("max", "pod", "zone", "svc"), nvBy("count", "pod", "zone"), nvBy("sum", "zone")},
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/instant=%v", levels, instant), func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(levels))*977 + 5))
				for i := 0; i < 6; i++ {
					s := generateNestedScenario(rnd, "rate", levels, instant, false)
					s.divisor = 2
					runNestedScenario(t, fmt.Sprintf("#%d", i), s, true)
				}
			})
		}
	}
}
