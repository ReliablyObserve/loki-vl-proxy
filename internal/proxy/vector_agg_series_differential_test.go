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

// This file pins count, min, max and avg of a range function to Loki's, over
// generated rows and windows, with a reference evaluator written apart from the
// proxy: Loki v3.7.7 pkg/logql/evaluator.go VectorAggEvaluator.Next (every
// series of the input vector is folded into the group its labels name, `by`
// keeping the listed labels, `without` dropping them, no grouping keeping none;
// count is the number of series, avg a running mean, max and min the extreme of
// their values) over pkg/logql/range_vector.go (count_over_time and rate of the
// lines of a series in the window (t-range, t]).
//
// The proxy under test is the real handler; only VictoriaLogs is simulated, by
// the same vlEmulator the unwrap sum differential uses.

// vaStage is one vector aggregation of the query, innermost first.
type vaStage struct {
	op      string
	by      []string
	without bool
}

func (g vaStage) String() string {
	switch {
	case g.without:
		return g.op + " without (" + strings.Join(g.by, ", ") + ")"
	case g.by != nil:
		return g.op + " by (" + strings.Join(g.by, ", ") + ")"
	}
	return g.op
}

type vaScenario struct {
	stages     []vaStage
	fn, parser string // parser: "" or "logfmt"
	window     time.Duration
	step       time.Duration // 0: instant
	start, end int64         // ns, step-aligned evaluation times
	rows       []emuRow
	version    string
	// withLevel compares detected_level too: the stats routes of VictoriaLogs
	// answer no detected_level label for streams without a level, the evaluator
	// of the proxy answers "unknown" (registered apart), so the other cases
	// leave it out of both sides.
	withLevel bool
}

var vaStreams = []string{
	`{app="uw",pod="a",zone="x"}`,
	`{app="uw",pod="b",zone="x"}`,
	`{app="uw",pod="c",zone="y"}`,
	`{app="uw",pod="d",zone="y"}`,
}

func (s vaScenario) times() []int64 {
	if s.step == 0 {
		return []int64{s.end}
	}
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

func (s vaScenario) query() string {
	selector := `{app="uw"}`
	if s.parser != "" {
		selector += " | " + s.parser
	}
	q := fmt.Sprintf(`%s(%s [%s])`, s.fn, selector, uwWindowText(s.window))
	for _, stage := range s.stages {
		q = stage.String() + " (" + q + ")"
	}
	return q
}

// dropEdgeRows removes the lines exactly on an edge of a window (t-window, t].
func (s *vaScenario) dropEdgeRows() {
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

func (s vaScenario) String() string {
	return fmt.Sprintf("%s step=%v window=%v rows=%d version=%q", s.query(), s.step, s.window, len(s.rows), s.version)
}

func generateVectorAggScenario(rnd *rand.Rand, fn string, stages []vaStage, instant bool) vaScenario {
	steps := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second, 5 * time.Minute, 150 * time.Second}
	s := vaScenario{stages: stages, fn: fn, window: windows[rnd.Intn(len(windows))]}
	base := int64(1_800_000_000) * 1e9
	if !instant {
		s.step = steps[rnd.Intn(len(steps))]
		if rnd.Intn(3) == 0 {
			s.window = s.step // a tumbling window
		}
	}
	s.start = base + int64(rnd.Intn(600))*1e9
	if instant {
		s.end = s.start
	} else {
		s.end = s.start + int64(rnd.Intn(8))*int64(s.step)
	}
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	n := 8 + rnd.Intn(80)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(4) == 0 { // on a window edge
			edge := s.start + int64(rnd.Intn(8))*int64(max(s.step, time.Second))
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			ts = edge + int64(rnd.Intn(3)-1)
		}
		// Streams of uneven weight, so the values of a step differ per series.
		stream := vaStreams[rnd.Intn(len(vaStreams))]
		if rnd.Intn(2) == 0 {
			stream = vaStreams[0]
		}
		s.rows = append(s.rows, emuRow{ts: ts, stream: stream, msg: "x=1"})
	}
	return s
}

// vaSeries is a series of Loki's input vector.
type vaSeries struct {
	labels map[string]string
	value  float64
}

func labelsKey(labels map[string]string) string {
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

// lokiFold is VectorAggEvaluator.Next: the series of the vector fall into the
// group whose labels they share, and each group takes the operator's value.
func lokiFold(in []vaSeries, stage vaStage) []vaSeries {
	type group struct {
		labels map[string]string
		values []float64
	}
	groups := map[string]*group{}
	var order []string
	for _, series := range in {
		labels := map[string]string{}
		for name, value := range series.labels {
			if slices.Contains(stage.by, name) != stage.without {
				labels[name] = value
			}
		}
		key := labelsKey(labels)
		g := groups[key]
		if g == nil {
			g = &group{labels: labels}
			groups[key] = g
			order = append(order, key)
		}
		g.values = append(g.values, series.value)
	}
	var out []vaSeries
	for _, key := range order {
		g := groups[key]
		var v float64
		switch stage.op {
		case "sum", "avg":
			for _, x := range g.values {
				v += x
			}
			if stage.op == "avg" {
				v /= float64(len(g.values))
			}
		case "count":
			v = float64(len(g.values))
		case "max":
			v = slices.Max(g.values)
		case "min":
			v = slices.Min(g.values)
		default:
			panic("reference: " + stage.op)
		}
		out = append(out, vaSeries{g.labels, v})
	}
	return out
}

// lokiVectorAgg is the reference: per evaluation time, the value of the range
// function of every stream that has a line in (t-window, t] (count_over_time
// and rate), folded by the stages. It returns labels key -> time -> value.
func lokiVectorAgg(s vaScenario) (map[string]map[int64]float64, map[string]map[string]string) {
	out := map[string]map[int64]float64{}
	labelsOf := map[string]map[string]string{}
	for _, t := range s.times() {
		perStream := map[string]float64{}
		for _, row := range s.rows {
			if row.ts > t-int64(s.window) && row.ts <= t {
				perStream[row.stream]++
			}
		}
		var vec []vaSeries
		for stream, count := range perStream {
			value := count
			if s.fn == "rate" {
				value = count / s.window.Seconds()
			}
			labels := map[string]string{"service_name": "uw"}
			if s.withLevel {
				labels["detected_level"] = "unknown"
			}
			for _, name := range []string{"app", "pod", "zone"} {
				labels[name] = streamLabel(stream, name)
			}
			vec = append(vec, vaSeries{labels, value})
		}
		for _, stage := range s.stages {
			vec = lokiFold(vec, stage)
		}
		for _, series := range vec {
			key := labelsKey(series.labels)
			if out[key] == nil {
				out[key] = map[int64]float64{}
				labelsOf[key] = series.labels
			}
			out[key][t] = series.value
		}
	}
	return out, labelsOf
}

func streamLabel(stream, label string) string {
	for _, part := range strings.Split(strings.Trim(stream, "{}"), ",") {
		if k, v, ok := strings.Cut(part, "="); ok && k == label {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

func newVectorAggProxy(t *testing.T, emu *vlEmulator) (*Proxy, func()) {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return p, backend.Close
}

// runVectorAggScenario sends the scenario through the real handler against the
// emulated VictoriaLogs and compares the series, their labels and their values
// with the reference.
func runVectorAggScenario(t *testing.T, label string, s vaScenario) {
	t.Helper()
	emu := &vlEmulator{t: t, rows: s.rows, version: s.version, field: "amount"}
	p, closeBackend := newVectorAggProxy(t, emu)
	defer closeBackend()

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

	want, wantLabels := lokiVectorAgg(s)
	got := map[string]map[int64]float64{}
	for _, series := range resp.Data.Result {
		if !s.withLevel {
			delete(series.Metric, "detected_level")
		}
		key := labelsKey(series.Metric)
		if _, dup := got[key]; dup {
			t.Fatalf("%s %s: two series with the labels %v (%d series in all), Loki has %d", label, s, series.Metric, len(resp.Data.Result), len(want))
		}
		if _, ok := want[key]; !ok {
			t.Fatalf("%s %s: series with the labels %v, Loki has %d series, e.g. %v", label, s, series.Metric, len(want), firstOf(wantLabels))
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
}

func firstOf(m map[string]map[string]string) map[string]string {
	for _, v := range m {
		return v
	}
	return nil
}

// TestVectorAggregationOverRangeFunctionDifferential: count, min, max and avg of
// count_over_time and rate answer the series Loki's vector aggregation makes of
// the per-stream values, at every step and at an instant: ungrouped, by labels
// the streams have (pod, zone), by a label they have none of (baz), `by ()`,
// and without; and nested (sum, count, max over a grouped
// max, count or sum). Window shapes: tumbling, sliding, shorter and longer than
// the step, a range that is not a multiple of the step. VictoriaLogs lines:
// v1.52 with the stats offset argument, an unknown version, and v1.40.
//
// On v1.40 (no stats offset argument) a per-series sliding window misplaces a
// line exactly on its edge, with any operator (`sum by (pod)` too, and a rate
// there reads the wrong edge sample too), a divergence registered apart: v1.40
// runs count_over_time without a line on a window edge.
//
// conformance: semantics/non-sum-vector-aggregation-unreduced, metric-series-identity, window-bounds-and-step-alignment
func TestVectorAggregationOverRangeFunctionDifferential(t *testing.T) {
	var cases [][]vaStage
	for _, op := range []string{"count", "min", "max", "avg"} {
		for _, group := range []vaStage{
			{op: op},
			{op: op, by: []string{"pod"}},
			{op: op, by: []string{"zone"}},
			{op: op, by: []string{"pod", "zone"}},
			{op: op, by: []string{"baz"}},
			{op: op, by: []string{}},
			{op: op, by: []string{"pod"}, without: true},
		} {
			cases = append(cases, []vaStage{group})
		}
	}
	cases = append(cases,
		[]vaStage{{op: "max", by: []string{"pod"}}, {op: "sum"}},
		[]vaStage{{op: "max", by: []string{"zone"}}, {op: "sum"}},
		[]vaStage{{op: "count", by: []string{"zone"}}, {op: "max"}},
		[]vaStage{{op: "sum", by: []string{"pod"}}, {op: "max"}},
		[]vaStage{{op: "max", by: []string{"pod"}}, {op: "count"}},
	)
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, stages := range cases {
			for _, fn := range []string{"count_over_time", "rate"} {
				if version == "v1.40.0" && fn == "rate" {
					continue
				}
				for _, parser := range []string{"", "logfmt"} {
					for _, instant := range []bool{false, true} {
						name := fmt.Sprintf("version=%s/%s/instant=%v", version, vaScenario{stages: stages, fn: fn, parser: parser, window: time.Minute}.query(), instant)
						t.Run(name, func(t *testing.T) {
							rnd := rand.New(rand.NewSource(int64(len(name))*7919 + int64(name[len(name)-3]) + 31))
							for i := 0; i < 8; i++ {
								s := generateVectorAggScenario(rnd, fn, stages, instant)
								s.version, s.parser = version, parser
								if version == "v1.40.0" {
									s.dropEdgeRows()
								}
								runVectorAggScenario(t, fmt.Sprintf("#%d", i), s)
							}
						})
					}
				}
			}
		}
	}
}

// The stats routes of VictoriaLogs answer a grouped outer aggregation of a nested
// one with neither its labels (registered apart), and a detected_level they
// have none of; a sliding window is the proxy's evaluator, which keeps both: the
// fold reads the Loki-facing names (detected_level is level in VictoriaLogs),
// "unknown" for streams without a level, and groups nested aggregations by
// their own labels.
//
// conformance: semantics/non-sum-vector-aggregation-unreduced, metric-series-identity
func TestVectorAggregationGroupedLabelsSlidingWindow(t *testing.T) {
	var cases [][]vaStage
	for _, op := range []string{"count", "min", "max", "avg"} {
		cases = append(cases, []vaStage{{op: op, by: []string{"detected_level"}}})
	}
	cases = append(cases,
		[]vaStage{{op: "avg", by: []string{"pod", "zone"}}, {op: "min", by: []string{"zone"}}},
		[]vaStage{{op: "max", by: []string{"pod", "zone"}}, {op: "count", by: []string{"zone"}}},
	)
	for _, stages := range cases {
		for _, parser := range []string{"", "logfmt"} {
			t.Run(vaScenario{stages: stages, fn: "rate", parser: parser, window: time.Minute}.query(), func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(stages)) * 4241))
				for i := 0; i < 8; i++ {
					s := generateVectorAggScenario(rnd, "rate", stages, false)
					s.window, s.parser, s.withLevel = 5*time.Minute, parser, true
					runVectorAggScenario(t, fmt.Sprintf("#%d", i), s)
				}
			})
		}
	}
}
