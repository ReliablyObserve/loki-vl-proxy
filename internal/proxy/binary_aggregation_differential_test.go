package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// This file pins a vector aggregation over a binary expression to Loki's
// answer, over generated rows, windows and expressions: Loki's answers are
// recorded with Loki's engine over the same lines (test/oracle/loki engine;
// LVP_RUN_ORACLES=1 records them again). The operands are `sum by (...)` over
// count_over_time and rate; only VictoriaLogs is simulated (vlemu.Emulator).

// refNode is an operand (fn set), a literal (scalar set) or a binary.
type refNode struct {
	fn     string // "count", "rate" or "amount" (rate of the unwrapped amount)
	by     []string
	scalar *float64

	op        string
	boolean   bool
	match     string // "", "on" or "ignoring"
	labels    []string
	groupLeft bool
	l, r      *refNode
}

func (n *refNode) query(window string) string {
	switch {
	case n.fn != "":
		inner := map[string]string{
			"count":  `count_over_time({app="uw"}[` + window + `])`,
			"rate":   `rate({app="uw"}[` + window + `])`,
			"amount": `rate({app="uw"} | unwrap amount [` + window + `])`,
		}[n.fn]
		return "sum by (" + strings.Join(n.by, ", ") + ") (" + inner + ")"
	case n.scalar != nil:
		return strconv.FormatFloat(*n.scalar, 'f', -1, 64)
	}
	side := func(c *refNode) string {
		if c.op != "" {
			return "(" + c.query(window) + ")"
		}
		return c.query(window)
	}
	q := side(n.l) + " " + n.op
	if n.boolean {
		q += " bool"
	}
	if n.match != "" {
		q += " " + n.match + " (" + strings.Join(n.labels, ", ") + ")"
	}
	if n.groupLeft {
		q += " group_left"
	}
	return q + " " + side(n.r)
}

// joinsVectors reports whether the expression joins two vectors anywhere.
func (n *refNode) joinsVectors() bool {
	if n.op == "" {
		return false
	}
	return (n.l.scalar == nil && n.r.scalar == nil) || n.l.joinsVectors() || n.r.joinsVectors()
}

type refAggregation struct {
	op      string
	without bool
	labels  []string // nil: no grouping
}

func (a refAggregation) wrap(q string) string {
	switch {
	case a.labels == nil:
		return a.op + "(" + q + ")"
	case a.without:
		return a.op + " without (" + strings.Join(a.labels, ", ") + ") (" + q + ")"
	}
	return a.op + " by (" + strings.Join(a.labels, ", ") + ") (" + q + ")"
}

// generateBinaryAggregation draws an expression: operands grouped by (job, pod)
// or by (job), joined on every label, on() or ignoring() with group_left,
// ignoring() one-to-one (`sum by (job) (A) + ignoring(machine) sum by (job,
// machine) (B)`, one machine per job), with
// a literal on either side, a comparison with or without bool, and binaries
// nested in binaries; then one or two aggregations over it. A divisor is never
// a value that can be zero.
func generateBinaryAggregation(rnd *rand.Rand) (*refNode, []refAggregation) {
	pick := func(xs ...string) string { return xs[rnd.Intn(len(xs))] }
	operand := func(by ...string) *refNode { return &refNode{fn: pick("count", "rate", "amount"), by: by} }
	divisor := func(by ...string) *refNode { return &refNode{fn: pick("count", "rate"), by: by} }
	literal := func(v float64) *refNode { return &refNode{scalar: &v} }
	full := []string{"job", "pod"}
	var expr *refNode
	switch rnd.Intn(8) {
	case 0: // metric op metric on every label
		op := pick("+", "-", "*", "/")
		right := operand(full...)
		if op == "/" {
			right = divisor(full...)
		}
		expr = &refNode{op: op, l: operand(full...), r: right}
	case 1: // metric op literal, literal op metric
		op := pick("+", "-", "*", "/")
		v := float64(1 + rnd.Intn(4))
		if rnd.Intn(2) == 0 {
			expr = &refNode{op: op, l: operand(full...), r: literal(v)}
		} else if op == "/" {
			expr = &refNode{op: op, l: literal(v), r: divisor(full...)}
		} else {
			expr = &refNode{op: op, l: literal(v), r: operand(full...)}
		}
	case 2: // comparison with a literal
		v := []float64{0, 0.025, 0.04, 1.5, 2.5}[rnd.Intn(5)]
		if rnd.Intn(2) == 0 {
			expr = &refNode{op: pick(">", "<", ">=", "<="), boolean: rnd.Intn(2) == 0, l: operand(full...), r: literal(v)}
		} else {
			expr = &refNode{op: pick(">", "<", ">=", "<="), boolean: rnd.Intn(2) == 0, l: literal(v), r: operand(full...)}
		}
	case 3: // comparison of two vectors
		expr = &refNode{op: pick(">", "<", ">=", "<="), boolean: rnd.Intn(2) == 0, l: operand(full...), r: operand(full...)}
	case 4: // many-to-one
		match, labels := "on", []string{"job"}
		if rnd.Intn(2) == 0 {
			match, labels = "ignoring", []string{"pod"}
		}
		expr = &refNode{op: pick("/", "*", "-"), match: match, labels: labels, groupLeft: true, l: operand(full...), r: divisor("job")}
	case 7: // one-to-one ignoring() a label only one side has (one machine per job)
		left := &refNode{fn: pick("count", "rate", "amount"), by: []string{"job"}}
		if rnd.Intn(2) == 0 {
			expr = &refNode{op: pick("+", "-", "*"), match: "ignoring", labels: []string{"machine"}, l: left, r: operand("job", "machine")}
		} else {
			expr = &refNode{op: "/", match: "ignoring", labels: []string{"machine"}, l: left, r: divisor("job", "machine")}
		}
	case 5: // one-to-one on() the full label set: the answer keeps only those
		expr = &refNode{op: pick("+", "/"), match: "on", labels: full, l: operand(full...), r: divisor(full...)}
	default: // nested
		inner := &refNode{op: pick("+", "-", "*"), l: operand(full...), r: operand(full...)}
		if rnd.Intn(2) == 0 {
			expr = &refNode{op: pick("/", "*"), l: inner, r: literal(float64(2 + rnd.Intn(3)))}
		} else {
			expr = &refNode{op: "/", l: inner, r: divisor(full...)}
		}
	}
	grouping := func() refAggregation {
		a := refAggregation{op: pick("sum", "avg", "min", "max", "count")}
		switch rnd.Intn(4) {
		case 0:
			a.labels = []string{"job"}
		case 1:
			a.labels = []string{"pod"}
		case 2:
			a.without, a.labels = true, []string{"pod"}
		}
		return a
	}
	chain := []refAggregation{grouping()}
	if rnd.Intn(4) == 0 {
		chain = append(chain, refAggregation{op: pick("sum", "max", "count")})
	}
	return expr, chain
}

// TestAggregationOverBinaryDifferential: over generated rows (three streams in
// two jobs, values including zero, lines on window edges), generated windows
// (sliding, tumbling, longer and shorter than the step, starting on and off the
// step grid, instant) and generated expressions, the proxy's answer to an
// aggregation over a binary is Loki's: the same series, timestamps and values (to 1e-9 relative: sums of floats in
// another order), on VictoriaLogs v1.52 and v1.49 (the supported floor).
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query_range, loki_api_v1_query
func TestAggregationOverBinaryDifferential(t *testing.T) {
	jobs := map[string]string{"a": "x", "b": "x", "c": "y"}
	type scenario struct {
		name, query string
		s           uwScenario
	}
	var scenarios []scenario
	var sets []oraclerun.EngineDataSet
	for _, version := range []string{"v1.52.0", "v1.49.0"} {
		for _, instant := range []bool{false, true} {
			name := fmt.Sprintf("version=%s/instant=%v", version, instant)
			rnd := rand.New(rand.NewSource(int64(len(name))*6151 + 3))
			for i := 0; i < 40; i++ {
				s := generateUnwrapScenario(rnd, "rate", "", instant)
				if s.step > 0 && rnd.Intn(2) == 0 { // the other half starts off the step grid
					s.start -= s.start % int64(s.step)
					s.end -= s.end % int64(s.step)
				}
				expr, chain := generateBinaryAggregation(rnd)
				if s.step > 0 && s.start%int64(s.step) != 0 && expr.joinsVectors() {
					// Loki evaluates at start+k*step; a join of two vectors evaluates
					// on the step-aligned grid instead (semantics/binary-step-grid).
					continue
				}
				for j := range s.rows {
					pod := s.rows[j].Stream[len(`{app="uw",pod="`) : len(`{app="uw",pod="`)+1]
					s.rows[j].Stream = fmt.Sprintf(`{app="uw",job="%s",machine="m-%[1]s",pod="%s"}`, jobs[pod], pod)
				}
				query := expr.query(uwWindowText(s.window))
				for _, agg := range chain {
					query = agg.wrap(query)
				}
				id := fmt.Sprintf("%s #%d", name, i)
				scenarios = append(scenarios, scenario{name: id, query: query, s: s})
				sets = append(sets, oraclerun.EngineDataSet{
					Streams:  engineStreams(s.rows, func(r vlemu.Row) (string, map[string]string) { return unwrapRowEntry("", "amount", r) }),
					Requests: []oraclerun.EngineRequest{lokiMetricRequest(id, query, s.start, s.end, s.step)},
				})
			}
		}
	}
	answers := lokiEngineAnswers(t, "aggregation_over_binary", sets)
	offGrid := 0
	for _, sc := range scenarios {
		version := strings.TrimPrefix(strings.SplitN(sc.name, "/", 2)[0], "version=")
		s := sc.s
		points, labels := lokiMetricPoints(t, sc.name, answers[sc.name])
		want := map[string]map[int64]float64{}
		for key, series := range points {
			byKey := map[int64]float64{}
			for ts, v := range series {
				byKey[ts/1e9] = v
			}
			want[binaryLabelKey(labels[key])] = byKey
		}
		emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: version}
		got, _ := binaryAggregationAnswer(t, emu, sc.query, s.start, s.end, s.step)
		label := fmt.Sprintf("%s %s step=%v window=%v rows=%d", sc.name, sc.query, s.step, s.window, len(s.rows))
		compareBinaryAggregation(t, label, got, want)
		if s.step > 0 && s.start%int64(s.step) != 0 {
			offGrid++
		}
	}
	if offGrid == 0 {
		t.Fatal("no scenario started off the step grid")
	}
}

func compareBinaryAggregation(t *testing.T, label string, got, want map[string]map[int64]float64) {
	t.Helper()
	keys := func(m map[string]map[int64]float64) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	if strings.Join(keys(got), " ") != strings.Join(keys(want), " ") {
		t.Fatalf("%s:\nseries %v, Loki has %v\n got %v\nwant %v", label, keys(got), keys(want), got, want)
	}
	for key, points := range want {
		if len(got[key]) != len(points) {
			t.Fatalf("%s: %s has %d points, Loki %d\n got %v\nwant %v", label, key, len(got[key]), len(points), got[key], points)
		}
		for ts, w := range points {
			g, ok := got[key][ts]
			if !ok || math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("%s: %s at %d got %v (present %v), Loki has %v", label, key, ts, g, ok, w)
			}
		}
	}
}
