package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This file pins a vector aggregation over a binary expression to Loki's
// answer, over generated rows, windows and expressions, with a reference
// evaluator written apart from the proxy: Loki v3.7.7 pkg/logql/evaluator.go
// newBinOpStepEvaluator and vectorBinop (per step, the right-hand samples by
// matching signature, one match per left sample, resultMetric for the labels:
// on() keeps the listed labels and ignoring() drops them for one-to-one,
// group_left keeps the left labels; a comparison keeps the left value or, with
// bool, answers 0 or 1), newLiteralStepEvaluator (a literal on either side) and
// VectorAggEvaluator.Next (the aggregation per step over the binary's samples).
// The operands are `sum by (...)` over count_over_time and rate, read on the
// window (t-range, t]; only VictoriaLogs is simulated (vlEmulator).

type refSample struct {
	labels map[string]string
	v      float64
}

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

func refStreamLabels(stream string) map[string]string {
	out := map[string]string{}
	for _, m := range emuStreamLabelRE.FindAllStringSubmatch(stream, -1) {
		out[m[1]] = m[2]
	}
	return out
}

func refValue(op string, a, b float64) (float64, bool) {
	switch op {
	case "+":
		return a + b, true
	case "-":
		return a - b, true
	case "*":
		return a * b, true
	case "/":
		return a / b, true // the generator never divides by zero: Loki answers NaN there, the proxy ±Inf
	case ">":
		return a, a > b
	case "<":
		return a, a < b
	case ">=":
		return a, a >= b
	case "<=":
		return a, a <= b
	}
	panic("reference: " + op)
}

func refComparison(op string) bool {
	return op == ">" || op == "<" || op == ">=" || op == "<="
}

// eval returns the vector at t, or the literal.
func (n *refNode) eval(rows []emuRow, t, window int64) ([]refSample, float64, bool) {
	if n.scalar != nil {
		return nil, *n.scalar, true
	}
	if n.fn != "" {
		type group struct {
			labels     map[string]string
			count, sum float64
		}
		groups := map[string]*group{}
		for _, row := range rows {
			if row.ts <= t-window || row.ts > t {
				continue
			}
			stream := refStreamLabels(row.stream)
			labels := map[string]string{}
			for _, name := range n.by {
				if v := stream[name]; v != "" {
					labels[name] = v
				}
			}
			key := binaryLabelKey(labels)
			if groups[key] == nil {
				groups[key] = &group{labels: labels}
			}
			groups[key].count++
			groups[key].sum += row.value
		}
		var out []refSample
		seconds := float64(window) / 1e9
		for _, g := range groups {
			v := map[string]float64{"count": g.count, "rate": g.count / seconds, "amount": g.sum / seconds}[n.fn]
			out = append(out, refSample{g.labels, v})
		}
		return out, 0, false
	}
	lhs, lv, lScalar := n.l.eval(rows, t, window)
	rhs, rv, rScalar := n.r.eval(rows, t, window)
	var out []refSample
	if lScalar || rScalar {
		vec := lhs
		if lScalar {
			vec = rhs
		}
		for _, s := range vec {
			a, b := s.v, rv
			if lScalar {
				a, b = lv, s.v
			}
			v, keep := refValue(n.op, a, b)
			if refComparison(n.op) {
				v = s.v // the vector's value, whichever side the literal is on
				if n.boolean {
					v, keep = map[bool]float64{true: 1, false: 0}[keep], true
				}
			}
			if keep {
				out = append(out, refSample{s.labels, v})
			}
		}
		return out, 0, false
	}
	signature := func(labels map[string]string) string {
		sig := map[string]string{}
		for k, v := range labels {
			sig[k] = v
		}
		switch n.match {
		case "on":
			sig = map[string]string{}
			for _, name := range n.labels {
				if v := labels[name]; v != "" {
					sig[name] = v
				}
			}
		case "ignoring":
			for _, name := range n.labels {
				delete(sig, name)
			}
		}
		return binaryLabelKey(sig)
	}
	right := map[string]refSample{}
	for _, s := range rhs {
		right[signature(s.labels)] = s
	}
	for _, s := range lhs {
		match, ok := right[signature(s.labels)]
		if !ok {
			continue
		}
		labels := s.labels
		if !n.groupLeft && n.match != "" { // one-to-one: resultMetric keeps on(), drops ignoring()
			labels = map[string]string{}
			if n.match == "on" {
				for _, name := range n.labels {
					if v := s.labels[name]; v != "" {
						labels[name] = v
					}
				}
			} else {
				for k, v := range s.labels {
					labels[k] = v
				}
				for _, name := range n.labels {
					delete(labels, name)
				}
			}
		}
		v, keep := refValue(n.op, s.v, match.v)
		if n.boolean {
			v, keep = map[bool]float64{true: 1, false: 0}[keep], true
		}
		if keep {
			out = append(out, refSample{labels, v})
		}
	}
	return out, 0, false
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

func (a refAggregation) apply(in []refSample) []refSample {
	type group struct {
		labels map[string]string
		v      float64
		n      int
	}
	groups := map[string]*group{}
	var order []string
	for _, s := range in {
		labels := map[string]string{}
		if a.without {
			for k, v := range s.labels {
				labels[k] = v
			}
			for _, name := range a.labels {
				delete(labels, name)
			}
		} else {
			for _, name := range a.labels {
				if v := s.labels[name]; v != "" {
					labels[name] = v
				}
			}
		}
		key := binaryLabelKey(labels)
		g := groups[key]
		if g == nil {
			groups[key] = &group{labels: labels, v: s.v, n: 1}
			order = append(order, key)
			continue
		}
		g.n++
		switch a.op {
		case "sum":
			g.v += s.v
		case "avg":
			g.v += (s.v - g.v) / float64(g.n)
		case "min":
			g.v = math.Min(g.v, s.v)
		case "max":
			g.v = math.Max(g.v, s.v)
		}
	}
	out := make([]refSample, 0, len(order))
	for _, key := range order {
		g := groups[key]
		if a.op == "count" {
			g.v = float64(g.n)
		}
		out = append(out, refSample{g.labels, g.v})
	}
	return out
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
// another order), on VictoriaLogs v1.52 and v1.40.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query_range, loki_api_v1_query
func TestAggregationOverBinaryDifferential(t *testing.T) {
	jobs := map[string]string{"a": "x", "b": "x", "c": "y"}
	for _, version := range []string{"v1.52.0", "v1.40.0"} {
		for _, instant := range []bool{false, true} {
			name := fmt.Sprintf("version=%s/instant=%v", version, instant)
			t.Run(name, func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(name))*6151 + 3))
				offGrid := 0
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
						pod := s.rows[j].stream[len(`{app="uw",pod="`) : len(`{app="uw",pod="`)+1]
						s.rows[j].stream = fmt.Sprintf(`{app="uw",job="%s",machine="m-%[1]s",pod="%s"}`, jobs[pod], pod)
						if version == "v1.40.0" && s.step > 0 && s.rows[j].ts%1e9 == 0 {
							// Before v1.45 VictoriaLogs has no stats_query_range offset: its
							// epoch buckets [T, T+b) put a line exactly on a bucket edge T in
							// the next window, not Loki's (T-range, T], on any operand alone
							// (versions/stats-bucket-label-v1.45).
							s.rows[j].ts++
						}
					}
					query := expr.query(uwWindowText(s.window))
					for _, agg := range chain {
						query = agg.wrap(query)
					}
					want := map[string]map[int64]float64{}
					for _, ts := range s.times() {
						samples, _, _ := expr.eval(s.rows, ts, int64(s.window))
						for _, agg := range chain {
							samples = agg.apply(samples)
						}
						for _, sample := range samples {
							key := binaryLabelKey(sample.labels)
							if want[key] == nil {
								want[key] = map[int64]float64{}
							}
							want[key][ts/1e9] = sample.v
						}
					}
					emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
					got, _ := binaryAggregationAnswer(t, emu, query, s.start, s.end, s.step)
					label := fmt.Sprintf("%s #%d %s step=%v window=%v rows=%d", name, i, query, s.step, s.window, len(s.rows))
					compareBinaryAggregation(t, label, got, want)
					if s.step > 0 && s.start%int64(s.step) != 0 {
						offGrid++
					}
				}
				if !instant && offGrid == 0 {
					t.Fatal("no scenario started off the step grid")
				}
			})
		}
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
