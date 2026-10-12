package proxy

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// This file pins the vector-to-vector join of the binary evaluator to Loki
// v3.7.7 over generated operands and generated modifiers. The reference below
// is written apart from the proxy, step by step as Loki evaluates it:
// pkg/logql/engine.go reads the evaluator's error after every step and stops
// at the first one; pkg/logql/evaluator.go BinOpStepEvaluator.Next hashes each
// sample with matchingSignature (:945, on keeps the listed labels, ignoring
// removes them, an empty label value is no label) and calls vectorBinop (:955):
// the right vector (the left one after a group_right swap) must be unique by
// signature or the join fails with "found duplicate series on the <side>
// hand-side", a left sample that meets an already matched signature without a
// group modifier fails with "multiple matches for labels: many-to-one ...",
// and resultMetric (:1096) builds the labels of a sample; syntax.MergeBinOp
// (pkg/logql/syntax/ast.go:1944) gives the value (a filtering comparison
// keeps the left value, a bool comparison is 1 or 0).

type refSample struct {
	labels map[string]string
	value  float64
}

type refMatching struct {
	on      bool
	labels  []string
	card    string // "", "group_left", "group_right"
	include []string
}

func refSignature(labels map[string]string, m refMatching) string {
	keep := map[string]string{}
	for k, v := range labels {
		if v == "" {
			continue
		}
		keep[k] = v
	}
	if m.on {
		only := map[string]string{}
		for _, l := range m.labels {
			if v, ok := keep[l]; ok {
				only[l] = v
			}
		}
		keep = only
	} else {
		for _, l := range m.labels {
			delete(keep, l)
		}
	}
	return refLabelsKey(keep)
}

func refLabelsKey(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		if v != "" {
			parts = append(parts, k+"\xff"+v)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "\xfe")
}

func refResultLabels(many, one map[string]string, m refMatching) map[string]string {
	out := map[string]string{}
	for k, v := range many {
		if v != "" {
			out[k] = v
		}
	}
	if m.card == "" {
		if m.on {
			for k := range out {
				found := false
				for _, l := range m.labels {
					found = found || l == k
				}
				if !found {
					delete(out, k)
				}
			}
		} else {
			for _, l := range m.labels {
				delete(out, l)
			}
		}
	}
	for _, l := range m.include {
		if v := one[l]; v != "" {
			out[l] = v
		} else {
			delete(out, l)
		}
	}
	return out
}

func refMerge(op string, l, r float64, returnBool bool) (float64, bool) {
	switch op {
	case "+":
		return l + r, true
	case "-":
		return l - r, true
	case "*":
		return l * r, true
	case "/":
		return l / r, true // the generated divisors are never zero
	}
	var truth bool
	switch op {
	case "==":
		truth = l == r
	case "!=":
		truth = l != r
	case ">":
		truth = l > r
	case "<":
		truth = l < r
	case ">=":
		truth = l >= r
	case "<=":
		truth = l <= r
	}
	if returnBool {
		if truth {
			return 1, true
		}
		return 0, true
	}
	return l, truth
}

// refStep is one evaluation of Loki's vectorBinop.
func refStep(op string, m refMatching, returnBool bool, lhs, rhs []refSample) ([]refSample, error) {
	oneToMany := m.card == "group_right"
	if oneToMany {
		lhs, rhs = rhs, lhs
	}
	right := map[string]refSample{}
	for _, s := range rhs {
		sig := refSignature(s.labels, m)
		if _, dup := right[sig]; dup {
			side := "right"
			if oneToMany {
				side = "left"
			}
			return nil, fmt.Errorf("found duplicate series on the %s hand-side;many-to-many matching not allowed: matching labels must be unique on one side", side)
		}
		right[sig] = s
	}
	matched := map[string]map[string]bool{}
	var out []refSample
	for _, s := range lhs {
		sig := refSignature(s.labels, m)
		r, found := right[sig]
		if !found {
			continue
		}
		metric := refResultLabels(s.labels, r.labels, m)
		inserted, exists := matched[sig]
		if m.card == "" {
			if exists {
				return nil, fmt.Errorf("multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)")
			}
			matched[sig] = nil
		} else {
			key := refLabelsKey(metric)
			if !exists {
				inserted = map[string]bool{}
				matched[sig] = inserted
			} else if inserted[key] {
				return nil, fmt.Errorf("multiple matches for labels: grouping labels must ensure unique matches")
			}
			inserted[key] = true
		}
		l, rr := s, r
		if oneToMany {
			l, rr = r, s
		}
		if v, ok := refMerge(op, l.value, rr.value, returnBool); ok {
			out = append(out, refSample{metric, v})
		}
	}
	return out, nil
}

// refMatrix evaluates every step in order, stops at the first failing one.
func refMatrix(op string, m refMatching, returnBool bool, left, right map[float64][]refSample) (map[string][][]any, error) {
	steps := map[float64]bool{}
	for ts := range left {
		steps[ts] = true
	}
	for ts := range right {
		steps[ts] = true
	}
	axis := make([]float64, 0, len(steps))
	for ts := range steps {
		axis = append(axis, ts)
	}
	sort.Float64s(axis)
	out := map[string][][]any{}
	for _, ts := range axis {
		samples, err := refStep(op, m, returnBool, left[ts], right[ts])
		if err != nil {
			return nil, err
		}
		for _, s := range samples {
			key := binaryLabelKey(s.labels)
			out[key] = append(out[key], []any{ts, strconv.FormatFloat(s.value, 'f', -1, 64)})
		}
	}
	return out, nil
}

// refCollides reports two samples of one step that carry the same labels: Loki
// then holds two points of one series at one time, which the generator avoids.
func refCollides(samples []refSample) bool {
	seen := map[string]bool{}
	for _, s := range samples {
		k := refLabelsKey(s.labels)
		if seen[k] {
			return true
		}
		seen[k] = true
	}
	return false
}

func genOperand(rng *rand.Rand, steps []float64) (map[float64][]refSample, []byte) {
	apps, pods, machines := []string{"a", "b"}, []string{"", "p1", "p2"}, []string{"", "m1", "m2"}
	type key struct{ app, pod, machine string }
	all := []key{}
	for _, a := range apps {
		for _, p := range pods {
			for _, m := range machines {
				all = append(all, key{a, p, m})
			}
		}
	}
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	n := rng.Intn(5)
	byTime := map[float64][]refSample{}
	var series []matchSeries
	for _, k := range all[:n] {
		labels := map[string]string{"app": k.app}
		if k.pod != "" {
			labels["pod"] = k.pod
		}
		if k.machine != "" {
			labels["machine"] = k.machine
		}
		var points [][]any
		for _, ts := range steps {
			if rng.Intn(3) == 0 {
				continue
			}
			v := float64(rng.Intn(9) + 1)
			points = append(points, []any{ts, strconv.FormatFloat(v, 'f', -1, 64)})
			byTime[ts] = append(byTime[ts], refSample{labels, v})
		}
		if len(points) > 0 {
			series = append(series, matchSeries{labels, points})
		}
	}
	return byTime, matchBody(series...)
}

// conformance: semantics/binary-ignoring-matching, loki_api_v1_query_range
func TestBinaryMatchingDifferential(t *testing.T) {
	ops := []string{"+", "-", "*", "/", "==", "!=", ">", "<", ">=", "<="}
	labelSets := [][]string{{}, {"pod"}, {"machine"}, {"pod", "machine"}, {"app"}, {"app", "pod"}}
	includes := [][]string{nil, {"pod"}, {"machine"}, {"pod", "machine"}}
	rng := rand.New(rand.NewSource(20261012))
	steps := []float64{100, 160, 220}
	compared, errored, skipped := 0, 0, 0
	byError := map[string]int{}
	for i := 0; i < 6000; i++ {
		leftRef, leftBody := genOperand(rng, steps)
		rightRef, rightBody := genOperand(rng, steps)
		m := refMatching{on: rng.Intn(2) == 0, labels: labelSets[rng.Intn(len(labelSets))]}
		switch rng.Intn(4) {
		case 0:
			m.card, m.include = "group_left", includes[rng.Intn(len(includes))]
		case 1:
			m.card, m.include = "group_right", includes[rng.Intn(len(includes))]
		}
		op, returnBool := ops[rng.Intn(len(ops))], rng.Intn(2) == 0
		if op == "+" || op == "-" || op == "*" || op == "/" {
			returnBool = false
		}
		var vm *translator.VectorMatchInfo
		if i%5 == 4 {
			// A join with no modifier: the evaluator receives no matching info.
			m = refMatching{}
		} else {
			vm = &translator.VectorMatchInfo{}
			if m.on {
				vm.MatchOn, vm.On = true, m.labels
			} else {
				vm.Ignoring = m.labels
			}
			if m.card != "" {
				vm.GroupSide = m.card
				if m.card == "group_left" {
					vm.GroupLeft = m.include
				} else {
					vm.GroupRight = m.include
				}
			}
		}
		desc := fmt.Sprintf("%d: %s on=%v labels=%v card=%s include=%v bool=%v\nleft=%s\nright=%s", i, op, m.on, m.labels, m.card, m.include, returnBool, leftBody, rightBody)

		want, wantErr := refMatrix(op, m, returnBool, leftRef, rightRef)
		if wantErr == nil {
			collided := false
			for _, ts := range steps {
				if samples, _ := refStep(op, m, returnBool, leftRef[ts], rightRef[ts]); refCollides(samples) {
					collided = true
				}
			}
			if collided {
				skipped++
				continue
			}
		}
		body, err := matchBinaryMetricResults(leftBody, rightBody, op, "matrix", vm, returnBool)
		switch {
		case wantErr != nil:
			errored++
			byError[strings.SplitN(wantErr.Error(), ";", 2)[0]]++
			if err == nil || err.Error() != wantErr.Error() {
				t.Fatalf("error = %v, want %q\n%s", err, wantErr, desc)
			}
		case err != nil:
			t.Fatalf("unexpected error %q\n%s", err, desc)
		default:
			compared++
			got := decodeMatchResult(t, body)
			if len(want) == 0 && len(got) == 0 {
				continue
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %s\nwant %v\n%s", body, want, desc)
			}
		}
	}
	t.Logf("compared %d results, %d errors %v, skipped %d collisions", compared, errored, byError, skipped)
	if compared < 500 || errored < 500 {
		t.Fatalf("generator no longer covers both outcomes: %d results, %d errors", compared, errored)
	}
}
