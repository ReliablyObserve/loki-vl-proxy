package proxy

import (
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// This file pins the vector-to-vector join of the binary evaluator to Loki over
// generated operands and generated modifiers (on, ignoring, group_left,
// group_right with include labels, bool): every answer, result or error text,
// is the one Loki's engine gives for the same operands, recorded with
// test/oracle/loki engine (LVP_RUN_ORACLES=1 records them again).

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

// genOperand is a random operand: its samples by step and the proxy's matrix body.
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

// binaryMatchCase is one generated join.
type binaryMatchCase struct {
	op                  string
	returnBool          bool
	m                   refMatching
	vm                  *translator.VectorMatchInfo
	left, right         map[float64][]refSample
	leftBody, rightBody []byte
}

// query is the join as LogQL, each operand read from the streams of its side.
func (c binaryMatchCase) query() string {
	operand := func(side string) string {
		return `sum without (side) (last_over_time({side="` + side + `"} | logfmt | unwrap v [1m]))`
	}
	var b strings.Builder
	b.WriteString(operand("l") + " " + c.op)
	if c.returnBool {
		b.WriteString(" bool")
	}
	if c.vm != nil {
		if c.m.on {
			b.WriteString(" on(" + strings.Join(c.m.labels, ", ") + ")")
		} else {
			b.WriteString(" ignoring(" + strings.Join(c.m.labels, ", ") + ")")
		}
		if c.m.card != "" {
			b.WriteString(" " + c.m.card + "(" + strings.Join(c.m.include, ", ") + ")")
		}
	}
	b.WriteString(" " + operand("r"))
	return b.String()
}

// dataSet is the join as Loki data: a stream per operand series (its labels and side), whose line 30s before each
// step holds the sample's value.
func (c binaryMatchCase) dataSet(id string, steps []float64) oraclerun.EngineDataSet {
	set := oraclerun.EngineDataSet{}
	for side, samples := range map[string]map[float64][]refSample{"l": c.left, "r": c.right} {
		streams := map[string]int{}
		for _, ts := range steps {
			for _, s := range samples[ts] {
				labels := map[string]string{"side": side}
				for k, v := range s.labels {
					labels[k] = v
				}
				key := canonicalLabelText(labels)
				i, ok := streams[key]
				if !ok {
					i = len(set.Streams)
					streams[key] = i
					set.Streams = append(set.Streams, oraclerun.EngineStream{Labels: labels})
				}
				set.Streams[i].Entries = append(set.Streams[i].Entries, oraclerun.EngineEntry{TS: int64(ts-30) * 1e9, Line: "v=" + strconv.FormatFloat(s.value, 'f', -1, 64)})
			}
		}
	}
	sort.Slice(set.Streams, func(i, j int) bool {
		return canonicalLabelText(set.Streams[i].Labels) < canonicalLabelText(set.Streams[j].Labels)
	})
	set.Requests = []oraclerun.EngineRequest{{ID: id, Unaligned: true, Form: url.Values{
		"query": {c.query()}, "start": {strconv.FormatFloat(steps[0], 'f', -1, 64)}, "end": {strconv.FormatFloat(steps[len(steps)-1], 'f', -1, 64)},
		"step": {strconv.FormatFloat(steps[1]-steps[0], 'f', -1, 64)},
	}.Encode()}}
	return set
}

// conformance: semantics/binary-ignoring-matching, loki_api_v1_query_range
func TestBinaryMatchingDifferential(t *testing.T) {
	ops := []string{"+", "-", "*", "/", "==", "!=", ">", "<", ">=", "<="}
	labelSets := [][]string{{}, {"pod"}, {"machine"}, {"pod", "machine"}, {"app"}, {"app", "pod"}}
	includes := [][]string{nil, {"pod"}, {"machine"}, {"pod", "machine"}}
	rng := rand.New(rand.NewSource(20261012))
	steps := []float64{100, 160, 220}
	var cases []binaryMatchCase
	var sets []oraclerun.EngineDataSet
	for i := 0; i < 6000; i++ {
		var c binaryMatchCase
		c.left, c.leftBody = genOperand(rng, steps)
		c.right, c.rightBody = genOperand(rng, steps)
		c.m = refMatching{on: rng.Intn(2) == 0, labels: labelSets[rng.Intn(len(labelSets))]}
		switch rng.Intn(4) {
		case 0:
			c.m.card, c.m.include = "group_left", includes[rng.Intn(len(includes))]
		case 1:
			c.m.card, c.m.include = "group_right", includes[rng.Intn(len(includes))]
		}
		c.op, c.returnBool = ops[rng.Intn(len(ops))], rng.Intn(2) == 0
		if c.op == "+" || c.op == "-" || c.op == "*" || c.op == "/" {
			c.returnBool = false
		}
		if i%5 == 4 {
			// A join with no modifier: the evaluator receives no matching info.
			c.m = refMatching{}
		} else {
			c.vm = &translator.VectorMatchInfo{}
			if c.m.on {
				c.vm.MatchOn, c.vm.On = true, c.m.labels
			} else {
				c.vm.Ignoring = c.m.labels
			}
			if c.m.card != "" {
				c.vm.GroupSide = c.m.card
				if c.m.card == "group_left" {
					c.vm.GroupLeft = c.m.include
				} else {
					c.vm.GroupRight = c.m.include
				}
			}
		}
		cases = append(cases, c)
		sets = append(sets, c.dataSet(strconv.Itoa(i), steps))
	}
	answers := lokiEngineAnswers(t, "binary_matching", sets)
	compared, errored := 0, 0
	byError := map[string]int{}
	for i, c := range cases {
		desc := fmt.Sprintf("%d: %s\nleft=%s\nright=%s", i, c.query(), c.leftBody, c.rightBody)
		loki := answers[strconv.Itoa(i)]
		body, err := matchBinaryMetricResults(c.leftBody, c.rightBody, c.op, "matrix", c.vm, c.returnBool)
		switch {
		case loki.Status != 200:
			errored++
			byError[strings.SplitN(loki.Error, ";", 2)[0]]++
			if err == nil || err.Error() != loki.Error {
				t.Fatalf("error = %v, Loki %d %q\n%s", err, loki.Status, loki.Error, desc)
			}
		case err != nil:
			t.Fatalf("unexpected error %q\n%s", err, desc)
		default:
			compared++
			got, want := decodeMatchResult(t, body), decodeMatchResult(t, loki.Body)
			if len(want) == 0 && len(got) == 0 {
				continue
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %s\nLoki %s\n%s", body, loki.Body, desc)
			}
		}
	}
	t.Logf("compared %d results, %d errors %v", compared, errored, byError)
	if compared < 500 || errored < 500 {
		t.Fatalf("generator no longer covers both outcomes: %d results, %d errors", compared, errored)
	}
}
