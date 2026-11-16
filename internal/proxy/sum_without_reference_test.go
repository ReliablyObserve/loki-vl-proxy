package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// withoutReference is `<op> without (pod) (<fn>(...))` at every time: t -> group (the stream labels without pod)
// -> value. A series is a row's stream plus the labels its parser extracts; Loki's groups also keep those labels,
// and the answer is projected onto the stream labels with the same operator (the proxy keeps the stream identity,
// semantics/unwrap-per-series-identity-raw-rows), which sum, max and min allow.
func withoutReference(rows []vlemu.Row, fn, op string, window int64, times []int64) map[int64]map[string]float64 {
	return withoutReferenceIdentity(rows, fn, op, window, times, unwrapRowSeries, func(r vlemu.Row) string { return withoutPodKey(r.Stream) })
}

// withoutReferenceIdentity is withoutReference with the series a row belongs to (identity) and the group a series
// folds into (group) given. fn is an unwrap range function, or count_rate (rate over the lines).
func withoutReferenceIdentity(rows []vlemu.Row, fn, op string, window int64, times []int64, identity, group func(vlemu.Row) string) map[int64]map[string]float64 {
	sorted := append([]vlemu.Row(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].TS < sorted[j].TS })
	out := map[int64]map[string]float64{}
	for _, t := range times {
		values := map[string][]float64{}
		groupOf := map[string]string{}
		var order []string
		for _, r := range sorted {
			if r.TS <= t-window || r.TS > t {
				continue
			}
			id := identity(r)
			if _, ok := values[id]; !ok {
				order = append(order, id)
				groupOf[id] = group(r)
			}
			values[id] = append(values[id], r.Value)
		}
		layer := aggLayer{op: op}
		var vec []topKFoldRefSample
		for _, id := range order {
			v := values[id]
			var value float64
			if fn == "count_rate" {
				value = float64(len(v)) / (float64(window) / 1e9)
			} else {
				value = unwrapWindowValue(fn, v, window)
			}
			vec = append(vec, topKFoldRefSample{map[string]string{"g": groupOf[id]}, value})
		}
		layer.hasBy, layer.by = true, []string{"g"}
		folded, _ := layer.eval(vec)
		if len(folded) == 0 {
			continue
		}
		out[t] = map[string]float64{}
		for _, s := range folded {
			out[t][s.labels["g"]] = s.value
		}
	}
	return out
}

// The without reference gives Loki's answer projected onto the stream labels, on recorded scenarios of the
// differential: every function and operator, with no parser and with logfmt (a unique parsed label per line),
// range and instant (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestWithoutReferenceMatchesRecordedLoki(t *testing.T) {
	type tc struct{ fn, op, parser string }
	var combos []tc
	for _, parser := range []string{"", "logfmt"} {
		for _, fn := range []string{"rate", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "count_rate"} {
			combos = append(combos, tc{fn, "sum", parser})
		}
		combos = append(combos, tc{"max_over_time", "max", parser}, tc{"min_over_time", "min", parser}, tc{"sum_over_time", "max", parser})
	}
	combos = append(combos, tc{"max_over_time", "count", ""}, tc{"sum_over_time", "avg", ""}, tc{"min_over_time", "avg", ""})
	rnd := rand.New(rand.NewSource(8086))
	var cases []withoutScenario
	var sets []oraclerun.EngineDataSet
	for _, c := range combos {
		for _, instant := range []bool{true, false} {
			for i := 0; i < 6; i++ {
				s := generateWithoutScenario(rnd, c.fn, c.op, c.parser, instant)
				parser, unwrap := c.parser, c.fn != "count_rate"
				id := fmt.Sprint(len(cases))
				cases = append(cases, s)
				sets = append(sets, oraclerun.EngineDataSet{
					Streams: engineStreams(s.rows, func(r vlemu.Row) (string, map[string]string) {
						if parser != "" || !unwrap {
							// A line's structured metadata is a label of its sample; only an unwrap consumes it.
							return r.Msg, nil
						}
						return "amount=" + r.Text(), map[string]string{"amount": r.Text()}
					}),
					Requests: []oraclerun.EngineRequest{lokiMetricRequest(id, s.query(), s.start, s.end, s.step)},
				})
			}
		}
	}
	answers := lokiEngineAnswers(t, "without_reference", sets)
	for i, s := range cases {
		id := fmt.Sprint(i)
		points, labels := lokiMetricPoints(t, id, answers[id])
		// Loki's series projected onto the stream labels with the operator.
		byGroup := map[int64]map[string][]float64{}
		for key, series := range points {
			var parts []string
			for k, v := range labels[key] {
				if k == "app" || k == "ns" {
					parts = append(parts, k+"="+v)
				}
			}
			sort.Strings(parts)
			g := strings.Join(parts, ",")
			for ts, v := range series {
				if byGroup[ts] == nil {
					byGroup[ts] = map[string][]float64{}
				}
				byGroup[ts][g] = append(byGroup[ts][g], v)
			}
		}
		loki := map[int64]map[string]float64{}
		for ts, groups := range byGroup {
			loki[ts] = map[string]float64{}
			for g, vs := range groups {
				if len(vs) > 1 && s.parser == "" {
					t.Fatalf("#%d %s: Loki has %d series for %s with no parser", i, s.query(), len(vs), g)
				}
				v := vs[0]
				for _, x := range vs[1:] {
					switch s.op {
					case "max":
						v = math.Max(v, x)
					case "min":
						v = math.Min(v, x)
					default:
						v += x
					}
				}
				loki[ts][g] = v
			}
		}
		compareWithout(t, fmt.Sprintf("reference vs Loki #%d", i), s, withoutReference(s.rows, s.fn, s.op, int64(s.window), s.times()), loki, compareValues)
	}
}
