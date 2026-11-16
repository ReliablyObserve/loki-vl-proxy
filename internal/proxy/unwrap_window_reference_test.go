package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// unwrapRowSeries is the series a row belongs to under an unwrap range function: its stream plus the labels its
// parser extracts (the unwrapped label is not among them).
func unwrapRowSeries(r vlemu.Row) string {
	names := make([]string, 0, len(r.Labels))
	for k := range r.Labels {
		names = append(names, k)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(r.Stream)
	for _, k := range names {
		b.WriteString("|" + k + "=" + r.Labels[k])
	}
	return b.String()
}

// unwrapWindowValue is an unwrap range function over the values of one series in one window, in time order, as
// LogQL documents them: rate is the sum per second of the window, sum_over_time the sum, avg, max and min over time
// the mean, largest and smallest value, first and last over time the earliest and latest value, count_over_time the
// number of values.
func unwrapWindowValue(fn string, values []float64, window int64) float64 {
	switch fn {
	case "rate", "sum_over_time":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		if fn == "rate" {
			return sum / (float64(window) / 1e9)
		}
		return sum
	case "avg_over_time":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	case "max_over_time":
		m := values[0]
		for _, v := range values[1:] {
			m = math.Max(m, v)
		}
		return m
	case "min_over_time":
		m := values[0]
		for _, v := range values[1:] {
			m = math.Min(m, v)
		}
		return m
	case "first_over_time":
		return values[0]
	case "last_over_time":
		return values[len(values)-1]
	case "count_over_time":
		return float64(len(values))
	}
	panic("unwrapWindowValue: " + fn)
}

// unwrapWindowSeries is fn over every series of rows at each time t, over the window (t-window, t]: series key ->
// t -> value; a series with no row in a window has no value there.
func unwrapWindowSeries(rows []vlemu.Row, fn string, window int64, times []int64) map[string]map[int64]float64 {
	sorted := append([]vlemu.Row(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].TS < sorted[j].TS })
	out := map[string]map[int64]float64{}
	for _, t := range times {
		values := map[string][]float64{}
		for _, r := range sorted {
			if r.TS > t-window && r.TS <= t {
				key := unwrapRowSeries(r)
				values[key] = append(values[key], r.Value)
			}
		}
		for key, v := range values {
			if out[key] == nil {
				out[key] = map[int64]float64{}
			}
			out[key][t] = unwrapWindowValue(fn, v, window)
		}
	}
	return out
}

// unwrapSumReference is the ungrouped sum over every series of fn: t -> value, at the times some series has one.
func unwrapSumReference(rows []vlemu.Row, fn string, window int64, times []int64) map[int64]float64 {
	out := map[int64]float64{}
	for _, points := range unwrapWindowSeries(rows, fn, window, times) {
		for t, v := range points {
			out[t] += v
		}
	}
	return out
}

// unwrapReferenceCases are the scenarios the reference is held to Loki on: every function, parser and window shape
// the differential tests draw (sliding, tumbling, shorter than the step, instant), lines on window edges.
func unwrapReferenceCases() []uwScenario {
	rnd := rand.New(rand.NewSource(20261116))
	var out []uwScenario
	for _, fn := range []string{"rate", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "first_over_time", "last_over_time"} {
		for _, parser := range []string{"", "logfmt", "json"} {
			for _, instant := range []bool{false, true} {
				for i := 0; i < 12; i++ {
					s := generateUnwrapScenario(rnd, fn, parser, instant)
					if !instant && i%3 == 2 {
						s.window = []time.Duration{10 * time.Second, 30 * time.Second}[rnd.Intn(2)]
					}
					out = append(out, s)
				}
			}
		}
	}
	return out
}

// The unwrap window reference gives Loki's answer for every recorded scenario: sum(fn(... | unwrap amount [r]))
// at every step (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, semantics/unwrap-range-last-point, window-bounds-and-step-alignment
func TestUnwrapWindowReferenceMatchesRecordedLoki(t *testing.T) {
	cases := unwrapReferenceCases()
	var sets []oraclerun.EngineDataSet
	for i, s := range cases {
		parser := s.parser
		sets = append(sets, oraclerun.EngineDataSet{
			Streams:  engineStreams(s.rows, func(r vlemu.Row) (string, map[string]string) { return unwrapRowEntry(parser, "amount", r) }),
			Requests: []oraclerun.EngineRequest{lokiMetricRequest(fmt.Sprint(i), s.query(), s.start, s.end, s.step)},
		})
	}
	answers := lokiEngineAnswers(t, "unwrap_window_reference", sets)
	points := 0
	for i, s := range cases {
		loki, _ := lokiMetricPoints(t, fmt.Sprint(i), answers[fmt.Sprint(i)])
		got := map[int64]float64{}
		for _, series := range loki {
			for ts, v := range series {
				got[ts] = v
			}
		}
		points += len(got)
		compareUnwrapSum(t, fmt.Sprintf("reference vs Loki #%d", i), s, unwrapSumReference(s.rows, s.fn, int64(s.window), s.times()), got, len(loki))
	}
	if points < 500 {
		t.Fatalf("only %d points compared", points)
	}
}

// unwrapRowLabels are the labels of a row's series: its stream labels and the labels its parser extracts.
func unwrapRowLabels(r vlemu.Row) map[string]string {
	labels := vlemu.StreamLabels(r.Stream)
	for k, v := range r.Labels {
		labels[k] = v
	}
	return labels
}

// unwrapFoldAt folds fn's per-series values at t with agg (sum, avg, min, max, count) by the label by ("" for
// one group): group (the by label's value, "" for none) -> value.
func unwrapFoldAt(rows []vlemu.Row, fn, agg, by string, window, t int64) map[string]float64 {
	labels := map[string]map[string]string{}
	for _, r := range rows {
		labels[unwrapRowSeries(r)] = unwrapRowLabels(r)
	}
	layer := aggLayer{op: agg}
	if by != "" {
		layer.hasBy, layer.by = true, []string{by}
	}
	var vec []topKFoldRefSample
	for key, points := range unwrapWindowSeries(rows, fn, window, []int64{t}) {
		if v, ok := points[t]; ok {
			vec = append(vec, topKFoldRefSample{labels[key], v})
		}
	}
	folded, _ := layer.eval(vec)
	out := map[string]float64{}
	for _, s := range folded {
		out[s.labels[by]] = s.value
	}
	return out
}

// unwrapSeriesValuesAt is fn's value of every series at t.
func unwrapSeriesValuesAt(rows []vlemu.Row, fn string, window, t int64) []float64 {
	var out []float64
	for _, points := range unwrapWindowSeries(rows, fn, window, []int64{t}) {
		if v, ok := points[t]; ok {
			out = append(out, v)
		}
	}
	return out
}

// unwrapGroupedRangeFold is agg over fn(... [window]) by (by): fn takes the samples of every series of a group
// together, then agg folds the groups' values into one; ok is false when no group has a sample.
func unwrapGroupedRangeFold(rows []vlemu.Row, fn, agg, by string, window, t int64) (float64, bool) {
	grouped := make([]vlemu.Row, len(rows))
	for i, r := range rows {
		grouped[i] = vlemu.Row{TS: r.TS, Stream: "{" + by + "=" + strconv.Quote(unwrapRowLabels(r)[by]) + "}", Value: r.Value}
	}
	folded := unwrapFoldAt(grouped, fn, agg, "", window, t)
	v, ok := folded[""]
	return v, ok
}

// The first and last over time references (unwrapFoldAt, unwrapGroupedRangeFold, unwrapSeriesValuesAt) give
// Loki's answers on recorded scenarios of the first/last differentials: every fold ungrouped and by pod, an
// offset, a range function grouped by pod under a fold, and the series values topk ranks.
//
// conformance: semantics/unwrap-first-last-under-aggregation, metric-series-identity
func TestFirstLastReferenceMatchesRecordedLoki(t *testing.T) {
	type check struct {
		id   string
		want func(loki map[string]float64) string // "" when equal
	}
	rnd := rand.New(rand.NewSource(77))
	var sets []oraclerun.EngineDataSet
	var checks []check
	for _, parser := range []string{"", "logfmt", "json"} {
		for _, fn := range []string{"first_over_time", "last_over_time"} {
			for i := 0; i < 10; i++ {
				s := generateUnwrapScenario(rnd, fn, parser, true)
				s.rows = distinctPerPod(firstLastRows(s))
				rows, w, end := s.rows, int64(s.window), s.end
				body := fmt.Sprintf(`%s({app="uw"}%s | unwrap amount [%s]`, fn, parserPipe(parser), uwWindowText(s.window))
				set := oraclerun.EngineDataSet{Streams: engineStreams(rows, func(r vlemu.Row) (string, map[string]string) { return unwrapRowEntry(parser, "amount", r) })}
				add := func(query string, want func(map[string]float64) string) {
					id := fmt.Sprintf("%s/%s/%d/%d", parser, fn, i, len(set.Requests))
					set.Requests = append(set.Requests, lokiMetricRequest(id, query, end, end, 0))
					checks = append(checks, check{id + " " + query, want})
				}
				sameGroups := func(want map[string]float64) func(map[string]float64) string {
					return func(loki map[string]float64) string {
						if len(loki) != len(want) {
							return fmt.Sprintf("reference %v, Loki %v", want, loki)
						}
						for k, v := range want {
							if lv, ok := loki[k]; !ok || math.Abs(lv-v) > 1e-9*math.Max(1, math.Abs(v)) {
								return fmt.Sprintf("reference %v, Loki %v", want, loki)
							}
						}
						return ""
					}
				}
				for _, agg := range []string{"sum", "avg", "min", "max", "count"} {
					add(agg+"("+body+"))", sameGroups(unwrapFoldAt(rows, fn, agg, "", w, end)))
					add(agg+" by (pod) ("+body+"))", sameGroups(unwrapFoldAt(rows, fn, agg, "pod", w, end)))
					want := map[string]float64{}
					if v, ok := unwrapGroupedRangeFold(rows, fn, agg, "pod", w, end); ok {
						want[""] = v
					}
					add(agg+"("+body+") by (pod))", sameGroups(want))
				}
				add("sum("+body+" offset 1m))", sameGroups(unwrapFoldAt(rows, fn, "sum", "", w, end-int64(time.Minute))))
				values := unwrapSeriesValuesAt(rows, fn, w, end)
				sort.Float64s(values)
				add("bottomk(2, "+body+"))", func(loki map[string]float64) string {
					var got []float64
					for _, v := range loki {
						got = append(got, v)
					}
					sort.Float64s(got)
					want := values[:min(2, len(values))]
					if fmt.Sprint(got) != fmt.Sprint(want) {
						return fmt.Sprintf("reference %v, Loki %v", want, got)
					}
					return ""
				})
				sets = append(sets, set)
			}
		}
	}
	answers := lokiEngineAnswers(t, "first_last_reference", sets)
	for _, c := range checks {
		id := strings.SplitN(c.id, " ", 2)[0]
		points, labels := lokiMetricPoints(t, c.id, answers[id])
		loki := map[string]float64{}
		for key, series := range points {
			name := labels[key]["pod"]
			if _, dup := loki[name]; dup {
				name = canonicalLabelText(labels[key])
			}
			for _, v := range series {
				loki[name] = v
			}
		}
		if msg := c.want(loki); msg != "" {
			t.Errorf("%s: %s", c.id, msg)
		}
	}
}
