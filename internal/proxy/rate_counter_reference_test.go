package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// rateCounterParsedKey is the series a rate_counter line belongs to: its stream and the keys its parser reads from
// the line, the unwrapped one left out.
func rateCounterParsedKey(parser string, r vlemu.Row) string {
	pairs := map[string]string{}
	if parser == "json" {
		var obj map[string]any
		if json.Unmarshal([]byte(r.Msg), &obj) == nil {
			for k, v := range obj {
				pairs[k] = fmt.Sprint(v)
			}
		}
	} else {
		for _, field := range strings.Fields(r.Msg) {
			if k, v, ok := strings.Cut(field, "="); ok {
				pairs[k] = v
			}
		}
	}
	delete(pairs, "amount")
	names := make([]string, 0, len(pairs))
	for k := range pairs {
		names = append(names, k)
	}
	sort.Strings(names)
	key := r.Stream
	for _, k := range names {
		key += "|" + k + "=" + pairs[k]
	}
	return key
}

// rateCounterSingletonSum is `sum [by (group)] (rate_counter(... | unwrap amount [window]))` for windows in which
// every series holds at most one sample: a counter with one sample has not moved, so its rate is 0, and the sum
// of a group with a sample at t is 0; a group with no sample has no point. ok is false when some series holds two
// samples in a window, which this reference does not evaluate.
func rateCounterSingletonSum(parser string, rows []vlemu.Row, window int64, times []int64, group string) (out map[string]map[int64]float64, ok bool) {
	out = map[string]map[int64]float64{}
	for _, t := range times {
		seen := map[string]bool{}
		for _, r := range rows {
			if r.TS <= t-window || r.TS > t {
				continue
			}
			key := rateCounterParsedKey(parser, r)
			if seen[key] {
				return nil, false
			}
			seen[key] = true
			g := ""
			if group != "" {
				g = r.Labels[group]
			}
			if out[g] == nil {
				out[g] = map[int64]float64{}
			}
			out[g][t] = 0
		}
	}
	return out, true
}

// rateCounterReference is the reference the one-sample proof tests compare with; a scenario it cannot evaluate
// fails the test (the stats-only answer must never be given for a series with two samples).
func rateCounterReference(t testing.TB, s rcScenario) map[string]map[int64]float64 {
	t.Helper()
	want, ok := rateCounterSingletonSum(s.parser, s.rows, int64(s.window), s.times(), s.group)
	if !ok {
		t.Fatalf("%s: a series holds two samples in a window, and the proxy answered without reading rows", s.query())
	}
	return want
}

// The singleton reference gives Loki's answer bit for bit at every step whose windows hold at most one sample per
// series, on recorded scenarios of the one-sample differential: logfmt and json, ungrouped and by level, NaN, zero
// and dropping values, edge lines (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/unwrap-rate-counter-one-sample-series
func TestRateCounterReferenceMatchesRecordedLoki(t *testing.T) {
	rnd := rand.New(rand.NewSource(4242))
	var cases []rcScenario
	var sets []oraclerun.EngineDataSet
	for i := 0; i < 160; i++ {
		s := generateRateCounterScenario(rnd, []string{"logfmt", "json"}[i%2], i%5 == 4)
		id := fmt.Sprint(i)
		cases = append(cases, s)
		sets = append(sets, oraclerun.EngineDataSet{
			Streams:  engineStreams(s.rows, func(r vlemu.Row) (string, map[string]string) { return r.Msg, nil }),
			Requests: []oraclerun.EngineRequest{lokiMetricRequest(id, s.query(), s.start, s.end, s.step)},
		})
	}
	answers := lokiEngineAnswers(t, "rate_counter_reference", sets)
	steps := 0
	for i, s := range cases {
		id := fmt.Sprint(i)
		points, labels := lokiMetricPoints(t, id, answers[id])
		loki := map[string]map[int64]float64{}
		for key, series := range points {
			loki[labels[key][s.group]] = series
		}
		for _, ts := range s.times() {
			want, ok := rateCounterSingletonSum(s.parser, s.rows, int64(s.window), []int64{ts}, s.group)
			if !ok {
				continue
			}
			steps++
			for g, pts := range want {
				if lv, has := loki[g][ts]; !has || lv != pts[ts] {
					t.Fatalf("#%d %s at %d: group %q reference %v, Loki %v (present %v)", i, s.query(), ts, g, pts[ts], lv, has)
				}
			}
			for g, pts := range loki {
				if _, has := pts[ts]; has && want[g] == nil {
					t.Fatalf("#%d %s at %d: Loki has group %q = %v, the reference none", i, s.query(), ts, g, pts[ts])
				}
			}
		}
	}
	if steps < 300 {
		t.Fatalf("only %d steps compared", steps)
	}
}
