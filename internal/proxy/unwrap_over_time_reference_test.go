package proxy

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// overTimeReference is an unwrap range function per stream over the window (t-offset-window, t-offset], folded by
// group (the group key of a stream) with outer ("" is sum; max, min, avg, count): group -> t -> value.
func overTimeReference(rows []vlemu.Row, fn, outer string, window, offset int64, times []int64, group func(stream string) string) map[string]map[int64]float64 {
	if outer == "" {
		outer = "sum"
	}
	byStream := map[string][]vlemu.Row{}
	for _, r := range rows {
		byStream[r.Stream] = append(byStream[r.Stream], r)
	}
	out := map[string]map[int64]float64{}
	for _, t := range times {
		var vec []topKFoldRefSample
		for stream, rs := range byStream {
			for _, points := range unwrapWindowSeries(rs, fn, window, []int64{t - offset}) {
				if v, ok := points[t-offset]; ok {
					vec = append(vec, topKFoldRefSample{map[string]string{"g": group(stream)}, v})
				}
			}
		}
		folded, _ := aggLayer{op: outer, hasBy: true, by: []string{"g"}}.eval(vec)
		for _, s := range folded {
			if out[s.labels["g"]] == nil {
				out[s.labels["g"]] = map[int64]float64{}
			}
			out[s.labels["g"]][t] = s.value
		}
	}
	return out
}

// The over-time reference gives Loki's answer on recorded scenarios of the windows differential: every query form,
// function and parser it draws, offsets, ranges longer and shorter than the step, lines on window edges, instants
// (test/oracle/loki engine; LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment
func TestOverTimeReferenceMatchesRecordedLoki(t *testing.T) {
	rnd := rand.New(rand.NewSource(1985))
	var cases []owCase
	var sets []oraclerun.EngineDataSet
	for _, parser := range []string{"", "logfmt", "json"} {
		for _, fn := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
			for _, shape := range owShapes {
				if !shape.parser(parser, fn) {
					continue
				}
				for i := 0; i < 3; i++ {
					c := owGenerate(rnd, fn, parser, shape, i == 2 && !shape.rangeOnly)
					id := fmt.Sprint(len(cases))
					cases = append(cases, c)
					p := parser
					sets = append(sets, oraclerun.EngineDataSet{
						Streams:  engineStreams(c.rows, func(r vlemu.Row) (string, map[string]string) { return unwrapRowEntry(p, "amount", r) }),
						Requests: []oraclerun.EngineRequest{lokiMetricRequest(id, c.query(), c.start, c.end, c.step)},
					})
				}
			}
		}
	}
	answers := lokiEngineAnswers(t, "over_time_reference", sets)
	for i, c := range cases {
		a := answers[fmt.Sprint(i)]
		if a.Status != 200 {
			t.Fatalf("#%d %s: Loki answered %d %s", i, c.query(), a.Status, a.Error)
		}
		compareOverTime(t, fmt.Sprintf("reference vs Loki #%d", i), c, owParseAnswer(t, c, a.Body), c.want())
	}
}
