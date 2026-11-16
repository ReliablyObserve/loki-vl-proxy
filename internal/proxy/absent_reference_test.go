package proxy

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// absentReferenceSteps are the evaluation times at which absent_over_time answers its one series, as LogQL
// documents it: 1 where the range holds no sample, nothing where it holds one. The range of time t is
// (t-offset-window, t-offset].
func absentReferenceSteps(rows []vlemu.Row, window, offset int64, times []int64) []int64 {
	var out []int64
	for _, t := range times {
		hi := t - offset
		empty := true
		for _, r := range rows {
			if r.TS > hi-window && r.TS <= hi {
				empty = false
				break
			}
		}
		if empty {
			out = append(out, t)
		}
	}
	return out
}

// The absent reference gives Loki's steps on every recorded scenario of the differential's generator: range and
// instant, offsets, millisecond starts, lines one nanosecond beside the window edges (test/oracle/loki engine;
// LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/absent-over-time-semantics, window-bounds-and-step-alignment
func TestAbsentReferenceMatchesRecordedLoki(t *testing.T) {
	rnd := rand.New(rand.NewSource(424243))
	var cases []absentScenario
	var sets []oraclerun.EngineDataSet
	for i := 0; i < 240; i++ {
		s := generateAbsentScenario(rnd, i%3 == 0, i%2 == 0)
		id := fmt.Sprint(i)
		cases = append(cases, s)
		sets = append(sets, oraclerun.EngineDataSet{
			Streams:  engineStreams(s.rows, func(r vlemu.Row) (string, map[string]string) { return "amount=" + r.Text(), nil }),
			Requests: []oraclerun.EngineRequest{lokiMetricRequest(id, s.query(), s.start, s.end, s.step)},
		})
	}
	answers := lokiEngineAnswers(t, "absent_reference", sets)
	withData, absent := 0, 0
	for i, s := range cases {
		id := fmt.Sprint(i)
		points, _ := lokiMetricPoints(t, id, answers[id])
		if len(points) > 1 {
			t.Fatalf("#%d: %d series", i, len(points))
		}
		got := map[int64]bool{}
		for _, series := range points {
			for ts := range series {
				got[ts] = true
			}
		}
		want := absentReferenceSteps(s.rows, int64(s.window), int64(s.offset), s.times())
		if len(want) != len(got) {
			t.Fatalf("#%d %s: reference %v, Loki %v\n%s", i, s.query(), want, got, s.describe())
		}
		for _, ts := range want {
			if !got[ts] {
				t.Fatalf("#%d %s: reference has %d, Loki %v\n%s", i, s.query(), ts, got, s.describe())
			}
		}
		absent += len(want)
		withData += len(s.times()) - len(want)
	}
	if absent < 200 || withData < 200 {
		t.Fatalf("generator covers %d absent and %d present steps", absent, withData)
	}
}
