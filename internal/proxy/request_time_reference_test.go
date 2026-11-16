package proxy

import (
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The request reference of the sub-second step tests (lokiStep, lokiTimestamp, lokiMs, lokiEvalTimes, lokiTimeText)
// reads requests as Loki does: Loki's own request parser and engine (test/oracle/loki engine) answer `vector(1)`
// over the same start and end texts with a point at each of its evaluation times, written as Loki writes them,
// with and without step alignment; an instant query answers at its time. The step is sent as the whole
// milliseconds the reference keeps of it, the value Loki's query frontend hands its engine (the engine oracle
// runs without a frontend; the live TestCompat_SubSecondStep holds the truncation). Steps and times Loki rejects
// are the ones the reference cannot read (LVP_RUN_ORACLES=1 records again).
//
// conformance: semantics/step-duration-grammar-wider-than-loki, window-bounds-and-step-alignment
func TestRequestTimeReferenceMatchesRecordedLoki(t *testing.T) {
	rnd := rand.New(rand.NewSource(1001))
	var cases []ssScenario
	set := oraclerun.EngineDataSet{}
	for i := 0; i < 400; i++ {
		s := ssGenerate(rnd, ssKinds[i%len(ssKinds)], i%5 == 4, i%3 == 2)
		cases = append(cases, s)
		form := url.Values{"query": {"vector(1)"}}
		if s.instant {
			form.Set("time", s.endText)
		} else {
			form.Set("start", s.startText)
			form.Set("end", s.endText)
			form.Set("step", strconv.FormatInt(s.lokiStepNs()/1e6, 10)+"ms")
		}
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: fmt.Sprint(i), Instant: s.instant, Unaligned: !s.align, Form: form.Encode()})
	}
	// Spellings the reference must refuse as Loki does, and ones it must read (an empty step is Loki's default step, no spelling).
	steps := []string{"1.5s", "500us", "1h30m5.5s", "abc", "5x", "15", "15s", "1m30s", "0.5", "1h", "2d", "1w", "1y", "100ms", "1m5s100ms", "0.001", "3.0001"}
	for i, step := range steps {
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: "step" + strconv.Itoa(i), Unaligned: true, Form: url.Values{"query": {"vector(1)"}, "start": {"1800000000"}, "end": {"1800000000"}, "step": {step}}.Encode()})
	}
	times := []string{"1800000000", "1800000000.5", "1800000000.1234", "1800000000.9999", "1800000000000000000", "1800000000123456789", "18000000000", "2027-01-15T08:00:00Z", "2027-01-15T08:00:00.123456789Z", "not-a-time"}
	for i, ts := range times {
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: "time" + strconv.Itoa(i), Instant: true, Form: url.Values{"query": {"vector(1)"}, "time": {ts}}.Encode()})
	}
	answers := lokiEngineAnswers(t, "request_time_reference", []oraclerun.EngineDataSet{set})
	lokiTimes := func(id string) ([]string, bool) {
		a := answers[id]
		if a.Status != 200 {
			return nil, false
		}
		s, err := decodeLokiSeries(a.Body)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		var out []string
		for _, points := range s.Points {
			ms := make([]int64, 0, len(points))
			for m := range points {
				ms = append(ms, m)
			}
			sortInt64s(ms)
			for _, m := range ms {
				out = append(out, lokiTimeText(m))
			}
		}
		return out, true
	}
	for i, s := range cases {
		got, ok := lokiTimes(fmt.Sprint(i))
		if !ok {
			t.Fatalf("#%d %s: Loki answered %d %s", i, s.label(), answers[fmt.Sprint(i)].Status, answers[fmt.Sprint(i)].Error)
		}
		var want []string
		for _, ts := range s.times() {
			want = append(want, lokiTimeText(lokiMs(ts)))
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("#%d start=%q end=%q step=%q instant=%v align=%v: reference %v, Loki %v", i, s.startText, s.endText, s.step, s.instant, s.align, want, got)
		}
	}
	for i, step := range steps {
		_, lokiOK := lokiTimes("step" + strconv.Itoa(i))
		d, ok := lokiStep(step)
		if ok != lokiOK && !(ok && d <= 0) {
			t.Errorf("step %q: reference reads it %v (%v), Loki %v", step, ok, d, answers["step"+strconv.Itoa(i)])
		}
	}
	for i, ts := range times {
		got, lokiOK := lokiTimes("time" + strconv.Itoa(i))
		ns, ok := lokiTimestamp(ts)
		if ok != lokiOK {
			t.Errorf("time %q: reference reads it %v, Loki %d %s", ts, ok, answers["time"+strconv.Itoa(i)].Status, answers["time"+strconv.Itoa(i)].Error)
			continue
		}
		if ok && (len(got) != 1 || got[0] != lokiTimeText(lokiMs(ns))) {
			t.Errorf("time %q: reference %s, Loki %v", ts, lokiTimeText(lokiMs(ns)), got)
		}
	}
}

func sortInt64s(v []int64) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}
