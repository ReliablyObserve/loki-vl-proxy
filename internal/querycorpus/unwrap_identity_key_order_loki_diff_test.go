package querycorpus_test

import (
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The Loki differential of a parser's series identity over lines that hold the same keys in another order: real
// Loki (the engine oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and
// v1.49.0) over the same data, through the proxy's real query_range handler.
//
//   - Data (keyOrderFixture): a line every ten seconds over an hour and the range before it, on three streams, each
//     one series in Loki whose lines alternate two key orders: flat json (pod a of ko), nested json objects (pod b of
//     ko) and logfmt (kol).
//   - Queries (keyOrderQueries): sum, sum by the stream's pod, avg by pod over avg, max and stddev over time, with a
//     range longer than the step (the window evaluator, the series that lie in one bucket folded in VictoriaLogs)
//     and with a range up to the step (the stats in VictoriaLogs).
//   - Requests: an hour at steps of 60 s and 20 s.
//
// Every run must match Loki. The answers are recorded (keyOrderAnswersPath; LVP_RUN_ORACLES=1 records them again,
// with GOMODCACHE set to a fresh directory when the shared module cache holds other copies of VictoriaLogs).

const keyOrderAnswersPath = "testdata/unwrap-identity-key-order-loki-diff.jsonl.gz"

var keyOrderQueries = []string{
	`sum(avg_over_time({service_name="ko"} | json | unwrap duration_ms [5m]))`,
	`sum(avg_over_time({service_name="ko"} | json | unwrap duration_ms [1m]))`,
	`sum by (pod) (avg_over_time({service_name="ko"} | json | unwrap duration_ms [5m]))`,
	`sum by (pod) (stddev_over_time({service_name="ko"} | json | unwrap duration_ms [1m]))`,
	`avg by (pod) (max_over_time({service_name="ko"} | json | unwrap duration_ms [5m]))`,
	`sum(stddev_over_time({service_name="ko"} | json | unwrap duration_ms [5m]))`,
	`sum(avg_over_time({service_name="kol"} | logfmt | unwrap duration_ms [5m]))`,
	`sum(avg_over_time({service_name="kol"} | logfmt | unwrap duration_ms [1m]))`,
	`avg(stddev_over_time({service_name="kol"} | logfmt | unwrap duration_ms [5m]))`,
}

// keyOrderRequests are an hour at steps of 60 s and 20 s, the bounds on the step's grid.
func keyOrderRequests(q string) []metricRequest {
	var out []metricRequest
	for _, step := range []time.Duration{60 * time.Second, 20 * time.Second} {
		end := alignDown(metricEnd, step)
		start := alignDown(metricEnd.Add(-time.Hour), step)
		form := url.Values{"query": {q}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)},
			"step": {strconv.FormatInt(int64(step/time.Second), 10)}}
		out = append(out, metricRequest{kind: "1h step=" + step.String(), form: form, route: lower.RouteRequest{Step: step, Span: end.Sub(start), End: end}})
	}
	return out
}

// keyOrderFixture is a line every ten seconds per stream, the values alternating 3 and 4 and the key order with them:
// one series per stream in Loki, whose windows average 3.5.
func keyOrderFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"service_name": "ko", "pod": "a"},
		{"service_name": "ko", "pod": "b"},
		{"service_name": "kol", "pod": "a"},
	}}
	add := func(s int, at time.Time, line string) {
		f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line, Metadata: map[string]string{"detected_level": "info"}})
	}
	first := metricEnd.Add(-time.Hour - 10*time.Minute)
	for i := 0; ; i++ {
		at := first.Add(time.Duration(i)*10*time.Second + 500*time.Microsecond)
		if !at.Before(metricEnd) {
			break
		}
		d := strconv.Itoa(3 + i%2)
		if i%2 == 0 {
			add(0, at, `{"route":"/x","duration_ms":`+d+`,"user":"u"}`)
			add(1, at.Add(time.Millisecond), `{"a":{"x":"1","y":"2"},"duration_ms":`+d+`}`)
			add(2, at.Add(2*time.Millisecond), `route=/x duration_ms=`+d+` user=u`)
		} else {
			add(0, at, `{"user":"u","duration_ms":`+d+`,"route":"/x"}`)
			add(1, at.Add(time.Millisecond), `{"duration_ms":`+d+`,"a":{"y":"2","x":"1"}}`)
			add(2, at.Add(2*time.Millisecond), `user=u duration_ms=`+d+` route=/x`)
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// keyOrderMetricQueries are the differential's queries over the fixture.
func keyOrderMetricQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := keyOrderFixture()
	var out []metricQuery
	for _, q := range keyOrderQueries {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: keyOrderRequests(q)})
	}
	return out
}

// TestUnwrapIdentityKeyOrderMatchLoki replays the recorded answers through the handler: lines that hold the same keys
// in another order are one series, every run Loki's answer.
//
// conformance: semantics/unwrap-identity-key-order, semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestUnwrapIdentityKeyOrderMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := keyOrderMetricQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, keyOrderAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, keyOrderAnswersPath)
	var b strings.Builder
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				loki, ok := rec.Loki[mr.kind]
				if !ok || loki.Status != 200 {
					t.Fatalf("no Loki answer for %s %s: record them again", mr.kind, mq.q)
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				for c, ex := range compareMetric(loki, run).classes {
					t.Errorf("%s %s %s: %s: %s", prof.name, mr.kind, mq.q, c, ex)
				}
			}
		}
		b.WriteString("\n  " + mq.q)
	}
	t.Logf("lines of one series in two key orders, against Loki:%s", b.String())
}
