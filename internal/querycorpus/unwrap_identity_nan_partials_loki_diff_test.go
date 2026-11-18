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

// The Loki differential of a parser's series identity over series whose value is NaN without a line of nan: lines
// of an infinity of each sign (their sum and mean are NaN), and a value with an infinity (their deviation is NaN).
// Real Loki (the engine oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0
// and v1.49.0) over the same data, through the proxy's real query_range handler.
//
//   - Data: nanv, a request id on every line every ten seconds plus such series now and then (and a line of nan); nan1
//     and nanl (logfmt), the only series of the read, one series of 2 and inf, one of NaN and 3, one of inf and -inf.
//   - Queries: sum, avg, max and min over avg, stddev and min over time, with a range longer than the step (the
//     window evaluator, the series that lie in one bucket folded in VictoriaLogs) and up to the step (the stats in
//     VictoriaLogs).
//   - Requests: an hour at steps of 60 s and 20 s.
//
// VictoriaLogs' sum keeps NaN as its empty value: an outer sum over a series of NaN left it out, and the fold's
// second stats pipe answered 0 for a series that keeps its hash, where Loki answers NaN. Every run must match Loki.
// The answers are recorded (nanPartialsAnswersPath; LVP_RUN_ORACLES=1 records them again, with GOMODCACHE set to a
// fresh directory when the shared module cache holds other copies of VictoriaLogs).

const nanPartialsAnswersPath = "testdata/unwrap-identity-nan-partials-loki-diff.jsonl.gz"

var nanPartialsQueries = []string{
	`sum(avg_over_time({service_name="nanv"} | json | unwrap duration_ms [5m]))`,
	`sum(stddev_over_time({service_name="nanv"} | json | unwrap duration_ms [5m]))`,
	`max(avg_over_time({service_name="nanv"} | json | unwrap duration_ms [5m]))`,
	`sum(avg_over_time({service_name="nanv"} | json | unwrap duration_ms [1m]))`,
	`avg(avg_over_time({service_name="nanv"} | json | unwrap duration_ms [1m]))`,
	`sum(stddev_over_time({service_name="nanv"} | json | unwrap duration_ms [1m]))`,
	`sum(avg_over_time({service_name="nan1"} | json | unwrap duration_ms [5m]))`,
	`sum(stddev_over_time({service_name="nan1"} | json | unwrap duration_ms [5m]))`,
	`avg(avg_over_time({service_name="nan1"} | json | unwrap duration_ms [5m]))`,
	`avg(min_over_time({service_name="nan1"} | json | unwrap duration_ms [5m]))`,
	`max(avg_over_time({service_name="nan1"} | json | unwrap duration_ms [5m]))`,
	`sum(avg_over_time({service_name="nan1"} | json | unwrap duration_ms [1m]))`,
	`sum(avg_over_time({service_name="nanl"} | logfmt | unwrap duration_ms [5m]))`,
	`sum(stddev_over_time({service_name="nanl"} | logfmt | unwrap duration_ms [5m]))`,
	`sum(avg_over_time({service_name="nanl"} | logfmt | unwrap duration_ms [1m]))`,
}

// nanPartialsRequests are an hour at steps of 60 s and 20 s, the bounds on the step's grid.
func nanPartialsRequests(q string) []metricRequest {
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

func nanPartialsAdd(f *metricFixture, s int, at time.Time, line string) {
	f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line, Metadata: map[string]string{"detected_level": "info"}})
}

func nanPartialsFinish(f *metricFixture) metricFixture {
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return *f
}

// nanPartialsMixed: a request id on every line, plus, now and then, a series of 2 and inf, one of inf and -inf, one
// of inf, -inf and 5 (VictoriaLogs' sum restarts after the NaN) and one of a nan line and 3.
func nanPartialsMixed() metricFixture {
	f := metricFixture{Streams: []map[string]string{{"service_name": "nanv", "pod": "a"}}}
	first := metricEnd.Add(-time.Hour - 10*time.Minute)
	for i := 0; ; i++ {
		at := first.Add(time.Duration(i)*10*time.Second + 500*time.Microsecond)
		if !at.Before(metricEnd) {
			break
		}
		n := strconv.Itoa(i)
		nanPartialsAdd(&f, 0, at, `{"duration_ms":`+strconv.Itoa(i%97+1)+`,"req":"r`+n+`"}`)
		var values []string
		switch i % 40 {
		case 5:
			values = []string{"2", `"inf"`}
		case 15:
			values = []string{`"inf"`, `"-inf"`}
		case 25:
			values = []string{`"NaN"`, "3"}
		case 35:
			values = []string{`"inf"`, `"-inf"`, "5"}
		}
		for k, v := range values {
			nanPartialsAdd(&f, 0, at.Add(time.Duration(k+1)*time.Millisecond), `{"duration_ms":`+v+`,"req":"s`+n+`"}`)
		}
	}
	return nanPartialsFinish(&f)
}

// nanPartialsAlone: the only series of the read, in json (nan1) or logfmt (nanl): 2 and inf, then nan and 3, then
// inf and -inf, each pair in one bucket.
func nanPartialsAlone(svc string, logfmt bool) metricFixture {
	f := metricFixture{Streams: []map[string]string{{"service_name": svc, "pod": "a"}}}
	for _, s := range []struct {
		before   time.Duration
		req      string
		from, to string
	}{{30 * time.Minute, "o", "2", "inf"}, {15 * time.Minute, "n", "NaN", "3"}, {5 * time.Minute, "pm", "inf", "-inf"}} {
		at := metricEnd.Add(-s.before + 10*time.Second + 500*time.Microsecond)
		for k, v := range []string{s.from, s.to} {
			line := `duration_ms=` + v + ` req=` + s.req
			if !logfmt {
				if _, err := strconv.ParseFloat(v, 64); err != nil || v == "inf" || v == "-inf" || v == "NaN" {
					v = strconv.Quote(v)
				}
				line = `{"duration_ms":` + v + `,"req":"` + s.req + `"}`
			}
			nanPartialsAdd(&f, 0, at.Add(time.Duration(k)*time.Millisecond), line)
		}
	}
	return nanPartialsFinish(&f)
}

// nanPartialsFixture is the data set of a query's service.
func nanPartialsFixture(q string) metricFixture {
	switch {
	case strings.Contains(q, `"nanv"`):
		return nanPartialsMixed()
	case strings.Contains(q, `"nanl"`):
		return nanPartialsAlone("nanl", true)
	}
	return nanPartialsAlone("nan1", false)
}

// TestUnwrapIdentityNaNPartialsMatchLoki replays the recorded answers through the handler: a series whose value is
// NaN is NaN in the sum and the average over the series, on both step regimes, every run Loki's answer.
//
// conformance: semantics/unwrap-identity-nan-partials, semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestUnwrapIdentityNaNPartialsMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	var queries []metricQuery
	for _, q := range nanPartialsQueries {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		queries = append(queries, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: nanPartialsFixture(q), reqs: nanPartialsRequests(q)})
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, nanPartialsAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, nanPartialsAnswersPath)
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
	}
}
