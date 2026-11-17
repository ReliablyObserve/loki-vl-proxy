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

// The Loki differential of a parser's series identity over a range longer than the step, read with the series that
// lie in one bucket folded in VictoriaLogs (internal/proxy/series_fold_read.go, lower.SeriesFold): real Loki (the
// engine oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and v1.49.0) over
// the same data, through the proxy's real query_range handler.
//
//   - Data, three sets of json lines over an hour and the range before it:
//   - per line (seriesFoldPerLine): a request id on every line, so every line is a series of its own;
//   - mixed (seriesFoldMixed): the same, plus health checks that repeat one series per stream all along, request
//     ids logged twice (two seconds apart, and seventy seconds apart, across a bucket edge), a line of a NaN (a series
//     that keeps its partials), and a third stream whose lines carry no level. An infinity is left out: Loki's mean
//     of the series is NaN or the infinity by the order it visits them in, which differed between two recordings of
//     the same data (semantics/unwrap-gate-parsefloat-divergences); the fold keeps an infinite series' partials.
//   - Queries (seriesFoldQueries): sum, avg, max and min over avg, max, min, stddev and stdvar over time, by a stream
//     label, by a parsed label and none.
//   - spanning (seriesFoldSpanFixture): every request id logged twice 70 s apart, so every series spans buckets;
//     read once and in time chunks under a budget of 600 series, the chunked answer the one read's byte for byte.
//   - Requests (seriesFoldRequests): an hour at steps of 60 s, 20 s, 7 s (buckets of one second) and 120 s (a range
//     the step does not divide), and at 60 s with bounds off the minute grid (moved to it), each read in one
//     stats_query_range call. A subquery grid offset from the minute (bounds the proxy does not align) is held by
//     the emulator's differentials (internal/proxy TestDynamicIdentityWindowsMatchLokiReference, FuzzSeriesFold).
//
// Every run must match Loki. The answers are recorded (seriesFoldAnswersPath; LVP_RUN_ORACLES=1 records them again,
// with GOMODCACHE set to a fresh directory when the shared module cache holds other copies of VictoriaLogs).

const seriesFoldAnswersPath = "testdata/series-fold-loki-diff.jsonl.gz"

// seriesFoldQueries are the shapes over the unwrapped duration_ms of a service (SVC: api for the lines that are a
// series each, mix for the mixed set).
var seriesFoldQueries = []string{
	`sum by (service_name) (avg_over_time({service_name="SVC"} | json | unwrap duration_ms [5m]))`,
	`sum(stddev_over_time({service_name="SVC"} | json | unwrap duration_ms [5m]))`,
	`avg(avg_over_time({service_name="SVC"} | json | unwrap duration_ms [5m]))`,
	`max by (pod) (avg_over_time({service_name="SVC"} | json | unwrap duration_ms [5m]))`,
	`sum(max_over_time({service_name="SVC"} | json | unwrap duration_ms [5m]))`,
	`min(stdvar_over_time({service_name="SVC"} | json | unwrap duration_ms [5m]))`,
	`avg by (route) (min_over_time({service_name="SVC"} | json | unwrap duration_ms [3m]))`,
}

// seriesFoldService is the service of a fixture.
func seriesFoldService(mixed bool) string {
	if mixed {
		return "mix"
	}
	return "api"
}

// seriesFoldSpan is the window of the requests.
const seriesFoldSpan = time.Hour

// seriesFoldRequests are the requests of a query.
func seriesFoldRequests(q string) []metricRequest {
	var out []metricRequest
	for _, tc := range []struct {
		step time.Duration
		off  time.Duration
	}{{60 * time.Second, 0}, {20 * time.Second, 0}, {7 * time.Second, 0}, {120 * time.Second, 0}, {60 * time.Second, 17 * time.Second}} {
		end := alignDown(metricEnd, tc.step).Add(-tc.off)
		start := alignDown(metricEnd.Add(-seriesFoldSpan), tc.step).Add(-tc.off)
		form := url.Values{"query": {q}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)},
			"step": {strconv.FormatInt(int64(tc.step/time.Second), 10)}}
		name := "1h step=" + tc.step.String()
		if tc.off != 0 {
			name += " off the grid"
		}
		// Bounds off the grid are moved to it by the proxy (-align-queries-with-step, the deployed default), as Loki
		// evaluates them.
		out = append(out, metricRequest{kind: name, form: form, align: tc.off != 0, route: lower.RouteRequest{Step: tc.step, Span: end.Sub(start), End: end}})
	}
	return out
}

// seriesFoldLine is a json line of the fixtures.
func seriesFoldLine(duration string, keys ...string) string {
	fields := []string{`"duration_ms":` + duration}
	for i := 0; i+1 < len(keys); i += 2 {
		fields = append(fields, strconv.Quote(keys[i])+":"+strconv.Quote(keys[i+1]))
	}
	return "{" + strings.Join(fields, ",") + "}"
}

// seriesFoldFixture is a line every second over the window and the range before it, on two streams in turn, each
// with a request id of its own; mixed adds the repeated and the shared series.
func seriesFoldFixture(mixed bool) metricFixture {
	svc := seriesFoldService(mixed)
	f := metricFixture{Streams: []map[string]string{
		{"service_name": svc, "pod": "a"},
		{"service_name": svc, "pod": "b"},
	}}
	if mixed {
		// A stream whose lines carry no level.
		f.Streams = append(f.Streams, map[string]string{"service_name": svc, "pod": "c"})
	}
	first := metricEnd.Add(-seriesFoldSpan - 10*time.Minute)
	add := func(s int, at time.Time, line string) {
		f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line, Metadata: map[string]string{"detected_level": "info"}})
	}
	for i := 0; ; i++ {
		// Half a millisecond past a millisecond: every bound and step is whole seconds, so no line sits on an edge.
		at := first.Add(time.Duration(i)*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
		if !at.Before(metricEnd) {
			break
		}
		s := i % 2
		duration := strconv.Itoa((i*7919)%5000 + 1)
		req := "r" + strconv.Itoa(i)
		route := "/r" + strconv.Itoa(i%7)
		add(s, at, seriesFoldLine(duration, "req", req, "route", route))
		if !mixed {
			continue
		}
		switch {
		case i%20 == 1: // a health check: one series per stream all along
			add(s, at.Add(2*time.Millisecond), seriesFoldLine(strconv.Itoa(i%50+1), "route", "/health"))
		case i%100 == 3: // a request id logged twice, two seconds apart
			add(s, at.Add(2*time.Second), seriesFoldLine(strconv.Itoa(i%300+1), "req", req, "route", route))
		case i%150 == 7: // a request id logged twice, across a bucket edge
			add(s, at.Add(70*time.Second), seriesFoldLine(strconv.Itoa(i%400+1), "req", req, "route", route))
		case i == 1801:
			add(s, at.Add(time.Millisecond), seriesFoldLine(`"NaN"`, "req", "nan", "route", route))
		case i%10 == 9: // a line of the stream without a level
			f.Entries = append(f.Entries, metricEntry{Stream: 2, TS: at.Add(3 * time.Millisecond).UnixNano(), Line: seriesFoldLine(duration, "req", "c"+req)})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// seriesFoldMetricQueries are the differential's queries over both fixtures.
func seriesFoldMetricQueries(t *testing.T) []metricQuery {
	t.Helper()
	var out []metricQuery
	for _, mixed := range []bool{false, true} {
		fix := seriesFoldFixture(mixed)
		for _, shape := range seriesFoldQueries {
			q := strings.ReplaceAll(shape, "SVC", seriesFoldService(mixed))
			p, err := planOfText(q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: seriesFoldRequests(q)})
		}
	}
	span := seriesFoldSpanFixture()
	for _, q := range []string{
		`sum by (pod) (avg_over_time({service_name="span"} | json | unwrap duration_ms [5m]))`,
		`sum(stddev_over_time({service_name="span"} | json | unwrap duration_ms [5m]))`,
	} {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		end := alignDown(metricEnd, time.Minute)
		start := alignDown(metricEnd.Add(-seriesFoldSpan), time.Minute)
		form := url.Values{"query": {q}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)}, "step": {"60"}}
		route := lower.RouteRequest{Step: time.Minute, Span: end.Sub(start), End: end}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: span, reqs: []metricRequest{
			{kind: "1h step=1m0s", form: form, route: route},
			{kind: "1h step=1m0s chunked", form: form, route: route, rowLimit: seriesFoldSpanBudget},
		}})
	}
	return out
}

// seriesFoldSpanBudget is the read budget of the chunked runs of the spanning set: more than the differential's
// series limit (500), less than the series that span buckets in an hour's read.
const seriesFoldSpanBudget = 600

// seriesFoldSpanFixture is a line every two seconds on two streams, each request id logged again 70 seconds later,
// across a bucket edge: every series spans buckets, so every one keeps its partials, and an hour's read holds more of
// them than seriesFoldSpanBudget. A series whose two lines fall in two time chunks lies in one bucket of each chunk's
// read and folds there.
func seriesFoldSpanFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"service_name": "span", "pod": "a"},
		{"service_name": "span", "pod": "b"},
	}}
	first := metricEnd.Add(-seriesFoldSpan - 10*time.Minute)
	for i := 0; ; i++ {
		at := first.Add(time.Duration(i)*2*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
		if !at.Before(metricEnd) {
			break
		}
		req := "r" + strconv.Itoa(i)
		for k, ts := range []time.Time{at, at.Add(70 * time.Second)} {
			if ts.Before(metricEnd) {
				f.Entries = append(f.Entries, metricEntry{Stream: i % 2, TS: ts.UnixNano(), Metadata: map[string]string{"detected_level": "info"},
					Line: seriesFoldLine(strconv.Itoa((i*7919+k*13)%5000+1), "req", req)})
			}
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestSeriesFoldMatchLoki replays the recorded answers through the handler: every run is Loki's answer, read in one
// stats_query_range call that folds the series in one bucket.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestSeriesFoldMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := seriesFoldMetricQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, seriesFoldAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, seriesFoldAnswersPath)
	var b strings.Builder
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			bodies := map[string]string{}
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
				folded := 0
				for _, k := range run.sent {
					if strings.HasPrefix(k, "/select/logsql/stats_query_range?") && strings.Contains(k, "__lvp_ks%3Ain%28") {
						folded++
					}
				}
				calls := statsRangeCalls(run.sent)
				switch chunked := mr.rowLimit > 0; {
				case chunked && (calls < 2 || folded != calls):
					t.Errorf("%s %s %s: %d stats_query_range reads (%d folded), want folded chunks", prof.name, mr.kind, mq.q, calls, folded)
				case !chunked && (calls != 1 || folded != 1):
					t.Errorf("%s %s %s: %d stats_query_range reads (%d folded), want one folded read", prof.name, mr.kind, mq.q, calls, folded)
				}
				// A chunked run answers the one read's answer byte for byte.
				result := listedChunksResult(t, run.body)
				if one, ok := bodies[strings.TrimSuffix(mr.kind, " chunked")]; ok && one != result {
					t.Errorf("%s %s %s: the chunked answer is not the one-read answer", prof.name, mr.kind, mq.q)
				}
				bodies[mr.kind] = result
			}
		}
		b.WriteString("\n  " + mq.q)
	}
	t.Logf("a parser's series identity, folded, against Loki:%s", b.String())
}
