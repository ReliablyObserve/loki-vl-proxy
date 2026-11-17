package querycorpus_test

import (
	"encoding/json"
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

// The Loki differential of an extraction list that names a label of many values, over a long range at a small step:
// real Loki (the engine oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and
// v1.49.0) over the same data, through the proxy's real query_range handler.
//
// The series are the stream and the listed user, a few thousand of them, so the windows of a range longer than the
// step are read in time chunks when one read may hold fewer series than the request reads
// (internal/proxy/listed_window_chunks.go):
//   - Data (listedChunksFixture): logfmt lines of an amount and one of 2,500 users on four streams of two namespaces,
//     a line every three seconds over six hours and the range before them.
//   - Requests: query_range over six hours at a step of 15 s (one read of 1,460 buckets) and of 7 s (a range of five
//     minutes and that step share one-second buckets: 21,900 of them, more than one read may ask for, so read in
//     chunks); each again on a proxy whose reads hold at most 600 series before the fold (-manual-range-metric-row-limit),
//     which reads it in tens of chunks.
//
// Every run must match Loki, the chunked runs answer byte for byte what the run of the default bound answers, and the
// runs read in the chunks named. The answers are recorded (listedChunksAnswersPath; LVP_RUN_ORACLES=1 records them
// again).

const listedChunksAnswersPath = "testdata/listed-window-chunks-loki-diff.jsonl.gz"

// listedChunksBudget is the bound of the chunked runs: more than Loki's default series limit (the bound of a read
// that folds afterwards is the row limit then), less than the series of the request.
const listedChunksBudget = 600

// listedChunksQueries are the shapes: sum, avg and max over the functions that merge from buckets, by a stream label
// or none.
var listedChunksQueries = []string{
	`sum(max_over_time({app="xc"} | logfmt amount, user | unwrap amount [5m]))`,
	`sum by (ns) (stdvar_over_time({app="xc"} | logfmt amount, user | unwrap amount [5m]))`,
	`avg(avg_over_time({app="xc"} | logfmt amount, user | unwrap amount [5m]))`,
	`avg by (ns) (stddev_over_time({app="xc"} | logfmt amount, user | unwrap amount [5m]))`,
	`max by (ns) (min_over_time({app="xc"} | logfmt amount, user | unwrap amount [5m]))`,
}

// listedChunksSpan is the window of the requests.
const listedChunksSpan = 6 * time.Hour

// listedChunksRequests are the requests of q: each step at the default bound and at listedChunksBudget.
func listedChunksRequests(q string) []metricRequest {
	var out []metricRequest
	for _, step := range []time.Duration{15 * time.Second, 7 * time.Second} {
		end := alignDown(metricEnd, step)
		start := alignDown(metricEnd.Add(-listedChunksSpan), step)
		form := url.Values{"query": {q}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)},
			"step": {strconv.FormatInt(int64(step/time.Second), 10)}}
		route := lower.RouteRequest{Step: step, Span: end.Sub(start), End: end}
		name := "6h step=" + step.String()
		out = append(out, metricRequest{kind: name, form: form, route: route},
			metricRequest{kind: name + " chunked", form: form, route: route, rowLimit: listedChunksBudget})
	}
	return out
}

// listedChunksFixture is the data of every query: a line every three seconds, on the four streams in turn.
func listedChunksFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"app": "xc", "ns": "x", "pod": "a", "service_name": "xc"},
		{"app": "xc", "ns": "x", "pod": "b", "service_name": "xc"},
		{"app": "xc", "ns": "y", "pod": "a", "service_name": "xc"},
		{"app": "xc", "ns": "y", "pod": "c", "service_name": "xc"},
	}}
	first := metricEnd.Add(-listedChunksSpan - 10*time.Minute)
	const every = 3 * time.Second
	for i := 0; ; i++ {
		// Half a millisecond past a millisecond: every bound and step is whole seconds, so no line sits on an edge.
		at := first.Add(time.Duration(i)*every + time.Duration(i*7919%2000)*time.Millisecond + 500*time.Microsecond)
		if !at.Before(metricEnd) {
			break
		}
		s := i % len(f.Streams)
		amount := strconv.FormatFloat(float64((i*7919)%2000+1)/4, 'f', -1, 64)
		user := "u" + strconv.Itoa((i*104729)%2500)
		f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: "amount=" + amount + " user=" + user + " msg=paid",
			Metadata: map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}})
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// listedChunksMetricQueries are the differential's queries over listedChunksFixture.
func listedChunksMetricQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := listedChunksFixture()
	var out []metricQuery
	for _, q := range listedChunksQueries {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: listedChunksRequests(q)})
	}
	return out
}

// listedChunksResult is an answer's result, byte for byte (its query statistics, which time the run, left out).
func listedChunksResult(t *testing.T, body string) string {
	t.Helper()
	var resp struct {
		Data struct {
			Result json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil || len(resp.Data.Result) == 0 {
		t.Fatalf("not a query answer (%v): %.300s", err, body)
	}
	return string(resp.Data.Result)
}

// statsRangeCalls counts the stats_query_range requests among the sent request keys.
func statsRangeCalls(sent []string) int {
	n := 0
	for _, k := range sent {
		if strings.HasPrefix(k, "/select/logsql/stats_query_range?") {
			n++
		}
	}
	return n
}

// TestListedWindowChunksMatchLoki replays the recorded answers through the handler: every run is Loki's answer, a
// chunked run's answer is the one-read run's byte for byte, and the runs read in the chunks named above.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestListedWindowChunksMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := listedChunksMetricQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, listedChunksAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, listedChunksAnswersPath)
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
				calls := statsRangeCalls(run.sent)
				single := strings.HasSuffix(mr.kind, "15s")
				switch {
				case single && calls != 1:
					t.Errorf("%s %s %s: %d stats_query_range reads, want one", prof.name, mr.kind, mq.q, calls)
				case !single && calls < 2:
					t.Errorf("%s %s %s: %d stats_query_range read, want chunks", prof.name, mr.kind, mq.q, calls)
				}
				result := listedChunksResult(t, run.body)
				if base, ok := bodies[strings.TrimSuffix(mr.kind, " chunked")]; ok && base != result {
					t.Errorf("%s %s %s: the chunked answer is not the one-read answer", prof.name, mr.kind, mq.q)
				}
				bodies[mr.kind] = result
				if prof.name == diffProfiles()[0].name {
					b.WriteString("\n  " + mr.kind + " " + strconv.Itoa(calls) + " reads: " + mq.q)
				}
			}
		}
	}
	t.Logf("long ranges of an extraction list of many values against Loki:%s", b.String())
}
