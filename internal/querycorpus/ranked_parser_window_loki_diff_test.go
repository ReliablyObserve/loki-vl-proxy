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

// The Loki differential of a window metric with a parser stage over a short range at a fine step (issue #1795):
// real Loki (the engine oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle, test/oracle/vl, v1.54.0 and
// v1.49.0) over the same data, through the proxy's real query_range handler.
//
//   - Data (rankedParserFixture): access log lines of a reverse proxy in the combined format, the client address the
//     last quoted field, over an hour and the range after it: busy clients with rates far enough apart that no two
//     tie at the cut of a ranking at any step, three thousand clients with one or two lines each, lines the regexp
//     does not match (the series {}), streams whose own peer_ip stream label or structured metadata the regexp or
//     logfmt extracts too (Loki's peer_ip_extracted); JSON lines of a third of them on a stream of their own, and half
//     of those with lines that are not JSON on another.
//   - Queries (rankedParserQueries): topk over sum by (client_ip) of count_over_time, rate and bytes_over_time with a
//     regexp stage, a topk and a sum with a pattern stage over the lines it matches whole (and a sum over every line,
//     a known difference), a sum by (peer_ip) without a ranking and a topk of three by it, sums by the extracted name a line
//     already holds (rankedParserKnown at the whole-second steps), and topk over `| json` with no error filter, with
//     `__error__=""` and with `drop __error__`, over valid JSON and over lines that are not JSON (Loki's 400).
//   - Requests (rankedParserRequests): a range of 80 s at the steps Grafana picks for it (53 ms: a window no bucket
//     grid of 11,000 points holds; 50 ms: a step that divides the window, still past the bound), at 1 s, and at 10 s
//     (a grid within the bound); each again on a proxy that reads at most 8,000 rows per read
//     (-manual-range-metric-row-limit), fewer than the hour holds, so a read of every line of the window fails, and as
//     Logs Drilldown sends it (its X-Query-Tags source); rankedParserChunked also at 300 rows per read (time chunks).
//
// Every run must match Loki. The answers are recorded (rankedParserAnswersPath; LVP_RUN_ORACLES=1 records them again,
// with GOMODCACHE set to a fresh directory when the shared module cache holds other copies of VictoriaLogs).

const rankedParserAnswersPath = "testdata/ranked-parser-window-loki-diff.jsonl.gz"

// rankedParserRowLimit is the row bound of the bounded runs: fewer rows than the hour of lines, more than the window
// reads of the recurrence hold (the hour's clients, and the buckets of the 80 s twice).
const rankedParserRowLimit = 8000

const rankedParserRegexp = "`(?P<peer_ip>\\S+) - - .* \"(?P<client_ip>[^\"]+)\"$`"

// rankedParserPattern reads the same fields with a pattern stage (VictoriaLogs' extract).
const rankedParserPattern = "`<peer_ip> - - <_> \"<_>\" <_> <_> \"<_>\" \"<_>\" \"<client_ip>\"`"

// rankedParserQueries are the shapes.
var rankedParserQueries = []string{
	`topk(20, sum by (client_ip) (count_over_time({app="edge-gateway"} | regexp ` + rankedParserRegexp + ` [1h])))`,
	`topk(5, sum by (client_ip) (rate({app="edge-gateway"} | regexp ` + rankedParserRegexp + ` [1h])))`,
	`topk(20, sum by (client_ip) (bytes_over_time({app="edge-gateway"} | regexp ` + rankedParserRegexp + ` [1h])))`,
	`sum by (peer_ip) (count_over_time({app="edge-gateway"} | regexp ` + rankedParserRegexp + ` [1h]))`,
	`topk(19, sum by (client_ip) (count_over_time({app="edge-gateway"} |= "agent/1.0" | pattern ` + rankedParserPattern + ` [1h])))`,
	`sum by (peer_ip) (bytes_over_time({app="edge-gateway"} |= "agent/1.0" | pattern ` + rankedParserPattern + ` [1h]))`,
	`sum by (peer_ip) (bytes_over_time({app="edge-gateway"} | pattern ` + rankedParserPattern + ` [1h]))`,
	rankedParserChunked,
	`sum by (peer_ip) (count_over_time({app=~"edge-(gateway|collide)"} | regexp ` + rankedParserRegexp + ` [1h]))`,
	`sum by (peer_ip) (count_over_time({app="edge-collide-sm"} | regexp ` + rankedParserRegexp + ` [1h]))`,
	`sum by (peer_ip) (count_over_time({app="edge-collide-logfmt"} | logfmt [1h]))`,
	`topk(19, sum by (client_ip) (count_over_time({app="edge-json"} | json [1h])))`,
	`topk(19, sum by (client_ip) (count_over_time({app="edge-json"} | json | __error__="" [1h])))`,
	`topk(19, sum by (client_ip) (count_over_time({app="edge-json"} | json | drop __error__ [1h])))`,
	`topk(19, sum by (client_ip) (count_over_time({app=~"edge-json.*"} | json [1h])))`,
	`topk(19, sum by (client_ip) (count_over_time({app=~"edge-json.*"} | json | __error__="" [1h])))`,
}

// rankedParserChunked is the query also run at rankedParserChunkRows rows per read: a grouping of a few values whose
// windows' buckets at the fine steps hold more rows than that, so its steps are ranked in time chunks
// (internal/proxy sliding_window_chunks.go).
const rankedParserChunked = `topk(3, sum by (peer_ip) (count_over_time({app="edge-gateway"} | regexp ` + rankedParserRegexp + ` [1h])))`

// rankedParserChunkRows is the row bound of the chunked runs.
const rankedParserChunkRows = 300

// rankedParserSpan is the range of the requests.
const rankedParserSpan = 80 * time.Second

// rankedParserRequests are the requests of q: each step at the default bounds and at rankedParserRowLimit rows.
func rankedParserRequests(q string) []metricRequest {
	var out []metricRequest
	for _, step := range []time.Duration{53 * time.Millisecond, 50 * time.Millisecond, time.Second, 10 * time.Second} {
		end := alignDown(metricEnd, step)
		start := alignDown(metricEnd.Add(-rankedParserSpan), step)
		form := url.Values{"query": {q}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)}}
		if step%time.Second == 0 {
			form.Set("step", strconv.FormatInt(int64(step/time.Second), 10))
		} else {
			form.Set("step", strconv.FormatInt(step.Milliseconds(), 10)+"ms")
		}
		route := lower.RouteRequest{Step: step, Span: end.Sub(start), End: end}
		name := "80s step=" + step.String()
		out = append(out, metricRequest{kind: name, form: form, route: route},
			metricRequest{kind: name + " bounded", form: form, route: route, rowLimit: rankedParserRowLimit},
			metricRequest{kind: name + " drilldown", form: form, route: route, drilldown: true})
		if q == rankedParserChunked && step < time.Second {
			out = append(out, metricRequest{kind: name + " chunked", form: form, route: route, rowLimit: rankedParserChunkRows})
		}
	}
	return out
}

// rankedParserFixture is the data of every query.
func rankedParserFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"app": "edge-gateway", "service_name": "edge-gateway"},
		{"app": "edge-json", "service_name": "edge-json"},
		// A stream label the regexp extracts too: Loki names the extracted label peer_ip_extracted.
		{"app": "edge-collide", "service_name": "edge-collide", "peer_ip": "10.9.9.9"},
		// JSON lines and lines that are not JSON: a parser error for every query that neither filters nor drops it.
		{"app": "edge-json-mixed", "service_name": "edge-json-mixed"},
		// The extracted name as structured metadata, and logfmt lines whose key is a stream label.
		{"app": "edge-collide-sm", "service_name": "edge-collide-sm"},
		{"app": "edge-collide-logfmt", "service_name": "edge-collide-logfmt", "peer_ip": "10.9.9.8"},
	}}
	first := metricEnd.Add(-rankedParserSpan - time.Hour - time.Minute)
	span := metricEnd.Sub(first)
	spread := func(n, i, salt int) (time.Time, bool) {
		// Spread over the span, off every millisecond edge (half a microsecond past one).
		off := (int64(span) / int64(n)) * int64(i)
		off += int64(((i+1)*7919+salt*104729)%997) * int64(time.Millisecond)
		off -= off % int64(time.Millisecond)
		at := first.Add(time.Duration(off) + 500*time.Microsecond)
		return at, at.Before(metricEnd)
	}
	add := func(stream int, client string, n, salt int) {
		for i := 0; i < n; i++ {
			at, ok := spread(n, i, salt)
			if !ok {
				continue
			}
			// Four peers at 40, 30, 20 and 10 percent of the lines: no two tie at the cut of a ranking.
			peer := "10.0.0." + strconv.Itoa([10]int{1, 1, 1, 1, 2, 2, 2, 3, 3, 4}[(i*7+salt)%10])
			size := strconv.Itoa(100 + (i*31+salt)%900)
			line := peer + ` - - [01/Jan/2026:00:00:00 +0000] "GET /p/` + strconv.Itoa(i%17) + ` HTTP/1.1" 200 ` + size + ` "-" "agent/1.0" "` + client + `"`
			f.Entries = append(f.Entries, metricEntry{Stream: stream, TS: at.UnixNano(), Line: line})
			if stream == 0 && i%3 == 0 {
				js := `{"peer_ip":"` + peer + `","client_ip":"` + client + `","status":200,"bytes":` + size + `}`
				f.Entries = append(f.Entries, metricEntry{Stream: 1, TS: at.Add(time.Millisecond).UnixNano(), Line: js})
				if i%2 == 0 {
					f.Entries = append(f.Entries, metricEntry{Stream: 3, TS: at.Add(2 * time.Millisecond).UnixNano(), Line: js})
				}
			}
		}
	}
	// Five clients far apart (the cut of a topk of five), fourteen more (the cut of twenty, with the lines the
	// regexp does not match: the series {}), ten at a fraction of the twentieth, three thousand of one or two lines.
	for j := 0; j < 5; j++ {
		add(0, "203.0.113."+strconv.Itoa(j+1), 1600-100*j, j)
	}
	for j := 0; j < 14; j++ {
		add(0, "203.0.113."+strconv.Itoa(j+6), 1000-30*j, 5+j)
	}
	for j := 0; j < 10; j++ {
		add(0, "198.51.100."+strconv.Itoa(j+1), 200, 20+j)
	}
	for j := 0; j < 3000; j++ {
		add(0, "c"+strconv.Itoa(j), 1+j%2, 30+j)
	}
	add(2, "192.0.2.1", 240, 7)
	for i := 0; i < 300; i++ {
		if at, ok := spread(300, i, 3003); ok {
			peer := "10.0.0." + strconv.Itoa(i%3+1)
			line := peer + ` - - [01/Jan/2026:00:00:00 +0000] "GET / HTTP/1.1" 200 10 "-" "agent/1.0" "c` + strconv.Itoa(i%7) + `"`
			f.Entries = append(f.Entries, metricEntry{Stream: 4, TS: at.UnixNano(), Line: line, Metadata: map[string]string{"peer_ip": "10.8.8.8"}})
			f.Entries = append(f.Entries, metricEntry{Stream: 5, TS: at.Add(time.Millisecond).UnixNano(), Line: "peer_ip=" + peer + " client_ip=c" + strconv.Itoa(i%7)})
		}
	}
	// Health checks the regexp does not match, and lines whose last quoted field is empty (no match either).
	for i := 0; i < 700; i++ {
		if at, ok := spread(700, i, 3001); ok {
			line := `GET /health 200`
			if i%5 == 0 {
				line = `10.0.0.1 - - [01/Jan/2026:00:00:00 +0000] "GET / HTTP/1.1" 200 0 "-" "probe" ""`
			}
			f.Entries = append(f.Entries, metricEntry{Stream: 0, TS: at.UnixNano(), Line: line})
		}
	}
	// Lines that are not JSON on the mixed stream, in the window of every request.
	for i := 0; i < 40; i++ {
		if at, ok := spread(40, i, 3002); ok {
			f.Entries = append(f.Entries, metricEntry{Stream: 3, TS: at.Add(3 * time.Millisecond).UnixNano(), Line: `not json ` + strconv.Itoa(i)})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// rankedParserMetricQueries are the differential's queries over rankedParserFixture.
func rankedParserMetricQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := rankedParserFixture()
	var out []metricQuery
	for _, q := range rankedParserQueries {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: rankedParserRequests(q)})
	}
	return out
}

// windowGridReads counts the stats pipe reads of the window recurrence (internal/proxy sliding_window_stats.go)
// among the sent request keys.
func windowGridReads(sent []string) int {
	n := 0
	for _, k := range sent {
		path, raw, _ := strings.Cut(k, "?")
		if path != "/select/logsql/query" {
			continue
		}
		if form, err := url.ParseQuery(raw); err == nil && strings.Contains(form.Get("query"), "| stats by (_time:") {
			n++
		}
	}
	return n
}

// rankedParserKnown are the queries whose answer differs from Loki's on the bucket reads (the requests at whole
// seconds), by their registry case: an extracted name that a stream label or structured metadata of the line holds
// already is Loki's name_extracted, and the bucket reads group by the extracted value. The window grids read the
// line's own value (the fine steps match Loki). The test fails when a known one matches Loki, so the case is closed.
var rankedParserKnown = map[string]rankedParserKnownCase{
	`sum by (peer_ip) (count_over_time({app=~"edge-(gateway|collide)"} | regexp ` + rankedParserRegexp + ` [1h]))`: {id: "parsed-identity/extracted-name-held-by-the-line"},
	`sum by (peer_ip) (count_over_time({app="edge-collide-sm"} | regexp ` + rankedParserRegexp + ` [1h]))`:         {id: "parsed-identity/extracted-name-held-by-the-line"},
	`sum by (peer_ip) (count_over_time({app="edge-collide-logfmt"} | logfmt [1h]))`:                                {id: "parsed-identity/extracted-name-held-by-the-line"},
	// A line the pattern matches only in part: Loki keeps the captures up to where the match stops (the health
	// check line is all peer_ip), VictoriaLogs' extract none. Every route, every step.
	`sum by (peer_ip) (bytes_over_time({app="edge-gateway"} | pattern ` + rankedParserPattern + ` [1h]))`: {id: "semantics/pattern-capture-label-residual", everyStep: true},
}

// rankedParserKnownCase is a known difference: its registry case, and whether it holds at every step (else at the
// whole-second steps only, the bucket reads).
type rankedParserKnownCase struct {
	id        string
	everyStep bool
}

// rankedParserLokiFails is a query Loki answers with its JSON parser error: a line that is not JSON reaches the
// range aggregation.
func rankedParserLokiFails(q string) bool {
	return strings.Contains(q, `{app=~"edge-json.*"} | json [`)
}

// TestRankedParserWindowMatchesLoki replays the recorded answers through the handler: every run is Loki's answer,
// with and without a row bound the hour of lines passes.
//
// conformance: semantics/ranked-parser-window-fine-step
func TestRankedParserWindowMatchesLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := rankedParserMetricQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, rankedParserAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, rankedParserAnswersPath)
	var b strings.Builder
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				loki, ok := rec.Loki[mr.kind]
				if !ok || loki.Status != 200 && !rankedParserLokiFails(mq.q) {
					t.Fatalf("no Loki answer for %s %s: record them again", mr.kind, mq.q)
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				// A parser window metric at a step no bucket grid of the bound holds is ranked from the window grids,
				// never from stats_query_range buckets or raw rows; a chunked run reads more than the three grids.
				if mr.route.Step < time.Second && !strings.Contains(mq.q, "| json") && run.status == 200 {
					grids, ranges := windowGridReads(run.sent), statsRangeCalls(run.sent)
					switch {
					case ranges != 0 || grids < 3:
						t.Errorf("%s %s %s: %d window grid reads and %d stats_query_range, want the window grids only", prof.name, mr.kind, mq.q, grids, ranges)
					case strings.HasSuffix(mr.kind, " chunked") && grids <= 3:
						t.Errorf("%s %s %s: %d window grid reads, want time chunks", prof.name, mr.kind, mq.q, grids)
					}
				}
				diff := compareMetric(loki, run).classes
				if known, ok := rankedParserKnown[mq.q]; ok && (known.everyStep || mr.route.Step >= time.Second) {
					if len(diff) == 0 {
						t.Errorf("%s %s %s: matches Loki: close %s", prof.name, mr.kind, mq.q, known.id)
					}
					diff = nil
				}
				for c, ex := range diff {
					t.Errorf("%s %s %s: %s: %s", prof.name, mr.kind, mq.q, c, ex)
				}
				if prof.name == diffProfiles()[0].name {
					b.WriteString("\n  " + mr.kind + ": " + strconv.Itoa(run.status) + ", " + strconv.Itoa(statsRangeCalls(run.sent)) + " stats_query_range, " +
						strconv.Itoa(windowGridReads(run.sent)) + " window grid reads, " + strconv.Itoa(len(run.sent)) + " requests: " + mq.q)
				}
			}
		}
	}
	t.Logf("window metrics with a parser stage over a short range against Loki:%s", b.String())
}
