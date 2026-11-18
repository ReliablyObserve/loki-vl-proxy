package querycorpus_test

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// The Loki differential of first_over_time and last_over_time over an unwrapped label when the range is longer than
// the step: real Loki (the engine oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle, test/oracle/vl,
// v1.54.0 and v1.49.0) over the same data, through the proxy's real query and query_range handlers, as the metric
// differential runs them (plan_loki_vl_metric_diff_test.go), with data of its own: sparse lines, so a window's
// latest step can hold no line while its earlier steps do (the case a step bucket per point gets wrong for last).
//
//   - Data (firstLastFixture): json lines of duration_ms on three streams of job=fl (two of app=api, one of
//     app=web), a line every 97 seconds per stream, with a twelve-minute stretch without lines on the web stream;
//     one more web stream of job=fl with a single line (a series of one sample); the same values as the structured
//     metadata latency on three streams of job=sm (no parser: a series per stream); and a stream of job=tie whose
//     lines come in pairs at the same time with different values in both (Loki's and VictoriaLogs' choice among
//     equal times).
//   - Requests (firstLastRequests): an instant query, and query_range with the step equal to the range, a fifth of
//     it, two thirds of it (a range that is not a multiple of the step) and three times it, both bounds on the
//     step's grid.
//   - Shapes: each function bare, under sum, sum by, max and topk, with a range by, an offset, a 90-second range and
//     a binary operation, over a keep before the unwrap and over streams without a parser.
//
// The answers are recorded (firstLastAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match Loki,
// or be listed in firstLastKnown with the registry case that holds the difference open; a listed run that matches
// fails too, so the list only shrinks.

const firstLastAnswersPath = "testdata/first-last-loki-diff.jsonl.gz"

// firstLastShapes are the query shapes, %s the function.
var firstLastShapes = []string{
	`%s({job="fl"} | json | keep app, duration_ms | unwrap duration_ms [5m])`,
	`sum by (app) (%s({job="fl"} | json | keep app, duration_ms | unwrap duration_ms [5m]))`,
	`sum(%s({job="fl"} | json | keep app, duration_ms | unwrap duration_ms [5m]))`,
	`%s({job="fl"} | json | keep app, duration_ms | unwrap duration_ms [5m] offset 10m)`,
	`%s({job="fl"} | json | keep app, duration_ms | unwrap duration_ms [90s])`,
	`%s({job="sm"} | unwrap latency [5m])`,
	`sum by (app) (%s({job="sm"} | unwrap latency [5m]))`,
	`max(%s({job="sm"} | unwrap latency [5m]))`,
	`%s({job="sm"} | unwrap latency [5m]) by (app)`,
	`%s({job="sm"} | unwrap latency [5m] offset 10m)`,
	`topk(1, sum by (app) (%s({job="sm"} | unwrap latency [5m])))`,
	`%s({job="sm"} | unwrap latency [5m]) * 2`,
	`%s({job="tie"} | unwrap latency [5m])`,
	`%s({job="tie"} | json | keep app, duration_ms | unwrap duration_ms [5m])`,
	`%s({job="smi"} | unwrap latency [5m])`,
}

// firstLastKnown are the runs that differ from Loki, by query and request kind, with the registry case holding the
// difference open.
var firstLastKnown = func() map[string]map[string]string {
	known := map[string]map[string]string{}
	for _, fn := range []string{"first_over_time", "last_over_time"} {
		// A whole number stored as a field: VictoriaLogs keeps the column as integers, and the regexp the unwrap gate
		// classifies a value with in a pipe's if condition matches no row of such a column, so the rows are not read.
		q := fn + `({job="smi"} | unwrap latency [5m])`
		known[q] = map[string]string{}
		for _, kind := range []string{"instant", "step=range", "step<range", "step>range", "step<range uneven"} {
			known[q][kind] = "semantics/unwrap-stored-integer-field-gate"
		}
	}
	return known
}()

// firstLastTies are the queries over lines of one series at the same time: Loki takes the first and the last of them
// in its own order, VictoriaLogs keeps the row it meets first, and neither order is one the proxy can read
// (semantics/unwrap-first-last-under-aggregation). Their runs may write another value than Loki's, and must match it
// in everything else: status, series and the points they hold.
var firstLastTies = map[string]bool{
	`first_over_time({job="tie"} | unwrap latency [5m])`:                                    true,
	`last_over_time({job="tie"} | unwrap latency [5m])`:                                     true,
	`first_over_time({job="tie"} | json | keep app, duration_ms | unwrap duration_ms [5m])`: true,
	`last_over_time({job="tie"} | json | keep app, duration_ms | unwrap duration_ms [5m])`:  true,
}

// rowMaxV149 reports a run whose value VictoriaLogs v1.49 may pick wrongly: last_over_time over a field stored with
// the line (no parser), on v1.49. row_max(_time, ...) there takes the value of the block's first row when every
// group key of the block is the same (fixed in v1.50.0), which depends on how the store laid out its blocks, so a
// recording may hold Loki's value or another one (semantics/last-over-time-vl149-row-max-wrong-row). Such a run may
// write another value, and must match Loki in everything else.
func rowMaxV149(version, q string) bool {
	return version == "v1.49.0" && strings.Contains(q, `last_over_time({job="sm"}`)
}

// tieValueOnly reports whether every difference of d is a value: the only one firstLastTies and rowMaxV149 accept.
func tieValueOnly(d metricDiff) bool {
	for c := range d.classes {
		if c != "writes another value" {
			return false
		}
	}
	return true
}

// firstLastRequests are a query's requests: metricRequests, and a query_range whose step is two thirds of the range
// (the windows' buckets are then a third of the range: gcd(step, range)).
func firstLastRequests(q string, r time.Duration) []metricRequest {
	out := metricRequests(q, r)
	for _, mr := range metricRequests(q, r*2/3) {
		if mr.kind == "step=range" {
			mr.kind = "step<range uneven"
			out = append(out, mr)
		}
	}
	return out
}

// firstLastQueries are the differential's queries over firstLastFixture.
func firstLastQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := firstLastFixture()
	var out []metricQuery
	for _, fn := range []string{"first_over_time", "last_over_time"} {
		for _, shape := range firstLastShapes {
			q := fmt.Sprintf(shape, fn)
			p, err := planOfText(q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: firstLastRequests(q, firstRangeOf(p.Root))})
		}
	}
	return out
}

// firstLastFixture is the data of every query.
func firstLastFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "fl", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "fl", "app": "api", "pod": "b", "service_name": "api"},
		{"job": "fl", "app": "web", "pod": "c", "service_name": "web"},
		{"job": "fl", "app": "web", "pod": "d", "service_name": "web"},
		{"job": "sm", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "sm", "app": "api", "pod": "b", "service_name": "api"},
		{"job": "sm", "app": "web", "pod": "c", "service_name": "web"},
		{"job": "tie", "app": "api", "pod": "t", "service_name": "api"},
		{"job": "smi", "app": "api", "pod": "i", "service_name": "api"},
	}}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 97 * time.Second
	gapFrom, gapTo := metricEnd.Add(-41*time.Minute), metricEnd.Add(-29*time.Minute)
	for s, labels := range f.Streams {
		if labels["pod"] == "d" {
			// A series of one sample.
			at := metricEnd.Add(-17*time.Minute - 13*time.Second + 500*time.Microsecond)
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: `{"duration_ms":4242}`,
				Metadata: map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}})
			continue
		}
		for i := 0; ; i++ {
			// Each stream on a phase of its own, half a millisecond past a millisecond (every bound and step is whole
			// milliseconds, so no line sits on a window edge).
			at := first.Add(time.Duration(i)*every + time.Duration(s)*11*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			if labels["pod"] == "c" && !at.Before(gapFrom) && at.Before(gapTo) {
				continue
			}
			pairs := 1
			if labels["job"] == "tie" {
				pairs = 2
			}
			for k := 0; k < pairs; k++ {
				v := strconv.Itoa((i*7919+s*104729+k*613)%5000 + k)
				md := map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}
				line := `{"duration_ms":` + v + `,"seq":` + strconv.Itoa(k) + `}`
				if labels["job"] != "fl" {
					// A fraction keeps the stored column text: VictoriaLogs stores whole numbers as an integer
					// column, whose rows a regexp in a pipe's if condition does not match
					// (semantics/unwrap-stored-integer-field-gate, its shape below).
					md["latency"] = v + ".5"
					if labels["pod"] == "i" {
						md["latency"] = v
					}
					line = "tick " + strconv.Itoa(i) + " " + strconv.Itoa(k)
					if labels["job"] == "tie" {
						line = `{"duration_ms":` + v + `,"seq":` + strconv.Itoa(k) + `}`
					}
				}
				f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line, Metadata: md})
			}
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestFirstLastMatchLoki replays the recorded answers through the handlers and holds every run to Loki's answer, or
// to its listed difference.
//
// conformance: semantics/unwrap-first-last-range-longer-than-step, semantics/unwrap-first-last-range-timestamps, semantics/unwrap-first-last-under-aggregation, semantics/keep-before-unwrap-series-identity, semantics/unwrap-stored-integer-field-gate, semantics/last-over-time-vl149-row-max-wrong-row
func TestFirstLastMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := firstLastQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, firstLastAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, firstLastAnswersPath)
	type row struct{ match, runs int }
	table := map[string]*row{}
	seenKnown := map[string]bool{}
	nonEmpty := 0
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				loki, ok := rec.Loki[mr.kind]
				if !ok {
					t.Fatalf("no Loki answer for %s %s: record them again", mr.kind, mq.q)
				}
				if res, err := decodeMetricBody(loki.Body); loki.Status == 200 && err == nil && len(res.series) > 0 {
					nonEmpty++
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				d := compareMetric(loki, run)
				key := prof.name + " " + mr.kind
				if table[key] == nil {
					table[key] = &row{}
				}
				table[key].runs++
				known := firstLastKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case (firstLastTies[mq.q] || rowMaxV149(prof.version, mq.q)) && tieValueOnly(d):
					table[key].match++
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from firstLastKnown (%s)", prof.name, mr.kind, mq.q, known)
				case len(d.classes) == 0:
					table[key].match++
				case known == "":
					for c, ex := range d.classes {
						t.Errorf("%s %s %s: %s: %s", prof.name, mr.kind, mq.q, c, ex)
					}
				}
			}
		}
	}
	for q, kinds := range firstLastKnown {
		for kind := range kinds {
			if !seenKnown[q+"\x00"+kind] {
				t.Errorf("firstLastKnown lists %s %s, which the differential does not run", kind, q)
			}
		}
	}
	keys := make([]string, 0, len(table))
	for k := range table {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "\n  %-36s %3d of %3d match Loki", k, table[k].match, table[k].runs)
	}
	t.Logf("first and last over time against Loki (%d non-empty Loki answers):%s", nonEmpty, b.String())
	// The data must give Loki something to answer on every kind, or the runs prove nothing.
	if nonEmpty < len(queries)*len(firstLastRequests("", 5*time.Minute))*len(diffProfiles())*3/4 {
		t.Errorf("only %d of the runs have a non-empty Loki answer", nonEmpty)
	}
}
