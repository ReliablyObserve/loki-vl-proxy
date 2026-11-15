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

// The Loki differential of first_over_time and last_over_time with an offset that is not a multiple of the step
// (semantics/first-last-over-time-translation: `last_over_time({...} | logfmt | unwrap latency_ms [1m] offset 2s)
// by (level)` at a 60s step). Real Loki (the engine oracle) and real VictoriaLogs (the serve oracle, v1.53.0 and
// v1.49.0) over the same data, through the proxy's real handlers, as the metric differential runs them.
//
//   - Data (firstLastOffsetFixture): logfmt lines of level and latency_ms on two streams of job=fo, a line every
//     5.3 seconds or so per stream, so most two-second stretches hold a line: a window moved by the offset holds
//     another last and first line than the window without it. A stream of job=fot writes its lines in batches of
//     three at one time, as the compose stack's log generator does (test/e2e-compat/log-generator.py stream()).
//   - Requests: metricRequests (an instant query and query_range at, under and over the range).
//   - Shapes: the case's query, its first_over_time form, an offset that is a multiple of the step and none.
//
// The answers are recorded (firstLastOffsetAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match
// Loki, or be listed in firstLastOffsetKnown with the registry case that holds the difference open.

const firstLastOffsetAnswersPath = "testdata/first-last-offset-loki-diff.jsonl.gz"

// firstLastOffsetShapes are the query shapes, %s the function.
var firstLastOffsetShapes = []string{
	`%s({job="fo"} | logfmt | unwrap latency_ms [1m] offset 2s) by (level)`,
	`%s({job="fo"} | logfmt | unwrap latency_ms [1m] offset 1m) by (level)`,
	`%s({job="fo"} | logfmt | unwrap latency_ms [1m]) by (level)`,
	`%s({job="fo"} | logfmt | unwrap latency_ms [1m] offset 2s)`,
	`%s({job="fot"} | logfmt | unwrap latency_ms [1m] offset 2s) by (level)`,
}

// firstLastOffsetTies reports a query over lines of one series at the same time: Loki takes the first and the last
// of them in the order it was given them, VictoriaLogs keeps the row it meets first, and the proxy can read neither
// order (semantics/unwrap-first-last-under-aggregation). Such a run may write another value than Loki's, and must
// match it in everything else; the test logs how many of its runs do.
func firstLastOffsetTies(q string) bool { return strings.Contains(q, `{job="fot"}`) }

// firstLastOffsetKnown are the runs that differ from Loki, by query and request kind, with the registry case holding
// the difference open.
var firstLastOffsetKnown = map[string]map[string]string{}

// firstLastOffsetFixture is the data of every query.
func firstLastOffsetFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "fo", "pod": "a", "service_name": "payment-service"},
		{"job": "fo", "pod": "b", "service_name": "payment-service"},
		{"job": "fot", "pod": "c", "service_name": "payment-service"},
	}}
	first := metricEnd.Add(-metricWindow - 10*time.Minute)
	for s := range f.Streams {
		for i := 0; ; i++ {
			// Half a millisecond past a millisecond: no line sits on a window edge.
			at := first.Add(time.Duration(i)*5300*time.Millisecond + time.Duration(s)*1700*time.Millisecond + time.Duration(i*7919%600)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			level := []string{"info", "warn"}[(i+s)%2]
			batch := 1
			if f.Streams[s]["job"] == "fot" {
				batch = 3
			}
			for k := 0; k < batch; k++ {
				v := strconv.Itoa((i*7919 + s*104729 + k*613) % 7000)
				f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: "level=" + level + " latency_ms=" + v + " seq=" + strconv.Itoa(k),
					Metadata: map[string]string{"detected_level": level}})
			}
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// firstLastOffsetQueries are the differential's queries over firstLastOffsetFixture.
func firstLastOffsetQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := firstLastOffsetFixture()
	var out []metricQuery
	for _, fn := range []string{"first_over_time", "last_over_time"} {
		for _, shape := range firstLastOffsetShapes {
			q := fmt.Sprintf(shape, fn)
			p, err := planOfText(q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
		}
	}
	return out
}

// TestFirstLastOffsetMatchLoki replays the recorded answers through the handlers and holds every run to Loki's
// answer, or to its listed difference.
//
// conformance: semantics/first-last-over-time-translation
func TestFirstLastOffsetMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := firstLastOffsetQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, firstLastOffsetAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, firstLastOffsetAnswersPath)
	type row struct{ match, runs int }
	tieRuns, tieDiffer := 0, 0
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
				known := firstLastOffsetKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case firstLastOffsetTies(mq.q) && tieValueOnly(d):
					tieRuns++
					if len(d.classes) > 0 {
						tieDiffer++
					} else {
						table[key].match++
					}
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from firstLastOffsetKnown (%s)", prof.name, mr.kind, mq.q, known)
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
	for q, kinds := range firstLastOffsetKnown {
		for kind := range kinds {
			if !seenKnown[q+"\x00"+kind] {
				t.Errorf("firstLastOffsetKnown lists %s %s, which the differential does not run", kind, q)
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
	t.Logf("first and last over time with an offset against Loki (%d non-empty Loki answers; %d of %d runs over lines at one time write another value than Loki's):%s", nonEmpty, tieDiffer, tieRuns, b.String())
	if nonEmpty < len(queries)*len(metricRequests("", time.Minute))*len(diffProfiles())*3/4 {
		t.Errorf("only %d of the runs have a non-empty Loki answer", nonEmpty)
	}
}
