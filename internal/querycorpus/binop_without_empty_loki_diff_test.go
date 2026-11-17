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

// The Loki differential of a binary operation with an operand that has a `without ()` clause: real Loki (the engine
// oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and v1.49.0) over the
// same data, through the proxy's real query and query_range handlers, as the metric differential runs them
// (plan_loki_vl_metric_diff_test.go).
//
//   - Data (emptyWithoutFixture): plain and json lines on four streams of job=we (two pods of app=api, two of
//     app=web), a line every 41 seconds per stream, and one stream of job=one (a single series).
//   - Requests (metricRequests): an instant query, and query_range with the step equal to the range, a fifth of it
//     and three times it, both bounds on the step's grid.
//   - Shapes (emptyWithoutShapes): sum, avg, min and max `without ()` as an operand against a number, vector(0), a
//     grouped and an ungrouped aggregation, with the clause before and after the operand, after a json parser, two
//     levels of it, under sum, label_replace and topk, with an offset, the shapes Loki rejects at run time (a
//     one-to-one match of several series on no label), and a count under the clause.
//
// The answers are recorded (emptyWithoutAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match
// Loki, or be listed in emptyWithoutKnown with the registry case that holds the difference open; a listed run that
// matches fails too, so the list only shrinks.

const emptyWithoutAnswersPath = "testdata/binop-without-empty-loki-diff.jsonl.gz"

// emptyWithoutShapes are the differential's queries.
var emptyWithoutShapes = []string{
	`sum without () (count_over_time({job="we"}[5m])) / 2`,
	`sum(count_over_time({job="we"}[5m])) without () / 2`,
	`2 * sum without () (rate({job="we"}[5m]))`,
	`avg without () (count_over_time({job="we"}[5m])) - 1`,
	`min without () (bytes_over_time({job="we"}[5m])) > 0`,
	`max without () (rate({job="we"}[5m])) > on(app) group_left sum by (app) (rate({job="we"}[5m]))`,
	`sum without () (count_over_time({job="we"}[5m])) / sum without (pod) (count_over_time({job="we"}[5m]))`,
	`sum without () (count_over_time({job="we"}[5m])) / ignoring(pod) group_left sum without (pod) (count_over_time({job="we"}[5m]))`,
	`sum without () (count_over_time({job="we"} | json [5m])) * 2`,
	`sum without () (sum without () (count_over_time({job="we"}[5m]))) / 2`,
	`sum without () (count_over_time({job="one"}[5m])) + on() vector(0)`,
	`sum without () (count_over_time({job="we"}[5m])) + on() vector(0)`,
	`sum without () (count_over_time({job="we"}[5m])) + sum(count_over_time({job="we"}[5m]))`,
	`sum without () (count_over_time({job="we"}[5m])) + on() sum(count_over_time({job="we"}[5m]))`,
	`sum without () (count_over_time({job="we"}[5m])) + on() group_left sum(count_over_time({job="we"}[5m]))`,
	// the wrappers Loki keeps the operation under, and an offset
	`sum(sum without () (count_over_time({job="we"}[5m])) / 2)`,
	`label_replace(sum without () (count_over_time({job="we"}[5m])) / 2, "svc", "$1", "app", "(.*)")`,
	`topk(2, max without () (rate({job="we"}[5m])) * 2)`,
	`sum without () (count_over_time({job="we"}[5m] offset 10m)) / 2`,
	// a count under the clause is 1 for every series, which the route still reads as no clause
	`count without () (count_over_time({job="we"}[5m])) / 2`,
}

// emptyWithoutKnown are the runs that differ from Loki, by query and request kind, with the registry case holding the
// difference open.
var emptyWithoutKnown = func() map[string]map[string]string {
	known := map[string]map[string]string{}
	all := []string{"instant", "step=range", "step<range", "step>range"}
	// Still the handlers' reading, the open part of the case: a ranking over the operation (the post-aggregation
	// route reads the operation as text, not through the binary route), and a count under the clause (1 per series).
	for _, q := range []string{
		`topk(2, max without () (rate({job="we"}[5m])) * 2)`,
		`count without () (count_over_time({job="we"}[5m])) / 2`,
	} {
		known[q] = map[string]string{}
		for _, kind := range all {
			known[q][kind] = "semantics/binary-operand-without-empty-collapses"
		}
	}
	return known
}()

// emptyWithoutQueries are the differential's queries over emptyWithoutFixture.
func emptyWithoutQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := emptyWithoutFixture()
	var out []metricQuery
	for _, q := range emptyWithoutShapes {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	return out
}

// emptyWithoutFixture is the data of every query.
func emptyWithoutFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "we", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "we", "app": "api", "pod": "b", "service_name": "api"},
		{"job": "we", "app": "web", "pod": "c", "service_name": "web"},
		{"job": "we", "app": "web", "pod": "d", "service_name": "web"},
		{"job": "one", "app": "api", "pod": "o", "service_name": "api"},
	}}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 41 * time.Second
	for s := range f.Streams {
		for i := 0; ; i++ {
			// Each stream on a phase of its own, half a millisecond past a millisecond (every bound and step is whole
			// milliseconds, so no line sits on a window edge).
			at := first.Add(time.Duration(i)*every + time.Duration(s)*7*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			line := `{"route":"r` + strconv.Itoa(i%2) + `","n":` + strconv.Itoa((i*31+s)%97) + `}`
			if i%3 == 0 {
				line = "plain line " + strconv.Itoa(i)
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line,
				Metadata: map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// pipelineErrorBySeriesOnly reports a run whose only difference is the series a pipeline error names: Loki's text and
// the proxy's are the same once the series after `for series: '` is cut out of both.
func pipelineErrorBySeriesOnly(d metricDiff, body, lokiError string) bool {
	if _, ok := d.classes["writes another error text"]; !ok || len(d.classes) != 1 {
		return false
	}
	cut := func(s string) (string, bool) {
		s = strings.TrimSpace(s)
		i, j := strings.Index(s, "for series: '"), strings.LastIndex(s, "}'.")
		if i < 0 || j < i {
			return s, false
		}
		return s[:i] + s[j+3:], true
	}
	got, okGot := cut(proxyErrorText(body))
	want, okWant := cut(lokiError)
	return okGot && okWant && got == want
}

// TestEmptyWithoutOperandsMatchLoki replays the recorded answers through the handlers and holds every run to Loki's
// answer, or to its listed difference.
//
// conformance: semantics/binary-operand-without-empty-collapses, semantics/pipeline-error-text-parity
func TestEmptyWithoutOperandsMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := emptyWithoutQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, emptyWithoutAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, emptyWithoutAnswersPath)
	type row struct{ match, runs int }
	table := map[string]*row{}
	seenKnown := map[string]bool{}
	nonEmpty, errors, byShape := 0, 0, 0
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
				if loki.Status != 200 {
					errors++
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
				known := emptyWithoutKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case pipelineErrorBySeriesOnly(d, run.body, loki.Error):
					// Loki names the first failing series its shards meet: compared by shape
					// (semantics/pipeline-error-text-parity).
					table[key].match++
					byShape++
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from emptyWithoutKnown (%s)", prof.name, mr.kind, mq.q, known)
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
	for q, kinds := range emptyWithoutKnown {
		for kind := range kinds {
			if !seenKnown[q+"\x00"+kind] {
				t.Errorf("emptyWithoutKnown lists %s %s, which the differential does not run", kind, q)
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
	t.Logf("binary operations over a without () operand against Loki (%d non-empty Loki answers, %d Loki errors, %d pipeline errors equal but for the series named):%s", nonEmpty, errors, byShape, b.String())
	if nonEmpty < len(queries)*len(metricKinds())*len(diffProfiles())/2 {
		t.Errorf("only %d of the runs have a non-empty Loki answer", nonEmpty)
	}
}
