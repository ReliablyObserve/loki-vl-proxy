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

// The Loki differential of stddev_over_time and stdvar_over_time over an unwrapped label: real Loki (the engine
// oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and v1.49.0) over the
// same data, through the proxy's real query and query_range handlers, as the metric differential runs them
// (plan_loki_vl_metric_diff_test.go), with data of its own: the corpus fixture holds a few unwrapped values per
// selector, too few for a window to hold several step buckets with several samples each, which is what a sliding
// window merges.
//
//   - Data (stddevFixture): json and logfmt lines of a numeric duration_ms on three streams each, two of app=api and
//     one of app=web (each with its service_name, which Loki would otherwise give it at ingest), a line every 23 seconds per stream over the request window and the range before it, with a
//     ten-minute stretch without lines on the web streams (absent points, windows of one sample).
//   - Requests (metricRequests): an instant query, and query_range with the step equal to the range, a fifth of it
//     and three times it, both bounds on the step's grid.
//   - Shapes: each function by a label of the range aggregation, bare, with no parser, under sum, max, avg, topk
//     and sort_desc, an aggregation over a range by, a binary operation with a number and with the other function,
//     label_replace, an offset, a one-minute range, and the outer sum with and without keep.
//
// The answers are recorded (stddevAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match Loki, or
// be listed in stddevKnown with the registry case that holds the difference open; a listed run that matches fails
// too, so the list only shrinks.

const stddevAnswersPath = "testdata/stddev-stdvar-loki-diff.jsonl.gz"

// stddevShapes are the query shapes, %s the function.
var stddevShapes = []string{
	`%s({job="json"} | json | unwrap duration_ms [5m]) by (app)`,
	`%s({job="logfmt"} | logfmt | unwrap duration_ms [5m]) by (app)`,
	`%s({job="json"} | json | unwrap duration_ms [5m]) by (app, pod)`,
	`%s({job="json"} | json | unwrap duration_ms [5m])`,
	`%s({job="json"} | unwrap duration_ms [5m])`,
	`%s({job="json"} | unwrap duration_ms [5m]) by (app)`,
	`max by (app) (%s({job="logfmt"} | logfmt | unwrap duration_ms [5m]))`,
	`sum by (app) (%s({job="json"} | json | keep app, duration_ms | unwrap duration_ms [5m]))`,
	`sum by (app) (%s({job="json"} | json | unwrap duration_ms [5m]))`,
	`sum(%s({job="json"} | json | unwrap duration_ms [5m]))`,
	`%s({job="json"} | json | unwrap duration_ms [5m]) by (app) * 2`,
	`topk(1, %s({job="json"} | json | unwrap duration_ms [5m]) by (app))`,
	`label_replace(%s({job="json"} | json | unwrap duration_ms [5m]) by (app), "svc", "$1", "app", "(.*)")`,
	`sort_desc(%s({job="json"} | json | unwrap duration_ms [5m]) by (app))`,
	`%s({job="json"} | json | unwrap duration_ms [5m] offset 10m) by (app)`,
	`%s({job="json"} | json | unwrap duration_ms [1m]) by (app)`,
	`avg by (app) (%s({job="logfmt"} | logfmt | unwrap duration_ms [5m]))`,
	`sum by (app) (%s({job="json"} | json | unwrap duration_ms [5m]) by (app, pod))`,
	`max(%s({job="json"} | json | unwrap duration_ms [5m]) by (pod))`,
}

// stddevExtraQueries are queries of both functions.
var stddevExtraQueries = []string{
	`stdvar_over_time({job="json"} | json | unwrap duration_ms [5m]) by (app) / stddev_over_time({job="json"} | json | unwrap duration_ms [5m]) by (app)`,
}

// stddevKnown are the runs that differ from Loki, by query and request kind, with the registry case holding the
// difference open.
var stddevKnown = func() map[string]map[string]string {
	all := []string{"instant", "step=range", "step<range", "step>range"}
	known := map[string]map[string]string{}
	add := func(q, id string, kinds ...string) {
		known[q] = map[string]string{}
		for _, k := range kinds {
			known[q][k] = id
		}
	}
	// The group's pooled samples, where Loki aggregates the deviation or variance of each series: the translator
	// groups the stats by the outer labels (stdvar under any outer by, stddev under sum by).
	const pooled = "semantics/stdvar-grouped-pooled-variance"
	add(`sum by (app) (stddev_over_time({job="json"} | json | unwrap duration_ms [5m]))`, pooled, all...)
	add(`sum by (app) (stdvar_over_time({job="json"} | json | unwrap duration_ms [5m]))`, pooled, all...)
	add(`max by (app) (stdvar_over_time({job="logfmt"} | logfmt | unwrap duration_ms [5m]))`, pooled, all...)
	add(`avg by (app) (stdvar_over_time({job="logfmt"} | logfmt | unwrap duration_ms [5m]))`, pooled, all...)
	// An ungrouped sum over a parser query reads each line as a series on a range query (0 where Loki sums the
	// streams' deviations); the instant query matches.
	const perLine = "semantics/unwrap-per-series-identity-raw-rows"
	add(`sum(stddev_over_time({job="json"} | json | unwrap duration_ms [5m]))`, perLine, all[1:]...)
	add(`sum(stdvar_over_time({job="json"} | json | unwrap duration_ms [5m]))`, perLine, all[1:]...)
	// An aggregation over a range aggregation with a by of its own: not applied (the series of the range by), or on
	// a sliding window applied over the step buckets.
	const rangeBy = "semantics/aggregation-over-range-by-not-applied"
	for _, fn := range []string{"stddev_over_time", "stdvar_over_time"} {
		add(`sum by (app) (`+fn+`({job="json"} | json | unwrap duration_ms [5m]) by (app, pod))`, rangeBy, all...)
		add(`max(`+fn+`({job="json"} | json | unwrap duration_ms [5m]) by (pod))`, rangeBy, "instant", "step<range")
	}
	return known
}()

// stddevQueries are the differential's queries over stddevFixture.
func stddevQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := stddevFixture()
	var out []metricQuery
	for _, fn := range []string{"stddev_over_time", "stdvar_over_time"} {
		for _, shape := range stddevShapes {
			q := fmt.Sprintf(shape, fn)
			p, err := planOfText(q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
		}
	}
	for _, q := range stddevExtraQueries {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	return out
}

// stddevFixture is the data of every query: six streams, a line of duration_ms every 23 seconds on each.
func stddevFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "json", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "json", "app": "api", "pod": "b", "service_name": "api"},
		{"job": "json", "app": "web", "pod": "c", "service_name": "web"},
		{"job": "logfmt", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "logfmt", "app": "api", "pod": "b", "service_name": "api"},
		{"job": "logfmt", "app": "web", "pod": "c", "service_name": "web"},
	}}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 23 * time.Second
	gapFrom, gapTo := metricEnd.Add(-37*time.Minute), metricEnd.Add(-27*time.Minute)
	for s, labels := range f.Streams {
		for i := 0; ; i++ {
			// Each stream on a phase of its own, half a millisecond past a millisecond (every bound and step is whole
			// milliseconds, so no line sits on a window edge).
			at := first.Add(time.Duration(i)*every + time.Duration(s)*3*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			if labels["app"] == "web" && !at.Before(gapFrom) && at.Before(gapTo) {
				continue
			}
			v := strconv.FormatFloat(float64((i*7919+s*104729)%5000)+0.25*float64(i%4), 'f', -1, 64)
			line := `{"duration_ms":` + v + `}`
			if labels["job"] == "logfmt" {
				line = "duration_ms=" + v
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line,
				Metadata: map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestStddevStdvarMatchLoki replays the recorded answers through the handlers and holds every run to Loki's answer,
// or to its listed difference.
//
// conformance: semantics/stddev-stdvar-over-time-range-window, semantics/stddev-stdvar-over-time-step-bucket, semantics/stdvar-grouped-binary-marker, semantics/stdvar-grouped-pooled-variance, semantics/unwrap-per-series-identity-raw-rows, semantics/aggregation-over-range-by-not-applied
func TestStddevStdvarMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := stddevQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, stddevAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, stddevAnswersPath)
	type row struct{ match, runs int }
	table := map[string]*row{}
	seenKnown := map[string]bool{}
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
				known := stddevKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from stddevKnown (%s)", prof.name, mr.kind, mq.q, known)
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
	for q, kinds := range stddevKnown {
		for kind := range kinds {
			if !seenKnown[q+"\x00"+kind] {
				t.Errorf("stddevKnown lists %s %s, which the differential does not run", kind, q)
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
		fmt.Fprintf(&b, "\n  %-32s %3d of %3d match Loki", k, table[k].match, table[k].runs)
	}
	t.Logf("stddev and stdvar over time against Loki:%s", b.String())
}

// answerRequests are the request keys that decide an answer: all but the background refresh of the stream label
// names (refreshStreamLabelNames), which lists the minute before the wall clock, so no recording holds it, and whose
// answer a fresh proxy does not wait for (it answers from no names, as every run here does).
func answerRequests(keys []string) []string {
	var out []string
	for _, k := range keys {
		if strings.HasPrefix(k, "/select/logsql/stream_field_names?") && strings.Contains(k, "&query=%2A&") {
			continue
		}
		out = append(out, k)
	}
	return out
}
