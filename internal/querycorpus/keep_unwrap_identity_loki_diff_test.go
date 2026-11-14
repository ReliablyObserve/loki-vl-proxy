package querycorpus_test

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The Loki differential of the series an unwrapped range function reads when a keep or a drop stage stands before
// the unwrap: real Loki (the engine oracle, test/oracle/loki) and real VictoriaLogs (the serve oracle,
// test/oracle/vl, v1.53.0 and v1.49.0) over the same data, through the proxy's real query and query_range handlers,
// as the metric differential runs them (plan_loki_vl_metric_diff_test.go).
//
// A Loki series is the set of labels a sample carries after the pipeline: a keep deletes every label it does not
// name (stream labels included), the unwrapped label leaves the sample, and the samples of every line that ends with
// the same labels are one series. A keep can so merge the lines of several streams into one series, which first,
// last, quantile, stddev, stdvar, avg, max, sum and rate_counter over time then read as one.
//
//   - Data (keepIdentityFixture): json lines on four streams, two pods of app=api, one of app=web and one of
//     app=web on another job, a line every 17 seconds per stream, each stream on a phase of its own (no two lines at
//     the same time, so first and last are decided). Every line has a key of its own (req), so without a keep each
//     line is a Loki series of its own, a key every line shares (route), and a key only some lines have (region).
//   - Requests (metricRequests): an instant query, and query_range with the step equal to the range, a fifth of it
//     and three times it.
//   - Shapes: each function bare, under sum by (app), under sum and under max by (app), over a keep of app and the
//     unwrapped label and over the same pipeline without a keep; a keep of the unwrapped label alone, a keep of a
//     label some lines lack, a keep before the parser, and a drop of a stream label.
//
// The answers are recorded (keepIdentityAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match
// Loki, or be listed in keepIdentityKnown with the registry case that holds the difference open; a listed run that
// matches fails too, so the list only shrinks.

const keepIdentityAnswersPath = "testdata/keep-unwrap-identity-loki-diff.jsonl.gz"

// keepIdentityFuncs are the range functions of the differential.
var keepIdentityFuncs = []string{
	"first_over_time", "last_over_time", "quantile_over_time", "stddev_over_time", "stdvar_over_time",
	"avg_over_time", "max_over_time", "min_over_time", "sum_over_time", "rate_counter", "rate",
}

// keepIdentityShapes are the query shapes, %s the function call's head (`quantile_over_time(0.9, ` or
// `sum_over_time(`).
var keepIdentityShapes = []string{
	`%s{job="kj"} | json | keep app, duration_ms | unwrap duration_ms [5m])`,
	`sum by (app) (%s{job="kj"} | json | keep app, duration_ms | unwrap duration_ms [5m]))`,
	`sum(%s{job="kj"} | json | keep app, duration_ms | unwrap duration_ms [5m]))`,
	`max by (app) (%s{job="kj"} | json | keep app, duration_ms | unwrap duration_ms [5m]))`,
	`sum by (app) (%s{job="kj"} | json | unwrap duration_ms [5m]))`,
	`sum(%s{job="kj"} | json | unwrap duration_ms [5m]))`,
}

// keepIdentityEdgeShapes are run for first_over_time, quantile_over_time and stddev_over_time.
var keepIdentityEdgeShapes = []string{
	`%s{job="kj"} | json | keep duration_ms | unwrap duration_ms [5m])`,
	`%s{job="kj"} | json | keep app, region, duration_ms | unwrap duration_ms [5m])`,
	`sum by (app) (%s{job="kj"} | keep app | json | unwrap duration_ms [5m]))`,
	`%s{job="kj"} | json duration_ms | drop pod | unwrap duration_ms [5m])`,
	`%s{job=~"kj|kx"} | json duration_ms | keep app, duration_ms | unwrap duration_ms [5m])`,
	`max without (app) (%s{job="kj"} | json | keep app, duration_ms | unwrap duration_ms [5m]))`,
	`%s{job="kj"} | json | keep app, duration_ms | unwrap duration_ms [5m]) by (app)`,
}

// keepIdentityKnown are the runs that differ from Loki, by query and request kind, with the registry case that holds
// the difference open.
var keepIdentityKnown = func() map[string]map[string]string {
	all := []string{"instant", "step=range", "step<range", "step>range"}
	known := map[string]map[string]string{}
	add := func(id string, kinds []string, queries ...string) {
		for _, q := range queries {
			known[q] = map[string]string{}
			for _, k := range kinds {
				known[q][k] = id
			}
		}
	}
	keep := func(fn string) string {
		return keepIdentityHead(fn) + `{job="kj"} | json | keep app, duration_ms | unwrap duration_ms [5m])`
	}
	plain := func(fn string) string {
		return keepIdentityHead(fn) + `{job="kj"} | json | unwrap duration_ms [5m])`
	}
	// first and last over time on a range longer than the step read one step, with or without a keep.
	sliding := []string{"step<range"}
	for _, fn := range []string{"first_over_time", "last_over_time"} {
		add("semantics/unwrap-first-last-range-longer-than-step", sliding, "sum by (app) ("+plain(fn)+")", "sum("+plain(fn)+")")
	}
	add("semantics/unwrap-first-last-range-longer-than-step", sliding,
		keep("first_over_time"), "sum by (app) ("+keep("first_over_time")+")", "sum("+keep("first_over_time")+")",
		"max by (app) ("+keep("first_over_time")+")", "max without (app) ("+keep("first_over_time")+")", keep("first_over_time")+" by (app)",
		`first_over_time({job="kj"} | json | keep duration_ms | unwrap duration_ms [5m])`,
		`first_over_time({job="kj"} | json | keep app, region, duration_ms | unwrap duration_ms [5m])`,
		`first_over_time({job=~"kj|kx"} | json duration_ms | keep app, duration_ms | unwrap duration_ms [5m])`,
		`sum by (app) (first_over_time({job="kj"} | keep app | json | unwrap duration_ms [5m]))`)
	// Without a keep each line is a Loki series of its own; the proxy reads a value per stream (or the group's).
	const perSeries = "semantics/unwrap-per-series-identity-raw-rows"
	add(perSeries, all, "sum("+plain("quantile_over_time")+")", "sum by (app) ("+plain("avg_over_time")+")",
		"sum by (app) ("+plain("max_over_time")+")", "sum by (app) ("+plain("min_over_time")+")", "sum by (app) ("+plain("rate_counter")+")",
		"sum("+plain("rate_counter")+")")
	add(perSeries, []string{"instant"}, "sum("+plain("stddev_over_time")+")", "sum("+plain("stdvar_over_time")+")")
	add(perSeries, []string{"instant", "step<range"}, "sum("+plain("avg_over_time")+")", "sum("+plain("max_over_time")+")", "sum("+plain("min_over_time")+")")
	add("semantics/unwrap-quantile-sum-by-group-quantile", all, "sum by (app) ("+plain("quantile_over_time")+")")
	add("semantics/stdvar-grouped-pooled-variance", all, "sum by (app) ("+plain("stddev_over_time")+")", "sum by (app) ("+plain("stdvar_over_time")+")")
	// A keep before the parser: the series are the kept labels and the parsed ones.
	add("semantics/keep-before-parser-unwrap-identity", all,
		`sum by (app) (quantile_over_time(0.9, {job="kj"} | keep app | json | unwrap duration_ms [5m]))`,
		`sum by (app) (stddev_over_time({job="kj"} | keep app | json | unwrap duration_ms [5m]))`)
	// A drop of a stream label before the unwrap: Loki's series lose the label.
	add("semantics/drop-before-unwrap-stream-label", all,
		`first_over_time({job="kj"} | json duration_ms | drop pod | unwrap duration_ms [5m])`,
		`quantile_over_time(0.9, {job="kj"} | json duration_ms | drop pod | unwrap duration_ms [5m])`,
		`stddev_over_time({job="kj"} | json duration_ms | drop pod | unwrap duration_ms [5m])`)
	// rate_counter's series are right; its value differs from Loki's extrapolated rate in the sixth digit.
	add("semantics/unwrap-rate-counter-window-extrapolation", all, keep("rate_counter"), "sum by (app) ("+keep("rate_counter")+")",
		"sum("+keep("rate_counter")+")", "max by (app) ("+keep("rate_counter")+")")
	return known
}()

// keepIdentityHead is the call head of a function: its name and an opening parenthesis, and quantile_over_time's
// parameter.
func keepIdentityHead(fn string) string {
	if fn == "quantile_over_time" {
		return fn + "(0.9, "
	}
	return fn + "("
}

// keepIdentityQueries are the differential's queries over keepIdentityFixture.
func keepIdentityQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := keepIdentityFixture()
	var out []metricQuery
	add := func(q string) {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	for _, fn := range keepIdentityFuncs {
		for _, shape := range keepIdentityShapes {
			add(fmt.Sprintf(shape, keepIdentityHead(fn)))
		}
		switch fn {
		case "first_over_time", "quantile_over_time", "stddev_over_time":
			for _, shape := range keepIdentityEdgeShapes {
				add(fmt.Sprintf(shape, keepIdentityHead(fn)))
			}
		}
	}
	return out
}

// keepIdentityFixture is the data of every query.
func keepIdentityFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "kj", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "kj", "app": "api", "pod": "b", "service_name": "api"},
		{"job": "kj", "app": "web", "pod": "c", "service_name": "web"},
		{"job": "kx", "app": "web", "pod": "d", "service_name": "web"},
	}}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 17 * time.Second
	for s, labels := range f.Streams {
		for i := 0; ; i++ {
			// Each stream on a phase of its own, half a millisecond past a millisecond (every bound and step is whole
			// milliseconds, so no line sits on a window edge).
			at := first.Add(time.Duration(i)*every + time.Duration(s)*4*time.Second + time.Duration((i*7919+s*31)%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			v := strconv.Itoa((i*7919 + s*104729) % 5000)
			line := `{"duration_ms":` + v + `,"req":"r` + strconv.Itoa(s) + "-" + strconv.Itoa(i) + `","route":"/x"`
			if i%3 == 0 {
				line += `,"region":"eu"`
			}
			line += "}"
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line,
				Metadata: map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}})
			_ = labels
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestKeepUnwrapIdentityMatchLoki replays the recorded answers through the handlers and holds every run to Loki's
// answer, or to its listed difference.
//
// conformance: semantics/keep-before-unwrap-series-identity, semantics/keep-before-parser-unwrap-identity, semantics/drop-before-unwrap-stream-label, semantics/unwrap-rate-counter-window-extrapolation, semantics/unwrap-first-last-range-longer-than-step, semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-quantile-sum-by-group-quantile, semantics/stdvar-grouped-pooled-variance
func TestKeepUnwrapIdentityMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := keepIdentityQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, keepIdentityAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, keepIdentityAnswersPath)
	report := os.Getenv("LVP_KEEPID_REPORT") == "1"
	type row struct{ match, runs int }
	table := map[string]*row{}
	seenKnown := map[string]bool{}
	identities := 0
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		if checkKeptIdentityAgainstLoki(t, mq, rec) {
			identities++
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
				known := keepIdentityKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case report:
					if len(d.classes) == 0 {
						table[key].match++
						continue
					}
					if prof.name == diffProfiles()[0].name {
						var cs []string
						for c := range d.classes {
							cs = append(cs, c)
						}
						sort.Strings(cs)
						t.Logf("DIFF %s | %s | %s", mr.kind, mq.q, strings.Join(cs, "; "))
					}
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from keepIdentityKnown (%s)", prof.name, mr.kind, mq.q, known)
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
	if !report {
		for q, kinds := range keepIdentityKnown {
			for kind := range kinds {
				if !seenKnown[q+"\x00"+kind] {
					t.Errorf("keepIdentityKnown lists %s %s, which the differential does not run", kind, q)
				}
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
	t.Logf("keep and drop before an unwrap against Loki (%d series identities held to Loki's series):%s", identities, b.String())
	if identities < 8 {
		t.Errorf("only %d series identities held to Loki's series", identities)
	}
}

// checkKeptIdentityAgainstLoki holds the plan's series identity (queryplan.SeriesLabels) to Loki's own answers: for
// a range function alone whose series a keep decides, every series Loki answers carries only labels the identity
// names (the engine oracle is the reference of the attribute, as the facts oracle is of the stages it reads).
func checkKeptIdentityAgainstLoki(t *testing.T, mq metricQuery, rec *metricRecord) bool {
	t.Helper()
	ra, ok := mq.plan.Root.(*queryplan.RangeAgg)
	if !ok || ra.By != nil {
		return false
	}
	names, decided := queryplan.SeriesLabels(ra.In.Stages).KeptIdentity()
	if !decided {
		return false
	}
	for kind, a := range rec.Loki {
		if a.Status != 200 {
			continue
		}
		res, err := decodeMetricBody(a.Body)
		if err != nil {
			t.Fatalf("%s %s: %v", kind, mq.q, err)
		}
		for _, s := range res.series {
			for l := range s.labels {
				if !slices.Contains(names, l) {
					t.Errorf("%s %s: Loki's series has %s, which the series identity %v does not name", kind, mq.q, l, names)
				}
			}
		}
	}
	return true
}
