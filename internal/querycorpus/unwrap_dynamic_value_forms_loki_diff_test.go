package querycorpus_test

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// The Loki differential of the values a parser's series identity carries (lower.DynamicUnwrapIdentity): numbers
// whose text VictoriaLogs' stats cannot read once the series hash has unpacked the line again, against real Loki (the
// engine oracle) and real VictoriaLogs (the serve oracle, v1.54.0 and v1.49.0), through the proxy's real handlers.
//
// VictoriaLogs' math writes a number as fixed-point text: 1e308 is 309 digits, 1e-30 is 32 characters and an infinity
// is "+Inf". Its stats read text with a parser that refuses an infinity and anything longer than 27 characters, so a
// per-series avg, sum or stddev over such values, and an outer sum or avg over the per-series values, answered NaN or
// left the series out (the value-forms e2e row, TestCompat_UnwrapValueFormsParity, found it). The lowering reads the
// value and each series' value again with math (dynamicPackHash, dynamicInnerNumber).
//
// One logfmt stream and one json stream per app, a line every 11 seconds cycling through the forms; each form is a
// series of its own (its `form` label), and every query reads one form through a label filter between the parser and
// the unwrap, after a parser of every key or an extraction list of the line's keys (`| logfmt u, form`), whose
// series identity groups by the listed labels with no second unpack. The answers are recorded (dynamicValueFormsAnswersPath; LVP_RUN_ORACLES=1 records them
// again). Every run must match Loki or be listed in dynamicValueFormsKnown with the registry case that holds it open.

const dynamicValueFormsAnswersPath = "testdata/unwrap-dynamic-value-forms-loki-diff.jsonl.gz"

// dynamicValueForms are the forms and their unwrapped text, in logfmt and in json (a json number where JSON has one).
var dynamicValueForms = []struct{ name, logfmt, json string }{
	{"exp308", "1e308", "1e308"},
	{"largest", "1.7976931348623157e308", "1.7976931348623157e308"},
	{"inf", "inf", `"inf"`},
	{"neginf", "-Infinity", `"-Infinity"`},
	{"big", "1e25", "1e25"},
	{"negbig", "-1e25", "-1e25"},
	{"tiny", "1e-30", "1e-30"},
	{"plain", "5", "5"},
}

// dynamicValueFormsShapes are the query shapes, %s the function call's head and %s the selector's pipeline.
var dynamicValueFormsShapes = []string{
	`sum by (app) (%s%s [1m]))`,
	`sum(%s%s [1m]))`,
	`avg(%s%s [1m]))`,
	`max by (app) (%s%s [1m]))`,
	`min(%s%s [1m]))`,
}

// dynamicValueFormsPipelines are the pipelines, each one form through a label filter between the parser and the unwrap
// (the e2e row's shape). One form a query: the outer aggregation then adds at most two equal values (one series of
// each app), which IEEE addition gives the same way in any order; mixing magnitudes (1e25 and 5) or signs of an
// infinity would make the answers depend on the order Loki and VictoriaLogs add the series in.
var dynamicValueFormsPipelines = []string{
	`{job="vf"} | logfmt | form="inf" | unwrap u`,
	`{job="vf"} | logfmt | form="neginf" | unwrap u`,
	`{job="vf"} | logfmt | form="exp308" | unwrap u`,
	`{job="vf"} | logfmt | form="largest" | unwrap u`,
	`{job="vf"} | logfmt | form="big" | unwrap u`,
	`{job="vf"} | logfmt | form="tiny" | unwrap u`,
	`{job="vj"} | json | form="exp308" | unwrap u`,
	`{job="vj"} | json | form="inf" | unwrap u`,
	// An extraction list of the line's keys: the same series, grouped by the listed labels (no series hash).
	`{job="vf"} | logfmt u, form | form="exp308" | unwrap u`,
	`{job="vf"} | logfmt u, form | form="inf" | unwrap u`,
	`{job="vj"} | json u, form | form="exp308" | unwrap u`,
}

// dynamicValueFormsFuncs are the functions taken per series that the outer aggregations fold.
var dynamicValueFormsFuncs = []string{"max_over_time", "min_over_time", "avg_over_time", "stddev_over_time", "stdvar_over_time"}

// dynamicValueFormsKnown are the runs that differ from Loki, by query and request kind, with the registry case that
// holds the difference open (semantics/unwrap-gate-parsefloat-divergences, the values VictoriaLogs reads otherwise
// than Loki):
//
//   - avg over the series of an infinity: Loki's mean of +Inf and +Inf is NaN, VictoriaLogs' is +Inf;
//   - sum, sum by or avg over stddev or stdvar of an infinity on an instant: Loki answers NaN, VictoriaLogs' outer sum
//     leaves the NaN series out (0). Under sum by (app) whether it does depends on how VictoriaLogs' blocks hold the
//     column (its sum skips NaN in a float column), so those runs are listed as layout-dependent (dynamicValueFormsLayout:
//     either answer is accepted): the recording before 2026-11-16 had VictoriaLogs v1.49.0 and v1.53.0 answer NaN over a
//     logfmt line, the one of that date 0 (over a json line, NaN), and the one of 2026-11-18 (the series hash written
//     with set_stream_fields) 0 for an infinity over a json line and -Infinity over a logfmt line, NaN for the others.
//
// A range longer than the step matches Loki on every row: the window evaluator merges each series' buckets (an
// extraction list's fields, or the series hash of a parser of every key) and the proxy folds the series, reading every
// value as a number.
// dynamicValueFormsLayout marks a known run whose answer depends on VictoriaLogs' block layout: Loki's answer or the
// known difference, both accepted.
const dynamicValueFormsLayout = "layout-dependent: "

var dynamicValueFormsKnown = func() map[string]map[string]string {
	const id = "semantics/unwrap-gate-parsefloat-divergences"
	known := map[string]map[string]string{}
	for _, fn := range dynamicValueFormsFuncs {
		for _, shape := range dynamicValueFormsShapes {
			for _, listed := range dynamicValueFormsPipelines {
				q := fmt.Sprintf(shape, keepIdentityHead(fn), listed)
				// An extraction list of every key of the line reads as the bare parser does.
				pipe := strings.NewReplacer("| logfmt u, form |", "| logfmt |", "| json u, form |", "| json |").Replace(listed)
				infinity := strings.Contains(pipe, `"inf"`) || strings.Contains(pipe, `"neginf"`)
				deviation := fn == "stddev_over_time" || fn == "stdvar_over_time"
				var kinds []string
				switch {
				case infinity && strings.HasPrefix(shape, "avg(") && !deviation:
					kinds = append(kinds, "instant", "step=range", "step>range")
				case infinity && deviation && (strings.HasPrefix(shape, "avg(") || strings.HasPrefix(shape, "sum(")):
					kinds = append(kinds, "instant")
				case infinity && deviation && strings.HasPrefix(shape, "sum by"):
					if known[q] == nil {
						known[q] = map[string]string{}
					}
					known[q]["instant"] = dynamicValueFormsLayout + id
				}
				for _, k := range kinds {
					if known[q] == nil {
						known[q] = map[string]string{}
					}
					known[q][k] = id
				}
			}
		}
	}
	return known
}()

// dynamicValueFormsQueries are the differential's queries over dynamicValueFormsFixture.
func dynamicValueFormsQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := dynamicValueFormsFixture()
	var out []metricQuery
	for _, fn := range dynamicValueFormsFuncs {
		for _, shape := range dynamicValueFormsShapes {
			for _, pipe := range dynamicValueFormsPipelines {
				q := fmt.Sprintf(shape, keepIdentityHead(fn), pipe)
				p, err := planOfText(q)
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
			}
		}
	}
	return out
}

// dynamicValueFormsFixture is the data of every query: a logfmt stream (job vf) and a json stream (job vj) of app
// api and of app web, a line every 11 seconds cycling through dynamicValueForms.
func dynamicValueFormsFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "vf", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "vf", "app": "web", "pod": "b", "service_name": "web"},
		{"job": "vj", "app": "api", "pod": "c", "service_name": "api"},
		{"job": "vj", "app": "web", "pod": "d", "service_name": "web"},
	}}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 11 * time.Second
	for s, labels := range f.Streams {
		for i := 0; ; i++ {
			// Half a millisecond past a millisecond, each stream on a phase of its own: no line on a window edge.
			at := first.Add(time.Duration(i)*every + time.Duration(s)*1700*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			form := dynamicValueForms[(i+s)%len(dynamicValueForms)]
			line := `form=` + form.name + ` u=` + form.logfmt
			if labels["job"] == "vj" {
				line = `{"form":"` + form.name + `","u":` + form.json + `}`
			}
			level := metricStreamLevels[s%len(metricStreamLevels)]
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line,
				Metadata: map[string]string{"detected_level": level}})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestUnwrapDynamicValueFormsMatchLoki replays the recorded answers through the handlers and holds every run to Loki's
// answer, or to its listed difference.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapDynamicValueFormsMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := dynamicValueFormsQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, dynamicValueFormsAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, dynamicValueFormsAnswersPath)
	report := os.Getenv("LVP_KEEPID_REPORT") == "1"
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
				known := dynamicValueFormsKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case report:
					if len(d.classes) == 0 {
						table[key].match++
						continue
					}
					if prof.name == diffProfiles()[0].name {
						var cs []string
						for c, ex := range d.classes {
							cs = append(cs, c+": "+ex)
						}
						sort.Strings(cs)
						t.Logf("DIFF %s | %s | %s", mr.kind, mq.q, strings.Join(cs, "; "))
					}
				case strings.HasPrefix(known, dynamicValueFormsLayout):
					if len(d.classes) == 0 {
						table[key].match++
					}
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from dynamicValueFormsKnown (%s)", prof.name, mr.kind, mq.q, known)
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
		for q, kinds := range dynamicValueFormsKnown {
			for kind := range kinds {
				if !seenKnown[q+"\x00"+kind] {
					t.Errorf("dynamicValueFormsKnown lists %s %s, which the differential does not run", kind, q)
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
	t.Logf("the values of a parser's series against Loki:%s", b.String())
}
