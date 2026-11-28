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
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The Loki differential of metric queries over an unpack: real
// Loki (the engine oracle) and real VictoriaLogs (the serve oracle, v1.54.0 and v1.49.0) over the same data, through
// the proxy's real query and query_range handlers, as the metric differential runs them.
//
//   - Data (unpackSumFixture): job=pk holds only JSON objects (packed lines, lines without an `_entry`, an empty,
//     numeric or object `_entry`, values that are not strings, a packed key that collides with a stream label);
//     job=bad adds lines that are no JSON object in a ten-minute stretch, which Loki's unpack marks with __error__.
//   - Open: a range aggregation over lines Loki's unpack rejects answers where Loki fails with its pipeline error
//     (semantics/unpack-parser-error-not-raised); a sum without grouping needs no label and matches.
//   - An unwrapped value over lines that are not packed and hold no number fails nothing, as in Loki
//     (semantics/unwrap-conversion-check-reads-unpack-unlike-loki).
//   - A sum with no clause adds up the series, so Loki's answer is held series by series and value by value (it
//     answers over job=bad too: Loki parses no line for a sum that needs no label). A range aggregation of its own
//     splits the series by the promoted labels (the owner-kept stream identity of parser metrics), so it is held to
//     the total per timestamp.
//
// The answers are recorded (unpackSumAnswersPath; LVP_RUN_ORACLES=1 records them again, with LVP_RECORD_PACE=1s).

const unpackSumAnswersPath = "testdata/unpack-sum-loki-diff.jsonl.gz"

// unpackSumCase holds open the stages after an unpack that read the labels or the line it made.
const unpackSumCase = "semantics/translator-writes-unpack-as-unpack-json"

// unpackSumOpen is the runs of a query that differ from Loki, and the registry case holding them open.
type unpackSumOpen struct {
	id   string
	runs []string
}

func (o unpackSumOpen) has(run string) bool { return slices.Contains(o.runs, run) }

// unpackSumKnown are the runs that differ from Loki, by query: version and request kind. A listed run that matches
// fails too, so the list only shrinks.
var unpackSumKnown = func() map[string]unpackSumOpen {
	ranged := []string{"v1.54.0 step=range", "v1.54.0 step<range", "v1.54.0 step>range", "v1.49.0 step=range", "v1.49.0 step<range", "v1.49.0 step>range"}
	all := append([]string{"v1.54.0 instant", "v1.49.0 instant"}, ranged...)
	return map[string]unpackSumOpen{
		// A label filter on __error__ and a line filter after the unpack read what it made.
		`count_over_time({job="bad"} | unpack | __error__="" [1m])`: {unpackSumCase, ranged},
		`count_over_time({job="bad"} | unpack != "k" [1m])`:         {unpackSumCase, ranged},
		// A value is unwrapped from a line that is not packed.
		`sum_over_time({job="uwline"} | unpack | unwrap a [1m])`: {unpackSumCase, all},
		// A sum over a maximum adds the maxima of Loki's series, which the promoted labels split.
		`sum(max_over_time({job="uw"} | unpack | unwrap a [1m]))`:    {unpackSumCase, ranged},
		`sum(max_over_time({job="uwbad"} | unpack | unwrap a [1m]))`: {unpackSumCase, []string{"v1.54.0 step<range", "v1.49.0 step<range"}},
	}
}()

// unpackSumQueries are the queries, each range aggregation alone and under a sum.
func unpackSumQueries() []string {
	var out []string
	for _, job := range []string{`{job="pk"}`, `{job="bad"}`} {
		for _, leaf := range []string{
			`count_over_time(` + job + ` | unpack [1m])`,
			`rate(` + job + ` | unpack [1m])`,
			`count_over_time(` + job + ` |= "k" | unpack | drop a [1m])`,
			`rate(` + job + ` | unpack | keep a, b [1m] offset 40m)`,
		} {
			out = append(out, leaf, `sum(`+leaf+`)`)
		}
		out = append(out, `sum(absent_over_time(`+job+` | unpack [1m]))`)
	}
	// An absence check over a selector with no line: the sum answers 1.
	// Parser errors reach every function above the range aggregation unless the pipeline intercepts them, and a
	// grouping makes Loki parse under a sum.
	leaf := `count_over_time({job="bad"} | unpack [1m])`
	out = append(out,
		`sum by (app) (`+leaf+`)`,
		`sort(`+leaf+`)`,
		`topk(2, `+leaf+`)`,
		`max(`+leaf+`)`,
		`count by (pod) (`+leaf+`)`,
		`label_replace(`+leaf+`, "dst", "$1", "app", "(.*)")`,
		leaf+` * 2`,
		`sum(`+leaf+`) + sum by (app) (`+leaf+`)`,
		`rate({job="bad"} |= "k" | unpack [1m] offset 40m)`,
		`bytes_over_time({job="bad"} != "json" | unpack [1m])`,
		`count_over_time({job="bad"} | unpack | drop __error__, __error_details__ [1m])`,
	)
	// The stages after an unpack that read what it made (unpackSumKnown holds the runs open).
	out = append(out,
		`count_over_time({job="bad"} | unpack | __error__="" [1m])`,
		`count_over_time({job="bad"} | unpack != "k" [1m])`,
		`sum_over_time({job="uwline"} | unpack | unwrap a [1m])`,
	)
	// A grouped sum reads VictoriaLogs' stats at an instant too.
	for _, job := range []string{`{job="pk"}`, `{job=~"pk|bad"}`} {
		out = append(out,
			`sum by (job) (count_over_time(`+job+` | unpack [5m]))`,
			`sum by (job, service_name) (rate(`+job+` | unpack | drop a [5m]))`,
		)
	}
	// A keep makes the series the labels it leaves, which the handlers read as a grouped sum.
	out = append(out,
		`count_over_time({job="pk"} | unpack | keep job, app [5m])`,
		`rate({job="pk"} | unpack | keep job [5m])`,
	)
	// An unwrapped value is a label the unpack promotes.
	for _, job := range []string{`{job="uw"}`, `{job="uwbad"}`} {
		for _, fn := range []string{"sum_over_time", "max_over_time"} {
			out = append(out, fn+`(`+job+` | unpack | unwrap a [1m])`, `sum(`+fn+`(`+job+` | unpack | unwrap a [1m]))`)
		}
	}
	// A sum with no clause of a count or a rate reads no line for the unpack (semantics/ungrouped-sum-skips-unpack):
	// over packed keys named like VictoriaLogs' fields, over a selector with no line, with an empty by clause (Loki's
	// tree holds no grouping) and a count behind a keep; a rate behind a keep keeps the unpack. An empty without clause
	// keeps every series and the unpack.
	out = append(out,
		`sum(count_over_time({job="rs"} | unpack [1m]))`,
		`sum(rate({job="rs"} | unpack | drop a [1m]))`,
		`sum(count_over_time({job="none"} | unpack [1m]))`,
		`sum(rate({job="none"} | unpack | keep a [1m] offset 40m))`,
		`sum by () (count_over_time({job="bad"} | unpack [1m]))`,
		`sum by () (rate({job="pk"} | unpack | drop a [1m]))`,
		`sum by () (rate({job="bad"} | unpack | keep a [1m]))`,
		`sum without () (count_over_time({job="pk"} | unpack [1m]))`,
		`sum(rate({job="pk"} | unpack | keep a [1m]))`,
		`sum(rate({job="bad"} | unpack | keep app, a [1m]))`,
		`sum(count_over_time({job="bad"} | unpack | drop b | keep a [1m]))`,
		`sum(count_over_time({job="none"} | unpack | keep a [1m]))`,
	)
	// The rewritten sum under the wrappers Loki keeps it through: the proxy serves it from the plan as the operand of
	// a binary operation or a label_replace call, and a ranking translates it as its operand.
	for _, sum := range []string{
		`sum(count_over_time({job="pk"} | unpack [1m]))`,
		`sum(rate({job="pk"} | unpack | drop a [1m]))`,
		`sum by () (count_over_time({job="bad"} | unpack [1m]))`,
		`sum(rate({job="pk"} | unpack | keep a [1m]))`,
		`sum(count_over_time({job="bad"} | unpack | keep a [1m]))`,
	} {
		out = append(out,
			sum+` * 2`,
			`2 * `+sum,
			sum+` / sum(count_over_time({job="pk"}[1m]))`,
			sum+` + sum by (app) (count_over_time({job="pk"} | unpack [1m]))`,
			`sort(`+sum+`)`,
			`sort_desc(`+sum+`)`,
			`topk(1, `+sum+`)`,
			`label_replace(`+sum+`, "dst", "x", "", "")`,
			`label_replace(`+sum+` * 2, "dst", "x", "", "")`,
			`max(`+sum+`)`,
		)
	}
	// A ranking over a grouped sum: the instant ranking hands the stats routes the sum as its operand.
	out = append(out, `sort(sum by (app) (count_over_time({job="pk"} | unpack [1m])))`)
	// A ranking over a range aggregation behind a keep, which the handlers group by the kept labels: the rewrite is
	// handed on in the request's canonical spelling (it printed `sort (`, which the translation read as a phrase).
	out = append(out,
		`sort(rate({job="pk"} | json | keep job [1m]))`,
		`sort_desc(count_over_time({job="pk"} | json | keep job, service_name [1m]))`,
		`topk(1, rate({job="pk"} | json | keep service_name [1m]))`,
	)
	// absent_over_time reads the same count at an instant.
	return append(out, `sum(absent_over_time({job="none"} | unpack [1m]))`,
		`absent_over_time({job="none"} | unpack [1m])`, `absent_over_time({job="pk"} | unpack [1m])`)
}

func unpackSumFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "pk", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "pk", "app": "web", "pod": "b", "service_name": "web"},
		{"job": "bad", "app": "api", "pod": "c", "service_name": "api"},
		{"job": "bad", "app": "web", "pod": "d", "service_name": "web"},
		{"job": "uw", "app": "api", "pod": "e", "service_name": "api"},
		{"job": "uwbad", "app": "api", "pod": "f", "service_name": "api"},
		{"job": "uwline", "app": "api", "pod": "g", "service_name": "api"},
		{"job": "rs", "app": "api", "pod": "h", "service_name": "api"},
	}}
	valid := []string{
		`{"a":"1","b":"x","_entry":"hello world k"}`,
		`{"a":"1","b":"k"}`,
		`{"a":"1","_entry":""}`,
		`{"a":1,"b":true,"c":null,"d":{"e":"k"},"_entry":"L"}`,
		`{"a":"1","_entry":5,"k":"v"}`,
		`{"a":"1","_entry":{"x":"k"}}`,
		`{"app":"packed","pod":"p","_entry":"k"}`,
		`{"a":"1","n":{"_entry":"k"}}`,
		`{}`,
	}
	// job=uw is unwrapped: packed values, and a value that is no number beside an `_entry` that is no top-level
	// string, which Loki does not promote (so it converts nothing and fails nothing). job=uwbad adds plain text.
	unwrapped := []string{`{"a":"2","_entry":"x"}`, `{"a":"oops","_entry":5}`, `{"a":"oops","n":{"_entry":"z"}}`, `{"a":"x","_entry":{"x":"y"}}`, `{"a":"3","_entry":""}`}
	bad := []string{`not json k`, `{"a":"1","_entry":"k"`, `["k"]`}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	badFrom, badTo := metricEnd.Add(-45*time.Minute), metricEnd.Add(-35*time.Minute)
	for s, labels := range f.Streams {
		md := map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}
		for i := 0; ; i++ {
			at := first.Add(time.Duration(i)*47*time.Second + time.Duration(s)*13*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			line := valid[(i+s)%len(valid)]
			switch labels["job"] {
			case "uw":
				line = unwrapped[i%len(unwrapped)]
			case "uwline":
				// A numeric value on a line that is not packed: Loki promotes nothing from it.
				line = []string{`{"a":"2","_entry":"x"}`, `{"a":"1000"}`, `{"a":"1","_entry":""}`}[i%3]
			case "uwbad":
				line = []string{`{"a":"2","_entry":"x"}`, `not json`}[i%2]
			case "rs":
				// Packed lines whose keys are VictoriaLogs' own field names, which an unpack_json pipe would write.
				line = []string{`{"_time":"2020-01-01T00:00:00Z","_entry":"k"}`, `{"_stream":"{x=\"y\"}","a":"1","_entry":"k"}`,
					`{"_msg":"m","_entry":"k"}`, `{"_stream_id":"0","_entry":"k"}`, `{"_time":"later","a":"1"}`}[i%5]
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line, Metadata: md})
		}
		if labels["job"] != "bad" {
			continue
		}
		for i := 0; ; i++ {
			at := badFrom.Add(time.Duration(i)*29*time.Second + time.Duration(s)*3*time.Second + 300*time.Millisecond + 500*time.Microsecond)
			if !at.Before(badTo) {
				break
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: bad[i%len(bad)] + " " + strconv.Itoa(i), Metadata: md})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestUnpackSumMatchesLoki replays the recorded answers through the handlers.
//
// conformance: semantics/translator-writes-unpack-as-unpack-json, semantics/ungrouped-sum-skips-unpack, semantics/unpack-parser-error-not-raised, semantics/unpack-instant-sum-rate-per-stream, semantics/unpack-instant-grouped-sum-reads-every-line, semantics/unwrap-conversion-check-reads-unpack-unlike-loki
func TestUnpackSumMatchesLoki(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	fix := unpackSumFixture()
	var queries []metricQuery
	for _, q := range unpackSumQueries() {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		queries = append(queries, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, unpackSumAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, unpackSumAnswersPath)
	match, runs, sums, failing, open, notRaised, skippedRuns := 0, 0, 0, 0, 0, 0, 0
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
				runs++
				if skipsUnpackServed(mq) {
					// The plan writes no stage from the unpack on, and VictoriaLogs answers from stats, not raw rows.
					for _, sent := range run.sent {
						// A grouped operand beside the sum keeps its unpack.
						grouped := strings.Contains(mq.q, "sum by (app)")
						if strings.Contains(sent, "unpack_json") && !grouped || strings.HasPrefix(sent, "/select/logsql/query?") {
							t.Errorf("%s %s %s: sent %.300s", prof.name, mr.kind, mq.q, sent)
						}
					}
					skippedRuns++
				}
				diff := parserErrorDiff(loki, run, true)
				sum := strings.HasPrefix(mq.q, "sum(count") || strings.HasPrefix(mq.q, "sum(rate") || strings.HasPrefix(mq.q, "sum(absent") ||
					strings.HasPrefix(mq.q, "sum by () (") || lower.WritesSkippedUnpack(mq.plan)
				if sum && diff == "" && loki.Status == 200 {
					if d := compareMetric(loki, run); len(d.classes) != 0 {
						diff = fmt.Sprintf("another answer: %v", d.classes)
					}
				}
				known := unpackSumKnown[mq.q]
				runKey := prof.version + " " + mr.kind
				switch {
				case known.has(runKey) && diff == "":
					t.Errorf("%s %s %s matches Loki: remove it from unpackSumKnown (%s)", prof.name, mr.kind, mq.q, known.id)
					continue
				case known.has(runKey):
					open++
					continue
				}
				// Open: a metric over a line the unpack rejects answers where Loki fails, unless the unwrap conversion
				// check reads the line (semantics/unpack-parser-error-not-raised).
				if loki.Status == 400 && !strings.Contains(mq.q, "unwrap") {
					if run.status != 200 {
						t.Errorf("%s %s %s: status %d where Loki fails: take the open parser error difference off the harness", prof.name, mr.kind, mq.q, run.status)
					}
					notRaised++
					continue
				}
				if diff != "" {
					t.Errorf("%s %s %s (Loki %d, %d VictoriaLogs requests): %.600s", prof.name, mr.kind, mq.q, loki.Status, len(run.sent), diff)
					continue
				}
				match++
				if loki.Status == 400 {
					failing++
				} else if sum {
					sums++
				}
			}
		}
	}
	listed := 0
	for _, k := range unpackSumKnown {
		listed += len(k.runs)
	}
	if open != listed {
		t.Errorf("%d runs listed in unpackSumKnown were open, %d are listed: a listed query is no longer run", open, listed)
	}
	t.Logf("unpack aggregations against Loki: %d of %d runs match (%d of them answered sums, %d matching Loki errors), %d listed as open, %d answering where Loki fails; %d runs read no line for the unpack", match, runs, sums, failing, open, notRaised, skippedRuns)
	if sums == 0 || notRaised == 0 {
		t.Error("no sum, or no run Loki fails, reached the comparison")
	}
}

// skipsUnpackServed reports a query the proxy serves with no stage from the unpack on: the sum at the root, as the
// operand of a binary operation or a label_replace call (lower.SkipsUnpackWrapped), or under a ranking, which
// translates its operand as a query of its own. A sum under another aggregation keeps the translator's text.
func skipsUnpackServed(mq metricQuery) bool {
	if lower.SkipsUnpack(mq.plan) || lower.SkipsUnpackWrapped(mq.plan) {
		return true
	}
	rank, ok := mq.plan.Root.(*queryplan.Rank)
	return ok && lower.SkipsUnpack(&queryplan.Plan{Root: rank.In})
}
