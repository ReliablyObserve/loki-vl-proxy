package querycorpus_test

import (
	"fmt"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The Loki differential of sums over json and logfmt parsers (semantics/ungrouped-sum-skips-parsers): real Loki (the
// engine oracle) and real VictoriaLogs (the serve oracle, v1.54.0 and v1.49.0) over the same data, through the proxy's
// real query and query_range handlers, as the metric differential runs them.
//
//   - Data (parserSumFixture): job=js holds JSON objects (keys named like VictoriaLogs' fields and like Loki's error
//     labels among them), job=lf logfmt lines (an unterminated value among them), job=mix both kinds, plain text,
//     broken JSON and a line of blanks, so each parser fails on some lines of it. job=none selects no line.
//   - A sum with no clause (or an empty by clause) of a count or a rate over bare json and logfmt parsers needs no
//     label: Loki runs no parser for it and answers the number of lines. The plan writes no stage from the first
//     parser on and the proxy sends no parser probe (lower.SkipsParsers); every run of such a query is held to
//     Loki's series and values, and must send no unpack pipe, no probe and no raw-row read.
//   - The controls keep today's text (a grouping, another aggregation, a filter, format or extraction list after a
//     parser, a flag, a regexp, a rate behind a keep, the bytes functions, absent_over_time): their runs are held
//     to Loki's status and totals, and the ones that differ on main are listed (parserSumKnown, shrink-only).
//
// The answers are recorded (parserSumAnswersPath; LVP_RUN_ORACLES=1 records them again, with LVP_RECORD_PACE=1s).

const parserSumAnswersPath = "testdata/parser-sum-loki-diff.jsonl.gz"

// parserSumKnown are the control runs that differ from Loki, by query: version and request kind, with the registry
// case that holds them open. A listed run that matches fails too, so the list only shrinks.
var parserSumKnown = map[string]unpackSumOpen{}

// parserSumSkipped are the sums the plan writes with no stage from the first parser on, over a selector S.
func parserSumSkipped(s string) []string {
	return []string{
		`sum(count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [1m]))`,
		`sum(count_over_time(` + s + ` | json | logfmt [1m]))`,
		`sum(count_over_time(` + s + ` | logfmt | json [1m]))`,
		`sum(count_over_time(` + s + ` | json [1m]))`,
		`sum(count_over_time(` + s + ` | logfmt [1m]))`,
		`sum(count_over_time(` + s + ` | logfmt | drop __error__, __error_details__ [1m]))`,
		`sum(rate(` + s + ` | json | logfmt | drop __error__, __error_details__ [1m]))`,
		`sum(rate(` + s + ` | logfmt [1m]))`,
		`sum(rate(` + s + ` | json | logfmt [1m] offset 40m))`,
		`sum by () (count_over_time(` + s + ` | json | logfmt [1m]))`,
		`sum(count_over_time(` + s + ` |= "k" | json | logfmt | drop __error__, __error_details__ [1m]))`,
		`sum(count_over_time(` + s + ` != "k" | logfmt [1m]))`,
		`sum(count_over_time(` + s + ` | json | drop a | logfmt [1m] offset 40m))`,
		`sum(count_over_time(` + s + ` | json | logfmt | keep a [1m]))`,
	}
}

// parserSumControls are the queries over a selector S that keep today's text.
func parserSumControls(s string) []string {
	return []string{
		`sum by (job) (count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [1m]))`,
		`sum by (a) (count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [1m]))`,
		`sum without () (count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [1m]))`,
		`count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [1m])`,
		`count(count_over_time(` + s + ` | json | logfmt | drop __error__, __error_details__ [1m]))`,
		`max(count_over_time(` + s + ` | logfmt [1m]))`,
		`sum(rate(` + s + ` | json | logfmt | keep a [1m]))`,
		`sum(count_over_time(` + s + ` | json | logfmt | a!="" [1m]))`,
		`sum(count_over_time(` + s + ` | json | logfmt | __error__="" [1m]))`,
		`sum(count_over_time(` + s + ` | json |= "k" [1m]))`,
		`sum(count_over_time(` + s + ` | json a="a" [1m]))`,
		`sum(count_over_time(` + s + ` | logfmt --strict [1m]))`,
		`sum(count_over_time(` + s + ` | regexp "(?P<w>\\w+)" [1m]))`,
		`sum(count_over_time(` + s + ` | json | line_format "{{.a}}" [1m]))`,
		`sum(count_over_time(` + s + ` | level="error" | logfmt [1m]))`,
		`sum(bytes_over_time(` + s + ` | json | logfmt [1m]))`,
		`sum(absent_over_time(` + s + ` | json | logfmt [1m]))`,
		// A rate over a bare json parser alone with an offset is answered by the ordered JSON route before the
		// lowering (as on main): it is held to Loki here, not to the skipped text.
		`sum(rate(` + s + ` | json [1m] offset 40m))`,
	}
}

// parserSumQueries are the queries of the differential.
func parserSumQueries() []string {
	var out []string
	for _, s := range []string{`{job="js"}`, `{job="lf"}`, `{job="mix"}`} {
		out = append(out, parserSumSkipped(s)...)
	}
	out = append(out, parserSumSkipped(`{job=~"js|lf|mix"}`)[:2]...)
	out = append(out, parserSumSkipped(`{job="none"}`)[:2]...)
	out = append(out, `sum(rate({job="none"} | logfmt [1m]))`)
	// The skipped sum under the wrappers Loki keeps it through: a binary operation, label_replace and a ranking.
	sum := `sum(count_over_time({job="mix"} | json | logfmt | drop __error__, __error_details__ [1m]))`
	out = append(out,
		sum+` * 2`,
		sum+` / sum(count_over_time({job="mix"}[1m]))`,
		`label_replace(`+sum+`, "dst", "x", "", "")`,
		`sort(`+sum+`)`,
		`topk(1, `+sum+`)`,
	)
	for _, s := range []string{`{job="mix"}`, `{job="lf"}`} {
		out = append(out, parserSumControls(s)...)
	}
	return out
}

func parserSumFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "js", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "js", "app": "web", "pod": "b", "service_name": "web"},
		{"job": "lf", "app": "api", "pod": "c", "service_name": "api"},
		{"job": "lf", "app": "web", "pod": "d", "service_name": "web"},
		{"job": "mix", "app": "api", "pod": "e", "service_name": "api"},
		{"job": "mix", "app": "web", "pod": "f", "service_name": "web"},
	}}
	lines := map[string][]string{
		"js": {
			`{"a":"1","b":"x","msg":"hello k"}`,
			`{"a":2,"b":true,"c":null,"d":{"e":"k"}}`,
			`{"__error__":"boom","a":"3"}`,
			`{"_msg":"m","_time":"2020-01-01T00:00:00Z","_stream":"{x=\"y\"}","a":"4"}`,
			`{"level":"error","a":"5"}`,
			`{}`,
		},
		"lf": {
			`a=1 b=x msg="hello k"`,
			`a=2 level=error`,
			`a="unterminated k`,
			`=novalue a=3`,
			`level=info msg=hi`,
			`__error__=x a=4`,
		},
		"mix": {
			`{"a":"1","msg":"k"}`,
			`a=2 b=y`,
			`plain text line k`,
			`{"a":"3"`,
			`["k"]`,
			`a="unterminated`,
			`   `,
			`{"a":"4","level":"warn"}`,
		},
	}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	for s, labels := range f.Streams {
		md := map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}
		ls := lines[labels["job"]]
		for i := 0; ; i++ {
			at := first.Add(time.Duration(i)*53*time.Second + time.Duration(s)*11*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: ls[(i+s)%len(ls)], Metadata: md})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestParserSumMatchesLoki replays the recorded answers through the handlers.
//
// conformance: semantics/ungrouped-sum-skips-parsers, semantics/two-parsers-keep-one, semantics/loki-pipeline-error-rules
func TestParserSumMatchesLoki(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	fix := parserSumFixture()
	var queries []metricQuery
	for _, q := range parserSumQueries() {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		queries = append(queries, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, parserSumAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, parserSumAnswersPath)
	match, runs, skippedRuns, sums, open := 0, 0, 0, 0, 0
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		skipped := parserSumServed(mq)
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
				if skipped {
					// The plan writes no stage from the first parser on: VictoriaLogs parses no line, the proxy sends
					// no parser probe, and the answer comes from stats, not raw rows.
					for _, sent := range run.sent {
						if parserSumReadsLines(sent) {
							t.Errorf("%s %s %s: sent %.300s", prof.name, mr.kind, mq.q, sent)
						}
					}
					skippedRuns++
				}
				diff := parserErrorDiff(loki, run, true)
				if skipped && diff == "" && loki.Status == 200 {
					if d := compareMetric(loki, run); len(d.classes) != 0 {
						diff = fmt.Sprintf("another answer: %v", d.classes)
					}
				}
				known := parserSumKnown[mq.q]
				runKey := prof.version + " " + mr.kind
				switch {
				case known.has(runKey) && diff == "":
					t.Errorf("%s %s %s matches Loki: remove it from parserSumKnown (%s)", prof.name, mr.kind, mq.q, known.id)
					continue
				case known.has(runKey):
					open++
					continue
				}
				if diff != "" {
					t.Errorf("%s %s %s (Loki %d, %d VictoriaLogs requests): %.600s", prof.name, mr.kind, mq.q, loki.Status, len(run.sent), diff)
					continue
				}
				match++
				if skipped && loki.Status == 200 {
					sums++
				}
			}
		}
	}
	listed := 0
	for _, k := range parserSumKnown {
		listed += len(k.runs)
	}
	if open != listed {
		t.Errorf("%d runs listed in parserSumKnown were open, %d are listed: a listed query is no longer run", open, listed)
	}
	t.Logf("parser sums against Loki: %d of %d runs match (%d of them skipped sums compared series by series), %d listed as open; %d runs read no line for a parser", match, runs, sums, open, skippedRuns)
	if sums == 0 || skippedRuns == 0 {
		t.Error("no skipped sum reached the comparison")
	}
}

// parserSumServed reports a query the proxy serves with no stage from the first parser on: the sum at the root, as
// the operand of a binary operation or a label_replace call, or under a ranking, which translates its operand as a
// query of its own.
func parserSumServed(mq metricQuery) bool {
	if parserSumOrderedRoute(mq.q) {
		return false
	}
	if lower.SkipsParsers(mq.plan) || lower.SkipsParsersWrapped(mq.plan) {
		return true
	}
	rank, ok := mq.plan.Root.(*queryplan.Rank)
	return ok && lower.SkipsParsers(&queryplan.Plan{Root: rank.In})
}

// parserSumOrderedRoute reports a rate over a bare json parser alone with an offset, which the ordered JSON route
// answers before any translation (as on main), so the plan's text is not what the proxy sends for it.
func parserSumOrderedRoute(q string) bool {
	return strings.HasPrefix(q, "sum(rate(") && strings.HasSuffix(q, " | json [1m] offset 40m))")
}

// parserSumReadsLines reports a sent request key that makes VictoriaLogs parse or return lines: an unpack pipe, the
// parser probe (25 newest lines) or any raw-row read.
func parserSumReadsLines(sent string) bool {
	path, raw, _ := strings.Cut(sent, "?")
	if path == "/select/logsql/query" {
		return true
	}
	form, _ := url.ParseQuery(raw)
	q := form.Get("query")
	return strings.Contains(q, "unpack_json") || strings.Contains(q, "unpack_logfmt")
}

// TestParserSumQueriesSplit holds the differential's split to the plan: every skipped row is one the plan skips,
// and no control is.
//
// conformance: semantics/ungrouped-sum-skips-parsers
func TestParserSumQueriesSplit(t *testing.T) {
	controls := map[string]bool{}
	for _, s := range []string{`{job="mix"}`, `{job="lf"}`} {
		for _, q := range parserSumControls(s) {
			controls[q] = true
		}
	}
	for _, q := range parserSumQueries() {
		p, err := planOfText(q)
		if err != nil {
			t.Fatal(err)
		}
		served := parserSumServed(metricQuery{q: q, plan: p})
		if served == controls[q] {
			t.Errorf("%s: skipped %v, control %v", q, served, controls[q])
		}
	}
	if !slices.Contains(parserSumQueries(), `sum(count_over_time({job="js"} | json | logfmt | drop __error__, __error_details__ [1m]))`) {
		t.Error("the Drilldown count is not a row")
	}
}
