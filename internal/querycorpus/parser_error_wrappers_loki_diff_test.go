package querycorpus_test

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The Loki differential of parser errors under wrappers: real Loki (the engine oracle, test/oracle/loki) and real
// VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and v1.49.0) over the same data, through the proxy's real
// query and query_range handlers, as the metric differential runs them (plan_loki_vl_metric_diff_test.go).
//
// Loki fails a metric query with HTTP 400 and its pipeline error text when a sample that reaches a range vector
// carries __error__ (a line its parser rejects, a value its unwrap conversion rejects) and no label filter or drop
// intercepts it. Every function above the range aggregation keeps that failure. The generator wraps each leaf that
// can fail (parserErrorLeaves) in each wrapper and nesting (parserErrorWrappers), and the leaves Loki answers
// (parserErrorControls) in a few. Every run must answer Loki's status; an error Loki's errorType and text, with the
// series it names held to its form (parserErrorDiff); an answer Loki's series and values (compareMetric). A run that
// differs is listed in parserErrorKnown with the registry case that holds the difference open; a listed run that
// matches fails too, so the list only shrinks.
//
// Cost: a run Loki answers sends no other VictoriaLogs request than the metric's own reads (parserErrorExtraCalls):
// the JSON parser error check rides on them, and only a marked line costs a lookup (job=oke, a line the mark
// cannot vouch for and Loki parses). The answers of the leaves over clean streams (job=ok, job=oke), whose reads
// are marked and read again, are held to Loki's series and values too.
//
//   - Data (parserErrorFixture): streams of json lines and of logfmt lines (job=sp), valid lines all the hour, and
//     lines their parser rejects (plain text, a truncated object, an unterminated quote, a value the unwrap cannot
//     convert) only in a ten-minute stretch; streams of valid json only (job=ok); a stream of valid json, empty
//     objects `{}` and `{ }` (job=oke): the mark cannot vouch for the last, which Loki parses, so its stats answers
//     cost a lookup and are read again without the mark. An instant request at the window end sees no rejected line (Loki answers 200), a range
//     request over the hour does (Loki answers 400), an offset moves an instant request into the stretch.
//   - Requests: metricRequests (an instant query and three query_range steps).
//
// The answers are recorded (parserErrorAnswersPath; LVP_RUN_ORACLES=1 records them again, with LVP_RECORD_PACE=1s:
// every run is a fresh proxy, and a faster recording runs the host out of ephemeral ports, which records transport
// failures instead of VictoriaLogs' answers).

const parserErrorAnswersPath = "testdata/parser-error-wrappers-loki-diff.jsonl.gz"

// parserErrorLeaves are the range aggregations whose samples carry __error__ in Loki, each written with a [1m] range
// the offset forms extend; each runs under every wrapper, with and without the offset.
var parserErrorLeaves = []string{
	`rate({job="sp", enc="json"} | json [1m])`,
	`count_over_time({job="sp", enc="logfmt"} | logfmt --strict [1m])`,
	`sum_over_time({job="sp", enc="logfmt"} | logfmt | unwrap n [1m])`,
}

// parserErrorControls are leaves whose answer Loki serves (no error reaches the result: valid lines only, a filter
// or drop of __error__, a parser that never fails, absent_over_time), each under parserErrorControlWrappers.
// parserErrorSpellings are leaves Loki fails, written in another spelling the parser reads, each under
// parserErrorControlWrappers; and parserErrorHazards leaves over lines both parsers read as JSON while Loki's rejects
// them, which the mark does not find (parserErrorHazardCase).
var (
	parserErrorSpellings = []string{`rate({job="sp", enc="json"} | JSON [1m])`}
	parserErrorHazards   = []string{`rate({job="hz", enc="json"} | json [1m])`}
)

var parserErrorControls = []string{
	`rate({job="ok", enc="json"} | json [1m])`,
	`count_over_time({job="oke", enc="json"} | json [1m])`,
	`rate({job="sp", enc="json"} | json | __error__="" [1m])`,
	`rate({job="sp", enc="json"} | json | drop __error__, __error_details__ [1m])`,
	`count_over_time({job="sp", enc="logfmt"} | regexp "status=(?P<status>\\d+)" [1m])`,
	`count_over_time({job="sp", enc="logfmt"} | pattern "<_> status=<status> <_>" [1m])`,
	`absent_over_time({job="sp", enc="json"} | json [1m])`,
}

// parserErrorWrappers are the functions and nestings around a leaf, %[1]s the leaf. The sum rows without grouping
// hold Loki's parser hints: a sum of rate or count_over_time without grouping needs no label, so Loki does not parse
// the lines and answers, under any wrapper above the sum.
var parserErrorWrappers = []string{
	`%[1]s`,
	`sort(%[1]s)`,
	`sort_desc(%[1]s)`,
	`topk(2, %[1]s)`,
	`bottomk(2, %[1]s)`,
	`sum(%[1]s)`,
	`sum by (app) (%[1]s)`,
	`max by (app) (%[1]s)`,
	`avg(%[1]s)`,
	`count by (pod) (%[1]s)`,
	`label_replace(%[1]s, "dst", "$1", "app", "(.*)")`,
	`%[1]s * 2`,
	`2 * %[1]s`,
	`%[1]s > 0`,
	`%[1]s + %[1]s`,
	`%[1]s and %[1]s`,
	`%[1]s or %[1]s`,
	`%[1]s unless %[1]s`,
	`%[1]s and count_over_time({job="ok"}[1m])`,
	`count_over_time({job="ok"}[1m]) or %[1]s`,
	`sort(sum by (app) (%[1]s))`,
	`topk(1, sum by (app) (%[1]s))`,
	`sum by (app) (count by (app, pod) (%[1]s))`,
	`max(sort(%[1]s))`,
	`sum by (app) (%[1]s) / on (app) sum by (app) (%[1]s)`,
	`sort(sum(%[1]s))`,
	`topk(1, sum(%[1]s))`,
	`sum(%[1]s) + sum(%[1]s)`,
	`sum(sum by (app) (%[1]s))`,
	`label_replace(sum(%[1]s), "dst", "x", "dst", ".*")`,
	`sort(sum by (detected_level) (%[1]s))`,
	`topk(1, sum by (detected_level) (%[1]s))`,
	`max by (detected_level) (%[1]s)`,
}

// parserErrorControlWrappers are the wrappers the controls run under.
var parserErrorControlWrappers = []string{
	`%[1]s`,
	`sort(%[1]s)`,
	`topk(2, %[1]s)`,
	`sum by (app) (%[1]s)`,
	`label_replace(%[1]s, "dst", "$1", "app", "(.*)")`,
	`%[1]s + %[1]s`,
}

// parserErrorOffsets are the range forms of a leaf: as written, and with an offset that moves an instant request
// into the stretch of rejected lines.
var parserErrorOffsets = []string{"[1m]", "[1m] offset 40m"}

// parserErrorKnown are the runs that differ from Loki, by query and request kind, with the registry case holding
// the difference open. Every run of a `logfmt --strict` leaf Loki fails is listed as well (parserErrorKnownFor).
var parserErrorKnown = func() map[string]map[string]string {
	known := map[string]map[string]string{}
	all := []string{"instant", "step=range", "step<range", "step>range"}
	for q, kinds := range map[string][]string{
		// An unwrap conversion error in an operand of `and`/`or` whose other operand reads a clean stream: the
		// conversion check does not see the rows the operand reads.
		`count_over_time({job="ok"}[1m]) or sum_over_time({job="sp", enc="logfmt"} | logfmt | unwrap n [1m] offset 40m)`:  all,
		`count_over_time({job="ok"}[1m]) or sum_over_time({job="sp", enc="logfmt"} | logfmt | unwrap n [1m])`:             all[1:],
		`sum_over_time({job="sp", enc="logfmt"} | logfmt | unwrap n [1m] offset 40m) and count_over_time({job="ok"}[1m])`: all,
	} {
		known[q] = map[string]string{}
		for _, kind := range kinds {
			known[q][kind] = "semantics/unwrap-conversion-error-set-operand"
		}
	}
	return known
}()

// parserErrorStrictCase holds open the strict logfmt parser's errors: VictoriaLogs' unpack_logfmt accepts every
// line, and no check marks the lines Loki's strict parser rejects, so the proxy answers where Loki fails.
const parserErrorStrictCase = "semantics/logfmt-strict-parser-error-not-raised"

// parserErrorHazardCase holds open the lines both parsers read as JSON while Loki's rejects them (a value NaN, .5 or
// +1, an invalid escape in a key): the mark relies on VictoriaLogs' parser, and the filter that finds such lines
// costs every request a regular expression per line.
const parserErrorHazardCase = "semantics/json-parser-error-hazard-lines-not-marked"

// parserErrorKnownFor is the registry case holding a run's difference open, "" when the run must match Loki.
func parserErrorKnownFor(q, kind string, loki metricLokiAnswer) string {
	if strings.Contains(q, "logfmt --strict") && loki.Status == 400 {
		return parserErrorStrictCase
	}
	if strings.Contains(q, `{job="hz"`) && loki.Status == 400 && (strings.HasPrefix(q, "sort(") || strings.HasPrefix(q, "topk(")) {
		// The ordered JSON evaluator, which answers the other wrappers' leaves, finds such lines with a probe.
		return parserErrorHazardCase
	}
	return parserErrorKnown[q][kind]
}

// parserErrorQueries are the differential's queries over parserErrorFixture.
//
// refs names, per query whose leaf Loki fails, the leaf's reference query: the leaf with `| __error__!=""` after its
// last stage, so Loki keeps the failing samples as series (with __error__ and __error_details__) instead of failing.
// Its answer at the first step that has a series is the set of series Loki can name in its error at that step.
func parserErrorQueries(t *testing.T) (queries []metricQuery, refs map[string]string) {
	t.Helper()
	fix := parserErrorFixture()
	var out []metricQuery
	refs = map[string]string{}
	seen := map[string]bool{}
	add := func(q string) {
		if seen[q] {
			return
		}
		seen[q] = true
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	for _, leaf := range parserErrorLeaves {
		for _, rng := range parserErrorOffsets {
			l := strings.Replace(leaf, "[1m]", rng, 1)
			ref := strings.Replace(l, " [1m]", ` | __error__!="" [1m]`, 1)
			add(ref)
			for _, w := range parserErrorWrappers {
				q := fmt.Sprintf(w, l)
				add(q)
				// A sum with a grouping directly over the leaf pushes its labels down as Loki's parser hints, so the
				// failing samples hold fewer extracted labels than the reference's.
				if !strings.Contains(w, ") (%[1]s)") || !strings.Contains(w, "sum by (") {
					refs[q] = ref
				}
			}
		}
	}
	for _, leaf := range append(append(append([]string(nil), parserErrorControls...), parserErrorSpellings...), parserErrorHazards...) {
		for _, w := range parserErrorControlWrappers {
			add(fmt.Sprintf(w, leaf))
		}
	}
	for _, leaf := range parserErrorSpellings {
		ref := strings.Replace(leaf, " [1m]", ` | __error__!="" [1m]`, 1)
		add(ref)
		for _, w := range parserErrorControlWrappers {
			if q := fmt.Sprintf(w, leaf); !strings.Contains(w, "sum by (") {
				refs[q] = ref
			}
		}
	}
	return out, refs
}

// parserErrorFixture is the data of every query.
func parserErrorFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "sp", "enc": "json", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "sp", "enc": "json", "app": "web", "pod": "b", "service_name": "web"},
		{"job": "sp", "enc": "logfmt", "app": "api", "pod": "c", "service_name": "api"},
		{"job": "sp", "enc": "logfmt", "app": "web", "pod": "d", "service_name": "web"},
		{"job": "ok", "enc": "json", "app": "api", "pod": "e", "service_name": "api"},
		{"job": "ok", "enc": "json", "app": "web", "pod": "f", "service_name": "web"},
		{"job": "oke", "enc": "json", "app": "api", "pod": "g", "service_name": "api"},
		// Level labels in every spelling Loki reads, on streams with lines the parser rejects: the series an error
		// names holds them as Loki renders them.
		{"job": "sp", "enc": "json", "app": "api", "pod": "h", "service_name": "api", "LEVEL": "warning"},
		{"job": "sp", "enc": "json", "app": "web", "pod": "i", "service_name": "web", "level": "ERROR"},
		{"job": "sp", "enc": "json", "app": "api", "pod": "j", "service_name": "api", "severity": "error"},
		{"job": "sp", "enc": "json", "app": "web", "pod": "k", "service_name": "web", "lvl": "x", "level": "Debug"},
		{"job": "hz", "enc": "json", "app": "api", "pod": "l", "service_name": "api"},
	}}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	badFrom, badTo := metricEnd.Add(-45*time.Minute), metricEnd.Add(-35*time.Minute)
	valid := map[string]func(i int) string{
		"json":   func(i int) string { return `{"status":200,"n":` + strconv.Itoa(i%7+1) + `,"msg":"ok"}` },
		"logfmt": func(i int) string { return `level=info status=200 n=` + strconv.Itoa(i%7+1) + ` msg=ok` },
	}
	bad := map[string][]string{
		"json":   {`plain text line`, `{"status":500,"n":1`},
		"logfmt": {`msg="unterminated status=500`, `level=warn status=500 n=abc msg=bad`},
	}
	for s, labels := range f.Streams {
		md := map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}
		for i := 0; ; i++ {
			// Each stream on a phase of its own, half a millisecond past a millisecond (every bound and step is whole
			// milliseconds, so no line sits on a window edge).
			at := first.Add(time.Duration(i)*97*time.Second + time.Duration(s)*13*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			line := valid[labels["enc"]](i)
			switch {
			case labels["job"] == "oke" && i%3 == 0:
				line = "{}" // spliced without the comma that would break it: not marked
			case labels["job"] == "oke" && i%3 == 1:
				line = `{ }` // VictoriaLogs rejects the spliced line, Loki parses it: marked, and Loki answers
			case labels["job"] == "hz" && i%4 == 0:
				line = `{"status":NaN,"n":1}` // both parsers read it as JSON, Loki's rejects it
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line, Metadata: md})
		}
		if labels["job"] != "sp" {
			continue
		}
		for i := 0; ; i++ {
			at := badFrom.Add(time.Duration(i)*23*time.Second + time.Duration(s)*3*time.Second + 300*time.Millisecond + 500*time.Microsecond)
			if !at.Before(badTo) {
				break
			}
			lines := bad[labels["enc"]]
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: lines[i%len(lines)], Metadata: md})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// TestParserErrorsUnderWrappersMatchLoki replays the recorded answers through the handlers and holds every run to
// Loki's status and error text, or to its listed difference.
//
// conformance: semantics/json-parser-error-under-wrappers, semantics/pipeline-error-text-parity, semantics/logfmt-strict-parser-error-not-raised, semantics/unwrap-conversion-error-set-operand, semantics/json-parser-error-hazard-lines-not-marked
func TestParserErrorsUnderWrappersMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries, refs := parserErrorQueries(t)
	isRef := map[string]bool{}
	for _, ref := range refs {
		isRef[ref] = true
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, parserErrorAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, parserErrorAnswersPath)
	type row struct{ match, runs int }
	table := map[string]*row{}
	seenKnown := map[string]bool{}
	lokiErrors, answered, refetched, named := 0, 0, 0, 0
	// markedTexts are the marked VictoriaLogs texts the runs sent, for VictoriaLogs' own parser (recording only).
	markedTexts := map[string]bool{}
	// retryBudget bounds the time the retries below sleep in one run of the test.
	retryBudget, retries := 30*time.Second, 0
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		if isRef[mq.q] {
			continue // a reference: its Loki answers are read by the queries over its leaf
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				loki, ok := rec.Loki[mr.kind]
				if !ok {
					t.Fatalf("no Loki answer for %s %s: record them again", mr.kind, mq.q)
				}
				if loki.Status == 400 {
					lokiErrors++
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				// A run whose request set depends on process-wide state (a proxy pointer or a test server port reused by
				// an earlier run) or that met a host out of ephemeral ports is run again on a fresh proxy; a real
				// difference stays through the retries.
				for retry := 0; retry < 3 && retryBudget > 0 && (len(answerRequests(run.missing)) > 0 || run.status == 500 && loki.Status != 500); retry++ {
					wait := time.Duration(1+retry*retry*4) * time.Second // 1s, 5s, 17s: a TIME_WAIT on a busy host
					retryBudget -= wait
					retries++
					time.Sleep(wait)
					run = runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				}
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				// The leaves over clean streams are held to Loki's totals too: their reads are marked, and job=oke's are
				// read again unmarked. topk keeps per series, which the owner-kept stream identity of parser metrics
				// (parsed-identity/dynamic-key-parsers) splits otherwise than Loki; it is held to status.
				values := (strings.Contains(mq.q, `{job="ok", enc="json"}`) || strings.Contains(mq.q, `{job="oke"`)) && !strings.Contains(mq.q, "topk")
				for _, k := range run.sent {
					if _, raw, ok := strings.Cut(k, "?"); ok {
						if form, err := url.ParseQuery(raw); err == nil && strings.Contains(form.Get("query"), "__vlp") {
							markedTexts[form.Get("query")] = true
						}
					}
				}
				diff := parserErrorDiff(loki, run, values)
				if diff == "" && loki.Status == 400 && refs[mq.q] != "" {
					diff = parserErrorSeriesNamed(loki, run, answers[refs[mq.q]].Loki[mr.kind])
					if diff == "" {
						named++
					}
				}
				if loki.Status == 200 && run.status == 200 {
					answered++
					lookups, again := parserErrorExtraCalls(run.sent)
					if (lookups > 0 || again > 0) && !strings.Contains(mq.q, `job="oke"`) {
						t.Errorf("%s %s %s: Loki answers, and the proxy sent %d parser error lookups and read %d stats answers again", prof.name, mr.kind, mq.q, lookups, again)
					}
					refetched += again
				}
				key := prof.name + " " + mr.kind
				if table[key] == nil {
					table[key] = &row{}
				}
				table[key].runs++
				known := parserErrorKnownFor(mq.q, mr.kind, loki)
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case diff == "" && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from parserErrorKnown (%s)", prof.name, mr.kind, mq.q, known)
				case diff == "":
					table[key].match++
				case known == "":
					t.Errorf("%s %s %s: %s", prof.name, mr.kind, mq.q, diff)
				}
			}
		}
	}
	// The `{ }` lines of job=oke are marked, and Loki parses them: the marked answers are read again unmarked.
	if refetched == 0 {
		t.Error("no stats answer of job=oke was read again: its `{ }` lines no longer reach the mark")
	}
	for q, kinds := range parserErrorKnown {
		for kind := range kinds {
			if !seenKnown[q+"\x00"+kind] {
				t.Errorf("parserErrorKnown lists %s %s, which the differential does not run", kind, q)
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
		fmt.Fprintf(&b, "\n  %-36s %4d of %4d match Loki", k, table[k].match, table[k].runs)
	}
	t.Logf("parser errors under wrappers against Loki (%d Loki 400 answers, %d naming a series Loki fails on at its first failing step; %d runs Loki answers, without a lookup but job=oke's; %d stats answers of job=oke read again; %d runs retried):%s",
		lokiErrors, named, answered, refetched, retries, b.String())
	if named == 0 {
		t.Error("no error run was held to Loki's failing series")
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		parserErrorVLRoundTrip(t, markedTexts)
	}
}

// parserErrorDiff is how a run differs from Loki's answer in what the differential holds: the status, and for an
// error its text with the series it names masked, and with values an answer's total per timestamp (the sum over its
// series, which the owner-kept stream identity of parser metrics does not change); "" when it does not. Loki names the series of the first failing
// sample its evaluation meets, and among the series failing at one step its choice changes from run to run (two
// recordings of one query name different series), so the series is held to its form instead: a label set holding
// __error__ of the error's own type (parserErrorSeriesForm).
func parserErrorDiff(loki metricLokiAnswer, run metricHandlerRun, values bool) string {
	if loki.Status != run.status {
		return fmt.Sprintf("status %d where Loki answers %d: proxy %.160q, Loki %.160q", run.status, loki.Status, proxyErrorText(run.body), loki.Error)
	}
	if loki.Status == 200 && !values {
		return ""
	}
	if loki.Status == 200 {
		return parserErrorTotalsDiff(loki.Body, []byte(run.body))
	}
	got, want := strings.TrimSpace(proxyErrorText(run.body)), strings.TrimSpace(loki.Error)
	gotMasked, gotSeries := maskPipelineErrorSeries(got)
	wantMasked, _ := maskPipelineErrorSeries(want)
	if gotMasked != wantMasked {
		return fmt.Sprintf("another error text: proxy %.200q, Loki %.200q", got, want)
	}
	if gotSeries != "" {
		if problem := parserErrorSeriesForm(gotMasked, gotSeries); problem != "" {
			return "names a series " + problem + ": " + gotSeries
		}
	}
	return ""
}

// maskPipelineErrorSeries replaces the series of a Loki pipeline error text with a placeholder and returns it.
func maskPipelineErrorSeries(text string) (masked, series string) {
	const open = "for series: '"
	at := strings.Index(text, open)
	if !strings.HasPrefix(text, "pipeline error: '") || at < 0 {
		return text, ""
	}
	rest := text[at+len(open):]
	end := strings.Index(rest, "}'.")
	if end < 0 {
		return text, ""
	}
	return text[:at+len(open)] + "{…" + rest[end:], rest[:end+1]
}

// parserErrorSeriesForm checks a named series: Loki's label set rendering (names in byte order, `, ` between pairs,
// values Go-quoted) holding __error__ of the error's type and __error_details__; "" when it holds.
func parserErrorSeriesForm(masked, series string) string {
	typ := strings.TrimPrefix(masked, "pipeline error: '")
	typ = typ[:strings.Index(typ, "'")]
	labels, err := parserErrorLabels(series)
	if err != nil {
		return "Loki's parser does not read as a selector (" + err.Error() + ")"
	}
	if labels["__error__"] != typ || labels["__error_details__"] == "" {
		return "without __error__ " + typ + " and its details"
	}
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = name + "=" + strconv.Quote(labels[name])
	}
	if want := "{" + strings.Join(parts, ", ") + "}"; series != want {
		return "not rendered as Loki renders a label set (" + want + ")"
	}
	return ""
}

// parserErrorExtraCalls counts, among the request keys a run sent, the JSON parser error lookups and the stats
// queries sent twice, once with the mark and once without it (a marked answer read again).
func parserErrorExtraCalls(sent []string) (lookups, again int) {
	marked := map[string]bool{}
	var plain []string
	for _, k := range sent {
		path, raw, _ := strings.Cut(k, "?")
		form, err := url.ParseQuery(raw)
		if err != nil {
			continue
		}
		q := form.Get("query")
		switch {
		case path == "/select/logsql/query" && strings.Contains(q, "| filter NOT __vlp:*"):
			lookups++
		case strings.HasPrefix(path, "/select/logsql/stats_query") && strings.Contains(q, "__vlp"):
			form.Set("query", unmarkParserErrorQuery(q))
			marked[path+"?"+form.Encode()] = true
		case strings.HasPrefix(path, "/select/logsql/stats_query"):
			form.Set("query", normalizeLogsQL(q))
			plain = append(plain, path+"?"+form.Encode())
		}
	}
	for _, k := range plain {
		path, raw, _ := strings.Cut(k, "?")
		form, _ := url.ParseQuery(raw)
		if marked[path+"?"+form.Encode()] {
			again++
		}
	}
	return lookups, again
}

// unmarkParserErrorQuery is a marked stats query as the metric sends it without the mark: the splice pipes before
// its unpack_json, the restore after it, the sentinel in the field list and in the stats groups taken out.
func unmarkParserErrorQuery(q string) string {
	const splice, unpack = ` | replace ("{", "{\"__vlp\":1,")`, " | unpack_json"
	const restore, restored = ` | replace ("{\"__vlp\":1,", "{")`, `"{}") at _msg limit 1`
	if at := strings.Index(q, splice); at >= 0 {
		if end := strings.Index(q[at:], unpack); end >= 0 {
			q = q[:at] + q[at+end:]
		}
	}
	if at := strings.Index(q, restore); at >= 0 {
		if end := strings.Index(q[at:], restored); end >= 0 {
			q = q[:at] + q[at+end+len(restored):]
		}
	}
	q = strings.ReplaceAll(q, "stats by (__vlp) ", "stats ")
	return normalizeLogsQL(strings.ReplaceAll(q, ", __vlp)", ")"))
}

// normalizeLogsQL is a query as the LogsQL printer writes it (the mark is written by the printer).
func normalizeLogsQL(q string) string {
	if parsed, err := logsql.Parse(q); err == nil {
		return parsed.String()
	}
	return q
}

// parserErrorTotalsDiff compares two answers' totals per timestamp: the sum of every series' value there.
func parserErrorTotalsDiff(lokiBody, proxyBody []byte) string {
	totals := func(body []byte) (map[string]float64, error) {
		res, err := decodeMetricBody(body)
		if err != nil {
			return nil, err
		}
		out := map[string]float64{}
		for _, series := range res.series {
			for ts, v := range series.points {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, err
				}
				out[ts] += f
			}
		}
		return out, nil
	}
	want, err := totals(lokiBody)
	if err != nil {
		return "Loki's answer does not decode: " + err.Error()
	}
	got, err := totals(proxyBody)
	if err != nil {
		return "the answer does not decode: " + err.Error()
	}
	if len(got) != len(want) {
		return fmt.Sprintf("%d points, Loki %d", len(got), len(want))
	}
	for ts, w := range want {
		g, ok := got[ts]
		if !ok || math.Abs(g-w) > metricFloatTolerance*math.Max(1, math.Abs(w)) {
			return fmt.Sprintf("total %v at %s, Loki %v", g, ts, w)
		}
	}
	return ""
}

// parserErrorSeriesNamed checks the series a matching error names against Loki's reference answer: it must be one of
// the failing series at the first step where the leaf has one (Loki fails at that step, naming one of them; which one
// changes from run to run). "" when it is.
func parserErrorSeriesNamed(loki metricLokiAnswer, run metricHandlerRun, ref metricLokiAnswer) string {
	_, named := maskPipelineErrorSeries(strings.TrimSpace(proxyErrorText(run.body)))
	if named == "" || ref.Status != 200 {
		return ""
	}
	got, err := parserErrorLabels(named)
	if err != nil {
		return "names a series Loki's parser does not read: " + named
	}
	res, err := decodeMetricBody(ref.Body)
	if err != nil {
		return "the reference answer does not decode: " + err.Error()
	}
	first := ""
	for _, series := range res.series {
		for ts := range series.points {
			if first == "" || parserErrorTSLess(ts, first) {
				first = ts
			}
		}
	}
	var candidates []string
	for _, series := range res.series {
		if _, ok := series.points[first]; !ok {
			continue
		}
		if labelsKey(series.labels) == labelsKey(got) {
			return ""
		}
		candidates = append(candidates, labelsKey(series.labels))
	}
	sort.Strings(candidates)
	return fmt.Sprintf("names %s, not a series failing at Loki's first failing step %s: %.400s", labelsKey(got), first, strings.Join(candidates, " | "))
}

// parserErrorTSLess orders two answer timestamps (seconds, as Loki writes them).
func parserErrorTSLess(a, b string) bool {
	x, _ := strconv.ParseFloat(a, 64)
	y, _ := strconv.ParseFloat(b, 64)
	return x < y
}

// parserErrorLabels reads the label set a pipeline error names.
func parserErrorLabels(series string) (map[string]string, error) {
	expr, err := logql.Parse(series)
	if err != nil {
		return nil, err
	}
	lq, ok := expr.(*logql.LogQuery)
	if !ok {
		return nil, fmt.Errorf("not a selector")
	}
	out := map[string]string{}
	for _, m := range lq.Selector.Matchers {
		out[m.Name] = m.Value
	}
	return out, nil
}

// parserErrorVLRoundTrip holds every marked text to VictoriaLogs' own parser, v1.49.0 and v1.53.0: it accepts the text
// and its print parses back to the same query (the clone round trip every text the proxy sends must survive).
func parserErrorVLRoundTrip(t *testing.T, texts map[string]bool) {
	t.Helper()
	list := make([]string, 0, len(texts))
	for tx := range texts {
		list = append(list, tx)
	}
	sort.Strings(list)
	work := t.TempDir()
	in := filepath.Join(work, "in.jsonl")
	var rows []map[string]any
	for i, tx := range list {
		rows = append(rows, map[string]any{"id": strconv.Itoa(i), "q": tx})
	}
	writeJSONLines(t, in, rows)
	type verdict struct {
		ID     string `json:"id"`
		Accept bool   `json:"accept"`
		Error  string `json:"error"`
		OK     bool   `json:"ok"`
		Panic  string `json:"panic"`
	}
	for _, v := range sweepVersions {
		modfile := ""
		if file := oracleModfile(v, goldenVersions(t)[0]); file != "go.mod" {
			modfile = file
		}
		bin, err := oraclerun.Build(oracleDir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(work, v+".jsonl")
		if err := oraclerun.Run(bin, "roundtrip", "-in", in, "-out", out); err != nil {
			t.Fatal(err)
		}
		bad := 0
		for _, r := range readJSONLines[verdict](t, out) {
			if !r.Accept || !r.OK || r.Panic != "" {
				i, _ := strconv.Atoi(r.ID)
				if bad++; bad <= 5 {
					t.Errorf("VictoriaLogs %s: %s: accept %v, round trip %v, %s%s", v, list[i], r.Accept, r.OK, r.Error, r.Panic)
				}
			}
		}
		t.Logf("VictoriaLogs %s parses and round-trips %d of %d marked texts", v, len(list)-bad, len(list))
	}
}
