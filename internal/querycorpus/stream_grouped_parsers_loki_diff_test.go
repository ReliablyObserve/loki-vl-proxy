package querycorpus_test

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The Loki differential of sums grouped by stream labels over json and logfmt parsers
// (semantics/stream-grouped-sum-skips-parsers): real Loki (the engine oracle) and real VictoriaLogs (the serve
// oracle, v1.54.0 and v1.49.0) over the same data, through the proxy's real query and query_range handlers.

const streamGroupedAnswersPath = "testdata/stream-grouped-parsers-loki-diff.jsonl.gz"

// streamGroupedLines are lines each parser reads differently: JSON objects whose level key differs from the stream
// label, logfmt lines with a level, broken JSON, plain text, an empty level, a nested level and a logfmt pair inside
// a JSON value.
var streamGroupedLines = map[string][]string{
	"mixed": {
		`{"level":"error","route":"/a"}`,
		`level=debug route=/b`,
		`{"route":"/c"}`,
		`plain text line`,
		`{"level":"x"`,
		`{"level":"","route":"/d"}`,
		`level= route=/e`,
		`{"nested":{"level":"n"},"route":"/f"}`,
		`{"level":"error","msg":"level=warn x=1"}`,
		`{"level_extracted":"y","route":"/g"}`,
	},
	"json": {
		`{"level":"error","route":"/a"}`,
		`{"route":"/c"}`,
		`{"level":"","route":"/d"}`,
		`{"nested":{"level":"n"},"route":"/f"}`,
		`{"level":"warn","app":"zz"}`,
	},
	// The bench generator's api-gateway lines: JSON written with spaces after its separators, a nested object.
	"spaced": {
		`{"service": {"name": "api-gateway"}, "method": "GET", "path": "/api/v1/users", "status": 200, "level": "info", "user_id": "u1"}`,
		`{"service": {"name": "api-gateway"}, "method": "POST", "path": "/api/v1/orders/7", "status": 404, "level": "warn", "error": "user not found"}`,
		`{"service": {"name": "api-gateway"}, "method": "PUT", "path": "/health", "status": 503, "level": "error", "duration_ms": 1200}`,
	},
	"logfmt": {
		`level=debug route=/b`,
		`level= route=/e`,
		`route=/c app=zz`,
		`level=error msg="a b"`,
	},
}

// streamGroupedStream is a stream of the fixture: its labels, its lines and when it has lines.
type streamGroupedStream struct {
	labels   map[string]string
	lines    string
	metadata map[string]string
	// from and to bound the stream's lines, as offsets before metricEnd (to < from).
	from, to time.Duration
}

func streamGroupedStreams() []streamGroupedStream {
	all := metricWindow + 50*time.Minute
	return []streamGroupedStream{
		{labels: map[string]string{"job": "sg", "app": "a", "level": "info"}, lines: "mixed", from: all},
		{labels: map[string]string{"job": "sg", "app": "b", "level": "warn"}, lines: "mixed", from: all},
		// job=lt: a stream without the label that has lines only long before the window, which an offset reaches.
		{labels: map[string]string{"job": "lt", "app": "c", "level": "error"}, lines: "mixed", from: all},
		{labels: map[string]string{"job": "lt", "app": "d"}, lines: "mixed", from: all, to: metricWindow + 20*time.Minute},
		// job=ln: a stream without the label that starts inside the window.
		{labels: map[string]string{"job": "ln", "app": "e", "level": "debug"}, lines: "mixed", from: all},
		{labels: map[string]string{"job": "ln", "app": "f"}, lines: "mixed", from: 10 * time.Minute},
		// job=nl: no stream has the label.
		{labels: map[string]string{"job": "nl", "app": "g"}, lines: "mixed", from: all},
		// job=sm: the label is a stream label and a structured metadata field; job=so: structured metadata only.
		{labels: map[string]string{"job": "sm", "app": "h", "level": "info"}, lines: "mixed", from: all, metadata: map[string]string{"level": "meta"}},
		{labels: map[string]string{"job": "so", "app": "i"}, lines: "mixed", from: all, metadata: map[string]string{"level": "meta"}},
		// job=sp: the bench generator's spaced JSON, level label equal to the line's.
		{labels: map[string]string{"job": "sp", "app": "m", "level": "info"}, lines: "spaced", from: all},
		{labels: map[string]string{"job": "sp", "app": "n", "level": "warn"}, lines: "spaced", from: all},
		// job=js: valid JSON only; job=lf: logfmt only.
		{labels: map[string]string{"job": "js", "app": "j", "level": "info"}, lines: "json", from: all},
		{labels: map[string]string{"job": "js", "app": "k", "level": "warn"}, lines: "json", from: all},
		{labels: map[string]string{"job": "lf", "app": "l", "level": "info"}, lines: "logfmt", from: all},
	}
}

func streamGroupedFixture() metricFixture {
	var f metricFixture
	streams := streamGroupedStreams()
	for s, st := range streams {
		f.Streams = append(f.Streams, st.labels)
		lines := streamGroupedLines[st.lines]
		for i := 0; ; i++ {
			at := metricEnd.Add(-st.from).Add(time.Duration(i)*47*time.Second + time.Duration(s)*7*time.Second + time.Duration(i*7919%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd.Add(-st.to)) {
				break
			}
			md := map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}
			for k, v := range st.metadata {
				md[k] = v
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: lines[(i+s)%len(lines)], Metadata: md})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

const streamGroupedDrop = ` | drop __error__, __error_details__`

// streamGroupedQueries are the queries of the differential.
func streamGroupedQueries() []string {
	sum := func(by, fn, s, pipe, rng string) string {
		return `sum by (` + by + `) (` + fn + `(` + s + pipe + ` [1m]` + rng + `))`
	}
	var out []string
	for _, s := range []string{`{job="sg"}`, `{job="lt"}`, `{job="ln"}`, `{job="nl"}`, `{job="sm"}`, `{job="so"}`, `{job="js"}`, `{job="lf"}`, `{job="sp"}`} {
		out = append(out,
			sum("level", "count_over_time", s, ` | json | logfmt`+streamGroupedDrop, ""),
			sum("level", "count_over_time", s, ` | json | logfmt`, ""),
		)
	}
	for _, s := range []string{`{job="sg"}`, `{job="js"}`, `{job="lf"}`, `{job="sp"}`} {
		out = append(out,
			sum("level", "count_over_time", s, ` | logfmt | json`+streamGroupedDrop, ""),
			sum("level", "count_over_time", s, ` | logfmt | json`, ""),
			sum("level", "rate", s, ` | json | logfmt`+streamGroupedDrop, ""),
			sum("level", "count_over_time", s, ` | json | logfmt | drop __error__`, ""),
			sum("level, app", "count_over_time", s, ` | json | logfmt`+streamGroupedDrop, ""),
			sum("app", "count_over_time", s, ` | logfmt | json`+streamGroupedDrop, ""),
		)
	}
	out = append(out,
		sum("level", "count_over_time", `{job="sg"} |= "route"`, ` | json | logfmt`+streamGroupedDrop, ""),
		sum("level", "rate", `{job="sg"}`, ` | logfmt | json`+streamGroupedDrop, ""),
		sum("job", "count_over_time", `{job="sg"}`, ` | json | logfmt`+streamGroupedDrop, ""),
		sum("level", "count_over_time", `{job="lt"}`, ` | json | logfmt`+streamGroupedDrop, " offset 40m"),
		sum("level", "count_over_time", `{job="sg"}`, ` | json | logfmt`+streamGroupedDrop, " offset 40m"),
		sum("level", "count_over_time", `{job=~"sg|js"}`, ` | json | logfmt`+streamGroupedDrop, ""),
		`topk(1, `+sum("level", "count_over_time", `{job="sg"}`, ` | json | logfmt`+streamGroupedDrop, "")+`)`,
		`sort_desc(`+sum("level", "count_over_time", `{job="sg"}`, ` | logfmt | json`+streamGroupedDrop, "")+`)`,
		sum("level", "count_over_time", `{job="sg"}`, ` | json | logfmt`+streamGroupedDrop, "")+` * 2`,
		`label_replace(`+sum("level", "count_over_time", `{job="sg"}`, ` | json | logfmt`+streamGroupedDrop, "")+`, "dst", "$1", "level", "(.*)")`,
		sum("level", "bytes_rate", `{job="sg"}`, ` | logfmt | json`+streamGroupedDrop, ""),
		sum("level", "count_over_time", `{job="sm"}`, ` | logfmt | json`+streamGroupedDrop, ""),
		sum("level", "count_over_time", `{job="so"}`, ` | logfmt | json`+streamGroupedDrop, ""),
		// The cut queries themselves, where the label is a stream label and a structured metadata field, or only the
		// latter.
		sum("level", "count_over_time", `{job="sm"}`, "", ""),
		sum("level", "count_over_time", `{job="so"}`, "", ""),
		sum("level", "count_over_time", `{job="sg"}`, "", ""),
		// Controls: a filter, the grouping label dropped, a parsed key, bytes, without.
		sum("level", "count_over_time", `{job="sg"}`, ` | json | logfmt | level!=""`+streamGroupedDrop, ""),
		sum("level", "count_over_time", `{job="sg"}`, ` | json | logfmt | drop __error__, __error_details__, level`, ""),
		sum("route", "count_over_time", `{job="sg"}`, ` | json | logfmt`+streamGroupedDrop, ""),
		sum("level, route", "count_over_time", `{job="sg"}`, ` | json | logfmt`+streamGroupedDrop, ""),
		sum("level", "bytes_over_time", `{job="sg"}`, ` | json | logfmt`+streamGroupedDrop, ""),
		`sum without (app) (count_over_time({job="sg"} | json | logfmt`+streamGroupedDrop+` [1m]))`,
	)
	return out
}

// streamGroupedKnown are the runs that differ from Loki, by query: version and request kind, with the registry case
// that holds them open. Every one differs on main too (recorded with this harness at 3ca68eca0), and none is a run
// the cut query answers: a stream without the label in the window sends the request on to the ordered JSON evaluator,
// whose answer is main's. A listed run that matches fails too, so the list only shrinks.
var streamGroupedKnown = func() map[string]unpackSumOpen {
	const twoParsers = "semantics/two-parsers-keep-one"
	ranged := []string{"v1.54.0 step=range", "v1.54.0 step<range", "v1.54.0 step>range", "v1.49.0 step=range", "v1.49.0 step<range", "v1.49.0 step>range"}
	instant := []string{"v1.54.0 instant", "v1.49.0 instant"}
	all := append(append([]string(nil), instant...), ranged...)
	return map[string]unpackSumOpen{
		// Lines without the label: Loki keeps an empty label in an instant vector and counts them where the
		// evaluator adds a point to the series with no label.
		`sum by (level) (count_over_time({job="nl"} | json | logfmt | drop __error__, __error_details__ [1m]))`:            {twoParsers, all},
		`sum by (level) (count_over_time({job="nl"} | json | logfmt [1m]))`:                                                {twoParsers, instant},
		`sum by (level) (count_over_time({job="ln"} | json | logfmt | drop __error__, __error_details__ [1m]))`:            {twoParsers, ranged},
		`sum by (level) (count_over_time({job="lt"} | json | logfmt | drop __error__, __error_details__ [1m] offset 40m))`: {twoParsers, ranged},
		`sum without (app) (count_over_time({job="sg"} | json | logfmt | drop __error__, __error_details__ [1m]))`:         {twoParsers, all},
		// Two levels count alike at some steps: Loki's pick of the one it keeps differs.
		`topk(1, sum by (level) (count_over_time({job="sg"} | json | logfmt | drop __error__, __error_details__ [1m])))`: {"semantics/topk-per-step-membership-differs", ranged},
		// The query with no parser: VictoriaLogs stores the structured metadata field over the stream label's value.
		`sum by (level) (count_over_time({job="sm"} [1m]))`: {"semantics/stream-level-shadowed-by-ingest-json-key", all},
	}
}()

// streamGroupedCutData reports a request whose window holds only lines of streams with the labels q groups by
// (level, app or job here), whose stored fields agree with them: the cut query answers it.
func streamGroupedCutData(q string, mr metricRequest) bool {
	for _, by := range []string{"by (route)", "by (level, route)"} {
		if strings.Contains(q, by) {
			return false // route is a parsed key, not a stream label
		}
	}
	for _, s := range []string{`{job="sg"}`, `{job="js"}`, `{job="lf"}`, `{job="sp"}`, `{job=~"sg|js"}`, `{job="sg"} |= "route"`} {
		if strings.Contains(q, s+" |") {
			return true
		}
	}
	// job=lt has a stream without the label whose lines end 80 minutes before the end: only a range request with
	// the offset reads them.
	return strings.Contains(q, `{job="lt"} |`) && (mr.instant || !strings.Contains(q, "offset"))
}

// TestStreamGroupedParsersMatchLoki replays the recorded answers through the handlers. Every run is held to Loki's
// status, series and values (streamGroupedKnown apart). A sum of lower.StreamGroupedParsersAt's shape, or an operand
// of a binary operation or label_replace call holding one, over data the cut query answers (streamGroupedCutData) is
// answered with no line read: no unpack pipe, no parser probe, no raw-row read, only the stats of the cut query and
// the one-row stream value check (which finds none). Every other run is answered as on main.
//
// conformance: semantics/stream-grouped-sum-skips-parsers, semantics/two-parsers-keep-one, semantics/loki-pipeline-error-rules
func TestStreamGroupedParsersMatchLoki(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	fix := streamGroupedFixture()
	var queries []metricQuery
	for _, q := range streamGroupedQueries() {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		queries = append(queries, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, streamGroupedAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, streamGroupedAnswersPath)
	match, runs, cut, open := 0, 0, 0, 0
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				served := streamGroupedServed(mq) && streamGroupedCutData(mq.q, mr)
				loki, ok := rec.Loki[mr.kind]
				if !ok {
					t.Fatalf("no Loki answer for %s %s: record them again", mr.kind, mq.q)
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				runs++
				reads := 0
				for _, sent := range run.sent {
					if streamGroupedReadsLines(sent) {
						reads++
						if served {
							t.Errorf("%s %s %s: sent %.300s", prof.name, mr.kind, mq.q, sent)
						}
					}
				}
				if served {
					cut++
				} else if reads == 0 && streamGroupedServed(mq) {
					t.Errorf("%s %s %s: answered from the cut query over data it does not answer", prof.name, mr.kind, mq.q)
				}
				diff := parserErrorDiff(loki, run, true)
				if diff == "" && loki.Status == 200 {
					if d := compareMetric(loki, run); len(d.classes) != 0 {
						diff = fmt.Sprintf("another answer: %v", d.classes)
					}
				}
				known := streamGroupedKnown[mq.q]
				runKey := prof.version + " " + mr.kind
				switch {
				case known.has(runKey) && diff == "":
					t.Errorf("%s %s %s matches Loki: remove it from streamGroupedKnown (%s)", prof.name, mr.kind, mq.q, known.id)
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
			}
		}
	}
	listed := 0
	for _, k := range streamGroupedKnown {
		listed += len(k.runs)
	}
	if open != listed {
		t.Errorf("%d runs listed in streamGroupedKnown were open, %d are listed: a listed query is no longer run", open, listed)
	}
	t.Logf("stream grouped sums against Loki: %d of %d runs match, %d listed as open; %d runs answered from the cut query", match, runs, open, cut)
	if cut == 0 {
		t.Error("no run was answered from the cut query")
	}
}

// streamGroupedServed reports a query of the cut's shape: the root (a sum or a ranking of one), or the operand of a
// binary operation with a number or of a label_replace call, which the handlers answer as a query of its own.
func streamGroupedServed(mq metricQuery) bool {
	if lower.StreamGroupedParsersAt(mq.plan) >= 0 {
		return true
	}
	switch n := mq.plan.Root.(type) {
	case *queryplan.BinOp:
		return lower.StreamGroupedParsersAt(&queryplan.Plan{Root: n.L}) >= 0
	case *queryplan.LabelReplace:
		return lower.StreamGroupedParsersAt(&queryplan.Plan{Root: n.In}) >= 0
	}
	return false
}

// streamGroupedReadsLines reports a sent request that makes VictoriaLogs parse or return lines: an unpack pipe, the
// parser probe or a raw-row read. The stream value check reads at most one row's time and none on this data; a
// ranking's stats sent to the query endpoint read no line.
func streamGroupedReadsLines(sent string) bool {
	path, raw, _ := strings.Cut(sent, "?")
	form, _ := url.ParseQuery(raw)
	q := form.Get("query")
	if strings.Contains(q, "unpack_json") || strings.Contains(q, "unpack_logfmt") {
		return true
	}
	return path == "/select/logsql/query" && !strings.HasSuffix(q, " | fields _time | limit 1") && !strings.Contains(q, " | stats ")
}
