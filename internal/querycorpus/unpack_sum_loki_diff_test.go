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
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The Loki differential of the unpack aggregations the query plan lowers (internal/queryplan/lower unpack.go): real
// Loki (the engine oracle) and real VictoriaLogs (the serve oracle, v1.54.0 and v1.49.0) over the same data, through
// the proxy's real query and query_range handlers, as the metric differential runs them.
//
//   - Data (unpackSumFixture): job=pk holds only JSON objects (packed lines, lines without an `_entry`, an empty,
//     numeric or object `_entry`, values that are not strings, a packed key that collides with a stream label);
//     job=bad adds lines that are no JSON object in a ten-minute stretch, which Loki's unpack marks with __error__.
//   - Open differences, both served from the string translator and unchanged by the plan's lowering: a range
//     aggregation over lines Loki's unpack rejects answers where Loki fails
//     (semantics/unpack-parser-error-not-raised), and an instant `sum(rate(… | unpack …))` answers one series per
//     stream (semantics/unpack-instant-sum-rate-per-stream).
//   - A sum with no clause adds up the series, so Loki's answer is held series by series and value by value (it
//     answers over job=bad too: Loki parses no line for a sum that needs no label). A range aggregation of its own
//     splits the series by the promoted labels (the owner-kept stream identity of parser metrics), so it is held to
//     the total per timestamp.
//
// The answers are recorded (unpackSumAnswersPath; LVP_RUN_ORACLES=1 records them again, with LVP_RECORD_PACE=1s).

const unpackSumAnswersPath = "testdata/unpack-sum-loki-diff.jsonl.gz"

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
	return out
}

func unpackSumFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "pk", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "pk", "app": "web", "pod": "b", "service_name": "web"},
		{"job": "bad", "app": "api", "pod": "c", "service_name": "api"},
		{"job": "bad", "app": "web", "pod": "d", "service_name": "web"},
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
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: valid[(i+s)%len(valid)], Metadata: md})
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
// conformance: semantics/translator-writes-unpack-as-unpack-json, semantics/unpack-parser-error-not-raised, semantics/unpack-instant-sum-rate-per-stream
func TestUnpackSumMatchesLoki(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	fix := unpackSumFixture()
	var queries []metricQuery
	for _, q := range unpackSumQueries() {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		// The plan lowers every query but a sum of a rate, which stays delegated for its instant answers.
		if _, err := lower.PlanQuery(q, p, lower.Profile{}); (err == nil) == strings.HasPrefix(q, "sum(rate(") {
			t.Fatalf("%s: lowered %v: %v", q, err == nil, err)
		}
		queries = append(queries, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, unpackSumAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, unpackSumAnswersPath)
	match, runs, sums, failing, split := 0, 0, 0, 0, 0
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
				diff := parserErrorDiff(loki, run, true)
				sum := strings.HasPrefix(mq.q, "sum(")
				if sum && diff == "" && loki.Status == 200 {
					if d := compareMetric(loki, run); len(d.classes) != 0 {
						diff = fmt.Sprintf("another answer: %v", d.classes)
					}
				}
				// The open differences: a range aggregation of its own over lines Loki's unpack rejects, and an instant
				// sum of a rate over an unpack, which answers per stream.
				failsInLoki := !sum && strings.Contains(mq.q, `{job="bad"}`) && loki.Status == 400
				perStream := mr.instant && strings.HasPrefix(mq.q, "sum(rate(") && strings.HasSuffix(mq.q, "| unpack [1m]))")
				switch {
				case (failsInLoki || perStream) && diff == "":
					t.Errorf("%s %s %s matches Loki now: take it off the open differences", prof.name, mr.kind, mq.q)
				case failsInLoki && run.status == 200:
					failing++
				case perStream && loki.Status == 200 && run.status == 200:
					split++
				case diff != "":
					t.Errorf("%s %s %s (Loki %d, %d VictoriaLogs requests): %.600s", prof.name, mr.kind, mq.q, loki.Status, len(run.sent), diff)
				default:
					match++
					if sum && loki.Status == 200 {
						sums++
					}
				}
			}
		}
	}
	t.Logf("unpack aggregations against Loki: %d of %d runs match (%d of them sums), %d answer where Loki fails, %d answer an instant sum of a rate per stream", match, runs, sums, failing, split)
	if sums == 0 || failing == 0 || split == 0 {
		t.Error("no sum, or no run Loki fails, reached the comparison")
	}
}
