package querycorpus_test

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// The Loki differential of a ranking over a binary operation (semantics/rank-over-binary-operation): topk, bottomk,
// sort, sort_desc, stddev and stdvar whose operand is a binary operation between two aggregations, an aggregation and a number, with
// vector matching, a bool modifier, a set operator or a chain of operations. Real Loki (the engine oracle) and real
// VictoriaLogs (the serve oracle, v1.54.0 and v1.49.0) over the same data, through the proxy's real handlers, as the
// metric differential runs them.
//
//   - Data (rankBinaryFixture): lines on five streams of job=rb, two levels on pods a and b and one on pod c, each at
//     its own pace, so the rankings change between steps and the operands' series differ.
//   - Requests: metricRequests (an instant query and query_range at, under and over the range).
//   - Shapes: rankBinaryShapes, and two queries Loki answers with an error: a many-to-one match without group_left
//     (Loki's 422 text at evaluation) and a set operator with a number operand (Loki's 400 at parse).
//
// No VictoriaLogs request may carry the marker the string translator writes for a binary operation (the operation's
// operands joined by |||, after __binary__:): that text is two queries, never one VictoriaLogs reads.
//
// The answers are recorded (rankBinaryAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match Loki,
// or differ only by a tie at the k-th place (rankedTieOnly: the values kept at every step are Loki's; Loki keeps one of
// the tied samples by the order of a Go map, so the union of the kept series over a range can differ), or be listed in
// rankBinaryKnown with the registry case that holds the difference open. The shapes over job=rbx rank pods whose
// order changes between steps, under and, unless and a division, and +Inf and NaN at every step.

const rankBinaryAnswersPath = "testdata/rank-binary-loki-diff.jsonl.gz"

// rankBinaryShapes are the query shapes: a ranking over each kind of binary operation.
var rankBinaryShapes = []string{
	`topk(2, sum by (pod) (count_over_time({job="rb"}[1m])) / sum by (pod) (count_over_time({job="rb", level="info"}[1m])))`,
	`bottomk(1, sum by (pod) (rate({job="rb"}[1m])) - on (pod) sum by (pod) (rate({job="rb", level="warn"}[1m])))`,
	`topk(1, sum by (pod, level) (count_over_time({job="rb"}[1m])) / ignoring (level) group_left sum by (pod) (count_over_time({job="rb"}[1m])))`,
	`topk by (level) (1, sum by (pod, level) (count_over_time({job="rb"}[1m])) * 2)`,
	`sort(sum by (pod) (count_over_time({job="rb"}[1m])) > bool 9)`,
	`sort_desc(sum by (pod) (count_over_time({job="rb"}[1m])) > 9)`,
	// k as large as the series: a bool answers 0 or 1, and Loki breaks a tie by the order of an unordered map.
	`topk(3, sum by (pod) (count_over_time({job="rb"}[1m])) > bool sum by (pod) (count_over_time({job="rb", level="info"}[1m])))`,
	`sort_desc(sum by (pod) (count_over_time({job="rb"}[1m])) and sum by (pod) (count_over_time({job="rb", level="warn"}[1m])))`,
	`sort(sum by (pod) (count_over_time({job="rb"}[1m])) unless sum by (pod) (count_over_time({job="rb", level="warn"}[1m])))`,
	`topk(3, sum by (pod) (count_over_time({job="rb", level="warn"}[1m])) or sum by (pod) (count_over_time({job="rb"}[1m])))`,
	`topk(2, sum by (pod) (count_over_time({job="rb"}[1m])) / sum by (pod) (count_over_time({job="rb", level="info"}[1m])) * 100)`,
	`bottomk(2, sum by (pod) (count_over_time({job="rb"}[1m])) - (sum by (pod) (count_over_time({job="rb", level="info"}[1m])) + 1))`,
	// stddev and stdvar over a binary operation take the same route.
	`stddev(sum by (pod) (count_over_time({job="rb"}[1m])) / sum by (pod) (count_over_time({job="rb", level="info"}[1m])))`,
	`stdvar by (level) (sum by (pod, level) (count_over_time({job="rb"}[1m])) * 2)`,
	// A binary operation under an aggregation or a label_replace inside the ranking.
	`topk(2, sum by (pod) (count_over_time({job="rb"}[1m]) * count_over_time({job="rb", level="info"}[1m])))`,
	`sort(label_replace(sum by (pod) (count_over_time({job="rb"}[1m])) / sum by (pod) (count_over_time({job="rb", level="info"}[1m])), "p", "$1", "pod", "(.*)"))`,
	// A parser error inside an operand: the lines are logfmt, so | json marks every sample with __error__.
	`topk(2, count_over_time({job="rb"} | json [1m]) > 0)`,
	// The winners change between steps, under a set operator and a division.
	`bottomk(1, sum by (pod) (count_over_time({job="rbx"}[1m])) and sum by (pod) (count_over_time({job="rbx"} |= "x" [1m])))`,
	`bottomk(1, sum by (pod) (count_over_time({job="rbx"}[5m])) and sum by (pod) (count_over_time({job="rbx"} |= "x" [5m])))`,
	`topk(1, sum by (pod) (count_over_time({job="rbx"}[1m])) unless sum by (pod) (count_over_time({job="rbx"} |= "y" [1m])))`,
	`topk(2, sum by (pod) (rate({job="rbx"}[5m])) / sum by (pod) (rate({job="rbx"} |= "x" [5m])))`,
	`topk(2, sum by (pod) (rate({job="rbx"}[5m])) / on (pod) sum by (pod) (rate({job="rbx", pod=~"p|q"}[5m])))`,
	// The stack shapes at an hour with Grafana's steps (rankBinaryHourRequests).
	`sort_desc(sum by (pod) (count_over_time({job="rbx"}[5m])) > bool 10)`,
	`stddev(sum by (pod) (count_over_time({job="rbx"}[5m])) / sum by (pod) (count_over_time({job="rbx"} |= "x" [5m])) * 100)`,
	// +Inf and NaN per step: x/0 ranks above every number, NaN last.
	`topk(2, (sum by (pod) (count_over_time({job="rb"}[1m])) - sum by (pod) (count_over_time({job="rb", level="info"}[1m]))) / (sum by (pod) (count_over_time({job="rb"}[1m])) - sum by (pod) (count_over_time({job="rb"}[1m]))))`,
	`bottomk(3, sum by (pod) (count_over_time({job="rbx"}[1m])) / (sum by (pod) (count_over_time({job="rbx"}[1m])) - 30))`,
	// Loki's errors: a many-to-one match without group_left, and a set operator with a number operand.
	`topk(1, sum by (pod, level) (count_over_time({job="rb"}[1m])) / on (pod) sum by (pod) (count_over_time({job="rb"}[1m])))`,
	`topk(1, sum by (pod) (count_over_time({job="rb"}[1m])) and 1)`,
}

// rankBinaryKnown are the runs that differ from Loki, by query and request kind, with the registry case holding the
// difference open.
var rankBinaryKnown = map[string]map[string]string{
	// Status 400 and Loki's pipeline error text; the series the text names is not the one Loki names first, which is
	// not deterministic in Loki either (as for the same query with no binary operation).
	`topk(2, count_over_time({job="rb"} | json [1m]) > 0)`: {
		"instant": "semantics/pipeline-error-text-parity", "step=range": "semantics/pipeline-error-text-parity",
		"step<range": "semantics/pipeline-error-text-parity", "step>range": "semantics/pipeline-error-text-parity",
	},
}

// rankBinaryFixture is the data of every query.
func rankBinaryFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "rb", "pod": "a", "level": "info", "service_name": "rank-service"},
		{"job": "rb", "pod": "a", "level": "warn", "service_name": "rank-service"},
		{"job": "rb", "pod": "b", "level": "info", "service_name": "rank-service"},
		{"job": "rb", "pod": "b", "level": "warn", "service_name": "rank-service"},
		{"job": "rb", "pod": "c", "level": "info", "service_name": "rank-service"},
		// Three pods whose ranking changes between steps: p is busy in the first half of every ten minutes, q in the
		// second, r at a steady pace in between; s writes no line with "x".
		{"job": "rbx", "pod": "p", "service_name": "rank-service"},
		{"job": "rbx", "pod": "q", "service_name": "rank-service"},
		{"job": "rbx", "pod": "r", "service_name": "rank-service"},
		{"job": "rbx", "pod": "s", "service_name": "rank-service"},
	}}
	// A line every 3.1 to 11.5 seconds, by stream, with a jitter that moves every stream's count between windows.
	paces := []time.Duration{3100 * time.Millisecond, 9700 * time.Millisecond, 4300 * time.Millisecond, 5900 * time.Millisecond, 11500 * time.Millisecond}
	first := metricEnd.Add(-metricWindow - 10*time.Minute)
	for s := range paces {
		for i := 0; ; i++ {
			// Half a millisecond past a millisecond: no line sits on a window edge.
			at := first.Add(time.Duration(i)*paces[s] + time.Duration(s)*700*time.Millisecond + time.Duration(i*7919%2300)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: "level=" + f.Streams[s]["level"] + " seq=" + strconv.Itoa(i)})
		}
	}
	for s := len(paces); s < len(f.Streams); s++ {
		pod := f.Streams[s]["pod"]
		for at := first.Add(time.Duration(s)*300*time.Millisecond + 500*time.Microsecond); at.Before(metricEnd); {
			busy := at.Sub(first)%(10*time.Minute) < 5*time.Minute
			pace := 6 * time.Second
			switch {
			case pod == "p" && busy, pod == "q" && !busy:
				pace = 2100 * time.Millisecond
			case pod == "p", pod == "q":
				pace = 17 * time.Second
			}
			line := "x pod=" + pod
			if pod == "s" {
				line = "y pod=" + pod
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line})
			at = at.Add(pace)
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// rankBinaryQueries are the differential's queries over rankBinaryFixture.
func rankBinaryQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := rankBinaryFixture()
	out := make([]metricQuery, 0, len(rankBinaryShapes))
	for _, q := range rankBinaryShapes {
		p, err := planOfText(q)
		if err != nil {
			// Loki rejects it at parse too: the parse error is compared like an answer.
			out = append(out, metricQuery{q: q, id: queryID("logql", q), fix: fix, reqs: metricRequests(q, time.Minute)})
			continue
		}
		reqs := metricRequests(q, firstRangeOf(p.Root))
		if strings.Contains(q, `job="rbx"`) {
			reqs = append(reqs, rankBinaryHourRequests(q)...)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: reqs})
	}
	return out
}

// rankBinaryHourRequests are query_range requests over an hour that ends a minute before the data does, as a dashboard
// asks: the steps Grafana picks for an hour (15 s, 30 s, 60 s), and bounds off the step's grid, which Loki's frontend
// and the proxy (-align-queries-with-step, on as deployed) move down to it.
func rankBinaryHourRequests(q string) []metricRequest {
	var out []metricRequest
	end := metricEnd.Add(-53 * time.Second)
	start := end.Add(-time.Hour)
	for _, step := range []int{15, 30, 60} {
		form := url.Values{"query": {q}, "start": {strconv.FormatInt(start.Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}, "step": {strconv.Itoa(step)}}
		out = append(out, metricRequest{kind: "1h step=" + strconv.Itoa(step), form: form, align: true})
	}
	return out
}

// TestRankOverBinaryMatchesLoki replays the recorded answers through the handlers and holds every run to Loki's
// answer, or to its listed difference, and holds every VictoriaLogs request to a query VictoriaLogs reads.
//
// conformance: semantics/rank-over-binary-operation
func TestRankOverBinaryMatchesLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := rankBinaryQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, rankBinaryAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, rankBinaryAnswersPath)
	type row struct{ match, runs, ties int }
	table := map[string]*row{}
	seenKnown := map[string]bool{}
	nonEmpty, errorsSeen := 0, 0
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
				if loki.Status != 200 {
					errorsSeen++
				} else if res, err := decodeMetricBody(loki.Body); err == nil && len(res.series) > 0 {
					nonEmpty++
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				for _, sent := range run.sent {
					if strings.Contains(sent, "__binary__") || strings.Contains(sent, "%7C%7C%7C") || strings.Contains(sent, "|||") {
						t.Errorf("%s %s %s: the binary marker reached VictoriaLogs: %s", prof.name, mr.kind, mq.q, sent)
					}
				}
				d := compareMetric(loki, run)
				key := prof.name + " " + mr.kind
				if table[key] == nil {
					table[key] = &row{}
				}
				table[key].runs++
				known := rankBinaryKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case len(d.classes) > 0 && known == "" && rankedTieOnly(loki, run):
					// Loki keeps one of the samples tied at the k-th place by the order of a Go map: any of them is
					// its answer. The values kept at every step are Loki's.
					table[key].ties++
					table[key].match++
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from rankBinaryKnown (%s)", prof.name, mr.kind, mq.q, known)
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
	for q, kinds := range rankBinaryKnown {
		for kind := range kinds {
			if !seenKnown[q+"\x00"+kind] {
				t.Errorf("rankBinaryKnown lists %s %s, which the differential does not run", kind, q)
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
		fmt.Fprintf(&b, "\n  %-36s %3d of %3d match Loki (%d by a tie at the k-th place)", k, table[k].match, table[k].runs, table[k].ties)
	}
	t.Logf("rankings over binary operations against Loki (%d non-empty Loki answers, %d Loki errors):%s", nonEmpty, errorsSeen, b.String())
	if errorsSeen == 0 {
		t.Error("no Loki error answer: the error shapes no longer reach Loki's error")
	}
	if nonEmpty < (len(queries)-3)*len(metricRequests("", time.Minute))*len(diffProfiles())*3/4 {
		t.Errorf("only %d of the runs have a non-empty Loki answer", nonEmpty)
	}
}

// rankedTieOnly reports two answers of a ranking that differ only in which of the samples tied at the cut a step
// keeps: the same status, and at every step the same values kept, as a multiset (a tie Loki breaks by the order of a
// Go map, so any of the tied series is its answer).
func rankedTieOnly(loki metricLokiAnswer, run metricHandlerRun) bool {
	if loki.Status != http.StatusOK || run.status != http.StatusOK {
		return false
	}
	l, errL := decodeMetricBody(loki.Body)
	p, errP := decodeMetricBody([]byte(run.body))
	if errL != nil || errP != nil || l.resultType != p.resultType {
		return false
	}
	// The kept series carry the same label names as Loki's: a tie changes which series, never its labels' shape.
	names := func(r metricResult) string {
		seen := map[string]bool{}
		for _, s := range r.series {
			for k := range s.labels {
				seen[k] = true
			}
		}
		out := make([]string, 0, len(seen))
		for k := range seen {
			out = append(out, k)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	if names(l) != names(p) {
		return false
	}
	byStep := func(r metricResult) map[string][]string {
		out := map[string][]string{}
		for _, s := range r.series {
			for ts, v := range s.points {
				out[ts] = append(out[ts], v)
			}
		}
		for ts := range out {
			sort.Strings(out[ts])
		}
		return out
	}
	ls, ps := byStep(l), byStep(p)
	if len(ls) != len(ps) {
		return false
	}
	for ts, lv := range ls {
		if strings.Join(lv, ",") != strings.Join(ps[ts], ",") {
			return false
		}
	}
	return true
}
