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

// The Loki differential of binary operations at the root of a query: comparisons with and without bool, the set
// operators and, or and unless (with and without on and ignoring), vector matching with group_left and group_right,
// chains of operations read with Loki's precedence and associativity, NaN and infinities, empty operands, and the
// errors Loki answers for invalid matchings. Real Loki (the engine oracle) and real VictoriaLogs (the serve oracle,
// v1.53.0 and v1.49.0) over the same data, through the proxy's real handlers, as the metric differential runs them.
//
//   - Data: rankBinaryFixture (five streams of job=rb, two levels on pods a and b and one on pod c).
//   - Requests: metricRequests (an instant query and query_range at, under and over the range).
//   - Shapes: binaryOpsShapes.
//
// No VictoriaLogs request may carry the marker the string translator writes for a binary operation. The answers are
// recorded (binaryOpsAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match Loki, or be listed in
// binaryOpsKnown with the registry case that holds the difference open.

const binaryOpsAnswersPath = "testdata/binary-ops-loki-diff.jsonl.gz"

// binaryOpsShapes are the query shapes; A is the count of every line by pod, I of the info lines, W of the warn lines.
var binaryOpsShapes = []string{
	// bool: 0 or 1 for every pair, the metric name dropped, with a number and between two vectors.
	`sum by (pod) (count_over_time({job="rb"}[1m])) > bool 9`,
	`9 < bool sum by (pod) (count_over_time({job="rb"}[1m]))`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) > bool sum by (pod) (count_over_time({job="rb", level="info"}[1m]))`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) == bool on (pod) sum by (pod) (count_over_time({job="rb", level="info"}[1m]))`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) != bool ignoring (level) sum by (pod, level) (count_over_time({job="rb", level="warn"}[1m]))`,
	// Set operators, on the whole label set and with on and ignoring.
	`sum by (pod, level) (count_over_time({job="rb"}[1m])) and sum by (pod, level) (count_over_time({job="rb", level="warn"}[1m]))`,
	`sum by (pod, level) (count_over_time({job="rb", level="warn"}[1m])) or sum by (pod, level) (count_over_time({job="rb"}[1m]))`,
	`sum by (pod, level) (count_over_time({job="rb"}[1m])) unless sum by (pod, level) (count_over_time({job="rb", level="warn"}[1m]))`,
	`sum by (pod, level) (count_over_time({job="rb"}[1m])) and on (pod) sum by (pod) (count_over_time({job="rb", level="warn"}[1m]))`,
	`sum by (pod, level) (count_over_time({job="rb"}[1m])) unless ignoring (level) sum by (pod) (count_over_time({job="rb", level="warn"}[1m]))`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) > 30 or sum by (pod) (count_over_time({job="rb", level="info"}[1m]))`,
	// Vector matching with a group side.
	`sum by (pod, level) (count_over_time({job="rb"}[1m])) / ignoring (level) group_left sum by (pod) (count_over_time({job="rb"}[1m]))`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) * on (pod) group_right sum by (pod, level) (count_over_time({job="rb"}[1m]))`,
	// Chains: precedence and associativity as Loki reads them.
	`sum by (pod) (count_over_time({job="rb"}[1m])) + sum by (pod) (count_over_time({job="rb", level="info"}[1m])) * 2`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) - sum by (pod) (count_over_time({job="rb", level="info"}[1m])) - 1`,
	`sum by (pod) (count_over_time({job="rb", level="info"}[1m])) ^ 2 ^ 0.5`,
	`(sum by (pod) (count_over_time({job="rb"}[1m])) + 1) / sum by (pod) (count_over_time({job="rb", level="info"}[1m])) * 100`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) + sum by (pod) (count_over_time({job="rb", level="info"}[1m])) > bool 40`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) > 20 and sum by (pod) (count_over_time({job="rb", level="info"}[1m])) > 10`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) % 7 + 1`,
	// vector(n) inside a chain: the operand is a binary operation, not a constant.
	`(vector(1) + sum by (pod) (count_over_time({job="rb"}[1m]))) * 2`,
	`sum(count_over_time({job="rb"}[1m])) * (vector(2) + sum(count_over_time({job="rb", level="info"}[1m])))`,
	// NaN and infinities: Loki writes NaN, +Inf and -Inf.
	`sum by (pod) (count_over_time({job="rb"}[1m])) / 0`,
	`(sum by (pod) (count_over_time({job="rb"}[1m])) - sum by (pod) (count_over_time({job="rb"}[1m]))) / 0`,
	`-1 * sum by (pod) (count_over_time({job="rb"}[1m])) / 0`,
	// Empty operands.
	`sum by (pod) (count_over_time({job="none"}[1m])) or sum by (pod) (count_over_time({job="rb"}[1m]))`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) and sum by (pod) (count_over_time({job="none"}[1m]))`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) unless sum by (pod) (count_over_time({job="none"}[1m]))`,
	`sum by (pod) (count_over_time({job="rb"}[1m])) / sum by (pod) (count_over_time({job="none"}[1m]))`,
	// Loki's errors: many-to-many and many-to-one matchings, a set operator with a number.
	`sum by (pod, level) (count_over_time({job="rb"}[1m])) + on (pod) sum by (pod, level) (count_over_time({job="rb"}[1m]))`,
	`sum by (pod, level) (count_over_time({job="rb"}[1m])) + on (pod) sum by (pod) (count_over_time({job="rb"}[1m]))`,
	`1 or sum by (pod) (count_over_time({job="rb"}[1m]))`,
}

// binaryOpsKnown are the runs that differ from Loki, by query and request kind, with the registry case holding the
// difference open.
var binaryOpsKnown = map[string]map[string]string{}

// binaryOpsFixture is the data of every query: the fixture of the ranking differential (rank_binary_loki_diff_test.go).
func binaryOpsFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"job": "rb", "pod": "a", "level": "info", "service_name": "rank-service"},
		{"job": "rb", "pod": "a", "level": "warn", "service_name": "rank-service"},
		{"job": "rb", "pod": "b", "level": "info", "service_name": "rank-service"},
		{"job": "rb", "pod": "b", "level": "warn", "service_name": "rank-service"},
		{"job": "rb", "pod": "c", "level": "info", "service_name": "rank-service"},
	}}
	// A line every 3.1 to 11.5 seconds, by stream, with a jitter that moves every stream's count between windows.
	paces := []time.Duration{3100 * time.Millisecond, 9700 * time.Millisecond, 4300 * time.Millisecond, 5900 * time.Millisecond, 11500 * time.Millisecond}
	first := metricEnd.Add(-metricWindow - 10*time.Minute)
	for s := range f.Streams {
		for i := 0; ; i++ {
			// Half a millisecond past a millisecond: no line sits on a window edge.
			at := first.Add(time.Duration(i)*paces[s] + time.Duration(s)*700*time.Millisecond + time.Duration(i*7919%2300)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: "level=" + f.Streams[s]["level"] + " seq=" + strconv.Itoa(i)})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// binaryOpsQueries are the differential's queries over binaryOpsFixture.
func binaryOpsQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := binaryOpsFixture()
	out := make([]metricQuery, 0, len(binaryOpsShapes))
	for _, q := range binaryOpsShapes {
		p, err := planOfText(q)
		if err != nil {
			// Loki rejects it at parse too: the parse error is compared like an answer.
			out = append(out, metricQuery{q: q, id: queryID("logql", q), fix: fix, reqs: metricRequests(q, time.Minute)})
			continue
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	return out
}

// TestBinaryOperationsMatchLoki replays the recorded answers through the handlers and holds every run to Loki's
// answer, or to its listed difference, and holds every VictoriaLogs request to a query VictoriaLogs reads.
//
// conformance: operator-and, operator-or, operator-unless, operator-on, operator-ignoring, operator-group-left, operator-group-right, operator-gt, operator-eq, operator-neq, operator-pow, operator-mod
func TestBinaryOperationsMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := binaryOpsQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, binaryOpsAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, binaryOpsAnswersPath)
	type row struct{ match, runs int }
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
				known := binaryOpsKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from binaryOpsKnown (%s)", prof.name, mr.kind, mq.q, known)
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
	for q, kinds := range binaryOpsKnown {
		for kind := range kinds {
			if !seenKnown[q+"\x00"+kind] {
				t.Errorf("binaryOpsKnown lists %s %s, which the differential does not run", kind, q)
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
	t.Logf("binary operations against Loki (%d non-empty Loki answers, %d Loki errors):%s", nonEmpty, errorsSeen, b.String())
	if errorsSeen == 0 {
		t.Error("no Loki error answer: the error shapes no longer reach Loki's error")
	}
	if nonEmpty < (len(queries)-8)*len(metricRequests("", time.Minute))*len(diffProfiles())*3/4 {
		t.Errorf("only %d of the runs have a non-empty Loki answer", nonEmpty)
	}
}
