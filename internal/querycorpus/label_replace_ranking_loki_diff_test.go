package querycorpus_test

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The Loki differential of label_replace calls around a ranking (semantics/label-replace-around-ranking-and-vector):
// topk, bottomk, sort and sort_desc, alone, grouped, under an aggregation and over a binary operation, wrapped in one
// or two label_replace calls, with a call that names a new label, one that rewrites a label the ranking keeps, one
// that folds every ranked series into one label set, and one whose regexp does not compile. Real Loki (the engine
// oracle) and real VictoriaLogs (the serve oracle, v1.54.0 and v1.49.0) over the same data, through the proxy's real
// handlers, as the metric differential runs them.
//
//   - Data: rankBinaryFixture (pods whose ranking changes between steps).
//   - Requests: metricRequests (an instant query and query_range at, under and over the range), and for the job=rbx
//     shapes the hour a dashboard asks with Grafana's steps (rankBinaryHourRequests).
//
// The answers are recorded (labelReplaceRankingAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must
// match Loki, or differ only by a tie at the k-th place (rankedTieOnly), or, for a query whose calls give several
// series of an aggregation one label set, only by the order of their samples at a step (sameSamplesAtEachStep: Loki
// writes an aggregation's groups in the order of a Go map), or be listed in labelReplaceRankingKnown with the registry
// case that holds the difference open.

const labelReplaceRankingAnswersPath = "testdata/label-replace-ranking-loki-diff.jsonl.gz"

// labelReplaceRankingShapes are the query shapes.
var labelReplaceRankingShapes = []string{
	`label_replace(topk(2, sum by (pod) (count_over_time({job="rbx"}[1m]))), "p", "$1", "pod", "(.*)")`,
	`label_replace(bottomk(1, sum by (pod) (rate({job="rb"}[1m]))), "short", "$1", "pod", "(.)")`,
	`label_replace(topk by (level) (1, sum by (pod, level) (count_over_time({job="rb"}[1m]))), "lv", "l-$1", "level", "(.*)")`,
	`label_replace(sort_desc(sum by (pod) (count_over_time({job="rb"}[1m]))), "p", "$1", "pod", "(.*)")`,
	`label_replace(sort(sum by (pod) (count_over_time({job="rb"}[1m]))), "pod", "x-$1", "pod", "(a|b)")`,
	// Every ranked series ends with one label set.
	`label_replace(topk(2, sum by (pod) (count_over_time({job="rbx"}[5m]))), "pod", "", "", "")`,
	`label_replace(sort(sum by (pod) (count_over_time({job="rb"}[1m]))), "pod", "same", "", "")`,
	`label_replace(topk(3, sum by (pod) (count_over_time({job="rbx"}[5m]))), "pod", "", "", "")`,
	`label_replace(bottomk(2, sum by (pod) (count_over_time({job="rbx"}[5m]))), "pod", "", "", "")`,
	`label_replace(sort_desc(sum by (pod) (count_over_time({job="rbx"}[5m]))), "pod", "", "", "")`,
	`label_replace(topk by (level) (2, sum by (pod, level) (count_over_time({job="rb"}[1m]))), "pod", "", "", "")`,
	`label_replace(max by (pod) (topk(3, sum by (pod) (count_over_time({job="rbx"}[5m])))), "pod", "", "", "")`,
	// Under an aggregation, over a binary operation, and two calls.
	`label_replace(sum(topk(2, sum by (pod) (count_over_time({job="rb"}[1m])))), "all", "yes", "", "")`,
	`label_replace(topk(2, sum by (pod) (count_over_time({job="rb"}[1m])) / 2), "p", "$1", "pod", "(.*)")`,
	`label_replace(label_replace(topk(3, sum by (pod) (count_over_time({job="rbx"}[1m]))), "a", "$1", "pod", "(.*)"), "b", "x$1", "a", "(.*)")`,
	// A regexp that does not compile: Loki's error.
	`label_replace(topk(2, sum by (pod) (count_over_time({job="rb"}[1m]))), "p", "$1", "pod", "(")`,
}

// labelReplaceRankingKnown are the runs that differ from Loki, by query and request kind, with the registry case
// holding the difference open.
var labelReplaceRankingKnown = map[string]map[string]string{}

// labelReplaceRankingQueries are the differential's queries over rankBinaryFixture.
func labelReplaceRankingQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := rankBinaryFixture()
	out := make([]metricQuery, 0, len(labelReplaceRankingShapes))
	for _, q := range labelReplaceRankingShapes {
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

// TestLabelReplaceAroundRankingMatchesLoki replays the recorded answers through the handlers and holds every run to
// Loki's answer, or to its listed difference.
//
// conformance: semantics/label-replace-around-ranking-and-vector
func TestLabelReplaceAroundRankingMatchesLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := labelReplaceRankingQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, labelReplaceRankingAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, labelReplaceRankingAnswersPath)
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
				d := compareMetric(loki, run)
				if mr.instant && !aggregatedUnderCalls(mq.plan) && len(d.classes) == 0 {
					// A ranking at the top writes an instant answer in its order, as Loki does (compareMetric reads it
					// as a set): the values in answer order are Loki's.
					if l, p := instantValueOrder(loki.Body), instantValueOrder([]byte(run.body)); l != p {
						d.classes = map[string]string{"orders the samples otherwise": "loki " + l + ", proxy " + p}
					}
				}
				key := prof.name + " " + mr.kind
				if table[key] == nil {
					table[key] = &row{}
				}
				table[key].runs++
				known := labelReplaceRankingKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case len(d.classes) > 0 && known == "" && aggregatedUnderCalls(mq.plan) && sameSamplesAtEachStep(loki, run):
					// The groups of an aggregation come in the order of a Go map in Loki: any order of the samples
					// one label set holds at a step is its answer.
					table[key].ties++
					table[key].match++
				case len(d.classes) > 0 && known == "" && rankedTieOnly(loki, run):
					// Loki keeps one of the samples tied at the k-th place by the order of a Go map.
					table[key].ties++
					table[key].match++
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from labelReplaceRankingKnown (%s)", prof.name, mr.kind, mq.q, known)
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
	for q, kinds := range labelReplaceRankingKnown {
		for kind := range kinds {
			if !seenKnown[q+"\x00"+kind] {
				t.Errorf("labelReplaceRankingKnown lists %s %s, which the differential does not run", kind, q)
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
		fmt.Fprintf(&b, "\n  %-36s %3d of %3d match Loki (%d by a tie at the k-th place or an aggregation's group order)", k, table[k].match, table[k].runs, table[k].ties)
	}
	t.Logf("label_replace around rankings against Loki (%d non-empty Loki answers, %d Loki errors):%s", nonEmpty, errorsSeen, b.String())
	if errorsSeen == 0 {
		t.Error("no Loki error answer: the regexp that does not compile no longer reaches Loki's error")
	}
	if nonEmpty < (len(queries)-1)*len(metricRequests("", time.Minute))*len(diffProfiles())*3/4 {
		t.Errorf("only %d of the runs have a non-empty Loki answer", nonEmpty)
	}
}

// aggregatedUnderCalls reports a plan whose label_replace calls wrap a vector aggregation (above the ranking).
func aggregatedUnderCalls(p *queryplan.Plan) bool {
	if p == nil {
		return false
	}
	_, ok := lower.LabelReplaceOperand(p.Root).(*queryplan.VectorAgg)
	return ok
}

// sameSamplesAtEachStep reports two 200 answers with the same series and, in each, the same samples at each step as a
// multiset (a range answer may hold several samples of one label set at a step).
func sameSamplesAtEachStep(loki metricLokiAnswer, run metricHandlerRun) bool {
	if loki.Status != 200 || run.status != 200 {
		return false
	}
	samples := func(body []byte) (map[string][]string, bool) {
		var doc struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Values [][2]any          `json:"values"`
					Value  [2]any            `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &doc) != nil {
			return nil, false
		}
		out := map[string][]string{}
		for _, s := range doc.Data.Result {
			labels, _ := json.Marshal(s.Metric) // encoding/json sorts map keys
			points := s.Values
			if points == nil {
				points = [][2]any{s.Value}
			}
			for _, pt := range points {
				k := string(labels) + fmt.Sprint(pt[0])
				out[k] = append(out[k], fmt.Sprint(pt[1]))
			}
		}
		for k := range out {
			sort.Strings(out[k])
		}
		return out, true
	}
	l, okL := samples(loki.Body)
	p, okP := samples([]byte(run.body))
	if !okL || !okP || len(l) != len(p) {
		return false
	}
	for k, v := range l {
		if strings.Join(v, ",") != strings.Join(p[k], ",") {
			return false
		}
	}
	return true
}

// instantValueOrder is the values of an instant answer in answer order.
func instantValueOrder(body []byte) string {
	var doc struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &doc) != nil {
		return ""
	}
	out := make([]string, 0, len(doc.Data.Result))
	for _, s := range doc.Data.Result {
		out = append(out, fmt.Sprint(s.Value[1]))
	}
	return strings.Join(out, ",")
}
