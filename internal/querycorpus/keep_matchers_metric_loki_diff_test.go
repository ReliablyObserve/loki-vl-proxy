package querycorpus_test

import (
	"os"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

const keepMatcherMetricPath = "testdata/keep-matchers-metric-loki.jsonl.gz"

// keepMatcherMetricQueries are metric queries over a keep with matchers: the series are the labels the keeps leave.
// A keep behind a parser is held by TestCompat_KeepNamesAndMatchersLikeLoki: the handler's parser probe reads the
// wall clock, so its answers cannot be recorded.
var keepMatcherMetricQueries = []string{
	`count_over_time({app="x"} | keep app, level="error" [5m])`,
	`sum by (level) (count_over_time({app="x"} | keep app, level="error" [5m]))`,
	`sum by (level) (count_over_time({app="x"} | keep level="error", app | level="warn" [5m]))`,
	`sum by (level) (count_over_time({app="x"} | keep level="error" | level!="warn" [5m]))`,
	`count_over_time({app="x"} | keep level="warn" [5m])`,
	`sum(count_over_time({app="x"} | keep app, level="error" [5m]))`,
	`sum by (cluster) (count_over_time({app="x"} | keep app, cluster="c1" [5m]))`,
	`count_over_time({app="x"} | keep app, cluster="c1" [5m])`,
	`rate({app="x"} | keep cluster!~"c1" [5m])`,
	`bytes_over_time({app="x"} |= "level" | keep cluster=~"c.*", trace_id="tr-1" [5m])`,
	`bytes_rate({app="x"} | keep cluster!="c1", app [5m])`,
	`count(count_over_time({app="x"} | keep cluster="c1" [5m]))`,
	`count_over_time({app="x"} | keep cluster="c1" | keep app [5m])`,
	`count_over_time({app="x"} | keep cluster="c1", app | keep cluster [5m])`,
	`count_over_time({app="x"} | keep cluster="c1" |= "level" [5m])`,
	`count_over_time({app="x"} | keep cluster="c1", app | app="x" [5m])`,
	`count_over_time({app="x"} | keep cluster="c1", app | cluster="" [5m])`,
	`count_over_time({app="x"} | keep level="error", app | level="error" [5m])`,
	`count_over_time({app="x"} | keep app, service_name="x" [5m])`,
	`count_over_time({app="x"} | keep service_name!~"x|y" [5m])`,
	`sum by (service_name) (count_over_time({app="x"} | keep service_name="nope", app [5m]))`,
	`sum(rate({app="x"} | keep level="error" [5m]))`,
	`sum(count_over_time({app="x"} | drop level="error" | level="error" [5m]))`,
	`sum(count_over_time({app="x"} | drop cluster="c1" | cluster!="c1" [5m]))`,
	`sum by (level) (count_over_time({app="x"} | drop level="error" | level!="warn" [5m]))`,
}

// A metric query over a keep with matchers answers Loki's series and values: instant and range, on VictoriaLogs v1.49
// and v1.54. The Loki engine's answers and VictoriaLogs' are recorded (LVP_RUN_ORACLES=1 records them).
//
// conformance: semantics/keep-names-and-matchers
func TestKeepMatchersMetricMatchLoki(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	var queries []metricQuery
	for _, q := range keepMatcherMetricQueries {
		expr, err := logql.Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		queryplan.Resolve(p)
		mq := metricQuery{q: q, id: queryID("logql", q), plan: p, routes: map[string]map[string]*lower.QueryRoute{}}
		mq.reqs = metricRequests(q, firstRangeOf(p.Root))
		mq.fix = buildMetricFixture(p)
		queries = append(queries, mq)
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, keepMatcherMetricPath, queries)
	}
	answers := readMetricAnswersFrom(t, keepMatcherMetricPath)
	runs, answered := 0, 0
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("%s: the recorded answers are stale: record them with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				loki := rec.Loki[mr.kind]
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if len(run.missing) > 0 {
					t.Fatalf("%s %s %s: no recorded VictoriaLogs answer for %s", prof.name, mr.kind, mq.q, run.missing[0])
				}
				runs++
				if loki.Status == 200 && len(loki.Body) > 200 {
					answered++
				}
				for class, example := range compareMetric(loki, run).classes {
					t.Errorf("%s %s %s: %s: %s", prof.name, mr.kind, mq.q, class, example)
				}
			}
		}
	}
	if runs == 0 || answered < runs/4 {
		t.Fatalf("%d of %d runs hold series: the fixture lost its data", answered, runs)
	}
}
