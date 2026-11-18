package proxy

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
)

// planWork is the proxy's own planning work on one query as a request does it on a cold cache: the validation (the
// parser, its semantic pass and the tree checks) and the translation (the query plan's lowering, or the string
// translator where it falls back).
func planWork(p *Proxy, text string) (validate, translate time.Duration) {
	start := time.Now()
	_, _, phase := validateQueryPhases(text)
	validate = time.Since(start)
	if phase == phaseParse || phase == phaseTree {
		return validate, 0 // answered with Loki's error, never translated
	}
	start = time.Now()
	_, _ = p.translateQuery(text)
	return validate, time.Since(start)
}

// deepQueryShapes are the shapes of the Loki oracle recording that Loki accepts below its 128 KB input limit.
func deepQueryShapes() map[string]string {
	return map[string]string{
		"parentheses 65000":     strings.Repeat("(", 65000) + `1` + strings.Repeat(")", 65000),
		"nested sum 26000":      strings.Repeat("sum(", 26000) + `rate({a="b"}[1m])` + strings.Repeat(")", 26000),
		"binary operands 65000": strings.Repeat("1+", 65000) + "1",
		"json stages 3000":      `{a="b"}` + strings.Repeat(" | json", 3000),
	}
}

// The default -max-query-plan-time never refuses a query the plan corpus holds: the slowest row plans in a small
// fraction of it. LVP_PLAN_TIME_DEEP=1 adds the deep shapes Loki accepts, the measurement the limits registry quotes
// (go test -v -run TestPlanBudgetDefaultNeverTripsOnTheCorpus).
//
// conformance: query-complexity-limits
func TestPlanBudgetDefaultNeverTripsOnTheCorpus(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	rows, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	p := newTestProxy(t, "http://unused")
	type timed struct {
		q string
		d time.Duration
	}
	var worst []timed
	var total time.Duration
	for _, row := range rows {
		v, tr := planWork(p, RequestQueryText(row))
		worst = append(worst, timed{row, v + tr})
		total += v + tr
	}
	sort.Slice(worst, func(i, j int) bool { return worst[i].d > worst[j].d })
	corpusMax := worst[0].d
	if limit := DefaultMaxQueryPlanTime / 10; corpusMax > limit {
		t.Fatalf("the slowest corpus row took %s, over a tenth of the default budget %s", corpusMax, DefaultMaxQueryPlanTime)
	}
	max := corpusMax
	// The deep shapes take seconds (the string translator on 26,000 nested calls), more under -race or a busy host:
	// they are measured on request (LVP_PLAN_TIME_DEEP=1), where the registry's figures come from.
	if os.Getenv("LVP_PLAN_TIME_DEEP") == "1" {
		for name, q := range deepQueryShapes() {
			v, tr := planWork(p, q)
			t.Logf("%s: validate %s translate %s", name, v, tr)
			if v+tr > max {
				max = v + tr
			}
		}
		if max > DefaultMaxQueryPlanTime {
			t.Fatalf("the longest planning work is %s, over the default budget %s", max, DefaultMaxQueryPlanTime)
		}
	}
	if testing.Verbose() {
		t.Logf("%d corpus rows, total %s, slowest row %s (%.80q)", len(rows), total, worst[0].d, worst[0].q)
	}
	t.Logf("longest planning work %s, default budget %s: margin x%.0f", max, DefaultMaxQueryPlanTime, float64(DefaultMaxQueryPlanTime)/float64(max))
}
