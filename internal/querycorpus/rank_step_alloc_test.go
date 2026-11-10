package querycorpus_test

import (
	"sort"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// rankStreamLabels is the tenant's stream labels of the allocation test and the benchmarks.
var rankStreamLabels = translator.WithStreamLabels(nil, []string{"app", "namespace", "service_name", "env", "cluster", "pod"})

// The two sides take the query as the handlers hand it on: keywords in lower case, comments and logfmt flags removed
// (routedQuery).
//
// rankRouteAllocs is what the proxy does for a ranking, stddev or stdvar before it asks VictoriaLogs: the route
// decision on the routed query text, and the translation of the operand it slices (the canonical form of its ranges,
// as translateQueryWithContext reads it, then the translator alone: the proxy's translation cache and singleflight
// come on top, so this is the least the route costs).
func rankRouteAllocs(rq string, label func(string) string) float64 {
	return testing.AllocsPerRun(5, func() {
		if inner, routed := proxy.PostAggregationRoute(rq); routed {
			_, _ = translator.TranslateLogQLWithLabels(logql.CanonicalizeLogRanges(inner), label)
		}
	})
}

// rankLoweredAllocs is what the typed plan does for the same ranking: Parse, Build, Resolve, the text screen and Plan.
func rankLoweredAllocs(q string, label func(string) string) float64 {
	return testing.AllocsPerRun(5, func() {
		expr, _ := logql.Parse(logql.CanonicalizeLogRanges(q))
		pl, _ := queryplan.Build(expr, queryplan.Inputs{})
		queryplan.Resolve(pl)
		_, _ = lower.PlanQuery(q, pl, lower.Profile{Label: label})
	})
}

// A ranking, stddev or stdvar the lowering answers allocates no more than the proxy's route does for it (the decision
// and the operand's translation), with and without the tenant's stream labels. The comparator is the proxy's current route, not the
// string translator's text for the whole query, which the proxy never translates; a shape where the lowering
// allocates more is left to the translator under a named construct.
// conformance: architecture-syntax-package-layering
func TestRankLoweringAllocatesNoMoreThanTheProxyRoute(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	if raceDetectorEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	seen := map[string]bool{}
	var qs []string
	for _, q := range append(append(append(planCorpus(t), rankGrid()...), rankStepGrid()...), seriesAggGrid()...) {
		if !seen[q] {
			seen[q] = true
			qs = append(qs, q)
		}
	}
	checked, worst := 0, map[string]float64{}
	for _, label := range []func(string) string{nil, rankStreamLabels} {
		for _, q := range qs {
			expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
			if err != nil {
				continue
			}
			plan, err := queryplan.Build(expr, queryplan.Inputs{})
			if err != nil {
				continue
			}
			queryplan.Resolve(plan)
			if routed, _ := evaluatedRoot(plan); !routed {
				continue
			}
			rq := routedQuery(q)
			if _, err := lower.PlanQuery(rq, plan, lower.Profile{Label: label}); err != nil {
				continue
			}
			route, lowered := rankRouteAllocs(rq, label), rankLoweredAllocs(rq, label)
			checked++
			if lowered > route {
				t.Errorf("label=%v: %s: lowered %.0f allocs, the proxy's route %.0f", label != nil, q, lowered, route)
			}
			if key := lower.Construct(plan.Root); route > 0 && lowered/route > worst[key] {
				worst[key] = lowered / route
			}
		}
	}
	if checked < 4000 {
		t.Fatalf("only %d lowered rankings checked", checked)
	}
	var keys []string
	for k := range worst {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("%s roots: the lowering allocates at most %.2f of the route (%d lowered shapes checked)", k, worst[k], checked)
	}
}
