package querycorpus_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// loweredRankQueries are the rankings of the plan corpus and the rank grids that the lowering answers, as the
// handlers receive them (routedQuery), so that each shape is measured.
func loweredRankQueries(b *testing.B) []string {
	b.Helper()
	var qs []string
	seen := map[string]bool{}
	corpus, err := plancorpus.Queries("../..")
	if err != nil {
		b.Fatal(err)
	}
	for _, q := range append(append(corpus, rankGrid()...), rankStepGrid()...) {
		rq := routedQuery(q)
		if seen[rq] {
			continue
		}
		seen[rq] = true
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(rq))
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
		if _, err := lower.PlanQuery(rq, plan, lower.Profile{}); err != nil {
			continue
		}
		qs = append(qs, rq)
	}
	if len(qs) == 0 {
		b.Fatal("no lowered ranking")
	}
	b.Logf("%d ranking queries", len(qs))
	return qs
}

// The proxy's route for the lowered rankings: the decision on the query text, and the string translator on the operand
// it slices.
func BenchmarkRankRoute(b *testing.B)             { benchRankRoute(b, nil) }
func BenchmarkRankRouteStreamLabels(b *testing.B) { benchRankRoute(b, rankStreamLabels) }

func benchRankRoute(b *testing.B, label func(string) string) {
	qs := loweredRankQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inner, routed := proxy.PostAggregationRoute(qs[i%len(qs)])
		if !routed {
			b.Fatal("not routed")
		}
		if _, err := translator.TranslateLogQLWithLabels(logql.CanonicalizeLogRanges(inner), label); err != nil {
			b.Fatal(err)
		}
	}
}

// The typed plan on the same queries: Parse, Build, Resolve, the text screen and Plan (the RankStep over the
// operand's operators, which encodes as the operand's request).
func BenchmarkRankLowered(b *testing.B)             { benchRankLowered(b, nil) }
func BenchmarkRankLoweredStreamLabels(b *testing.B) { benchRankLowered(b, rankStreamLabels) }

func benchRankLowered(b *testing.B, label func(string) string) {
	qs := loweredRankQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i%len(qs)]
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			b.Fatal(err)
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(plan)
		if _, err := lower.PlanQuery(q, plan, lower.Profile{Label: label}); err != nil {
			b.Fatal(err)
		}
	}
}
