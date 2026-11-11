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
	return loweredEvaluatedQueries(b, "ranking", func(p *queryplan.Plan) bool {
		_, ok := p.Root.(*queryplan.Rank)
		return ok
	}, append(rankGrid(), rankStepGrid()...))
}

// loweredSeriesAggQueries are the stddev and stdvar roots of the plan corpus and the grids that the lowering answers.
func loweredSeriesAggQueries(b *testing.B) []string {
	b.Helper()
	return loweredEvaluatedQueries(b, "stddev or stdvar", func(p *queryplan.Plan) bool {
		va, ok := p.Root.(*queryplan.VectorAgg)
		return ok && (va.Op == logql.VectorStddev || va.Op == logql.VectorStdvar)
	}, seriesAggGrid())
}

// loweredEvaluatedQueries are the queries of the plan corpus and grid that the proxy routes as one evaluation, whose
// plan keep accepts, that the lowering answers.
func loweredEvaluatedQueries(b *testing.B, what string, keep func(*queryplan.Plan) bool, grid []string) []string {
	b.Helper()
	var qs []string
	seen := map[string]bool{}
	corpus, err := plancorpus.Queries("../..")
	if err != nil {
		b.Fatal(err)
	}
	for _, q := range append(corpus, grid...) {
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
		if routed, _ := evaluatedRoot(plan); !routed || !keep(plan) {
			continue
		}
		if _, err := lower.PlanQuery(rq, plan, lower.Profile{}); err != nil {
			continue
		}
		qs = append(qs, rq)
	}
	if len(qs) == 0 {
		b.Fatalf("no lowered %s", what)
	}
	b.Logf("%d %s queries", len(qs), what)
	return qs
}

// The proxy's route for the lowered rankings: the decision on the query text, and the string translator on the operand
// it slices.
func BenchmarkRankRoute(b *testing.B)             { benchRankRoute(b, nil) }
func BenchmarkRankRouteStreamLabels(b *testing.B) { benchRankRoute(b, rankStreamLabels) }

func benchRankRoute(b *testing.B, label func(string) string) {
	benchRoute(b, loweredRankQueries(b), label)
}

func benchRoute(b *testing.B, qs []string, label func(string) string) {
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
	benchLowered(b, loweredRankQueries(b), label)
}

func benchLowered(b *testing.B, qs []string, label func(string) string) {
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

// The same two measures for the stddev and stdvar roots: the proxy's route (the decision and the string translator on
// the operand it slices), and the typed plan (a SeriesAgg over the operand's operators).
func BenchmarkSeriesAggRoute(b *testing.B) { benchRoute(b, loweredSeriesAggQueries(b), nil) }
func BenchmarkSeriesAggRouteStreamLabels(b *testing.B) {
	benchRoute(b, loweredSeriesAggQueries(b), rankStreamLabels)
}
func BenchmarkSeriesAggLowered(b *testing.B) { benchLowered(b, loweredSeriesAggQueries(b), nil) }
func BenchmarkSeriesAggLoweredStreamLabels(b *testing.B) {
	benchLowered(b, loweredSeriesAggQueries(b), rankStreamLabels)
}
