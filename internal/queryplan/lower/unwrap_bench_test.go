package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// unwrapQueries are the plan corpus's range and vector aggregations over an unwrapped label that the plan lowers.
func unwrapQueries(b *testing.B) []string {
	b.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		b.Fatal(err)
	}
	var out []string
	for _, q := range qs {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		ra, ok := p.Root.(*queryplan.RangeAgg)
		if va, isVec := p.Root.(*queryplan.VectorAgg); isVec {
			ra, ok = va.In.(*queryplan.RangeAgg)
		}
		if !ok || unwrapStage(ra.In) == nil || ra.Op == logql.RangeAbsentOverTime {
			continue
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		b.Fatal("no unwrap aggregation in the corpus")
	}
	b.Logf("%d unwrap aggregation queries", len(out))
	return out
}

// The string translator on the lowered unwrap aggregations: text to LogsQL text.
func BenchmarkUnwrapQueriesTranslator(b *testing.B) {
	qs := unwrapQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkUnwrapQueriesLowered(b *testing.B) {
	qs := unwrapQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i%len(qs)]
		expr, err := logql.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan alone on the same queries, built once: the lowering's own cost.
func BenchmarkUnwrapQueriesPlanOnly(b *testing.B) {
	qs := unwrapQueries(b)
	plans := make([]*queryplan.Plan, len(qs))
	for i, q := range qs {
		expr, err := logql.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		if plans[i], err = queryplan.Build(expr, queryplan.Inputs{}); err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(plans[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Plan(plans[i%len(plans)], Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}

// The string translator on the lowered unwrap aggregations under the tenant's stream labels.
func BenchmarkUnwrapQueriesStreamLabelsTranslator(b *testing.B) {
	qs := unwrapQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], benchStreamLabels); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries under the tenant's stream labels.
func BenchmarkUnwrapQueriesStreamLabelsLowered(b *testing.B) {
	qs := unwrapQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i%len(qs)]
		expr, err := logql.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{Label: benchStreamLabels}); err != nil {
			b.Fatal(err)
		}
	}
}
