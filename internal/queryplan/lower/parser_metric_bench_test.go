package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// parserMetricQueries are the plan corpus's range and vector aggregations over a log query with a json or logfmt
// parser (an unwrapped label or not) that the plan lowers.
func parserMetricQueries(b *testing.B) []string {
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
		if !ok || !hasParser(ra.In) {
			continue
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		b.Fatal("no parser aggregation in the corpus")
	}
	b.Logf("%d parser metric queries", len(out))
	return out
}

// The string translator on the lowered parser aggregations: text to LogsQL text.
func BenchmarkParserMetricQueriesTranslator(b *testing.B) {
	qs := parserMetricQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkParserMetricQueriesLowered(b *testing.B) {
	qs := parserMetricQueries(b)
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
func BenchmarkParserMetricQueriesPlanOnly(b *testing.B) {
	qs := parserMetricQueries(b)
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

// The string translator on the lowered parser aggregations under the tenant's stream labels.
func BenchmarkParserMetricQueriesStreamLabelsTranslator(b *testing.B) {
	qs := parserMetricQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], benchStreamLabels); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries under the tenant's stream labels.
func BenchmarkParserMetricQueriesStreamLabelsLowered(b *testing.B) {
	qs := parserMetricQueries(b)
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
