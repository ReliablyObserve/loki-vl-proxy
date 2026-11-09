package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// loweredRangeAggQueries are the plan corpus's range aggregations the plan lowers natively.
func loweredRangeAggQueries(b *testing.B) []string {
	b.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		b.Fatal(err)
	}
	var out []string
	for _, q := range qs {
		expr, err := logql.Parse(q)
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		if _, ok := p.Root.(*queryplan.RangeAgg); !ok {
			continue
		}
		queryplan.Resolve(p)
		if _, err := Plan(p, Profile{}); err == nil {
			out = append(out, q)
		}
	}
	if len(out) == 0 {
		b.Fatal("no lowered range aggregation")
	}
	return out
}

// The string translator on the lowered range aggregations: text to LogsQL text.
func BenchmarkRangeAggQueriesTranslator(b *testing.B) {
	qs := loweredRangeAggQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkRangeAggQueriesLowered(b *testing.B) {
	qs := loweredRangeAggQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr, err := logql.Parse(qs[i%len(qs)])
		if err != nil {
			b.Fatal(err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(p)
		if _, err := Plan(p, Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan alone on the same queries, built once: the lowering's own cost.
func BenchmarkRangeAggQueriesPlanOnly(b *testing.B) {
	qs := loweredRangeAggQueries(b)
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

// benchStreamLabels is the label function of a tenant with stream labels, as the proxy builds it on every request
// once it knows them.
var benchStreamLabels = translator.WithStreamLabels(nil, []string{"app", "namespace", "service_name", "env", "cluster", "pod"})

// The string translator on the lowered range aggregations under the tenant's stream labels.
func BenchmarkRangeAggQueriesStreamLabelsTranslator(b *testing.B) {
	qs := loweredRangeAggQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], benchStreamLabels); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries under the tenant's stream labels.
func BenchmarkRangeAggQueriesStreamLabelsLowered(b *testing.B) {
	qs := loweredRangeAggQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr, err := logql.Parse(qs[i%len(qs)])
		if err != nil {
			b.Fatal(err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(p)
		if _, err := Plan(p, Profile{Label: benchStreamLabels}); err != nil {
			b.Fatal(err)
		}
	}
}
