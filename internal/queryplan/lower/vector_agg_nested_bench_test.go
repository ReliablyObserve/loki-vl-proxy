package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// nestedVectorAggQueries are the plan corpus's nested vector aggregations that the plan lowers, plus nests of the
// shapes the corpus has few of (three levels, a rate, derived labels, a without at the top).
func nestedVectorAggQueries(b *testing.B) []string {
	b.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		b.Fatal(err)
	}
	qs = append(qs,
		`sum by (app) (max by (app, env) (rate({app="a"} |= "x" [5m])))`,
		`avg(min by (service_name) (count by (service_name, level) (count_over_time({app="a"}[5m]))))`,
		`max by (detected_level) (sum by (detected_level, app) (bytes_over_time({app="a"} != "y" [1m])))`,
		`min without (env) (max by (app, env) (bytes_rate({app="a"}[5m])))`,
		`sum(count by (app) (count_over_time({app="a"} |~ "z.+" [5m])))`,
	)
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
		top, ok := p.Root.(*queryplan.VectorAgg)
		if !ok {
			continue
		}
		if _, nested := top.In.(*queryplan.VectorAgg); !nested {
			continue
		}
		queryplan.Resolve(p)
		if _, err := Plan(p, Profile{}); err != nil {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		b.Fatal("no nested vector aggregation in the corpus")
	}
	b.Logf("%d nested vector aggregation queries", len(out))
	return out
}

// The string translator on the lowered nested vector aggregations: text to LogsQL text.
func BenchmarkNestedVectorAggQueriesTranslator(b *testing.B) {
	qs := nestedVectorAggQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkNestedVectorAggQueriesLowered(b *testing.B) {
	qs := nestedVectorAggQueries(b)
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
func BenchmarkNestedVectorAggQueriesPlanOnly(b *testing.B) {
	qs := nestedVectorAggQueries(b)
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
