package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// vectorAggQueries are the plan corpus's vector aggregations that the plan lowers (sum, count, min, max and avg,
// with by and without, over a range function with line, pattern and label filters), plus the grouping shapes the
// corpus has few of (service_name, detected_level, the series identity of count and min), so that each stats form
// is measured.
func vectorAggQueries(b *testing.B) []string {
	b.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		b.Fatal(err)
	}
	qs = append(qs,
		`sum by (service_name) (rate({app="a"} |= "x" [5m]))`, `count by (detected_level) (count_over_time({app="a"}[5m]))`,
		`min by (app, env) (bytes_rate({app="a"} != "y" [1m]))`, `avg without (pod) (rate({app="a"}[5m]))`,
		`sum by (app) (bytes_over_time({app="a"} |~ "z.+" [5m]))`, `max(count_over_time({app="a"} | status >= 500 [5m]))`,
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
		if _, ok := p.Root.(*queryplan.VectorAgg); !ok {
			continue
		}
		queryplan.Resolve(p)
		if _, err := Plan(p, Profile{}); err != nil {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		b.Fatal("no vector aggregation in the corpus")
	}
	b.Logf("%d vector aggregation queries", len(out))
	return out
}

// The string translator on the lowered vector aggregations: text to LogsQL text.
func BenchmarkVectorAggQueriesTranslator(b *testing.B) {
	qs := vectorAggQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The string translator on the same queries under the tenant's stream labels.
func BenchmarkVectorAggQueriesStreamLabelsTranslator(b *testing.B) {
	qs := vectorAggQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], benchStreamLabels); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkVectorAggQueriesLowered(b *testing.B) { benchVectorAggLowered(b, nil) }

// The plan on the same queries under the tenant's stream labels.
func BenchmarkVectorAggQueriesStreamLabelsLowered(b *testing.B) {
	benchVectorAggLowered(b, benchStreamLabels)
}

func benchVectorAggLowered(b *testing.B, label func(string) string) {
	qs := vectorAggQueries(b)
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
		if _, err := Plan(p, Profile{Label: label}); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan alone on the same queries, built once: the lowering's own cost.
func BenchmarkVectorAggQueriesPlanOnly(b *testing.B) {
	qs := vectorAggQueries(b)
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
