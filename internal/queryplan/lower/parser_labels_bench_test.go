package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The string translator on the metric queries whose parser labels the plan reads from typed data
// (parserLabelQueries): text to LogsQL text.
func BenchmarkParserLabelsTranslator(b *testing.B) { benchParserLabelsTranslator(b, nil) }

// The string translator on the same queries under the tenant's stream labels.
func BenchmarkParserLabelsStreamLabelsTranslator(b *testing.B) {
	benchParserLabelsTranslator(b, benchStreamLabels)
}

func benchParserLabelsTranslator(b *testing.B, label func(string) string) {
	qs := parserLabelQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], label); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkParserLabelsLowered(b *testing.B) { benchParserLabelsLowered(b, nil) }

// The plan on the same queries under the tenant's stream labels.
func BenchmarkParserLabelsStreamLabelsLowered(b *testing.B) {
	benchParserLabelsLowered(b, benchStreamLabels)
}

func benchParserLabelsLowered(b *testing.B, label func(string) string) {
	qs := parserLabelQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i%len(qs)]
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			b.Fatal(err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{Label: label}); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan alone on the same queries, built once: the lowering's own cost.
func BenchmarkParserLabelsPlanOnly(b *testing.B) {
	qs := parserLabelQueries(b)
	plans := make([]*queryplan.Plan, len(qs))
	for i, q := range qs {
		plans[i] = rangeAggPlan(q)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(qs)
		if _, err := PlanQuery(qs[j], plans[j], Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}
