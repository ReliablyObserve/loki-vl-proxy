package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// binOpMoreQueries are the binary operations with a bool modifier or a without clause that the plan lowers, so that
// the translator and the lowering are measured on the same shapes.
func binOpMoreQueries(b *testing.B) []string {
	b.Helper()
	var out []string
	for _, q := range binOpMoreAllocGrid() {
		p := rangeAggPlan(q)
		if p == nil {
			continue
		}
		if _, err := PlanQuery(q, p, Profile{}); err == nil {
			out = append(out, q)
		}
	}
	if len(out) == 0 {
		b.Fatal("no lowered binary operation with bool or without")
	}
	b.Logf("%d queries", len(out))
	return out
}

// The string translator on the lowered binary operations with a bool modifier or a without clause.
func BenchmarkBinOpMoreQueriesTranslator(b *testing.B) { benchBinOpMoreTranslator(b, nil) }

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve, the text screen and Plan.
func BenchmarkBinOpMoreQueriesLowered(b *testing.B) { benchBinOpMoreLowered(b, nil) }

// The same pair under the tenant's stream labels.
func BenchmarkBinOpMoreQueriesStreamLabelsTranslator(b *testing.B) {
	benchBinOpMoreTranslator(b, benchStreamLabels)
}

func BenchmarkBinOpMoreQueriesStreamLabelsLowered(b *testing.B) {
	benchBinOpMoreLowered(b, benchStreamLabels)
}

func benchBinOpMoreTranslator(b *testing.B, label func(string) string) {
	qs := binOpMoreQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], label); err != nil {
			b.Fatal(err)
		}
	}
}

func benchBinOpMoreLowered(b *testing.B, label func(string) string) {
	qs := binOpMoreQueries(b)
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
