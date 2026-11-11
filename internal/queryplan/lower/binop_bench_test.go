package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// binOpQueries are the plan corpus's binary operations and label_replace calls that the plan lowers (root is the
// construct), so that the translator and the lowering are measured on the same shapes. A binary operation held to
// the proxy's binary route (RoutedBinary) is measured against that route (BenchmarkBinaryWithout*, internal/querycorpus).
func binOpQueries(b *testing.B, root string) []string {
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
		if err != nil || Construct(p.Root) != root {
			continue
		}
		queryplan.Resolve(p)
		if _, err := PlanQuery(q, p, Profile{}); err != nil || RoutedBinary(p.Root) {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		b.Fatalf("no lowered %s in the corpus", root)
	}
	b.Logf("%d %s queries", len(out), root)
	return out
}

// The string translator on the lowered binary operations: text to LogsQL text.
func BenchmarkBinOpQueriesTranslator(b *testing.B) { benchBinOpTranslator(b, "BinOp", nil) }

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve, the text screen and Plan.
func BenchmarkBinOpQueriesLowered(b *testing.B) { benchBinOpLowered(b, "BinOp", nil) }

// The translator and the plan under the tenant's stream labels.
func BenchmarkBinOpQueriesStreamLabelsTranslator(b *testing.B) {
	benchBinOpTranslator(b, "BinOp", benchStreamLabels)
}

func BenchmarkBinOpQueriesStreamLabelsLowered(b *testing.B) {
	benchBinOpLowered(b, "BinOp", benchStreamLabels)
}

// The same pair on the lowered label_replace calls.
func BenchmarkLabelReplaceQueriesTranslator(b *testing.B) {
	benchBinOpTranslator(b, "LabelReplace", nil)
}

func BenchmarkLabelReplaceQueriesLowered(b *testing.B) { benchBinOpLowered(b, "LabelReplace", nil) }

func benchBinOpTranslator(b *testing.B, root string, label func(string) string) {
	qs := binOpQueries(b, root)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], label); err != nil {
			b.Fatal(err)
		}
	}
}

func benchBinOpLowered(b *testing.B, root string, label func(string) string) {
	qs := binOpQueries(b, root)
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
		if _, err := PlanQuery(q, p, Profile{Label: label}); err != nil {
			b.Fatal(err)
		}
	}
}

// The text screen and the plan alone on the lowered binary operations and label_replace calls, plans built once:
// the lowering's own cost.
func BenchmarkBinOpQueriesPlanOnly(b *testing.B) {
	qs := append(binOpQueries(b, "BinOp"), binOpQueries(b, "LabelReplace")...)
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
		if _, err := PlanQuery(qs[i%len(qs)], plans[i%len(plans)], Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}
