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

// loweredRoutedBinaries are the binary operations with an operand that has a without clause (lower.RoutedBinary) of
// the plan corpus and the bool and without grid that the lowering answers, as the handlers receive them. The ones with
// a `without ()` operand are loweredEmptyWithoutBinaries': they are measured apart, so these keep the inputs they
// were measured on.
func loweredRoutedBinaries(b *testing.B) []string {
	b.Helper()
	return loweredBinaries(b, func(root queryplan.Node) bool { return lower.RoutedBinary(root) && !lower.EmptyWithoutOperand(root) })
}

// loweredEmptyWithoutBinaries are the binary operations with a `without ()` operand that the lowering answers.
func loweredEmptyWithoutBinaries(b *testing.B) []string {
	b.Helper()
	return loweredBinaries(b, lower.EmptyWithoutOperand)
}

// loweredBinaries are the corpus and grid queries whose root keep accepts and the lowering answers.
func loweredBinaries(b *testing.B, keep func(queryplan.Node) bool) []string {
	b.Helper()
	var qs []string
	seen := map[string]bool{}
	corpus, err := plancorpus.Queries("../..")
	if err != nil {
		b.Fatal(err)
	}
	for _, q := range append(corpus, binOpMoreGrid()...) {
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
		if !keep(plan.Root) {
			continue
		}
		if _, err := lower.PlanQuery(rq, plan, lower.Profile{}); err != nil {
			continue
		}
		qs = append(qs, rq)
	}
	if len(qs) == 0 {
		b.Fatal("no lowered binary operation of the kind")
	}
	b.Logf("%d binary operations", len(qs))
	return qs
}

// The proxy's binary route for them (proxy.BinaryOperandsPhysical): its parse of the query, and each operand that is
// not a number translated alone and decoded.
func BenchmarkBinaryWithoutRoute(b *testing.B)             { benchBinaryRoute(b, nil) }
func BenchmarkBinaryWithoutRouteStreamLabels(b *testing.B) { benchBinaryRoute(b, rankStreamLabels) }

func benchBinaryRoute(b *testing.B, label func(string) string) {
	qs := loweredRoutedBinaries(b)
	translate := func(q string) (string, error) {
		return translator.TranslateLogQLWithLabels(logql.CanonicalizeLogRanges(q), label)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, routed, err := proxy.BinaryOperandsPhysical(qs[i%len(qs)], translate); err != nil || !routed {
			b.Fatal(routed, err)
		}
	}
}

// The string translator's text for the whole query, which the handlers also write before the binary route.
func BenchmarkBinaryWithoutTranslator(b *testing.B) {
	qs := loweredRoutedBinaries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(logql.CanonicalizeLogRanges(qs[i%len(qs)]), nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The typed plan on the same queries: Parse, Build, Resolve, the text screen and Plan.
func BenchmarkBinaryWithoutLowered(b *testing.B) { benchLowered(b, loweredRoutedBinaries(b), nil) }
func BenchmarkBinaryWithoutLoweredStreamLabels(b *testing.B) {
	benchLowered(b, loweredRoutedBinaries(b), rankStreamLabels)
}

// The binary operations with a `without ()` operand, which the lowering writes as Loki reads them (every series
// kept): the proxy's binary route for them reads the clause as none, and the plan on the same queries.
func BenchmarkBinaryEmptyWithoutRoute(b *testing.B) {
	qs := loweredEmptyWithoutBinaries(b)
	translate := func(q string) (string, error) {
		return translator.TranslateLogQLWithLabels(logql.CanonicalizeLogRanges(q), nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, routed, err := proxy.BinaryOperandsPhysical(qs[i%len(qs)], translate); err != nil || !routed {
			b.Fatal(routed, err)
		}
	}
}

func BenchmarkBinaryEmptyWithoutLowered(b *testing.B) {
	benchLowered(b, loweredEmptyWithoutBinaries(b), nil)
}

func BenchmarkBinaryEmptyWithoutLoweredStreamLabels(b *testing.B) {
	benchLowered(b, loweredEmptyWithoutBinaries(b), rankStreamLabels)
}
