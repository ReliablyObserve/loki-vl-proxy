package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The kinds of log query the benchmarks measure: line filters only, a pattern line filter (|> or !>) among them,
// a label filter without a parser among them, and a json or logfmt parser, a drop or a keep among them.
const (
	lineFilterQueries = iota
	patternFilterQueries
	labelFilterQueries
	parserQueries
)

// loweredLineFilterQueries are the plan corpus's log queries with line filters that the plan lowers natively and
// that have no pattern line filter and no label filter: the set these benchmarks had before pattern filters were
// lowered.
func loweredLineFilterQueries(b *testing.B) []string {
	return loweredQueries(b, lineFilterQueries)
}

// loweredPatternFilterQueries are the plan corpus's log queries the plan lowers natively that have a pattern line
// filter and no label filter.
func loweredPatternFilterQueries(b *testing.B) []string {
	return loweredQueries(b, patternFilterQueries)
}

// loweredLabelFilterQueries are the plan corpus's log queries the plan lowers natively that have a label filter
// and no parser, drop or keep.
func loweredLabelFilterQueries(b *testing.B) []string {
	return loweredQueries(b, labelFilterQueries)
}

// loweredParserQueries are the plan corpus's log queries the plan lowers natively that have a json or logfmt
// parser, a drop or a keep.
func loweredParserQueries(b *testing.B) []string {
	return loweredQueries(b, parserQueries)
}

func loweredQueries(b *testing.B, kind int) []string {
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
		sel, ok := p.Root.(*queryplan.LogSelect)
		if !ok || len(sel.Stages) == 0 {
			continue
		}
		queryplan.Resolve(p)
		if _, err := Plan(p, Profile{}); err == nil && filterKind(sel) == kind {
			out = append(out, q)
		}
	}
	if len(out) == 0 {
		b.Fatal("no lowered query")
	}
	return out
}

func filterKind(sel *queryplan.LogSelect) int {
	kind := lineFilterQueries
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.Parse, *queryplan.Drop, *queryplan.Keep:
			return parserQueries
		case *queryplan.LabelFilter:
			kind = labelFilterQueries
		case *queryplan.LineFilter:
			if kind == lineFilterQueries && (st.Op == logql.LineFilterContainsPat || st.Op == logql.LineFilterExcludePat) {
				kind = patternFilterQueries
			}
		}
	}
	return kind
}

// The string translator on the lowered line-filter queries: text to LogsQL text.
func BenchmarkLineFilterQueriesTranslator(b *testing.B) {
	qs := loweredLineFilterQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkLineFilterQueriesLowered(b *testing.B) {
	qs := loweredLineFilterQueries(b)
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

// The string translator on the lowered pattern line filter queries: text to LogsQL text.
func BenchmarkPatternFilterQueriesTranslator(b *testing.B) {
	qs := loweredPatternFilterQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkPatternFilterQueriesLowered(b *testing.B) {
	qs := loweredPatternFilterQueries(b)
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

// The string translator on the lowered label filter queries: text to LogsQL text.
func BenchmarkLabelFilterQueriesTranslator(b *testing.B) {
	qs := loweredLabelFilterQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkLabelFilterQueriesLowered(b *testing.B) {
	qs := loweredLabelFilterQueries(b)
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

// The string translator on the lowered parser queries: text to LogsQL text.
func BenchmarkParserQueriesTranslator(b *testing.B) {
	qs := loweredParserQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve and Plan.
func BenchmarkParserQueriesLowered(b *testing.B) {
	qs := loweredParserQueries(b)
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
