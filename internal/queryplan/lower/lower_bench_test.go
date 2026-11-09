package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// loweredLineFilterQueries are the plan corpus's log queries with line filters that the plan lowers natively.
func loweredLineFilterQueries(b *testing.B) []string {
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
		if _, err := Plan(p, Profile{}); err == nil {
			out = append(out, q)
		}
	}
	if len(out) == 0 {
		b.Fatal("no lowered line-filter query")
	}
	return out
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
