package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// rankQueries are the plan corpus's rankings that the plan lowers (topk, bottomk, sort and sort_desc without a
// grouping over a range function), plus the shapes the corpus has few of (each function, a parser, an unwrapped
// value, a line filter), so that each text is measured. Parsed with PlanQuery's screen: only the rows it lowers.
func rankQueries(b *testing.B) []string {
	b.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		b.Fatal(err)
	}
	qs = append(qs,
		`topk(5, rate({app="a"} |= "x" [5m]))`, `bottomk(3, count_over_time({app="a"}[1h]))`, `sort(bytes_rate({app="a"} != "y" [1m]))`,
		`sort_desc(bytes_over_time({app="a"} |~ "z.+" [5m]))`, `topk(10, absent_over_time({app="a"}[5m]))`,
		`topk(3, rate({app="a"} | json | status >= 500 [5m]))`, `topk(3, last_over_time({app="a"} | logfmt | unwrap amount [5m]))`,
	)
	var out []string
	for _, q := range qs {
		p := rangeAggPlan(q)
		if p == nil {
			continue
		}
		if _, ok := p.Root.(*queryplan.Rank); !ok {
			continue
		}
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		b.Fatal("no lowered ranking in the corpus")
	}
	b.Logf("%d ranking queries", len(out))
	return out
}

// The string translator on the lowered rankings: text to LogsQL text.
func BenchmarkRankQueriesTranslator(b *testing.B) {
	qs := rankQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The string translator on the same queries under the tenant's stream labels.
func BenchmarkRankQueriesStreamLabelsTranslator(b *testing.B) {
	qs := rankQueries(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithLabels(qs[i%len(qs)], benchStreamLabels); err != nil {
			b.Fatal(err)
		}
	}
}

// The plan on the same queries: text to LogsQL text through Parse, Build, Resolve, the text screen and Plan.
func BenchmarkRankQueriesLowered(b *testing.B) { benchRankLowered(b, nil) }

// The plan on the same queries under the tenant's stream labels.
func BenchmarkRankQueriesStreamLabelsLowered(b *testing.B) { benchRankLowered(b, benchStreamLabels) }

func benchRankLowered(b *testing.B, label func(string) string) {
	qs := rankQueries(b)
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

// The screen and the plan alone on the same queries, built once: the lowering's own cost.
func BenchmarkRankQueriesPlanOnly(b *testing.B) {
	qs := rankQueries(b)
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
