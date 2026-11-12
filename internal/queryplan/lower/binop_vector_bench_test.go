package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// binOpVectorQueries are binary operations with a vector(n) operand next to a range or vector aggregation, the ones
// the lowering writes: either side, the arithmetic and comparison operators and the vector matching modifiers. They are
// not in BenchmarkBinOpQueries* (a binary operation held to the proxy's route is left out of those), which keep the
// inputs they were measured on.
func binOpVectorQueries() []string {
	rate, sum := `rate({app="a"}[5m])`, `sum by (app) (count_over_time({app="b"}[5m]))`
	return []string{
		rate + ` + vector(1)`, `vector(2) * ` + sum, rate + ` > vector(0.5)`, `vector(1) <= ` + sum, rate + ` / on(app) vector(2)`,
		sum + ` - ignoring(pod) vector(1)`, rate + ` * on(app) group_left(zone) vector(2)`, `vector(2) / ignoring(pod) group_right() ` + sum,
		`sum by () (count_over_time({app="a"}[5m])) + on() vector(0)`, `bytes_rate({app="a"}[5m] offset 1h) - vector(1)`, `sum without (pod) (` + rate + `) + vector(1)`,
	}
}

// The text screen and the plan alone on the vector(n) rows, plans built once: the lowering's own cost.
func BenchmarkBinOpVectorQueriesPlanOnly(b *testing.B) {
	qs := binOpVectorQueries()
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

// Parse, Build, Resolve, the text screen and Plan on the vector(n) rows.
func BenchmarkBinOpVectorQueriesLowered(b *testing.B) {
	qs := binOpVectorQueries()
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
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}
