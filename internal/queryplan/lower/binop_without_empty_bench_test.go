package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// binOpEmptyWithoutQueries are binary operations with a `without ()` operand, the ones the lowering writes (a fold
// of no label over the operand's series rows), and the same queries with a label in the clause. They are not in
// BenchmarkBinOpQueries* (a binary operation held to the proxy's route is left out of those), which keep the inputs
// they were measured on.
func binOpEmptyWithoutQueries(clause string) []string {
	rate, sum := `rate({app="a"}[5m])`, `sum by (app) (count_over_time({app="b"}[5m]))`
	w := func(agg, in string) string { return agg + ` without (` + clause + `) (` + in + `)` }
	return []string{
		w("sum", rate) + ` / 2`, `100 > ` + w("max", `bytes_rate({app="a"} |= "x" [5m])`), w("count", `count_over_time({app="a"}[5m])`) + ` + on() vector(0)`,
		sum + ` / on(app) group_left(env) ` + w("avg", rate), w("sum", rate) + ` - ignoring(pod) ` + w("min", rate),
		`vector(1) > ` + w("sum", `bytes_over_time({app="a"}[5m])`), w("sum", rate) + ` * on(app) sum without (pod) (` + rate + `)`,
	}
}

func benchBinOpEmptyWithout(b *testing.B, clause string, build bool) {
	qs := binOpEmptyWithoutQueries(clause)
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
		q, p := qs[i%len(qs)], plans[i%len(plans)]
		if build {
			expr, err := logql.Parse(q)
			if err != nil {
				b.Fatal(err)
			}
			if p, err = queryplan.Build(expr, queryplan.Inputs{}); err != nil {
				b.Fatal(err)
			}
			queryplan.Resolve(p)
		}
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}

// The text screen and the plan alone on the `without ()` rows, plans built once: the lowering's own cost.
func BenchmarkBinOpEmptyWithoutQueriesPlanOnly(b *testing.B) { benchBinOpEmptyWithout(b, "", false) }

// Parse, Build, Resolve, the text screen and Plan on the `without ()` rows.
func BenchmarkBinOpEmptyWithoutQueriesLowered(b *testing.B) { benchBinOpEmptyWithout(b, "", true) }

// The same rows with a label in each clause, the lowering this one is built on: the two cost the same.
func BenchmarkBinOpLabelledWithoutQueriesPlanOnly(b *testing.B) {
	benchBinOpEmptyWithout(b, "pod", false)
}

func BenchmarkBinOpLabelledWithoutQueriesLowered(b *testing.B) {
	benchBinOpEmptyWithout(b, "pod", true)
}
