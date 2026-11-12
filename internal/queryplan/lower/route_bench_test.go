package lower

import (
	"slices"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// routeBenchRequests are the request shapes the route rule decides: instant, and range with the step equal to,
// under and over a 5m range.
var routeBenchRequests = []RouteRequest{
	{Instant: true},
	{Step: 5 * time.Minute, Span: time.Hour},
	{Step: time.Minute, Span: time.Hour},
	{Step: 15 * time.Minute, Span: time.Hour},
}

// holdsWithout reports whether a vector aggregation of the plan has a without clause.
func holdsWithout(p *queryplan.Plan) bool {
	for n := p.Root; ; {
		v, ok := n.(*queryplan.VectorAgg)
		if !ok {
			return false
		}
		if v.By != nil && v.By.Without {
			return true
		}
		n = v.In
	}
}

// holdsRangeFunction reports whether the range aggregation of the plan is absent_over_time or a function over an
// unwrapped label, which the rule routes since it decides their stats.
func holdsRangeFunction(p *queryplan.Plan) bool {
	for n := p.Root; ; {
		switch v := n.(type) {
		case *queryplan.VectorAgg:
			n = v.In
		case *queryplan.RangeAgg:
			return v.Op == logql.RangeAbsentOverTime || slices.ContainsFunc(v.In.Stages, func(st queryplan.Stage) bool { _, ok := st.(*queryplan.Unwrap); return ok })
		default:
			return false
		}
	}
}

// routedPlans are the plan corpus's plans the route rule routes for every request shape: with a without clause
// (the plans routed since the rule folds them), or without one (the plans the rule has routed from the start, so
// the benchmarks of that set compare with the tree before the without clauses); of a range function the rule
// decides beyond the counts and rates (absent_over_time, the unwrap functions), or of neither kind.
func routedPlans(tb testing.TB, withoutClause, functions bool) []*queryplan.Plan {
	tb.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		tb.Fatal(err)
	}
	var out []*queryplan.Plan
	for _, q := range qs {
		p := routePlan(q)
		if p == nil {
			continue
		}
		routed := holdsWithout(p) == withoutClause && holdsRangeFunction(p) == functions
		for _, req := range routeBenchRequests {
			if _, err := Route(p, Profile{}, req); err != nil {
				routed = false
			}
		}
		if routed {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		tb.Fatal("no routed plan in the corpus")
	}
	return out
}

func benchRoutedPlansLowered(b *testing.B, withoutClause, functions bool) {
	plans := routedPlans(b, withoutClause, functions)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range plans {
			for range routeBenchRequests {
				if _, err := Plan(p, Profile{}); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

func benchRoutedPlansRoute(b *testing.B, withoutClause, functions bool) {
	plans := routedPlans(b, withoutClause, functions)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range plans {
			for _, req := range routeBenchRequests {
				if _, err := Route(p, Profile{}, req); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

// The lowering the route rule replaces on the routed plans with no without clause: Plan, once per request.
func BenchmarkRoutedPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, false, false) }

// The route rule on the same plans and requests: the lowering plus the route's decisions.
func BenchmarkRoutedPlansRoute(b *testing.B) { benchRoutedPlansRoute(b, false, false) }

// The same two on the plans with a without clause, which the rule routes since it folds them.
func BenchmarkRoutedWithoutPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, true, false) }
func BenchmarkRoutedWithoutPlansRoute(b *testing.B)   { benchRoutedPlansRoute(b, true, false) }

// And on the plans of absent_over_time and the unwrap functions, which the rule routes since it decides their stats.
func BenchmarkRoutedFunctionPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, false, true) }
func BenchmarkRoutedFunctionPlansRoute(b *testing.B)   { benchRoutedPlansRoute(b, false, true) }

// The route rule allocates no more than the lowering it decides over, on every routed plan and request: its
// decisions read the typed plan and the lowered stats pipes and allocate nothing of their own.
//
// conformance: architecture-syntax-package-layering
func TestRouteAllocatesNoMoreThanTheLowering(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("the race detector changes allocations")
	}
	for _, set := range []struct{ withoutClause, functions bool }{{false, false}, {true, false}, {false, true}} {
		for _, p := range routedPlans(t, set.withoutClause, set.functions) {
			for _, req := range routeBenchRequests {
				lowered := testing.AllocsPerRun(20, func() { _, _ = Plan(p, Profile{}) })
				routed := testing.AllocsPerRun(20, func() { _, _ = Route(p, Profile{}, req) })
				if routed > lowered {
					t.Fatalf("%s %+v: Route allocates %.0f, Plan %.0f", p.Key, req, routed, lowered)
				}
			}
		}
	}
}
