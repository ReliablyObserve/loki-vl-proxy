package lower

import (
	"testing"
	"time"

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

// routedPlans are the plan corpus's plans the route rule routes for every request shape: with a without clause
// (the plans routed since the rule folds them), or without one (the plans the rule has routed from the start, so
// the benchmarks of that set compare with the tree before the without clauses).
func routedPlans(tb testing.TB, withoutClause bool) []*queryplan.Plan {
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
		routed := holdsWithout(p) == withoutClause
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

func benchRoutedPlansLowered(b *testing.B, withoutClause bool) {
	plans := routedPlans(b, withoutClause)
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

func benchRoutedPlansRoute(b *testing.B, withoutClause bool) {
	plans := routedPlans(b, withoutClause)
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
func BenchmarkRoutedPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, false) }

// The route rule on the same plans and requests: the lowering plus the route's decisions.
func BenchmarkRoutedPlansRoute(b *testing.B) { benchRoutedPlansRoute(b, false) }

// The same two on the plans with a without clause, which the rule routes since it folds them.
func BenchmarkRoutedWithoutPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, true) }
func BenchmarkRoutedWithoutPlansRoute(b *testing.B)   { benchRoutedPlansRoute(b, true) }

// The route rule allocates no more than the lowering it decides over, on every routed plan and request: its
// decisions read the typed plan and the lowered stats pipes and allocate nothing of their own.
//
// conformance: architecture-syntax-package-layering
func TestRouteAllocatesNoMoreThanTheLowering(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("the race detector changes allocations")
	}
	for _, withoutClause := range []bool{false, true} {
		for _, p := range routedPlans(t, withoutClause) {
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
