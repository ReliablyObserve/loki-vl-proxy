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

// routedPlans are the plan corpus's plans the route rule routes for every request shape.
func routedPlans(tb testing.TB) []*queryplan.Plan {
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
		routed := true
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

// The lowering the route rule replaces on the routed plans: Plan, once per request.
func BenchmarkRoutedPlansLowered(b *testing.B) {
	plans := routedPlans(b)
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

// The route rule on the same plans and requests: the lowering plus the route's decisions.
func BenchmarkRoutedPlansRoute(b *testing.B) {
	plans := routedPlans(b)
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

// The route rule allocates no more than the lowering it decides over, on every routed plan and request: its
// decisions read the typed plan and the lowered stats pipes and allocate nothing of their own.
//
// conformance: architecture-syntax-package-layering
func TestRouteAllocatesNoMoreThanTheLowering(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("the race detector changes allocations")
	}
	for _, p := range routedPlans(t) {
		for _, req := range routeBenchRequests {
			lowered := testing.AllocsPerRun(20, func() { _, _ = Plan(p, Profile{}) })
			routed := testing.AllocsPerRun(20, func() { _, _ = Route(p, Profile{}, req) })
			if routed > lowered {
				t.Fatalf("%s %+v: Route allocates %.0f, Plan %.0f", p.Key, req, routed, lowered)
			}
		}
	}
}
