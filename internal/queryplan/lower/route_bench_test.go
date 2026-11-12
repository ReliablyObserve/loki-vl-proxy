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

// routeBenchOffsetRequests are the same request shapes with the end an offset needs.
var routeBenchOffsetRequests = func() []RouteRequest {
	out := slices.Clone(routeBenchRequests)
	for i := range out {
		out[i].End = routeTestEnd
	}
	return out
}()

// holdsOffset reports whether the range aggregation of the plan has an offset.
func holdsOffset(p *queryplan.Plan) bool {
	return p.Sem.OffsetKind != queryplan.OffsetNone
}

// requestsOf are the requests a set is timed with.
func requestsOf(set routeSet) []RouteRequest {
	if set == setOffset {
		return routeBenchOffsetRequests
	}
	return routeBenchRequests
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

// routeSet is a kind of plan the benchmarks time apart, so a shape the rule routes later does not move the sets
// measured before it.
type routeSet uint8

const (
	// setPlain are the plans with no without clause and no range function the rule decides beyond the counts and
	// rates: the plans the rule has routed from the start.
	setPlain routeSet = iota
	// setWithout are the plans with a without clause, which the rule routes since it folds them.
	setWithout
	// setFunction are the plans of absent_over_time and the unwrap functions at the root, which the rule routes
	// since it decides their stats.
	setFunction
	// setVectorUnwrap are the vector aggregations over an unwrap function, which the rule routes since the lowering
	// hands back their stats typed.
	setVectorUnwrap
	// setOffset are the plans with an offset, which the rule routes since it moves the request's bounds. They are
	// timed with a request that holds its end, apart from the sets above, which they leave as they were.
	setOffset
)

// setOf is the set a plan belongs to.
func setOf(p *queryplan.Plan) routeSet {
	switch {
	case holdsOffset(p):
		return setOffset
	case holdsRangeFunction(p) && !isRangeAggRoot(p):
		return setVectorUnwrap
	case holdsRangeFunction(p):
		return setFunction
	case holdsWithout(p):
		return setWithout
	}
	return setPlain
}

// isRangeAggRoot reports whether the plan is a range aggregation at the root.
func isRangeAggRoot(p *queryplan.Plan) bool {
	_, ok := p.Root.(*queryplan.RangeAgg)
	return ok
}

// routedPlans are the plan corpus's plans of a set that the route rule routes for every request shape.
func routedPlans(tb testing.TB, set routeSet) []*queryplan.Plan {
	tb.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		tb.Fatal(err)
	}
	var out []*queryplan.Plan
	for _, q := range qs {
		p := routePlan(q)
		if p == nil || setOf(p) != set {
			continue
		}
		routed := true
		for _, req := range requestsOf(set) {
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

func benchRoutedPlansLowered(b *testing.B, set routeSet) {
	plans := routedPlans(b, set)
	reqs := requestsOf(set)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range plans {
			for range reqs {
				if _, err := Plan(p, Profile{}); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

func benchRoutedPlansRoute(b *testing.B, set routeSet) {
	plans := routedPlans(b, set)
	reqs := requestsOf(set)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range plans {
			for _, req := range reqs {
				if _, err := Route(p, Profile{}, req); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

// The lowering the route rule replaces on the routed plans with no without clause: Plan, once per request.
func BenchmarkRoutedPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, setPlain) }

// The route rule on the same plans and requests: the lowering plus the route's decisions.
func BenchmarkRoutedPlansRoute(b *testing.B) { benchRoutedPlansRoute(b, setPlain) }

// The same two on the plans with a without clause, which the rule routes since it folds them.
func BenchmarkRoutedWithoutPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, setWithout) }
func BenchmarkRoutedWithoutPlansRoute(b *testing.B)   { benchRoutedPlansRoute(b, setWithout) }

// And on the plans of absent_over_time and the unwrap functions, which the rule routes since it decides their stats.
func BenchmarkRoutedFunctionPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, setFunction) }
func BenchmarkRoutedFunctionPlansRoute(b *testing.B)   { benchRoutedPlansRoute(b, setFunction) }

// And on the vector aggregations over the unwrap functions, which the rule routes since the lowering hands back their
// stats typed. These plans were not routed before, so they are timed apart from the sets above.
func BenchmarkRoutedVectorUnwrapPlansLowered(b *testing.B) {
	benchRoutedPlansLowered(b, setVectorUnwrap)
}
func BenchmarkRoutedVectorUnwrapPlansRoute(b *testing.B) { benchRoutedPlansRoute(b, setVectorUnwrap) }

// And on the plans with an offset, which the rule routes since it moves the request's bounds by it. These plans were
// not routed before, so they are timed apart from the sets above.
func BenchmarkRoutedOffsetPlansLowered(b *testing.B) { benchRoutedPlansLowered(b, setOffset) }
func BenchmarkRoutedOffsetPlansRoute(b *testing.B)   { benchRoutedPlansRoute(b, setOffset) }

// routedBinaryPlans are the plan corpus's binary operations that RouteQuery routes for every request shape, with the
// end an offset needs.
func routedBinaryPlans(tb testing.TB) []*queryplan.Plan {
	tb.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		tb.Fatal(err)
	}
	var out []*queryplan.Plan
	for _, q := range qs {
		p := routeQueryPlan(q)
		if p == nil {
			continue
		}
		if _, ok := p.Root.(*queryplan.BinOp); !ok {
			continue
		}
		routed := true
		for _, req := range routeBenchOffsetRequests {
			if _, err := RouteQuery(p, Profile{}, req); err != nil {
				routed = false
			}
		}
		if routed {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		tb.Fatal("no routed binary operation in the corpus")
	}
	return out
}

// And on the binary operations, which the rule routes since it routes their operands. These plans were not routed
// before, so they are timed apart from the sets above.
func BenchmarkRoutedBinaryPlansLowered(b *testing.B) {
	plans := routedBinaryPlans(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range plans {
			for range routeBenchOffsetRequests {
				if _, err := Plan(p, Profile{}); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

func BenchmarkRoutedBinaryPlansRoute(b *testing.B) {
	plans := routedBinaryPlans(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range plans {
			for _, req := range routeBenchOffsetRequests {
				if _, err := RouteQuery(p, Profile{}, req); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

// The route rule allocates no more than the lowering it decides over, on every routed plan and request: its
// decisions read the typed plan and the stats the lowering hands back and allocate nothing of their own.
//
// conformance: architecture-syntax-package-layering
func TestRouteAllocatesNoMoreThanTheLowering(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("the race detector changes allocations")
	}
	for _, set := range []routeSet{setPlain, setWithout, setFunction, setVectorUnwrap, setOffset} {
		for _, p := range routedPlans(t, set) {
			for _, req := range requestsOf(set) {
				lowered := testing.AllocsPerRun(20, func() { _, _ = Plan(p, Profile{}) })
				routed := testing.AllocsPerRun(20, func() { _, _ = Route(p, Profile{}, req) })
				if routed > lowered {
					t.Fatalf("%s %+v: Route allocates %.0f, Plan %.0f", p.Key, req, routed, lowered)
				}
			}
		}
	}
}

// The same holds for a binary operation: routing its operands allocates no more than lowering the operation.
//
// conformance: architecture-syntax-package-layering
func TestRouteQueryAllocatesNoMoreThanTheLowering(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("the race detector changes allocations")
	}
	for _, p := range routedBinaryPlans(t) {
		for _, req := range routeBenchOffsetRequests {
			lowered := testing.AllocsPerRun(20, func() { _, _ = Plan(p, Profile{}) })
			routed := testing.AllocsPerRun(20, func() { _, _ = RouteQuery(p, Profile{}, req) })
			if routed > lowered {
				t.Fatalf("%s %+v: RouteQuery allocates %.0f, Plan %.0f", p.Key, req, routed, lowered)
			}
		}
	}
}
