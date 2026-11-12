package lower

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// routeQueryPlan is the resolved plan of a query RouteQuery may be asked about (an aggregation or a binary operation
// at the root), nil for another.
func routeQueryPlan(q string) *queryplan.Plan {
	expr, msg := logql.ValidateLogQLExpr(q)
	if msg != "" || expr == nil {
		return nil
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil
	}
	queryplan.Resolve(p)
	switch p.Root.(type) {
	case *queryplan.RangeAgg, *queryplan.VectorAgg, *queryplan.BinOp:
		return p
	}
	return nil
}

// A binary operation is routed as its operands are: the handlers translate the operation, then evaluate each range
// or vector aggregation operand as a query of its own over the same request, and a number or vector(n) operand has no
// route. The table pins the windows each operand is read with under every request shape, the offsets, and the
// delegations (the lowering's own, an operand the rule does not route, operands that share a probe).
//
// conformance: architecture-syntax-package-layering
func TestRouteQueryComposesTheOperands(t *testing.T) {
	hour := time.Hour
	instant := RouteRequest{Instant: true, End: routeTestEnd}
	tumbling := RouteRequest{Step: 5 * time.Minute, Span: hour, End: routeTestEnd}
	buckets := RouteRequest{Step: time.Minute, Span: hour, End: routeTestEnd}
	phase := RouteRequest{Step: 15 * time.Minute, Span: hour, End: routeTestEnd}
	day := RouteRequest{Step: time.Minute, Span: 24 * hour, End: routeTestEnd}
	for _, c := range []struct {
		q       string
		req     RouteRequest
		windows []Window
		probes  []bool
		offsets []time.Duration
		// construct names the delegation the query is left to.
		construct string
	}{
		// a number on one side: the other operand's route, under every request shape
		{q: `sum by (level) (count_over_time({app="a"}[5m])) > 0`, req: instant, windows: []Window{WindowInstant}, probes: []bool{false}},
		{q: `sum by (level) (count_over_time({app="a"}[5m])) > 0`, req: tumbling, windows: []Window{WindowTumbling}, probes: []bool{false}},
		{q: `sum by (level) (count_over_time({app="a"}[5m])) > 0`, req: buckets, windows: []Window{WindowBuckets}, probes: []bool{false}},
		{q: `sum by (level) (count_over_time({app="a"}[5m])) > 0`, req: phase, windows: []Window{WindowPhase}, probes: []bool{false}},
		{q: `sum by (level) (count_over_time({app="a"}[5m])) > 0`, req: day, windows: []Window{WindowBuckets}, probes: []bool{false}},
		{q: `rate({app="a"}[5m]) % 2`, req: tumbling, windows: []Window{WindowTumbling}, probes: []bool{true}},
		{q: `2 * rate({app="a"}[5m])`, req: buckets, windows: []Window{WindowBuckets}, probes: []bool{true}},
		{q: `5 / sum(rate({app="a"}[1m]))`, req: instant, windows: []Window{WindowInstant}, probes: []bool{true}},
		// vector(n) next to an aggregation is a constant to the route
		{q: `sum by (app) (rate({app="a"}[5m])) + on() vector(0)`, req: phase, windows: []Window{WindowPhase}, probes: []bool{false}},
		{q: `vector(2) * sum by (app) (rate({app="a"}[5m]))`, req: tumbling, windows: []Window{WindowTumbling}, probes: []bool{false}},
		// two aggregations: each read with its own range under the one request
		{q: `sum by (app) (rate({app="a"}[5m])) / sum by (app) (rate({app="b"}[1m]))`, req: tumbling, windows: []Window{WindowTumbling, WindowPhase}, probes: []bool{false, false}},
		{q: `sum by (app) (rate({app="a"}[5m])) / sum by (app) (rate({app="b"}[1m]))`, req: buckets, windows: []Window{WindowBuckets, WindowTumbling}, probes: []bool{false, false}},
		{q: `rate({app="a",pod="p1"}[5m]) / ignoring(pod) rate({app="a",pod="p2"}[5m])`, req: tumbling, windows: []Window{WindowTumbling, WindowTumbling}, probes: []bool{true, true}},
		{q: `sum by (app, pod) (count_over_time({cluster="c"}[5m])) * on(app) group_left(level) sum by (app, level) (count_over_time({cluster="c", level="error"}[5m]))`, req: instant, windows: []Window{WindowInstant, WindowInstant}, probes: []bool{false, false}},
		// one log selector read by two operands: the same windows ask the same probe, so they are routed; windows that
		// differ share a probe whose request depends on which operand is asked first, so they are not (an instant
		// request probes nothing; an operand with no level chain has no probe)
		{q: `rate({app="a"}[5m]) / bytes_rate({app="a"}[5m])`, req: tumbling, windows: []Window{WindowTumbling, WindowTumbling}, probes: []bool{true, true}},
		{q: `sum(rate({app="a"}[5m])) - sum(rate({app="a"}[1m]))`, req: tumbling, construct: "BinOp route shared level probe"},
		{q: `sum(rate({app="a"}[5m])) - sum(rate({app="a"}[1m]))`, req: instant, windows: []Window{WindowInstant, WindowInstant}, probes: []bool{true, true}},
		{q: `sum by (app) (rate({app="a"}[5m])) - sum by (app) (rate({app="a"}[1m]))`, req: tumbling, windows: []Window{WindowTumbling, WindowPhase}, probes: []bool{false, false}},
		{q: `sum(rate({app="a"}[5m])) - sum(rate({app="a"} |= "x" [1m]))`, req: tumbling, windows: []Window{WindowTumbling, WindowPhase}, probes: []bool{true, true}},
		// an offset: one for the whole query, whichever operands carry it
		{q: `rate({app="a"}[5m] offset 1h) > 1`, req: tumbling, windows: []Window{WindowTumbling}, probes: []bool{true}, offsets: []time.Duration{hour}},
		{q: `rate({app="a"}[5m] offset 1h) / rate({app="b"}[5m] offset 1h)`, req: instant, windows: []Window{WindowInstant, WindowInstant}, probes: []bool{true, true}, offsets: []time.Duration{hour, hour}},
		{q: `sum by (app) (rate({app="a"}[5m] offset -1h)) + on() vector(0)`, req: buckets, windows: []Window{WindowBuckets}, probes: []bool{false}, offsets: []time.Duration{-hour}},
		{q: `rate({app="a"}[5m] offset 1h) / rate({app="b"}[5m])`, req: tumbling, construct: "BinOp route mixed offsets"},
		{q: `rate({app="a"}[5m]) / rate({app="b"}[5m] offset 1h)`, req: instant, construct: "BinOp route mixed offsets"},
		{q: `rate({app="a"}[5m] offset 1h) / rate({app="b"}[5m] offset 2h)`, req: instant, construct: "BinOp route mixed offsets"},
		{q: `rate({app="a"}[5m] offset 400000h) > 1`, req: instant, construct: "BinOp operand RangeAgg route offset bounds"},
		// an offset the step does not divide: any binary operation but an aggregation against a number aligns the moved
		// bounds to the step; an instant request has none
		{q: `rate({app="a"}[5m] offset 5m) > 1`, req: phase, windows: []Window{WindowPhase}, probes: []bool{true}, offsets: []time.Duration{5 * time.Minute}},
		{q: `rate({app="a"}[5m] offset 5m) / rate({app="b"}[5m] offset 5m)`, req: phase, construct: "BinOp route offset step"},
		{q: `rate({app="a"}[5m] offset 5m) / rate({app="b"}[5m] offset 5m)`, req: instant, windows: []Window{WindowInstant, WindowInstant}, probes: []bool{true, true}, offsets: []time.Duration{5 * time.Minute, 5 * time.Minute}},
		{q: `rate({app="a"}[5m] offset 15m) / rate({app="b"}[5m] offset 15m)`, req: phase, windows: []Window{WindowPhase, WindowPhase}, probes: []bool{true, true}, offsets: []time.Duration{15 * time.Minute, 15 * time.Minute}},
		{q: `sum by (pod) (rate({app="a"}[5m] offset 5m)) + on() vector(0)`, req: phase, construct: "BinOp route offset step"},
		{q: `rate({app="a"}[5m] offset -5m) / on() vector(2)`, req: phase, construct: "BinOp route offset step"},
		// a window that does not start on the step's grid before the offset moves it
		{q: `rate({app="a"}[5m] offset 15m) / on() vector(2)`, req: RouteRequest{Step: 15 * time.Minute, Span: hour + time.Minute, End: routeTestEnd}, construct: "BinOp route offset step"},
		{q: `rate({app="a"}[5m]) / rate({app="b"}[5m] offset 0s)`, req: instant, construct: "BinOp route mixed offsets"},
		{q: `rate({app="a"}[5m] offset 0s) / rate({app="b"}[5m] offset 0s)`, req: instant, windows: []Window{WindowInstant, WindowInstant}, probes: []bool{true, true}, offsets: []time.Duration{0, 0}},
		// no bucket grid, no step: the operand's own delegation
		{q: `rate({app="a"}[1001ms]) + 1`, req: RouteRequest{Step: time.Second, Span: hour}, construct: "BinOp operand RangeAgg route no bucket grid"},
		{q: `1 + sum by (pod) (count_over_time({app="a"}[5m]))`, req: RouteRequest{Step: time.Second, Span: 24 * hour}, construct: "BinOp operand VectorAgg route no bucket grid"},
		{q: `count_over_time({app="a"}[1m]) > 1`, req: RouteRequest{Span: hour}, construct: "BinOp operand RangeAgg route step"},
		// absent_over_time has a route of its own, with its own steps
		{q: `absent_over_time({app="a"}[5m]) + 1`, req: instant, construct: "BinOp route absent operand"},
		{q: `rate({app="a"}[5m]) / absent_over_time({app="b"}[5m] offset 1h)`, req: tumbling, construct: "BinOp route mixed offsets"},
		{q: `rate({app="a"}[5m]) / sum(absent_over_time({app="b"}[5m]))`, req: tumbling, construct: "BinOp operand VectorAgg route absent operand"},
		// an operand the rule does not route delegates the operation, whichever side it is on
		{q: `sum(rate({app="a"} | unwrap n [5m])) / 2`, req: instant, construct: "BinOp operand VectorAgg route unwrap rate"},
		{q: `rate({app="a"}[5m]) / sum(rate({app="a"} | unwrap n [5m]))`, req: instant, construct: "BinOp operand VectorAgg route unwrap rate"},
		{q: `rate({app="a"} | json [5m]) > 1`, req: instant, construct: "BinOp operand RangeAgg route pipeline"},
		{q: `stddev(rate({app="a"}[5m])) / 2`, req: instant, construct: "BinOp operand VectorAgg route evaluated level"},
		{q: `sum without () (count_over_time({app="a"}[5m])) / 2`, req: instant, construct: "BinOp operand VectorAgg without empty"},
		{q: `sum_over_time({app="a"} | unwrap n [5m]) / sum_over_time({app="a"} | json | unwrap n [5m])`, req: instant, construct: "BinOp operand RangeAgg route unwrap pipeline"},
		// what the lowering leaves to the string translator keeps its name
		{q: `rate({app="a"}[5m]) and rate({app="b"}[5m])`, req: instant, construct: "BinOp set operator"},
		{q: `rate({app="a"}[5m]) > bool 1`, req: instant, construct: "BinOp bool"},
		{q: `1 + 2`, req: instant, construct: "BinOp scalar operands"},
		{q: `vector(1) + 2`, req: instant, construct: "BinOp constant operands"},
		{q: `label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)") + 1`, req: instant, construct: "BinOp operand LabelReplace"},
		{q: `topk(1, rate({app="a"}[5m])) + 1`, req: instant, construct: "BinOp operand Rank"},
		{q: `(rate({app="a"}[5m]) + 1) / 2`, req: instant, construct: "BinOp operand BinOp"},
	} {
		t.Run(c.q, func(t *testing.T) {
			p := routeQueryPlan(c.q)
			if p == nil {
				t.Fatalf("%q is no routable query", c.q)
			}
			got, err := RouteQuery(p, Profile{}, c.req)
			if c.construct != "" {
				var nl *NotLoweredError
				if !errors.As(err, &nl) || nl.Construct != c.construct {
					t.Fatalf("RouteQuery = %+v, %v; want a delegation named %q", got.Operands(), err, c.construct)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !got.Binary {
				t.Fatal("not a binary route")
			}
			var windows []Window
			var probes []bool
			var offsets []time.Duration
			for _, o := range got.Operands() {
				windows, probes, offsets = append(windows, o.Window), append(probes, o.LevelProbe), append(offsets, o.Offset)
			}
			if !slices.Equal(windows, c.windows) || !slices.Equal(probes, c.probes) {
				t.Fatalf("windows %v probes %v; want %v, %v", windows, probes, c.windows, c.probes)
			}
			if c.offsets != nil && !slices.Equal(offsets, c.offsets) {
				t.Fatalf("offsets %v; want %v", offsets, c.offsets)
			}
		})
	}
}

// An aggregation at the root is the one operand RouteQuery returns, and Route's own answer, with no binary mark.
//
// conformance: architecture-syntax-package-layering
func TestRouteQueryOfAnAggregationIsRoute(t *testing.T) {
	for _, q := range []string{
		`rate({app="a"}[5m])`,
		`sum by (pod) (count_over_time({app="a"}[5m]))`,
		`sum without (pod) (bytes_over_time({app="a"}[5m] offset 1h))`,
		`max by (pod) (max_over_time({app="a"} | unwrap n [1m]))`,
	} {
		p := routeQueryPlan(q)
		for _, req := range []RouteRequest{{Instant: true, End: routeTestEnd}, {Step: time.Minute, Span: time.Hour, End: routeTestEnd}} {
			want, err := Route(p, Profile{}, req)
			if err != nil {
				t.Fatalf("%q: %v", q, err)
			}
			got, err := RouteQuery(p, Profile{}, req)
			if err != nil || got.Binary || len(got.Operands()) != 1 || got.Operands()[0] != want {
				t.Fatalf("%q: RouteQuery = %+v, %v; Route = %+v", q, got.Operands(), err, want)
			}
		}
	}
	// A delegation of Route's keeps its name.
	p := routeQueryPlan(`rate({app="a"} | json [5m])`)
	if _, err := RouteQuery(p, Profile{}, RouteRequest{Instant: true}); err == nil || err.Error() != "lower: RangeAgg route pipeline is not lowered" {
		t.Fatalf("RouteQuery = %v", err)
	}
}

// Each operand of a binary operation is routed as the query it is evaluated as: the route RouteQuery gives it equals
// the route of the operand's printed form alone, which is what the handlers hand on, on every routed corpus row and
// request shape; and every operation it routes is one the lowering writes.
//
// conformance: architecture-syntax-package-layering
func TestRouteQueryIsWhatTheLoweringLowers(t *testing.T) {
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		t.Fatal(err)
	}
	reqs := append([]RouteRequest{{Instant: true, End: routeTestEnd}, {Step: time.Minute, Span: time.Hour, End: routeTestEnd}, {Step: 5 * time.Minute, Span: time.Hour, End: routeTestEnd}, {Step: 15 * time.Minute, Span: time.Hour, End: routeTestEnd}}, RouteRequest{Step: time.Minute, Span: 24 * time.Hour, End: routeTestEnd})
	routed := 0
	for _, q := range qs {
		p := routeQueryPlan(q)
		if p == nil {
			continue
		}
		bin, isBin := p.Root.(*queryplan.BinOp)
		if !isBin {
			continue
		}
		expr, _ := logql.ValidateLogQLExpr(q)
		sides := [2]string{expr.(*logql.BinOpExpr).Left.String(), expr.(*logql.BinOpExpr).Right.String()}
		for _, req := range reqs {
			got, err := RouteQuery(p, Profile{}, req)
			if err != nil {
				continue
			}
			routed++
			if _, err := Plan(p, Profile{}); err != nil {
				t.Fatalf("%q routed, but Plan: %v", q, err)
			}
			var alone []MetricRoute
			for i, n := range [2]queryplan.Node{bin.L, bin.R} {
				switch n.(type) {
				case *queryplan.RangeAgg, *queryplan.VectorAgg:
				default:
					continue
				}
				r, err := Route(routeQueryPlan(sides[i]), Profile{}, req)
				if err != nil {
					t.Fatalf("%q: operand %q: %v", q, sides[i], err)
				}
				alone = append(alone, r)
			}
			if !slices.Equal(got.Operands(), alone) {
				t.Fatalf("%q under %+v:\n operands %+v\n alone    %+v", q, req, got.Operands(), alone)
			}
		}
	}
	if routed == 0 {
		t.Fatal("no binary corpus row routed")
	}
}
