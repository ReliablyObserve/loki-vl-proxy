package lower

import (
	"errors"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// evaluatedPlan is the resolved plan of a query whose root is a ranking or a label_replace, nil for another.
func evaluatedPlan(q string) *queryplan.Plan {
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
	case *queryplan.Rank, *queryplan.LabelReplace:
		return p
	}
	return nil
}

// The ranking the handlers evaluate is the innermost one under rankings and sum, avg, min, max or count layers, or the
// root ranking when another level stands between; its operand is what it reads.
//
// conformance: architecture-syntax-package-layering
func TestRankedOperand(t *testing.T) {
	for _, c := range []struct {
		q       string
		op      logql.VectorOp
		k       int
		operand string
	}{
		{`topk(3, sum by (app) (rate({app="a"}[5m])))`, logql.VectorTopK, 3, "VectorAgg"},
		{`sort(rate({app="a"}[5m]))`, logql.VectorSort, 0, "RangeAgg"},
		{`topk(2, topk(3, sum by (app) (rate({app="a"}[5m]))))`, logql.VectorTopK, 3, "VectorAgg"},
		{`topk(2, sum by (app) (bottomk(6, sum by (app, env) (rate({app="a"}[5m])))))`, logql.VectorBottomK, 6, "VectorAgg"},
		{`topk(3, sort(rate({app="a"}[5m])))`, logql.VectorSort, 0, "RangeAgg"},
		// a level the handlers do not fold stands between: the root ranks the level's answer
		{`sort_desc(stddev by (app) (topk(2, rate({app="a"}[5m]))))`, logql.VectorSortDesc, 0, "VectorAgg"},
		{`topk(2, topk(3, stddev by (app) (topk(4, rate({app="a"}[5m])))))`, logql.VectorTopK, 2, "Rank"},
		{`topk(2, sum by (app) (stddev by (app, env) (rate({app="a"}[5m]))))`, logql.VectorTopK, 2, "VectorAgg"},
		// a level below the innermost ranking is its operand's
		{`topk(2, topk(3, stddev by (app) (rate({app="a"}[5m]))))`, logql.VectorTopK, 3, "VectorAgg"},
		{`topk(3, label_replace(sum by (app) (rate({app="a"}[5m])), "b", "$1", "app", "(.*)"))`, logql.VectorTopK, 3, "LabelReplace"},
	} {
		p := evaluatedPlan(c.q)
		if p == nil {
			t.Fatalf("%q: no ranking plan", c.q)
		}
		rank, operand := RankedOperand(p.Root.(*queryplan.Rank))
		if rank.Op != c.op || rank.K != c.k || Construct(operand) != c.operand || operand != rank.In {
			t.Fatalf("%q: ranking %s %d over %s, want %s %d over %s", c.q, rank.Op, rank.K, Construct(operand), c.op, c.k, c.operand)
		}
	}
	p := evaluatedPlan(`label_replace(label_replace(rate({app="a"}[5m]), "b", "$1", "app", "(.*)"), "c", "$1", "app", "(.*)")`)
	if got := LabelReplaceOperand(p.Root); Construct(got) != "RangeAgg" {
		t.Fatalf("LabelReplaceOperand = %s", Construct(got))
	}
	if n := (&queryplan.VectorLit{}); LabelReplaceOperand(n) != queryplan.Node(n) {
		t.Fatal("LabelReplaceOperand of a node without label_replace is not the node")
	}
}

// A ranking or a label_replace at the root is routed as its operand, under every request shape: the ranking reads a
// line count or a byte sum by labels on tumbling windows as one grid of rows, folds a `without` itself, and leaves the
// stream's series it ranks per step to raw rows; a label_replace around a ranking is read as another kind of query.
//
// conformance: architecture-syntax-package-layering
func TestRouteQueryOfARankingOrLabelReplace(t *testing.T) {
	hour := time.Hour
	instant := RouteRequest{Instant: true, End: routeTestEnd}
	tumbling := RouteRequest{Step: 5 * time.Minute, Span: hour, End: routeTestEnd}
	buckets := RouteRequest{Step: time.Minute, Span: hour, End: routeTestEnd}
	const grouped = `sum by (app) (count_over_time({app="a"}[5m]))`
	for _, c := range []struct {
		q      string
		req    RouteRequest
		window Window
		grid   bool
		fold   WithoutFold
		binary bool
		// construct names the delegation the query is left to.
		construct string
	}{
		{q: `topk(3, ` + grouped + `)`, req: tumbling, window: WindowTumbling, grid: true},
		{q: `bottomk by (app) (3, sum by (app, env) (bytes_rate({app="a"}[5m])))`, req: tumbling, window: WindowTumbling, grid: true},
		{q: `topk(3, ` + grouped + `)`, req: buckets, window: WindowBuckets},
		{q: `topk(3, ` + grouped + `)`, req: instant, window: WindowInstant},
		{q: `sort(` + grouped + `)`, req: tumbling, window: WindowTumbling},
		{q: `topk(2, topk(3, ` + grouped + `))`, req: tumbling, window: WindowTumbling, grid: true},
		{q: `topk(3, label_replace(` + grouped + `, "b", "$1", "app", "(.*)"))`, req: tumbling, window: WindowTumbling, grid: true},
		// one series: nothing to rank per step from rows
		{q: `topk(1, sum(count_over_time({app="a"}[5m])))`, req: tumbling, window: WindowTumbling},
		// an unwrapped value is read from its own stats
		{q: `topk(1, sum by (app) (sum_over_time({app="a"} | unwrap n [5m])))`, req: tumbling, window: WindowTumbling},
		// the stream's series, ranked per step, are read as raw rows
		{q: `topk(2, count_over_time({app="a"}[5m]))`, req: tumbling, construct: "Rank route ranked rows"},
		{q: `topk(2, count_over_time({app="a"}[5m]))`, req: buckets, window: WindowBuckets},
		{q: `bottomk(5, max by (app) (count_over_time({app="a"}[5m])))`, req: tumbling, construct: "Rank route ranked rows"},
		{q: `topk(1, sum without (pod) (count_over_time({app="a"}[5m])))`, req: tumbling, construct: "Rank route ranked rows"},
		// a without under a ranking is folded by the handlers, not pushed down
		{q: `sort(sum without (pod) (count_over_time({app="a"}[5m])))`, req: tumbling, window: WindowTumbling, fold: FoldRows},
		{q: `label_replace(` + grouped + `, "b", "$1", "app", "(.*)")`, req: buckets, window: WindowBuckets},
		{q: `label_replace(sum without (pod) (count_over_time({app="a"}[5m])), "b", "$1", "app", "(.*)")`, req: tumbling, window: WindowTumbling, fold: FoldPushed},
		{q: `label_replace(sum by (app) (rate({app="a"}[5m])) / sum by (app) (rate({app="b"}[5m])), "b", "$1", "app", "(.*)")`, req: instant, window: WindowInstant, binary: true},
		// a label_replace around a ranking is routed as the ranking; nested with a stddev, not as one evaluation
		{q: `label_replace(topk(2, ` + grouped + `), "b", "$1", "app", "(.*)")`, req: instant, window: WindowInstant},
		{q: `label_replace(topk(2, ` + grouped + `), "b", "$1", "app", "(.*)")`, req: tumbling, window: WindowTumbling, grid: true},
		{q: `label_replace(label_replace(sort_desc(` + grouped + `), "b", "$1", "app", "(.*)"), "c", "$1", "b", "(.*)")`, req: tumbling, window: WindowTumbling},
		{q: `label_replace(topk(2, stddev by (app) (count_over_time({app="a"}[5m]))), "b", "$1", "app", "(.*)")`, req: instant, construct: "LabelReplace route ranking operand"},
		// the operand's own delegation, named under the root
		{q: `topk(2, sum by (app) (count_over_time({app="a"} | json [5m])))`, req: instant, construct: "Rank operand VectorAgg route pipeline"},
		{q: `label_replace(rate({app="a"} | json [5m]), "b", "$1", "app", "(.*)")`, req: instant, construct: "LabelReplace operand RangeAgg route pipeline"},
		// the lowering's own delegation of the whole query comes first
		{q: `topk(2, sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m])))`, req: instant, construct: "Rank operand BinOp"},
	} {
		t.Run(c.q, func(t *testing.T) {
			p := evaluatedPlan(c.q)
			if p == nil {
				t.Fatal("no plan")
			}
			got, err := RouteQuery(p, Profile{}, c.req)
			if c.construct != "" {
				var nl *NotLoweredError
				if !errors.As(err, &nl) || nl.Construct != c.construct {
					t.Fatalf("RouteQuery = %+v, %v; want the delegation %q", got, err, c.construct)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			r := got.Operands()[0]
			if got.Binary != c.binary || r.Window != c.window || r.RankedGrid != c.grid || r.Fold != c.fold {
				t.Fatalf("binary %v window %d grid %v fold %d, want %v %d %v %d", got.Binary, r.Window, r.RankedGrid, r.Fold, c.binary, c.window, c.grid, c.fold)
			}
		})
	}
}

// Every ranking or label_replace the rule routes on the corpus is one the lowering writes, and its operand is routed
// as the query it is alone, but for what the ranking changes (the grid, the fold the handlers keep).
//
// conformance: architecture-syntax-package-layering
func TestRouteQueryOfAnOperandIsTheOperandAlone(t *testing.T) {
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		t.Fatal(err)
	}
	reqs := []RouteRequest{{Instant: true, End: routeTestEnd}, {Step: time.Minute, Span: time.Hour, End: routeTestEnd}, {Step: 5 * time.Minute, Span: time.Hour, End: routeTestEnd}, {Step: 15 * time.Minute, Span: time.Hour, End: routeTestEnd}}
	routed := map[string]int{}
	for _, q := range qs {
		p := evaluatedPlan(q)
		if p == nil {
			continue
		}
		operand := LabelReplaceOperand(p.Root)
		kind := "LabelReplace"
		if r, ok := p.Root.(*queryplan.Rank); ok {
			kind = "Rank"
			_, operand = RankedOperand(r)
		}
		sub := *p
		sub.Root = operand
		for _, req := range reqs {
			got, err := RouteQuery(p, Profile{}, req)
			if err != nil {
				continue
			}
			routed[kind]++
			if _, err := Plan(p, Profile{}); err != nil {
				t.Fatalf("%q routed, but Plan: %v", q, err)
			}
			alone, err := RouteQuery(&sub, Profile{}, req)
			if err != nil || len(alone.Operands()) != len(got.Operands()) || alone.Binary != got.Binary {
				t.Fatalf("%q under %+v: routed %+v, its operand alone %+v, %v", q, req, got.Operands(), alone.Operands(), err)
			}
			for i, r := range got.Operands() {
				a := alone.Operands()[i]
				if kind == "Rank" {
					if a.Fold == FoldPushed {
						a.Fold = FoldRows
					}
					a.RankedGrid = r.RankedGrid
				}
				if r != a {
					t.Fatalf("%q under %+v: operand %d routed as %+v, alone as %+v", q, req, i, r, a)
				}
			}
		}
	}
	if routed["Rank"] == 0 || routed["LabelReplace"] == 0 {
		t.Fatalf("routed corpus rows: %v", routed)
	}
}

// The route rule of a ranking or a label_replace at the root never panics, and either names the delegation or
// routes the query as its operand alone (but for the grid and the fold a ranking changes), a query the lowering
// writes.
func FuzzRouteEvaluated(f *testing.F) {
	f.Add(`topk(3, sum by (app) (count_over_time({app="a"}[5m])))`, int64(5*time.Minute), int64(time.Hour), false)
	f.Add(`bottomk by (app) (1, sum by (app, env) (bytes_rate({app="a"}[1m])))`, int64(time.Minute), int64(time.Hour), false)
	f.Add(`sort_desc(sum without (pod) (count_over_time({app="a"}[5m])))`, int64(5*time.Minute), int64(time.Hour), false)
	f.Add(`topk(2, topk(3, stddev by (app) (rate({app="a"}[5m]))))`, int64(0), int64(0), true)
	f.Add(`topk(2, count_over_time({app="a"}[5m]))`, int64(5*time.Minute), int64(time.Hour), false)
	f.Add(`label_replace(sum by (app) (rate({app="a"}[5m])), "b", "$1", "app", "(.*)")`, int64(time.Minute), int64(24*time.Hour), false)
	f.Add(`label_replace(sum by (app) (rate({app="a"}[5m])) / sum by (app) (rate({app="b"}[5m])), "b", "$1", "app", "(.*)")`, int64(0), int64(0), true)
	f.Add(`label_replace(topk(2, rate({app="a"}[5m])), "b", "$1", "app", "(.*)")`, int64(0), int64(0), true)
	f.Fuzz(func(t *testing.T, q string, step, span int64, instant bool) {
		if len(q) > 512 {
			return
		}
		p := evaluatedPlan(q)
		if p == nil {
			return
		}
		req := RouteRequest{Instant: instant, Step: time.Duration(step), Span: time.Duration(span), End: routeTestEnd}
		got, err := RouteQuery(p, Profile{}, req)
		if err != nil {
			if !errors.Is(err, ErrNotLowered) {
				t.Fatalf("%q: %v is not a delegation", q, err)
			}
			return
		}
		if _, err := Plan(p, Profile{}); err != nil {
			t.Fatalf("%q: routed, but Plan: %v", q, err)
		}
		operand := LabelReplaceOperand(p.Root)
		r, ranks := p.Root.(*queryplan.Rank)
		if ranks {
			_, operand = RankedOperand(r)
		}
		sub := *p
		sub.Root = operand
		alone, err := RouteQuery(&sub, Profile{}, req)
		if err != nil || alone.Binary != got.Binary || len(alone.Operands()) != len(got.Operands()) {
			t.Fatalf("%q: routed %+v, its operand alone %+v, %v", q, got.Operands(), alone.Operands(), err)
		}
		for i, o := range got.Operands() {
			a := alone.Operands()[i]
			if ranks {
				if a.Fold == FoldPushed {
					a.Fold = FoldRows
				}
				if o.RankedGrid && (a.Window != WindowTumbling || !a.LabelGrouped) {
					t.Fatalf("%q: a ranked grid on %+v", q, a)
				}
				a.RankedGrid = o.RankedGrid
			} else if o.RankedGrid {
				t.Fatalf("%q: a ranked grid under no ranking", q)
			}
			if o != a {
				t.Fatalf("%q: operand %d routed as %+v, alone as %+v", q, i, o, a)
			}
		}
	})
}
