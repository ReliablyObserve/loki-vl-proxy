package lower

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// A ranking lowers to a RankStep over the operand's own operators: the proxy evaluates the ranking over the answer of
// the operand it translates alone. The operand is lowered as it stands alone, the clause and k are the ranking's, a
// ranking over a ranking is a RankStep over a RankStep, and the sum, avg, min, max and count layers between or above
// rankings are SeriesAgg levels, innermost first. The RankStep encodes as its operand's request (rank_grid_test.go in
// internal/querycorpus holds it to the proxy's route on many more shapes).
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersRankingsToRankSteps(t *testing.T) {
	prof := Profile{}
	operandOf := func(q string) physical.Op {
		t.Helper()
		op, err := PlanQuery(q, rangeAggPlan(q), prof)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return op
	}
	lowered := func(q string) physical.Op {
		t.Helper()
		op, err := PlanQuery(q, rangeAggPlan(q), prof)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return op
	}
	rate := `rate({app="a"}[5m])`
	sum := `sum by (app) (` + rate + `)`
	unwrap := `sum_over_time({app="a"} | unwrap amount [5m])`
	app := &physical.Grouping{Labels: []string{"app"}}
	for _, c := range []struct {
		query string
		want  physical.Op
	}{
		{`topk(3, ` + rate + `)`, physical.RankStep{Op: "topk", K: 3, In: operandOf(rate)}},
		{`bottomk(2, ` + rate + `)`, physical.RankStep{Op: "bottomk", K: 2, In: operandOf(rate)}},
		{`sort(` + rate + `)`, physical.RankStep{Op: "sort", In: operandOf(rate)}},
		{`sort_desc(` + rate + `)`, physical.RankStep{Op: "sort_desc", In: operandOf(rate)}},
		{`topk by (app) (3, ` + rate + `)`, physical.RankStep{Op: "topk", K: 3, By: app, In: operandOf(rate)}},
		{`topk(3, ` + rate + `) by (app)`, physical.RankStep{Op: "topk", K: 3, By: app, In: operandOf(rate)}},
		{`bottomk without (app) (2, ` + rate + `)`, physical.RankStep{Op: "bottomk", K: 2, By: &physical.Grouping{Labels: []string{"app"}, Without: true}, In: operandOf(rate)}},
		{`topk by () (3, ` + rate + `)`, physical.RankStep{Op: "topk", K: 3, By: &physical.Grouping{}, In: operandOf(rate)}},
		{`topk(3, ` + sum + `)`, physical.RankStep{Op: "topk", K: 3, In: operandOf(sum)}},
		{`sort_desc(` + unwrap + `)`, physical.RankStep{Op: "sort_desc", In: operandOf(unwrap)}},
		{`topk(3, label_replace(` + sum + `, "a", "$1", "app", "(.*)"))`, physical.RankStep{Op: "topk", K: 3, In: operandOf(`label_replace(` + sum + `, "a", "$1", "app", "(.*)")`)}},
		{`bottomk(2, topk(3, ` + sum + `))`, physical.RankStep{Op: "bottomk", K: 2, In: physical.RankStep{Op: "topk", K: 3, In: operandOf(sum)}}},
		{`topk(1, sort(bottomk(2, ` + sum + `)))`, physical.RankStep{Op: "topk", K: 1, In: physical.RankStep{Op: "sort", In: physical.RankStep{Op: "bottomk", K: 2, In: operandOf(sum)}}}},
		{`topk(1, sum(sort(avg(bottomk(2, ` + rate + `)))))`, physical.RankStep{Op: "topk", K: 1, In: physical.SeriesAgg{Op: "sum", In: physical.RankStep{Op: "sort", In: physical.SeriesAgg{Op: "avg", In: physical.RankStep{Op: "bottomk", K: 2, In: operandOf(rate)}}}}}},
		{`topk(2, max by (app) (sort_desc(` + sum + `)))`, physical.RankStep{Op: "topk", K: 2, In: physical.SeriesAgg{Op: "max", By: app, In: physical.RankStep{Op: "sort_desc", In: operandOf(sum)}}}},
	} {
		got := lowered(c.query)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got %#v\nwant %#v", c.query, got, c.want)
		}
		if _, operand := physical.SplitEval(got); physical.Encode(operand) != physical.Encode(got) {
			t.Errorf("%s: Encode = %q, want the operand's %q", c.query, physical.Encode(got), physical.Encode(operand))
		}
	}
	// A chain of rankings wrapped whole in parentheses is routed, with the parentheses of every layer.
	for _, q := range []string{`(topk(2, topk(3, ` + rate + `)))`, `topk(2, (sort(` + rate + `)))`, `((bottomk(1, sort_desc(` + sum + `))))`, `sort(sort_desc ( ` + rate + `))`} {
		if _, ok := lowered(q).(physical.RankStep); !ok {
			t.Errorf("%s: not a RankStep", q)
		}
	}
	// label_replace around a ranking is the label rewrite around the RankStep.
	lr := lowered(`label_replace(topk(3, ` + rate + `), "a", "b", "c", "d")`)
	core, post := physical.Split(lr)
	if step, ok := core.(physical.RankStep); !ok || step.Op != "topk" || len(post.LabelReplace) != 1 {
		t.Errorf("label_replace around a ranking = %#v", lr)
	}
}

// What the lowering leaves to the string translator, and the name each shape is delegated under. A sort with a clause
// and a k below 1 are not routed as a ranking; an operand that is not a range or vector aggregation (or a
// label_replace around one) is the proxy's other evaluations or a query the plan does not write; a ranking inside a
// label_replace operand is translated as text by the proxy, which drops it; the spellings are the ones the proxy's
// route reads unlike the plan (a sort with a blank before its parenthesis, a ranking wrapped whole in parentheses, a
// blank or parenthesis around the operand's own call).
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesRankShapes(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		`sort by (app) (` + rate + `)`:                                      "Rank grouping",
		`sort(` + rate + `) by ()`:                                          "Rank grouping",
		`sort_desc without (app) (` + rate + `)`:                            "Rank grouping",
		`topk(3, ` + rate + ` > 1)`:                                         "Rank operand BinOp",
		`topk(3, vector(1))`:                                                "Rank operand VectorLit",
		`topk(3, label_replace(topk(2, ` + rate + `), "a", "b", "c", "d"))`: "LabelReplace operand Rank",
		`topk(3, rate({app="a"} | detected_level="error" [5m]))`:            "Rank operand RangeAgg",
		`topk(3, sum by (app) (rate({app="a"} | detected_level="e" [5m])))`: "VectorAgg LogSelect label filter detected_level",
		`topk(3, sum without () (` + rate + `))`:                            "VectorAgg without empty",
		`sort(count_over_time({app="a"} |> "<_> x" [5m]))`:                  "Rank operand pattern",
		`sort_desc(absent_over_time({app="a"} !> "<_> x" [5m]))`:            "Rank operand pattern",
		`sort (` + rate + `)`:                                               "Rank spelling",
		`sort_desc (` + rate + `)`:                                          "Rank spelling",
		`topk(3, rate ({app="a"}[5m]))`:                                     "Rank spelling",
		`topk(3, (` + rate + `))`:                                           "Rank spelling",
		`sort((` + rate + `))`:                                              "Rank spelling",
		`topk(3, sum( rate({app="a"}[5m])))`:                                "VectorAgg operand spelling",
		`topk(3, sum_over_time({app="a"} | json |unwrap x [5m]))`:           "RangeAgg unwrap spelling",
		`(topk(3, ` + rate + `))`:                                           "Rank spelling",
		`label_replace(topk(3, ` + rate + ` > 1), "a", "b", "c", "d")`:      "LabelReplace operand Rank",
		`topk(3, ` + rate + `) + 1`:                                         "BinOp operand Rank",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := PlanQuery(q, p, Profile{})
		var nl *NotLoweredError
		switch {
		case !errors.As(err, &nl) || nl.Construct != construct:
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		case len(construct) > 48:
			t.Errorf("%s: the construct name %q is longer than the shadow's label allows", q, construct)
		}
	}
}

// Plan alone (no query text to screen) leaves a ranking the route does not take to the translator: a sort with a
// clause.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesRankLayersTheRouteDoesNotTake(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		`sort by (app) (` + rate + `)`:          "Rank grouping",
		`sort_desc(` + rate + `) without (pod)`: "Rank grouping",
		`topk(2, sort by () (` + rate + `))`:    "Rank grouping",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := Plan(p, Profile{})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
}
