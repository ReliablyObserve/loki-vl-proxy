package lower

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// A stddev or stdvar lowers to a SeriesAgg over the operand's own operators: the proxy translates the operand alone
// and folds the series of each group of its answer. The clause is the aggregation's own, in either position, the
// layers sum, avg, min, max and count above a ranking are SeriesAgg levels over the RankStep, and a level below the
// first one the route evaluates is lowered to the chain Loki evaluates (the shadow records those shapes as known
// divergences, the proxy sends the translator's text for them). The SeriesAgg encodes as its operand's request
// (internal/querycorpus series_agg_grid_test.go holds it to the proxy's route on many more shapes).
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersStddevToSeriesAgg(t *testing.T) {
	operandOf := func(q string) physical.Op {
		t.Helper()
		op, err := PlanQuery(q, rangeAggPlan(q), Profile{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return op
	}
	rate := `rate({app="a"}[5m])`
	sum := `sum by (app) (` + rate + `)`
	unwrap := `sum_over_time({app="a"} | unwrap amount [5m])`
	app := &physical.Grouping{Labels: []string{"app"}}
	without := &physical.Grouping{Labels: []string{"pod"}, Without: true}
	lr := `label_replace(` + sum + `, "a", "$1", "app", "(.*)")`
	for _, c := range []struct {
		query string
		want  physical.Op
	}{
		{`stddev(` + rate + `)`, physical.SeriesAgg{Op: "stddev", In: operandOf(rate)}},
		{`stdvar(` + rate + `)`, physical.SeriesAgg{Op: "stdvar", In: operandOf(rate)}},
		{`stddev by (app) (` + rate + `)`, physical.SeriesAgg{Op: "stddev", By: app, In: operandOf(rate)}},
		{`stddev(` + rate + `) by (app)`, physical.SeriesAgg{Op: "stddev", By: app, In: operandOf(rate)}},
		{`stdvar without (pod) (` + rate + `)`, physical.SeriesAgg{Op: "stdvar", By: without, In: operandOf(rate)}},
		{`stddev by () (` + rate + `)`, physical.SeriesAgg{Op: "stddev", By: &physical.Grouping{}, In: operandOf(rate)}},
		{`stddev(` + sum + `)`, physical.SeriesAgg{Op: "stddev", In: operandOf(sum)}},
		{`stdvar by (app) (` + unwrap + `)`, physical.SeriesAgg{Op: "stdvar", By: app, In: operandOf(unwrap)}},
		{`stddev(` + lr + `)`, physical.SeriesAgg{Op: "stddev", In: operandOf(lr)}},
		{`(stddev by (app) (` + rate + `))`, physical.SeriesAgg{Op: "stddev", By: app, In: operandOf(rate)}},
		// the layers above a ranking
		{`sum(topk(3, ` + rate + `))`, physical.SeriesAgg{Op: "sum", In: physical.RankStep{Op: "topk", K: 3, In: operandOf(rate)}}},
		{`max by (app) (sort_desc(` + sum + `))`, physical.SeriesAgg{Op: "max", By: app, In: physical.RankStep{Op: "sort_desc", In: operandOf(sum)}}},
		{`avg(sum(bottomk(2, ` + rate + `)))`, physical.SeriesAgg{Op: "avg", In: physical.SeriesAgg{Op: "sum", In: physical.RankStep{Op: "bottomk", K: 2, In: operandOf(rate)}}}},
		// a level below the first one the route evaluates
		{`sum(stddev by (app) (` + rate + `))`, physical.SeriesAgg{Op: "sum", In: physical.SeriesAgg{Op: "stddev", By: app, In: operandOf(rate)}}},
		{`stddev(stdvar by (app) (` + rate + `))`, physical.SeriesAgg{Op: "stddev", In: physical.SeriesAgg{Op: "stdvar", By: app, In: operandOf(rate)}}},
		{`topk(2, stddev by (app) (` + rate + `))`, physical.RankStep{Op: "topk", K: 2, In: physical.SeriesAgg{Op: "stddev", By: app, In: operandOf(rate)}}},
		{`stddev(topk(3, ` + rate + `))`, physical.SeriesAgg{Op: "stddev", In: physical.RankStep{Op: "topk", K: 3, In: operandOf(rate)}}},
		{`bottomk(1, sum(stddev(topk(2, ` + rate + `))))`, physical.RankStep{Op: "bottomk", K: 1, In: physical.SeriesAgg{Op: "sum", In: physical.SeriesAgg{Op: "stddev", In: physical.RankStep{Op: "topk", K: 2, In: operandOf(rate)}}}}},
		{`sort(stdvar by (app) (sort_desc(` + rate + `)))`, physical.RankStep{Op: "sort", In: physical.SeriesAgg{Op: "stdvar", By: app, In: physical.RankStep{Op: "sort_desc", In: operandOf(rate)}}}},
	} {
		p := rangeAggPlan(c.query)
		if p == nil {
			t.Fatalf("%s: does not parse", c.query)
		}
		got, err := PlanQuery(c.query, p, Profile{})
		if err != nil {
			t.Errorf("%s: %v", c.query, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got %#v\nwant %#v", c.query, got, c.want)
		}
		if _, operand := physical.SplitEval(got); physical.Encode(operand) != physical.Encode(got) {
			t.Errorf("%s: Encode = %q, want the operand's %q", c.query, physical.Encode(got), physical.Encode(operand))
		}
	}
	// label_replace around a stddev is the label rewrite around the SeriesAgg.
	q := `label_replace(stddev by (app) (` + rate + `), "a", "b", "c", "d")`
	core, post := physical.Split(operandOf(q))
	if step, ok := core.(physical.SeriesAgg); !ok || step.Op != "stddev" || len(post.LabelReplace) != 1 {
		t.Errorf("label_replace around a stddev = %#v", core)
	}
}

// What the lowering leaves to the string translator for stddev and stdvar, and the name each shape is delegated
// under: an operand the route evaluates in another way (a binary operation, vector(), a number), one the operand's
// own lowering leaves out, a clause by a label of a query with a parser (the route rewrites the operand as the
// series-level form of max by, in the text, quoted text included), and the spellings the route reads unlike the plan.
// A stddev under a binary operation is a sub-request with a route of its own and stays with the translator.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesStddevShapes(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		`stddev by (app) (rate({app="a"} | json [5m]))`:                                      "",
		`stdvar by (a) (sum by (a) (count_over_time({app="a"} | logfmt [5m])))`:              "",
		`stddev by (route) (topk(2, count_over_time({app="a"} | json [5m])))`:                "VectorAgg stddev parsed label",
		`stddev by (route) (last_over_time({app="a"} | json | unwrap x [5m]))`:               "VectorAgg stddev parsed label operand",
		`stddev by (route) (label_replace(rate({app="a"} | json [5m]), "a", "b", "c", "d"))`: "VectorAgg stddev parsed label operand",
		`stddev by (route) (min without (pod) (rate({app="a"} | json [5m])))`:                "",
		`stddev by (_Time) (rate({app="a"} | json [5m]))`:                                    "VectorAgg stddev parsed label reserved name",
		`stdvar by (route, _STREAM_ID) (rate({app="a"} | json [5m]))`:                        "VectorAgg stddev parsed label reserved name",
		`stddev(` + rate + `) by (app) `:                                                     "",
		`stddev without (app) (rate({app="a"} | json [5m]))`:                                 "",
		`stddev(rate({app="a"} | json [5m]))`:                                                "",
		`stddev by (app) (rate({app="a"} |= "| json" [5m]))`:                                 "VectorAgg stddev parsed label",
		`stddev(` + rate + ` > 1)`:                                                           "SeriesAgg operand BinOp",
		`stddev(vector(1))`:                                                                  "SeriesAgg operand VectorLit",
		`stddev(sum by (app) (rate({app="a"} | detected_level="e" [5m])))`:                   "VectorAgg LogSelect label filter detected_level",
		`stdvar(sum without () (` + rate + `))`:                                              "VectorAgg without empty",
		`stddev(sum( rate({app="a"}[5m])))`:                                                  "VectorAgg operand spelling",
		`stddev (rate ({app="a"}[5m]))`:                                                      "SeriesAgg spelling",
		`stddev((` + rate + `))`:                                                             "SeriesAgg spelling",
		`topk(2, (stddev by (app) (` + rate + `)))`:                                          "SeriesAgg spelling",
		`stddev((stdvar(` + rate + `)))`:                                                     "SeriesAgg spelling",
		`topk(2, (sum(topk(3, ` + rate + `))))`:                                              "",
		`stddev(` + rate + `) / 2`:                                                           "BinOp operand VectorAgg",
		`sum(stddev(` + rate + `) / 2)`:                                                      "VectorAgg operand",
		`topk(3, label_replace(stddev(` + rate + `), "a", "b", "c", "d"))`:                   "LabelReplace operand VectorAgg",
		`stddev(label_replace(topk(2, ` + rate + `), "a", "b", "c", "d"))`:                   "LabelReplace operand Rank",
		`stddev(label_replace(stdvar(` + rate + `), "a", "b", "c", "d"))`:                    "LabelReplace operand VectorAgg",
		`sort by (app) (stddev(` + rate + `))`:                                               "Rank grouping",
		`stddev(sort_desc by (app) (` + rate + `))`:                                          "Rank grouping",
		`stddev(sum_over_time({app="a"} | json |unwrap x [5m]))`:                             "RangeAgg unwrap spelling",
		`stddev(sum_over_time({app="a"} | unwrap x | unwrap y [5m]))`:                        "SeriesAgg operand RangeAgg",
		`stddev(absent_over_time({app="a"}[5m]))`:                                            "",
		`stddev by (app) (sum(count_over_time({app="a"} | json | unwrap x [5m])))`:           "VectorAgg nested unwrap",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := PlanQuery(q, p, Profile{})
		var nl *NotLoweredError
		switch {
		case construct == "" && err != nil:
			t.Errorf("%s: got %v, want lowered", q, err)
		case construct == "":
		case !errors.As(err, &nl) || nl.Construct != construct:
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		case len(construct) > 48:
			t.Errorf("%s: the construct name %q is longer than the shadow's label allows", q, construct)
		}
	}
	// Plan alone reads the plan's parsers: a clause by labels over an operand with a parser is the route's rewrite
	// without the text too.
	q := `stdvar by (route) (sum by (route) (count_over_time({app="a"} | logfmt [5m])))`
	alone, err := Plan(rangeAggPlan(q), Profile{})
	withText, err2 := PlanQuery(q, rangeAggPlan(q), Profile{})
	if err != nil || err2 != nil || !reflect.DeepEqual(alone, withText) {
		t.Errorf("%s: Plan %#v (%v), PlanQuery %#v (%v)", q, alone, err, withText, err2)
	}
}

// A stddev or stdvar by labels over a query with a parser lowers to the proxy's rewrite of its operand: the
// series-level max by form cut after its first stats pipes, the alias of an unread value removed, a rate summed per
// grouping (postAggOperandText; internal/querycorpus holds it to the route on the corpus and the grid).
//
// conformance: architecture-syntax-package-layering
func TestPlanRebuildsParsedLabelOperand(t *testing.T) {
	route := &physical.Grouping{Labels: []string{"route"}}
	for q, want := range map[string]string{
		`stddev by (route) (count_over_time({app="a"} | logfmt [5m]))`:                     `app:="a" | unpack_logfmt | stats by (route, _stream) count()`,
		`stddev(count_over_time({app="a"} | logfmt [5m])) by (route)`:                      `app:="a" | unpack_logfmt | stats by (route, _stream) count()`,
		`stdvar by (route) (rate({app="a"} | logfmt [5m]))`:                                `app:="a" | unpack_logfmt | stats by (route, _stream) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (route, _stream) sum(__lvp_rate)`,
		`stddev by (route) (bytes_rate({app="a"} | json [5m]))`:                            `app:="a" | unpack_json | stats by (route, _stream) sum_len(_msg) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (route, _stream) sum(__lvp_rate)`,
		`stddev by (route) (count_over_time({app="a"} | regexp "(?P<route>\\w+)" [5m]))`:   `app:="a" | extract_regexp "(?P<route>\\w+)" | stats by (route, _stream) count()`,
		`stddev by (route) (count_over_time({app="a"} | pattern "<route> <_>" [5m]))`:      `app:="a" | extract "<route> <_>" | stats by (route, _stream) count()`,
		`stddev by (route) (sum by (route, pod) (count_over_time({app="a"} | json [5m])))`: `app:="a" | unpack_json | stats by (route, pod) count()`,
	} {
		got, err := PlanQuery(q, rangeAggPlan(q), Profile{})
		if err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		agg, ok := got.(physical.SeriesAgg)
		if !ok || !reflect.DeepEqual(agg.By, route) {
			t.Errorf("%s: got %#v, want a SeriesAgg by route", q, got)
			continue
		}
		if text := physical.Encode(agg.In); text != want {
			t.Errorf("%s:\n got %s\nwant %s", q, text, want)
		}
	}
}

// routeOperandCut declines where the route does not cut the text or falls back to the operand alone (fewer than two
// stats pipes, a math pipe with no grouping to sum by) and where the route's scan reads a pipe unlike the typed one
// (a grouping field with a parenthesis, a math pipe whose expression holds a pipe).
//
// conformance: architecture-syntax-package-layering
func TestRouteOperandCutDeclines(t *testing.T) {
	stats := func(by []string, alias string) logsql.PipeStats {
		keys := make([]logsql.GroupKey, len(by))
		for i, f := range by {
			keys[i] = logsql.GroupKey{Field: f}
		}
		return logsql.PipeStats{By: keys, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: alias}}}
	}
	rate := logsql.PipeMath{Expr: "__lvp_inner/300", Alias: "__lvp_rate"}
	for name, pipes := range map[string][]logsql.Pipe{
		"one stats pipe":         {stats([]string{"route", "_stream"}, "")},
		"math with no grouping":  {stats(nil, "__lvp_inner"), rate, stats(nil, "")},
		"parenthesis in a field": {stats([]string{"a)b", "_stream"}, "__lvp_inner"), rate, stats([]string{"a)b"}, "")},
		"pipe in a math pipe":    {stats([]string{"route"}, "__lvp_inner"), logsql.PipeMath{Expr: "a|b", Alias: "__lvp_rate"}, stats([]string{"route"}, "")},
	} {
		if _, ok := routeOperandCut(pipes); ok {
			t.Errorf("%s: cut, want declined", name)
		}
	}
	got, ok := routeOperandCut([]logsql.Pipe{stats([]string{"route", "_stream"}, "__lvp_inner"), rate, stats([]string{"route"}, "")})
	if !ok || len(got) != 3 || got[2].String() != "| stats by (route, _stream) sum(__lvp_rate)" {
		t.Errorf("a rate: got %v %v", got, ok)
	}
}
