package lower

import (
	"errors"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// emptyWithoutClause is how a `without ()` clause is spelled, with blanks inside it or not.
var emptyWithoutClause = regexp.MustCompile(`without\s*\(\s*\)`)

// noneLabel is the label the reference queries of these tests put in each `without ()` clause: a clause the lowering
// writes as it writes any other, whose label the fold of an empty clause does not have.
const noneLabel = "zz_none"

// withLabelledClause is q with a label in each `without ()` clause.
func withLabelledClause(q string) string {
	return emptyWithoutClause.ReplaceAllString(q, "without ("+noneLabel+")")
}

// emptyWithoutClauses counts the `without ()` clauses of the vector aggregations of a plan's binary operation.
func emptyWithoutClauses(root queryplan.Node) int {
	n, ok := root.(*queryplan.BinOp)
	if !ok {
		return 0
	}
	count := 0
	for _, x := range [2]queryplan.Node{n.L, n.R} {
		for lv, _ := x.(*queryplan.VectorAgg); lv != nil; lv, _ = lv.In.(*queryplan.VectorAgg) {
			if lv.By != nil && lv.By.Without && len(lv.By.Labels) == 0 {
				count++
			}
		}
	}
	return count
}

// withoutNoneLabel is the operators of a binary operation with noneLabel taken out of the labels of its operands' folds:
// what the lowering of the labelled reference query is when its clauses are empty.
func withoutNoneLabel(b physical.Binary) physical.Binary {
	strip := func(x physical.Op) physical.Op {
		f, ok := x.(physical.Fold)
		if !ok {
			return x
		}
		levels := slices.Clone(f.Levels)
		for i := range levels {
			if slices.Equal(levels[i].Labels, []string{noneLabel}) {
				levels[i].Labels = nil
			}
		}
		f.Levels = levels
		return f
	}
	b.L, b.R = strip(b.L), strip(b.R)
	return b
}

// emptyWithoutLikeLabelled reports whether the lowering op of q, a binary operation with `without ()` operands, is the
// lowering of the same query with a label in each clause, the label taken out of the folds. A query whose clauses
// cannot all be told by the spelling (a value that holds `without ()`), or whose reference the lowering leaves to the
// translator, is not compared (compared is false).
func emptyWithoutLikeLabelled(t testing.TB, q string, p *queryplan.Plan, op physical.Op, prof rangeProfile) (same, compared bool) {
	t.Helper()
	ref := withLabelledClause(q)
	if len(emptyWithoutClause.FindAllString(q, -1)) != emptyWithoutClauses(p.Root) {
		return false, false
	}
	rp := rangeAggPlan(ref)
	if rp == nil {
		return false, false
	}
	want, err := PlanQuery(ref, rp, prof.lowerProfile())
	if err != nil {
		t.Fatalf("%s: %s lowers with an empty clause, not with a labelled one: %v", prof.name, q, err)
	}
	wb, ok := want.(physical.Binary)
	gb, gok := op.(physical.Binary)
	if !ok || !gok {
		return false, true
	}
	return physical.Equal(withoutNoneLabel(wb), gb), true
}

// lowerBinary lowers q, a binary operation, and returns its operators.
func lowerBinary(t *testing.T, q string) physical.Binary {
	t.Helper()
	op, err := PlanQuery(q, rangeAggPlan(q), Profile{})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	b, ok := op.(physical.Binary)
	if !ok {
		t.Fatalf("%s: lowered to %T, want a binary operation", q, op)
	}
	return b
}

// A `without ()` clause on an operand removes no label (Loki keeps every series of the operand), so the operand is the
// fold of its per-series rows by no label: a physical.Fold whose level is the aggregation, without, and no labels,
// over the very rows the same operand writes under a `without (pod)` clause. Every aggregation, the clause in the
// head and tail spellings, either side, with a vector(n) beside it, and a range function that counts, rates and sums
// bytes.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestPlanLowersEmptyWithoutOperands(t *testing.T) {
	spellings := []func(agg, clause, in string) string{
		func(agg, clause, in string) string { return agg + " without (" + clause + ") (" + in + ")" },
		func(agg, clause, in string) string { return agg + "(" + in + ") without (" + clause + ")" },
		func(agg, clause, in string) string { return agg + " without(" + clause + ")(" + in + ")" },
	}
	other := `sum by (app) (count_over_time({app="b"}[5m]))`
	checked := 0
	for _, agg := range []string{"sum", "count", "min", "max", "avg"} {
		for _, fn := range []string{`rate({app="a"}[5m])`, `count_over_time({app="a"}[5m])`, `bytes_over_time({app="a"} |= "x"[5m])`, `bytes_rate({app="a"}[5m])`, `absent_over_time({app="a"}[5m])`} {
			for _, spelling := range spellings {
				empty, labelled := spelling(agg, "", fn), spelling(agg, "pod", fn)
				for _, shape := range []struct{ q, ref string }{
					{empty + " / 2", labelled + " / 2"},
					{"100 < " + empty, "100 < " + labelled},
					{other + " / on(app) group_left(env) " + empty, other + " / on(app) group_left(env) " + labelled},
					{empty + " - ignoring(pod) " + empty, labelled + " - ignoring(pod) " + labelled},
					{empty + " + on() vector(0)", labelled + " + on() vector(0)"},
					{"vector(1) > " + empty, "vector(1) > " + labelled},
				} {
					got, want := lowerBinary(t, shape.q), lowerBinary(t, shape.ref)
					checked++
					if got.Op != want.Op || !reflect.DeepEqual(got.Match, want.Match) || got.Tail != want.Tail {
						t.Errorf("%s: operator or matching differs from the labelled clause's: %#v", shape.q, got)
					}
					folds := 0
					for i, pair := range [][2]physical.Op{{got.L, want.L}, {got.R, want.R}} {
						gf, isFold := pair[0].(physical.Fold)
						wf, wantFold := pair[1].(physical.Fold)
						if isFold != wantFold {
							t.Errorf("%s: operand %d is %T, the labelled clause's %T", shape.q, i, pair[0], pair[1])
							continue
						}
						if !isFold {
							if !reflect.DeepEqual(pair[0], pair[1]) {
								t.Errorf("%s: operand %d = %#v, want %#v", shape.q, i, pair[0], pair[1])
							}
							continue
						}
						folds++
						// nil, not an empty list: physical.Equal compares the labels as they are.
						if len(gf.Levels) != 1 || gf.Levels[0].Op != agg || !gf.Levels[0].Without || gf.Levels[0].Labels != nil {
							t.Errorf("%s: operand %d folds %#v, want %s without no label", shape.q, i, gf.Levels, agg)
						}
						if !reflect.DeepEqual(gf.In, wf.In) {
							t.Errorf("%s: operand %d rows differ from the labelled clause's:\n got %#v\nwant %#v", shape.q, i, gf.In, wf.In)
						}
						// The text of a fold of no label is its rows' query: the marker needs a label.
						if physical.Encode(gf) != physical.Encode(gf.In) {
							t.Errorf("%s: operand %d encodes %q, want its query %q", shape.q, i, physical.Encode(gf), physical.Encode(gf.In))
						}
						if core, post := physical.Split(gf); !post.Folds() || len(post.Without.Labels) != 0 || post.Without.Op != agg || core != gf.In {
							t.Errorf("%s: operand %d splits to %#v, %#v", shape.q, i, core, post)
						}
					}
					if folds == 0 {
						t.Errorf("%s: no operand is folded", shape.q)
					}
				}
			}
		}
	}
	if checked < 400 {
		t.Fatalf("only %d queries checked", checked)
	}
}

// The clause is lowered as Loki reads it under any profile: stream fields, a label function, the line kept.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestPlanLowersEmptyWithoutOperandsUnderEveryProfile(t *testing.T) {
	qs := []string{
		`sum without () (rate({app="a"}[5m])) / 2`,
		`max(rate({app="a"} |= "x" [5m])) without () > on(app) group_left sum by (app) (rate({app="a"}[5m]))`,
		`count without () (count_over_time({service_name="a"}[5m])) + on() vector(0)`,
		`avg without () (bytes_rate({app="a", k8s_pod="p"}[5m])) * 2`,
	}
	for _, q := range qs {
		p := rangeAggPlan(q)
		for _, prof := range rangeProfiles() {
			op, err := PlanQuery(q, p, prof.lowerProfile())
			if err != nil {
				continue
			}
			if same, compared := emptyWithoutLikeLabelled(t, q, p, op, prof); !compared || !same {
				t.Errorf("%s: %s: compared %v, like the labelled clause %v", prof.name, q, compared, same)
			}
		}
	}
}

// An operand that the fold of one level cannot carry stays with the translator under its own name: a `without ()` at
// a level of a nest (the levels above it are not in the operators, and `count without ()` is 1 for every series), and
// over a range aggregation of an unwrapped label, whose lowering reads the clause the translator's way. An unwrap
// that absent_over_time reads nothing from is no unwrap.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestPlanDelegatesEmptyWithoutNestsAndUnwraps(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		`count without () (sum without () (` + rate + `)) / 2`:                          "BinOp operand without empty nest",
		`sum without () (sum without (pod) (` + rate + `)) / 2`:                         "BinOp operand without empty nest",
		`sum without () (sum by (app) (` + rate + `)) / 2`:                              "BinOp operand without empty nest",
		`sum by (app) (sum without () (` + rate + `)) / 2`:                              "BinOp operand without empty nest",
		`2 * avg(sum without () (` + rate + `))`:                                        "BinOp operand without empty nest",
		`sum(sum by (app, pod) (sum without () (` + rate + `))) / 2`:                    "BinOp operand without empty nest",
		`sum without () (sum_over_time({app="a"} | unwrap n [5m])) / 2`:                 "BinOp operand without empty unwrap",
		`vector(1) + max without () (rate({app="a"} | logfmt | unwrap n [5m]))`:         "BinOp operand without empty unwrap",
		`sum without () (avg_over_time({app="a"} | unwrap n [5m])) / sum(` + rate + `)`: "BinOp operand without empty unwrap",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := PlanQuery(q, p, Profile{})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
	// absent_over_time reads no value, so the clause lowers over it as over any range aggregation.
	for _, q := range []string{
		`sum without () (absent_over_time({app="a"} | unwrap n [5m])) / 2`,
	} {
		if _, err := PlanQuery(q, rangeAggPlan(q), Profile{}); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// Only a binary operation's operand lowers the clause: the aggregation on its own, under a ranking, stddev or
// label_replace, the route rule and a nest still read it as the handlers do, so they keep their delegations.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestEmptyWithoutIsDelegatedOutsideBinaryOperands(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		`sum without () (` + rate + `)`:                                        "VectorAgg without empty",
		`sum(` + rate + `) without ()`:                                         "VectorAgg without empty",
		`topk(3, sum without () (` + rate + `))`:                               "VectorAgg without empty",
		`stddev(sum without () (` + rate + `))`:                                "VectorAgg without empty",
		`sum without () (sum by (app) (` + rate + `))`:                         "VectorAgg without empty",
		`sum without () (sum_over_time({app="a"} | unwrap n [5m]))`:            "VectorAgg without empty",
		`label_replace(sum without () (` + rate + `), "a", "b", "c", "d")`:     "LabelReplace operand VectorAgg",
		`label_replace(sum without () (` + rate + `) / 2, "a", "b", "c", "d")`: "LabelReplace operand BinOp",
	} {
		p := rangeAggPlan(q)
		_, err := PlanQuery(q, p, Profile{})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
}

// The route rule decides the handlers' route, which reads `without ()` as no clause: it does not route the aggregation
// that the binary operand lowers.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestRouteLeavesEmptyWithoutToTheHandlers(t *testing.T) {
	for _, q := range []string{
		`sum without () (rate({app="a"}[5m]))`,
		`max(count_over_time({app="a"}[5m])) without ()`,
	} {
		for _, req := range []RouteRequest{{Instant: true}, {Step: 5 * 60e9, Span: 3600e9}, {Step: 60e9, Span: 3600e9}} {
			_, err := Route(rangeAggPlan(q), Profile{}, req)
			var nl *NotLoweredError
			if !errors.As(err, &nl) || nl.Construct != "VectorAgg without empty" {
				t.Errorf("%s %+v: got %v, want not lowered: VectorAgg without empty", q, req, err)
			}
		}
	}
}

// Lowering does not touch the plan: the clause of each level is as the client wrote it afterwards.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestLoweringEmptyWithoutLeavesThePlanAlone(t *testing.T) {
	q := `count without () (rate({app="a"}[5m])) / sum without () (rate({app="b"}[5m]))`
	p := rangeAggPlan(q)
	clauses := func() string {
		var out []string
		var walk func(n queryplan.Node)
		walk = func(n queryplan.Node) {
			switch v := n.(type) {
			case *queryplan.BinOp:
				walk(v.L)
				walk(v.R)
			case *queryplan.VectorAgg:
				out = append(out, string(v.Op)+":"+v.By.String())
				walk(v.In)
			}
		}
		walk(p.Root)
		return strings.Join(out, ",")
	}
	before := clauses()
	if _, err := PlanQuery(q, p, Profile{}); err != nil {
		t.Fatal(err)
	}
	if after := clauses(); after != before {
		t.Errorf("the plan's clauses changed: %s, was %s", after, before)
	}
}

// EmptyWithoutOperand tells the binary operations with a `without ()` level in an operand apart: a level with a
// label, a `by ()` and a clause on a range aggregation are not it, and nothing but a binary operation at the root is.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestEmptyWithoutOperand(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, want := range map[string]bool{
		`sum without () (` + rate + `) / 2`:                                    true,
		`sum(` + rate + `) without ( ) / 2`:                                    true,
		`2 * max by (app) (sum without () (` + rate + `))`:                     true,
		`sum without (pod) (` + rate + `) / sum without () (` + rate + `)`:     true,
		`sum without (pod) (` + rate + `) / 2`:                                 false,
		`sum by () (` + rate + `) / 2`:                                         false,
		`sum(` + rate + `) / 2`:                                                false,
		rate + ` / 2`:                                                          false,
		`sum without () (` + rate + `)`:                                        false,
		`label_replace(sum without () (` + rate + `) / 2, "a", "b", "c", "d")`: false,
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		if got := EmptyWithoutOperand(p.Root); got != want {
			t.Errorf("EmptyWithoutOperand(%s) = %v, want %v", q, got, want)
		}
	}
}
