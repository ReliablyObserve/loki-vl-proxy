package querycorpus_test

import (
	"regexp"
	"slices"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// knownEmptyWithoutOperand is the registry case of a binary operation with an operand that has a `without ()` clause:
// Loki keeps every series of the operand, the string translator writes the operand ungrouped.
const knownEmptyWithoutOperand = "semantics/binary-operand-without-empty-collapses"

// emptyWithoutDiffers reports whether plan is a binary operation with a `without ()` operand whose lowering differs from
// the proxy's binary route, which reads the clause as the translator does: the lowering is Loki's, the route is not
// (knownEmptyWithoutOperand). The native lowering decides, not the lowering a harness is holding to the string
// translator, so a wrong lowering cannot move a row into the known divergence
// (TestEmptyWithoutOperandsKeepEverySeries holds the native lowering itself).
func emptyWithoutDiffers(q string, plan *queryplan.Plan, prof planProfile) bool {
	if !lower.EmptyWithoutOperand(plan.Root) {
		return false
	}
	native, err := lower.PlanQuery(q, plan, prof.lowerProfile())
	return err == nil && checkProxyEvaluated(q, prof, native) != ""
}

var emptyWithoutClause = regexp.MustCompile(`without\s*\(\s*\)`)

// withLabelledClause is q with a label in each `without ()` clause: a clause the string translator and the proxy's
// route read as they read any other.
func withLabelledClause(q string) string {
	return emptyWithoutClause.ReplaceAllString(q, "without (zz_none)")
}

// foldedBy returns the binary operation with the label zz_none taken out of the labels of the folds of its operands:
// the labelled clause's lowering, as the empty clause's lowering must write it.
func foldedBy(b physical.Binary) physical.Binary {
	strip := func(x physical.Op) physical.Op {
		f, ok := x.(physical.Fold)
		if !ok {
			return x
		}
		levels := slices.Clone(f.Levels)
		for i := range levels {
			if slices.Equal(levels[i].Labels, []string{"zz_none"}) {
				levels[i].Labels = nil
			}
		}
		f.Levels = levels
		return f
	}
	b.L, b.R = strip(b.L), strip(b.R)
	return b
}

// A binary operation with an operand that has a `without ()` clause lowers as Loki reads it: each series of the
// operand stays its own. A sum, avg, min or max level with the clause answers its operand's series unchanged, so the
// operand lowers as the query without that level (lower.EmptyWithoutLevelsOff), which is what the proxy's binary
// route sends (proxy.BinaryOperandsPhysical): such a row equals the route. Any other level with the clause (count,
// stddev, stdvar) lowers to the operators the same query writes with a label in the clause, the fold's labels being
// none, and still differs from the route. On every corpus and grid row the lowering answers, under every profile,
// one of the two holds. A row with another operand the lowering leaves to the translator, or a nest or an unwrap it
// names, is not lowered and is not counted.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestEmptyWithoutOperandsKeepEverySeries(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	seen := map[string]bool{}
	checked, levelsOff, notEqualRoute := 0, 0, 0
	for _, q := range append(append(planCorpus(t), binOpMoreGrid()...), binOpGrid()...) {
		if seen[q] {
			continue
		}
		seen[q] = true
		plan := buildPlan(t, q)
		if plan == nil || !lower.EmptyWithoutOperand(plan.Root) {
			continue
		}
		// The query with its sum, avg, min and max `without ()` levels taken off its operands, and whether a clause
		// is left that the lowering folds.
		off := emptyWithoutLevelsOffText(q)
		offPlan := buildPlan(t, off)
		if offPlan == nil {
			t.Errorf("%q: the query without its levels %q does not build", q, off)
			continue
		}
		folded := lower.EmptyWithoutOperand(offPlan.Root)
		ref, refQuery := offPlan, off
		if folded {
			refQuery = withLabelledClause(off)
			if ref = buildPlan(t, refQuery); ref == nil {
				continue
			}
		}
		for _, prof := range planProfiles() {
			got, err := lower.PlanQuery(q, plan, prof.lowerProfile())
			if err != nil {
				continue
			}
			b, ok := got.(physical.Binary)
			if !ok {
				t.Errorf("%s: %q lowered to %T, want a binary operation", prof.name, q, got)
				continue
			}
			want, err := lower.PlanQuery(refQuery, ref, prof.lowerProfile())
			if err != nil {
				t.Errorf("%s: %q lowers, its reference %q does not: %v", prof.name, q, refQuery, err)
				continue
			}
			checked++
			wb, ok := want.(physical.Binary)
			if !ok {
				t.Errorf("%s: %q: the reference %q lowered to %T", prof.name, q, refQuery, want)
				continue
			}
			if folded {
				wb = foldedBy(wb)
			}
			if !physical.Equal(wb, b) {
				t.Errorf("%s: %q:\n lowered   %#v\n reference %#v (%s)", prof.name, q, got, wb, refQuery)
			}
			differs := emptyWithoutDiffers(q, plan, prof)
			switch {
			case !folded && differs:
				t.Errorf("%s: %q: lowered without its levels, unlike the proxy's route", prof.name, q)
			case !folded:
				levelsOff++
			case differs:
				notEqualRoute++
			}
		}
	}
	t.Logf("%d lowered rows with a `without ()` operand checked: %d lowered without the level as the proxy's route sends them, %d folded (count, stddev, stdvar) unlike the route", checked, levelsOff, notEqualRoute)
	if checked < 1000 || levelsOff+notEqualRoute != checked || levelsOff == 0 {
		t.Fatalf("only %d lowered rows checked, %d without the level, %d folded unlike the route", checked, levelsOff, notEqualRoute)
	}
}

// emptyWithoutLevelsOffText is q, a binary operation, with the outer sum, avg, min and max levels that have a
// `without ()` clause taken off each operand, as the proxy's binary route sends them; q when it is no binary operation.
func emptyWithoutLevelsOffText(q string) string {
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		return q
	}
	b, ok := expr.(*logql.BinOpExpr)
	if !ok {
		return q
	}
	off := func(e logql.Expr) logql.Expr {
		for {
			v, ok := e.(*logql.VectorAggregation)
			if !ok || v.Grouping == nil || !v.Grouping.Without || len(v.Grouping.Labels) != 0 {
				return e
			}
			switch v.Op {
			case logql.VectorSum, logql.VectorAvg, logql.VectorMin, logql.VectorMax:
				e = v.Inner
			default:
				return e
			}
		}
	}
	c := *b
	c.Left, c.Right = off(b.Left), off(b.Right)
	return c.String()
}

// buildPlan is the resolved plan of q, nil when it does not parse or build.
func buildPlan(t *testing.T, q string) *queryplan.Plan {
	t.Helper()
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		return nil
	}
	plan, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil
	}
	queryplan.Resolve(plan)
	return plan
}
