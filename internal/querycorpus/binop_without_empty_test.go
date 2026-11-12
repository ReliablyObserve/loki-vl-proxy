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
// operand stays its own. On every corpus and grid row the lowering answers, under every profile, the operators are
// the ones the same query writes with a label in the clause, the fold's labels being none. A row with another
// operand the lowering leaves to the translator, or a nest or an unwrap it names, is not lowered and is not counted.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestEmptyWithoutOperandsKeepEverySeries(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	seen := map[string]bool{}
	checked, notEqualRoute := 0, 0
	for _, q := range append(append(planCorpus(t), binOpMoreGrid()...), binOpGrid()...) {
		if seen[q] {
			continue
		}
		seen[q] = true
		plan := buildPlan(t, q)
		if plan == nil || !lower.EmptyWithoutOperand(plan.Root) {
			continue
		}
		ref := buildPlan(t, withLabelledClause(q))
		if ref == nil {
			continue
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
			want, err := lower.PlanQuery(withLabelledClause(q), ref, prof.lowerProfile())
			if err != nil {
				t.Errorf("%s: %q lowers with an empty clause, not with a labelled one: %v", prof.name, q, err)
				continue
			}
			checked++
			if !physical.Equal(foldedBy(want.(physical.Binary)), b) {
				t.Errorf("%s: %q:\n lowered %#v\n labelled %#v", prof.name, q, got, want)
			}
			if emptyWithoutDiffers(q, plan, prof) {
				notEqualRoute++
			}
		}
	}
	t.Logf("%d lowered rows with a `without ()` operand checked against the labelled clause; %d unlike the proxy's route", checked, notEqualRoute)
	if checked < 1000 || notEqualRoute != checked {
		t.Fatalf("only %d lowered rows checked, %d unlike the route", checked, notEqualRoute)
	}
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
