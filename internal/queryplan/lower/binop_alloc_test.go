package lower

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// A binary operation the lowering writes allocates no more than the string translator does for the same text, with
// and without the tenant's stream labels (Parse, Build, Resolve, the text screen and Plan against the translation),
// on the shapes below: numbers and two operands of the range and vector aggregations, with on and ignoring.
//
// Measured and not held, because the cost is the operand lowering's (step 15 and 14) and twice it in a binary, or
// the group side's text: group_left and group_right matching (2 to 7 more), ignoring over an unwrapped value (2
// more), a negative or regexp line filter in both operands (2 to 3 more), and an operand with a parser and a label
// filter (up to 8 more), with or without the binary around it. They are a follow-up of the operand lowerings.
//
// conformance: architecture-syntax-package-layering
func TestBinOpLoweringAllocatesNoMoreThanTheTranslator(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	checked := 0
	grid := binOpAllocGrid()
	for _, label := range []func(string) string{nil, benchStreamLabels} {
		for _, q := range grid {
			p := rangeAggPlan(q)
			if p == nil {
				continue
			}
			if _, err := PlanQuery(q, p, Profile{Label: label}); err != nil {
				continue
			}
			cq := logql.CanonicalizeLogRanges(q)
			tr := testing.AllocsPerRun(5, func() { _, _ = translator.TranslateLogQLWithLabels(cq, label) })
			lo := testing.AllocsPerRun(5, func() {
				expr, _ := logql.Parse(cq)
				pl, _ := queryplan.Build(expr, queryplan.Inputs{})
				queryplan.Resolve(pl)
				_, _ = PlanQuery(q, pl, Profile{Label: label})
			})
			checked++
			if lo > tr {
				t.Errorf("label=%v: %s: lowered %.0f allocs, translator %.0f", label != nil, q, lo, tr)
			}
		}
	}
	t.Logf("%d shapes checked (%d queries, two label modes)", checked, len(grid))
	if checked < 300 {
		t.Fatalf("only %d lowered binary operations checked", checked)
	}
}

func binOpAllocGrid() []string {
	var qs []string
	ranges := []string{`rate({app="a"}[5m])`, `count_over_time({app="a"} |= "x" [1m])`, `bytes_over_time({app=~"a.*"}[5m])`,
		`bytes_rate({app="a", env!="p"}[5m] offset 1h)`, `absent_over_time({app="a"}[5m])`, `rate({app="a"} | unwrap amount [150s])`,
		`sum_over_time({app="a"} | unwrap amount [5m])`}
	for _, in := range ranges {
		for _, op := range []string{"/", "*", "+", ">", "=="} {
			for _, f := range []string{`%[1]s %[2]s 2`, `2 %[2]s %[1]s`, `sum(%[1]s) %[2]s 2`, `sum by (app) (%[1]s) %[2]s %[1]s`,
				`count by (app, env) (%[1]s) %[2]s on(app) sum by (app) (%[1]s)`, `max(%[1]s) by (env) %[2]s on() max(%[1]s) by (env)`, `%[1]s %[2]s on (app, env) %[1]s`} {
				qs = append(qs, fmt.Sprintf(f, in, op))
			}
		}
	}
	return qs
}

// The helpers the binary screen and the lowering share allocate as little as they can: the modifier text of a vector
// matching is built on the caller's buffer and compared without a copy, the Matching and its text are the lowering's
// two allocations, and stripping the modifiers from a query is one buffer for the text and one for the modifiers.
//
// conformance: architecture-syntax-package-layering
func TestBinOpMatchingHelpersAllocate(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	m := &queryplan.Matching{Card: "on", MatchLabels: []string{"app", "env"}, GroupSide: "group_left", Include: []string{"pod"}}
	const want = "@@@on:app, env@@@group_left:pod"
	var buf [128]byte
	if got := string(matchingTail(buf[:0], m)); got != want {
		t.Fatalf("matchingTail = %q, want %q", got, want)
	}
	if n := testing.AllocsPerRun(20, func() {
		var b [128]byte
		if string(matchingTail(b[:0], m)) != want {
			t.Fatal("tail")
		}
	}); n != 0 {
		t.Errorf("comparing the modifier text allocates %.0f", n)
	}
	var pm *physical.Matching
	var tail string
	if n := testing.AllocsPerRun(20, func() { pm, tail = binMatching(m) }); n > 2 || tail != want || pm == nil || !pm.MatchOn || pm.GroupSide != "group_left" {
		t.Errorf("binMatching allocates %.0f (want at most 2), tail %q", n, tail)
	}
	q := `sum(rate({app="a"}[5m])) / on(app, env) group_left(pod) sum(rate({app="b"}[5m]))`
	var stripped, mods string
	if n := testing.AllocsPerRun(20, func() { stripped, mods = stripMatching(q) }); n > 2 || mods != want || stripped != `sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))` {
		t.Errorf("stripMatching allocates %.0f (want at most 2): %q, %q", n, stripped, mods)
	}
	if n := testing.AllocsPerRun(20, func() { stripped, mods = stripMatching(`sum(rate({app="a"}[5m])) / 2`) }); n != 0 {
		t.Errorf("stripMatching without a modifier allocates %.0f", n)
	}
}
