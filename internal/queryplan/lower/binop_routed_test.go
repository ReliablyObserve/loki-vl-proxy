package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// A bool modifier, a set operator and a chain lower to the operations the proxy's binary route evaluates: the operator
// and modifier as the plan holds them, each operand the query of its own, an operand that is a binary operation its own
// physical.Binary, in Loki's precedence (the plan's tree) and associativity. They are held to the route, not to the
// translator's text for the whole query (RoutedBinary), which drops the modifier, writes the left operand of a set
// operation alone and splits a chain at its first operator. The answers are held to Loki by internal/querycorpus
// TestBinaryOperationsMatchLoki, the tree by TestBinOpLoweringMatchesLoki.
//
// conformance: operator-and, operator-or, operator-unless, operator-optypegt, operator-optypepow
func TestPlanLowersRoutedBinaries(t *testing.T) {
	a, b := `sum by (app) (rate({app="a"}[5m]))`, `sum by (app) (rate({app="b"}[5m]))`
	query := func(x physical.Op) bool { _, ok := x.(physical.VLQuery); return ok }
	scalar := func(v float64) func(physical.Op) bool {
		return func(x physical.Op) bool { s, ok := x.(physical.ScalarOp); return ok && s.V == v }
	}
	type shape struct {
		op   string
		bool bool
		l, r func(physical.Op) bool
	}
	binary := func(want shape) func(physical.Op) bool {
		return func(x physical.Op) bool {
			v, ok := x.(physical.Binary)
			return ok && v.Op == want.op && v.Bool == want.bool && want.l(v.L) && want.r(v.R)
		}
	}
	for _, tc := range []struct {
		q    string
		want shape
	}{
		{a + ` > bool 1`, shape{">", true, query, scalar(1)}},
		{`1 < bool ` + a, shape{"<", true, scalar(1), query}},
		{a + ` == bool ` + b, shape{"==", true, query, query}},
		{a + ` and ` + b, shape{"and", false, query, query}},
		{a + ` or ` + b, shape{"or", false, query, query}},
		{a + ` unless on (app) ` + b, shape{"unless", false, query, query}},
		// * before +, left associative -, right associative ^, comparisons after arithmetic, and after the
		// comparisons.
		{a + ` + ` + b + ` * 2`, shape{"+", false, query, binary(shape{"*", false, query, scalar(2)})}},
		{a + ` - ` + b + ` - 1`, shape{"-", false, binary(shape{"-", false, query, query}), scalar(1)}},
		{a + ` ^ ` + b + ` ^ 2`, shape{"^", false, query, binary(shape{"^", false, query, scalar(2)})}},
		{a + ` + ` + b + ` > bool 4`, shape{">", true, binary(shape{"+", false, query, query}), scalar(4)}},
		{a + ` > 2 and ` + b + ` > 1`, shape{"and", false, binary(shape{">", false, query, scalar(2)}), binary(shape{">", false, query, scalar(1)})}},
		{`(` + a + ` + 1) / ` + b, shape{"/", false, binary(shape{"+", false, query, scalar(1)}), query}},
	} {
		p := rangeAggPlan(tc.q)
		if p == nil {
			t.Fatalf("%s: does not parse", tc.q)
		}
		if !RoutedBinary(p.Root) {
			t.Errorf("%s: not held to the binary route", tc.q)
		}
		op, err := PlanQuery(tc.q, p, Profile{Label: func(s string) string { return s }})
		if err != nil {
			t.Errorf("%s: %v", tc.q, err)
			continue
		}
		if !binary(tc.want)(op) {
			t.Errorf("%s: lowered %#v, want %+v", tc.q, op, tc.want)
		}
	}
}

// Left to the translator: a set operator next to a number (Loki rejects it at parse) or a vector(n), a bool modifier
// next to a vector(n) or between constants, a chain of constants (the proxy folds those itself), and a chain whose
// operand the lowering does not write or that is a chain of numbers (the route folds `2 + 3` to a number).
//
// conformance: operator-and, operator-optypegt
func TestPlanDelegatesRoutedBinaryEdges(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		rate + ` and 1`:                         "BinOp set operator",
		`1 unless ` + rate:                      "BinOp set operator",
		rate + ` or vector(1)`:                  "BinOp set operator",
		rate + ` > bool vector(1)`:              "BinOp bool",
		`1 > bool 2`:                            "BinOp bool",
		`1 + 2 * 3`:                             "BinOp constant operands",
		`vector(1) + vector(2) * vector(3)`:     "BinOp constant operands",
		rate + ` * (2 + 3)`:                     "BinOp operand BinOp",
		rate + ` / (topk(2, ` + rate + `) + 1)`: "BinOp operand BinOp",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := PlanQuery(q, p, Profile{})
		if nl, ok := err.(*NotLoweredError); !ok || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
}
