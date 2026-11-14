package queryplan

import (
	"math"
	"strconv"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Rejection is Loki's rejection of a query by one of the tree checks: a parse error without a position, whose
// detail is Detail (Loki writes it as "parse error : " + Detail, with status 400; internal/lokierr.Parse(0, 0,
// Detail) is that answer).
type Rejection struct {
	Detail string
}

func (r *Rejection) Error() string { return "parse error : " + r.Detail }

func reject(detail string) *Rejection { return &Rejection{Detail: detail} }

// Validate answers Loki's rejection of a query the parser and its semantic pass accept, read from the typed tree:
// the checks Loki makes once it has the whole tree and that need no position in the query, and one syntax error
// the tree shows (a log query as the operand of label_replace). It is nil when Loki accepts the query.
//
// Loki checks each node before its operands, the left operand first (the order recorded with the Loki oracle in
// testdata/validate_loki_answers.jsonl), so the first rejection in that order is the one reported:
//
//   - a logical/set operation (and, or, unless) with an operand that is a number once Loki folds it (`1 + 2`
//     folds to 3, a comparison of two numbers to 1 or 0): `unexpected literal for <left|right> leg of
//     logical/set binary operation (<op>): <value>`;
//   - sort or sort_desc with labels to group by: `sort and sort_desc doesn't allow grouping by `;
//   - quantile_over_time without its quantile: `parameter required for operation quantile_over_time`;
//   - label_replace over a log query: Loki's grammar stops at the comma after it. Loki names the position of that
//     comma, which the tree does not hold, so the answer is Loki's syntax error without a position.
//
// Validate reads only the tree; it allocates nothing for a query Loki accepts.
func Validate(expr logql.Expr) *Rejection {
	switch e := expr.(type) {
	case *logql.BinOpExpr:
		if isSetOperation(e.Op) {
			if v, ok := foldLiteral(e.Left); ok {
				return setOperationLiteral("left", e.Op, v)
			}
			if v, ok := foldLiteral(e.Right); ok {
				return setOperationLiteral("right", e.Op, v)
			}
		}
		if err := Validate(e.Left); err != nil {
			return err
		}
		return Validate(e.Right)
	case *logql.VectorAggregation:
		if (e.Op == logql.VectorSort || e.Op == logql.VectorSortDesc) && e.Grouping != nil && len(e.Grouping.Labels) > 0 {
			return reject("sort and sort_desc doesn't allow grouping by ")
		}
		return Validate(e.Inner)
	case *logql.RangeAggregation:
		if e.Op == logql.RangeQuantileOverTime && !e.HasParam {
			return reject("parameter required for operation " + string(e.Op))
		}
		return Validate(e.Inner)
	case *logql.OpaqueMetricExpr:
		if e.Func == "label_replace" {
			if _, ok := e.Inner.(*logql.LogQuery); ok {
				return reject("syntax error: unexpected ,")
			}
		}
		if e.Inner != nil {
			return Validate(e.Inner)
		}
	}
	return nil
}

func isSetOperation(op string) bool { return op == "and" || op == "or" || op == "unless" }

// setOperationLiteral is Loki's rejection of a number as an operand of a logical/set operation; the value is
// written as Go's %f writes it (6 decimals, NaN, +Inf, -Inf).
func setOperationLiteral(leg, op string, value float64) *Rejection {
	var b []byte
	switch {
	case math.IsNaN(value):
		b = []byte("NaN")
	case math.IsInf(value, 1):
		b = []byte("+Inf")
	case math.IsInf(value, -1):
		b = []byte("-Inf")
	default:
		b = strconv.AppendFloat(nil, value, 'f', 6, 64)
	}
	return reject("unexpected literal for " + leg + " leg of logical/set binary operation (" + op + "): " + string(b))
}

// foldLiteral is the number Loki makes of an expression of numbers: a literal, or an arithmetic or comparison
// operation of two of them, whatever its bool and vector-matching modifiers. A comparison folds to 1 or 0, a
// division or modulo by zero to NaN. A logical/set operation, vector() and anything holding a selector are no
// number.
func foldLiteral(expr logql.Expr) (float64, bool) {
	switch e := expr.(type) {
	case *logql.LiteralExpr:
		return e.Value, true
	case *logql.BinOpExpr:
		if isSetOperation(e.Op) {
			return 0, false
		}
		l, ok := foldLiteral(e.Left)
		if !ok {
			return 0, false
		}
		r, ok := foldLiteral(e.Right)
		if !ok {
			return 0, false
		}
		return foldOperation(e.Op, l, r)
	}
	return 0, false
}

// foldOperation applies one binary operator to two numbers as Loki folds them.
func foldOperation(op string, l, r float64) (float64, bool) {
	switch op {
	case "+":
		return l + r, true
	case "-":
		return l - r, true
	case "*":
		return l * r, true
	case "/":
		if r == 0 {
			return math.NaN(), true
		}
		return l / r, true
	case "%":
		if r == 0 {
			return math.NaN(), true
		}
		return math.Mod(l, r), true
	case "^":
		return math.Pow(l, r), true
	case "==":
		return boolValue(l == r), true
	case "!=":
		return boolValue(l != r), true
	case ">":
		return boolValue(l > r), true
	case "<":
		return boolValue(l < r), true
	case ">=":
		return boolValue(l >= r), true
	case "<=":
		return boolValue(l <= r), true
	}
	return 0, false
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
