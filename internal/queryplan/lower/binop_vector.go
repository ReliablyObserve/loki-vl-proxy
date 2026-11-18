package lower

import (
	"regexp"
	"strconv"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// A vector(n) operand of a binary operation is one series without labels with the value n at every step. The proxy's
// binary route answers it from that constant and never sends it to VictoriaLogs (proxy.BinaryOperandsPhysical), so the
// lowering writes a physical.VectorOp for it and the string translator's text is no reference: it reads the operand
// as a phrase to search for or drops it. A binary operation with a vector(n) operand is held to the proxy's route
// (RoutedBinary), as one with a without clause is.
//
// The route recognises the operand by its printed text (VectorLiteral), which the request's canonical spelling writes
// `vector(n)` for every spelling Loki reads as the call (proxy requestQueryText), so the plan describes what it answers.

// vectorLiteralRE is the text the proxy's binary route reads as a vector(n) operand: the call written without a blank
// before its parenthesis, one number inside, blanks around it allowed.
var vectorLiteralRE = regexp.MustCompile(`^\s*vector\(\s*([^)]+?)\s*\)\s*$`)

// VectorLiteral is the number of text when the proxy's binary route reads it as a vector(n) operand and answers it
// from the constant, false when it sends the text on as a query.
func VectorLiteral(text string) (float64, bool) {
	m := vectorLiteralRE.FindStringSubmatch(text)
	if len(m) != 2 {
		return 0, false
	}
	v, err := strconv.ParseFloat(m[1], 64)
	return v, err == nil
}

// vectorText is a vector(n) operand as the lowering writes it.
func vectorText(v float64) string { return "vector(" + binNumber(v) + ")" }

// constantOperand reports whether x is a number or a vector(n).
func constantOperand(x queryplan.Node) bool {
	switch v := x.(type) {
	case *queryplan.Scalar, *queryplan.VectorLit:
		return true
	case *queryplan.BinOp:
		// A chain of constants is a constant.
		return constantOperand(v.L) && constantOperand(v.R)
	}
	return false
}

// queryOperand reports whether x is a range or vector aggregation, a query the proxy sends to VictoriaLogs.
func queryOperand(x queryplan.Node) bool {
	switch x.(type) {
	case *queryplan.RangeAgg, *queryplan.VectorAgg:
		return true
	}
	return false
}

// vectorOperand reports whether n has a vector(n) operand next to a range or vector aggregation, the binary
// operation the proxy's route evaluates operand by operand. A query of constants only is left to the translator by
// binOpShape (BinOp constant operands), so it is not held to the route.
func vectorOperand(n *queryplan.BinOp) bool {
	_, l := n.L.(*queryplan.VectorLit)
	_, r := n.R.(*queryplan.VectorLit)
	return l && queryOperand(n.R) || r && queryOperand(n.L)
}
