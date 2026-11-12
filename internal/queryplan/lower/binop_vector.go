package lower

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// A vector(n) operand of a binary operation is one series without labels with the value n at every step. The proxy's
// binary route answers it from that constant and never sends it to VictoriaLogs (proxy.BinaryOperandsPhysical), so the
// lowering writes a physical.VectorOp for it and the string translator's text is no reference: it reads the operand
// as a phrase to search for or drops it. A binary operation with a vector(n) operand is held to the proxy's route
// (RoutedBinary), as one with a without clause is.
//
// The route recognises the operand by the text it was written as (VectorLiteral), so a spelling it does not
// recognise (`vector (1)`) is left to the translator, whose reading the plan does not describe: BinOp operand spelling.

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
	switch x.(type) {
	case *queryplan.Scalar, *queryplan.VectorLit:
		return true
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

// screenVectorOperands turns away a binary operation with a vector(n) operand that the proxy's route does not read as
// one: it splits the query at its operator, as screenBinOp does, and requires the text of each vector(n) side to be
// one the route recognises (VectorLiteral), with the plan's number. A query it cannot split is left to the translator.
func screenVectorOperands(query string, n *queryplan.BinOp) error {
	q := strings.TrimSpace(logql.CanonicalizeLogRanges(query))
	stripped, _ := stripMatching(q)
	at, tok := topLevelOperator(stripped)
	if at < 0 || tok != translate.BinaryOperatorToken(n.Op) {
		return errBinOperatorSpell
	}
	for i, x := range [2]queryplan.Node{n.L, n.R} {
		lit, ok := x.(*queryplan.VectorLit)
		if !ok {
			continue
		}
		text := stripped[:at]
		if i == 1 {
			text = stripped[at+len(tok):]
		}
		if v, ok := VectorLiteral(text); !ok || v != lit.V {
			return errBinOperandSpelling
		}
	}
	return nil
}
