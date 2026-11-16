package proxy

import (
	"context"
	"strconv"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// BinaryOperandsPhysical is what the proxy runs for a binary operation at the top of a query, as physical operators:
// the binary route (binaryExprForRequest, then proxyBinaryLogQL) reads the query again from the request
// (binaryRouteQuery: incomplete unwrap stubs removed, a uniform offset stripped), and sends each operand that is not
// a number through the query handlers as a query of its own, the operand's printed LogQL (Expr.String), which the
// handler translates alone (an operand that is itself a binary operation takes the binary route again); it then
// combines the operands' answers itself. The reference is a physical.Binary of the
// operator, its bool modifier and vector matching, and each operand's translation decoded (a number as a ScalarOp,
// a vector(n) as a VectorOp: evaluateBinaryLogQLOperand answers it from the constant and never translates it or asks
// VictoriaLogs), with operands the texts the proxy translates the operands to, "" for a number and a vector(n). As for the rankings
// (PostAggregationPhysical), it is the text before the operand's handler rewrites it (preserveMetricStreamIdentity,
// pushDownWithoutFold). routed is false when the query is not a binary operation; translate stands in for the
// proxy's translation of a query. It changes nothing the proxy does: it is the reference the typed plan's lowering of
// a binary operation the translator's text for the whole query does not describe is held to.
func BinaryOperandsPhysical(query string, translate func(string) (string, error)) (op physical.Op, operands [2]string, routed bool, err error) {
	bin, ok := binaryRouteExpr(context.Background(), binaryRouteQuery(query, "", "", "")).(*logqlpkg.BinOpExpr)
	if !ok {
		return nil, operands, false, nil
	}
	b, operands, err := binaryRoutePhysical(bin, translate, nil)
	if err != nil {
		return nil, operands, true, err
	}
	return b, operands, true, nil
}

// BinaryOperandLeaves is BinaryOperandsPhysical with the text the proxy translates every query operand to, at any depth
// of a chain, in the order the leaves of the returned operators are read (left before right, "" for a number and a
// vector(n)).
func BinaryOperandLeaves(query string, translate func(string) (string, error)) (op physical.Op, leaves []string, routed bool, err error) {
	bin, ok := binaryRouteExpr(context.Background(), binaryRouteQuery(query, "", "", "")).(*logqlpkg.BinOpExpr)
	if !ok {
		return nil, nil, false, nil
	}
	b, _, err := binaryRoutePhysical(bin, translate, &leaves)
	if err != nil {
		return nil, leaves, true, err
	}
	return b, leaves, true, nil
}

// binaryRoutePhysical is the physical.Binary of one binary operation of the route: each side a number, a vector(n), a
// query (its translation decoded) or, for an operand that is itself a binary operation, the Binary the route evaluates
// it by (evaluateBinaryLogQLOperand sends it through the handlers, which take the binary route again). operands are
// the texts of this operation's own query sides; leaves, when not nil, collects every leaf's text, left first.
func binaryRoutePhysical(bin *logqlpkg.BinOpExpr, translate func(string) (string, error), leaves *[]string) (physical.Binary, [2]string, error) {
	var operands [2]string
	var sides [2]physical.Op
	for i, side := range [2]logqlpkg.Expr{bin.Left, bin.Right} {
		if v, scalar := binaryScalarValue(side, 0); scalar {
			sides[i] = physical.ScalarOp{V: v}
			appendLeaf(leaves, "")
			continue
		}
		if nested, ok := side.(*logqlpkg.BinOpExpr); ok {
			b, _, err := binaryRoutePhysical(nested, translate, leaves)
			if err != nil {
				return physical.Binary{}, operands, err
			}
			sides[i] = b
			continue
		}
		text := emptyWithoutOperand(side).String()
		if v, vector := binaryVectorLiteral(text); vector {
			sides[i] = physical.VectorOp{V: v}
			appendLeaf(leaves, "")
			continue
		}
		var err error
		if operands[i], err = translate(text); err != nil {
			return physical.Binary{}, operands, err
		}
		if sides[i], err = translator.DecodePhysical(operands[i]); err != nil {
			return physical.Binary{}, operands, err
		}
		appendLeaf(leaves, operands[i])
	}
	return physical.Binary{Op: bin.Op, Bool: bin.ReturnBool, Match: binaryRouteMatching(bin.VectorMatching), L: sides[0], R: sides[1]}, operands, nil
}

func appendLeaf(leaves *[]string, text string) {
	if leaves != nil {
		*leaves = append(*leaves, text)
	}
}

// binaryVectorLiteral is the number of an operand's printed LogQL that evaluateBinaryLogQLOperand answers from a
// constant, a vector(n): one series without labels with n at every step, not sent to VictoriaLogs. An operand it does
// not recognise (the call spelled with a blank before its parenthesis, a number Go cannot read) is a query to it.
// The cheap prefix test keeps the expression off the regexp for every other operand.
func binaryVectorLiteral(text string) (float64, bool) {
	i := 0
	for i < len(text) && strings.IndexByte(" \t\n\f\r", text[i]) >= 0 {
		i++
	}
	if !strings.HasPrefix(text[i:], "vector(") {
		return 0, false
	}
	if matches := vectorLiteralRE.FindStringSubmatch(text); len(matches) == 2 {
		if value, err := strconv.ParseFloat(matches[1], 64); err == nil {
			return value, true
		}
	}
	return 0, false
}

// binaryRouteMatching is the vector matching the binary route joins the operands by (binOpExprToVMInfo), as a
// physical.Matching: nil with no modifier, on() is MatchOn with no labels, an empty label list is nil.
func binaryRouteMatching(vm *logqlpkg.VectorMatching) *physical.Matching {
	if vm == nil || vm.Card == "" {
		return nil
	}
	m := &physical.Matching{}
	if vm.Card == "on" {
		m.MatchOn, m.On = true, nilIfEmpty(vm.MatchLabels)
	} else {
		m.Ignoring = nilIfEmpty(vm.MatchLabels)
	}
	switch vm.GroupSide {
	case "group_left":
		m.GroupSide, m.GroupLeft = vm.GroupSide, nilIfEmpty(vm.Include)
	case "group_right":
		m.GroupSide, m.GroupRight = vm.GroupSide, nilIfEmpty(vm.Include)
	}
	return m
}

func nilIfEmpty(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	return labels
}
