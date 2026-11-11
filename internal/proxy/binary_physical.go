package proxy

import (
	"context"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// BinaryOperandsPhysical is what the proxy runs for a binary operation at the top of a query, as physical operators:
// the binary route (binaryExprForRequest, then proxyBinaryLogQL) reads the query again from the request
// (binaryRouteQuery: incomplete unwrap stubs removed, a uniform offset stripped), and sends each operand that is not
// a number through the query handlers as a query of its own, the operand's printed LogQL (Expr.String), which the
// handler translates alone; it then combines the operands' answers itself. The reference is a physical.Binary of the
// operator, its bool modifier and vector matching, and each operand's translation decoded (a number as a ScalarOp),
// with operands the texts the proxy translates the operands to, "" for a number. As for the rankings
// (PostAggregationPhysical), it is the text before the operand's handler rewrites it (preserveMetricStreamIdentity,
// pushDownWithoutFold). routed is false when the query is not a binary operation; translate stands in for the
// proxy's translation of a query. It changes nothing the proxy does: it is the reference the typed plan's lowering of
// a binary operation the translator's text for the whole query does not describe is held to.
func BinaryOperandsPhysical(query string, translate func(string) (string, error)) (op physical.Op, operands [2]string, routed bool, err error) {
	bin, ok := binaryRouteExpr(context.Background(), binaryRouteQuery(query, "", "", "")).(*logqlpkg.BinOpExpr)
	if !ok {
		return nil, operands, false, nil
	}
	var sides [2]physical.Op
	for i, side := range [2]logqlpkg.Expr{bin.Left, bin.Right} {
		if v, scalar := binaryScalarValue(side, 0); scalar {
			sides[i] = physical.ScalarOp{V: v}
			continue
		}
		if operands[i], err = translate(side.String()); err != nil {
			return nil, operands, true, err
		}
		if sides[i], err = translator.DecodePhysical(operands[i]); err != nil {
			return nil, operands, true, err
		}
	}
	return physical.Binary{Op: bin.Op, Bool: bin.ReturnBool, Match: binaryRouteMatching(bin.VectorMatching), L: sides[0], R: sides[1]}, operands, true, nil
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
