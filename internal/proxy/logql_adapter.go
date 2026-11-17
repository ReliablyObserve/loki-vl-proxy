package proxy

import (
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// binOpExprToVMInfo converts a LogQL BinOpExpr's VectorMatching into the
// physical.Matching that the binary metric proxy functions expect.
func binOpExprToVMInfo(expr *logqlpkg.BinOpExpr) *physical.Matching {
	if expr.VectorMatching == nil {
		return nil
	}
	vm := &physical.Matching{}
	switch expr.VectorMatching.Card {
	case "on":
		vm.MatchOn = true
		vm.On = expr.VectorMatching.MatchLabels
	case "ignoring":
		vm.Ignoring = expr.VectorMatching.MatchLabels
	}
	switch expr.VectorMatching.GroupSide {
	case "group_left":
		vm.GroupSide = "group_left"
		vm.GroupLeft = expr.VectorMatching.Include
	case "group_right":
		vm.GroupSide = "group_right"
		vm.GroupRight = expr.VectorMatching.Include
	}
	return vm
}
