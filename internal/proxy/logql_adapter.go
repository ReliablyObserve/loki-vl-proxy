package proxy

import (
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// binOpExprToVMInfo converts a LogQL BinOpExpr's VectorMatching into the
// translator.VectorMatchInfo that the binary metric proxy functions expect.
func binOpExprToVMInfo(expr *logqlpkg.BinOpExpr) *translator.VectorMatchInfo {
	if expr.VectorMatching == nil {
		return nil
	}
	vm := &translator.VectorMatchInfo{}
	switch expr.VectorMatching.Card {
	case "on":
		vm.On = expr.VectorMatching.MatchLabels
	case "ignoring":
		vm.Ignoring = expr.VectorMatching.MatchLabels
	}
	switch expr.VectorMatching.GroupSide {
	case "group_left":
		vm.GroupLeft = expr.VectorMatching.Include
	case "group_right":
		vm.GroupRight = expr.VectorMatching.Include
	}
	return vm
}
