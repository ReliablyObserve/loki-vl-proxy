package proxy

import (
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// rankOverBinaryResponseVersion fences the query_range answers cached before a ranking, stddev or stdvar whose operand
// is a binary operation was evaluated by the binary route (semantics/rank-over-binary-operation): the range handler
// sent the translation of the whole operation, a marker of its two operands' queries, to VictoriaLogs as one query,
// and answered another value, a series too many or too few, or a 500. Instant answers were Loki's and keep their keys.
const rankOverBinaryResponseVersion = "@rank-over-binary-v1:"

// rankOverBinaryResponseKeyVersion is the fence of a query whose plan holds a ranking, stddev or stdvar over a binary
// operation at any depth: at the root, under an aggregation, and in an operand of a binary operation or a
// label_replace, which the handlers evaluate as a query of its own, so their answers changed too. A text screen comes
// first, so no other query's key changes and a query without a ranking word builds no plan here: ranked is whether the
// chain's word scans read one, in every spelling the parser reads (topk, bottomk, sort and sort_desc fire the per-stream
// series screen, streamLevelSeriesResponseKeyVersion; stddev and stdvar are words of scanFenceWords). The query's
// shared plan decides.
func rankOverBinaryResponseKeyVersion(query string, ranked bool) string {
	if !ranked {
		return ""
	}
	if plan := resolvedPlan(query); plan != nil && rankOverBinary(plan.Root) {
		return rankOverBinaryResponseVersion
	}
	return ""
}

// rankOverBinary reports a ranking, stddev or stdvar whose operand is a binary operation, anywhere in n.
func rankOverBinary(n queryplan.Node) bool {
	switch v := n.(type) {
	case *queryplan.Rank:
		_, binary := v.In.(*queryplan.BinOp)
		return binary || rankOverBinary(v.In)
	case *queryplan.VectorAgg:
		if _, binary := v.In.(*queryplan.BinOp); binary && (v.Op == logqlpkg.VectorStddev || v.Op == logqlpkg.VectorStdvar) {
			return true
		}
		return rankOverBinary(v.In)
	case *queryplan.LabelReplace:
		return rankOverBinary(v.In)
	case *queryplan.BinOp:
		return rankOverBinary(v.L) || rankOverBinary(v.R)
	}
	return false
}
