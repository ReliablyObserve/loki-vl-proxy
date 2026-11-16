package proxy

import (
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// rankOverBinaryResponseVersion fences the query_range answers cached before a ranking, stddev or stdvar whose operand
// is a binary operation (or one under aggregations and label_replace calls) was evaluated by the binary route
// (semantics/rank-over-binary-operation): the range handler sent the translation of the whole operation, a marker of
// its two operands' queries, to VictoriaLogs as one query, and answered another value, a series too many or too few,
// or a 500. An instant answer of a binary operation was Loki's and keeps its key; one under an aggregation or a
// label_replace answered a 500, which is never cached.
const rankOverBinaryResponseVersion = "@rank-over-binary-v1:"

// rankOverBinaryResponseKeyVersion is the fence of a query whose plan holds a ranking, stddev or stdvar over a binary
// operation at any depth: at the root, under an aggregation, and in an operand of a binary operation or a
// label_replace, which the handlers evaluate as a query of its own, so their answers changed too. A text screen comes
// first, so no other query's key changes and a query without a ranking word reads no plan here: ranked is whether the
// chain's scans read one, in every spelling the parser reads (topk, bottomk, sort and sort_desc are read by the
// per-stream series scan, streamLevelSeriesScan; stddev and stdvar are words of scanFenceWords). The query's shared plan
// decides.
func rankOverBinaryResponseKeyVersion(query string, ranked bool) string {
	if !ranked {
		return ""
	}
	if plan := resolvedPlan(query); plan != nil && rankOverBinary(plan.Root) {
		return rankOverBinaryResponseVersion
	}
	return ""
}

// rankOverBinary reports a ranking, stddev or stdvar whose operand is a binary operation, or one under aggregations and
// label_replace calls (holdsBinaryOperand), anywhere in n.
func rankOverBinary(n queryplan.Node) bool {
	switch v := n.(type) {
	case *queryplan.Rank:
		return binaryOperandNode(v.In) || rankOverBinary(v.In)
	case *queryplan.VectorAgg:
		if (v.Op == logqlpkg.VectorStddev || v.Op == logqlpkg.VectorStdvar) && binaryOperandNode(v.In) {
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

// binaryOperandNode is holdsBinaryOperand of a plan node: a binary operation, under vector aggregations and
// label_replace calls.
func binaryOperandNode(n queryplan.Node) bool {
	for {
		switch v := n.(type) {
		case *queryplan.BinOp:
			return true
		case *queryplan.VectorAgg:
			n = v.In
		case *queryplan.LabelReplace:
			n = v.In
		default:
			return false
		}
	}
}
