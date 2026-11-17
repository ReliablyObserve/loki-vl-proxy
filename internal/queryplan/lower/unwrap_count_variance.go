package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// CountsVarianceSeries reports a count by labels over stdvar_over_time of an unwrapped label: Loki answers the number
// of series of each group, whatever their variance. The string translator writes the deviation of each group's pooled
// samples and squares it, so the answer is a variance (semantics/count-over-stdvar-answers-variance); the lowering
// writes the series of the deviation, then their count per group (varianceVector), as it writes count over
// stddev_over_time, so the proxy serves the query from it.
func CountsVarianceSeries(p *queryplan.Plan) bool {
	return countsVarianceSeries(p.Root)
}

// countsVarianceSeries is CountsVarianceSeries of a node.
func countsVarianceSeries(n queryplan.Node) bool {
	va, ok := n.(*queryplan.VectorAgg)
	if !ok || va.Op != logql.VectorCount || va.By == nil || va.By.Without || len(va.By.Labels) == 0 {
		return false
	}
	ra, ok := va.In.(*queryplan.RangeAgg)
	return ok && ra.Op == logql.RangeStdvarOverTime && ra.By == nil && ra.In != nil && unwrapStage(ra.In) != nil
}

// WritesCountVarianceSeries reports a plan with, at any depth, a count by labels over stdvar_over_time
// (CountsVarianceSeries' shape): the lowering writes the count of the series wherever it stands, where the string
// translator writes a variance. The proxy serves only the root; the shadows hold every such plan to Loki.
func WritesCountVarianceSeries(p *queryplan.Plan) bool {
	var walk func(n queryplan.Node) bool
	walk = func(n queryplan.Node) bool {
		switch n := n.(type) {
		case *queryplan.VectorAgg:
			return countsVarianceSeries(n) || walk(n.In)
		case *queryplan.Rank:
			return walk(n.In)
		case *queryplan.LabelReplace:
			return walk(n.In)
		case *queryplan.BinOp:
			return walk(n.L) || walk(n.R)
		}
		return false
	}
	return walk(p.Root)
}
