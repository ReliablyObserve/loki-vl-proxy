package lower

import (
	"errors"
	"slices"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// The route rule of a ranking and of a label_replace at the root. The handlers evaluate both over the route of their
// operand: they slice the operand out of the query, send its requests as the query they would send for it alone, and
// apply the ranking (per step, k series per group or every series in order) or the label rewrite to its answer. The
// rule is the operand's route, with what the ranking changes in how its windows are read.

// RankedOperand is the ranking the handlers evaluate over the series of a query whose root is the ranking r, and the
// operand they evaluate it over. The handlers read the vector aggregations at the root down to the first other node:
// the innermost ranking among them is the one they evaluate when every layer above it is a ranking or a sum, avg,
// min, max or count (which fold what it keeps); with another level above it (a stddev, a stdvar), the root ranking is.
func RankedOperand(r *queryplan.Rank) (*queryplan.Rank, queryplan.Node) {
	// rank is the innermost ranking read so far, ok whether every layer above it folds; folds is whether every layer
	// read so far does.
	rank, ok, folds := r, true, true
	for n := r.In; ; {
		switch v := n.(type) {
		case *queryplan.Rank:
			rank, ok, n = v, folds, v.In
			continue
		case *queryplan.VectorAgg:
			folds, n = folds && routeOps[v.Op], v.In
			continue
		}
		if !ok {
			// A level the handlers do not fold stands above the innermost ranking: the root ranks.
			rank = r
		}
		return rank, rank.In
	}
}

// LabelReplaceOperand is the expression under the label_replace calls at the root of n, n itself when it has none.
func LabelReplaceOperand(n queryplan.Node) queryplan.Node {
	for {
		lr, ok := n.(*queryplan.LabelReplace)
		if !ok {
			return n
		}
		n = lr.In
	}
}

// LabelReplaces are the label_replace calls at the root of n, innermost first (the order the handlers apply them in,
// physical.LabelMap), and the expression they wrap: none and n itself when its root is not one.
func LabelReplaces(n queryplan.Node) ([]physical.LabelReplace, queryplan.Node) {
	var specs []physical.LabelReplace
	for {
		lr, ok := n.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		specs = append(specs, physical.LabelReplace{DstLabel: lr.Dst, Replacement: lr.Replacement, SrcLabel: lr.Src, Regex: lr.Regex})
		n = lr.In
	}
	slices.Reverse(specs)
	return specs, n
}

// routeEvaluated is the route of a plan whose root is a ranking or a label_replace: the route of the operand, its
// delegation named "<kind> operand <the operand's>". A label_replace around a ranking is read by the handlers as
// another kind of query (LabelReplace route ranking operand).
func routeEvaluated(p *queryplan.Plan, prof Profile, req RouteRequest) (QueryRoute, error) {
	var kind string
	var operand queryplan.Node
	var rank *queryplan.Rank
	switch v := p.Root.(type) {
	case *queryplan.Rank:
		kind = "Rank"
		rank, operand = RankedOperand(v)
	case *queryplan.LabelReplace:
		kind, operand = "LabelReplace", LabelReplaceOperand(v)
		if _, ok := operand.(*queryplan.Rank); ok {
			return QueryRoute{}, routeError(kind, "ranking operand")
		}
	}
	sub := *p
	sub.Root = operand
	out, err := RouteQuery(&sub, prof, req)
	if err != nil {
		var nl *NotLoweredError
		if errors.As(err, &nl) {
			err = &NotLoweredError{Construct: kind + " operand " + nl.Construct}
		}
		return QueryRoute{}, err
	}
	if rank != nil && !out.Binary && out.n == 1 {
		r := &out.operands[0]
		// The handlers fold the series of a `without` under a ranking themselves: VictoriaLogs does not.
		if r.Fold == FoldPushed {
			r.Fold = FoldRows
		}
		if ranksEveryStep(rank, *r) {
			// The ranking needs every series of each step: the series of the stream are read as raw rows, which the
			// rule does not decide yet, and a grouping by labels as one grid of rows. Ungrouped stats are one series.
			switch {
			case r.Identity || r.StreamGrouped || r.Fold != FoldNone:
				return QueryRoute{}, routeError(kind, "ranked rows")
			case r.LabelGrouped:
				r.RankedGrid = true
			}
		}
	}
	return out, nil
}

// ranksEveryStep reports whether the handlers rank every series of each step of the operand route r of a topk or
// bottomk themselves, from rows they read: tumbling windows of a line count or a byte sum.
func ranksEveryStep(rank *queryplan.Rank, r MetricRoute) bool {
	return (rank.Op == logql.VectorTopK || rank.Op == logql.VectorBottomK) && r.Window == WindowTumbling && routeRangeOps[r.Op] && !r.Counted
}
