package proxy

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// PostAggregationRoute reports how the proxy routes a metric query it evaluates in part itself (a ranking, a
// stddev or stdvar, or an aggregation over a ranking): routed, and the text of the operand it translates, sliced
// from the query as written. It is the decision both query handlers take before they translate anything
// (parseInstantMetricPostAggQuery), exported for the tests that hold the typed plan to it.
func PostAggregationRoute(query string) (operand string, routed bool) {
	postAgg, routed := parseInstantMetricPostAggQuery(query)
	return postAgg.inner, routed
}

// PostAggregationPhysical is what the proxy runs for a query it evaluates in part itself, as physical operators: the
// operand's operators (the translation of the operand text decoded) under one RankStep or SeriesAgg for each level
// the proxy evaluates, innermost first. operandText is the text the proxy translates the operand to, which is the
// request it sends to VictoriaLogs before preserveMetricStreamIdentity (the translation of the operand alone, the
// whole query is never translated on this route). translate stands in for the proxy's translation of a query. It
// changes nothing the proxy does: it is the reference the typed plan's lowering of these roots is held to.
func PostAggregationPhysical(query string, translate func(string) (string, error)) (op physical.Op, operandText string, routed bool, err error) {
	postAgg, routed := parseInstantMetricPostAggQuery(query)
	if !routed {
		return nil, "", false, nil
	}
	operandText, err = postAggOperandText(postAgg, translate)
	if err != nil {
		return nil, "", true, err
	}
	op, err = postAggregationOp(postAgg, operandText)
	return op, operandText, true, err
}

// postAggregationOp is the reference both shadows hold the lowering of a proxy-evaluated root to: the operand's
// operators (operandText, the text the proxy sends for the operand, decoded) under one RankStep or SeriesAgg for each
// level of postAgg, innermost first. The corpus shadow reaches it through PostAggregationPhysical, the live shadow
// (shadowProxyEvaluated) with the text the handler has just sent, so neither translates anything twice.
func postAggregationOp(postAgg instantMetricPostAgg, operandText string) (physical.Op, error) {
	op, err := translator.DecodePhysical(operandText)
	if err != nil {
		return nil, err
	}
	for layer := &postAgg; layer != nil; layer = layer.outer {
		by := layer.group
		if layer.grouping != nil {
			by = layer.grouping
		}
		var clause *physical.Grouping
		if by != nil {
			clause = &physical.Grouping{Labels: by.Labels, Without: by.Without}
		}
		switch layer.name {
		case "topk", "bottomk", "sort", "sort_desc":
			op = physical.RankStep{Op: layer.name, K: layer.k, By: clause, In: op}
		default:
			op = physical.SeriesAgg{Op: layer.name, By: clause, In: op}
		}
	}
	return op, nil
}
