package proxy

import (
	"encoding/json"
	"net/http"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// binaryAggregationChain returns the sum, avg, min, max and count aggregations
// over a binary expression, innermost first, and that expression. Loki
// evaluates the binary per series (pkg/logql/evaluator.go newBinOpStepEvaluator)
// and aggregates its answer per step (VectorAggEvaluator.Next); a binary of two
// literals is a literal, which no aggregation here takes.
func binaryAggregationChain(expr logqlpkg.Expr) ([]*logqlpkg.VectorAggregation, *logqlpkg.BinOpExpr, bool) {
	var chain []*logqlpkg.VectorAggregation
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		switch agg.Op {
		case logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorCount:
		default:
			return nil, nil, false
		}
		chain = append([]*logqlpkg.VectorAggregation{agg}, chain...)
		expr = agg.Inner
	}
	binary, ok := expr.(*logqlpkg.BinOpExpr)
	if !ok || len(chain) == 0 {
		return nil, nil, false
	}
	_, leftScalar := binaryScalarValue(binary.Left, 0)
	_, rightScalar := binaryScalarValue(binary.Right, 0)
	return chain, binary, !leftScalar || !rightScalar
}

// proxyBinaryAggregation answers a vector aggregation over a binary expression
// when routed is one: each operand is its own VictoriaLogs stats query, joined
// by the binary evaluator, and the aggregation folds the joined series per
// step. Both work on aggregated series only. The expression is read from the
// request, as the binary path does: the handler's query may have lost a parser
// stage another operand needed (preferWorkingParser).
func (p *Proxy) proxyBinaryAggregation(w http.ResponseWriter, r *http.Request, routed logqlpkg.Expr, resultType string) bool {
	if _, _, ok := binaryAggregationChain(routed); !ok {
		return false
	}
	chain, binary, ok := binaryAggregationChain(binaryRequestExpr(r))
	if !ok {
		// Routed from under a label_replace, which the handler applies to the answer.
		expr, _ := logqlpkg.Parse(stripOuterLabelReplace(binaryRequestQuery(r)))
		chain, binary, ok = binaryAggregationChain(expr)
	}
	if !ok {
		p.writeError(w, http.StatusBadRequest, "unsupported aggregation over a binary expression")
		return true
	}
	buf := &bufferedResponseWriter{}
	p.proxyBinaryLogQL(buf, r, binary, resultType)
	body := buf.body
	if buf.code == 0 || buf.code == http.StatusOK {
		for _, agg := range chain {
			body = foldBinaryAggregation(body, agg)
		}
	}
	copyHeaders(w.Header(), buf.Header())
	if buf.code != 0 {
		w.WriteHeader(buf.code)
	}
	_, _ = w.Write(body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- the binary evaluator's own JSON, Content-Type copied above
	return true
}

// foldBinaryAggregation applies one aggregation to a matrix or vector body:
// series that share the labels the grouping keeps become one, valued by the
// operator (VectorAggEvaluator.Next: `by` keeps the listed labels, `without`
// drops them, no grouping keeps none).
func foldBinaryAggregation(body []byte, agg *logqlpkg.VectorAggregation) []byte {
	op := string(agg.Op)
	if agg.Grouping != nil && agg.Grouping.Without {
		return applyWithoutGrouping(body, agg.Grouping.Labels, op)
	}
	exclude := []string{withoutAllLabels}
	if agg.Grouping != nil && len(agg.Grouping.Labels) > 0 {
		keep := make(map[string]bool, len(agg.Grouping.Labels))
		for _, label := range agg.Grouping.Labels {
			keep[label] = true
		}
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
				} `json:"result"`
			} `json:"data"`
		}
		_ = json.Unmarshal(body, &resp)
		exclude = exclude[:0]
		for _, series := range resp.Data.Result {
			for name := range series.Metric {
				if !keep[name] {
					keep[name] = true // exclude each name once
					exclude = append(exclude, name)
				}
			}
		}
	}
	return applyWithoutGrouping(body, exclude, op)
}
