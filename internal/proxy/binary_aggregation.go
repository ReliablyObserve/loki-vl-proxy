package proxy

import (
	"encoding/json"
	"math"
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

func isBinaryExpr(expr logqlpkg.Expr) bool {
	_, ok := expr.(*logqlpkg.BinOpExpr)
	return ok
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
	// Under a label_replace or label_join the handler has cloned the request
	// to the expression inside it, and applies the transform to the answer.
	chain, binary, ok := binaryAggregationChain(binaryRequestExpr(r))
	if !ok {
		// The request's own query does not parse as the routed one did: answer
		// Loki's parse error for it.
		msg := "unsupported aggregation over a binary expression"
		if err := validateLogQLSyntax(binaryRequestQuery(r)); err != "" {
			msg = truncateQueryError(err)
		}
		p.writeError(w, http.StatusBadRequest, msg)
		return true
	}
	if fast := sumRateScalarDivision(chain, binary); fast != nil {
		// A log-line rate has a non-negative finite sample per active stream.
		// Summing those samples before division preserves the answer and lets
		// the existing grouped stats route avoid a costly per-stream read.
		p.proxyBinaryLogQL(w, cloneMetricQueryRequest(r, fast.String()), fast, resultType)
		return true
	}
	if fast := sumRateCountRatio(chain, binary); fast != nil {
		// For each active stream, rate/count_over_time is one divided by the
		// window seconds. Count active streams in one grouped stats query.
		p.proxyBinaryLogQL(w, cloneMetricQueryRequest(r, fast.String()), fast, resultType)
		return true
	}
	buf := p.binaryAggregationResponse(r, binary, chain, resultType, 0)
	copyHeaders(w.Header(), buf.Header())
	w.Header().Set("Content-Type", "application/json")
	if buf.code != 0 {
		w.WriteHeader(buf.code)
	}
	_, _ = w.Write(buf.body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- the binary evaluator's own JSON
	return true
}

func sumRateScalarDivision(chain []*logqlpkg.VectorAggregation, binary *logqlpkg.BinOpExpr) *logqlpkg.BinOpExpr {
	if len(chain) != 1 || chain[0].Op != logqlpkg.VectorSum || binary.Op != "/" || binary.ReturnBool || binary.VectorMatching != nil {
		return nil
	}
	divisor, scalar := binaryScalarValue(binary.Right, 0)
	if !scalar || divisor <= 0 || math.IsInf(divisor, 0) || math.IsNaN(divisor) {
		return nil
	}
	rate, ok := binary.Left.(*logqlpkg.RangeAggregation)
	if !ok || rate.Op != logqlpkg.RangeRate || rate.Grouping != nil {
		return nil
	}
	logs, ok := rate.Inner.(*logqlpkg.LogQuery)
	if !ok || len(logs.Pipeline) != 0 {
		return nil
	}
	agg := *chain[0]
	agg.Inner = rate
	return &logqlpkg.BinOpExpr{Left: &agg, Right: binary.Right, Op: "/"}
}

func sumRateCountRatio(chain []*logqlpkg.VectorAggregation, binary *logqlpkg.BinOpExpr) *logqlpkg.BinOpExpr {
	if len(chain) != 1 || chain[0].Op != logqlpkg.VectorSum || binary.Op != "/" || binary.ReturnBool || binary.VectorMatching != nil {
		return nil
	}
	if chain[0].Grouping == nil || chain[0].Grouping.Without {
		return nil
	}
	rate, leftOK := binary.Left.(*logqlpkg.RangeAggregation)
	count, rightOK := binary.Right.(*logqlpkg.RangeAggregation)
	if !leftOK || !rightOK || rate.Op != logqlpkg.RangeRate || count.Op != logqlpkg.RangeCountOverTime || rate.Grouping != nil || count.Grouping != nil || rate.Range != count.Range || rate.Offset != count.Offset || rate.Inner.String() != count.Inner.String() {
		return nil
	}
	logs, ok := rate.Inner.(*logqlpkg.LogQuery)
	if !ok || len(logs.Pipeline) != 0 {
		return nil
	}
	window, ok := parsePositiveStepDuration(rate.Range)
	if !ok || window.Seconds() <= 0 {
		return nil
	}
	agg := *chain[0]
	agg.Op = logqlpkg.VectorCount
	agg.Inner = count
	return &logqlpkg.BinOpExpr{Left: &agg, Right: &logqlpkg.LiteralExpr{Value: window.Seconds()}, Op: "/"}
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
