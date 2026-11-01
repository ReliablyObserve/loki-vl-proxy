package proxy

import (
	"context"
	"math"
	"net/http"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// orderedJSONVectorPlan keeps the innermost sum's parser hints and ordered
// pipeline semantics. Outer vector operators consume its samples; they cannot
// turn parser failures into successful samples or discard its error filters.
// Read the AST directly: its display rendering need not preserve pipeline text.
func orderedJSONVectorPlan(query string) (*orderedJSONMetricPlan, []*logqlpkg.VectorAggregation, bool) {
	if !strings.Contains(query, "json") {
		return nil, nil, false
	}
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return nil, nil, false
	}
	var outer []*logqlpkg.VectorAggregation
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			return nil, nil, false
		}
		if plan, ok := compileOrderedJSONMetricExpr(expr); ok && len(outer) > 0 && plan.parser == "json" {
			return plan, outer, true
		}
		switch agg.Op {
		case logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorCount:
		case logqlpkg.VectorTopK, logqlpkg.VectorBottomK:
			if !agg.HasParam || math.IsNaN(agg.Param) || math.IsInf(agg.Param, 0) || agg.Param >= float64(math.MaxInt) || agg.Param < float64(math.MinInt) {
				return nil, nil, false
			}
		default:
			return nil, nil, false
		}
		outer = append(outer, agg)
		expr = agg.Inner
	}
}

func (p *Proxy) handleOrderedJSONVectorMetric(w http.ResponseWriter, r *http.Request, requestStart time.Time, query string, isRange bool) bool {
	plan, outer, ok := orderedJSONVectorPlan(query)
	if !ok {
		return false
	}
	buffered := &bufferedResponseWriter{header: make(http.Header)}
	// The native rewrite enters a complete request handler and records a
	// request before the outer result exists. Keep this inner evaluation here
	// so the final answer owns request status and tracking exactly once.
	plan.withoutJSON = ""
	// As on the existing without-fold route, read the inner series under the
	// bounded row budget and enforce the tenant's series limit on the result.
	p.recordFoldBudget(r.Context())
	inner := withFoldedSeries(r.WithContext(context.WithValue(r.Context(), foldedSumKey{}, true)))
	if !p.serveOrderedJSONMetric(buffered, inner, requestStart, query, isRange, plan, false) {
		return false
	}
	endpoint := "query"
	if isRange {
		endpoint = "query_range"
	}
	status := buffered.code
	if status == 0 {
		status = http.StatusOK
	}
	defer func() {
		if run := queryStepsRunFor(r); run != nil && run.inner {
			return // the outer settled-steps request owns client observations
		}
		p.metrics.RecordRequest(endpoint, status, time.Since(requestStart))
		p.queryTracker.Record(endpoint, query, time.Since(requestStart), status >= http.StatusBadRequest)
	}()
	body := buffered.body
	if buffered.code == 0 || buffered.code == http.StatusOK {
		for index := len(outer) - 1; index >= 0; index-- {
			agg := outer[index]
			switch agg.Op {
			case logqlpkg.VectorTopK, logqlpkg.VectorBottomK:
				k := max(0, int(agg.Param))
				if isRange {
					body = applyTopKToMatrix(body, k, agg.Op == logqlpkg.VectorTopK, agg.Grouping)
				} else {
					body = applyTopKToVector(body, k, agg.Op == logqlpkg.VectorTopK, agg.Grouping)
				}
			default:
				if folded, ok := foldLevels(body, foldLevelOf(agg)); ok {
					body = folded
				} else {
					body = foldByGrouping(body, agg)
				}
			}
		}
		var err error
		body, err = p.capWithoutFold(r.Context(), body)
		if err != nil {
			status = p.writeErr(w, r.Context(), err)
			return true
		}
	}
	copyBackendHeaders(w.Header(), buffered.Header())
	if buffered.code != 0 {
		w.WriteHeader(buffered.code)
	}
	_, _ = w.Write(body)
	return true
}
