package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// seriesAggregationChain returns the vector aggregations over a range
// aggregation, innermost first, when one of them is count, min, max or avg:
// they read one value per Loki series, so the translated query keeps the series
// identity in its first stats pipe, and the manual range evaluator, which reads
// only that first pipe, answers every series unreduced. A `without` anywhere is
// folded by the without marker; the other operators are not series-level.
func seriesAggregationChain(query string) ([]*logqlpkg.VectorAggregation, bool) {
	expr, err := logqlpkg.Parse(stripOuterLabelReplace(strings.TrimSpace(query)))
	if err != nil {
		return nil, false
	}
	var chain []*logqlpkg.VectorAggregation
	seriesLevel := false
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		switch agg.Op {
		case logqlpkg.VectorSum:
		case logqlpkg.VectorCount, logqlpkg.VectorMin, logqlpkg.VectorMax, logqlpkg.VectorAvg:
			seriesLevel = true
		default:
			return nil, false
		}
		if agg.Grouping != nil && agg.Grouping.Without {
			return nil, false
		}
		chain = append([]*logqlpkg.VectorAggregation{agg}, chain...)
		expr = agg.Inner
	}
	ra, ok := expr.(*logqlpkg.RangeAggregation)
	return chain, ok && ra.Grouping == nil && seriesLevel
}

// vectorAggFoldKey marks a request whose vector aggregations
// foldVectorAggregations folds: its series keep the labels a parser extracts and
// the aggregation groups by (`count by (route)` over `| logfmt route`), which
// the fold reads (withParsedGroupLabels).
type vectorAggFoldKey struct{}

// keepsParsedGroupLabels reports whether the series of the request carry the
// labels a parser extracts that the stats group by: the proxy folds a `without`
// (withoutFoldKey) or the vector aggregations (vectorAggFoldKey) afterwards.
func keepsParsedGroupLabels(ctx context.Context) bool {
	without, _ := ctx.Value(withoutFoldKey{}).(bool)
	vector, _ := ctx.Value(vectorAggFoldKey{}).(bool)
	return without || vector
}

// foldVectorAggregations runs a manual range metric evaluator and applies the
// vector aggregations of the query to the series it returns, per step, the way
// Loki's VectorAggEvaluator does (pkg/logql/evaluator.go VectorAggEvaluator.Next:
// each series of the input falls into the group its labels name, `by` keeping
// the listed labels, none keeping none, and the group takes the count, minimum,
// maximum or mean of the series' values). Loki counts max_query_series on that
// answer, not on the series the range aggregation yields. Any other query is
// left to foldUngroupedSum.
func foldVectorAggregations(p *Proxy, w http.ResponseWriter, r *http.Request, query string, serve func(http.ResponseWriter, *http.Request) bool) bool {
	chain, ok := seriesAggregationChain(query)
	if !ok {
		return foldUngroupedSum(w, r, query, serve)
	}
	buf := &bufferedResponseWriter{}
	ctx := context.WithValue(context.WithValue(r.Context(), foldedSumKey{}, true), vectorAggFoldKey{}, true)
	if !serve(buf, r.WithContext(ctx)) {
		return false
	}
	body, code := buf.body, buf.code
	if code == 0 || code == http.StatusOK {
		for _, agg := range chain {
			body = foldByGrouping(body, agg)
		}
		capped, err := capSeriesToLimit(r.Context(), body, p.resolvedMaxStatsQuerySeries(r.Context()))
		if err != nil {
			p.writeError(w, badRequestStatusOr(err, http.StatusServiceUnavailable), err.Error())
			return true
		}
		body = capped
	}
	copyHeaders(w.Header(), buf.Header())
	if code != 0 {
		w.WriteHeader(code)
	}
	_, _ = w.Write(body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- the evaluator's own JSON, Content-Type copied above
	return true
}

// foldByGrouping applies one vector aggregation to a matrix or vector body:
// series that share the labels the grouping keeps become one, valued by the
// aggregation's operator. A grouping of no labels keeps none.
func foldByGrouping(body []byte, agg *logqlpkg.VectorAggregation) []byte {
	exclude := []string{withoutAllLabels}
	if agg.Grouping != nil && len(agg.Grouping.Labels) > 0 {
		keep := make(map[string]bool, len(agg.Grouping.Labels))
		for _, label := range agg.Grouping.Labels {
			keep[label] = true
		}
		exclude = exclude[:0]
		for _, name := range metricLabelNames(body) {
			if !keep[name] {
				exclude = append(exclude, name)
			}
		}
	}
	return applyWithoutGrouping(body, exclude, string(agg.Op))
}

// metricLabelNames returns the label names the series of a matrix or vector body
// carry.
func metricLabelNames(body []byte) []string {
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &resp) != nil {
		return nil
	}
	seen := map[string]struct{}{}
	var names []string
	for _, series := range resp.Data.Result {
		for name := range series.Metric {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				names = append(names, name)
			}
		}
	}
	return names
}
