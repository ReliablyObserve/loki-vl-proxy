package proxy

import (
	"net/http"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// validateQueryTree is the validation of a query the cache in cachedQueryValidation holds: the parser and its
// semantic pass, then Loki's checks on the typed tree (queryplan.Validate). It returns the parsed expression
// (nil for the bare wildcard or on a rejection) and Loki's error text, or "", and whether the text is one of
// the tree's checks (the query passed the parser and its semantic pass).
func validateQueryTree(query string) (logqlpkg.Expr, string, bool) {
	expr, msg := logqlpkg.ValidateLogQLExpr(query)
	if msg != "" || expr == nil {
		return expr, msg, false
	}
	if rejection := queryplan.Validate(expr); rejection != nil {
		return nil, treeRejection(rejection).Text(), true
	}
	return expr, "", false
}

// treeRejection is the client error of a tree check's rejection: Loki's parse error without a position.
func treeRejection(r *queryplan.Rejection) *lokierr.Error { return lokierr.Parse(0, 0, r.Detail) }

// validateMatchersQuery is Loki's check of the query parameter of the routes that read only a stream selector
// (/labels, /label/<name>/values, /detected_labels, /index/stats, /index/volume(_range), /patterns). Loki
// validates the whole expression before it requires a selector, so a query the tree checks reject is answered
// with that rejection, not with `only label matchers are supported` (as Loki answers
// `invalid parameter (must be greater than 0) topk(0` there). A query the whole validation accepts is decided by
// its shape, from the same cached validation: a selector passes, anything else is not a selector. A query it
// rejects keeps the selector routes' own order (logql.ValidateMatchersQuery): Loki reports a pipeline that does
// not build after the selector rule there.
func validateMatchersQuery(query string) string {
	v := cachedQueryValidation(query)
	switch {
	case v.tree:
		return v.msg
	case v.msg == "" && v.selector:
		return ""
	case v.msg == "" && (v.sample || v.logQuery):
		return lokierr.OnlyLabelMatchers().Text()
	}
	return logqlpkg.ValidateMatchersQuery(query)
}

// validateLogSelectorQuery is Loki's check of the query parameter of /detected_fields and
// /detected_field/<name>/values: the whole expression is validated (the parser, its semantic pass and the tree
// checks), then a metric expression is refused with `only log selector is supported`.
func validateLogSelectorQuery(query string) string {
	v := cachedQueryValidation(query)
	if v.msg != "" || !v.sample {
		return v.msg
	}
	return lokierr.OnlyLogSelector().Text()
}

// emptyWhateverTheData reports a metric query Loki answers with no series whatever the data: a range aggregation
// over a zero range (`[0s]`), absent_over_time aside, selects no sample in any window, and so has no series; so has
// every expression of it that keeps only the series of its operands (a vector aggregation, label_replace, an
// arithmetic or comparison operation or `and` with such an operand, `or` of two of them, `unless` with one on the
// left). Loki's engine answers these with an empty matrix or vector (test/oracle/loki engine,
// TestZeroRangeAnswersLikeLoki); absent_over_time over a zero range answers 1 at every step.
func emptyWhateverTheData(expr logqlpkg.Expr) bool {
	switch e := expr.(type) {
	case *logqlpkg.RangeAggregation:
		return e.Op != logqlpkg.RangeAbsentOverTime && isZeroDuration(e.Range)
	case *logqlpkg.VectorAggregation:
		return emptyWhateverTheData(e.Inner)
	case *logqlpkg.OpaqueMetricExpr:
		return e.Func == "label_replace" && e.Inner != nil && emptyWhateverTheData(e.Inner)
	case *logqlpkg.BinOpExpr:
		left, right := emptyWhateverTheData(e.Left), emptyWhateverTheData(e.Right)
		switch e.Op {
		case "or":
			return left && right
		case "unless":
			return left
		}
		return left || right
	}
	return false
}

// answerEmptyResult answers a query cachedQueryValidation found empty whatever the data with Loki's empty result
// (a matrix on query_range, a vector on query), before any other work; it reports whether it answered.
func (p *Proxy) answerEmptyResult(w http.ResponseWriter, endpoint, query string, start time.Time) bool {
	if !cachedQueryValidation(query).empty {
		return false
	}
	p.writeJSON(w, emptyLimitedResponse(endpoint, query))
	p.metrics.RecordRequest(endpoint, http.StatusOK, time.Since(start))
	return true
}
