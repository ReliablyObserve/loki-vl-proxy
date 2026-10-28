package proxy

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// stepAlignedKey marks a query_range request whose bounds stepAlignMiddleware
// moved onto the step grid; the value is the end the client sent. Loki checks
// the resolution limit and max_query_length on the bounds the client sent,
// before its step alignment, and never again after it; the handler does not
// repeat those checks on the moved bounds.
type stepAlignedKey struct{}

func requestStepAligned(r *http.Request) bool {
	_, aligned := r.Context().Value(stepAlignedKey{}).(string)
	return aligned
}

// stepAlignedRawEnd is the end the client sent with a request whose bounds
// were moved onto the step grid. Whether a request is near now is judged on
// it (requestEndsWithin): Loki recomputes the newest
// max_cache_freshness_per_query of every request instead of serving it from
// its results cache, and a refresh whose end moved down by up to a step is
// still a request for the newest data.
func stepAlignedRawEnd(r *http.Request) (string, bool) {
	raw, ok := r.Context().Value(stepAlignedKey{}).(string)
	return raw, ok
}

// stepAlignMiddleware is Loki's step alignment under
// query_range.align_queries_with_step (-align-queries-with-step). Loki's
// query frontend runs it on metric range queries only, in the metric
// tripperware after its limits middleware and before splitting and the
// results cache (pkg/querier/queryrange/roundtrip.go:1028-1058; the
// tripperware serves SampleExpr and VariantsExpr range queries,
// roundtrip.go:419-475). Log queries, instant queries and the other
// endpoints are not aligned. Here it sits after tenantLimitsMiddleware (the
// limits middleware) and before the response cache, so a refresh that moves a
// window by less than a step becomes the same request and is answered from
// the caches, as Loki's results cache answers it.
func (p *Proxy) stepAlignMiddleware(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	if endpoint != "query_range" || !p.alignQueriesWithStep {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if aligned, ok := alignQueryRangeToStep(r, time.Now()); ok {
			r = aligned
		}
		next(w, r)
	}
}

// alignQueryRangeToStep returns r with its start and end moved down to
// multiples of its step, Loki's StepAlignMiddleware
// (pkg/querier/queryrange/queryrangebase/step_align.go:20-24):
//
//	start := (r.GetStart().UnixMilli() / r.GetStep()) * r.GetStep()
//	end := (r.GetEnd().UnixMilli() / r.GetStep()) * r.GetStep()
//
// with the step in milliseconds (the codec keeps req.Step.Milliseconds(),
// pkg/querier/queryrange/codec.go:436). Both bounds go down; the bounds are
// truncated to milliseconds first. A request without start or end is aligned
// on the bounds Loki derives for it (pkg/loghttp/params.go:91-120), which are
// written into the request.
//
// It reports false, and the request goes on as it came, when Loki would not
// align it or would reject it first:
//   - no step: Loki aligns to its derived default step, range/250 at least 1s
//     (params.go:140-142), and this proxy does not evaluate a step-less
//     request on that step;
//   - not a metric query (a log query has no evaluation grid), a query that
//     does not parse, or one with Grafana's unresolved $__ tokens;
//   - more than 11,000 points, or a start after the end: Loki answers 400 at
//     decode time, on the bounds the client sent;
//   - a Grafana query-split residual chunk (isQuerySplitResidual), which the
//     handler answers on its raw bounds.
func alignQueryRangeToStep(r *http.Request, now time.Time) (*http.Request, bool) {
	step, ok := parsePositiveStepDuration(r.FormValue("step"))
	stepMs := step.Milliseconds()
	if !ok || stepMs <= 0 {
		return r, false
	}
	query := stripIncompleteUnwrapStubs(r.FormValue("query"))
	if strings.Contains(query, "$__") {
		return r, false
	}
	if expr, err := logqlpkg.Parse(query); err != nil || !isSampleExpr(expr) {
		return r, false
	}
	if exceedsLokiRangeResolution(r, now) || isQuerySplitResidual(r) {
		return r, false
	}
	start, end, ok := lokiRangeBounds(r, now)
	if !ok || end.Before(start) {
		return r, false
	}
	alignedStart := start.UnixMilli() / stepMs * stepMs * int64(time.Millisecond)
	alignedEnd := end.UnixMilli() / stepMs * stepMs * int64(time.Millisecond)
	aligned := r.Clone(context.WithValue(r.Context(), stepAlignedKey{}, strings.TrimSpace(r.FormValue("end"))))
	_ = aligned.ParseForm()
	params := aligned.URL.Query()
	for key, value := range map[string]int64{"start": alignedStart, "end": alignedEnd} {
		encoded := strconv.FormatInt(value, 10)
		aligned.Form.Set(key, encoded)
		if aligned.Method == http.MethodPost {
			aligned.PostForm.Set(key, encoded)
		}
		params.Set(key, encoded)
	}
	aligned.URL.RawQuery = params.Encode()
	return aligned, true
}
