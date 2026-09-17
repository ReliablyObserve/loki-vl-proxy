package proxy

import (
	"net/http"
	"net/url"
	"strconv"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// alignRangeRequestToStepGrid rewrites a metric range request's start and end
// onto Loki's evaluation grid: multiples of `step`, both bounds truncated DOWN.
//
// This is what Loki's queryrange step-align middleware does when
// `query_range.align_queries_with_step` is set, and it is visible from the
// outside. On 3.7.1 with step=137s and a start that is not a multiple of 137,
// every returned timestamp satisfies `ts % 137 == 0` and the first point lands
// 17 seconds BEFORE the requested start. Starting the grid at `start` instead
// shifts the whole series by `start mod step` — 17s at step=137 and 19s at
// step=97 for start=1700000000 — while a step that happens to divide the start
// looks perfectly fine, which is why the mismatch only shows up on odd steps.
//
// LOG queries are left alone: they have no evaluation grid, and moving their
// bounds would change which lines they return.
//
// Two places Loki aligns and this does not, both because aligning them here
// would align to a step nothing downstream uses. A request with no `step` is
// left untouched: Loki computes max(floor(range/250), 1s) at decode time and
// aligns to that, while this proxy's range evaluator falls back to 1m, so the
// two would disagree either way. `/loki/api/v1/patterns` is left untouched for
// the same reason: Loki runs it through the same middleware (patternConfig
// inherits align_queries_with_step in roundtrip.go), but its default step is
// range/250 against this proxy's range/120.
func alignRangeRequestToStepGrid(r *http.Request, logqlQuery string) {
	if r == nil {
		return
	}
	// parseStepToNanos, not parsePositiveStepDuration: Grafana sometimes sends
	// the step as a raw nanosecond integer, which the latter rejects. Aligning
	// only the other spellings would leave those panels on the unaligned grid
	// while their neighbours moved.
	stepNs, ok := parseStepToNanos(r.FormValue("step"))
	if !ok || stepNs <= 0 {
		return
	}
	if !isMetricRangeExpr(logqlQuery) {
		return
	}

	aligned := func(raw string) (string, bool) {
		ns, ok := parseLokiTimeToUnixNano(raw)
		if !ok {
			return "", false
		}
		rem := ns % stepNs
		if rem < 0 {
			rem += stepNs
		}
		if rem == 0 {
			return "", false
		}
		return strconv.FormatInt(ns-rem, 10), true
	}

	setForm := func(key, value string) {
		if r.Form == nil {
			r.Form = url.Values{}
		}
		r.Form.Set(key, value)
		if r.PostForm != nil && r.PostForm.Get(key) != "" {
			r.PostForm.Set(key, value)
		}
		if r.URL != nil {
			q := r.URL.Query()
			if q.Get(key) != "" {
				q.Set(key, value)
				r.URL.RawQuery = q.Encode()
			}
		}
	}

	if v, ok := aligned(r.FormValue("start")); ok {
		setForm("start", v)
	}
	if v, ok := aligned(r.FormValue("end")); ok {
		setForm("end", v)
	}
}

// isMetricRangeExpr reports whether a LogQL query produces a matrix — the only
// shape Loki evaluates on a step grid.
func isMetricRangeExpr(logqlQuery string) bool {
	parsed, err := logqlpkg.Parse(logqlQuery)
	if err != nil {
		return false
	}
	switch parsed.(type) {
	case *logqlpkg.RangeAggregation, *logqlpkg.VectorAggregation, *logqlpkg.BinOpExpr,
		*logqlpkg.OpaqueMetricExpr, *logqlpkg.LiteralExpr:
		return true
	}
	return false
}
