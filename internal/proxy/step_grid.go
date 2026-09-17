package proxy

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// lokiRangeBounds returns the start and end of a range request as Loki decodes
// them (pkg/loghttp.ParseRangeQuery): `end` defaults to now, `start` to
// min(end, now) minus `since` (1h). It fails on a bound that does not parse and
// on a `since` that does not.
func lokiRangeBounds(r *http.Request, now time.Time) (start, end time.Time, ok bool) {
	end = now
	if raw := strings.TrimSpace(r.FormValue("end")); raw != "" {
		ns, ok := parseLokiTimeToUnixNano(raw)
		if !ok {
			return time.Time{}, time.Time{}, false
		}
		end = time.Unix(0, ns)
	}
	if raw := strings.TrimSpace(r.FormValue("start")); raw != "" {
		ns, ok := parseLokiTimeToUnixNano(raw)
		if !ok {
			return time.Time{}, time.Time{}, false
		}
		return time.Unix(0, ns), end, true
	}
	since := time.Hour
	if raw := strings.TrimSpace(r.FormValue("since")); raw != "" {
		var ok bool
		if since, ok = parsePositiveStepDuration(raw); !ok {
			return time.Time{}, time.Time{}, false
		}
	}
	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}
	return endOrNow.Add(-since), end, true
}

// alignDownToStep truncates a unix-nanosecond timestamp down to the nearest
// multiple of step, including a timestamp before the epoch.
func alignDownToStep(ns, stepNs int64) int64 {
	rem := ns % stepNs
	if rem < 0 {
		rem += stepNs
	}
	return ns - rem
}

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
// A request without `start` or `end` is aligned on the bounds Loki derives for
// it (end=now, start=end-since), which are written into the request so every
// downstream path evaluates the same grid.
//
// LOG queries are left alone: they have no evaluation grid, and moving their
// bounds would change which lines they return. The query has passed
// validateQuery by the time this runs, so it parses.
//
// Two places Loki aligns and this does not, both because aligning them here
// would align to a step nothing downstream uses. A request with no `step` is
// left untouched: Loki computes max(floor(range/250), 1s) at decode time and
// aligns to that, while this proxy's range evaluator falls back to 1m, so the
// two would disagree either way. `/loki/api/v1/patterns` is left untouched for
// the same reason: Loki runs it through the same middleware (patternConfig
// inherits align_queries_with_step in roundtrip.go), but its default step is
// range/250 against this proxy's range/120. Both are listed in
// docs/compatibility-loki.md.
func alignRangeRequestToStepGrid(r *http.Request, logqlQuery string, now time.Time) {
	if r == nil {
		return
	}
	// The same parser as the 11,000-point check and the range evaluators, so
	// the grid, the limit and the evaluation agree on what one step is. That
	// includes the raw nanosecond integer Grafana sometimes sends, a proxy
	// extension Loki does not have (Loki reads a bare integer as seconds).
	stepNs, ok := parseStepToNanos(r.FormValue("step"))
	if !ok || stepNs <= 0 {
		return
	}
	if !isMetricRangeExpr(logqlQuery) {
		return
	}
	start, end, ok := lokiRangeBounds(r, now)
	if !ok {
		return
	}

	setForm := func(key string, value int64) {
		encoded := strconv.FormatInt(value, 10)
		if r.Form == nil {
			r.Form = url.Values{}
		}
		r.Form.Set(key, encoded)
		if r.PostForm != nil && r.PostForm.Get(key) != "" {
			r.PostForm.Set(key, encoded)
		}
		if r.URL != nil {
			q := r.URL.Query()
			q.Set(key, encoded)
			r.URL.RawQuery = q.Encode()
		}
	}

	if aligned := alignDownToStep(start.UnixNano(), stepNs); aligned != start.UnixNano() || r.FormValue("start") == "" {
		setForm("start", aligned)
	}
	if aligned := alignDownToStep(end.UnixNano(), stepNs); aligned != end.UnixNano() || r.FormValue("end") == "" {
		setForm("end", aligned)
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
