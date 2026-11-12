package proxy

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
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
// roundtrip.go:419-475). Log queries, instant queries, volume and the
// metadata endpoints are not aligned. Loki's pattern tripperware aligns
// /loki/api/v1/patterns too (roundtrip.go:263-285); the proxy does not: its
// pattern samples are already bucketed on the step grid, and patterns
// autodetect warms them under the log query's bounds. The flag defaults to
// on. Here it sits after tenantLimitsMiddleware (the
// limits middleware) and before the response cache, so a refresh that moves a
// window by less than a step becomes the same request and is answered from
// the caches, as Loki's results cache answers it.
func (p *Proxy) stepAlignMiddleware(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	if endpoint != "query_range" || !p.alignQueriesWithStep {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if !boundsOnStepGrid(r) {
			if aligned, ok := alignQueryRangeToStep(r, time.Now()); ok {
				r = aligned
			}
		}
		next(w, r)
	}
}

// boundsOnStepGrid reports whether the request carries a step and both a
// start and an end that alignment would leave where they are: each decodes to
// a multiple of the step as the proxy reads it (lokiBoundUnixNano, which reads
// a decimal as Loki does). Such a request goes on as it came,
// without parsing its query or copying it, as on a build without alignment.
// A bound that is missing is derived (end defaults to now) and is aligned.
func boundsOnStepGrid(r *http.Request) bool {
	step, ok := stepWithoutAllocating(r.FormValue("step"))
	gridNs := step.Milliseconds() * int64(time.Millisecond)
	if !ok || gridNs <= 0 {
		return false
	}
	for _, key := range [...]string{"start", "end"} {
		raw := strings.TrimSpace(r.FormValue(key))
		if raw == "" {
			return false
		}
		ns, ok := lokiBoundUnixNano(raw)
		if !ok || ns%gridNs != 0 {
			return false
		}
	}
	return true
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
	// No metric expression starts with a stream selector: a log query is
	// left without a lookup, which costs a parse once the memo is full.
	if strings.HasPrefix(strings.TrimSpace(query), "{") {
		return r, false
	}
	// The handler validates the same query string through the same memo
	// (validateQuery), so a query is parsed once across both, and a repeated
	// query not at all.
	if !cachedQueryValidation(query).sample {
		return r, false
	}
	if exceedsLokiRangeResolution(r, now) || isQuerySplitResidual(r) {
		return r, false
	}
	start, end, ok := lokiRangeBounds(r, now)
	if !ok || end.Before(start) {
		return r, false
	}
	alignedStartMs := start.UnixMilli() / stepMs * stepMs
	alignedEndMs := end.UnixMilli() / stepMs * stepMs
	// Loki aligns on time.Time in milliseconds; the proxy writes the moved
	// bounds as int64 nanoseconds, which hold 1677-09-21 to 2262-04-11 only.
	// A bound outside that (an end near either limit, or the start Loki
	// derives an hour before such an end) would wrap to the other end of the
	// range, so the request goes on as it came.
	if !millisFitNanos(alignedStartMs) || !millisFitNanos(alignedEndMs) {
		return r, false
	}
	alignedStart := encodeAlignedBound(alignedStartMs * int64(time.Millisecond))
	alignedEnd := encodeAlignedBound(alignedEndMs * int64(time.Millisecond))
	// A shallow copy, as the rest of the chain passes requests on: only the
	// form, the URL and the context differ from r, and r's are not written.
	aligned := r.WithContext(context.WithValue(r.Context(), stepAlignedKey{}, strings.TrimSpace(r.FormValue("end"))))
	aligned.Form = withAlignedBounds(r.Form, alignedStart, alignedEnd)
	if r.Method == http.MethodPost {
		aligned.PostForm = withAlignedBounds(r.PostForm, alignedStart, alignedEnd)
	}
	u := *r.URL
	u.RawQuery = rawQueryWithBounds(r.URL.RawQuery, alignedStart, alignedEnd)
	aligned.URL = &u
	return aligned, true
}

// withAlignedBounds is a copy of values with start and end replaced; the
// other entries share their slices with values, which is not written.
func withAlignedBounds(values url.Values, start, end string) url.Values {
	out := make(url.Values, len(values)+2)
	for key, value := range values {
		out[key] = value
	}
	out["start"] = []string{start}
	out["end"] = []string{end}
	return out
}

// rawQueryWithBounds is rawQuery with the values of start and end replaced in
// place and every other parameter left as the client wrote it, so a moved
// request has the cache key of the same request sent on the grid. A bound
// that is absent is appended. A query string that names start or end more
// than once, or spells a key with escapes, is re-encoded whole instead.
func rawQueryWithBounds(rawQuery, start, end string) string {
	var b strings.Builder
	b.Grow(len(rawQuery) + len(start) + len(end) + len("&start=&end="))
	seenStart, seenEnd := false, false
	for rest := rawQuery; rest != ""; {
		var part string
		part, rest, _ = strings.Cut(rest, "&")
		if part == "" {
			continue
		}
		key, _, _ := strings.Cut(part, "=")
		if strings.ContainsAny(key, "%+;") {
			return reencodeWithBounds(rawQuery, start, end)
		}
		if b.Len() > 0 {
			b.WriteByte('&')
		}
		switch {
		case key == "start" && !seenStart:
			seenStart = true
			b.WriteString("start=")
			b.WriteString(url.QueryEscape(start))
		case key == "end" && !seenEnd:
			seenEnd = true
			b.WriteString("end=")
			b.WriteString(url.QueryEscape(end))
		case key == "start" || key == "end":
			return reencodeWithBounds(rawQuery, start, end)
		default:
			b.WriteString(part)
		}
	}
	for _, missing := range [...]struct {
		seen       bool
		key, value string
	}{{seenStart, "start", start}, {seenEnd, "end", end}} {
		if !missing.seen {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(missing.key)
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(missing.value))
		}
	}
	return b.String()
}

// reencodeWithBounds is rawQueryWithBounds for a query string it does not
// rewrite in place: parsed, the bounds set, and encoded again.
func reencodeWithBounds(rawQuery, start, end string) string {
	params, _ := url.ParseQuery(rawQuery)
	params.Set("start", start)
	params.Set("end", end)
	return params.Encode()
}

// lokiBoundUnixNano parses a start or end as parseLokiTimeToUnixNano does, for
// the two forms Grafana and API clients send without first trying the RFC3339
// layouts, whose failures allocate: an integer (integerBoundUnixNano), and a
// number with a decimal point below 1e11, seconds whose fraction is rounded to
// milliseconds (pkg/loghttp/params.go:166-171 parseTimestamp) by
// lokiFloatSecondsToNanos, the one reader of that form. Everything else goes
// through parseLokiTimeToUnixNano.
//
// ok is false for a bound in either form that int64 nanoseconds cannot hold
// (before 1677-09-21 or after 2262-04-11): Loki keeps such a bound as a
// time.Time, the proxy's readers would wrap it to the other end of the range.
func lokiBoundUnixNano(raw string) (int64, bool) {
	if isIntegerBound(raw) {
		return integerBoundUnixNano(raw)
	}
	if strings.Contains(raw, ".") {
		if seconds, err := strconv.ParseFloat(raw, 64); err == nil && math.Abs(seconds) < 1e11 {
			if !lokiFloatSecondsFitNanos(seconds) {
				return 0, false
			}
			return lokiFloatSecondsToNanos(seconds), true
		}
	}
	return parseLokiTimeToUnixNano(raw)
}

// lokiFloatSecondsFitNanos reports whether lokiFloatSecondsToNanos(seconds)
// is the instant Loki reads, within int64 nanoseconds.
func lokiFloatSecondsFitNanos(seconds float64) bool {
	whole, fraction := math.Modf(seconds)
	fraction = math.Round(fraction*1000) / 1000
	t := time.Unix(int64(whole), int64(fraction*float64(time.Second)))
	return !t.Before(time.Unix(0, math.MinInt64)) && !t.After(time.Unix(0, math.MaxInt64))
}

// stepWithoutAllocating is parsePositiveStepDuration with a whole number of
// seconds read directly: time.ParseDuration's error for a step without a unit
// allocates. Up to nine digits, n seconds is exactly what the float path
// computes.
func stepWithoutAllocating(raw string) (time.Duration, bool) {
	if raw != "" && len(raw) <= 9 && strings.Trim(raw, "0123456789") == "" {
		n, _ := strconv.Atoi(raw)
		return time.Duration(n) * time.Second, n > 0
	}
	return parsePositiveStepDuration(raw)
}

// integerBoundUnixNano reads an integer bound, the form Grafana sends, as
// parseLokiTimeToUnixNano does (by its magnitude), without first trying the
// RFC3339 layouts, whose failures allocate.
//
// ok is false when the integer read by its magnitude (seconds, milliseconds,
// microseconds) is outside int64 nanoseconds: normalizeLokiIntTimeToUnixNano
// would wrap it.
func integerBoundUnixNano(raw string) (int64, bool) {
	if !isIntegerBound(raw) {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return integerNanosIfHeld(n)
}

// integerNanosIfHeld reads an integer time by its magnitude (seconds below
// 1e11, then milliseconds, then microseconds, then nanoseconds), or reports
// false when that reading is outside int64 nanoseconds, which
// normalizeLokiIntTimeToUnixNano would wrap.
func integerNanosIfHeld(n int64) (int64, bool) {
	if n == math.MinInt64 {
		return 0, false // its absolute value overflows, so its magnitude cannot be read
	}
	abs := n
	if abs < 0 {
		abs = -abs
	}
	unit := int64(1)
	switch {
	case abs < 1e11:
		unit = int64(time.Second)
	case abs < 1e14:
		unit = int64(time.Millisecond)
	case abs < 1e17:
		unit = int64(time.Microsecond)
	}
	if abs > math.MaxInt64/unit {
		return 0, false
	}
	return normalizeLokiIntTimeToUnixNano(n), true
}

// floatNanosIfHeld reads a decimal time by its magnitude as
// normalizeLokiNumericTimeToUnixNano does, or reports false for one that int64
// nanoseconds cannot hold (the conversion of such a float is undefined and
// differs between architectures).
func floatNanosIfHeld(v float64) (int64, bool) {
	abs := math.Abs(v)
	if math.IsNaN(v) {
		return 0, false
	}
	unit := 1.0
	switch {
	case abs < 1e11:
		if !lokiFloatSecondsFitNanos(v) {
			return 0, false
		}
		return lokiFloatSecondsToNanos(v), true
	case abs < 1e14:
		unit = float64(time.Millisecond)
	case abs < 1e17:
		unit = float64(time.Microsecond)
	}
	if abs*unit >= float64(math.MaxInt64) {
		return 0, false
	}
	return normalizeLokiNumericTimeToUnixNano(v), true
}

// isIntegerBound reports whether raw is a decimal integer with an optional
// sign.
func isIntegerBound(raw string) bool {
	digits := strings.TrimPrefix(strings.TrimPrefix(raw, "-"), "+")
	if digits == "" || len(digits) < len(raw)-1 {
		return false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	return true
}

// millisFitNanos reports whether ms milliseconds can be written as int64
// nanoseconds without overflowing.
func millisFitNanos(ms int64) bool {
	return ms >= math.MinInt64/int64(time.Millisecond) && ms <= math.MaxInt64/int64(time.Millisecond)
}

// encodeAlignedBound writes a moved bound as integer nanoseconds, the form
// Grafana sends. The proxy reads an integer by its magnitude (seconds below
// 1e11, then milliseconds, microseconds), so a time before 1973 written as
// nanoseconds would be read back as another time; such a bound is written in
// RFC3339 instead, which every parser reads exactly.
func encodeAlignedBound(ns int64) string {
	encoded := strconv.FormatInt(ns, 10)
	if back, ok := integerBoundUnixNano(encoded); ok && back == ns {
		return encoded
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
}
