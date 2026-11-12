package proxy

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Bounds checks for request times and offsets.
//
// The handlers hold every time as int64 nanoseconds since the Unix epoch, which
// reaches from 1677-09-21 to 2262-04-11. A request whose window, moved by the
// query's offset and widened by its range, would leave that range cannot be
// read back from the numbers the handlers compute with: they would wrap to the
// other end of the range. Loki keeps such a time as a time.Time and, where its
// own arithmetic reaches the same limits, answers something that is not a
// contract (timestamps that wrapped, a 500 "unexpected empty result", no answer
// at all; see testdata/loki_time_bounds.jsonl). The proxy refuses them with a
// 400 that names its limit, apart from an integer time beyond int64, where
// Loki's own 400 is repeated.
//
// The accepted range keeps timeBoundsMargin away from both ends of int64, so a
// handler may still add a rounding, a millisecond or a minute to a time it
// accepted. A step is not part of the check: the evaluation loops count their
// steps without wrapping, and the bounds are read after the step alignment has
// moved them.

const (
	// timeBoundsMargin is the distance from either end of int64 nanoseconds that
	// an accepted time, with its offset and range applied, keeps.
	timeBoundsMargin = 15 * time.Minute

	// minRequestTimeNs and maxRequestTimeNs are 1677-09-21T00:27:43Z and
	// 2262-04-11T23:32:16Z.
	minRequestTimeNs = math.MinInt64 + int64(timeBoundsMargin)
	maxRequestTimeNs = math.MaxInt64 - int64(timeBoundsMargin)

	// errRequestTimeOutOfRange is the proxy's own 400 for a window outside the
	// accepted range: Loki has no stable answer for one.
	errRequestTimeOutOfRange = "the time range of the query, moved by its offset, must lie between 1677-09-21 and 2262-04-11"
)

// addNs and subNs are int64 addition and subtraction that report an overflow.
func addNs(a, b int64) (int64, bool) {
	sum := a + b
	return sum, (b >= 0) == (sum >= a)
}

func subNs(a, b int64) (int64, bool) {
	diff := a - b
	return diff, (b >= 0) == (diff <= a)
}

// shiftNs is a - b, saturated at the ends of int64 instead of wrapped.
func shiftNs(a, b int64) int64 {
	if diff, ok := subNs(a, b); ok {
		return diff
	}
	if b > 0 {
		return math.MinInt64
	}
	return math.MaxInt64
}

// stepCount is the number of evaluation times start, start+step, ... that do
// not pass end: 0 for a step that is not positive or a start after the end. It
// is computed on the unsigned distance, which end-start overflows for a start
// far below zero and an end far above it. The time of index i, start+i*step,
// is end or less for every i below the count, and wraps harmlessly to the right
// value in int64 arithmetic where i*step alone would not fit. The count is
// capped at MaxInt64.
func stepCount(start, end, step int64) int64 {
	if step <= 0 || end < start {
		return 0
	}
	n := (uint64(end) - uint64(start)) / uint64(step)
	if n >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(n) + 1
}

// halfOpenStepCount is stepCount for the times start, start+step, ... below end.
func halfOpenStepCount(start, end, step int64) int64 {
	if end <= start {
		return 0
	}
	return stepCount(start, end-1, step)
}

// queryShift is how far the range selectors of a query move the window it reads
// and how far they widen it: the offsets are the smallest and largest over the
// selectors (0 counts for a selector without one), the range the longest.
type queryShift struct {
	offsetMin, offsetMax, rangeMax time.Duration
}

func (s *queryShift) add(offset, window time.Duration) {
	// (A test file of this package declares its own min and max for int, so
	// the builtins are not used on durations.)
	if offset < s.offsetMin {
		s.offsetMin = offset
	}
	if offset > s.offsetMax {
		s.offsetMax = offset
	}
	if window > s.rangeMax {
		s.rangeMax = window
	}
}

// queryShiftOf reads the offsets and ranges from the parsed query (its range
// selectors carry them as durations), through the vector aggregations, binary
// operations and the functions kept as typed expressions. A function kept only
// as text is read from that text.
func queryShiftOf(expr logqlpkg.Expr) queryShift {
	var s queryShift
	var visit func(logqlpkg.Expr)
	visit = func(e logqlpkg.Expr) {
		switch n := e.(type) {
		case *logqlpkg.RangeAggregation:
			s.add(n.OffsetDuration, n.RangeDuration)
		case *logqlpkg.VectorAggregation:
			visit(n.Inner)
		case *logqlpkg.BinOpExpr:
			visit(n.Left)
			visit(n.Right)
		case *logqlpkg.OpaqueMetricExpr:
			if n.Typed && n.Inner != nil {
				visit(n.Inner)
				return
			}
			for _, m := range logqlOffsetRE.FindAllStringSubmatch(n.Raw, -1) {
				s.add(parseLokiDuration(m[1]), 0)
			}
			for _, m := range opaqueRangeSelectorRE.FindAllStringSubmatch(n.Raw, -1) {
				s.add(0, parseLokiDuration(m[1]))
			}
		}
	}
	visit(expr)
	return s
}

// windowFits reports whether the window [start, end] read by a query with the
// given shift stays inside [minRequestTimeNs, maxRequestTimeNs]: the window
// itself, moved back by the largest offset (forward by a negative one),
// widened back by the longest range, and moved by the smallest offset. Every
// sum is checked for overflow.
func (s queryShift) windowFits(start, end int64) bool {
	lo, ok := subNs(start, int64(s.offsetMax))
	if !ok {
		return false
	}
	if lo, ok = subNs(lo, int64(s.rangeMax)); !ok {
		return false
	}
	hi, ok := subNs(end, int64(s.offsetMin))
	if !ok {
		return false
	}
	return start >= minRequestTimeNs && end <= maxRequestTimeNs && lo >= minRequestTimeNs && hi <= maxRequestTimeNs
}

// boundState says what requestBound made of a start, end or time parameter.
type boundState int

const (
	boundAbsent     boundState = iota // not sent
	boundRead                         // read as nanoseconds
	boundUnreadable                   // not a time the proxy reads: the handlers answer for it
	boundOutOfRange                   // a time that int64 nanoseconds cannot hold
	boundBeyondInt                    // an integer beyond int64: Loki's own 400
)

// requestBound reads a time parameter the way the handlers read it
// (lokiBoundUnixNano, parseLokiTimeToUnixNano), and tells a time that cannot be
// held in int64 nanoseconds from one that is merely not a time, which stays
// with the handlers. For boundBeyondInt the error is strconv's, the text of
// Loki's 400.
func requestBound(raw string, now time.Time) (int64, boundState, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, boundAbsent, nil
	}
	if isIntegerBound(raw) {
		if ns, ok := integerBoundUnixNano(raw); ok {
			return ns, boundRead, nil
		}
		if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
			return 0, boundBeyondInt, err
		}
		return 0, boundOutOfRange, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		if ns, ok := unixNanoIfHeld(t); ok {
			return ns, boundRead, nil
		}
		return 0, boundOutOfRange, nil
	}
	if strings.HasPrefix(raw, "now-") || strings.HasPrefix(raw, "now+") {
		d, err := time.ParseDuration(raw[4:])
		if err != nil {
			return 0, boundUnreadable, nil
		}
		if raw[3] == '-' {
			d = -d
		}
		// now moved by up to 292 years can still leave int64 nanoseconds.
		if ns, ok := unixNanoIfHeld(now.Add(d)); ok {
			return ns, boundRead, nil
		}
		return 0, boundOutOfRange, nil
	}
	if raw == "now" {
		return now.UnixNano(), boundRead, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, boundUnreadable, nil
	}
	abs := math.Abs(f)
	unit := float64(time.Second)
	switch {
	case abs >= 1e17:
		unit = 1
	case abs >= 1e14:
		unit = float64(time.Microsecond)
	case abs >= 1e11:
		unit = float64(time.Millisecond)
	}
	if math.IsNaN(abs) || abs*unit >= math.MaxInt64 {
		return 0, boundOutOfRange, nil
	}
	if ns, ok := lokiBoundUnixNano(raw); ok {
		return ns, boundRead, nil
	}
	return 0, boundOutOfRange, nil
}

// timeBoundsWindow is the time window of a query or query_range request, with
// the proxy's defaults (end is now, start an hour before it) for a bound that
// is not sent. ok is false when the request carries something the handlers
// answer for themselves (a time that is not a time, a start after the end), and
// msg is the 400 of a time outside the representable range.
func timeBoundsWindow(endpoint string, r *http.Request, now time.Time) (start, end int64, ok bool, msg string) {
	nowNs := now.UnixNano()
	read := func(name string) (ns int64, st boundState, text string) {
		raw := r.FormValue(name)
		ns, st, err := requestBound(raw, now)
		switch st {
		case boundBeyondInt:
			text = err.Error()
			if endpoint == "query_range" {
				text = "could not parse '" + name + "' parameter: " + text
			}
		case boundOutOfRange:
			text = errRequestTimeOutOfRange
		}
		return ns, st, text
	}
	if endpoint == "query" {
		ns, st, text := read("time")
		switch st {
		case boundAbsent:
			ns = nowNs
		case boundRead:
		case boundUnreadable:
			return 0, 0, false, ""
		default:
			return 0, 0, false, text
		}
		return ns, ns, true, ""
	}
	// Loki reads end before start (pkg/loghttp/params.go determineBounds).
	end, endState, text := read("end")
	switch endState {
	case boundAbsent:
		end = nowNs
	case boundRead:
	case boundUnreadable:
		return 0, 0, false, ""
	default:
		return 0, 0, false, text
	}
	start, startState, text := read("start")
	switch startState {
	case boundAbsent:
		since := time.Hour
		if raw := strings.TrimSpace(r.FormValue("since")); raw != "" {
			if d, parsed := parsePositiveStepDuration(raw); parsed {
				since = d
			}
		}
		start = shiftNs(min64(end, nowNs), int64(since))
	case boundRead:
	case boundUnreadable:
		return 0, 0, false, ""
	default:
		return 0, 0, false, text
	}
	return start, end, start <= end, ""
}

// requestTimeBoundsError is the 400 message of a query or query_range request
// whose window, moved and widened by its query, leaves the representable time
// range, or "". A request the handlers answer for themselves gives "".
func requestTimeBoundsError(endpoint string, r *http.Request, now time.Time) string {
	var shift queryShift
	if q := r.FormValue("query"); q != "" {
		v := cachedQueryValidation(q)
		if endpoint == "query" && v.msg == "" && !v.sample {
			return "" // Loki refuses a log query at the instant endpoint first
		}
		shift = v.shift
	}
	start, end, ok, msg := timeBoundsWindow(endpoint, r, now)
	if msg != "" {
		return msg
	}
	if ok && !shift.windowFits(start, end) {
		return errRequestTimeOutOfRange
	}
	return ""
}

// requestTimeBounds refuses a query or query_range request whose time window
// leaves the representable range, before the handler reads it. It stands behind
// the response cache of the compat layer, whose key holds the request's exact
// parameters: a cached answer is the answer to a request that passed this check,
// so a hit costs nothing more.
func (p *Proxy) requestTimeBounds(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	if endpoint != "query" && endpoint != "query_range" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if msg := requestTimeBoundsError(endpoint, r, time.Now()); msg != "" {
			p.writeError(w, http.StatusBadRequest, msg)
			p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
			return
		}
		next(w, r)
	}
}

// movedBound is a time moved back by offset, written for the parameters the
// handlers read: integer nanoseconds where those read back as themselves, RFC
// 3339 where they would not (before 1973 an integer is read as another unit).
// A move that leaves int64 saturates.
func movedBound(ns int64, offset time.Duration) string {
	return encodeAlignedBound(shiftNs(ns, int64(offset)))
}
