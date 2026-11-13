package proxy

import (
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// Request decoding and parameter checks, in the order Loki makes them.
//
// A request is read in three steps, each before the response cache and before
// any VictoriaLogs call:
//
//  1. decodeRequest parses the URL and the form body once (Go's own parse, the
//     one the handlers read from afterwards) and answers what Loki answers when
//     it cannot: an invalid percent escape, a semicolon, a body over 10 MB.
//  2. The route's checks (requestParamChecks), in the order recorded for it,
//     read the raw parameters the way Loki reads them and answer its 400 text
//     for the first that is invalid. Each check builds a typed internal/lokierr
//     error from the failing input; none rewrites another error's text.
//  3. The query itself (the selector rules of lokiQueryParamValidation, the
//     handlers' syntax parse) follows, so a bad parameter is reported before a
//     bad query, as Loki does. Tail reads its query first.
//
// The order of two checks is Loki's only where a recorded request shows it
// (error-layer rows r1566-r1601): a request carrying both errors answers the
// earlier one. The pairs no recorded request separates keep the order listed
// below, and are marked unrecorded in conformance case
// semantics/error-request-validation-parity.

// lokiMaxFormBytes is the largest form body Loki reads.
const lokiMaxFormBytes = 10 << 20

// lokiDefaultSince is the window a range request without a start covers.
const lokiDefaultSince = time.Hour

// lokiDelayForMax is the largest delay_for a tail accepts, in seconds.
const lokiDelayForMax = 5

// paramCheck is one step of a route's validation.
type paramCheck uint8

const (
	ckSince         paramCheck = iota + 1 // since: a Prometheus duration
	ckEnd                                 // end: a time, default now
	ckStart                               // start: a time, default the end minus since
	ckTime                                // time (instant query): a time, default now
	ckLimitPositive                       // limit: an integer above zero
	ckLimitSigned                         // limit: an integer, zero allowed
	ckDirection                           // direction: forward or backward, any case
	ckStep                                // step: float seconds or a duration, above zero
	ckInterval                            // interval: float seconds or a duration, not negative
	ckAggregateBy                         // aggregateBy: series or labels
	ckDelayFor                            // delay_for: an integer from 0 to 5
	ckEndAfterStart                       // the end is not before the start
	ckResolution                          // at most 11,000 points per series
	ckLength                              // max_query_length
)

// requestParamChecks is the ordered checks of every route that reads
// parameters (the endpoint name it registers with). A route that is not listed
// has none.
//
// What is checked on a route is what a recorded Loki request shows it
// checking: a parameter the recorded answers accept in every form is not
// checked (patterns and series ignore limit; labels, label values and series
// do not check end against start; the instant query ignores step and interval;
// detected_labels and detected field values do not read since).
var requestParamChecks = map[string][]paramCheck{
	"query_range":           {ckSince, ckEnd, ckStart, ckLimitPositive, ckDirection, ckStep, ckInterval, ckEndAfterStart, ckResolution},
	"query":                 {ckLimitPositive, ckDirection, ckTime},
	"labels":                {ckSince, ckEnd, ckStart, ckLength},
	"label_values":          {ckSince, ckEnd, ckStart, ckLength},
	"series":                {ckSince, ckEnd, ckStart, ckLength},
	"detected_labels":       {ckEnd, ckStart, ckLength},
	"index_stats":           {ckSince, ckEnd, ckStart, ckEndAfterStart, ckLength},
	"volume":                {ckSince, ckEnd, ckStart, ckLimitSigned, ckAggregateBy, ckEndAfterStart, ckLength},
	"volume_range":          {ckSince, ckEnd, ckStart, ckLimitSigned, ckAggregateBy, ckStep, ckEndAfterStart, ckResolution, ckLength},
	"patterns":              {ckSince, ckEnd, ckStart, ckStep, ckResolution},
	"detected_fields":       {ckSince, ckEnd, ckStart, ckLimitPositive, ckStep, ckEndAfterStart, ckLength},
	"detected_field_values": {ckEnd, ckStart, ckLimitPositive, ckLength},
	"tail":                  {ckLimitPositive, ckDelayFor, ckEnd, ckStart},
}

// paramScan is what the checks of one request share: the request, the clock
// read once, and the values an earlier check parsed for a later one.
type paramScan struct {
	r        *http.Request
	now      time.Time
	since    time.Duration
	end      time.Time
	start    time.Time
	step     time.Duration
	haveStep bool
}

// formValue is the first value of a parameter of the parsed form, as
// http.Request.FormValue reads it, without parsing again.
func formValue(r *http.Request, name string) string {
	if v := r.Form[name]; len(v) > 0 {
		return v[0]
	}
	return ""
}

// requestParamError is the first check of route's list that fails for r, whose
// form is parsed; nil when they all pass. A route without checks passes.
func (p *Proxy) requestParamError(checks []paramCheck, r *http.Request, now time.Time) *lokierr.Error {
	s := paramScan{r: r, now: now, since: lokiDefaultSince}
	for _, c := range checks {
		if err := p.runParamCheck(c, &s); err != nil {
			return err
		}
	}
	return nil
}

// runParamCheck runs one check.
func (p *Proxy) runParamCheck(c paramCheck, s *paramScan) *lokierr.Error {
	r := s.r
	switch c {
	case ckSince:
		return s.checkSince(formValue(r, "since"))
	case ckEnd:
		return s.checkEnd(formValue(r, "end"))
	case ckStart:
		return s.checkStart(formValue(r, "start"))
	case ckTime:
		return s.checkTime(formValue(r, "time"))
	case ckLimitPositive:
		return checkLimit(formValue(r, "limit"), 1)
	case ckLimitSigned:
		return checkLimit(formValue(r, "limit"), 0)
	case ckDirection:
		return checkDirection(formValue(r, "direction"))
	case ckStep:
		return s.checkStep(formValue(r, "step"))
	case ckInterval:
		return checkInterval(formValue(r, "interval"))
	case ckAggregateBy:
		return checkAggregateBy(formValue(r, "aggregateBy"))
	case ckDelayFor:
		return checkDelayFor(formValue(r, "delay_for"))
	case ckEndAfterStart:
		if s.end.Before(s.start) {
			return lokierr.EndBeforeStart()
		}
	case ckResolution:
		if s.haveStep && s.end.Sub(s.start)/s.step > lokiMaxPointsPerSeries {
			return lokierr.TooManyPoints()
		}
	case ckLength:
		return p.checkQueryLength(s)
	}
	return nil
}

// checkSince reads since, the length of the window a request without a start
// covers.
func (s *paramScan) checkSince(raw string) *lokierr.Error {
	if raw == "" {
		return nil
	}
	d, ok := promDuration(raw)
	if !ok {
		return lokierr.ParamDuration("since", raw)
	}
	s.since = d
	return nil
}

// checkEnd reads end; a request without one ends now.
func (s *paramScan) checkEnd(raw string) *lokierr.Error {
	s.end = s.now
	if raw == "" {
		return nil
	}
	t, numErr := lokiTimeParam(raw)
	if numErr != nil {
		return lokierr.ParamNumber("end", numErr)
	}
	s.end = t
	return nil
}

// checkStart reads start; a request without one starts since before its end
// (before now, for an end in the future).
func (s *paramScan) checkStart(raw string) *lokierr.Error {
	if raw == "" {
		base := s.end
		if base.After(s.now) {
			base = s.now
		}
		s.start = base.Add(-s.since)
		return nil
	}
	t, numErr := lokiTimeParam(raw)
	if numErr != nil {
		return lokierr.ParamNumber("start", numErr)
	}
	s.start = t
	return nil
}

// checkTime reads the time of an instant query: Loki's answer for a bad one is
// the number parse's own error, with no parameter name.
func (s *paramScan) checkTime(raw string) *lokierr.Error {
	if raw == "" {
		return nil
	}
	if _, numErr := lokiTimeParam(raw); numErr != nil {
		return lokierr.Number(numErr)
	}
	return nil
}

// checkLimit reads limit, an integer of at least least (an empty one is Loki's
// default).
func checkLimit(raw string, least int) *lokierr.Error {
	if raw == "" {
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) {
			return lokierr.Number(numErr)
		}
		return lokierr.LimitNotPositive()
	}
	if n < least {
		return lokierr.LimitNotPositive()
	}
	return nil
}

// checkDirection reads direction: forward or backward in any case.
func checkDirection(raw string) *lokierr.Error {
	if raw == "" || strings.EqualFold(raw, "forward") || strings.EqualFold(raw, "backward") {
		return nil
	}
	return lokierr.Direction(raw)
}

// checkStep reads step: float seconds or a duration, above zero. A request
// without one gets the step Loki derives from the range.
func (s *paramScan) checkStep(raw string) *lokierr.Error {
	if raw == "" {
		return nil
	}
	d, err := lokiStepParam(raw)
	if err != nil {
		return err
	}
	if d <= 0 {
		return lokierr.StepNotPositive()
	}
	s.step, s.haveStep = d, true
	return nil
}

// checkInterval reads interval, read like a step but not negative: zero keeps
// every entry.
func checkInterval(raw string) *lokierr.Error {
	if raw == "" {
		return nil
	}
	d, err := lokiStepParam(raw)
	if err != nil {
		return err
	}
	if d < 0 {
		return lokierr.IntervalNegative()
	}
	return nil
}

// checkAggregateBy reads aggregateBy of the volume endpoints.
func checkAggregateBy(raw string) *lokierr.Error {
	if raw == "" || raw == "series" || raw == "labels" {
		return nil
	}
	return lokierr.AggregateBy()
}

// checkDelayFor reads a tail's delay_for: whole seconds from 0 to 5.
func checkDelayFor(raw string) *lokierr.Error {
	if raw == "" {
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) {
			return lokierr.Number(numErr)
		}
		return lokierr.DelayFor(lokiDelayForMax)
	}
	if n < 0 || n > lokiDelayForMax {
		return lokierr.DelayFor(lokiDelayForMax)
	}
	return nil
}

// checkQueryLength is max_query_length: a range longer than the tenant's limit
// fails with Loki's text. With max_query_lookback set the start counts from the
// lookback, and a range wholly before it is answered empty by the limits
// middleware, which this check leaves alone.
func (p *Proxy) checkQueryLength(s *paramScan) *lokierr.Error {
	orgID := ""
	if v := s.r.Header["X-Scope-Orgid"]; len(v) > 0 { // the canonical key: Header.Get allocates it
		orgID = v[0]
	}
	limits := p.queryLimitsFor(orgID)
	if limits.MaxQueryLength <= 0 {
		return nil
	}
	start := s.start
	if limits.MaxQueryLookback > 0 {
		minStart := s.now.Add(-limits.MaxQueryLookback)
		if s.end.Before(minStart) {
			return nil
		}
		if start.Before(minStart) {
			start = minStart
		}
	}
	return queryLengthTypedError(limits.MaxQueryLength, start, s.end)
}

// queryLengthTypedError is Loki's max_query_length error for the range
// [start, end], or nil when it fits. Loki compares whole milliseconds.
func queryLengthTypedError(maxLen time.Duration, start, end time.Time) *lokierr.Error {
	if maxLen <= 0 || !end.After(start) {
		return nil
	}
	ms := end.UnixMilli() - start.UnixMilli()
	length := time.Duration(math.MaxInt64)
	if ms < math.MaxInt64/int64(time.Millisecond) {
		length = time.Duration(ms) * time.Millisecond
	}
	if length > maxLen {
		return lokierr.QueryLength(length, maxLen)
	}
	return nil
}

// lokiTimeParam reads a start, end or time parameter as Loki does: an integer
// is seconds up to ten characters and nanoseconds beyond, a number with a
// decimal point is float seconds rounded to the millisecond, and anything else
// is an RFC 3339 time. The value is not trimmed. A value that is none of these
// fails with the standard library's integer parse error for it, whose text is
// Loki's.
func lokiTimeParam(raw string) (time.Time, *strconv.NumError) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err == nil {
		if len(raw) <= 10 {
			return time.Unix(n, 0), nil
		}
		return time.Unix(0, n), nil
	}
	var numErr *strconv.NumError
	if !errors.As(err, &numErr) {
		return time.Time{}, &strconv.NumError{Func: "ParseInt", Num: raw, Err: strconv.ErrSyntax}
	}
	if strings.IndexByte(raw, '.') >= 0 {
		if f, ferr := strconv.ParseFloat(raw, 64); ferr == nil && math.Abs(f) < math.MaxInt64 {
			whole, fraction := math.Modf(f)
			fraction = math.Round(fraction*1000) / 1000
			return time.Unix(int64(whole), int64(fraction*float64(time.Second))), nil
		}
	}
	if t, terr := time.Parse(time.RFC3339Nano, raw); terr == nil {
		return t, nil
	}
	return time.Time{}, numErr
}

// lokiStepParam reads a step or interval as Loki does: float seconds, else a
// Prometheus duration. The duration may be zero or negative (the caller says
// which it accepts); an unreadable one gives Loki's text, and one whose
// nanoseconds overflow int64 its overflow text.
func lokiStepParam(raw string) (time.Duration, *lokierr.Error) {
	if d, ok := promDuration(raw); ok {
		return d, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, lokierr.StepDuration(raw)
	}
	ns := f * float64(time.Second)
	switch {
	case math.IsNaN(ns):
		return 0, nil
	case ns >= math.MaxInt64:
		return 0, lokierr.StepOverflow(raw)
	case ns <= math.MinInt64:
		return math.MinInt64, nil
	}
	return time.Duration(ns), nil
}

// promDuration reads s as Prometheus reads a duration: whole numbers of years,
// weeks, days, hours, minutes, seconds and milliseconds, each at most once and
// in that order ("1h30m", "2d"), or "0". It is not Go's grammar: "1ns", "1h1h",
// "1.5h" and a sign are not durations. ok is false for anything else and for
// a duration beyond int64 nanoseconds.
func promDuration(s string) (time.Duration, bool) {
	if s == "0" {
		return 0, true
	}
	if s == "" {
		return 0, false
	}
	var total int64 // milliseconds
	prev := -1
	for s != "" {
		i := 0
		var n int64
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			if n > (math.MaxInt64-9)/10 {
				return 0, false
			}
			n = n*10 + int64(s[i]-'0')
			i++
		}
		if i == 0 || i == len(s) {
			return 0, false
		}
		unit, width := promUnit(s[i:])
		if width == 0 || unit <= prev {
			return 0, false
		}
		prev = unit
		ms := promUnitMillis[unit]
		if n > (math.MaxInt64-total)/ms {
			return 0, false
		}
		total += n * ms
		s = s[i+width:]
	}
	if total > math.MaxInt64/int64(time.Millisecond) {
		return 0, false
	}
	return time.Duration(total) * time.Millisecond, true
}

// promUnitMillis is the milliseconds of each unit promUnit numbers, in the
// order a duration lists them.
var promUnitMillis = [...]int64{
	365 * 24 * 3600 * 1000, // y
	7 * 24 * 3600 * 1000,   // w
	24 * 3600 * 1000,       // d
	3600 * 1000,            // h
	60 * 1000,              // m
	1000,                   // s
	1,                      // ms
}

// promUnit is the unit a duration's text starts with (its number in
// promUnitMillis) and the length of its name, or width 0 for none.
func promUnit(s string) (unit, width int) {
	switch s[0] {
	case 'y':
		return 0, 1
	case 'w':
		return 1, 1
	case 'd':
		return 2, 1
	case 'h':
		return 3, 1
	case 'm':
		if len(s) > 1 && s[1] == 's' {
			return 6, 2
		}
		return 4, 1
	case 's':
		return 5, 1
	}
	return 0, 0
}

// decodeForm parses the request's URL and form body once, before anything else
// reads a parameter, and returns Loki's 400 for a request it cannot decode: an
// invalid percent escape, a semicolon, or a form body over 10 MB (the read is
// bounded). Every later reader of the request uses the form this parse built,
// so it runs where the request first reads one (requestLogger).
func decodeForm(w http.ResponseWriter, r *http.Request) *lokierr.Error {
	if r.Body != nil && r.Body != http.NoBody && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
		r.Body = http.MaxBytesReader(w, r.Body, lokiMaxFormBytes)
	}
	if err := r.ParseForm(); err != nil {
		return formDecodeError(r, err)
	}
	return nil
}

// requestDecodes reports whether a route decodes its request as Loki's query
// frontend does (the routes that read parameters).
func requestDecodes(endpoint string) bool {
	_, checked := requestParamChecks[endpoint]
	return checked
}

// formDecodeError is Loki's error for a request whose URL or form body does not
// decode, from the error of http.Request.ParseForm; nil for an error that is
// not a decode failure (the client went away mid-body).
func formDecodeError(r *http.Request, err error) *lokierr.Error {
	var escape url.EscapeError
	var tooLarge *http.MaxBytesError
	var netErr net.Error
	switch {
	case errors.As(err, &escape):
		return lokierr.URLEscape(escape)
	case errors.As(err, &tooLarge):
		return lokierr.PostTooLarge()
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF), errors.As(err, &netErr), r.Context().Err() != nil:
		return nil
	}
	// url.ParseQuery fails for an invalid escape or for a semicolon.
	return lokierr.Semicolon()
}

// requestParamValidation answers a request whose parameters Loki rejects with
// Loki's 400, in Loki's order, before the cache, the plan or any VictoriaLogs
// call. It runs before the query is read, except for tail, which reads its
// query first. It returns false when it answered.
func (p *Proxy) requestParamValidation(w http.ResponseWriter, r *http.Request, endpoint string, checks []paramCheck) bool {
	if len(checks) == 0 {
		return true
	}
	if err := p.requestParamError(checks, r, time.Now()); err != nil {
		p.metrics.RecordRequest(endpoint, p.fail(w, r, err), 0)
		return false
	}
	return true
}
