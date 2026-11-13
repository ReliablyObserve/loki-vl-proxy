package proxy

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// What a client reads when the proxy cannot answer a query it accepted.
//
// A query passes validation, so the proxy's LogQL parser accepts it. If the
// translation, the plan or the evaluation then fails, the failure is the
// proxy's: its text (the translator's wording, a Go error, a LogsQL fragment)
// never reaches the client. The client reads what Loki answers for the same
// request, from typed facts the failing code holds:
//
//   - Loki rejects the query with a message the proxy can build from the AST
//     or the request (`parameter required for operation quantile_over_time`,
//     `invalid aggregation sum_over_time without unwrap`): that message.
//   - Loki rejects it with a position only the parser has (`parse error at
//     line 1, col 14: syntax error: unexpected $end`): Loki's status and its
//     position-free syntax error, until the parser reports the position
//     (unplacedSyntaxError).
//   - Loki accepts it (the proxy has no way to serve the construct yet): Loki's
//     fixed `500 Internal Server Error` (notServed). The construct is named in
//     the log line, and a registry case holds the gap open.
//
// The internal reason is the error's cause, written to the log line only.

// notServedError is the internal cause of an answer for a query Loki accepts
// and the proxy cannot serve yet: the construct, for the log line, and the
// failure the proxy hit.
type notServedError struct {
	construct string
	cause     error
}

// Error is the reason for the log line: the failure the proxy hit, or the
// construct when there is none (the construct has an attribute of its own).
func (e *notServedError) Error() string {
	if e.cause == nil {
		return e.construct
	}
	return e.cause.Error()
}

func (e *notServedError) Unwrap() error { return e.cause }

// notServed is the client error for a query Loki accepts and the proxy cannot
// serve yet: Loki's fixed 500, with construct (a short name, such as
// "vector-aggregation-over-scalar") and cause in the log line only.
func notServed(construct string, cause error) *lokierr.Error {
	return lokierr.Internal(&notServedError{construct: construct, cause: cause})
}

// constructOf is the construct a failure names (empty when it is no notServedError).
func constructOf(err error) string {
	if ns, ok := asType[*notServedError](err); ok {
		return ns.construct
	}
	return ""
}

// asType is errors.As for a concrete error type, without reflection and without
// an allocation: every failed request meets it. It walks err's tree as errors.As
// does (Unwrap() error and Unwrap() []error, depth first).
func asType[T error](err error) (T, bool) {
	var zero T
	for err != nil {
		if v, ok := err.(T); ok {
			return v, true
		}
		switch u := err.(type) {
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, inner := range u.Unwrap() {
				if v, ok := asType[T](inner); ok {
					return v, true
				}
			}
			return zero, false
		default:
			return zero, false
		}
	}
	return zero, false
}

// proxyFailure is the client error for a failure of the proxy's own work on
// a request (a body it cannot decode, an evaluation that fails): Loki's fixed
// 500. The cause is for the log line only.
func proxyFailure(cause error) *lokierr.Error { return lokierr.Internal(cause) }

// unplacedSyntaxDetail is the detail of the position-free syntax error the
// proxy answers where Loki's own error carries a line and a column the proxy
// does not have: Loki's text for a query that ends where a token is missing.
const unplacedSyntaxDetail = "syntax error: unexpected $end"

// unplacedSyntaxError is the client error for a query Loki rejects with a
// positioned syntax error the proxy cannot place. Status and class are
// Loki's (400, a parse error); the position is missing until the parser can
// report it. cause is the failure the proxy hit, for the log line only.
func unplacedSyntaxError(cause error) *lokierr.Error {
	return unplacedSyntax.WithCause(cause)
}

// unplacedSyntax is the error unplacedSyntaxError answers, built once.
var unplacedSyntax = lokierr.Parse(0, 0, unplacedSyntaxDetail)

// queryRefusal is the client error for a query that passed validation and
// that the translation (or the plan, or the lowering) refused. The class is
// read from the failure's type, never from its text:
//
//   - translator.UnsupportedError (the translator has no LogsQL for a construct
//     the parser accepts): Loki accepts what the translator refuses at its
//     vector aggregations over scalars and at an unwrap it cannot read
//     (conformance cases vector-aggregation-over-scalar-rejected and
//     translator-reads-only-space-between-tokens), so a notServed answer;
//     count_values is no LogQL function at all, so a syntax error;
//   - every other failure (translator.ParseError, an invalid stage the parser
//     read as a label filter): a query Loki rejects with a position, an
//     unplaced syntax error.
func queryRefusal(err error) *lokierr.Error {
	if typed, ok := lokierr.As(err); ok {
		return typed
	}
	if unsupported, ok := asType[*translator.UnsupportedError](err); ok && unsupported.Func != "count_values" {
		return notServed(unsupported.Func, err)
	}
	return unplacedSyntaxError(err)
}

// bareMetricRefusal is the client error for a bare metric function query the
// translator turned into no stats query. Loki rejects a quantile_over_time
// without its quantile with `parameter required for operation
// quantile_over_time` (no position: the check follows the parse); any other
// function reaching here is a construct the proxy cannot serve.
func bareMetricRefusal(expr logqlpkg.Expr) *lokierr.Error {
	if ra, ok := expr.(*logqlpkg.RangeAggregation); ok && ra.Op == logqlpkg.RangeQuantileOverTime && !ra.HasParam {
		return lokierr.Parse(0, 0, "parameter required for operation "+string(ra.Op))
	}
	return notServed("bare-metric-function", nil)
}

// withoutUnwrapError is Loki's rejection of a range aggregation that needs an
// unwrap and has none: `parse error : invalid aggregation <function> without
// unwrap`.
func withoutUnwrapError(function string) *lokierr.Error {
	return lokierr.Parse(0, 0, "invalid aggregation "+function+" without unwrap")
}

// rangeWindowRefusal is the client error for a range metric whose range the
// proxy cannot read as a positive duration. Loki accepts a zero range and
// answers an empty result (a construct the proxy cannot serve yet), and
// rejects a range too large for a duration with `duration out of range`
// (Loki adds the position).
func rangeWindowRefusal(window string) *lokierr.Error {
	if isZeroDuration(window) {
		return notServed("zero-range", nil)
	}
	return durationOutOfRange
}

// durationOutOfRange is the error rangeWindowRefusal answers for a range too large for a duration, built once.
var durationOutOfRange = lokierr.Parse(0, 0, "duration out of range")

// isZeroDuration reports a duration literal whose every number is zero ("0s",
// "0m0s"); the literal is empty or not a duration when it holds anything else.
func isZeroDuration(literal string) bool {
	seen := false
	for i := 0; i < len(literal); i++ {
		switch c := literal[i]; {
		case c == '0':
			seen = true
		case c >= '1' && c <= '9':
			return false
		case c >= 'a' && c <= 'z':
		default:
			return false
		}
	}
	return seen
}

// timestampParamError is the client error for a start or end parameter the
// proxy cannot read as a time: Loki's `could not parse '<name>' parameter:
// <strconv error>`, the error of the same strconv.ParseInt on the raw value.
func timestampParamError(name, raw string) *lokierr.Error {
	if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
		if numErr, ok := err.(*strconv.NumError); ok {
			return lokierr.ParamNumber(name, numErr)
		}
	}
	return notServed("time-parameter", errors.New(name+" parameter "+strconv.Quote(raw)))
}

// offsetError is why extractLogQLOffset cannot apply one time shift to a
// query: some of its range vectors carry an offset and others do not (mixed),
// or they carry different offsets (distinct). Its text is for the log line.
type offsetError struct {
	msg   string
	mixed bool
}

func (e *offsetError) Error() string { return e.msg }

// isMixedOffsetError reports the failure of a query whose range vectors do
// not all carry an offset (a top-level binary expression is evaluated side by
// side, each with its own, as Loki does).
func isMixedOffsetError(err error) bool {
	oe, ok := asType[*offsetError](err)
	return ok && oe.mixed
}

// offsetRefusal is the client error for a query whose offsets the proxy
// cannot apply as one time shift. Loki evaluates each range vector with its
// own offset, so the query is one Loki accepts and the proxy cannot serve yet.
func offsetRefusal(err error) *lokierr.Error {
	if isMixedOffsetError(err) {
		return notServed("mixed-offsets", err)
	}
	return notServed("distinct-offsets", err)
}

// labelReplaceRegexError is Loki's rejection of a label_replace whose regex
// does not compile: `parse error : invalid regex in label_replace: <the
// regexp package's error>`, from compiling the same pattern Loki does.
func labelReplaceRegexError(err error) *lokierr.Error {
	return lokierr.Parse(0, 0, "invalid regex in label_replace: "+err.Error()).WithCause(err)
}

// stepParamError is the client error for a step the proxy cannot read as a
// positive duration: Loki's `zero or negative query resolution step widths are
// not accepted. Try a positive integer` for a number or duration that is not
// positive, and `cannot parse "<raw>" to a valid duration` otherwise.
func stepParamError(raw string) *lokierr.Error {
	text := strings.TrimSpace(raw)
	if f, err := strconv.ParseFloat(text, 64); err == nil && f <= 0 {
		return lokierr.StepNotPositive()
	}
	if d, err := time.ParseDuration(text); err == nil && d <= 0 {
		return lokierr.StepNotPositive()
	}
	return lokierr.StepDuration(raw)
}

// rangeParamsError is the client error for a range query whose start, end or
// step the proxy cannot read, in Loki's order of checks: end, then start, then
// step; okStart and okEnd tell which of the first two was read, and the step is the one left.
func rangeParamsError(r *http.Request, okStart, okEnd bool) *lokierr.Error {
	switch {
	case !okEnd:
		return timestampParamError("end", r.FormValue("end"))
	case !okStart:
		return timestampParamError("start", r.FormValue("start"))
	}
	return stepParamError(r.FormValue("step"))
}

// instantTimeError is the client error for the time of an instant query the
// proxy cannot read: Loki answers the strconv error of the raw value alone
// (`strconv.ParseInt: parsing "abc": invalid syntax`).
func instantTimeError(raw string) *lokierr.Error {
	if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
		if numErr, ok := err.(*strconv.NumError); ok {
			return lokierr.Number(numErr)
		}
	}
	return notServed("time-parameter", errors.New("time parameter "+strconv.Quote(raw)))
}

// failUnparseableQuery answers a query the parser rejects at a point the
// request validation has not run: the validator's message (Loki's own text for
// the failing token), or Loki's syntax error without the position when the
// validator accepts what the parser rejects.
func (p *Proxy) failUnparseableQuery(w http.ResponseWriter, r *http.Request, parseErr error) {
	if msg := validateLogQLSyntax(r.FormValue("query")); msg != "" {
		p.writeError(w, http.StatusBadRequest, truncateQueryError(msg))
		return
	}
	p.fail(w, r, unplacedSyntaxError(parseErr))
}
