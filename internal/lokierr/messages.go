package lokierr

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Msg is the stable id of one Loki message. Its name (ID) is the key of the
// message in recorded Loki answers and in logs.
type Msg uint8

const (
	msgNone Msg = iota
	// MsgParse: "parse error at line L, col C: <detail>" or, with no
	// position, "parse error : <detail>".
	MsgParse
	// MsgInputTooLong: "parse error : input size too long (N > MAX)".
	MsgInputTooLong
	// MsgParamNumber: "could not parse '<name>' parameter: <strconv error>".
	MsgParamNumber
	// MsgParamDuration: "could not parse '<name>' parameter: not a valid
	// duration string: "<raw>"".
	MsgParamDuration
	// MsgNumber: a strconv error alone ("strconv.Atoi: parsing ..."), as Loki
	// answers a bad limit, or a bad time on an instant query.
	MsgNumber
	// MsgStepDuration: "cannot parse "<raw>" to a valid duration".
	MsgStepDuration
	// MsgStepOverflow: the same with ". It overflows int64".
	MsgStepOverflow
	// MsgStepNotPositive: zero or negative step.
	MsgStepNotPositive
	// MsgTooManyPoints: more than 11,000 points per series.
	MsgTooManyPoints
	// MsgEndBeforeStart: end not after start.
	MsgEndBeforeStart
	// MsgLimitNotPositive: limit zero or negative.
	MsgLimitNotPositive
	// MsgDirection: "invalid direction '<raw>'".
	MsgDirection
	// MsgIntervalNegative: "interval must be >= 0".
	MsgIntervalNegative
	// MsgAggregateBy: volume's aggregateBy is neither series nor labels.
	MsgAggregateBy
	// MsgDelayFor: tail's delay_for above its maximum.
	MsgDelayFor
	// MsgInstantLogQuery: a log query sent to the instant query endpoint.
	MsgInstantLogQuery
	// MsgOnlyLabelMatchers: a metadata endpoint given more than a selector.
	MsgOnlyLabelMatchers
	// MsgOnlyLogSelector: detected_fields given a metric query.
	MsgOnlyLogSelector
	// MsgURLEscape: "invalid URL escape "%zz"".
	MsgURLEscape
	// MsgSemicolon: a semicolon in the query string.
	MsgSemicolon
	// MsgPostTooLarge: a form body over Loki's 10 MB.
	MsgPostTooLarge
	// MsgQueryLength: max_query_length exceeded.
	MsgQueryLength
	// MsgLookback: the whole range is before max_query_lookback.
	MsgLookback
	// MsgSeriesLimit: max_query_series exceeded by a metric query.
	MsgSeriesLimit
	// MsgIndexSeriesLimit: the volume endpoints' series limit (a 500).
	MsgIndexSeriesLimit
	// MsgLimitReached: a query over a limit of its own evaluation (a 400).
	MsgLimitReached
	// MsgResponseTooLarge: a response above the message size a component
	// accepts (a 500).
	MsgResponseTooLarge
	// MsgNotFound: an unknown path.
	MsgNotFound
	// MsgMethodNotAllowed: a method the route does not serve (no body).
	MsgMethodNotAllowed
	// MsgTailNotWebSocket: a tail request without a websocket upgrade.
	MsgTailNotWebSocket
	// MsgQueueFull: the query queue is full.
	MsgQueueFull
	// MsgCanceled: the client cancelled the request.
	MsgCanceled
	// MsgDeadline: the request ran out of time.
	MsgDeadline
	// MsgBackendFailed: a failed storage call.
	MsgBackendFailed
	// MsgInternal: a failure of the proxy's own work.
	MsgInternal
	// MsgWriteNotSupported: a push to the read-only proxy.
	MsgWriteNotSupported
	msgCount
)

// StatusClientClosedRequest is the status Loki answers a request its client
// cancelled with (nginx's 499; net/http has no constant for it).
const StatusClientClosedRequest = 499

// message is one Loki message: its id, class, status and shape.
type message struct {
	id     string
	kind   Kind
	status int
	shape  Shape
}

var messages = [msgCount]message{
	msgNone:              {"none", KindInternal, http.StatusInternalServerError, ShapeRoute},
	MsgParse:             {"parse", KindParse, http.StatusBadRequest, ShapeRoute},
	MsgInputTooLong:      {"parse-input-size", KindParse, http.StatusBadRequest, ShapeRoute},
	MsgParamNumber:       {"param-number", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgParamDuration:     {"param-duration", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgNumber:            {"number", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgStepDuration:      {"step-duration", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgStepOverflow:      {"step-overflow", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgStepNotPositive:   {"step-not-positive", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgTooManyPoints:     {"step-too-many-points", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgEndBeforeStart:    {"end-before-start", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgLimitNotPositive:  {"limit-not-positive", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgDirection:         {"direction", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgIntervalNegative:  {"interval-negative", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgAggregateBy:       {"aggregate-by", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgDelayFor:          {"delay-for", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgInstantLogQuery:   {"instant-log-query", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgOnlyLabelMatchers: {"only-label-matchers", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgOnlyLogSelector:   {"only-log-selector", KindParam, http.StatusBadRequest, ShapeRoute},
	MsgURLEscape:         {"url-escape", KindForm, http.StatusBadRequest, ShapeRoute},
	MsgSemicolon:         {"url-semicolon", KindForm, http.StatusBadRequest, ShapeRoute},
	MsgPostTooLarge:      {"post-too-large", KindForm, http.StatusBadRequest, ShapeRoute},
	MsgQueryLength:       {"limit-query-length", KindLimit, http.StatusBadRequest, ShapeRoute},
	MsgLookback:          {"limit-lookback", KindLimit, http.StatusBadRequest, ShapeRoute},
	MsgSeriesLimit:       {"limit-series", KindLimit, http.StatusBadRequest, ShapeRoute},
	MsgIndexSeriesLimit:  {"limit-index-series", KindLimit, http.StatusInternalServerError, ShapeRoute},
	MsgLimitReached:      {"limit-reached", KindLimit, http.StatusBadRequest, ShapeRoute},
	MsgResponseTooLarge:  {"limit-response-size", KindLimit, http.StatusInternalServerError, ShapeRoute},
	MsgNotFound:          {"not-found", KindRoute, http.StatusNotFound, ShapeHTTPError},
	MsgMethodNotAllowed:  {"method-not-allowed", KindRoute, http.StatusMethodNotAllowed, ShapeBodyless},
	MsgTailNotWebSocket:  {"tail-not-websocket", KindRoute, http.StatusBadRequest, ShapeHTTPError},
	MsgQueueFull:         {"queue-full", KindQueueFull, http.StatusTooManyRequests, ShapeRoute},
	MsgCanceled:          {"canceled", KindCanceled, StatusClientClosedRequest, ShapeRoute},
	MsgDeadline:          {"deadline", KindDeadline, http.StatusGatewayTimeout, ShapeRoute},
	MsgBackendFailed:     {"backend-failed", KindBackendFailed, http.StatusInternalServerError, ShapeRoute},
	MsgInternal:          {"internal", KindInternal, http.StatusInternalServerError, ShapeRoute},
	MsgWriteNotSupported: {"write-not-supported", KindRoute, http.StatusMethodNotAllowed, ShapeHTTPError},
}

// ID is the message's stable name.
func (m Msg) ID() string {
	if m < msgCount {
		return messages[m].id
	}
	return messages[msgNone].id
}

// Loki's fixed texts.
const (
	textCanceled      = "the request was cancelled by the client"
	textDeadline      = "request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed"
	textQueueFull     = "too many outstanding requests"
	textInternal      = "Internal Server Error"
	textWriteBlocked  = "write operations are not supported, this is a read-only proxy"
	textNotFound      = "404 page not found"
	textTailNotWS     = "Bad Request"
	textInstantLog    = "log queries are not supported as an instant query type, please change your query to a range query type"
	textOnlyMatchers  = "only label matchers are supported"
	textOnlySelector  = "only log selector is supported"
	textStepNotPos    = "zero or negative query resolution step widths are not accepted. Try a positive integer"
	textTooManyPoints = "exceeded maximum resolution of 11,000 points per time series. Try increasing the value of the step parameter"
	textEndNotAfter   = "end timestamp must not be before or equal to start time"
	textLimitNotPos   = "limit must be a positive value"
	textIntervalNeg   = "interval must be >= 0"
	textAggregateBy   = "invalid aggregation option"
	textSemicolon     = "invalid semicolon separator in query"
	textPostTooLarge  = "http: POST too large"
	textLimitReached  = "limit reached while evaluating the query"
	textSeriesLimit   = "reached for a single query; consider reducing query cardinality by adding more specific stream selectors, reducing the time range, or aggregating results with functions like sum(), count() or topk()"
)

// Parse is a LogQL parse error at a position, as the parser reports it at
// the failing token: "parse error at line L, col C: detail". Line and col 0
// is an error with no position: "parse error : detail". detail is the
// parser's own message for the token (for example "syntax error: unexpected
// $end"), built where the parser fails.
func Parse(line, col int, detail string) *Error {
	if line == 0 && col == 0 {
		return newError(MsgParse, "parse error : "+detail, nil)
	}
	var b strings.Builder
	b.Grow(len("parse error at line , col : ") + 8 + len(detail))
	b.WriteString("parse error at line ")
	b.WriteString(strconv.Itoa(line))
	b.WriteString(", col ")
	b.WriteString(strconv.Itoa(col))
	b.WriteString(": ")
	b.WriteString(detail)
	return newError(MsgParse, b.String(), nil)
}

// ParserMessage is a parse error the LogQL parser has already rendered in
// Loki's words at the failing token (internal/logql's ParseError). It keeps
// the parser's text as it is; it is the bridge until the parser hands over
// the position and the detail apart. Only the parser's own message may be
// passed: never another error's text.
func ParserMessage(rendered string) *Error {
	return newError(MsgParse, rendered, nil)
}

// InputTooLong is a query longer than the parser accepts.
func InputTooLong(size, limit int) *Error {
	return newError(MsgInputTooLong, "parse error : input size too long ("+strconv.Itoa(size)+" > "+strconv.Itoa(limit)+")", nil)
}

// ParamNumber is a request parameter Loki failed to read as a number: name
// is the parameter, err the standard library's error for the raw value
// (strconv.ParseInt for start, end and since's number forms).
func ParamNumber(name string, err *strconv.NumError) *Error {
	return newError(MsgParamNumber, "could not parse '"+name+"' parameter: "+err.Error(), nil)
}

// ParamDuration is a parameter Loki failed to read as a duration (since).
func ParamDuration(name, raw string) *Error {
	return newError(MsgParamDuration, "could not parse '"+name+"' parameter: not a valid duration string: "+strconv.Quote(raw), nil)
}

// Number is the standard library's error for a raw number alone, as Loki
// answers a bad limit (strconv.Atoi) or the time of an instant query
// (strconv.ParseInt).
func Number(err *strconv.NumError) *Error {
	return newError(MsgNumber, err.Error(), nil)
}

// StepDuration is a step that is neither a number nor a duration.
func StepDuration(raw string) *Error {
	return newError(MsgStepDuration, "cannot parse "+strconv.Quote(raw)+" to a valid duration", nil)
}

// StepOverflow is a step in seconds too large for int64 nanoseconds.
func StepOverflow(raw string) *Error {
	return newError(MsgStepOverflow, "cannot parse "+strconv.Quote(raw)+" to a valid duration. It overflows int64", nil)
}

// StepNotPositive is a zero or negative step.
func StepNotPositive() *Error { return newError(MsgStepNotPositive, textStepNotPos, nil) }

// TooManyPoints is a range and step that give more than 11,000 points.
func TooManyPoints() *Error { return newError(MsgTooManyPoints, textTooManyPoints, nil) }

// EndBeforeStart is an end before or equal to the start.
func EndBeforeStart() *Error { return newError(MsgEndBeforeStart, textEndNotAfter, nil) }

// LimitNotPositive is a zero or negative limit.
func LimitNotPositive() *Error { return newError(MsgLimitNotPositive, textLimitNotPos, nil) }

// Direction is a direction other than forward or backward; raw is as sent.
func Direction(raw string) *Error {
	return newError(MsgDirection, "invalid direction '"+raw+"'", nil)
}

// IntervalNegative is a negative interval.
func IntervalNegative() *Error { return newError(MsgIntervalNegative, textIntervalNeg, nil) }

// AggregateBy is an aggregateBy other than series or labels.
func AggregateBy() *Error { return newError(MsgAggregateBy, textAggregateBy, nil) }

// DelayFor is a tail delay_for above limit seconds.
func DelayFor(limit int) *Error {
	return newError(MsgDelayFor, "delay_for can't be greater than "+strconv.Itoa(limit), nil)
}

// InstantLogQuery is a log query sent to the instant query endpoint.
func InstantLogQuery() *Error { return newError(MsgInstantLogQuery, textInstantLog, nil) }

// OnlyLabelMatchers is a metadata endpoint given more than a stream selector.
func OnlyLabelMatchers() *Error { return newError(MsgOnlyLabelMatchers, textOnlyMatchers, nil) }

// OnlyLogSelector is detected_fields given something other than a log query.
func OnlyLogSelector() *Error { return newError(MsgOnlyLogSelector, textOnlySelector, nil) }

// URLEscape is a query string with an invalid percent escape: err is the
// standard library's error for it.
func URLEscape(err url.EscapeError) *Error {
	return newError(MsgURLEscape, err.Error(), nil)
}

// Semicolon is a query string holding a semicolon.
func Semicolon() *Error { return newError(MsgSemicolon, textSemicolon, nil) }

// PostTooLarge is a form body over 10 MB.
func PostTooLarge() *Error { return newError(MsgPostTooLarge, textPostTooLarge, nil) }

// QueryLength is a range longer than max_query_length: length as a Go
// duration, limit as a Prometheus duration.
func QueryLength(length, limit time.Duration) *Error {
	return newError(MsgQueryLength, "the query time range exceeds the limit (query length: "+length.String()+", limit: "+FormatDuration(limit)+")", nil)
}

// Lookback is a range wholly before now minus max_query_lookback.
func Lookback(limit time.Duration) *Error {
	return newError(MsgLookback, "this data is no longer available, it is past now - max_query_lookback ("+FormatDuration(limit)+")", nil)
}

// SeriesLimitOf is SeriesLimit for the proxy's own limit: flag is the proxy
// flag that sets it, set to limit, and cause says what was counted, both for the
// log line only. It builds the error in one allocation.
func SeriesLimitOf(flag string, limit int, cause error) *Error {
	e := SeriesLimit(limit)
	e.limitFlag, e.limitValue, e.cause = flag, int64(limit), cause
	return e
}

// SeriesLimit is a metric query over max_query_series.
func SeriesLimit(limit int) *Error {
	return newError(MsgSeriesLimit, "maximum number of series ("+strconv.Itoa(limit)+") "+textSeriesLimit, nil)
}

// IndexSeriesLimit is the volume endpoints' series limit, which Loki answers
// with 500.
func IndexSeriesLimit(limit int) *Error {
	return newError(MsgIndexSeriesLimit, "rpc error: code = Unknown desc = the query hit the max number of series limit (limit: "+strconv.Itoa(limit)+" series)", nil)
}

// LimitReached is a query that went over a limit of its own evaluation: the
// class of Loki's logqlmodel.ErrLimit, answered 400 with its fixed text. flag
// is the proxy flag that sets the limit and value its setting, for the log
// line only; cause says what was counted, for the log line only.
func LimitReached(flag string, value int64, cause error) *Error {
	e := newError(MsgLimitReached, textLimitReached, cause)
	e.limitFlag, e.limitValue = flag, value
	return e
}

// ResponseTooLarge is a response above the message size a component accepts,
// as Loki's querier answers one over its gRPC message limit: 500 with the
// ResourceExhausted text. read is the size seen when the read stopped (at
// least limit+1 bytes), limit the bound; flag is the proxy flag that sets it
// and cause says what was too large, both for the log line only.
func ResponseTooLarge(flag string, read, limit int64, cause error) *Error {
	size, bound := strconv.FormatInt(read, 10), strconv.FormatInt(limit, 10)
	e := newError(MsgResponseTooLarge, "rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max ("+size+" vs. "+bound+")", cause)
	e.limitFlag, e.limitValue = flag, limit
	return e
}

// NotFound is an unknown path, as Loki's router answers it.
func NotFound() *Error { return newError(MsgNotFound, textNotFound, nil) }

// MethodNotAllowed is a method the route does not serve: the status alone.
func MethodNotAllowed() *Error { return newError(MsgMethodNotAllowed, "", nil) }

// TailNotWebSocket is a tail request that is not a websocket upgrade.
func TailNotWebSocket() *Error { return newError(MsgTailNotWebSocket, textTailNotWS, nil) }

// WriteNotSupported is a push to the proxy, which only reads. Loki's own push
// answers (204, or 400 for a body it cannot read) have no read-only
// counterpart, so the proxy answers the status of a method the route does not
// serve, in the shape Loki writes its push errors in (http.Error's), and says
// why.
func WriteNotSupported() *Error { return newError(MsgWriteNotSupported, textWriteBlocked, nil) }

// QueueFull is a request the query queue has no room for; cause says which
// queue, for the log.
func QueueFull(cause error) *Error { return newError(MsgQueueFull, textQueueFull, cause) }

// QueueFullAt is QueueFull for one of the proxy's own queues and rate limits:
// flag is the proxy flag that sets the limit, set to value, for the log line
// only. It builds the error in one allocation.
func QueueFullAt(flag string, value int64, cause error) *Error {
	e := newError(MsgQueueFull, textQueueFull, cause)
	e.limitFlag, e.limitValue = flag, value
	return e
}

// Canceled is a request its client cancelled.
func Canceled(cause error) *Error { return newError(MsgCanceled, textCanceled, cause) }

// Deadline is a request that ran out of time.
func Deadline(cause error) *Error { return newError(MsgDeadline, textDeadline, cause) }

// BackendFailed is a failed storage call: cause is the backend's failure,
// for the log only.
func BackendFailed(cause error) *Error { return newError(MsgBackendFailed, textInternal, cause) }

// Internal is a failure of the proxy's own work on a request.
func Internal(cause error) *Error { return newError(MsgInternal, textInternal, cause) }

// FormatDuration writes d as a Prometheus duration, the form Loki prints
// its configured limits in: units from years to milliseconds, years and
// weeks only when they divide d exactly (90d, not 12w6d), "0s" for zero.
func FormatDuration(d time.Duration) string {
	ms := int64(d / time.Millisecond)
	if ms == 0 {
		return "0s"
	}
	var b strings.Builder
	if ms < 0 {
		b.WriteByte('-')
		ms = -ms
	}
	for _, u := range durationUnits {
		if u.exact && ms%u.ms != 0 {
			continue
		}
		if v := ms / u.ms; v > 0 {
			b.WriteString(strconv.FormatInt(v, 10))
			b.WriteString(u.name)
			ms -= v * u.ms
		}
	}
	return b.String()
}

var durationUnits = [...]struct {
	name  string
	ms    int64
	exact bool
}{
	{"y", 1000 * 60 * 60 * 24 * 365, true},
	{"w", 1000 * 60 * 60 * 24 * 7, true},
	{"d", 1000 * 60 * 60 * 24, false},
	{"h", 1000 * 60 * 60, false},
	{"m", 1000 * 60, false},
	{"s", 1000, false},
	{"ms", 1, false},
}
