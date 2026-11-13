// Package lokierr is the typed form of the errors Loki answers its clients
// with. An Error carries the class of the failure, Loki's HTTP status, a
// stable message id, the exact text a client reads and, apart from it, the
// internal cause that only the proxy's log line may show.
//
// Errors are built where the failure happens, by the code that holds the facts
// (the parser's position, the raw request parameter, the configured limit, the
// classified backend failure), through one typed constructor per Loki message
// (messages.go). Each renders Loki's message from typed arguments, and no
// constructor reads another error's text. Two take text from the LogQL parser
// only: Parse takes the parser's detail for the failing token, and
// ParserMessage takes a message the parser has already rendered in Loki's
// words; ParserMessage is the bridge until the parser hands over the position
// and the detail apart, and nothing else may call it (internal/proxy's
// TestErrorLayerSites counts its calls).
//
// The package is a leaf: it imports only the standard library, so the syntax
// packages, the query plan and the proxy can all build its errors.
package lokierr

// Kind is the class of a failure: what went wrong, independent of the text.
type Kind uint8

const (
	// KindParse is a LogQL query Loki rejects while parsing or checking it.
	KindParse Kind = iota + 1
	// KindParam is a request parameter Loki cannot read or does not accept.
	KindParam
	// KindForm is a request URL or form body Loki cannot decode.
	KindForm
	// KindRoute is a request Loki's router answers before any handler: an
	// unknown path, a method the route does not serve, a tail request that
	// is not a websocket.
	KindRoute
	// KindLimit is a request over one of Loki's limits.
	KindLimit
	// KindQueueFull is a request the query queue has no room for.
	KindQueueFull
	// KindCanceled is a request its client cancelled.
	KindCanceled
	// KindDeadline is a request that ran out of time.
	KindDeadline
	// KindBackendFailed is a failed call to the storage behind the proxy.
	KindBackendFailed
	// KindInternal is a failure of the proxy's own work on a request.
	KindInternal
)

var kindNames = [...]string{
	KindParse:         "parse",
	KindParam:         "param",
	KindForm:          "form",
	KindRoute:         "route",
	KindLimit:         "limit",
	KindQueueFull:     "queue_full",
	KindCanceled:      "canceled",
	KindDeadline:      "deadline",
	KindBackendFailed: "backend_failed",
	KindInternal:      "internal",
}

// String is the kind's name, for logs.
func (k Kind) String() string {
	if int(k) < len(kindNames) && kindNames[k] != "" {
		return kindNames[k]
	}
	return "unknown"
}

// Shape is the body shape of an error answer. Most messages are written in
// the shape of the route that answers them (ShapeRoute); a few are written by
// Loki's router in one shape on every route.
type Shape uint8

const (
	// ShapeRoute: the route decides (its route table entry).
	ShapeRoute Shape = iota
	// ShapeFrontend: text/plain with nosniff and the message alone, as Loki's
	// query frontend writes errors.
	ShapeFrontend
	// ShapeHTTPError: Go's http.Error, text/plain with nosniff and the
	// message followed by a newline (Loki's router, compactor and ruler views).
	ShapeHTTPError
	// ShapeBodyless: the status alone, no body and no Content-Type.
	ShapeBodyless
	// ShapeFormatQuery: format_query's JSON {"status":"invalid-query","error"}.
	ShapeFormatQuery
	// ShapeRulerJSON: the ruler's Prometheus JSON with errorType.
	ShapeRulerJSON
	// ShapeAlertsJSON: the ruler's Prometheus JSON under application/json.
	ShapeAlertsJSON
)

// Error is one failure answered as Loki answers it.
type Error struct {
	msg   Msg
	text  string
	cause error
	// limitFlag and limitValue are the operator flag behind a limit or queue
	// answer and its value. They are for the log line only: no text a client
	// reads carries them.
	limitFlag  string
	limitValue int64
}

// Error is the text a client reads: Loki's message only, never the cause.
func (e *Error) Error() string { return e.text }

// Unwrap is the internal cause, for errors.Is and errors.As and for logs.
func (e *Error) Unwrap() error { return e.cause }

// Text is the exact text a client reads.
func (e *Error) Text() string { return e.text }

// Cause is the internal error behind the answer (a backend message, a Go
// error); nil when there is none. It is for the log line only.
func (e *Error) Cause() error { return e.cause }

// Msg is the stable id of Loki's message.
func (e *Error) Msg() Msg { return e.msg }

// Kind is the failure's class.
func (e *Error) Kind() Kind { return messages[e.msg].kind }

// Status is Loki's HTTP status for the failure.
func (e *Error) Status() int { return messages[e.msg].status }

// Shape is the body shape the message is always written in, or ShapeRoute
// when the answering route decides.
func (e *Error) Shape() Shape { return messages[e.msg].shape }

// Limit is the operator flag behind a limit or queue answer, as the operator
// types it (with its leading dash), and the value it is set to; ok is false when
// the answer has none. It is for the log line only.
func (e *Error) Limit() (flag string, value int64, ok bool) {
	return e.limitFlag, e.limitValue, e.limitFlag != ""
}

// WithLimit is e with the flag that sets the limit behind it and the flag's
// value, for the log line; the client text is unchanged.
func (e *Error) WithLimit(flag string, value int64) *Error {
	c := *e
	c.limitFlag, c.limitValue = flag, value
	return &c
}

// WithCause is e with cause as its internal cause; the client text is
// unchanged.
func (e *Error) WithCause(cause error) *Error {
	c := *e
	c.cause = cause
	return &c
}

// As reports the first *Error in err's tree, walked as errors.As walks it
// (Unwrap() error and Unwrap() []error, depth first). It does not call As
// methods, and it allocates nothing, which errors.As with a pointer target
// does: every failed request goes through it.
func As(err error) (*Error, bool) {
	for err != nil {
		if e, ok := err.(*Error); ok {
			return e, true
		}
		switch u := err.(type) {
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, inner := range u.Unwrap() {
				if e, ok := As(inner); ok {
					return e, true
				}
			}
			return nil, false
		default:
			return nil, false
		}
	}
	return nil, false
}

func newError(m Msg, text string, cause error) *Error {
	return &Error{msg: m, text: text, cause: cause}
}
