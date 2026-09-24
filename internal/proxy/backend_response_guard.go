package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"
)

// lokiErrDeadlineExceeded is Loki's server.ErrDeadlineExceeded
// (pkg/util/server/error.go, v3.7.7), answered with HTTP 504 whenever a query
// runs past its deadline.
const lokiErrDeadlineExceeded = "request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed"

// labelValuesResponseTooLargeError is Loki's answer to a label values
// response above the querier's grpc_server_max_send_msg_size: HTTP 500 with
// the gRPC ResourceExhausted text. The proxy's bound is
// -label-values-max-response-bytes (per tenant label_values_max_response_bytes)
// on the bytes read from VictoriaLogs; the read stops at the limit, so read is
// the bytes seen when it stopped, not the size of the whole response.
type labelValuesResponseTooLargeError struct {
	read  int64
	limit int64
}

func (e *labelValuesResponseTooLargeError) Error() string {
	return fmt.Sprintf("rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (%d vs. %d); raise -label-values-max-response-bytes or narrow the query", e.read, e.limit)
}

func isLabelValuesResponseTooLarge(err error) bool {
	var tooLarge *labelValuesResponseTooLargeError
	return errors.As(err, &tooLarge)
}

// vlResponseAbortedError is a VictoriaLogs response whose body failed after
// the status line had been sent. VictoriaLogs ends a response this way when a
// query outlives its deadline (the smaller of the timeout argument and
// -search.maxQueryDuration): it writes the error into the body, then hijacks
// the connection, writes a raw "the connection has been aborted" line and
// closes it. Go's HTTP client then reports "chunked line ends with bare LF",
// io.ErrUnexpectedEOF or a decompression error, none of which a client should
// see.
type vlResponseAbortedError struct {
	elapsed  time.Duration
	budget   time.Duration // timeout argument sent to VictoriaLogs; 0 = none
	timedOut bool
}

func (e *vlResponseAbortedError) Error() string {
	if e.timedOut {
		return lokiErrDeadlineExceeded
	}
	return fmt.Sprintf("VictoriaLogs aborted the response after %s, before it was complete; it ends a response this way when a query exceeds -search.maxQueryDuration or fails while its result is being written", e.elapsed.Round(time.Millisecond))
}

// vlDeadlineMarker is the start of the error VictoriaLogs writes into an
// already started response when the query deadline passes
// (app/vlselect/main.go logRequestErrorIfNeeded).
var vlDeadlineMarker = []byte("couldn't be executed in")

type metadataResponseCapKey struct{}

// withLabelValuesResponseCap bounds the bytes read from each VictoriaLogs
// response of a label values request.
func withLabelValuesResponseCap(ctx context.Context, limit int) context.Context {
	if limit <= 0 {
		return ctx
	}
	return context.WithValue(ctx, metadataResponseCapKey{}, int64(limit))
}

// guardBackendBody wraps a VictoriaLogs response body: a body that fails
// after the headers becomes a vlResponseAbortedError, and a label values
// response above its cap stops being read with a
// labelValuesResponseTooLargeError. Either error reaches the caller in place
// of a partial body, so nothing partial is decoded, indexed or cached.
func (p *Proxy) guardBackendBody(ctx context.Context, body io.ReadCloser, path string, params url.Values, start time.Time) io.ReadCloser {
	if body == nil {
		return body
	}
	g := &backendBodyGuard{ReadCloser: body, p: p, ctx: ctx, path: path, start: start}
	if d, err := time.ParseDuration(params.Get("timeout")); err == nil {
		g.budget = d
	}
	if limit, ok := ctx.Value(metadataResponseCapKey{}).(int64); ok {
		g.limit = limit
	}
	return g
}

type backendBodyGuard struct {
	io.ReadCloser
	p      *Proxy
	ctx    context.Context
	path   string
	start  time.Time
	budget time.Duration
	limit  int64
	read   int64
	tail   [256]byte
	tailN  int
	err    error
}

// BodyLimit lets the request coalescer read up to this guard's cap when it is
// above the coalescer's own default.
func (g *backendBodyGuard) BodyLimit() int64 { return g.limit }

func (g *backendBodyGuard) Read(b []byte) (int, error) {
	if g.err != nil {
		return 0, g.err
	}
	if g.limit > 0 {
		// Read at most one byte past the cap, so the proxy never holds more
		// than the cap of a response it rejects.
		if room := g.limit - g.read + 1; int64(len(b)) > room {
			b = b[:room]
		}
	}
	n, err := g.ReadCloser.Read(b)
	g.read += int64(n)
	g.keepTail(b[:n])
	if g.limit > 0 && g.read > g.limit {
		g.err = &labelValuesResponseTooLargeError{read: g.read, limit: g.limit}
		g.p.observeInternalOperation(g.ctx, "label_values_response_cap", "rejected", time.Since(g.start))
		g.p.log.Warn("label values response exceeds -label-values-max-response-bytes",
			"backend.route", g.path, "bytes_read", g.read, "limit", g.limit, "limit_flag", "-label-values-max-response-bytes")
		return n, g.err
	}
	if err == nil || err == io.EOF {
		return n, err
	}
	if g.ctx.Err() == context.Canceled {
		// The client went away; keep the cancellation as it is.
		return n, err
	}
	elapsed := time.Since(g.start)
	aborted := &vlResponseAbortedError{elapsed: elapsed, budget: g.budget}
	aborted.timedOut = g.ctx.Err() == context.DeadlineExceeded ||
		(g.budget > 0 && elapsed >= g.budget-g.budget/20) ||
		bytes.Contains(g.tail[:g.tailN], vlDeadlineMarker)
	outcome := "error"
	if aborted.timedOut {
		outcome = "timeout"
	}
	g.err = aborted
	g.p.observeInternalOperation(g.ctx, "backend_response_aborted", outcome, elapsed)
	g.p.log.Error("VictoriaLogs response aborted after its headers",
		"backend.route", g.path, "outcome", outcome, "elapsed", elapsed, "timeout_arg", g.budget, "bytes_read", g.read, "error", err)
	return n, g.err
}

// keepTail keeps the last bytes read, where VictoriaLogs leaves its error text
// before aborting an uncompressed response.
func (g *backendBodyGuard) keepTail(b []byte) {
	if len(b) >= len(g.tail) {
		g.tailN = copy(g.tail[:], b[len(b)-len(g.tail):])
		return
	}
	if over := g.tailN + len(b) - len(g.tail); over > 0 {
		copy(g.tail[:], g.tail[over:g.tailN])
		g.tailN -= over
	}
	g.tailN += copy(g.tail[g.tailN:], b)
}
