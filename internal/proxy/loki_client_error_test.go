package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// lokiClientStatuses is every status Loki v3.7.9 answers a failed query with
// (pkg/util/server/error.go ClientHTTPStatusAndError; pkg/queue/queue.go:21).
var lokiClientStatuses = map[int]bool{
	http.StatusBadRequest:          true,
	http.StatusTooManyRequests:     true,
	499:                            true,
	http.StatusInternalServerError: true,
	http.StatusGatewayTimeout:      true,
}

// lokiFixedClientTexts are Loki's fixed texts: the only texts an error the
// proxy did not write itself may answer with.
var lokiFixedClientTexts = map[string]bool{
	lokiErrClientCanceled:          true,
	lokiErrDeadlineExceeded:        true,
	lokiTooManyOutstandingRequests: true,
	lokiErrParse:                   true,
	lokiErrInternal:                true,
}

type mapperTimeoutErr struct{}

func (mapperTimeoutErr) Error() string   { return "read tcp 10.0.0.7:41234->10.0.0.9:9428: i/o timeout" }
func (mapperTimeoutErr) Timeout() bool   { return true }
func (mapperTimeoutErr) Temporary() bool { return true }

// TestLokiClientError_LokiAnswers locks the error layer's mapping to Loki's
// ClientHTTPStatusAndError (Loki v3.7.9 pkg/util/server/error.go): every input
// a route can fail with, and the status, errorType and text the client reads.
// conformance: semantics/error-layer-backend-failure-status, semantics/error-layer-transport-text, semantics/error-layer-backend-text, semantics/error-layer-cancel-deadline, semantics/error-layer-internal-go-text, status-500, status-502, status-503, status-400
func TestLokiClientError_LokiAnswers(t *testing.T) {
	p := &Proxy{}
	vl := func(status int, body string) error { return p.redactedBackendStatusError("", status, []byte(body)) }
	refused := &url.Error{Op: "Post", URL: "http://10.0.0.9:9428/select/logsql/query", Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}}
	clientTimeout := &url.Error{Op: "Post", URL: "http://10.0.0.9:9428/select/logsql/stats_query_range", Err: context.DeadlineExceeded}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	var syntaxErr error
	if err := json.Unmarshal([]byte(`{"status":"success","data":{"result":[VLSENTINEL`), new(any)); err != nil {
		syntaxErr = fmt.Errorf("decode VL stats response: %w", err)
	}
	const rowLimit = "limit reached while evaluating the query"
	seriesLimit := lokierr.SeriesLimit(1).Text()

	for _, tc := range []struct {
		name   string
		ctx    context.Context
		err    error
		status int
		text   string // "" = err's own text (a proxy text in Loki's words)
	}{
		// error.go:26, :103-105: a cancellation is 499 with Loki's text, whatever the transport said.
		{"request context cancelled", canceled, refused, 499, lokiErrClientCanceled},
		{"context.Canceled", context.Background(), fmt.Errorf("fetch: %w", context.Canceled), 499, lokiErrClientCanceled},
		// error.go:27, :106-107: a deadline is 504 with Loki's text.
		{"request deadline", expired, refused, http.StatusGatewayTimeout, lokiErrDeadlineExceeded},
		{"client timeout awaiting headers", context.Background(), clientTimeout, http.StatusGatewayTimeout, lokiErrDeadlineExceeded},
		{"net timeout", context.Background(), mapperTimeoutErr{}, http.StatusGatewayTimeout, lokiErrDeadlineExceeded},
		{"VictoriaLogs aborted at its deadline", context.Background(), &vlResponseAbortedError{elapsed: time.Minute, timedOut: true}, http.StatusGatewayTimeout, lokiErrDeadlineExceeded},
		// pkg/queue/queue.go:21: a full queue is 429 "too many outstanding requests".
		{"proxy heavy-query queue", context.Background(), newHeavyQueueFull(&heavyQueryQueueFullError{maxConcurrent: 2, queueWait: time.Second}), http.StatusTooManyRequests, lokiTooManyOutstandingRequests},
		// VictoriaLogs v1.49.0-v1.53.0 answer a query that waited
		// -search.maxQueueDuration for a slot with 503 (app/vlselect/main.go:268
		// in v1.49.0, :313-318 in v1.53.0): Loki's full queue.
		{"VictoriaLogs queue timeout 503", context.Background(), vl(http.StatusServiceUnavailable, "couldn't start executing the request in 10.000 seconds, since -search.maxConcurrentRequests=16 concurrent requests are executed. Possible solutions: to reduce query load; to add more compute resources to the server; to increase -search.maxQueueDuration=10s; to increase -search.maxQueryDuration=30s; to increase -search.maxConcurrentRequests; to pass bigger value to 'timeout' query arg"), http.StatusTooManyRequests, lokiTooManyOutstandingRequests},
		// ... and a query past -search.maxQueryDuration before its headers with
		// 503 (main.go:236 in v1.49.0, :276-278 in v1.53.0): Loki's deadline.
		{"VictoriaLogs query deadline 503", context.Background(), vl(http.StatusServiceUnavailable, "the request couldn't be executed in 30.000 seconds; possible solutions: to increase -search.maxQueryDuration=30s; to pass bigger value to 'timeout' query arg"), http.StatusGatewayTimeout, lokiErrDeadlineExceeded},
		{"VictoriaLogs 429", context.Background(), vl(http.StatusTooManyRequests, "couldn't start executing the request in 10.000 seconds, since -search.maxConcurrentRequests=16 concurrent requests are executed"), http.StatusTooManyRequests, lokiTooManyOutstandingRequests},
		// error.go:110-117 (logqlmodel.ErrParse, pkg/logqlmodel/error.go:13): a rejected query is 400.
		{"VictoriaLogs 400 parse error", context.Background(), vl(http.StatusBadRequest, vlBodyParsePattern), http.StatusBadRequest, lokiErrParse},
		{"VictoriaLogs 422 parse error", context.Background(), fmt.Errorf("left query: %w", vl(http.StatusUnprocessableEntity, vlBodyParseRegexStats)), http.StatusBadRequest, lokiErrParse},
		// The translator's wording never reaches a client: Loki's syntax error without a position.
		{"translator parse error", context.Background(), &translator.ParseError{Msg: "unmatched '{' in stream selector", Pos: -1}, http.StatusBadRequest, "parse error : syntax error: unexpected $end"},
		{"ordered JSON pipeline error", context.Background(), &orderedJSONPipelineError{errLokiStepTooSmall}, http.StatusBadRequest, ""},
		// pkg/querier/queryrange/limits.go:46, :448: max_query_series is 400 with limitErrTmpl.
		{"series limit", context.Background(), fmt.Errorf("fold: %w", newSeriesLimitError(context.Background(), 1)), http.StatusBadRequest, seriesLimit},
		// error.go:124-128: every other failed backend call is 500, with no backend text.
		{"connection refused", context.Background(), refused, http.StatusInternalServerError, lokiErrInternal},
		{"VictoriaLogs 500", context.Background(), vl(http.StatusInternalServerError, `{"error":"unexpected error at lib/logstorage/storage.go:123"}`), http.StatusInternalServerError, lokiErrInternal},
		{"front proxy 502", context.Background(), vl(http.StatusBadGateway, "<html><body>502 Bad Gateway nginx</body></html>"), http.StatusInternalServerError, lokiErrInternal},
		{"front proxy 503", context.Background(), vl(http.StatusServiceUnavailable, "<html><body>503 Service Unavailable envoy</body></html>"), http.StatusInternalServerError, lokiErrInternal},
		{"VictoriaLogs 400 memory limit", context.Background(), vl(http.StatusBadRequest, vlBodyRowsMemory), http.StatusInternalServerError, lokiErrInternal},
		{"VictoriaLogs 422 execution", context.Background(), vl(http.StatusUnprocessableEntity, vlBodyStatsMemory), http.StatusInternalServerError, lokiErrInternal},
		{"VictoriaLogs unsupported path", context.Background(), vl(http.StatusBadRequest, vlBodyUnsupportedPath), http.StatusInternalServerError, lokiErrInternal},
		{"truncated body", context.Background(), fmt.Errorf("scanning VL response: %w", io.ErrUnexpectedEOF), http.StatusInternalServerError, lokiErrInternal},
		{"malformed body", context.Background(), syntaxErr, http.StatusInternalServerError, lokiErrInternal},
		{"VictoriaLogs aborted early", context.Background(), &vlResponseAbortedError{elapsed: time.Second}, http.StatusInternalServerError, lokiErrInternal},
		{"circuit breaker open", context.Background(), errors.New("circuit breaker open — backend unavailable"), http.StatusInternalServerError, lokiErrInternal},
		{"guard rejection", context.Background(), mw.ErrGuardRejected, http.StatusInternalServerError, lokiErrInternal},
		{"translator unsupported", context.Background(), &translator.UnsupportedError{Msg: "unsupported"}, http.StatusInternalServerError, lokiErrInternal},
		{"nil error", context.Background(), nil, http.StatusInternalServerError, lokiErrInternal},
		// A message that merely reads like a proxy limit is no limit: only a typed error answers as one.
		{"limit text with no type", context.Background(), errors.New("manual metric response exceeds 64 bytes; narrow the query or increase -backend-max-buffered-response-bytes"), http.StatusInternalServerError, lokiErrInternal},
		{"limit text around a transport error", context.Background(), fmt.Errorf("x; narrow the query or increase -flag: %w", refused), http.StatusInternalServerError, lokiErrInternal},
		// A text that merely says timeout, deadline exceeded or canceled is a failed backend call.
		{"text says timeout", context.Background(), errors.New("vl said: read timeout while scanning"), http.StatusInternalServerError, lokiErrInternal},
		{"text says deadline exceeded", context.Background(), errors.New("rpc: deadline exceeded"), http.StatusInternalServerError, lokiErrInternal},
		{"text says context canceled", context.Background(), errors.New("post: context canceled"), http.StatusInternalServerError, lokiErrInternal},
		// Loki's own 500 texts the proxy writes in Loki's words: a matching
		// error (pkg/logql/engine.go vectorBinop), the gRPC size limit
		// (error.go:93-100) and the proxy's limits, in the text of their Loki class.
		{"vector matching", context.Background(), vectorMatchError("multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"), http.StatusInternalServerError, ""},
		{"label values too large", context.Background(), newLabelValuesTooLarge(10, 5), http.StatusInternalServerError, ""},
		{"buffered response limit", context.Background(), fmt.Errorf("stats: %w", newBackendBodyTooLarge(4096)), http.StatusInternalServerError, "rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (4097 vs. 4096)"},
		// A proxy buffer limit is Loki's message size answer (500), with no flag in it.
		{"buffer limit", context.Background(), newManualMetricResponseTooLarge(64), http.StatusInternalServerError, "rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (65 vs. 64)"},
		// error.go:110-118 (logqlmodel.ErrLimit): a query over a limit is 400. The raw-row limit has no Loki flag, so the text is the class's.
		{"row limit", context.Background(), manualRowLimitError(3), http.StatusBadRequest, rowLimit},
		{"row limit wrapped", context.Background(), fmt.Errorf("fold: %w", manualRowLimitError(3)), http.StatusBadRequest, rowLimit},
		{"row limit of a bucket grid", context.Background(), newRowLimitError(3, "stats rows in one bucket grid", 0), http.StatusBadRequest, rowLimit},
		{"row limit of a count precheck", context.Background(), newRowLimitError(3, "", 5000), http.StatusBadRequest, rowLimit},
		{"row limit of a without fold", context.Background(), newSeriesLimitError(foldingBudgetContext(3), 3), http.StatusBadRequest, rowLimit},
		// The binary evaluation budgets are query limits too (400, the class's text).
		{"binary arrays budget", context.Background(), newBinaryBudgetError(flagBinaryMetricMaxArrays), http.StatusBadRequest, rowLimit},
		{"binary operand bytes budget wrapped", context.Background(), fmt.Errorf("left query: %w", newBinaryBudgetError(flagBinaryMetricMaxOperandBytes)), http.StatusBadRequest, rowLimit},
		{"binary budget of a cancelled request", canceled, newBinaryBudgetError(flagBinaryMetricMaxArrays), 499, lokiErrClientCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, errorType, text := lokiClientError(tc.ctx, tc.err)
			want := tc.text
			if want == "" {
				want = tc.err.Error()
			}
			if status != tc.status || errorType != lokiErrorType(tc.status) || text != want {
				t.Fatalf("lokiClientError = %d %q %q, want %d %q %q", status, errorType, text, tc.status, lokiErrorType(tc.status), want)
			}
		})
	}
}

// TestWriteErr_EnvelopeCarriesOnlyLokiText checks the writer: Loki's status,
// the text alone, and nothing of the backend.
// conformance: semantics/error-layer-transport-text, semantics/error-layer-backend-text
func TestWriteErr_EnvelopeCarriesOnlyLokiText(t *testing.T) {
	p := &Proxy{}
	rec := httptest.NewRecorder()
	err := &url.Error{Op: "Post", URL: "http://10.0.0.9:9428/select/logsql/query", Err: errors.New("dial tcp 10.0.0.9:9428: connect: connection refused")}
	if got := p.writeErr(rec, context.Background(), err); got != http.StatusInternalServerError || rec.Code != got {
		t.Fatalf("writeErr = %d, recorded %d, want 500", got, rec.Code)
	}
	// Loki's WriteError: text/plain, nosniff and the text alone, no envelope.
	if rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" || rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Body.String() != lokiErrInternal {
		t.Fatalf("headers = %v, body = %q", rec.Header(), rec.Body.String())
	}
}

// FuzzLokiClientError builds failed backend calls the way the proxy does
// (a VictoriaLogs error response through redactedBackendStatusError, a
// transport error naming the backend URL, a decoder error over the body, an
// aborted body), optionally wrapped and under a live, cancelled or expired
// request context, and asserts the client answer: a status in Loki's set,
// the errorType of that status, and one of Loki's fixed texts, so no
// VictoriaLogs body, host:port or URL can reach a client.
//
// conformance: semantics/error-layer-transport-text, semantics/error-layer-backend-text, semantics/error-layer-internal-go-text, semantics/error-layer-cancel-deadline
func FuzzLokiClientError(f *testing.F) {
	f.Add(uint8(0), uint8(0), uint8(0), 500, `{"error":"unexpected error at lib/logstorage/storage.go:123"}`)
	f.Add(uint8(0), uint8(1), uint8(0), 400, vlBodyParsePattern)
	f.Add(uint8(0), uint8(0), uint8(0), 422, vlBodyStatsMemory)
	f.Add(uint8(0), uint8(2), uint8(0), 429, "-search.maxConcurrentRequests=16")
	f.Add(uint8(1), uint8(0), uint8(1), 0, "connect: connection refused")
	f.Add(uint8(2), uint8(1), uint8(2), 200, `{"status":"success","data":{"result":[VLSENTINEL`)
	f.Add(uint8(3), uint8(0), uint8(0), 200, "the connection has been aborted")
	f.Add(uint8(4), uint8(2), uint8(0), 0, "x; narrow the query or increase -flag")
	f.Add(uint8(0), uint8(0), uint8(0), 502, "<html>502 Bad Gateway nginx; narrow the query or increase -x</html>")
	f.Add(uint8(5), uint8(1), uint8(0), 1, "")
	f.Add(uint8(5), uint8(2), uint8(1), 0, "")
	p := &Proxy{}
	f.Fuzz(func(t *testing.T, kind, wrap, ctxState uint8, status int, body string) {
		host := "10.0.0.9:9428"
		target := "http://" + host + "/select/logsql/query"
		var err error
		var budget error
		switch kind % 6 {
		case 0: // a completed VictoriaLogs (or front proxy) error response
			if status < 400 || status > 599 {
				status = 400 + (status%200+200)%200
			}
			err = p.redactedBackendStatusError("", status, []byte(body))
		case 1: // a transport error carrying the backend URL
			err = &url.Error{Op: "Post", URL: target, Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New(body)}}
		case 2: // a decoder error over a 200 body
			var v any
			if decodeErr := json.Unmarshal([]byte(body), &v); decodeErr != nil {
				err = fmt.Errorf("scanning VL response from %s: %w", target, decodeErr)
			} else {
				err = fmt.Errorf("scanning VL response from %s: %w", target, io.ErrUnexpectedEOF)
			}
		case 3: // a body aborted after its headers
			err = &vlResponseAbortedError{elapsed: time.Duration(status) * time.Millisecond, timedOut: status%2 == 0, cause: errors.New(body)}
		case 4: // a transport error whose text looks like a proxy limit
			err = &url.Error{Op: "Get", URL: target, Err: errors.New(body)}
		case 5: // a binary evaluation budget: a query limit naming its flag
			budget = newBinaryBudgetError([]string{flagBinaryMetricMaxArrays, flagBinaryMetricMaxOperandBytes}[status&1])
			err = budget
		}
		switch wrap % 3 {
		case 1:
			err = fmt.Errorf("left query: %w", err)
		case 2:
			err = fmt.Errorf("fetch %s: %w", target, err)
		}
		ctx := context.Background()
		switch ctxState % 3 {
		case 1:
			c, cancel := context.WithCancel(ctx)
			cancel()
			ctx = c
		case 2:
			c, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
			defer cancel()
			ctx = c
		}
		gotStatus, errorType, text := lokiClientError(ctx, err)
		if !lokiClientStatuses[gotStatus] || errorType != lokiErrorType(gotStatus) {
			t.Fatalf("status %d errorType %q for %v", gotStatus, errorType, err)
		}
		if budget != nil && ctxState%3 == 0 {
			if gotStatus != http.StatusBadRequest || text != budget.Error() {
				t.Fatalf("binary budget %v answered %d %q", err, gotStatus, text)
			}
			return
		}
		if !lokiFixedClientTexts[text] {
			t.Fatalf("text %q is not one of Loki's fixed texts (err %v)", text, err)
		}
		if strings.Contains(text, host) || strings.Contains(text, "http://") {
			t.Fatalf("text %q leaks the backend address", text)
		}
		if trimmed := strings.TrimSpace(body); trimmed != "" && strings.Contains(text, trimmed) && !withinLokiFixedText(trimmed) {
			t.Fatalf("text %q carries the VictoriaLogs body %q", text, body)
		}
		switch ctxState % 3 {
		case 1:
			if gotStatus != 499 || text != lokiErrClientCanceled {
				t.Fatalf("cancelled request answered %d %q", gotStatus, text)
			}
		case 2:
			if gotStatus != http.StatusGatewayTimeout || text != lokiErrDeadlineExceeded {
				t.Fatalf("expired request answered %d %q", gotStatus, text)
			}
		}
	})
}

// withinLokiFixedText reports whether s is part of one of Loki's fixed texts,
// so finding it in an answer says nothing about the body it came from.
func withinLokiFixedText(s string) bool {
	for text := range lokiFixedClientTexts {
		if strings.Contains(text, s) {
			return true
		}
	}
	return false
}
