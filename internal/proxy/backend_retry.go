package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// A VictoriaLogs read that fails in a way another try fixes is repeated here,
// once for every handler, so the client gets the answer Loki would give and
// not an error Loki would not have produced. The failures are the ones that
// happen before VictoriaLogs answered with data: the connection was refused,
// reset, or closed without a response, and a 429, 502, 503 or 504 answer (a
// full select queue, a node leaving or restarting, a front proxy that lost its
// backend). VictoriaLogs' own 503s are the queue timeout and the delayed
// shutdown (app/vlselect/main.go, lib/httpserver/httpserver.go in v1.53.0); it
// sends no Retry-After, a proxy in front may, and it is honoured.
//
// Nothing is repeated once a response body reached a handler: handlers stream,
// and a body that breaks halfway is the handler's failure to report. A call
// that timed out (the response header timeout, the client timeout) is not
// repeated either, since the attempt already used the time a slow VictoriaLogs
// had.
//
// The retries are bounded three ways so they cannot amplify an overload:
// a small attempt count and a total wait per call, a budget that earns a
// fraction of a retry for every call and spends one per retry (a sustained
// failure is retried for at most that fraction of the calls, per replica), and
// no retry while the circuit breaker is not closed.
const (
	// DefaultBackendRetryAttempts is the retries after the first try.
	DefaultBackendRetryAttempts = 2
	// DefaultBackendRetryMaxWait bounds the time one call waits between tries.
	DefaultBackendRetryMaxWait = 5 * time.Second
	// DefaultBackendRetryBudgetRatio is the retries earned per call.
	DefaultBackendRetryBudgetRatio = 0.1

	backendRetryInitialBackoff = 200 * time.Millisecond
	backendRetryMaxBackoff     = 2 * time.Second
	// backendRetryBudgetBurst is the retries a quiet replica may spend at
	// once: a replica that has not made a call for a while can still ride out
	// a blip, and a replica that is failing sustained cannot save them up.
	backendRetryBudgetBurst = 20
	// backendRetryLogBody bounds the discarded answer read for the log line.
	backendRetryLogBody = 512
)

// Operation outcomes of loki_vl_proxy_internal_operation_total{operation="backend_retry"}.
const (
	backendRetryOp           = "backend_retry"
	backendRetryRetried      = "retried"       // a wait before another try began; the duration is the wait
	backendRetryRecovered    = "recovered"     // a repeated call succeeded; the duration is the time spent retrying
	backendRetryExhausted    = "exhausted"     // tries, wait or the request's deadline ran out; the last answer is returned
	backendRetryBudgetDenied = "budget_denied" // the retry budget was empty
	backendRetryBreakerOpen  = "breaker_open"  // the circuit breaker was not closed
)

// retryBudget lets retries be a fraction of the calls: a call earns ratio of a
// retry (capped at the burst) and a retry spends one. Counts are in thousandths
// so the fraction needs no float on the hot path.
type retryBudget struct {
	milli    atomic.Int64
	earnEach int64
	capMilli int64
}

func newRetryBudget(ratio float64, burst int) *retryBudget {
	b := &retryBudget{earnEach: int64(ratio * 1000), capMilli: int64(burst) * 1000}
	b.milli.Store(b.capMilli)
	return b
}

// earn credits one call. At the cap this is a single atomic load.
func (b *retryBudget) earn() {
	for {
		cur := b.milli.Load()
		if cur >= b.capMilli {
			return
		}
		next := cur + b.earnEach
		if next > b.capMilli {
			next = b.capMilli
		}
		if b.milli.CompareAndSwap(cur, next) {
			return
		}
	}
}

// spend takes one retry and reports whether the budget had it.
func (b *retryBudget) spend() bool {
	for {
		cur := b.milli.Load()
		if cur < 1000 {
			return false
		}
		if b.milli.CompareAndSwap(cur, cur-1000) {
			return true
		}
	}
}

// backendRetryPolicy is the settings of the shared retry; nil on the Proxy
// means retries are off.
type backendRetryPolicy struct {
	retries int           // tries after the first
	initial time.Duration // first backoff; doubles per retry
	max     time.Duration // longest single backoff
	maxWait time.Duration // total backoff one call may spend
	budget  *retryBudget
}

func newBackendRetryPolicy(retries int, maxWait time.Duration, ratio float64) *backendRetryPolicy {
	if retries <= 0 || maxWait <= 0 {
		return nil
	}
	if ratio < 0 {
		ratio = 0
	}
	return &backendRetryPolicy{
		retries: retries,
		initial: backendRetryInitialBackoff,
		max:     backendRetryMaxBackoff,
		maxWait: maxWait,
		budget:  newRetryBudget(ratio, backendRetryBudgetBurst),
	}
}

// backendRetryable reports whether req may be sent again: a read of the
// VictoriaLogs query API that can be rebuilt (no body, or one GetBody
// recreates). The delete call, a write, is never repeated.
func backendRetryable(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet:
	case http.MethodPost:
		if !strings.Contains(req.URL.Path, "/select/") || strings.HasSuffix(req.URL.Path, "/delete") {
			return false
		}
	default:
		return false
	}
	return req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
}

// transientBackendFailure reports whether the outcome of one try is one a
// repeat may fix, and how long the backend asked to wait (Retry-After, zero
// when it did not). Only typed errors and statuses count, never error text.
func transientBackendFailure(resp *http.Response, err error) (time.Duration, bool) {
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, false
		}
		// The call never reached VictoriaLogs, or it closed the connection
		// before it sent a response.
		if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
			errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE) ||
			errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) ||
			errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, true
		}
		var opErr *net.OpError
		return 0, errors.As(err, &opErr) && opErr.Op == "dial"
	}
	if resp == nil {
		return 0, false
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), true
	}
	return 0, false
}

// parseRetryAfter reads a Retry-After value, seconds or an HTTP date; an
// absent, malformed or past value is zero.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs <= 0 || secs > int64(24*time.Hour/time.Second) {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if at, err := http.ParseTime(v); err == nil {
		if d := at.Sub(now); d > 0 && d <= 24*time.Hour {
			return d
		}
	}
	return 0
}

// doBackendRead sends a read to VictoriaLogs through doBackendRequest and, when
// the outcome is a transient failure, sends it again (see the package comment
// above). The answer after the last try is returned unchanged, its body
// unread, so the error layer maps it exactly as it does without retries.
func (p *Proxy) doBackendRead(req *http.Request, client *http.Client) (*http.Response, error) {
	rp := p.backendRetry
	if rp == nil || !backendRetryable(req) {
		return p.doBackendRequest(req, client)
	}
	rp.budget.earn()
	ctx := req.Context()
	began := time.Now()
	backoff := rp.initial
	var waited time.Duration
	for attempt := 0; ; attempt++ {
		sent := req
		if attempt > 0 {
			sent = req.Clone(ctx)
			if req.GetBody != nil {
				body, err := req.GetBody()
				if err != nil {
					return nil, err
				}
				sent.Body = body
			}
		}
		tried := time.Now()
		resp, err := p.doBackendRequest(sent, client)
		retryAfter, transient := transientBackendFailure(resp, err)
		if !transient {
			if attempt > 0 {
				p.observeInternalOperation(ctx, backendRetryOp, backendRetryRecovered, time.Since(began))
			}
			return resp, err
		}
		if ctx.Err() != nil {
			return resp, err
		}
		if attempt >= rp.retries {
			p.observeInternalOperation(ctx, backendRetryOp, backendRetryExhausted, time.Since(began))
			return resp, err
		}
		wait := backoff/2 + rand.N(backoff/2+1) // equal jitter: tries of replicas that failed together spread out
		if retryAfter > wait {
			wait = retryAfter
		}
		if backoff *= 2; backoff > rp.max {
			backoff = rp.max
		}
		if !p.backendRetryAffordable(ctx, rp, wait, waited, time.Since(tried)) {
			p.observeInternalOperation(ctx, backendRetryOp, backendRetryExhausted, time.Since(began))
			return resp, err
		}
		if p.breaker != nil && p.breaker.State() != "closed" {
			p.observeInternalOperation(ctx, backendRetryOp, backendRetryBreakerOpen, 0)
			return resp, err
		}
		if !rp.budget.spend() {
			p.observeInternalOperation(ctx, backendRetryOp, backendRetryBudgetDenied, 0)
			return resp, err
		}
		p.logBackendRetry(sent, resp, err, attempt+1, wait)
		discardBackendAnswer(resp)
		p.observeInternalOperation(ctx, backendRetryOp, backendRetryRetried, wait)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			if cause := context.Cause(ctx); cause != nil {
				return nil, cause
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		waited += wait
	}
}

// backendRetryAffordable reports whether another try fits: the wait is inside
// the call's wait budget, and the request's deadline leaves room for the wait
// and for a try as long as the last one.
func (p *Proxy) backendRetryAffordable(ctx context.Context, rp *backendRetryPolicy, wait, waited, lastTry time.Duration) bool {
	if waited+wait > rp.maxWait {
		return false
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < wait+lastTry {
		return false
	}
	return true
}

// discardBackendAnswer closes the answer of a try that is repeated; closing
// the body also gives back its backend budget permit before the wait.
func discardBackendAnswer(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

// logBackendRetry records why a try is repeated. VictoriaLogs' answer text
// goes to the log only; the client never sees it.
func (p *Proxy) logBackendRetry(req *http.Request, resp *http.Response, err error, attempt int, wait time.Duration) {
	if p.log == nil || !p.log.Enabled(context.Background(), slog.LevelWarn) {
		return
	}
	attrs := []any{"path", req.URL.Path, "attempt", attempt, "wait", wait.String()}
	if err != nil {
		attrs = append(attrs, "error", p.sanitizeUpstreamError(err).Error())
	} else if resp != nil {
		attrs = append(attrs, "http.response.status_code", resp.StatusCode)
		if msg := p.discardedAnswerText(resp); msg != "" {
			attrs = append(attrs, "upstream_error", msg)
		}
	}
	p.log.Warn("VictoriaLogs call failed, retrying", attrs...)
}

// discardedAnswerText reads the start of an answer that is about to be
// dropped, decoded and redacted like any backend error text.
func (p *Proxy) discardedAnswerText(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}
	if err := decodeCompressedHTTPResponse(resp, p.decodedBodyLimit()); err != nil {
		return ""
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, backendRetryLogBody))
	if len(body) == 0 {
		return ""
	}
	return p.redactedBackendErrorMessage(resp.StatusCode, body)
}
