// Package middleware provides HTTP middleware for protecting the VL backend.
package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ErrGuardRejected is returned by DoWithGuard when guard() returns false and no
// call for that key is currently in-flight. Callers should treat it as a 503.
var ErrGuardRejected = errors.New("guard rejected: no in-flight call to join")

// coalescerBodyPool recycles bytes.Buffer scratch buffers for reading response bodies.
// Each buffer grows to steady-state capacity on first use and is reused, replacing
// the io.ReadAll growth chain (9+ intermediate allocations for a 256KB response).
var coalescerBodyPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 64*1024)) // 64KB initial
	},
}

// readBodyPooled reads r (up to limit bytes) using a pooled scratch buffer,
// then returns a right-sized stable copy. The scratch buffer is returned to
// the pool, so only the final copy allocation escapes to the heap.
func readBodyPooled(r io.Reader, limit int64) ([]byte, error) {
	buf := coalescerBodyPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer func() {
		if buf.Cap() <= 1<<20 {
			coalescerBodyPool.Put(buf)
		}
	}()
	_, err := io.Copy(buf, io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(buf.Len()) > limit {
		return nil, fmt.Errorf("upstream response exceeds %d bytes", limit)
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// defaultBodyReadLimit bounds a shared response body when its reader sets no
// limit of its own.
const defaultBodyReadLimit = 256 << 20

// bodyReadLimit is the read limit for body: a reader that enforces an
// operator-configured cap above the default (the proxy's per-request response
// caps) raises it, so the coalescer never hides a larger configured limit.
func bodyReadLimit(body io.Reader) int64 {
	if limited, ok := body.(interface{ BodyLimit() int64 }); ok && limited.BodyLimit() > defaultBodyReadLimit {
		return limited.BodyLimit()
	}
	return defaultBodyReadLimit
}

// Coalescer deduplicates identical concurrent requests.
// When N clients send the same query simultaneously, only 1 request
// goes to the backend. All N clients share the single response.
//
// The shared call runs on its own context (SharedContext), cancelled when the
// last waiting client leaves. A client that cancels stops waiting at once, and
// the call goes on for the clients that still wait.
//
// When disabled (NewCoalescerDisabled), Do and DoWithGuard call fn directly
// without deduplication — every request is a separate backend call.
// This is used for benchmarking to isolate raw translation overhead from coalescing gains.
type Coalescer struct {
	flightMu       sync.Mutex
	flights        map[string]*flight // calls in progress, one per key
	inflightMu     sync.Mutex
	inflight       map[string]int // reference count of goroutines inside DoWithGuard for each key
	disabled       bool           // when true, bypass singleflight (raw passthrough)
	ActiveShared   atomic.Int64   // requests currently sharing a coalesced result
	CoalescedTotal atomic.Int64   // total coalesced (deduplicated) requests
	CoalescedSaved atomic.Int64   // backend calls avoided by duplicate requests
	WaitCount      atomic.Int64
	WaitNanos      atomic.Int64
	// OnCall, when set, gives the context a shared call runs on, from the call's shared context (set before the
	// first call). OnShare, when set, is told of a caller that got a shared call's answer without running it: its
	// own context and the context the call ran on (OnCall's).
	OnCall  func(context.Context) context.Context
	OnShare func(waiter, call context.Context)
}

func NewCoalescer() *Coalescer {
	return &Coalescer{
		flights:  make(map[string]*flight),
		inflight: make(map[string]int),
	}
}

// NewCoalescerDisabled returns a Coalescer that calls fn directly without
// deduplication. Every concurrent request makes its own backend call.
func NewCoalescerDisabled() *Coalescer {
	return &Coalescer{
		flights:  make(map[string]*flight),
		inflight: make(map[string]int),
		disabled: true,
	}
}

type coalescedResponse struct {
	status int
	body   []byte
	// value is what a response whose body is a SharedValuer shares instead of its bytes.
	value any
}

// SharedValuer is a response body that carries a read-only value its readers share instead of bytes: the coalescer
// hands the value to every waiter (DoValueWithGuardCtx) and reads no body. The value must not be changed once shared.
type SharedValuer interface {
	SharedValue() any
}

// flight is one shared upstream call and the result its waiters read once done
// is closed.
type flight struct {
	shared *SharedContext
	// callCtx is the context fn runs on, set before fn starts.
	callCtx context.Context
	done    chan struct{}
	resp    *coalescedResponse
	err     error
	joined  int // callers that took part in the call, the leader included
	// ctxEnded: the shared context had ended (its deadline, or every waiter
	// left) when the call failed, read before the call is closed.
	ctxEnded bool
}

// Do executes the request, deduplicating identical concurrent requests.
// The key should uniquely identify the request (e.g., method + path + query).
// When the coalescer is disabled, fn is called directly for every request.
func (c *Coalescer) Do(key string, fn func() (*http.Response, error)) (int, http.Header, []byte, error) {
	status, headers, body, _, _, err := c.do(context.Background(), key, func(context.Context) (*http.Response, error) { return fn() })
	return status, headers, body, err
}

// maxInheritedRetries bounds how often a follower starts the call again after
// the call it waited on ended with another caller's cancellation or deadline.
const maxInheritedRetries = 2

// do runs fn once per key for all concurrent callers. fn gets the shared
// context of the call, not the context of the caller that started it.
//
// The shared call carries the deadline of the caller that started it. When it
// ends with a cancellation or a deadline while this caller's own context is
// still alive, the failure is not this caller's: it starts the call again
// (joining a newer one when another caller already did), so a follower never
// answers with the leader's cancellation or timeout.
func (c *Coalescer) do(ctx context.Context, key string, fn func(context.Context) (*http.Response, error)) (int, http.Header, []byte, any, bool, error) {
	if c.disabled {
		status, headers, body, value, err := c.callDirectValue(func() (*http.Response, error) { return fn(ctx) })
		return status, headers, body, value, false, err
	}
	for attempt := 0; ; attempt++ {
		status, headers, body, value, shared, inherited, err := c.doOnce(ctx, key, fn)
		if inherited && attempt < maxInheritedRetries && ctx.Err() == nil {
			continue
		}
		return status, headers, body, value, shared, err
	}
}

// doOnce joins or starts one shared call. inherited reports a call this
// caller only followed that failed because the shared context ended (the
// first caller's deadline, or every waiter leaving): not a failure of the
// backend, and not this caller's.
func (c *Coalescer) doOnce(ctx context.Context, key string, fn func(context.Context) (*http.Response, error)) (status int, headers http.Header, body []byte, value any, shared, inherited bool, err error) {

	// A caller that has already left starts no shared call: nobody would wait for it.
	if err := ctx.Err(); err != nil {
		return 0, nil, nil, nil, false, false, err
	}
	started := time.Now()
	executed := false
	c.flightMu.Lock()
	f := c.flights[key]
	if f != nil && f.shared.Join() {
		f.joined++
	} else {
		// No call to join, or its last waiter already left and cancelled it:
		// a cancelled call is never handed to a later caller.
		executed = true
		f = &flight{shared: NewSharedContext(ctx, true), done: make(chan struct{}), joined: 1}
		f.callCtx = f.shared.Context()
		if c.OnCall != nil {
			f.callCtx = c.OnCall(f.callCtx)
		}
		c.flights[key] = f
		go c.run(key, f, fn)
	}
	c.flightMu.Unlock()

	select {
	case <-f.done:
	case <-ctx.Done():
		f.shared.Leave()
		return 0, nil, nil, nil, false, false, ctx.Err()
	}

	c.flightMu.Lock()
	shared = f.joined > 1
	c.flightMu.Unlock()
	if f.err != nil {
		inherited = !executed && f.ctxEnded &&
			(errors.Is(f.err, context.Canceled) || errors.Is(f.err, context.DeadlineExceeded))
	}
	// A caller whose call did not execute avoided a backend call, even on shared failure; one that starts the call
	// again after an inherited cancellation (do) avoided nothing.
	if shared && !executed && !inherited {
		c.CoalescedSaved.Add(1)
		c.WaitCount.Add(1)
		c.WaitNanos.Add(time.Since(started).Nanoseconds())
	}
	if f.err != nil {
		return 0, nil, nil, nil, shared, inherited, f.err
	}
	if !executed && c.OnShare != nil {
		c.OnShare(ctx, f.callCtx)
	}
	if shared {
		c.CoalescedTotal.Add(1)
		c.ActiveShared.Add(1)
		defer c.ActiveShared.Add(-1)
	}
	return f.resp.status, nil, f.resp.body, f.resp.value, shared, false, nil
}

// run executes the shared call and publishes its result to every waiter.
func (c *Coalescer) run(key string, f *flight, fn func(context.Context) (*http.Response, error)) {
	defer func() {
		if r := recover(); r != nil {
			f.resp, f.err = nil, fmt.Errorf("coalesced call panicked: %v", r)
		}
		f.shared.Close()
		c.flightMu.Lock()
		if c.flights[key] == f {
			delete(c.flights, key)
		}
		c.flightMu.Unlock()
		close(f.done)
	}()
	resp, err := fn(f.callCtx)
	if err != nil {
		f.err, f.ctxEnded = err, f.shared.Context().Err() != nil
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if v, ok := resp.Body.(SharedValuer); ok {
		f.resp = &coalescedResponse{status: resp.StatusCode, value: v.SharedValue()}
		return
	}

	// Limit response body to 256MB to prevent unbounded memory allocation
	body, err := readBodyPooled(resp.Body, bodyReadLimit(resp.Body))
	if err != nil {
		f.err = err
		return
	}
	f.resp = &coalescedResponse{status: resp.StatusCode, body: body}
}

// callDirectValue calls fn without deduplication (the coalescer disabled) and returns the value a SharedValuer body
// carries (reading no body then). Headers are not cloned since all current callers discard the header return value.
func (c *Coalescer) callDirectValue(fn func() (*http.Response, error)) (int, http.Header, []byte, any, error) {
	resp, err := fn()
	if err != nil {
		return 0, nil, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if v, ok := resp.Body.(SharedValuer); ok {
		return resp.StatusCode, nil, nil, v.SharedValue(), nil
	}
	body, err := readBodyPooled(resp.Body, bodyReadLimit(resp.Body))
	if err != nil {
		return 0, nil, nil, nil, err
	}
	return resp.StatusCode, nil, body, nil, nil
}

// DoWithGuard is like Do but couples the circuit breaker with request coalescing.
//
// If a call for key is already in-flight (another goroutine is running fn for
// this key), this goroutine joins that call and shares the result — the guard
// is bypassed. This means: when the breaker is open and a probe request is
// already in flight, subsequent identical requests wait for the probe result
// rather than each failing independently with a CB-open error.
//
// If no call for key is in-flight and guard() returns false, ErrGuardRejected
// is returned immediately without calling fn or touching the backend.
//
// Typical usage: pass p.breaker.Allow as guard and vlGetInner (no CB check) as fn.
func (c *Coalescer) DoWithGuard(key string, guard func() bool, fn func() (*http.Response, error)) (int, http.Header, []byte, error) {
	return c.DoWithGuardCtx(context.Background(), key, guard, func(context.Context) (*http.Response, error) { return fn() })
}

// DoWithGuardCtx is DoWithGuard for a caller that can stop waiting: when ctx is
// done the caller returns ctx's error at once, and the shared call is cancelled
// only if no other caller still waits for it. fn gets the context of the shared
// call, which carries ctx's values (and deadline) but not its cancellation.
func (c *Coalescer) DoWithGuardCtx(ctx context.Context, key string, guard func() bool, fn func(context.Context) (*http.Response, error)) (int, http.Header, []byte, error) {
	status, headers, body, _, err := c.DoValueWithGuardCtx(ctx, key, guard, fn)
	return status, headers, body, err
}

// DoValueWithGuardCtx is DoWithGuardCtx that also returns the value of a response whose body is a SharedValuer (the
// body bytes are nil then): every waiter gets the same value, which none may change.
func (c *Coalescer) DoValueWithGuardCtx(ctx context.Context, key string, guard func() bool, fn func(context.Context) (*http.Response, error)) (int, http.Header, []byte, any, error) {
	if c.disabled {
		if !guard() {
			return 0, nil, nil, nil, ErrGuardRejected
		}
		return c.callDirectValue(func() (*http.Response, error) { return fn(ctx) })
	}

	c.inflightMu.Lock()
	count := c.inflight[key]
	if count == 0 && !guard() {
		c.inflightMu.Unlock()
		return 0, nil, nil, nil, ErrGuardRejected
	}
	c.inflight[key]++
	c.inflightMu.Unlock()

	status, headers, body, value, _, err := c.do(ctx, key, fn)

	c.inflightMu.Lock()
	c.inflight[key]--
	if c.inflight[key] == 0 {
		delete(c.inflight, key)
	}
	c.inflightMu.Unlock()

	return status, headers, body, value, err
}

// RequestKey builds a cache/coalescing key from an HTTP request.
// Includes X-Scope-OrgID to prevent cross-tenant data leaks.
func RequestKey(r *http.Request) string {
	h := sha256.New()
	tenant := r.Header.Get("X-Scope-OrgID")
	_, _ = fmt.Fprintf(h, "%s:%s:%s:%s", r.Method, r.URL.Path, r.URL.RawQuery, tenant)
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}
