// Package middleware provides HTTP middleware for protecting the VL backend.
package middleware

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
)

// ErrGuardRejected is returned by DoWithGuard when guard() returns false and no
// call for that key is currently in-flight. Callers should treat it as a 503.
var ErrGuardRejected = errors.New("guard rejected: no in-flight call to join")

// Coalescer deduplicates identical concurrent requests.
// When N clients send the same query simultaneously, only 1 request
// goes to the backend. All N clients share the single response.
//
// When disabled (NewCoalescerDisabled), Do and DoWithGuard call fn directly
// without singleflight deduplication — every request is a separate backend call.
// This is used for benchmarking to isolate raw translation overhead from coalescing gains.
type Coalescer struct {
	group          singleflight.Group
	inflightMu     sync.Mutex
	inflight       map[string]int // reference count of goroutines inside DoWithGuard for each key
	disabled       bool           // when true, bypass singleflight (raw passthrough)
	ActiveShared   atomic.Int64   // requests currently sharing a coalesced result
	CoalescedTotal atomic.Int64   // total coalesced (deduplicated) requests
}

func NewCoalescer() *Coalescer {
	return &Coalescer{
		inflight: make(map[string]int),
	}
}

// NewCoalescerDisabled returns a Coalescer that calls fn directly without
// deduplication. Every concurrent request makes its own backend call.
func NewCoalescerDisabled() *Coalescer {
	return &Coalescer{
		inflight: make(map[string]int),
		disabled: true,
	}
}

type coalescedResponse struct {
	status int
	body   []byte
}

// Do executes the request, deduplicating identical concurrent requests.
// The key should uniquely identify the request (e.g., method + path + query).
// When the coalescer is disabled, fn is called directly for every request.
func (c *Coalescer) Do(key string, fn func() (*http.Response, error)) (int, http.Header, []byte, error) {
	if c.disabled {
		return c.callDirect(fn)
	}

	result, err, shared := c.group.Do(key, func() (interface{}, error) {
		resp, err := fn()
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()

		// Limit response body to 256MB to prevent unbounded memory allocation
		body, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
		if err != nil {
			return nil, err
		}

		return &coalescedResponse{
			status: resp.StatusCode,
			body:   body,
		}, nil
	})

	if err != nil {
		return 0, nil, nil, err
	}

	if shared {
		c.CoalescedTotal.Add(1)
		c.ActiveShared.Add(1)
		defer c.ActiveShared.Add(-1)
	}

	cr := result.(*coalescedResponse)
	return cr.status, nil, cr.body, nil
}

// callDirect calls fn without singleflight — used when coalescer is disabled.
// Headers are not cloned since all current callers discard the header return value.
func (c *Coalescer) callDirect(fn func() (*http.Response, error)) (int, http.Header, []byte, error) {
	resp, err := fn()
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, nil, body, nil
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
	if c.disabled {
		if !guard() {
			return 0, nil, nil, ErrGuardRejected
		}
		return c.callDirect(fn)
	}

	c.inflightMu.Lock()
	count := c.inflight[key]
	if count == 0 && !guard() {
		c.inflightMu.Unlock()
		return 0, nil, nil, ErrGuardRejected
	}
	c.inflight[key]++
	c.inflightMu.Unlock()

	status, headers, body, err := c.Do(key, fn)

	c.inflightMu.Lock()
	c.inflight[key]--
	if c.inflight[key] == 0 {
		delete(c.inflight, key)
	}
	c.inflightMu.Unlock()

	return status, headers, body, err
}

// RequestKey builds a cache/coalescing key from an HTTP request.
// Includes X-Scope-OrgID to prevent cross-tenant data leaks.
func RequestKey(r *http.Request) string {
	h := sha256.New()
	tenant := r.Header.Get("X-Scope-OrgID")
	_, _ = fmt.Fprintf(h, "%s:%s:%s:%s", r.Method, r.URL.Path, r.URL.RawQuery, tenant)
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}
