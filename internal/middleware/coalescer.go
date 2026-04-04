// Package middleware provides HTTP middleware for protecting the VL backend.
package middleware

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
)

// Coalescer deduplicates identical concurrent requests.
// When N clients send the same query simultaneously, only 1 request
// goes to the backend. All N clients share the single response.
type Coalescer struct {
	group          singleflight.Group
	ActiveShared   atomic.Int64 // requests currently sharing a coalesced result
	CoalescedTotal atomic.Int64 // total coalesced (deduplicated) requests
}

func NewCoalescer() *Coalescer {
	return &Coalescer{}
}

type coalescedResponse struct {
	status  int
	headers http.Header
	body    []byte
}

// Do executes the request, deduplicating identical concurrent requests.
// The key should uniquely identify the request (e.g., method + path + query).
func (c *Coalescer) Do(key string, fn func() (*http.Response, error)) (int, http.Header, []byte, error) {
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
			status:  resp.StatusCode,
			headers: resp.Header.Clone(),
			body:    body,
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
	return cr.status, cr.headers, cr.body, nil
}

// RequestKey builds a cache/coalescing key from an HTTP request.
// Includes X-Scope-OrgID to prevent cross-tenant data leaks.
func RequestKey(r *http.Request) string {
	h := sha256.New()
	tenant := r.Header.Get("X-Scope-OrgID")
	_, _ = fmt.Fprintf(h, "%s:%s:%s:%s", r.Method, r.URL.Path, r.URL.RawQuery, tenant)
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}
