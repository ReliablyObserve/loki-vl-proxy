package middleware

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCoalescer_DedupConcurrentRequests verifies that concurrent identical
// requests result in only one backend call.
func TestCoalescer_DedupConcurrentRequests(t *testing.T) {
	c := NewCoalescer()
	var backendCalls atomic.Int32
	var wg sync.WaitGroup

	// Simulate 20 concurrent clients making the same query
	n := 20
	wg.Add(n)

	for range n {
		go func() {
			defer wg.Done()
			c.Do("same-key", func() (*http.Response, error) {
				backendCalls.Add(1)
				// Simulate backend latency
				time.Sleep(50 * time.Millisecond)
				return &http.Response{
					StatusCode: 200,
					Header:     http.Header{},
					Body:       http.NoBody,
				}, nil
			})
		}()
	}

	wg.Wait()

	calls := backendCalls.Load()
	if calls != 1 {
		t.Errorf("expected exactly 1 backend call for %d concurrent requests, got %d", n, calls)
	}

	coalesced := c.CoalescedTotal.Load()
	if coalesced < 1 {
		t.Errorf("expected coalesced requests > 0, got %d", coalesced)
	}
	t.Logf("Backend calls: %d, Coalesced: %d", calls, coalesced)
}

// TestCoalescer_DifferentKeysSeparateRequests verifies different keys
// result in separate backend calls.
func TestCoalescer_DifferentKeysSeparateRequests(t *testing.T) {
	c := NewCoalescer()
	var backendCalls atomic.Int32
	var wg sync.WaitGroup

	wg.Add(3)
	for i := range 3 {
		go func(key string) {
			defer wg.Done()
			c.Do(key, func() (*http.Response, error) {
				backendCalls.Add(1)
				return &http.Response{
					StatusCode: 200,
					Header:     http.Header{},
					Body:       http.NoBody,
				}, nil
			})
		}(fmt.Sprintf("key-%d", i))
	}

	wg.Wait()

	if backendCalls.Load() != 3 {
		t.Errorf("expected 3 separate backend calls, got %d", backendCalls.Load())
	}
}

// TestRateLimiter_AllowsUnderLimit verifies normal traffic is allowed.
func TestRateLimiter_AllowsUnderLimit(t *testing.T) {
	rl := NewRateLimiter(10, 100, 200)

	for range 50 {
		if !rl.AllowClient("client-1") {
			t.Fatal("expected request to be allowed under burst limit")
		}
	}
}

// TestRateLimiter_RejectsOverBurst verifies burst limit enforcement.
func TestRateLimiter_RejectsOverBurst(t *testing.T) {
	rl := NewRateLimiter(10, 1, 5) // 1 req/s, burst 5

	// Use all burst tokens
	for range 5 {
		if !rl.AllowClient("client-1") {
			t.Fatal("expected burst requests to be allowed")
		}
	}

	// 6th request should be rejected
	if rl.AllowClient("client-1") {
		t.Error("expected request to be rejected after burst exhausted")
	}

	if rl.RejectedTotal.Load() != 1 {
		t.Errorf("expected 1 rejection, got %d", rl.RejectedTotal.Load())
	}
}

// TestRateLimiter_ConcurrentLimit verifies global concurrent query limit.
func TestRateLimiter_ConcurrentLimit(t *testing.T) {
	rl := NewRateLimiter(2, 0, 0) // max 2 concurrent, no per-client limit

	// Acquire 2 slots
	if !rl.AcquireConcurrent() {
		t.Fatal("expected 1st slot to be available")
	}
	if !rl.AcquireConcurrent() {
		t.Fatal("expected 2nd slot to be available")
	}

	// 3rd should fail
	if rl.AcquireConcurrent() {
		t.Error("expected 3rd concurrent request to be rejected")
	}

	// Release one and try again
	rl.ReleaseConcurrent()
	if !rl.AcquireConcurrent() {
		t.Error("expected slot to be available after release")
	}

	rl.ReleaseConcurrent()
	rl.ReleaseConcurrent()
}

// TestCircuitBreaker_StartsClose verifies initial state is closed.
func TestCircuitBreaker_StartsClosed(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 1*time.Second)
	if cb.State() != "closed" {
		t.Errorf("expected closed, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Error("expected requests allowed when closed")
	}
}

// TestCircuitBreaker_OpensAfterFailures verifies transition to open state.
func TestCircuitBreaker_OpensAfterFailures(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 100*time.Millisecond)

	// Record 3 failures → should open
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != "closed" {
		t.Errorf("expected still closed after 2 failures, got %s", cb.State())
	}

	cb.RecordFailure() // 3rd failure
	if cb.State() != "open" {
		t.Errorf("expected open after 3 failures, got %s", cb.State())
	}

	// Requests should be rejected while open
	if cb.Allow() {
		t.Error("expected requests rejected when open")
	}

	if cb.TripsTotal.Load() != 1 {
		t.Errorf("expected 1 trip, got %d", cb.TripsTotal.Load())
	}
}

// TestCircuitBreaker_TransitionsToHalfOpen verifies recovery probing.
func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 2, 50*time.Millisecond)

	cb.RecordFailure() // opens
	if cb.State() != "open" {
		t.Fatalf("expected open, got %s", cb.State())
	}

	// Wait for open duration to expire
	time.Sleep(60 * time.Millisecond)

	// Should transition to half_open and allow a probe
	if !cb.Allow() {
		t.Error("expected probe request allowed after open duration expires")
	}
	if cb.State() != "half_open" {
		t.Errorf("expected half_open, got %s", cb.State())
	}

	// Successful probes → close
	cb.RecordSuccess()
	cb.RecordSuccess()
	if cb.State() != "closed" {
		t.Errorf("expected closed after 2 successful probes, got %s", cb.State())
	}
}

// TestCircuitBreaker_HalfOpenFailureReopens verifies that a failure
// during half-open reopens the circuit.
func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(1, 2, 50*time.Millisecond)

	cb.RecordFailure() // opens
	time.Sleep(60 * time.Millisecond)
	cb.Allow() // transitions to half_open

	cb.RecordFailure() // probe fails → back to open
	if cb.State() != "open" {
		t.Errorf("expected open after half-open failure, got %s", cb.State())
	}
}

// TestCircuitBreaker_SuccessResets verifies success resets failure count.
func TestCircuitBreaker_SuccessResets(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 1*time.Second)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess() // should reset failures to 0

	// 2 more failures — should NOT open (only 2, not 3)
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != "closed" {
		t.Errorf("expected closed (reset after success), got %s", cb.State())
	}
}

// TestCoalescer_ErrorPropagation verifies errors are returned to all waiters.
func TestCoalescer_ErrorPropagation(t *testing.T) {
	c := NewCoalescer()
	var wg sync.WaitGroup
	errCount := atomic.Int32{}

	wg.Add(5)
	for range 5 {
		go func() {
			defer wg.Done()
			_, _, _, err := c.Do("error-key", func() (*http.Response, error) {
				time.Sleep(20 * time.Millisecond)
				return nil, fmt.Errorf("backend error")
			})
			if err != nil {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if errCount.Load() != 5 {
		t.Errorf("expected all 5 callers to get error, got %d", errCount.Load())
	}
}

// TestRateLimiter_Middleware_Integration verifies HTTP middleware behavior.
func TestRateLimiter_Middleware_Integration(t *testing.T) {
	rl := NewRateLimiter(1, 100, 200) // max 1 concurrent

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(200)
	}))

	// First request should succeed
	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest("GET", "/test", nil)

	// Hold the slot
	go handler.ServeHTTP(w1, r1)
	time.Sleep(10 * time.Millisecond) // let it acquire

	// Second request should be rejected (max 1 concurrent)
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/test", nil)
	handler.ServeHTTP(w2, r2)

	if w2.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w2.Code)
	}
}

// TestRateLimiter_ClientID verifies client ID extraction.
func TestRateLimiter_ClientID(t *testing.T) {
	// With X-Forwarded-For
	r1, _ := http.NewRequest("GET", "/", nil)
	r1.Header.Set("X-Forwarded-For", "10.0.0.1")
	if ClientID(r1) != "10.0.0.1" {
		t.Errorf("expected X-Forwarded-For, got %q", ClientID(r1))
	}

	// Without — falls back to RemoteAddr
	r2, _ := http.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "192.168.1.1:12345"
	if ClientID(r2) != "192.168.1.1:12345" {
		t.Errorf("expected RemoteAddr, got %q", ClientID(r2))
	}
}

// TestRateLimiter_TokenRefill verifies tokens refill over time.
func TestRateLimiter_TokenRefill(t *testing.T) {
	rl := NewRateLimiter(100, 10, 2) // 10 req/s, burst 2

	// Exhaust burst
	rl.AllowClient("client")
	rl.AllowClient("client")
	if rl.AllowClient("client") {
		t.Error("expected rejection after burst")
	}

	// Wait for refill
	time.Sleep(200 * time.Millisecond)
	if !rl.AllowClient("client") {
		t.Error("expected token refill after wait")
	}
}

// TestCircuitBreaker_State verifies state string output.
func TestCircuitBreaker_StateStrings(t *testing.T) {
	cb := NewCircuitBreaker(1, 1, 50*time.Millisecond)

	if cb.State() != "closed" {
		t.Errorf("expected closed, got %s", cb.State())
	}

	cb.RecordFailure()
	if cb.State() != "open" {
		t.Errorf("expected open, got %s", cb.State())
	}

	time.Sleep(60 * time.Millisecond)
	cb.Allow() // transitions to half_open
	if cb.State() != "half_open" {
		t.Errorf("expected half_open, got %s", cb.State())
	}

	cb.RecordSuccess()
	if cb.State() != "closed" {
		t.Errorf("expected closed after success, got %s", cb.State())
	}
}

// TestRateLimiter_Middleware_RateLimitResponse verifies rate limit 429 response.
func TestRateLimiter_Middleware_RateLimitResponse(t *testing.T) {
	rl := NewRateLimiter(100, 1, 1) // 1 req/s, burst 1

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	// First request — OK
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, httptest.NewRequest("GET", "/test", nil))
	if w1.Code != 200 {
		t.Errorf("expected 200, got %d", w1.Code)
	}

	// Second request — rate limited
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, httptest.NewRequest("GET", "/test", nil))
	if w2.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", w2.Code)
	}
}

// TestRateLimiter_NoLimits verifies zero limits disable limiting.
func TestRateLimiter_NoLimits(t *testing.T) {
	rl := NewRateLimiter(0, 0, 0)

	for range 100 {
		if !rl.AllowClient("x") {
			t.Fatal("expected no rate limit when ratePerSecond=0")
		}
		if !rl.AcquireConcurrent() {
			t.Fatal("expected no concurrent limit when maxConcurrent=0")
		}
	}
}

// TestCircuitBreaker_HalfOpenLimitsProbes verifies that half-open state
// only allows successThreshold probe requests, not unlimited.
func TestCircuitBreaker_HalfOpenLimitsProbes(t *testing.T) {
	cb := NewCircuitBreaker(1, 2, 50*time.Millisecond)
	cb.RecordFailure() // opens

	time.Sleep(60 * time.Millisecond)

	// First Allow() transitions to half-open and allows probe 1
	if !cb.Allow() {
		t.Fatal("expected first probe to be allowed")
	}
	// Second probe allowed (successThreshold=2)
	if !cb.Allow() {
		t.Fatal("expected second probe to be allowed")
	}
	// Third probe should be rejected (only 2 probes allowed)
	if cb.Allow() {
		t.Error("expected third probe to be rejected in half-open (limit=2)")
	}
}

// TestCircuitBreaker_AllowOpenTimeout verifies requests rejected while open.
func TestCircuitBreaker_AllowOpenTimeout(t *testing.T) {
	cb := NewCircuitBreaker(1, 1, 100*time.Millisecond)
	cb.RecordFailure() // opens

	// Should be rejected
	for range 5 {
		if cb.Allow() {
			t.Error("expected rejected while open")
		}
	}
}

// TestRequestKey verifies key generation consistency.
func TestRequestKey(t *testing.T) {
	r1, _ := http.NewRequest("GET", "/loki/api/v1/labels?start=1&end=2", nil)
	r2, _ := http.NewRequest("GET", "/loki/api/v1/labels?start=1&end=2", nil)
	r3, _ := http.NewRequest("GET", "/loki/api/v1/labels?start=3&end=4", nil)

	k1 := RequestKey(r1)
	k2 := RequestKey(r2)
	k3 := RequestKey(r3)

	if k1 != k2 {
		t.Errorf("identical requests should produce same key: %s != %s", k1, k2)
	}
	if k1 == k3 {
		t.Error("different requests should produce different keys")
	}
}

// TestRequestKey_TenantIsolation verifies different tenants produce different keys
// even with identical queries, preventing cross-tenant data leaks.
func TestRequestKey_TenantIsolation(t *testing.T) {
	r1, _ := http.NewRequest("GET", "/loki/api/v1/query_range?query={app%3D%22nginx%22}", nil)
	r1.Header.Set("X-Scope-OrgID", "tenant-a")

	r2, _ := http.NewRequest("GET", "/loki/api/v1/query_range?query={app%3D%22nginx%22}", nil)
	r2.Header.Set("X-Scope-OrgID", "tenant-b")

	r3, _ := http.NewRequest("GET", "/loki/api/v1/query_range?query={app%3D%22nginx%22}", nil)
	r3.Header.Set("X-Scope-OrgID", "tenant-a")

	k1 := RequestKey(r1)
	k2 := RequestKey(r2)
	k3 := RequestKey(r3)

	if k1 == k2 {
		t.Error("different tenants with same query must produce different keys")
	}
	if k1 != k3 {
		t.Errorf("same tenant + same query should produce same key: %s != %s", k1, k3)
	}
}

// TestCoalescer_TenantIsolation verifies concurrent requests from different tenants
// are NOT coalesced together — each gets its own backend call.
func TestCoalescer_TenantIsolation(t *testing.T) {
	c := NewCoalescer()
	var tenantACalls, tenantBCalls atomic.Int32
	var wg sync.WaitGroup

	wg.Add(2)

	// Tenant A
	go func() {
		defer wg.Done()
		c.Do("tenant-a:query", func() (*http.Response, error) {
			tenantACalls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{},
				Body:       http.NoBody,
			}, nil
		})
	}()

	// Tenant B — different key, must get separate backend call
	go func() {
		defer wg.Done()
		c.Do("tenant-b:query", func() (*http.Response, error) {
			tenantBCalls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{},
				Body:       http.NoBody,
			}, nil
		})
	}()

	wg.Wait()

	if tenantACalls.Load() != 1 {
		t.Errorf("expected 1 backend call for tenant-a, got %d", tenantACalls.Load())
	}
	if tenantBCalls.Load() != 1 {
		t.Errorf("expected 1 backend call for tenant-b, got %d", tenantBCalls.Load())
	}
}
