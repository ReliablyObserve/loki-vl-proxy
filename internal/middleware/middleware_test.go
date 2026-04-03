package middleware

import (
	"fmt"
	"net/http"
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
