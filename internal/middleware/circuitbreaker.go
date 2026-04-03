package middleware

import (
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreaker protects the VL backend from cascading failures.
// States: Closed (normal) → Open (failing) → HalfOpen (probing).
type CircuitBreaker struct {
	mu sync.Mutex

	state       cbState
	failures    int
	successes   int
	lastFailure time.Time
	openUntil   time.Time

	// Config
	failureThreshold int           // failures before opening
	successThreshold int           // successes in half-open before closing
	openDuration     time.Duration // how long to stay open before probing

	// Stats
	TripsTotal atomic.Int64
}

type cbState int

const (
	cbClosed   cbState = iota
	cbOpen
	cbHalfOpen
)

func NewCircuitBreaker(failureThreshold, successThreshold int, openDuration time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            cbClosed,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		openDuration:     openDuration,
	}
}

// Allow checks if a request should be allowed through.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbClosed:
		return true
	case cbOpen:
		if time.Now().After(cb.openUntil) {
			cb.state = cbHalfOpen
			cb.successes = 0
			return true // allow probe request
		}
		return false
	case cbHalfOpen:
		return true // allow probe requests
	}
	return true
}

// RecordSuccess records a successful backend response.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbHalfOpen:
		cb.successes++
		if cb.successes >= cb.successThreshold {
			cb.state = cbClosed
			cb.failures = 0
		}
	case cbClosed:
		cb.failures = 0 // reset consecutive failures
	}
}

// RecordFailure records a failed backend response.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailure = time.Now()

	switch cb.state {
	case cbClosed:
		cb.failures++
		if cb.failures >= cb.failureThreshold {
			cb.state = cbOpen
			cb.openUntil = time.Now().Add(cb.openDuration)
			cb.TripsTotal.Add(1)
		}
	case cbHalfOpen:
		// Probe failed — go back to open
		cb.state = cbOpen
		cb.openUntil = time.Now().Add(cb.openDuration)
		cb.TripsTotal.Add(1)
	}
}

// State returns the current state as a string.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbClosed:
		return "closed"
	case cbOpen:
		return "open"
	case cbHalfOpen:
		return "half_open"
	}
	return "unknown"
}
