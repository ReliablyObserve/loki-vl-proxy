package middleware

import (
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreaker protects the VL backend from cascading failures.
// States: Closed (normal) → Open (failing) → HalfOpen (probing).
//
// Failure counting uses a sliding time window rather than consecutive
// failures. This prevents sporadic slow-query connection resets (which look
// like transport errors) from opening the breaker when VL is healthy overall.
// Only a burst of N failures within windowDuration opens the circuit.
//
// Hot-path optimization: atomicState allows Allow() and RecordSuccess() to
// bypass the mutex entirely in the closed state (the 99.9% common case).
// State transitions always go through mu and then publish via atomicState.
type CircuitBreaker struct {
	mu sync.Mutex

	atomicState    atomic.Int32 // shadow of state for lock-free read on hot path
	state          cbState
	failureTimes   []time.Time // ring of recent failure timestamps (pruned by window)
	successes      int
	halfOpenProbes int // number of probes allowed in half-open
	openUntil      time.Time

	// Config
	failureThreshold int           // failures within window before opening
	windowDuration   time.Duration // sliding window for failure counting
	successThreshold int           // successes in half-open before closing
	openDuration     time.Duration // how long to stay open before probing

	// Stats
	TripsTotal atomic.Int64
}

type cbState int

const (
	cbClosed cbState = iota
	cbOpen
	cbHalfOpen
)

// NewCircuitBreaker creates a circuit breaker.
// failureThreshold: failures within windowDuration before opening.
// successThreshold: consecutive successes in half-open before closing.
// openDuration: how long to stay open before probing.
// windowDuration: sliding window for failure rate (0 defaults to 30s).
func NewCircuitBreaker(failureThreshold, successThreshold int, openDuration, windowDuration time.Duration) *CircuitBreaker {
	if windowDuration <= 0 {
		windowDuration = 30 * time.Second
	}
	return &CircuitBreaker{
		state:            cbClosed,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		openDuration:     openDuration,
		windowDuration:   windowDuration,
	}
}

// Allow checks if a request should be allowed through.
func (cb *CircuitBreaker) Allow() bool {
	if cbState(cb.atomicState.Load()) == cbClosed {
		return true
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbClosed:
		return true
	case cbOpen:
		if time.Now().After(cb.openUntil) {
			cb.state = cbHalfOpen
			cb.atomicState.Store(int32(cbHalfOpen))
			cb.successes = 0
			cb.halfOpenProbes = 1
			return true
		}
		return false
	case cbHalfOpen:
		if cb.halfOpenProbes < cb.successThreshold {
			cb.halfOpenProbes++
			return true
		}
		return false
	}
	return true
}

// RecordSuccess records a successful backend response.
func (cb *CircuitBreaker) RecordSuccess() {
	if cbState(cb.atomicState.Load()) == cbClosed {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbHalfOpen:
		cb.successes++
		if cb.successes >= cb.successThreshold {
			cb.state = cbClosed
			cb.atomicState.Store(int32(cbClosed))
			cb.failureTimes = cb.failureTimes[:0]
		}
	case cbClosed:
		// Pruning deferred to failure path — not needed on success hot path.
	}
}

// RecordFailure records a failed backend response (transport-level errors only;
// callers must exclude timeouts and context cancellations before calling this).
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbClosed:
		cb.pruneWindow()
		cb.failureTimes = append(cb.failureTimes, time.Now())
		if len(cb.failureTimes) >= cb.failureThreshold {
			cb.state = cbOpen
			cb.atomicState.Store(int32(cbOpen))
			cb.openUntil = time.Now().Add(cb.openDuration)
			cb.TripsTotal.Add(1)
		}
	case cbHalfOpen:
		cb.state = cbOpen
		cb.atomicState.Store(int32(cbOpen))
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

// pruneWindow removes failure timestamps older than windowDuration.
// Must be called with mu held.
func (cb *CircuitBreaker) pruneWindow() {
	cutoff := time.Now().Add(-cb.windowDuration)
	i := 0
	for i < len(cb.failureTimes) && cb.failureTimes[i].Before(cutoff) {
		i++
	}
	cb.failureTimes = cb.failureTimes[i:]
}
