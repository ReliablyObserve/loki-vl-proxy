package middleware

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimiter provides per-client token bucket rate limiting
// and a global concurrent query semaphore.
type RateLimiter struct {
	mu      sync.Mutex
	clients map[string]*tokenBucket

	// Global concurrent query limit
	maxConcurrent int
	active        atomic.Int32

	// Per-client config
	ratePerSecond float64
	burstSize     int

	// Stats
	RejectedTotal atomic.Int64
	ThrottledTotal atomic.Int64
}

type tokenBucket struct {
	tokens    float64
	lastTime  time.Time
	rate      float64
	burstSize int
}

func NewRateLimiter(maxConcurrent int, ratePerSecond float64, burstSize int) *RateLimiter {
	rl := &RateLimiter{
		clients:       make(map[string]*tokenBucket),
		maxConcurrent: maxConcurrent,
		ratePerSecond: ratePerSecond,
		burstSize:     burstSize,
	}
	go rl.cleanupStaleClients()
	return rl
}

// AcquireConcurrent tries to acquire a concurrent query slot.
// Returns false if the global limit is exceeded.
func (rl *RateLimiter) AcquireConcurrent() bool {
	if rl.maxConcurrent <= 0 {
		return true // no limit
	}
	current := rl.active.Add(1)
	if int(current) > rl.maxConcurrent {
		rl.active.Add(-1)
		rl.ThrottledTotal.Add(1)
		return false
	}
	return true
}

// ReleaseConcurrent releases a concurrent query slot.
func (rl *RateLimiter) ReleaseConcurrent() {
	rl.active.Add(-1)
}

// AllowClient checks if the client (by IP or key) is within rate limits.
func (rl *RateLimiter) AllowClient(clientID string) bool {
	if rl.ratePerSecond <= 0 {
		return true // no limit
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, ok := rl.clients[clientID]
	if !ok {
		bucket = &tokenBucket{
			tokens:    float64(rl.burstSize),
			lastTime:  time.Now(),
			rate:      rl.ratePerSecond,
			burstSize: rl.burstSize,
		}
		rl.clients[clientID] = bucket
	}

	now := time.Now()
	elapsed := now.Sub(bucket.lastTime).Seconds()
	bucket.tokens += elapsed * bucket.rate
	if bucket.tokens > float64(bucket.burstSize) {
		bucket.tokens = float64(bucket.burstSize)
	}
	bucket.lastTime = now

	if bucket.tokens < 1 {
		rl.RejectedTotal.Add(1)
		return false
	}

	bucket.tokens--
	return true
}

// ClientID extracts a client identifier from an HTTP request.
func ClientID(r *http.Request) string {
	// Prefer X-Forwarded-For, fall back to RemoteAddr
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	return r.RemoteAddr
}

// Middleware wraps an http.Handler with rate limiting and concurrency control.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := ClientID(r)

		if !rl.AllowClient(clientID) {
			http.Error(w, `{"status":"error","error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}

		if !rl.AcquireConcurrent() {
			http.Error(w, `{"status":"error","error":"too many concurrent queries"}`, http.StatusServiceUnavailable)
			return
		}
		defer rl.ReleaseConcurrent()

		next.ServeHTTP(w, r)
	})
}

func (rl *RateLimiter) cleanupStaleClients() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		cutoff := time.Now().Add(-10 * time.Minute)
		for k, b := range rl.clients {
			if b.lastTime.Before(cutoff) {
				delete(rl.clients, k)
			}
		}
		rl.mu.Unlock()
	}
}
