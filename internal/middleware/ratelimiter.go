package middleware

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	rateLimiterCleanupInterval = 5 * time.Minute
	rateLimiterStaleAfter      = 10 * time.Minute
)

// RateLimiter provides per-client token bucket rate limiting
// and a global concurrent query semaphore.
type RateLimiter struct {
	clients sync.Map // string → *tokenBucket (lock-free reads, per-bucket mutex)

	// Global concurrent query limit
	maxConcurrent int
	active        atomic.Int32

	// Per-client config
	ratePerSecond float64
	burstSize     int

	// Cleanup timing captured at construction time so tests do not race on package globals.
	cleanupInterval time.Duration
	staleAfter      time.Duration

	// Shutdown signal for cleanup goroutine
	done chan struct{}

	// Stats
	RejectedTotal  atomic.Int64
	ThrottledTotal atomic.Int64
}

type tokenBucket struct {
	mu        sync.Mutex
	tokens    float64
	lastTime  time.Time
	rate      float64
	burstSize int
}

func NewRateLimiter(maxConcurrent int, ratePerSecond float64, burstSize int) *RateLimiter {
	rl := &RateLimiter{
		maxConcurrent:   maxConcurrent,
		ratePerSecond:   ratePerSecond,
		burstSize:       burstSize,
		cleanupInterval: rateLimiterCleanupInterval,
		staleAfter:      rateLimiterStaleAfter,
		done:            make(chan struct{}),
	}
	go rl.cleanupStaleClients()
	return rl
}

// Stop shuts down the cleanup goroutine. Call on config reload before creating a new RateLimiter.
func (rl *RateLimiter) Stop() {
	close(rl.done)
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
// Uses sync.Map for lock-free client lookup; only the per-bucket mutex is held.
func (rl *RateLimiter) AllowClient(clientID string) bool {
	if rl.ratePerSecond <= 0 {
		return true
	}

	val, _ := rl.clients.LoadOrStore(clientID, &tokenBucket{
		tokens:    float64(rl.burstSize),
		lastTime:  time.Now(),
		rate:      rl.ratePerSecond,
		burstSize: rl.burstSize,
	})
	bucket := val.(*tokenBucket)

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

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
// Uses RemoteAddr by default for security — X-Forwarded-For is attacker-controlled
// and can be spoofed to bypass rate limiting entirely.
func ClientID(r *http.Request) string {
	// Use RemoteAddr (connection-level, not spoofable) as the authoritative identifier.
	// Strip the port suffix for consistent bucketing (e.g., "10.0.1.42:5678" → "10.0.1.42").
	addr := r.RemoteAddr
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// Middleware wraps an http.Handler with rate limiting and concurrency control.
// When both the per-client rate limit and global concurrency limit are disabled
// (i.e., both are ≤ 0), this returns next directly with zero overhead.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	if rl.maxConcurrent <= 0 && rl.ratePerSecond <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := ClientID(r)

		if !rl.AllowClient(clientID) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%.0f", rl.ratePerSecond))
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"status":"error","error":"rate limit exceeded"}`))
			return
		}

		if !rl.AcquireConcurrent() {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"error","error":"too many concurrent queries"}`))
			return
		}
		defer rl.ReleaseConcurrent()

		next.ServeHTTP(w, r)
	})
}

func (rl *RateLimiter) cleanupStaleClients() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rl.done:
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-rl.staleAfter)
			rl.clients.Range(func(key, value any) bool {
				b := value.(*tokenBucket)
				b.mu.Lock()
				stale := b.lastTime.Before(cutoff)
				b.mu.Unlock()
				if stale {
					rl.clients.Delete(key)
				}
				return true
			})
		}
	}
}
