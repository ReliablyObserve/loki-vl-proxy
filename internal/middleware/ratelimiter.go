package middleware

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
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

// RejectFunc writes the answer to a request the limiter turned away: Loki's
// queue-full error, typed, with the flag that set the limit in its Limit (for
// the log line only). It is given per wrapped handler, never kept on the
// limiter: the limiter's cleanup goroutine runs until Stop, and a writer that
// refers to its owner would keep that owner alive with it.
type RejectFunc func(w http.ResponseWriter, r *http.Request, err *lokierr.Error)

// Flags behind the two admission limits, for the log line of a rejection.
const (
	flagRatePerSecond = "-rate-limit-per-second"
	flagMaxConcurrent = "-max-concurrent"
)

var (
	errRateLimited       = errors.New("per-client rate limit exceeded")
	errTooManyConcurrent = errors.New("too many concurrent queries")
)

// rejectQueueFull answers a turned-away request with Loki's queue-full status
// and text for both limits, when the caller gives no writer of its own.
func rejectQueueFull(w http.ResponseWriter, _ *http.Request, err *lokierr.Error) {
	// The message alone as text/plain, as Loki writes every error
	// (pkg/util/server/error.go WriteError).
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(err.Status())
	_, _ = io.WriteString(w, err.Text())
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
// reject writes the answer to a turned-away request (nil: Loki's queue-full
// answer as text); which limit it was goes to the log line only.
func (rl *RateLimiter) Middleware(next http.Handler, reject RejectFunc) http.Handler {
	if rl.maxConcurrent <= 0 && rl.ratePerSecond <= 0 {
		return next
	}
	if reject == nil {
		reject = rejectQueueFull
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := ClientID(r)

		if !rl.AllowClient(clientID) {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%.0f", rl.ratePerSecond))
			w.Header().Set("X-RateLimit-Remaining", "0")
			reject(w, r, lokierr.QueueFullAt(flagRatePerSecond, int64(rl.ratePerSecond), errRateLimited))
			return
		}

		if !rl.AcquireConcurrent() {
			w.Header().Set("Retry-After", "5")
			reject(w, r, lokierr.QueueFullAt(flagMaxConcurrent, int64(rl.maxConcurrent), errTooManyConcurrent))
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
