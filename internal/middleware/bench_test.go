package middleware

import (
	"testing"
	"time"
)

func BenchmarkCircuitBreakerAllow_Closed(b *testing.B) {
	cb := NewCircuitBreaker(5, 3, 10*time.Second, 30*time.Second)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Allow()
		}
	})
}

func BenchmarkCircuitBreakerRecordSuccess_Closed(b *testing.B) {
	cb := NewCircuitBreaker(5, 3, 10*time.Second, 30*time.Second)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.RecordSuccess()
		}
	})
}

func BenchmarkRateLimiterAllowClient(b *testing.B) {
	rl := NewRateLimiter(0, 1000000, 1000000)
	defer rl.Stop()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rl.AllowClient("10.0.0.1")
		}
	})
}

func BenchmarkRateLimiterAllowClient_MultiClient(b *testing.B) {
	rl := NewRateLimiter(0, 1000000, 1000000)
	defer rl.Stop()
	clients := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5",
		"10.0.0.6", "10.0.0.7", "10.0.0.8", "10.0.0.9", "10.0.0.10"}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rl.AllowClient(clients[i%len(clients)])
			i++
		}
	})
}
