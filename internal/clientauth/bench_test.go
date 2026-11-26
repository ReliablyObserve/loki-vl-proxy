package clientauth

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// guesses numbers the wrong passwords of the flood benchmark.
var guesses atomic.Int64

// benchUsers is one basic user at cost and one bearer user.
func benchUsers(b *testing.B, cost int) *Authenticator {
	b.Helper()
	data := fmt.Appendf(nil, "users:\n  - name: grafana\n    password_hash: %q\n  - name: ci\n    token_sha256: %q\n", hashOf(b, "secret", cost), tokenDigest("ci-token"))
	return mustAuth(b, data, Options{CacheTTL: time.Minute, MaxConcurrentVerifications: 64})
}

// BenchmarkAuthenticate measures what caller authentication costs a request:
// a cached basic credential (every Grafana request after the first), a basic
// credential verified with bcrypt at cost 10 and 12 (the first request, or
// every request with -server.auth-cache-ttl=0), a bearer token, and a flood of
// distinct wrong passwords (each one a bcrypt verification).
func BenchmarkAuthenticate(b *testing.B) {
	ctx := context.Background()
	for _, cost := range []int{10, 12} {
		a := benchUsers(b, cost)
		ok := basicHeader("grafana", "secret")
		b.Run(fmt.Sprintf("basic_cached_cost%d", cost), func(b *testing.B) {
			a.Authenticate(ctx, ok)
			b.ReportAllocs()
			for b.Loop() {
				if _, r := a.Authenticate(ctx, ok); r != ReasonNone {
					b.Fatal(r)
				}
			}
		})
		b.Run(fmt.Sprintf("basic_bcrypt_cost%d", cost), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				a.ok.clear()
				if _, r := a.Authenticate(ctx, ok); r != ReasonNone {
					b.Fatal(r)
				}
			}
		})
		b.Run(fmt.Sprintf("basic_wrong_distinct_cost%d", cost), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				// Never repeated, even across -count runs: each one is a bcrypt run.
				if _, r := a.Authenticate(ctx, basicHeader("grafana", fmt.Sprint("guess-", guesses.Add(1)))); r != ReasonInvalid {
					b.Fatal(r)
				}
			}
		})
	}
	a := benchUsers(b, 10)
	bearer := []string{"Bearer ci-token"}
	b.Run("bearer", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, r := a.Authenticate(ctx, bearer); r != ReasonNone {
				b.Fatal(r)
			}
		}
	})
}
