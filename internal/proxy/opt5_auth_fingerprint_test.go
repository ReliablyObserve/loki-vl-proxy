package proxy

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newOpt5Proxy(t testing.TB) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:     "http://unused",
		Cache:          cache.New(30*time.Second, 100),
		LogLevel:       "error",
		ForwardHeaders: []string{"X-Auth-Token"},
		ForwardCookies: []string{"session"},
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

// TestOPT5_FingerprintFromCtx_HitsCache verifies that fingerprintFromCtx returns
// the precomputed value from context without calling forwardedAuthFingerprint again.
func TestOPT5_FingerprintFromCtx_HitsCache(t *testing.T) {
	p := newOpt5Proxy(t)
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Auth-Token", "tok-abc")
	r.Header.Set("Cookie", "session=secret-session-value")

	// Inject fingerprint into context.
	r2 := p.injectAuthFingerprint(r)

	fp1 := p.fingerprintFromCtx(r2.Context(), r2)
	fp2 := p.fingerprintFromCtx(r2.Context(), r2)
	fp3 := p.fingerprintFromCtx(r2.Context(), r2)

	if fp1 == "" {
		t.Fatal("expected non-empty fingerprint with forward headers/cookies configured")
	}
	if fp1 != fp2 || fp2 != fp3 {
		t.Fatalf("fingerprint not stable: %q %q %q", fp1, fp2, fp3)
	}

	// Must match the direct computation.
	direct := p.forwardedAuthFingerprint(r)
	if fp1 != direct {
		t.Errorf("memoized %q != direct %q", fp1, direct)
	}
}

// TestOPT5_FingerprintFromCtx_FallbackWithoutInject verifies that fingerprintFromCtx
// falls back to live computation when injectAuthFingerprint was not called.
func TestOPT5_FingerprintFromCtx_FallbackWithoutInject(t *testing.T) {
	p := newOpt5Proxy(t)
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Auth-Token", "tok-xyz")

	// No injectAuthFingerprint — should fall back to live computation.
	got := p.fingerprintFromCtx(r.Context(), r)
	direct := p.forwardedAuthFingerprint(r)
	if got != direct {
		t.Errorf("fallback %q != direct %q", got, direct)
	}
}

// TestOPT5_FingerprintEmpty_NoForwardConfig verifies fingerprint is "" when no
// forwarding is configured (the fast early-return path in forwardedAuthFingerprint).
func TestOPT5_FingerprintEmpty_NoForwardConfig(t *testing.T) {
	p, _ := New(Config{
		BackendURL: "http://unused",
		Cache:      cache.New(30*time.Second, 100),
		LogLevel:   "error",
		// No ForwardHeaders or ForwardCookies
	})
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Auth-Token", "tok-abc")
	r2 := p.injectAuthFingerprint(r)
	if got := p.fingerprintFromCtx(r2.Context(), r2); got != "" {
		t.Errorf("expected empty fingerprint with no forwarding config, got %q", got)
	}
}

// BenchmarkOPT5_AuthFingerprint_LiveVsMemoized compares live computation against
// the O(1) context-lookup fast path. Threshold for memoized: < 50 ns/op.
func BenchmarkOPT5_AuthFingerprint_LiveVsMemoized(b *testing.B) {
	p := newOpt5Proxy(b)
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Auth-Token", "tok-abc")
	r.Header.Set("Cookie", "session=secret-session-value; other=x")

	r2 := p.injectAuthFingerprint(r)
	ctx := r2.Context()

	b.Run("memoized", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = p.fingerprintFromCtx(ctx, r2)
		}
	})
	b.Run("live", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = p.forwardedAuthFingerprint(r)
		}
	})
}
