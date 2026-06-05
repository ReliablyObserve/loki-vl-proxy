package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// TestBackendVersionStrict_HardFailsOnSubMin asserts that when
// --backend-version-strict=true is set, a backend reporting a semver below the
// configured minimum causes ValidateBackendVersionCompatibility to return an
// error (versus the default soft mode, which only warns).
func TestBackendVersionStrict_HardFailsOnSubMin(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Server", "VictoriaLogs/v1.10.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	// Strict mode → error.
	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
		BackendVersionStrict:       true,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err == nil {
		t.Fatalf("expected strict mode to return error for sub-min backend version, got nil")
	}

	// Soft mode (default) → no error (existing behavior already covered by the
	// non-strict suite, but we re-assert here to keep the strict/soft contrast
	// in one place and guard against regressions in either direction).
	pSoft, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy (soft): %v", err)
	}
	if err := pSoft.ValidateBackendVersionCompatibility(context.Background()); err == nil {
		// NOTE: the existing soft path returns an error too for sub-min unless
		// BackendAllowUnsupportedVersion is set. Strict mode adds the *additional*
		// failure modes (health probe failure, non-2xx, missing semver). Confirm
		// the existing behavior is still surfaced.
		t.Fatalf("expected soft mode to also return error for sub-min (existing behavior), got nil")
	}
}

// TestBackendVersionStrict_HealthFailureHardFails asserts that an unreachable
// /health endpoint causes ValidateBackendVersionCompatibility to return an
// error in strict mode (versus the default soft mode, which skips the check).
func TestBackendVersionStrict_HealthFailureHardFails(t *testing.T) {
	// 127.0.0.1:65535 is reserved and reliably refuses TCP — guaranteed health
	// probe failure without depending on outbound DNS or firewalls.
	const unreachable = "http://127.0.0.1:65535"

	// Strict mode → error.
	pStrict, err := New(Config{
		BackendURL:                 unreachable,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: 200 * time.Millisecond,
		BackendVersionStrict:       true,
	})
	if err != nil {
		t.Fatalf("new proxy (strict): %v", err)
	}
	gotErr := pStrict.ValidateBackendVersionCompatibility(context.Background())
	if gotErr == nil {
		t.Fatalf("expected strict mode to return error for unreachable /health, got nil")
	}
	if !strings.Contains(gotErr.Error(), "strict") {
		t.Fatalf("expected strict-mode error to mention strict, got: %v", gotErr)
	}

	// Soft mode → no error (existing behavior).
	pSoft, err := New(Config{
		BackendURL:                 unreachable,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new proxy (soft): %v", err)
	}
	if err := pSoft.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected soft mode to skip unreachable /health, got: %v", err)
	}
}

// TestBackendVersionStrict_HealthNon2xxHardFails asserts that a non-2xx
// /health response causes ValidateBackendVersionCompatibility to return an
// error in strict mode (versus the default soft mode, which skips the check).
func TestBackendVersionStrict_HealthNon2xxHardFails(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Server", "VictoriaLogs/v1.50.0")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("unavailable"))
	}))
	defer backend.Close()

	// Strict mode → error.
	pStrict, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
		BackendVersionStrict:       true,
	})
	if err != nil {
		t.Fatalf("new proxy (strict): %v", err)
	}
	gotErr := pStrict.ValidateBackendVersionCompatibility(context.Background())
	if gotErr == nil {
		t.Fatalf("expected strict mode to return error for /health 503, got nil")
	}
	if !strings.Contains(gotErr.Error(), "503") {
		t.Fatalf("expected strict-mode error to name the offending status (503), got: %v", gotErr)
	}

	// Soft mode → no error.
	pSoft, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy (soft): %v", err)
	}
	if err := pSoft.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected soft mode to skip non-2xx /health, got: %v", err)
	}
}

// TestBackendVersionStrict_MissingSemverHardFails asserts that a backend
// /health that returns 200 but never reveals a semver (no version headers and
// no /metrics version exposition) causes strict mode to return an error.
func TestBackendVersionStrict_MissingSemverHardFails(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case "/metrics":
			// No vm_app_version / victorialogs_build_info — no semver detectable.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("# HELP up\n# TYPE up gauge\nup 1\n"))
		default:
			// stream_field_names / field_names endpoint probes — return 404 so
			// the capability profile stays empty too (worst case: nothing known).
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backend.Close()

	pStrict, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
		BackendVersionStrict:       true,
	})
	if err != nil {
		t.Fatalf("new proxy (strict): %v", err)
	}
	if err := pStrict.ValidateBackendVersionCompatibility(context.Background()); err == nil {
		t.Fatalf("expected strict mode to return error when backend exposes no semver, got nil")
	}
}

// TestBackendVersionStrict_HealthyBackendPasses asserts that a healthy backend
// reporting an in-min version passes ValidateBackendVersionCompatibility under
// strict mode — i.e. strict mode does not introduce spurious failures.
func TestBackendVersionStrict_HealthyBackendPasses(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Server", "VictoriaLogs/v1.50.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
		BackendVersionStrict:       true,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected strict mode to pass for healthy in-min backend, got: %v", err)
	}
}
