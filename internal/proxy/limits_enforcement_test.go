package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestEffectiveMaxQueryLength_ZeroDefault(t *testing.T) {
	p := &Proxy{defaultMaxQueryLength: 0}
	got := p.effectiveMaxQueryLength("tenant-a")
	if got != 0 {
		t.Errorf("want 0 (unlimited), got %v", got)
	}
}

func TestEffectiveMaxQueryLength_FlagDefault(t *testing.T) {
	p := &Proxy{defaultMaxQueryLength: 7 * 24 * time.Hour}
	p.tenantLimits = map[string]map[string]any{}
	p.tenantDefaultLimits = map[string]any{}
	got := p.effectiveMaxQueryLength("tenant-a")
	if got != 7*24*time.Hour {
		t.Errorf("want 168h, got %v", got)
	}
}

func TestEffectiveMaxQueryLength_TenantOverride(t *testing.T) {
	p := &Proxy{defaultMaxQueryLength: 7 * 24 * time.Hour}
	p.tenantLimits = map[string]map[string]any{
		"tenant-a": {"max_query_length": "1h"},
	}
	p.tenantDefaultLimits = map[string]any{}
	got := p.effectiveMaxQueryLength("tenant-a")
	if got != time.Hour {
		t.Errorf("want 1h, got %v", got)
	}
}

func TestEffectiveMaxQueryLength_DefaultLimitOverride(t *testing.T) {
	p := &Proxy{defaultMaxQueryLength: 0}
	p.tenantLimits = map[string]map[string]any{}
	p.tenantDefaultLimits = map[string]any{"max_query_length": "24h"}
	got := p.effectiveMaxQueryLength("")
	if got != 24*time.Hour {
		t.Errorf("want 24h, got %v", got)
	}
}

func TestHandleQueryRange_ExceedsMaxQueryLength_Returns400(t *testing.T) {
	// Minimal VL backend that never gets called (limit enforced before upstream).
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("backend should not be called when query range exceeds limit")
	}))
	t.Cleanup(backend.Close)

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:            backend.URL,
		Cache:                 c,
		LogLevel:              "error",
		DefaultMaxQueryLength: time.Hour,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(nil) })

	// start=0s, end=10800s (3h) expressed as Unix seconds — parseLokiTimeToUnixNano
	// normalises abs<1e11 as seconds, so the range becomes 3h in nanoseconds,
	// which exceeds the 1h limit and should produce a 400.
	start := "0"
	end := fmt.Sprintf("%d", int64((3 * time.Hour).Seconds())) // "10800"
	req := httptest.NewRequest("GET",
		`/loki/api/v1/query_range?query={app="x"}&start=`+start+`&end=`+end, nil)
	w := httptest.NewRecorder()
	p.handleQueryRange(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d (body: %s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "exceeds limit") {
		t.Errorf("want 'exceeds limit' in body, got: %s", w.Body.String())
	}
}
