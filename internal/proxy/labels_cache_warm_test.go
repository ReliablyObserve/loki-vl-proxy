package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// -labels-cache-warm=false stops the startup warm-up and the keep-warm loop:
// no label-name scan reaches VictoriaLogs until a client asks for labels.
// With the default (true) the startup warm-up scans the preset windows.
//
// conformance: quality/label-cache-warm, tenant-label-isolation
func TestLabelsCacheWarm_FlagControlsBackgroundScans(t *testing.T) {
	for _, tc := range []struct {
		name        string
		disable     bool
		label       string
		allowGlobal bool
		wantScans   bool
	}{
		{"default warms", false, "", false, true},
		{"disabled stays idle", true, "", false, false},
		// A tenant-label scan without an org has no tenant to filter on, so it
		// only runs where allow-global admits it.
		{"tenant-label mode stays idle", false, "org_id", false, false},
		{"tenant-label mode with allow-global warms", false, "org_id", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scans atomic.Int64
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(http.StatusOK)
					return
				}
				scans.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
			}))
			defer backend.Close()
			p, err := New(Config{
				BackendURL:             backend.URL,
				Cache:                  cache.New(60*time.Second, 1000),
				LogLevel:               "error",
				DisableLabelsCacheWarm: tc.disable,
				TenantLabel:            tc.label,
				AllowGlobalTenant:      tc.allowGlobal,
			})
			if err != nil {
				t.Fatal(err)
			}
			p.Init()
			defer func() { _ = p.Shutdown(context.Background()) }()

			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) && scans.Load() == 0 {
				time.Sleep(20 * time.Millisecond)
			}
			if got := scans.Load() > 0; got != tc.wantScans {
				t.Fatalf("background label scans = %d, want scans=%v", scans.Load(), tc.wantScans)
			}
		})
	}
}
