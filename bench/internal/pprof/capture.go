// Package pprof captures Go pprof profiles from a running HTTP server
// that exposes /debug/pprof/* endpoints.
package pprof

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var client = &http.Client{Timeout: 120 * time.Second}

// CaptureCPU fetches a CPU profile of duration d from baseURL/debug/pprof/profile.
// The call blocks for ~d seconds while the profile is collected.
// authToken is sent as "Authorization: Bearer <token>" if non-empty.
func CaptureCPU(ctx context.Context, baseURL, authToken string, d time.Duration) ([]byte, error) {
	secs := int(d.Seconds())
	if secs < 1 {
		secs = 1
	}
	url := fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", baseURL, secs)
	return fetch(ctx, url, authToken)
}

// CaptureHeap fetches a heap profile snapshot from baseURL/debug/pprof/heap.
func CaptureHeap(ctx context.Context, baseURL, authToken string) ([]byte, error) {
	return fetch(ctx, baseURL+"/debug/pprof/heap", authToken)
}

// CaptureAllocs fetches an allocs profile snapshot from baseURL/debug/pprof/allocs.
func CaptureAllocs(ctx context.Context, baseURL, authToken string) ([]byte, error) {
	return fetch(ctx, baseURL+"/debug/pprof/allocs", authToken)
}

// CaptureGoroutine fetches a goroutine profile snapshot.
func CaptureGoroutine(ctx context.Context, baseURL, authToken string) ([]byte, error) {
	return fetch(ctx, baseURL+"/debug/pprof/goroutine?debug=1", authToken)
}

// Save writes data to path, creating parent directories as needed.
func Save(data []byte, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func fetch(ctx context.Context, url, authToken string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pprof %s: HTTP %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
