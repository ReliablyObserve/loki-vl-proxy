package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// shippedErrorCache gives a measured proxy the error cache the binary ships
// with (its flag defaults).
func shippedErrorCache(c *Config) {
	c.ErrorCache = ErrorCacheConfig{TransientTTL: DefaultErrorCacheTransientTTL, DataTTL: DefaultErrorCacheDataTTL}
}

// TestErrorCacheTTLSweep measures, per transient TTL, the VictoriaLogs calls of
// 20 clients refreshing one failing query every 100 ms (a dashboard storm)
// for 3 s of outage, and how long after VictoriaLogs recovers the first and
// the last client see a 200.
func TestErrorCacheTTLSweep(t *testing.T) {
	if os.Getenv("LVP_ERROR_BASELINE") == "" {
		t.Skip("set LVP_ERROR_BASELINE=1")
	}
	for _, route := range []struct {
		name   string
		target func() string
	}{
		{"/labels, fixed window", func() string { return "/loki/api/v1/labels?start=1700000000&end=1700003600" }},
		{"query_range, end=now per refresh (ns)", func() string {
			now := time.Now().UnixNano()
			return fmt.Sprintf("/loki/api/v1/query_range?query=%%7Bapp%%3D%%22api%%22%%7D&start=%d&end=%d&limit=100", now-int64(time.Hour), now)
		}},
		{"query_range metric, end=now, step 15s", func() string {
			now := time.Now().UnixNano()
			return fmt.Sprintf("/loki/api/v1/query_range?query=sum(count_over_time(%%7Bapp%%3D%%22api%%22%%7D%%5B1m%%5D))&start=%d&end=%d&step=15", now-int64(time.Hour), now)
		}},
	} {
		ttlSweep(t, route.name, route.target)
	}
}

func ttlSweep(t *testing.T, routeName string, targetOf func() string) {
	fmt.Printf("\n%s\n\n| transient TTL | VL calls during 3 s outage | calls/s | first 200 after recovery | every client 200 after recovery |\n|---|---:|---:|---:|---:|\n", routeName)
	for _, ttl := range []time.Duration{0, 500 * time.Millisecond, time.Second, 2 * time.Second} {
		stub := newErrorStub(t, "500")
		p, err := New(Config{
			BackendURL:  stub.srv.URL,
			Cache:       cache.New(60*time.Second, 1000),
			CompatCache: cache.New(60*time.Second, 1000),
			LogLevel:    "error",
			ErrorCache:  ErrorCacheConfig{TransientTTL: ttl, DataTTL: DefaultErrorCacheDataTTL, Disabled: ttl == 0},
		})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		front := httptest.NewServer(mux)

		const clients = 20
		var recovered atomic.Int64 // unix nanos of recovery
		firstOK := make([]atomic.Int64, clients)
		stopAt := time.Now().Add(6 * time.Second)
		var wg sync.WaitGroup
		for c := 0; c < clients; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				time.Sleep(time.Duration(c) * 5 * time.Millisecond) // spread like dashboards
				for time.Now().Before(stopAt) {
					req, _ := http.NewRequest(http.MethodGet, front.URL+targetOf(), nil)
					req.Header.Set("X-Scope-OrgID", "0")
					if resp, err := http.DefaultClient.Do(req); err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						if resp.StatusCode == http.StatusOK && recovered.Load() != 0 && firstOK[c].Load() == 0 {
							firstOK[c].Store(time.Now().UnixNano())
						}
					}
					time.Sleep(100 * time.Millisecond)
				}
			}(c)
		}
		time.Sleep(3 * time.Second)
		outageCalls := stub.calls.Load()
		stub.mode.Store("ok")
		recovered.Store(time.Now().UnixNano())
		wg.Wait()
		first, last := int64(-1), int64(0)
		for c := range firstOK {
			v := firstOK[c].Load() - recovered.Load()
			if firstOK[c].Load() == 0 {
				v = -1
			}
			if first < 0 || (v >= 0 && v < first) {
				first = v
			}
			if v > last {
				last = v
			}
		}
		fmt.Printf("| %v | %d | %.1f | %v | %v |\n", ttl, outageCalls, float64(outageCalls)/3, time.Duration(first).Round(time.Millisecond), time.Duration(last).Round(time.Millisecond))
		front.Close()
		_ = p.Shutdown(t.Context())
	}
}
