package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// errorStub is a VictoriaLogs stand-in whose select calls fail in one mode and
// count every call.
type errorStub struct {
	mode  atomic.Value // "500", "400", "hang", "ok"
	calls atomic.Int64
	srv   *httptest.Server
	stop  chan struct{}
}

func newErrorStub(t testing.TB, mode string) *errorStub {
	s := &errorStub{stop: make(chan struct{})}
	s.mode.Store(mode)
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/select/") {
			w.WriteHeader(http.StatusOK)
			return
		}
		s.calls.Add(1)
		switch s.mode.Load().(string) {
		case "500":
			http.Error(w, "storage node unavailable", http.StatusInternalServerError)
		case "400":
			http.Error(w, "cannot parse query", http.StatusBadRequest)
		case "hang":
			select {
			case <-r.Context().Done():
			case <-s.stop:
			}
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	t.Cleanup(s.srv.Close)
	t.Cleanup(func() { close(s.stop) })
	return s
}

func baselineProxy(t testing.TB, backend string) *Proxy {
	cfg := Config{
		BackendURL:  backend,
		Cache:       cache.New(60*time.Second, 1000),
		CompatCache: cache.New(60*time.Second, 1000),
		LogLevel:    "error",
	}
	shippedErrorCache(&cfg)
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

var baselineRoutes = []struct{ name, target string }{
	{"log query_range", "/loki/api/v1/query_range?query=" + url.QueryEscape(`{app="api"}`) + "&start=1700000000&end=1700003600&limit=100"},
	{"metric query_range", "/loki/api/v1/query_range?query=" + url.QueryEscape(`sum(count_over_time({app="api"}[1m]))`) + "&start=1700000000&end=1700003600&step=60"},
	{"binary query_range", "/loki/api/v1/query_range?query=" + url.QueryEscape(`sum(count_over_time({app="api"}[1m])) / sum(count_over_time({app="web"}[1m]))`) + "&start=1700000000&end=1700003600&step=60"},
	{"instant query", "/loki/api/v1/query?query=" + url.QueryEscape(`sum(count_over_time({app="api"}[5m]))`) + "&time=1700003600"},
	{"labels", "/loki/api/v1/labels?start=1700000000&end=1700003600"},
	{"label_values", "/loki/api/v1/label/app/values?start=1700000000&end=1700003600"},
	{"series", "/loki/api/v1/series?match[]=" + url.QueryEscape(`{app="api"}`) + "&start=1700000000&end=1700003600"},
}

// TestErrorFlightBaseline prints, per route and failure mode, the VictoriaLogs
// calls of a burst of identical concurrent requests and of a refresh loop.
func TestErrorFlightBaseline(t *testing.T) {
	if os.Getenv("LVP_ERROR_BASELINE") == "" {
		t.Skip("set LVP_ERROR_BASELINE=1")
	}
	const burst, refresh = 50, 10
	fmt.Printf("| route | VL answer | burst %d: VL calls | statuses | refresh x%d: VL calls |\n|---|---|---:|---|---:|\n", burst, refresh)
	for _, mode := range []string{"500", "400", "hang"} {
		for _, rt := range baselineRoutes {
			stub := newErrorStub(t, mode)
			p := baselineProxy(t, stub.srv.URL)
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			front := httptest.NewServer(mux)
			client := &http.Client{Timeout: 3 * time.Second}
			if mode == "hang" {
				client.Timeout = 300 * time.Millisecond
			}
			statuses := map[string]int{}
			var mu sync.Mutex
			var wg sync.WaitGroup
			for i := 0; i < burst; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					req, _ := http.NewRequest(http.MethodGet, front.URL+rt.target, nil)
					req.Header.Set("X-Scope-OrgID", "0")
					resp, err := client.Do(req)
					key := "client-timeout"
					if err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						key = fmt.Sprint(resp.StatusCode)
					}
					mu.Lock()
					statuses[key]++
					mu.Unlock()
				}()
			}
			wg.Wait()
			burstCalls := stub.calls.Load()
			for i := 0; i < refresh; i++ {
				req, _ := http.NewRequest(http.MethodGet, front.URL+rt.target, nil)
				req.Header.Set("X-Scope-OrgID", "0")
				if resp, err := client.Do(req); err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				time.Sleep(50 * time.Millisecond)
			}
			fmt.Printf("| %s | %s | %d | %v | %d |\n", rt.name, mode, burstCalls, statuses, stub.calls.Load()-burstCalls)
			front.Close()
			_ = p.Shutdown(t.Context())
		}
	}
}

// TestErrorFlightLoad is the load proof: per failure class (and a success),
// 200 identical requests at once, then 50 clients refreshing every 100 ms for
// 2 s; it prints VictoriaLogs calls, p50/p99 latency and the peak heap in use.
func TestErrorFlightLoad(t *testing.T) {
	if os.Getenv("LVP_ERROR_BASELINE") == "" {
		t.Skip("set LVP_ERROR_BASELINE=1")
	}
	type class struct {
		name, target string
		maxSeries    int
		answer       func(w http.ResponseWriter)
	}
	logTarget := "/loki/api/v1/query_range?query=" + url.QueryEscape(`{app="api"}`) + "&start=1700000000&end=1700003600&limit=100"
	metricTarget := "/loki/api/v1/query_range?query=" + url.QueryEscape(`sum by (app) (count_over_time({app=~"a.+"}[1m]))`) + "&start=1700000000&end=1700003600&step=60"
	classes := []class{
		{"transient (VL 500)", logTarget, 0, func(w http.ResponseWriter) { http.Error(w, "storage node unavailable", http.StatusInternalServerError) }},
		{"transient (VL 503 retried)", logTarget, 0, func(w http.ResponseWriter) { http.Error(w, "busy", http.StatusServiceUnavailable) }},
		{"data (series limit)", metricTarget, 1, func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a1"},"values":[[1700000060,"1"]]},{"metric":{"app":"a2"},"values":[[1700000060,"1"]]}]}}`))
		}},
		{"success (log lines)", logTarget, 0, func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:13:20Z","_msg":"hello","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
		}},
	}
	fmt.Printf("| class | burst 200: VL calls | burst p50 | burst p99 | refresh 50x100ms 2s: requests | VL calls | refresh p50 | refresh p99 | peak heap in use |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, c := range classes {
		var calls atomic.Int64
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, "/select/") {
				w.WriteHeader(http.StatusOK)
				return
			}
			calls.Add(1)
			time.Sleep(20 * time.Millisecond) // a VictoriaLogs answer takes time
			c.answer(w)
		}))
		cfg := Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 10000), CompatCache: cache.New(60*time.Second, 10000), LogLevel: "error", MaxStatsQuerySeries: c.maxSeries}
		shippedErrorCache(&cfg)
		p, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		front := httptest.NewServer(mux)
		client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256}}
		runtime.GC()
		var base runtime.MemStats
		runtime.ReadMemStats(&base)
		var peak atomic.Uint64
		stopSample := make(chan struct{})
		go func() {
			var ms runtime.MemStats
			for {
				select {
				case <-stopSample:
					return
				case <-time.After(5 * time.Millisecond):
				}
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > peak.Load() {
					peak.Store(ms.HeapInuse)
				}
			}
		}()
		send := func() time.Duration {
			start := time.Now()
			req, _ := http.NewRequest(http.MethodGet, front.URL+c.target, nil)
			req.Header.Set("X-Scope-OrgID", "0")
			if resp, err := client.Do(req); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			return time.Since(start)
		}
		measure := func(n int, each func(i int) []time.Duration) []time.Duration {
			var mu sync.Mutex
			var all []time.Duration
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					d := each(i)
					mu.Lock()
					all = append(all, d...)
					mu.Unlock()
				}(i)
			}
			wg.Wait()
			sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
			return all
		}
		pct := func(d []time.Duration, q float64) time.Duration {
			if len(d) == 0 {
				return 0
			}
			return d[min(len(d)-1, int(q*float64(len(d))))].Round(10 * time.Microsecond)
		}
		burstLat := measure(200, func(int) []time.Duration { return []time.Duration{send()} })
		burstCalls := calls.Load()
		stopAt := time.Now().Add(2 * time.Second)
		refreshLat := measure(50, func(i int) []time.Duration {
			time.Sleep(time.Duration(i) * 2 * time.Millisecond)
			var out []time.Duration
			for time.Now().Before(stopAt) {
				out = append(out, send())
				time.Sleep(100 * time.Millisecond)
			}
			return out
		})
		close(stopSample)
		fmt.Printf("| %s | %d | %v | %v | %d | %d | %v | %v | %.1f MiB |\n", c.name, burstCalls, pct(burstLat, 0.5), pct(burstLat, 0.99),
			len(refreshLat), calls.Load()-burstCalls, pct(refreshLat, 0.5), pct(refreshLat, 0.99), heapGrowthMiB(peak.Load(), base.HeapInuse))
		front.Close()
		backend.Close()
		_ = p.Shutdown(t.Context())
	}
}

func heapGrowthMiB(peak, base uint64) float64 {
	if peak < base {
		return 0
	}
	return float64(peak-base) / (1 << 20)
}
