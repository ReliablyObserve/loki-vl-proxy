package proxy

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkMissWhileAnotherQueryProbes: a response cache miss of a healthy
// query while another query of the replica probes after its window-free
// failure ("other-probing") and while none does ("quiet").
func BenchmarkMissWhileAnotherQueryProbes(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/select/") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:13:20Z","_msg":"hello","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
	}))
	defer backend.Close()
	for _, probing := range []bool{false, true} {
		name := "quiet"
		if probing {
			name = "other-probing"
		}
		b.Run(name, func(b *testing.B) {
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 100000), CompatCache: cache.New(time.Minute, 100000), LogLevel: "error",
				ErrorCache: ErrorCacheConfig{TransientTTL: time.Second, DataTTL: 5 * time.Second}})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = p.Shutdown(b.Context()) }()
			if probing {
				p.windowFreeSeenUntil.Store(time.Now().Add(time.Hour).UnixNano())
				p.windowFreeHeldUntil.Store(time.Now().Add(time.Hour).UnixNano())
			}
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				i++
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1700000000&end=1700003600&limit="+strconv.Itoa(1000+i), nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d", rec.Code)
				}
			}
		})
	}
}
