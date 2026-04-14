package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func benchmarkLabelNames(count int) []string {
	names := make([]string, 0, count)
	for i := range count {
		names = append(names, fmt.Sprintf("label_%05d", i))
	}
	return names
}

func benchmarkBypassRequests(rawURLFmt string, requestCount int) []*http.Request {
	if requestCount < 1 {
		requestCount = 1
	}
	requests := make([]*http.Request, 0, requestCount)
	for i := range requestCount {
		requests = append(requests, benchmarkRequest(fmt.Sprintf(rawURLFmt, i+1, i+2)))
	}
	return requests
}

func BenchmarkProxy_LabelKeys_Scale_CacheBypass(b *testing.B) {
	scales := []int{10, 100, 1000, 10000}
	for _, scale := range scales {
		scale := scale
		b.Run("keys_"+strconv.Itoa(scale), func(b *testing.B) {
			namesBody := buildFieldNamesResponse(benchmarkLabelNames(scale)...)
			mux, _ := newCompatBenchmarkProxy(b, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/select/logsql/stream_field_names", "/select/logsql/field_names":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(namesBody)
				default:
					http.NotFound(w, r)
				}
			}, false)

			requests := benchmarkBypassRequests("/loki/api/v1/labels?query=%7Bapp%3D%22api%22%7D&start=%d&end=%d", 256)
			var idx atomic.Uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				w := newBenchmarkResponseWriter()
				for pb.Next() {
					r := requests[int(idx.Add(1)-1)%len(requests)]
					w.reset()
					mux.ServeHTTP(w, r)
				}
			})
		})
	}
}

func BenchmarkProxy_LabelValues_Scale_CacheBypass(b *testing.B) {
	scales := []int{10, 100, 1000, 10000}
	for _, scale := range scales {
		scale := scale
		b.Run("values_"+strconv.Itoa(scale), func(b *testing.B) {
			valuesBody := buildFieldValuesResponse("app", scale)
			fieldNamesBody := buildFieldNamesResponse("app")

			vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/select/logsql/stream_field_names", "/select/logsql/field_names":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(fieldNamesBody)
				case "/select/logsql/stream_field_values", "/select/logsql/field_values":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(valuesBody)
				default:
					http.NotFound(w, r)
				}
			}))
			b.Cleanup(vlBackend.Close)

			p, err := New(Config{
				BackendURL:                 vlBackend.URL,
				Cache:                      cache.New(5*time.Minute, 200000),
				LogLevel:                   "error",
				LabelValuesIndexedCache:    true,
				LabelValuesHotLimit:        500,
				LabelValuesIndexMaxEntries: 250000,
			})
			if err != nil {
				b.Fatalf("create proxy: %v", err)
			}
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			requests := benchmarkBypassRequests("/loki/api/v1/label/app/values?query=%7Bapp%3D%22api%22%7D&start=%d&end=%d", 256)
			var idx atomic.Uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				w := newBenchmarkResponseWriter()
				for pb.Next() {
					r := requests[int(idx.Add(1)-1)%len(requests)]
					w.reset()
					mux.ServeHTTP(w, r)
				}
			})
		})
	}
}

func BenchmarkProxy_LabelValues_Scale_CacheHit(b *testing.B) {
	scales := []int{10, 100, 1000, 10000}
	for _, scale := range scales {
		scale := scale
		b.Run("values_"+strconv.Itoa(scale), func(b *testing.B) {
			valuesBody := buildFieldValuesResponse("app", scale)
			fieldNamesBody := buildFieldNamesResponse("app")

			mux, _ := newCompatBenchmarkProxy(b, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/select/logsql/stream_field_names", "/select/logsql/field_names":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(fieldNamesBody)
				case "/select/logsql/stream_field_values", "/select/logsql/field_values":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(valuesBody)
				default:
					http.NotFound(w, r)
				}
			}, false)

			rawURL := "/loki/api/v1/label/app/values?query=%7Bapp%3D%22api%22%7D&start=1&end=2"
			warmRoute(mux, rawURL, nil)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				req := benchmarkRequest(rawURL)
				w := newBenchmarkResponseWriter()
				for pb.Next() {
					w.reset()
					mux.ServeHTTP(w, req)
				}
			})
		})
	}
}
