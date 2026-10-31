package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// conformance: loki_api_v1_label, loki_api_v1_labels
func TestLabelsPOSTFormCacheSeparatesSelectors(t *testing.T) {
	for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/label"} {
		t.Run(path, func(t *testing.T) {
			var calls atomic.Int64
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(http.StatusOK)
					return
				}
				calls.Add(1)
				switch r.URL.Path {
				case "/select/logsql/stream_field_names", "/select/logsql/field_names":
					query := r.URL.Query().Get("query")
					switch {
					case strings.Contains(query, "alpha"):
						writeVLFieldNames(w, []fieldHit{{"app", 1}, {"alpha_label", 1}})
					case strings.Contains(query, "beta"):
						writeVLFieldNames(w, []fieldHit{{"app", 1}, {"beta_label", 1}})
					default:
						t.Errorf("unexpected backend selector %q", query)
						writeVLFieldNames(w, nil)
					}
				default:
					writeVLFieldValues(w, nil)
				}
			}))
			t.Cleanup(backend.Close)
			_, mux := newBehaviorProxy(t, backend.URL, 0)
			request := func(method, route, selector string) string {
				t.Helper()
				params := url.Values{"query": {selector}, "start": {"1790871720000000000"}, "end": {"1790875320000000000"}}
				req := postReadCacheRequest(route, params)
				if method == http.MethodGet {
					req = httptest.NewRequest(method, route+"?"+params.Encode(), nil)
				}
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, req)
				if w.Code != http.StatusOK {
					t.Fatalf("%s %s returned %d: %s", method, route, w.Code, w.Body.String())
				}
				var resp struct{ Data []string }
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				want, other := "alpha_label", "beta_label"
				if strings.Contains(selector, "beta") {
					want, other = other, want
				}
				if !contains(resp.Data, want) || contains(resp.Data, other) {
					t.Errorf("%s %s selector %s returned labels %v", method, route, selector, resp.Data)
				}
				return w.Body.String()
			}
			alpha := request(http.MethodPost, path, `{app="alpha"}`)
			alphaCalls := calls.Load()
			beta := request(http.MethodPost, path, `{app="beta"}`)
			if calls.Load() <= alphaCalls {
				t.Error("different POST selector reused the first response")
			}
			baseline := calls.Load()
			for _, method := range []string{http.MethodPost, http.MethodGet} {
				for _, route := range []string{"/loki/api/v1/labels", "/loki/api/v1/label"} {
					if got := request(method, route, `{app="alpha"}`); got != alpha {
						t.Errorf("repeat alpha payload differs: %s", got)
					}
					if got := request(method, route, `{app="beta"}`); got != beta {
						t.Errorf("repeat beta payload differs: %s", got)
					}
				}
			}
			if got := calls.Load(); got != baseline {
				t.Errorf("equivalent forms/aliases did not reuse response cache: got %d backend calls want %d", got, baseline)
			}
		})
	}
}

// conformance: loki_api_v1_label, loki_api_v1_labels
func TestLabelsPOSTFormMigrationIgnoresPriorWildcardAnswer(t *testing.T) {
	for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/label"} {
		for _, tier := range []string{"memory", "disk", "compatibility"} {
			t.Run(path+"/"+tier, func(t *testing.T) {
				backend, calls := fieldNamesServer(t, []string{"app", "fresh_label"})
				c := cache.New(time.Hour, 100)
				t.Cleanup(c.Close)
				compat := cache.New(time.Hour, 100)
				t.Cleanup(compat.Close)
				var disk *cache.DiskCache
				if tier == "disk" {
					var err error
					disk, err = cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/cache.db"})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = disk.Close() })
					c.SetL2(disk)
				}
				p, err := New(Config{BackendURL: backend.URL, Cache: c, CompatCache: compat, LogLevel: "error"})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				mux := http.NewServeMux()
				p.RegisterRoutes(mux)
				req := httptest.NewRequest(http.MethodGet, path, nil)
				key := p.canonicalReadCacheKey("labels", "", req)
				poisoned := lokiLabelsResponse([]string{"poisoned_selector"})
				previous := previousPOSTReadCacheKey("labels", key)
				switch tier {
				case "memory":
					c.SetWithTTL(previous, poisoned, time.Hour)
				case "disk":
					disk.Set(previous, poisoned, time.Hour)
					disk.Flush()
				case "compatibility":
					outer, ok := p.compatCacheKey("labels", req)
					if !ok {
						t.Fatal("GET labels is not compatibility-cacheable")
					}
					compat.SetWithTTL(previousPOSTReadCacheKey("labels", outer), poisoned, time.Hour)
				}
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
				var response struct{ Data []string }
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if rec.Code != http.StatusOK || !contains(response.Data, "fresh_label") || contains(response.Data, "poisoned_selector") {
					t.Fatalf("GET reused a wildcard answer poisoned by an older POST: status=%d body=%s", rec.Code, rec.Body.String())
				}
				if calls.Load() == 0 {
					t.Fatal("migration did not fetch a fresh backend answer")
				}
			})
		}
	}
}
