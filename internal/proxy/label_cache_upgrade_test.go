package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// unversionedKey returns the key format written by binaries before
// labelMetadataCacheKeyVersion existed.
func unversionedKey(t *testing.T, key string) string {
	t.Helper()
	segment := labelMetadataCacheKeyVersion + ":"
	if !strings.Contains(key, segment) {
		t.Fatalf("key %q carries no %s version segment", key, labelMetadataCacheKeyVersion)
	}
	return strings.Replace(key, segment, "", 1)
}

// Entries written by older binaries (which cached only the most recent minutes
// of the range) must never be served after an upgrade: not as fresh hits from
// memory, disk or the compatibility-edge cache, and not as stale-on-error
// answers.
func TestLabelMetadataCacheKeys_OldFormatEntriesAreNeverServed(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, label, path string
		backendFails                bool
	}{
		{name: "labels fresh", endpoint: "labels", path: "/loki/api/v1/labels"},
		{name: "labels stale on error", endpoint: "labels", path: "/loki/api/v1/labels", backendFails: true},
		{name: "label_values fresh", endpoint: "label_values", label: "app", path: "/loki/api/v1/label/app/values"},
		{name: "label_values stale on error", endpoint: "label_values", label: "app", path: "/loki/api/v1/label/app/values", backendFails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(http.StatusOK)
					return
				}
				if tc.backendFails {
					http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
					return
				}
				switch r.URL.Path {
				case "/select/logsql/stream_field_values", "/select/logsql/field_values":
					writeVLFieldValues(w, []fieldHit{{"full-a", 1}, {"full-b", 1}})
				default:
					writeVLFieldNames(w, []fieldHit{{"app", 1}, {"full_label", 1}})
				}
			}))
			t.Cleanup(srv.Close)

			dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/cache.db"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = dc.Close() })
			c := cache.New(60*time.Second, 1000)
			c.SetL2(dc)
			t.Cleanup(c.Close)
			compat := cache.New(60*time.Second, 100)
			t.Cleanup(compat.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: c, CompatCache: compat, LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			end := perfBaseTimeNs
			path := fmt.Sprintf("%s?start=%d&end=%d", tc.path, end-int64(24*time.Hour), end)
			req := httptest.NewRequest(http.MethodGet, path, nil)
			var key string
			if tc.label != "" {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req, tc.label)
			} else {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req)
			}
			compatKey, ok := p.compatCacheKey(tc.endpoint, req)
			if !ok {
				t.Fatal("request not compat-cacheable")
			}
			// Old-format entries in memory, on disk and in the compatibility-edge
			// cache; fresh when the backend is healthy, expired when it fails.
			ttl := time.Hour
			if tc.backendFails {
				ttl = time.Millisecond
			}
			partial := lokiLabelsResponse([]string{"partial-recent-only"})
			oldKey := unversionedKey(t, key)
			c.SetWithTTL(oldKey, partial, ttl)
			dc.Set(oldKey, partial, ttl)
			dc.Flush()
			compat.SetWithTTL(unversionedKey(t, compatKey), partial, ttl)
			time.Sleep(5 * time.Millisecond)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if strings.Contains(rec.Body.String(), "partial-recent-only") {
				t.Fatalf("served an old-format cache entry: %d %s", rec.Code, rec.Body.String())
			}
			if tc.backendFails && rec.Code == http.StatusOK {
				t.Fatalf("want the backend error without a versioned entry, got %d %s", rec.Code, rec.Body.String())
			}
			if !tc.backendFails && (rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "full")) {
				t.Fatalf("want the backend answer, got %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// A stale answer served after a backend failure must not be captured by the
// compatibility-edge cache, or it would be re-served as fresh for the full TTL
// after the backend recovers. Applies to every endpoint using the shared helper.
func TestCompatCache_DoesNotCaptureStaleOnErrorResponses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	for _, tc := range []struct {
		name, endpoint, label, path string
		body                        []byte
	}{
		{name: "labels", endpoint: "labels", path: "/loki/api/v1/labels?start=1703980800000000000&end=1704067200000000000", body: lokiLabelsResponse([]string{"cached"})},
		{name: "label_values", endpoint: "label_values", label: "app", path: "/loki/api/v1/label/app/values?start=1703980800000000000&end=1704067200000000000", body: lokiLabelsResponse([]string{"cached"})},
		{name: "detected_field_values", endpoint: "detected_field_values", label: "service_name", path: "/loki/api/v1/detected_field/service_name/values?query=%7Bservice_name%3D%22cached%22%7D", body: []byte(`{"status":"success","data":["cached"],"values":["cached"],"limit":1000}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compat := cache.New(60*time.Second, 100)
			t.Cleanup(compat.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(60*time.Second, 1000), CompatCache: compat, LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			var key string
			if tc.label != "" {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req, tc.label)
			} else {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req)
			}
			p.setEndpointReadCacheWithTTL(tc.endpoint, key, tc.body, time.Millisecond)
			time.Sleep(5 * time.Millisecond)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "cached") {
				t.Fatalf("want the stale answer, got %d %s", rec.Code, rec.Body.String())
			}
			if rec.Header().Get(staleResponseHeader) != "true" || rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("stale answer headers = %v, want %s: true and Cache-Control: no-store", rec.Header(), staleResponseHeader)
			}
			compatKey, ok := p.compatCacheKey(tc.endpoint, req)
			if !ok {
				t.Fatal("request not compat-cacheable")
			}
			if _, cached := compat.Get(compatKey); cached {
				t.Fatalf("compatibility-edge cache captured the stale answer")
			}
		})
	}
}

// On a 4xx from the streams endpoint, /label/service_name/values must not answer
// from the recent-data detection sample. A rejected query returns Loki's 400
// bad_data; a resource limit or storage failure reported with the same 400 is a
// backend failure (502); a backend without the endpoint keeps the full-range
// native answer.
func TestServiceNameValues_Streams4xxNeverUsesRecentSample(t *testing.T) {
	for _, tc := range []struct {
		name       string
		streamsErr string
		wantStatus int
	}{
		{name: "rejected query returns the error", streamsErr: "cannot parse `query` arg: unexpected token", wantStatus: http.StatusBadRequest},
		{name: "storage failure is a backend error", streamsErr: "cannot obtain streams: cannot read index: unexpected EOF", wantStatus: http.StatusBadGateway},
		{name: "unsupported endpoint keeps full-range native answer", streamsErr: `unsupported path requested: "/select/logsql/streams"`, wantStatus: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			end := perfBaseTimeNs
			start := end - int64(24*time.Hour)
			var mu sync.Mutex
			var calls []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(http.StatusOK)
					return
				}
				mu.Lock()
				calls = append(calls, r.URL.Path+"?start="+r.URL.Query().Get("start"))
				mu.Unlock()
				switch r.URL.Path {
				case "/select/logsql/streams":
					http.Error(w, tc.streamsErr, http.StatusBadRequest)
				case "/select/logsql/field_names", "/select/logsql/stream_field_names":
					// No service-name source field in the window: native lookup is empty.
					writeVLFieldNames(w, nil)
				case "/select/logsql/query":
					// A recent-data sample would find a service here.
					w.Header().Set("Content-Type", "application/stream+json")
					_, _ = w.Write([]byte(`{"_time":"2024-01-01T00:00:00Z","_msg":"x","_stream":"{service_name=\"sampled\"}","service_name":"sampled"}` + "\n"))
				default:
					writeVLFieldValues(w, []fieldHit{{"sampled", 1}})
				}
			}))
			t.Cleanup(srv.Close)
			_, mux := newBehaviorProxy(t, srv.URL, 0)

			path := fmt.Sprintf("/loki/api/v1/label/service_name/values?start=%d&end=%d", start, end)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if strings.Contains(rec.Body.String(), "sampled") {
				t.Fatalf("answered from the recent-data sample: %d %s", rec.Code, rec.Body.String())
			}
			if rec.Code != tc.wantStatus {
				t.Fatalf("got %d %s, want %d", rec.Code, rec.Body.String(), tc.wantStatus)
			}
			if tc.wantStatus == http.StatusBadRequest && !strings.Contains(rec.Body.String(), `"errorType":"bad_data"`) {
				t.Fatalf("want errorType bad_data, got %s", rec.Body.String())
			}
			mu.Lock()
			defer mu.Unlock()
			for _, call := range calls {
				if !strings.HasSuffix(call, fmt.Sprintf("?start=%d", start)) {
					t.Fatalf("backend call %s did not cover the full range (start=%d); calls %v", call, start, calls)
				}
			}
		})
	}
}

func newTwoTenantProxy(t *testing.T, backendURL string) (*Proxy, *cache.Cache, *http.ServeMux) {
	t.Helper()
	compat := cache.New(60*time.Second, 100)
	t.Cleanup(compat.Close)
	p, err := New(Config{
		BackendURL:  backendURL,
		Cache:       cache.New(60*time.Second, 1000),
		CompatCache: compat,
		LogLevel:    "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, compat, mux
}

// A multi-tenant merge that includes a tenant answered from a stale cache entry
// must carry the stale headers and must not be stored in the merge cache or the
// compatibility-edge cache.
func TestMultiTenantFanout_StaleTenantAnswerIsMarkedAndNotCached(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Header.Get("AccountID") == "10" {
			http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			writeVLFieldValues(w, []fieldHit{{"fresh-b", 1}})
		default:
			writeVLFieldNames(w, []fieldHit{{"app", 1}})
		}
	}))
	t.Cleanup(srv.Close)
	p, compat, mux := newTwoTenantProxy(t, srv.URL)

	path := "/loki/api/v1/label/app/values?start=1703980800000000000&end=1704067200000000000"
	// Sub-requests inherit the fanout request's auth scope and its tenant
	// selector filter (query=*); build the tenant-a key the same way.
	outer := httptest.NewRequest(http.MethodGet, path+"&query=%2A", nil)
	outer.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	scoped := p.injectAuthFingerprint(outer)
	tenantReq := scoped.Clone(scoped.Context())
	tenantReq.Header.Set("X-Scope-OrgID", "tenant-a")
	tenantKey := p.canonicalReadCacheKey("label_values", "tenant-a", tenantReq, "app")
	p.setEndpointReadCacheWithTTL("label_values", tenantKey, lokiLabelsResponse([]string{"stale-a"}), time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	body := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(body, "stale-a") || !strings.Contains(body, "fresh-b") {
		t.Fatalf("want merged stale-a and fresh-b, got %d %s", rec.Code, body)
	}
	if rec.Header().Get(staleResponseHeader) != "true" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("merged response headers = %v, want stale markers", rec.Header())
	}
	mtKey, ok := p.multiTenantCacheKey(req, "label_values")
	if !ok {
		t.Fatal("request not multi-tenant cacheable")
	}
	if _, cached := p.cache.Get(mtKey); cached {
		t.Fatal("stale merged response stored in the multi-tenant merge cache")
	}
	compatKey, ok := p.compatCacheKey("label_values", req)
	if !ok {
		t.Fatal("request not compat-cacheable")
	}
	if _, cached := compat.Get(compatKey); cached {
		t.Fatal("stale merged response captured by the compatibility-edge cache")
	}
}

// An empty merged label-value list is kept in the multi-tenant merge cache only
// for the negative TTL.
func TestMultiTenantFanout_EmptyMergedListUsesNegativeTTL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		switch r.URL.Path {
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			writeVLFieldValues(w, nil)
		default:
			writeVLFieldNames(w, nil)
		}
	}))
	t.Cleanup(srv.Close)
	p, _, mux := newTwoTenantProxy(t, srv.URL)

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/app/values?start=1703980800000000000&end=1704067200000000000", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !metadataListPayloadEmpty(rec.Body.Bytes()) {
		t.Fatalf("want an empty merged list, got %d %s", rec.Code, rec.Body.String())
	}
	mtKey, ok := p.multiTenantCacheKey(req, "label_values")
	if !ok {
		t.Fatal("request not multi-tenant cacheable")
	}
	_, remaining, cached := p.cache.GetWithTTL(mtKey)
	if !cached || remaining <= 0 || remaining > metadataNegativeCacheTTL {
		t.Fatalf("empty merged list cached=%v remaining=%v, want at most %v", cached, remaining, metadataNegativeCacheTTL)
	}
}

// During a backend outage an expired empty (negative) list in memory must not be
// used as the stale answer: the lookup falls through to the last non-empty
// answer on disk, and without one the error is returned.
func TestLabelsStaleOnError_SkipsExpiredEmptyList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	for _, tc := range []struct {
		name, endpoint, label, path string
		diskAnswer                  bool
	}{
		{name: "labels with disk answer", endpoint: "labels", path: "/loki/api/v1/labels", diskAnswer: true},
		{name: "labels without disk answer", endpoint: "labels", path: "/loki/api/v1/labels"},
		{name: "label_values with disk answer", endpoint: "label_values", label: "app", path: "/loki/api/v1/label/app/values", diskAnswer: true},
		{name: "label_values without disk answer", endpoint: "label_values", label: "app", path: "/loki/api/v1/label/app/values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/cache.db"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = dc.Close() })
			c := cache.New(60*time.Second, 1000)
			c.SetL2(dc)
			t.Cleanup(c.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			path := tc.path + "?start=1703980800000000000&end=1704067200000000000"
			req := httptest.NewRequest(http.MethodGet, path, nil)
			var key string
			if tc.label != "" {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req, tc.label)
			} else {
				key = p.canonicalReadCacheKey(tc.endpoint, "", req)
			}
			// The handler's write for an empty answer: memory and disk, 30s.
			p.setMetadataListCache(tc.endpoint, key, lokiLabelsResponse([]string{}), 0, time.Hour)
			dc.Flush()
			if body, onDisk := dc.Get(key); !onDisk || !metadataListPayloadEmpty(body) {
				t.Fatalf("empty answer not persisted through the normal write path: ok=%v body=%s", onDisk, body)
			}
			if tc.diskAnswer {
				// A non-empty disk copy the empty write did not replace (for
				// example written by another path after it).
				dc.Set(key, lokiLabelsResponse([]string{"last-good"}), time.Hour)
				dc.Flush()
			}
			// Both copies have expired.
			restore := cache.AdvanceClockForTesting(2 * time.Hour)
			defer restore()

			if tc.diskAnswer {
				// Checked at the stale lookup itself: a regular cache read on the
				// miss path already evicts expired disk entries.
				body, _, tier, ok := p.staleEndpointCacheEntry(tc.endpoint, key)
				if !ok || tier != "l2_disk" || !strings.Contains(string(body), "last-good") {
					t.Fatalf("want the last non-empty disk answer, got ok=%v tier=%q body=%s", ok, tier, body)
				}
				return
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code < 400 {
				t.Fatalf("want the error instead of an expired empty list, got %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}
