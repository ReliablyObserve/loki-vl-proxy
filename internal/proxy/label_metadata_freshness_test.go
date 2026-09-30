package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// freshnessVL is a fake VictoriaLogs whose stream inventory can grow: once
// grow is set it also lists the label "fresh_label" and the app value "fresh-app".
type freshnessVL struct {
	grow  atomic.Bool
	calls atomic.Int64
	delay time.Duration
}

func (f *freshnessVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		f.calls.Add(1)
		if f.delay > 0 {
			time.Sleep(f.delay)
		}
		switch r.URL.Path {
		case "/select/logsql/stream_field_names", "/select/logsql/field_names":
			hits := []fieldHit{{"app", 10}}
			if f.grow.Load() {
				hits = append(hits, fieldHit{"fresh_label", 1})
			}
			writeVLFieldNames(w, hits)
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			values := []fieldHit{{"old-app", 10}}
			if f.grow.Load() {
				values = append(values, fieldHit{"fresh-app", 1})
			}
			writeVLFieldValues(w, values)
		default:
			writeVLFieldNames(w, nil)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newFreshnessProxy(t *testing.T, vlURL string, freshness time.Duration) (*Proxy, *http.ServeMux) {
	t.Helper()
	p, err := New(Config{
		BackendURL:                    vlURL,
		Cache:                         cache.New(60*time.Second, 10000),
		LogLevel:                      "error",
		MetadataCacheFreshness:        freshness,
		RecentTailRefreshMaxStaleness: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

func getMetadataList(t *testing.T, mux *http.ServeMux, path string) []string {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s returned %d: %s", path, rec.Code, rec.Body.String())
	}
	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return resp.Data
}

func freshnessPath(base string, window, endAgo time.Duration) string {
	end := time.Now().Add(-endAgo)
	return fmt.Sprintf("%s?start=%d&end=%d", base, end.Add(-window).UnixNano(), end.UnixNano())
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_LabelsNearNowIncludeStreamWrittenAfterCaching(t *testing.T) {
	for _, window := range []time.Duration{6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour} {
		t.Run(window.String(), func(t *testing.T) {
			vl := &freshnessVL{}
			p, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
			_ = p
			path := freshnessPath("/loki/api/v1/labels", window, 0)
			if got := getMetadataList(t, mux, path); contains(got, "fresh_label") {
				t.Fatalf("label present before it was written: %v", got)
			}
			vl.grow.Store(true)
			time.Sleep(200 * time.Millisecond) // older than max-staleness
			// Same window, same request: the answer must include the new label.
			if got := getMetadataList(t, mux, path); !contains(got, "fresh_label") {
				t.Fatalf("near-now answer older than max-staleness omitted the new label: %v", got)
			}
		})
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_LabelsWithinMaxStalenessStayCached(t *testing.T) {
	vl := &freshnessVL{}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	path := freshnessPath("/loki/api/v1/labels", 7*24*time.Hour, 0)
	_ = getMetadataList(t, mux, path)
	before := vl.calls.Load()
	vl.grow.Store(true)
	if got := getMetadataList(t, mux, path); contains(got, "fresh_label") || vl.calls.Load() != before {
		t.Fatalf("an entry younger than max-staleness must be served from the cache (calls %d -> %d, %v)", before, vl.calls.Load(), got)
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_LabelsEndingBeforeTheWindowStayCached(t *testing.T) {
	vl := &freshnessVL{}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	path := freshnessPath("/loki/api/v1/labels", 7*24*time.Hour, 25*time.Hour)
	_ = getMetadataList(t, mux, path)
	before := vl.calls.Load()
	vl.grow.Store(true)
	time.Sleep(200 * time.Millisecond)
	got := getMetadataList(t, mux, path)
	if contains(got, "fresh_label") || vl.calls.Load() != before {
		t.Fatalf("a window ending before now-24h must be served from the cache with 0 backend calls (calls %d -> %d, %v)", before, vl.calls.Load(), got)
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_ZeroKeepsThePreviousCaching(t *testing.T) {
	vl := &freshnessVL{}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 0)
	path := freshnessPath("/loki/api/v1/labels", 7*24*time.Hour, 0)
	_ = getMetadataList(t, mux, path)
	before := vl.calls.Load()
	vl.grow.Store(true)
	time.Sleep(200 * time.Millisecond)
	got := getMetadataList(t, mux, path)
	if contains(got, "fresh_label") || vl.calls.Load() != before {
		t.Fatalf("with the freshness window at 0 a near-now hit is served as before (calls %d -> %d, %v)", before, vl.calls.Load(), got)
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_LabelValuesNearNowIncludeNewValue(t *testing.T) {
	vl := &freshnessVL{}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	path := freshnessPath("/loki/api/v1/label/app/values", 7*24*time.Hour, 0)
	if got := getMetadataList(t, mux, path); contains(got, "fresh-app") {
		t.Fatalf("value present before it was written: %v", got)
	}
	vl.grow.Store(true)
	time.Sleep(200 * time.Millisecond)
	if got := getMetadataList(t, mux, path); !contains(got, "fresh-app") {
		t.Fatalf("near-now label values omitted the new value: %v", got)
	}

	// A window ending before now-24h keeps its cached answer.
	old := freshnessPath("/loki/api/v1/label/app/values", 7*24*time.Hour, 25*time.Hour)
	vl.grow.Store(false)
	_ = getMetadataList(t, mux, old)
	before := vl.calls.Load()
	vl.grow.Store(true)
	time.Sleep(200 * time.Millisecond)
	if got := getMetadataList(t, mux, old); contains(got, "fresh-app") || vl.calls.Load() != before {
		t.Fatalf("historical label values must stay cached (calls %d -> %d, %v)", before, vl.calls.Load(), got)
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_ConcurrentNearNowRefetchesShareOnePass(t *testing.T) {
	vl := &freshnessVL{delay: 50 * time.Millisecond}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	path := freshnessPath("/loki/api/v1/labels", time.Hour, 0)
	_ = getMetadataList(t, mux, path)
	time.Sleep(200 * time.Millisecond)
	vl.grow.Store(true)

	before := vl.calls.Load()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := getMetadataList(t, mux, path); !contains(got, "fresh_label") {
				t.Errorf("concurrent near-now request omitted the new label: %v", got)
			}
		}()
	}
	wg.Wait()
	if passes := vl.calls.Load() - before; passes > 2 {
		t.Fatalf("12 concurrent refetches made %d backend calls, want one shared pass", passes)
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestSyncFetchStrings_ConcurrentSameKeyRunsOnce(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	var runs atomic.Int64
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := p.syncFetchStrings(context.Background(), "k", func() ([]string, error) {
				runs.Add(1)
				<-release
				return []string{"a", "b"}, nil
			})
			if err != nil || strings.Join(got, ",") != "a,b" {
				t.Errorf("got %v, %v", got, err)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	if runs.Load() != 1 {
		t.Fatalf("fetch ran %d times, want 1", runs.Load())
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_AgeSurvivesTheDiskTierAndShortenedTTLs(t *testing.T) {
	p, _ := newFreshnessProxy(t, "http://unused", 24*time.Hour)
	dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: filepath.Join(t.TempDir(), "c.db"), FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dc.Close() })
	first := cache.New(time.Minute, 100)
	first.SetL2(dc)
	p.cache = first

	ttl := metadataWindowTTL("0", fmt.Sprint(int64(7*24*time.Hour)), p.cacheTTLLabels) // 60m
	p.setEndpointReadCacheWithTTL("labels", "labels:k", []byte(`{"data":["a"]}`), ttl)
	time.Sleep(300 * time.Millisecond)

	// A replica with an empty memory tier reads the entry from disk: the
	// remaining TTL is the stored expiry, not a fresh stamp.
	second := cache.New(time.Minute, 100)
	second.SetL2(dc)
	p.cache = second
	_, remaining, tier, ok := p.endpointReadCacheEntry("labels", "labels:k")
	if !ok || tier != "l2_disk" {
		t.Fatalf("want an l2_disk hit, got ok=%v tier=%s", ok, tier)
	}
	if age := ttl - remaining; age < 250*time.Millisecond || age > 5*time.Second {
		t.Fatalf("age derived from the disk tier = %v, want about 300ms", age)
	}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil) // end omitted: now
	if !p.shouldBypassRecentTailCache("labels", ttl, remaining, r) {
		t.Fatal("a disk-promoted entry older than max-staleness must be refetched")
	}

	// Shortened TTLs (non-owner shadow copy, peer answer without a TTL) only make
	// an entry look older, so they never hide a refetch.
	if !p.shouldBypassRecentTailCache("labels", ttl, 30*time.Second, r) {
		t.Fatal("a shortened-TTL copy must be refetched")
	}
}
