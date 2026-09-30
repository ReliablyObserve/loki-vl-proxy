package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
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
		if strings.Contains(r.URL.Query().Get("query"), "none:") {
			// A selector that matches no stream: every listing is empty.
			writeVLFieldNames(w, nil)
			return
		}
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
			if r.URL.Query().Get("field") == detectedLevelLabel {
				values = []fieldHit{{"info", 10}}
				if f.grow.Load() {
					values = append(values, fieldHit{"warn", 1})
				}
			}
			writeVLFieldValues(w, values)
		case "/select/logsql/streams":
			values := []fieldHit{{`{app="old-app"}`, 10}}
			if f.grow.Load() {
				values = append(values, fieldHit{`{app="fresh-app"}`, 1})
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
		CompatCache:                   cache.New(60*time.Second, 100),
		TenantMap: map[string]TenantMapping{
			"a": {AccountID: "10", ProjectID: "0"},
			"b": {AccountID: "20", ProjectID: "0"},
		},
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

// Requests of one cache bucket carry different start/end values, so their
// backend calls differ and the VictoriaLogs call coalescer cannot merge them:
// only the shared refetch (labelRefreshGroup) keeps 12 concurrent near-now
// refetches to one backend pass.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_ConcurrentNearNowRefetchesShareOnePass(t *testing.T) {
	vl := &freshnessVL{delay: 100 * time.Millisecond}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	_ = getMetadataList(t, mux, freshnessPath("/loki/api/v1/labels", time.Hour, 0))
	time.Sleep(200 * time.Millisecond)
	vl.grow.Store(true)

	before := vl.calls.Load()
	_ = getMetadataList(t, mux, freshnessPath("/loki/api/v1/labels", time.Hour, 0))
	onePass := vl.calls.Load() - before
	if onePass == 0 {
		t.Fatal("the refetch made no backend call")
	}

	time.Sleep(200 * time.Millisecond)
	before = vl.calls.Load()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			path := freshnessPath("/loki/api/v1/labels", time.Hour, time.Duration(i)*time.Millisecond)
			if got := getMetadataList(t, mux, path); !contains(got, "fresh_label") {
				t.Errorf("concurrent near-now request omitted the new label: %v", got)
			}
		}(i)
	}
	wg.Wait()
	if passes := vl.calls.Load() - before; passes > 2*onePass {
		t.Fatalf("12 concurrent refetches made %d backend calls, one pass is %d", passes, onePass)
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

}

// An entry stored with a shorter TTL than the labels TTL (a non-owner shadow
// copy capped at 30s, an empty answer stored for the negative TTL) must be
// judged by the time it was stored, not by ttl minus remaining: measured that
// way a fresh copy looked minutes old and every near-now request refetched.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_ShortTTLCopiesAreJudgedByStoredAt(t *testing.T) {
	p, _ := newFreshnessProxy(t, "http://unused", 24*time.Hour)
	p.recentTailRefreshMaxStaleness = 2 * time.Second
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil) // end omitted: now
	ttl := time.Hour

	p.cache.SetLocalOnlyWithTTL("labels:shadow", []byte(`{"data":["a"]}`), 30*time.Second)
	if _, _, serve, fresh := p.metadataCacheLookup("labels", "labels:shadow", ttl, r); !serve || fresh {
		t.Fatalf("a just-stored 30s-capped copy must be served (serve=%v fresh=%v)", serve, fresh)
	}
	p.cache.SetLocalOnlyWithTTL("labels:empty", lokiLabelsResponse([]string{}), p.metadataNegativeTTL())
	if _, _, serve, fresh := p.metadataCacheLookup("labels", "labels:empty", ttl, r); !serve || fresh {
		t.Fatalf("a just-stored empty answer must be served (serve=%v fresh=%v)", serve, fresh)
	}
	restore := cache.AdvanceClockForTesting(3 * time.Second)
	defer restore()
	for _, key := range []string{"labels:shadow", "labels:empty"} {
		if _, _, serve, fresh := p.metadataCacheLookup("labels", key, ttl, r); serve || !fresh {
			t.Fatalf("%s older than max-staleness must be refetched (serve=%v fresh=%v)", key, serve, fresh)
		}
	}
}

// Back-to-back near-now /labels requests cost one backend pass per
// max-staleness, for a non-empty answer and for an empty one (stored for the
// negative TTL).
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_BackToBackNearNowRequestsMakeOnePass(t *testing.T) {
	for _, tc := range []struct{ name, query string }{{"non-empty", ""}, {"empty", `&query=%7Bnone%3D%22x%22%7D`}} {
		t.Run(tc.name, func(t *testing.T) {
			vl := &freshnessVL{}
			p, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
			p.recentTailRefreshMaxStaleness = 5 * time.Second
			now := time.Now().UnixNano()
			path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d%s", now-int64(7*24*time.Hour), now, tc.query)
			_ = getMetadataList(t, mux, path)
			afterFirst := vl.calls.Load()
			for i := 0; i < 4; i++ {
				_ = getMetadataList(t, mux, path)
			}
			if extra := vl.calls.Load() - afterFirst; extra != 0 {
				t.Fatalf("4 more near-now requests inside max-staleness made %d backend call(s)", extra)
			}
			// Past max-staleness the next request refetches, once: the ones after
			// it are inside max-staleness of the new entry again.
			restore := cache.AdvanceClockForTesting(6 * time.Second)
			defer restore()
			_ = getMetadataList(t, mux, path)
			afterRefetch := vl.calls.Load()
			if afterRefetch == afterFirst {
				t.Fatal("a request past max-staleness did not refetch")
			}
			for i := 0; i < 4; i++ {
				_ = getMetadataList(t, mux, path)
			}
			if extra := vl.calls.Load() - afterRefetch; extra != 0 {
				t.Fatalf("4 requests after the refetch made %d more backend call(s), want one pass", extra)
			}
		})
	}
}

// A near-now request with no entry (a miss) is answered from the inventory too:
// it must not read the short exact-window caches, which can hold a pass made
// before a stream was written.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_NearNowMissSkipsTheExactWindowCaches(t *testing.T) {
	vl := &freshnessVL{}
	p, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	now := time.Now().UnixNano()
	path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", now-int64(6*time.Hour), now)
	_ = getMetadataList(t, mux, path)
	vl.grow.Store(true)
	time.Sleep(200 * time.Millisecond) // past max-staleness for the compat edge cache
	// Drop only the endpoint answer: the exact-window label_inventory and
	// field-name entries of the first request stay.
	req := httptest.NewRequest(http.MethodGet, path, nil)
	p.cache.Invalidate(p.canonicalReadCacheKey("labels", "", req))
	if got := getMetadataList(t, mux, path); !contains(got, "fresh_label") {
		t.Fatalf("a near-now miss read a stale exact-window cache: %v", got)
	}
}

// Multi-tenant merged answers are cached too and follow the same rule.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_MultiTenantMergeIncludesNewStream(t *testing.T) {
	vl := &freshnessVL{}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	get := func() []string {
		req := httptest.NewRequest(http.MethodGet, freshnessPath("/loki/api/v1/labels", 6*time.Hour, 0), nil)
		req.Header.Set("X-Scope-OrgID", "a|b")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var resp struct {
			Data []string `json:"data"`
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil {
			t.Fatalf("multi-tenant /labels: %d %s", rec.Code, rec.Body.String())
		}
		return resp.Data
	}
	if got := get(); contains(got, "fresh_label") || !contains(got, "app") {
		t.Fatalf("before the write: %v", got)
	}
	vl.grow.Store(true)
	time.Sleep(200 * time.Millisecond)
	if got := get(); !contains(got, "fresh_label") {
		t.Fatalf("multi-tenant merged /labels omitted the new label: %v", got)
	}
}

// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_DetectedLevelValuesNearNow(t *testing.T) {
	vl := &freshnessVL{}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	path := freshnessPath("/loki/api/v1/label/detected_level/values", 7*24*time.Hour, 0)
	if got := getMetadataList(t, mux, path); contains(got, "warn") {
		t.Fatalf("before the write: %v", got)
	}
	vl.grow.Store(true)
	time.Sleep(200 * time.Millisecond)
	if got := getMetadataList(t, mux, path); !contains(got, "warn") {
		t.Fatalf("detected_level values omitted the new level: %v", got)
	}
}

// conformance: loki_api_v1_series, semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_SeriesNearNowIncludeNewStream(t *testing.T) {
	vl := &freshnessVL{}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	get := func() string {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, freshnessPath("/loki/api/v1/series", 6*time.Hour, 0)+"&match%5B%5D=%7Bapp%3D~%22.%2B%22%7D", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("series: %d %s", rec.Code, rec.Body.String())
		}
		return rec.Body.String()
	}
	if got := get(); strings.Contains(got, "fresh-app") {
		t.Fatalf("before the write: %s", got)
	}
	vl.grow.Store(true)
	time.Sleep(200 * time.Millisecond)
	if got := get(); !strings.Contains(got, "fresh-app") {
		t.Fatalf("near-now /series omitted the new stream: %s", got)
	}
}

// A sparse 7-day selector refreshed again after the negative TTL: empty buckets
// older than an hour follow their age schedule, so only the recent edges are
// revalidated.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_OldEmptyBucketsAreNotRevalidatedEachNegativeTTL(t *testing.T) {
	vl := &freshnessVL{}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	pathAt := func() string {
		now := time.Now().UnixNano()
		return fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d&query=%%7Bnone%%3D%%22x%%22%%7D", now-int64(7*24*time.Hour), now)
	}
	_ = getMetadataList(t, mux, pathAt())
	cold := vl.calls.Load()
	restore := cache.AdvanceClockForTesting(metadataNegativeCacheTTL + 5*time.Second)
	defer restore()
	time.Sleep(200 * time.Millisecond)
	_ = getMetadataList(t, mux, pathAt())
	refresh := vl.calls.Load() - cold
	t.Logf("cold pass %d calls, refresh after the negative TTL %d calls", cold, refresh)
	if refresh > 20 {
		t.Fatalf("refresh after the negative TTL made %d backend calls (cold pass %d): old empty buckets were revalidated", refresh, cold)
	}
}

// A non-owner replica that reads an owner's copy from the peer tier learns only
// what is left of the owner's TTL. The copy's age is the TTL minus that (the
// owner stores with the same TTL), not zero: a copy the owner stored four
// minutes ago must be refetched by a near-now request, a young one served.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_PeerCopyAgeIsDerivedFromTheOwnersTTL(t *testing.T) {
	const key = "labels:peer-key"
	var remainingMs atomic.Int64
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_cache/get" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("X-Cache-TTL-Ms", strconv.FormatInt(remainingMs.Load(), 10))
		_, _ = w.Write([]byte(`{"status":"success","data":["a"]}`))
	}))
	t.Cleanup(owner.Close)
	ownerHost := strings.TrimPrefix(owner.URL, "http://")

	var pc *cache.PeerCache
	for i := 0; i < 1024 && pc == nil; i++ {
		self := fmt.Sprintf("self-%d:3100", i)
		candidate := cache.NewPeerCache(cache.PeerConfig{
			SelfAddr: self, DiscoveryType: "static", StaticPeers: self + "," + ownerHost,
			Timeout: time.Second, WriteThrough: true, WriteThroughMinTTL: 5 * time.Second,
		})
		if candidate.IsOwner(key) {
			candidate.Close()
			continue
		}
		pc = candidate
	}
	if pc == nil {
		t.Fatal("no non-owner peer setup")
	}
	t.Cleanup(pc.Close)

	p, _ := newFreshnessProxy(t, "http://unused", 24*time.Hour)
	p.recentTailRefreshMaxStaleness = 2 * time.Second
	ttl := time.Hour
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil) // end omitted: now

	lookup := func(ownerAge time.Duration) (serve, fresh bool) {
		c := cache.New(time.Minute, 100)
		c.SetL3(pc)
		t.Cleanup(c.Close)
		p.cache = c
		remainingMs.Store((ttl - ownerAge).Milliseconds())
		_, _, serve, fresh = p.metadataCacheLookup("labels", key, ttl, r)
		return serve, fresh
	}
	if serve, fresh := lookup(4 * time.Minute); serve || !fresh {
		t.Fatalf("an owner copy stored 4m ago must be refetched (serve=%v fresh=%v)", serve, fresh)
	}
	if serve, fresh := lookup(500 * time.Millisecond); !serve || fresh {
		t.Fatalf("an owner copy stored 0.5s ago must be served (serve=%v fresh=%v)", serve, fresh)
	}
}
