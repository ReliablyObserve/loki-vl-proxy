package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// newBehaviorProxy builds a proxy+mux backed by vlURL with a custom label TTL.
// labelTTL=0 leaves CacheTTLs["labels"] at whatever the caller set.
func newBehaviorProxy(t *testing.T, vlURL string, labelTTL time.Duration) (*Proxy, *http.ServeMux) {
	t.Helper()
	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{BackendURL: vlURL, Cache: c, LogLevel: "error", LabelCacheTTL: labelTTL})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

// fieldNamesServer returns a VL /select/logsql/stream_field_names server that
// responds with the given labels on every call. The call counter is updated
// atomically — the handler runs on the httptest server's goroutine while the
// test reads the count from the test goroutine (and a background cache refresh
// may issue an extra request), so a plain int would race under -race.
func fieldNamesServer(t *testing.T, labels []string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	calls := new(atomic.Int64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		calls.Add(1)
		hits := make([]fieldHit, len(labels))
		for i, l := range labels {
			hits[i] = fieldHit{l, 1}
		}
		writeVLFieldNames(w, hits)
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

// getLabels issues GET /loki/api/v1/labels and returns the parsed label list.
func getLabels(t *testing.T, mux *http.ServeMux, window time.Duration) []string {
	t.Helper()
	end := perfBaseTimeNs
	start := end - int64(window)
	path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d&query=%%2A", start, end)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("labels returned %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode labels response: %v", err)
	}
	return resp.Data
}

// containsAll checks that every element in want appears in got.
func containsAll(got, want []string) bool {
	set := make(map[string]bool, len(got))
	for _, g := range got {
		set[g] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

// =============================================================================
// LabelCacheTTL configuration
// =============================================================================

func TestLabelCache_CustomTTLApplied(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app", "env"})
	p, mux := newBehaviorProxy(t, srv.URL, 3*time.Minute)

	// First call populates the cache.
	_ = getLabels(t, mux, time.Hour)

	// Per-proxy TTL fields must reflect the configured override (not the global map).
	if got := p.cacheTTLLabels; got != 3*time.Minute {
		t.Errorf("p.cacheTTLLabels = %v, want 3m", got)
	}
	if got := p.cacheTTLLabelValues; got != 3*time.Minute {
		t.Errorf("p.cacheTTLLabelValues = %v, want 3m", got)
	}
	// Global CacheTTLs must NOT be mutated — it's shared across Proxy instances.
	if got := CacheTTLs["labels"]; got != 5*time.Minute {
		t.Errorf("CacheTTLs[\"labels\"] was mutated: %v, want 5m (global must stay immutable)", got)
	}
}

func TestLabelCache_ZeroTTLUsesDefault(t *testing.T) {
	// TTL=0 should use the global default (5m) on the per-proxy fields.
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, mux := newBehaviorProxy(t, srv.URL, 0)
	_ = getLabels(t, mux, time.Hour)

	if got := p.cacheTTLLabels; got != 5*time.Minute {
		t.Errorf("zero TTL: p.cacheTTLLabels = %v, want 5m (global default)", got)
	}
}

// =============================================================================
// storeLabelsCache — each listing of the whole window replaces the answer
// =============================================================================

func TestStoreLabelsCache_EmptyExisting(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-store-empty"
	p.storeLabelsCache("labels", key, []string{"app", "env"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
	if !ok {
		t.Fatal("expected cache entry after merge into empty")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !containsAll(resp.Data, []string{"app", "env"}) {
		t.Errorf("expected app,env in cache, got %v", resp.Data)
	}
}

// Loki lists the labels of the streams with data in the requested window: a
// later listing of the window replaces the cached one, so labels whose last
// stream left the window are dropped instead of kept by a union.
//
// conformance: quality/label-cache-aged-labels-kept
func TestStoreLabelsCache_ReplacesExisting(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-store-replace"
	p.storeLabelsCache("labels", key, []string{"app", "env"}, 5*time.Minute)
	// The next listing of the window no longer holds app and env.
	p.storeLabelsCache("labels", key, []string{"namespace"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
	if !ok {
		t.Fatal("expected cache entry after second listing")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !slices.Equal(resp.Data, []string{"namespace"}) {
		t.Errorf("cached %v, want only the newest listing [namespace]", resp.Data)
	}
}

func TestStoreLabelsCache_DeduplicatesLabels(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-store-dedup"
	p.storeLabelsCache("labels", key, []string{"app", "env"}, 5*time.Minute)
	p.storeLabelsCache("labels", key, []string{"app", "env", "app"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
	if !ok {
		t.Fatal("expected cache entry")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	seen := make(map[string]int)
	for _, l := range resp.Data {
		seen[l]++
	}
	for l, count := range seen {
		if count > 1 {
			t.Errorf("label %q appears %d times, want 1", l, count)
		}
	}
}

func TestStoreLabelsCache_ResultIsSorted(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-store-sorted"
	// Insert in reverse alphabetical order.
	p.storeLabelsCache("labels", key, []string{"zoo", "mid", "aaa"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
	if !ok {
		t.Fatal("expected cache entry")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !sort.StringsAreSorted(resp.Data) {
		t.Errorf("merged labels not sorted: %v", resp.Data)
	}
}

func TestStoreLabelsCache_RefreshesTTL(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-store-ttl"
	// Write with a short TTL.
	p.storeLabelsCache("labels", key, []string{"app"}, 200*time.Millisecond)
	// Immediately store again with a longer TTL — entry should still be present.
	time.Sleep(50 * time.Millisecond)
	p.storeLabelsCache("labels", key, []string{"env"}, 5*time.Minute)

	time.Sleep(300 * time.Millisecond)
	cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
	if !ok {
		t.Fatal("expected entry to survive after TTL refresh")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !slices.Equal(resp.Data, []string{"env"}) {
		t.Errorf("cached %v, want [env]", resp.Data)
	}
}

// =============================================================================
// handleLabels and its refresh replace the cached answer
// =============================================================================

// A background refresh of a cached /labels answer lists the window again and
// replaces the answer: a label VictoriaLogs no longer lists for the window
// (its last stream left it) is gone, as in Loki.
//
// conformance: quality/label-cache-aged-labels-kept
func TestHandleLabels_RefreshReplacesCachedAnswer(t *testing.T) {
	var left atomic.Bool // the streams with app and env left the window
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if !left.Load() {
			writeVLFieldNames(w, []fieldHit{{"app", 1}, {"env", 1}})
			return
		}
		writeVLFieldNames(w, []fieldHit{{"namespace", 1}})
	}))
	t.Cleanup(srv.Close)

	// One listing call per answer: the time-bucketed inventory is off here.
	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", LabelCacheTTL: 5 * time.Minute, MetadataInventoryParallelism: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	end := time.Now().Add(-2 * time.Hour)
	start := end.Add(-time.Hour)
	params := url.Values{"start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "query": {"*"}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var first struct{ Data []string }
	_ = json.Unmarshal(rec.Body.Bytes(), &first)
	if !containsAll(first.Data, []string{"app", "env"}) {
		t.Fatalf("first response %v, want app and env", first.Data)
	}
	key := p.canonicalReadCacheKey("labels", "", req)
	if _, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key); !ok {
		t.Fatal("the first answer was not cached")
	}
	left.Store(true)
	p.refreshLabelsCacheAsync("", key, "*", params.Get("start"), params.Get("end"), "", nil)
	deadline := time.Now().Add(5 * time.Second)
	for {
		cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
		var got struct{ Data []string }
		if ok && json.Unmarshal(cached, &got) == nil && slices.Contains(got.Data, "namespace") {
			if slices.Contains(got.Data, "app") || slices.Contains(got.Data, "env") {
				t.Fatalf("refreshed answer %v still lists labels the window no longer holds", got.Data)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the refresh did not store its listing")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// =============================================================================
// Keep-warm interval derivation
// =============================================================================

func TestKeepWarm_IntervalDerivedFromTTL(t *testing.T) {
	cases := []struct {
		ttl             time.Duration
		wantIntervalMin time.Duration
		wantIntervalMax time.Duration
	}{
		{5 * time.Minute, 3*time.Minute + 30*time.Second, 4 * time.Minute},
		{2 * time.Minute, 80 * time.Second, 100 * time.Second},
		{10 * time.Minute, 7 * time.Minute, 8 * time.Minute},
	}
	for _, tc := range cases {
		interval := tc.ttl * 3 / 4
		if interval < tc.wantIntervalMin || interval > tc.wantIntervalMax {
			t.Errorf("TTL=%v: derived interval=%v not in [%v, %v]", tc.ttl, interval, tc.wantIntervalMin, tc.wantIntervalMax)
		}
	}
}

// =============================================================================
// Regression: existing labels endpoint shape unchanged
// =============================================================================

func TestHandleLabels_ResponseShape(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app", "env", "namespace"})
	_, mux := newBehaviorProxy(t, srv.URL, 0)

	labels := getLabels(t, mux, time.Hour)
	if len(labels) == 0 {
		t.Fatal("expected non-empty label list")
	}
	for _, l := range labels {
		if l == "" {
			t.Error("got empty label name in response")
		}
	}
}

func TestHandleLabels_VLInternalFieldsFiltered(t *testing.T) {
	// VL internal fields (_stream, _msg, _time) must not leak. A detected_level
	// stream field is an index label, as a pushed detected_level stream label
	// is in Loki, and stays listed.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		writeVLFieldNames(w, []fieldHit{
			{"app", 1}, {"_stream", 1}, {"_msg", 1}, {"_time", 1}, {"detected_level", 1},
		})
	}))
	t.Cleanup(srv.Close)
	_, mux := newBehaviorProxy(t, srv.URL, 0)

	labels := getLabels(t, mux, time.Hour)
	listed := false
	for _, l := range labels {
		if l == "_stream" || l == "_msg" || l == "_time" {
			t.Errorf("VL internal field %q leaked into labels response", l)
		}
		listed = listed || l == "detected_level"
	}
	if !listed {
		t.Errorf("detected_level stream field missing from labels response: %v", labels)
	}
}

// waitForBackendIdle blocks until the backend call counter has stopped moving.
// Background refreshes (scheduled on hits near expiry) issue asynchronous
// backend calls; sampling the counter while one is in flight attributes it to
// whichever request happens to be under test. Waiting for the counter to
// settle keeps assertions about a following request exact.
func waitForBackendIdle(t *testing.T, calls *atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	last := calls.Load()
	stable := 0
	for stable < 20 {
		if time.Now().After(deadline) {
			t.Fatalf("backend call counter still moving after 10s (last=%d)", last)
		}
		time.Sleep(10 * time.Millisecond)
		if n := calls.Load(); n != last {
			last = n
			stable = 0
			continue
		}
		stable++
	}
}

func TestHandleLabels_SecondRequestIsCacheHit(t *testing.T) {
	srv, calls := fieldNamesServer(t, []string{"app", "env"})
	_, mux := newBehaviorProxy(t, srv.URL, 5*time.Minute)

	_ = getLabels(t, mux, time.Hour)
	waitForBackendIdle(t, calls) // let any in-flight backend work land
	callsAfterFirst := calls.Load()

	_ = getLabels(t, mux, time.Hour)
	callsAfterSecond := calls.Load()

	if callsAfterSecond != callsAfterFirst {
		t.Errorf("second request triggered %d extra VL call(s), expected 0 (cache hit)", callsAfterSecond-callsAfterFirst)
	}
}

// The merged set is deduplicated and byte-sorted as the client reads it. The
// cached answer is JSON, which writes each invalid UTF-8 byte as U+FFFD, so a raw
// label and its replaced form are one entry, and a second merge of the same
// labels changes nothing. (Loki's labels are valid UTF-8 and returned in
// sort.Strings byte order.)
// conformance: loki_api_v1_labels
func TestStoreLabelsCache_InvalidUTF8IsDedupedAndByteSorted(t *testing.T) {
	for _, tc := range []struct {
		name   string
		labels []string
		want   []string
	}{
		{"fuzz_input_mixed_bytes", []string{"0", "\xff", "\xb30"}, []string{"0", "\ufffd", "\ufffd0"}},
		{"two_invalid_bytes_are_two_replacements", []string{"a\xff\xfeb", "a\ufffd\ufffdb"}, []string{"a\ufffd\ufffdb"}},
		{"valid_multibyte_keeps_byte_order", []string{"z", "\u00e9", "a", "\u4e2d"}, []string{"a", "z", "\u00e9", "\u4e2d"}},
		{"duplicates_and_empty", []string{"b", "", "a", "b", ""}, []string{"", "a", "b"}},
		{"truncated_sequence", []string{"\xe4\xb8", "\xe4\xb8\xad"}, []string{"\u4e2d", "\ufffd\ufffd"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newBehaviorProxy(t, "http://127.0.0.1:1", 0)
			for pass := 0; pass < 2; pass++ { // a second merge of the same labels is a no-op
				p.storeLabelsCache("labels", "key", tc.labels, 5*time.Minute)
				cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", "key")
				if !ok {
					t.Fatal("nothing cached")
				}
				var resp struct{ Data []string }
				if err := json.Unmarshal(cached, &resp); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(resp.Data, tc.want) || !sort.StringsAreSorted(resp.Data) {
					t.Fatalf("pass %d: %q, want %q (sorted)", pass, resp.Data, tc.want)
				}
			}
		})
	}
}

// =============================================================================
// Fuzz: storeLabelsCache is safe for arbitrary label inputs
// =============================================================================

func FuzzStoreLabelsCache(f *testing.F) {
	f.Add("app", "env", "namespace")
	f.Add("", "a", "b")
	f.Add("_internal", "normal-label", "dot.ted")
	f.Add("very-long-label-name-that-exceeds-typical-length", "x", "y")

	f.Fuzz(func(t *testing.T, a, b, c string) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c2 := cache.New(60*time.Second, 100)
		p, err := New(Config{BackendURL: srv.URL, Cache: c2, LogLevel: "error"})
		if err != nil {
			t.Skip("proxy init failed:", err)
		}

		labels := []string{a, b, c}
		const key = "fuzz-key"

		// Must not panic.
		p.storeLabelsCache("labels", key, labels, 5*time.Minute)
		p.storeLabelsCache("labels", key, labels, 5*time.Minute)

		cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
		if !ok {
			return
		}
		var resp struct{ Data []string }
		if err := json.Unmarshal(cached, &resp); err != nil {
			t.Errorf("corrupt JSON in cache after fuzz merge: %v", err)
		}
		if !sort.StringsAreSorted(resp.Data) {
			t.Errorf("merged labels not sorted: %v", resp.Data)
		}
	})
}

// =============================================================================
// Fuzz: storeLabelsCache with random large label sets
// =============================================================================

func TestStoreLabelsCache_LargeRandomSets(t *testing.T) {
	srv, _ := fieldNamesServer(t, nil)
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	rng := rand.New(rand.NewPCG(42, 1))
	alphabet := "abcdefghijklmnopqrstuvwxyz_.-"

	randLabel := func() string {
		n := rng.IntN(20) + 1
		var b strings.Builder
		for range n {
			b.WriteByte(alphabet[rng.IntN(len(alphabet))])
		}
		return b.String()
	}

	const key = "large-random-store"
	var allExpected map[string]bool

	for range 10 {
		batch := make([]string, rng.IntN(50)+1)
		allExpected = make(map[string]bool, len(batch))
		for i := range batch {
			batch[i] = randLabel()
			allExpected[batch[i]] = true
		}
		p.storeLabelsCache("labels", key, batch, 5*time.Minute)
	}

	cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
	if !ok {
		t.Fatal("expected cache entry after large random merge")
	}
	var resp struct{ Data []string }
	if err := json.Unmarshal(cached, &resp); err != nil {
		t.Fatalf("corrupt JSON: %v", err)
	}
	if !sort.StringsAreSorted(resp.Data) {
		t.Error("merged result not sorted")
	}
	// The cached answer is exactly the last listing, deduplicated.
	gotSet := make(map[string]bool, len(resp.Data))
	for _, l := range resp.Data {
		gotSet[l] = true
	}
	if len(gotSet) != len(resp.Data) || len(gotSet) != len(allExpected) {
		t.Fatalf("cached %d labels (%d distinct), want the last listing's %d", len(resp.Data), len(gotSet), len(allExpected))
	}
	for l := range allExpected {
		if !gotSet[l] {
			t.Errorf("label %q of the last listing missing from the cache", l)
		}
	}
}

// =============================================================================
// Edge case: single-label and zero-label inputs
// =============================================================================

func TestStoreLabelsCache_SingleLabel(t *testing.T) {
	srv, _ := fieldNamesServer(t, nil)
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "single-label"
	p.storeLabelsCache("labels", key, []string{"only"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
	if !ok {
		t.Fatal("expected cache entry")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if len(resp.Data) != 1 || resp.Data[0] != "only" {
		t.Errorf("expected [only], got %v", resp.Data)
	}
}

func TestStoreLabelsCache_ZeroLabels(t *testing.T) {
	srv, _ := fieldNamesServer(t, nil)
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "zero-labels"
	// Seed with data.
	p.storeLabelsCache("labels", key, []string{"app"}, 5*time.Minute)
	// The window's next listing is empty: it replaces the list, with the
	// short negative TTL of an empty answer.
	p.storeLabelsCache("labels", key, []string{}, 5*time.Minute)

	cached, remaining, _, ok := p.endpointReadCacheEntry(context.Background(), "labels", key)
	if !ok {
		t.Fatal("expected cache entry")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if len(resp.Data) != 0 {
		t.Errorf("cached %v after an empty listing, want an empty list", resp.Data)
	}
	if remaining > p.metadataNegativeTTL() {
		t.Errorf("empty answer kept for %s, want at most the negative TTL %s", remaining, p.metadataNegativeTTL())
	}
}

// =============================================================================
// Edge case: TTL label_values also updated
// =============================================================================

func TestLabelCache_LabelValuesTTLMatchesLabelsTTL(t *testing.T) {
	saved := CacheTTLs["label_values"]
	defer func() { CacheTTLs["label_values"] = saved }()

	CacheTTLs["labels"] = 5 * time.Minute
	CacheTTLs["label_values"] = 5 * time.Minute

	srv, _ := fieldNamesServer(t, []string{"app"})
	_, _ = newBehaviorProxy(t, srv.URL, 3*time.Minute)

	if CacheTTLs["labels"] != CacheTTLs["label_values"] {
		t.Errorf("labels TTL %v != label_values TTL %v — they should be equal", CacheTTLs["labels"], CacheTTLs["label_values"])
	}
}
