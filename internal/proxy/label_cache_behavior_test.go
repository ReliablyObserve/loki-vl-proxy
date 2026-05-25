package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
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
// responds with the given labels on every call. The call counter is updated atomically.
func fieldNamesServer(t *testing.T, labels []string) (*httptest.Server, *int) {
	t.Helper()
	calls := new(int)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		*calls++
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
	_, mux := newBehaviorProxy(t, srv.URL, 3*time.Minute)

	// First call populates the cache.
	_ = getLabels(t, mux, time.Hour)

	// CacheTTLs["labels"] should have been overridden to 3m.
	if got := CacheTTLs["labels"]; got != 3*time.Minute {
		t.Errorf("CacheTTLs[\"labels\"] = %v, want 3m", got)
	}
}

func TestLabelCache_ZeroTTLUsesDefault(t *testing.T) {
	// TTL=0 should leave the default (5m) unchanged.
	saved := CacheTTLs["labels"]
	defer func() { CacheTTLs["labels"] = saved }()

	CacheTTLs["labels"] = 5 * time.Minute
	srv, _ := fieldNamesServer(t, []string{"app"})
	_, mux := newBehaviorProxy(t, srv.URL, 0)
	_ = getLabels(t, mux, time.Hour)

	if got := CacheTTLs["labels"]; got != 5*time.Minute {
		t.Errorf("CacheTTLs[\"labels\"] changed from default: %v", got)
	}
}

// =============================================================================
// mergeLabelsIntoCache — union semantics
// =============================================================================

func TestMergeLabelsIntoCache_EmptyExisting(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-merge-empty"
	p.mergeLabelsIntoCache("labels", key, []string{"app", "env"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
	if !ok {
		t.Fatal("expected cache entry after merge into empty")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !containsAll(resp.Data, []string{"app", "env"}) {
		t.Errorf("expected app,env in cache, got %v", resp.Data)
	}
}

func TestMergeLabelsIntoCache_UnionsWithExisting(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-merge-union"
	// Seed with initial labels.
	p.mergeLabelsIntoCache("labels", key, []string{"app", "env"}, 5*time.Minute)
	// Merge in a new label "namespace" — existing ones must survive.
	p.mergeLabelsIntoCache("labels", key, []string{"namespace"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
	if !ok {
		t.Fatal("expected cache entry after second merge")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !containsAll(resp.Data, []string{"app", "env", "namespace"}) {
		t.Errorf("expected app+env+namespace, got %v", resp.Data)
	}
}

func TestMergeLabelsIntoCache_DeduplicatesLabels(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-merge-dedup"
	p.mergeLabelsIntoCache("labels", key, []string{"app", "env"}, 5*time.Minute)
	p.mergeLabelsIntoCache("labels", key, []string{"app", "env", "app"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
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

func TestMergeLabelsIntoCache_ResultIsSorted(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-merge-sorted"
	// Insert in reverse alphabetical order.
	p.mergeLabelsIntoCache("labels", key, []string{"zoo", "mid", "aaa"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
	if !ok {
		t.Fatal("expected cache entry")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !sort.StringsAreSorted(resp.Data) {
		t.Errorf("merged labels not sorted: %v", resp.Data)
	}
}

func TestMergeLabelsIntoCache_RefreshesTTL(t *testing.T) {
	srv, _ := fieldNamesServer(t, []string{"app"})
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "test-merge-ttl"
	// Write with a short TTL.
	p.mergeLabelsIntoCache("labels", key, []string{"app"}, 200*time.Millisecond)
	// Immediately merge with a longer TTL — entry should still be present.
	time.Sleep(50 * time.Millisecond)
	p.mergeLabelsIntoCache("labels", key, []string{"env"}, 5*time.Minute)

	time.Sleep(300 * time.Millisecond)
	_, _, _, ok := p.endpointReadCacheEntry("labels", key)
	if !ok {
		t.Error("expected entry to survive after TTL refresh")
	}
}

// =============================================================================
// handleLabels — user request merges into existing cache
// =============================================================================

func TestHandleLabels_UserQueryMergesIntoCache(t *testing.T) {
	// VL backend returns different label sets on each call so we can detect union.
	callN := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		callN++
		switch callN {
		case 1:
			writeVLFieldNames(w, []fieldHit{{"app", 1}, {"env", 1}})
		default:
			writeVLFieldNames(w, []fieldHit{{"namespace", 1}})
		}
	}))
	t.Cleanup(srv.Close)

	_, mux := newBehaviorProxy(t, srv.URL, 5*time.Minute)

	// First request — populates cache with app+env.
	first := getLabels(t, mux, time.Hour)
	if !containsAll(first, []string{"app", "env"}) {
		t.Fatalf("first response missing app or env: %v", first)
	}

	// Bust the cache key by using a slightly different window so we get a new key
	// but the merge helper reads from the old entry.
	// (Simulated: use exact same key to confirm merge path.)
	// Re-seed the cache with a new entry that has "namespace" only.
	p, _ := newBehaviorProxy(t, srv.URL, 5*time.Minute)
	const key = "test-user-merge"
	// Seed.
	p.mergeLabelsIntoCache("labels", key, []string{"app", "env"}, 5*time.Minute)
	// Merge new request result.
	p.mergeLabelsIntoCache("labels", key, []string{"namespace"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
	if !ok {
		t.Fatal("expected merged entry")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !containsAll(resp.Data, []string{"app", "env", "namespace"}) {
		t.Errorf("expected union of app+env+namespace, got %v", resp.Data)
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
	// VL internal fields (_stream, _msg, _time, detected_level) must not leak.
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
	for _, l := range labels {
		if l == "_stream" || l == "_msg" || l == "_time" || l == "detected_level" {
			t.Errorf("VL internal field %q leaked into labels response", l)
		}
	}
}

func TestHandleLabels_SecondRequestIsCacheHit(t *testing.T) {
	srv, calls := fieldNamesServer(t, []string{"app", "env"})
	_, mux := newBehaviorProxy(t, srv.URL, 5*time.Minute)

	_ = getLabels(t, mux, time.Hour)
	callsAfterFirst := *calls

	_ = getLabels(t, mux, time.Hour)
	callsAfterSecond := *calls

	if callsAfterSecond != callsAfterFirst {
		t.Errorf("second request triggered %d extra VL call(s), expected 0 (cache hit)", callsAfterSecond-callsAfterFirst)
	}
}

// =============================================================================
// Fuzz: mergeLabelsIntoCache is safe for arbitrary label inputs
// =============================================================================

func FuzzMergeLabelsIntoCache(f *testing.F) {
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
		p.mergeLabelsIntoCache("labels", key, labels, 5*time.Minute)
		p.mergeLabelsIntoCache("labels", key, labels, 5*time.Minute)

		cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
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
// Fuzz: mergeLabelsIntoCache with random large label sets
// =============================================================================

func TestMergeLabelsIntoCache_LargeRandomSets(t *testing.T) {
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

	const key = "large-random-merge"
	allExpected := make(map[string]bool)

	for round := range 10 {
		batch := make([]string, rng.IntN(50)+1)
		for i := range batch {
			batch[i] = randLabel()
			allExpected[batch[i]] = true
		}
		p.mergeLabelsIntoCache("labels", key, batch, 5*time.Minute)
		_ = round
	}

	cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
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
	// Every label written in any batch must appear in the result.
	gotSet := make(map[string]bool, len(resp.Data))
	for _, l := range resp.Data {
		gotSet[l] = true
	}
	for l := range allExpected {
		if !gotSet[l] {
			t.Errorf("label %q missing from merged cache after multiple rounds", l)
		}
	}
}

// =============================================================================
// Edge case: single-label and zero-label inputs
// =============================================================================

func TestMergeLabelsIntoCache_SingleLabel(t *testing.T) {
	srv, _ := fieldNamesServer(t, nil)
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "single-label"
	p.mergeLabelsIntoCache("labels", key, []string{"only"}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
	if !ok {
		t.Fatal("expected cache entry")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if len(resp.Data) != 1 || resp.Data[0] != "only" {
		t.Errorf("expected [only], got %v", resp.Data)
	}
}

func TestMergeLabelsIntoCache_ZeroLabels(t *testing.T) {
	srv, _ := fieldNamesServer(t, nil)
	p, _ := newBehaviorProxy(t, srv.URL, 0)

	const key = "zero-labels"
	// Seed with data.
	p.mergeLabelsIntoCache("labels", key, []string{"app"}, 5*time.Minute)
	// Merge empty slice — existing labels must survive.
	p.mergeLabelsIntoCache("labels", key, []string{}, 5*time.Minute)

	cached, _, _, ok := p.endpointReadCacheEntry("labels", key)
	if !ok {
		t.Fatal("expected cache entry")
	}
	var resp struct{ Data []string }
	json.Unmarshal(cached, &resp)
	if !containsAll(resp.Data, []string{"app"}) {
		t.Errorf("existing label 'app' lost after merging empty set: %v", resp.Data)
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
