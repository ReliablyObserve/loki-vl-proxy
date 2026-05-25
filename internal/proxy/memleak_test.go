//go:build memleak

package proxy

// Memory-leak tests for every major loki-vl-proxy component.
//
// Run with: go test -tags=memleak -race -run 'TestMemLeak_' -timeout=10m ./internal/proxy/
//
// Design: each test establishes a heap baseline, runs N cycles of the component
// under test, forces two GC passes, then asserts the heap grew less than a
// component-specific bound. Bounds are intentionally per-component: a pure-cache
// test has a tighter limit than a handler that allocates HTTP response bodies.
//
// Pattern (mirrors metrics-governor/internal/queue/race_test.go):
//   heapBefore → work → double GC + 10ms settle → heapAfter → compare

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func mlHeapBefore() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func mlHeapAfter() uint64 {
	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func mlAssert(t *testing.T, component string, before, after uint64, cycles int, boundMB int) {
	t.Helper()
	bound := uint64(boundMB) * 1024 * 1024
	if after > before+bound {
		t.Errorf("%s memory leak: heap %dKB → %dKB after %d cycles (>%dMB growth)",
			component, before/1024, after/1024, cycles, boundMB)
	}
}

// vlQueryLineBody returns a minimal VL newline-delimited JSON log body.
func vlQueryLineBody() string {
	return `{"_msg":"test log line","_time":"2026-01-01T00:00:00Z","_stream":"{app=\"nginx\"}"}`
}

// vlFieldNamesBody returns a minimal VL field_names JSON body.
func vlFieldNamesBody() string {
	return `{"values":[{"value":"app","hits":10},{"value":"env","hits":5},{"value":"level","hits":20}]}`
}

// vlFieldValuesBody returns a minimal VL field_values JSON body.
func vlFieldValuesBody() string {
	return `{"values":[{"value":"nginx","hits":10},{"value":"gateway","hits":5}]}`
}

// vlStreamFieldNamesBody mimics the /select/logsql/stream_field_names response.
func vlStreamFieldNamesBody() string {
	return `{"values":[{"value":"app","hits":50},{"value":"env","hits":30}]}`
}

// ── cache package ─────────────────────────────────────────────────────────────

// TestMemLeak_Cache_SetEvictCycles verifies the LRU cache evicts old entries so
// heap growth is bounded. A 1000-entry cap means entries beyond capacity are
// dropped; after 100×50=5000 writes the heap must not have grown by >5 MB.
func TestMemLeak_Cache_SetEvictCycles(t *testing.T) {
	const (
		cycles  = 100
		perCycle = 50
		boundMB = 5
	)
	c := cache.New(30*time.Second, 1000)
	payload := []byte(`{"status":"success","data":["app","env","level"]}`)

	before := mlHeapBefore()

	for i := 0; i < cycles; i++ {
		for j := 0; j < perCycle; j++ {
			c.SetLocalOnlyWithTTL(fmt.Sprintf("k:%d:%d", i, j), payload, 30*time.Second)
		}
		for j := 0; j < perCycle/2; j++ {
			c.GetWithTTL(fmt.Sprintf("k:%d:%d", i, j))
		}
	}

	mlAssert(t, "cache/LRU-set-evict", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_Cache_SharedTierCycles exercises SetLocalAndDiskWithTTL / GetSharedWithTTL
// (the path used by all metadata endpoints). Bound is slightly higher because L2
// disk writes may create transient buffers even with no actual disk configured.
func TestMemLeak_Cache_SharedTierCycles(t *testing.T) {
	const (
		cycles  = 200
		boundMB = 8
	)
	c := cache.New(60*time.Second, 2000)
	payload := []byte(`{"status":"success","data":["app","env"]}`)

	before := mlHeapBefore()

	for i := 0; i < cycles; i++ {
		key := fmt.Sprintf("labels:org1:k%d", i%500) // key space wraps → evicts & re-inserts
		c.SetLocalAndDiskWithTTL(key, payload, 60*time.Second)
		c.GetSharedWithTTL(key)
	}

	mlAssert(t, "cache/shared-tier-set-get", before, mlHeapAfter(), cycles, boundMB)
}

// ── label handlers ───────────────────────────────────────────────────────────

// TestMemLeak_LabelHandler_CacheHitCycles warms the labels response cache once
// then fires 200 repeated cache-hit requests. Catches leaks in the background
// refresh goroutine scheduling path or the JSON serialisation layer.
func TestMemLeak_LabelHandler_CacheHitCycles(t *testing.T) {
	const (
		cycles  = 200
		boundMB = 5
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vlStreamFieldNamesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	// Warm.
	p.handleLabels(httptest.NewRecorder(),
		httptest.NewRequest("GET", "/loki/api/v1/labels?start=1700000000000000000&end=1700003600000000000", nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleLabels(httptest.NewRecorder(),
			httptest.NewRequest("GET", "/loki/api/v1/labels?start=1700000000000000000&end=1700003600000000000", nil))
	}
	mlAssert(t, "handler/labels-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_LabelValues_CacheHitCycles covers the label-values handler and the
// label-values index accumulation path. Bound is 10 MB because index updates use
// maps that may allocate on each unique value insertion.
func TestMemLeak_LabelValues_CacheHitCycles(t *testing.T) {
	const (
		cycles  = 200
		boundMB = 10
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Both field_names (candidate resolution) and field_values calls return valid JSON.
		fmt.Fprint(w, vlFieldValuesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	p.handleLabelValues(httptest.NewRecorder(),
		httptest.NewRequest("GET", "/loki/api/v1/label/app/values?start=1700000000000000000&end=1700003600000000000", nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleLabelValues(httptest.NewRecorder(),
			httptest.NewRequest("GET", "/loki/api/v1/label/app/values?start=1700000000000000000&end=1700003600000000000", nil))
	}
	mlAssert(t, "handler/label-values-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_DetectedFields_CacheHitCycles exercises the detected-fields path,
// including label classification and JSON marshalling. Bound 8 MB covers the
// initial warm allocation that then stays flat.
func TestMemLeak_DetectedFields_CacheHitCycles(t *testing.T) {
	const (
		cycles  = 200
		boundMB = 8
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vlFieldNamesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	p.handleDetectedFields(httptest.NewRecorder(),
		httptest.NewRequest("GET", `/loki/api/v1/detected_fields?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`, nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleDetectedFields(httptest.NewRecorder(),
			httptest.NewRequest("GET", `/loki/api/v1/detected_fields?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`, nil))
	}
	mlAssert(t, "handler/detected-fields-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_DetectedLabels_CacheHitCycles tests the detected-labels handler.
func TestMemLeak_DetectedLabels_CacheHitCycles(t *testing.T) {
	const (
		cycles  = 200
		boundMB = 8
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vlStreamFieldNamesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	p.handleDetectedLabels(httptest.NewRecorder(),
		httptest.NewRequest("GET", `/loki/api/v1/detected_labels?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`, nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleDetectedLabels(httptest.NewRecorder(),
			httptest.NewRequest("GET", `/loki/api/v1/detected_labels?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`, nil))
	}
	mlAssert(t, "handler/detected-labels-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_Series_CacheHitCycles exercises the series handler, which uses a
// shorter TTL but follows the same read-cache path.
func TestMemLeak_Series_CacheHitCycles(t *testing.T) {
	const (
		cycles  = 100
		boundMB = 10
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vlFieldNamesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	p.handleSeries(httptest.NewRecorder(),
		httptest.NewRequest("GET", `/loki/api/v1/series?start=1700000000000000000&end=1700003600000000000&match[]={app="nginx"}`, nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleSeries(httptest.NewRecorder(),
			httptest.NewRequest("GET", `/loki/api/v1/series?start=1700000000000000000&end=1700003600000000000&match[]={app="nginx"}`, nil))
	}
	mlAssert(t, "handler/series-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_IndexStats_CacheHitCycles exercises the index-stats handler.
func TestMemLeak_IndexStats_CacheHitCycles(t *testing.T) {
	const (
		cycles  = 150
		boundMB = 8
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"streams":10,"chunks":100,"entries":1000,"bytes":50000}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	p.handleIndexStats(httptest.NewRecorder(),
		httptest.NewRequest("GET", `/loki/api/v1/index/stats?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`, nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleIndexStats(httptest.NewRecorder(),
			httptest.NewRequest("GET", `/loki/api/v1/index/stats?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`, nil))
	}
	mlAssert(t, "handler/index-stats-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// ── query handlers ────────────────────────────────────────────────────────────

// TestMemLeak_QueryRange_RepeatedRequests exercises the query_range handler end-to-end
// (proxies to VL, parses VL newline-JSON, converts to Loki matrix/streams format).
// Bound is higher (20 MB) because each request allocates HTTP response buffers.
func TestMemLeak_QueryRange_RepeatedRequests(t *testing.T) {
	const (
		cycles  = 50
		boundMB = 20
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		fmt.Fprintln(w, vlQueryLineBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleQueryRange(httptest.NewRecorder(),
			httptest.NewRequest("GET",
				`/loki/api/v1/query_range?query={app="nginx"}&start=1700000000000000000&end=1700003600000000000&limit=100`,
				nil))
	}
	mlAssert(t, "handler/query-range", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_Query_RepeatedRequests exercises the instant-query handler.
func TestMemLeak_Query_RepeatedRequests(t *testing.T) {
	const (
		cycles  = 50
		boundMB = 20
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		fmt.Fprintln(w, vlQueryLineBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleQuery(httptest.NewRecorder(),
			httptest.NewRequest("GET",
				`/loki/api/v1/query?query={app="nginx"}&time=1700001800000000000&limit=100`,
				nil))
	}
	mlAssert(t, "handler/query-instant", before, mlHeapAfter(), cycles, boundMB)
}

// ── volume handlers ───────────────────────────────────────────────────────────

// TestMemLeak_Volume_CacheHitCycles exercises the log-volume handler.
func TestMemLeak_Volume_CacheHitCycles(t *testing.T) {
	const (
		cycles  = 100
		boundMB = 10
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vlFieldValuesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	p.handleVolume(httptest.NewRecorder(),
		httptest.NewRequest("GET",
			`/loki/api/v1/index/volume?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`,
			nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleVolume(httptest.NewRecorder(),
			httptest.NewRequest("GET",
				`/loki/api/v1/index/volume?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`,
				nil))
	}
	mlAssert(t, "handler/volume-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_VolumeRange_CacheHitCycles exercises the log-volume-range handler.
func TestMemLeak_VolumeRange_CacheHitCycles(t *testing.T) {
	const (
		cycles  = 100
		boundMB = 10
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vlFieldValuesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	p.handleVolumeRange(httptest.NewRecorder(),
		httptest.NewRequest("GET",
			`/loki/api/v1/index/volume_range?start=1700000000000000000&end=1700003600000000000&step=300s&query={app="nginx"}`,
			nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleVolumeRange(httptest.NewRecorder(),
			httptest.NewRequest("GET",
				`/loki/api/v1/index/volume_range?start=1700000000000000000&end=1700003600000000000&step=300s&query={app="nginx"}`,
				nil))
	}
	mlAssert(t, "handler/volume-range-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// ── patterns handler ──────────────────────────────────────────────────────────

// TestMemLeak_Patterns_RepeatedRequests exercises the patterns handler. Patterns
// use an in-memory cluster store; the bound is 15 MB to cover the initial cluster
// map allocation that stabilises after the first few requests.
func TestMemLeak_Patterns_RepeatedRequests(t *testing.T) {
	const (
		cycles  = 50
		boundMB = 15
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		for i := 0; i < 5; i++ {
			fmt.Fprintln(w, vlQueryLineBody())
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handlePatterns(httptest.NewRecorder(),
			httptest.NewRequest("GET",
				`/loki/api/v1/patterns?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`,
				nil))
	}
	mlAssert(t, "handler/patterns", before, mlHeapAfter(), cycles, boundMB)
}

// ── cache key memoisation ─────────────────────────────────────────────────────

// TestMemLeak_ReadCacheKeyMemo verifies the memo map in canonicalReadCacheKey is
// bounded: when entries exceed maxReadCacheKeyMemoEntries the map is reset, so
// 5× unique URLs do not grow the heap by more than 10 MB.
func TestMemLeak_ReadCacheKeyMemo(t *testing.T) {
	const (
		total   = 5 * maxReadCacheKeyMemoEntries
		boundMB = 10
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, vlStreamFieldNamesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	before := mlHeapBefore()
	for i := 0; i < total; i++ {
		req := httptest.NewRequest("GET",
			fmt.Sprintf("/loki/api/v1/labels?start=1700000000000000000&end=1700003600000000000&query={app=%q}", fmt.Sprintf("svc%d", i)),
			nil)
		p.canonicalReadCacheKey("labels", "", req)
	}
	mlAssert(t, "cache/read-key-memo", before, mlHeapAfter(), total, boundMB)
}

// ── translation / LogQL → LogsQL ─────────────────────────────────────────────

// TestMemLeak_Translation_HighVolume verifies that repeated LogQL→LogsQL
// translation does not accumulate allocations (regex caches, AST pools). Tight
// bound (5 MB) because translation is a pure function with no retained state.
func TestMemLeak_Translation_HighVolume(t *testing.T) {
	const (
		cycles  = 1000
		boundMB = 5
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, vlFieldNamesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	queries := []string{
		`{app="nginx"} |= "error"`,
		`{app="nginx"} | json | level="error"`,
		`sum(rate({app="nginx"}[5m])) by (status)`,
		`{app="nginx"} | logfmt | duration > 100ms`,
		`count_over_time({app="nginx"}[1h])`,
	}

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		q := queries[i%len(queries)]
		_, _ = p.translateQuery(q)
	}
	mlAssert(t, "translation/logql-to-logsql", before, mlHeapAfter(), cycles, boundMB)
}
