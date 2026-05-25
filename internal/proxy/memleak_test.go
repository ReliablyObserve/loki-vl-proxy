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
	"net/url"
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

// ── detected field values ─────────────────────────────────────────────────────

// TestMemLeak_DetectedFieldValues_CacheHitCycles covers the detected-field-values
// handler (used by Grafana Drilldown to populate value pickers for parsed fields).
func TestMemLeak_DetectedFieldValues_CacheHitCycles(t *testing.T) {
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
	p.handleDetectedFieldValues(httptest.NewRecorder(),
		httptest.NewRequest("GET", `/loki/api/v1/detected_field_values/level?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`, nil))

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleDetectedFieldValues(httptest.NewRecorder(),
			httptest.NewRequest("GET", `/loki/api/v1/detected_field_values/level?start=1700000000000000000&end=1700003600000000000&query={app="nginx"}`, nil))
	}
	mlAssert(t, "handler/detected-field-values-cache-hit", before, mlHeapAfter(), cycles, boundMB)
}

// ── stream parsing ────────────────────────────────────────────────────────────

// TestMemLeak_VLLogsToLokiStreams verifies the VL→Loki stream conversion does not
// retain parsed results (maps/slices) across calls. 500 iterations of a 10-line
// body should produce <5 MB of net heap growth.
func TestMemLeak_VLLogsToLokiStreams(t *testing.T) {
	const (
		cycles  = 500
		boundMB = 5
	)
	lines := ""
	for i := 0; i < 10; i++ {
		lines += fmt.Sprintf(`{"_msg":"log line %d","_time":"2026-01-01T00:0%d:00Z","_stream":"{app=\"nginx\",env=\"prod\"}","level":"info"}`+"\n", i, i)
	}
	body := []byte(lines)

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		result := vlLogsToLokiStreams(body)
		_ = result
	}
	mlAssert(t, "stream/vl-to-loki-streams", before, mlHeapAfter(), cycles, boundMB)
}

// ── label index ───────────────────────────────────────────────────────────────

// TestMemLeak_LabelIndex_UpdateCycles verifies the label-values index does not
// grow unboundedly when the same label names are updated with overlapping value
// sets. The index uses bounded maps; re-inserting existing keys must not leak.
func TestMemLeak_LabelIndex_UpdateCycles(t *testing.T) {
	const (
		cycles  = 500
		boundMB = 5
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vlFieldValuesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	values := []string{"nginx", "gateway", "envoy", "caddy", "traefik"}

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		// Re-update with the same values — should not grow the map.
		p.updateLabelValuesIndex("", "app", values)
	}
	mlAssert(t, "label-index/update-same-values", before, mlHeapAfter(), cycles, boundMB)
}

// TestMemLeak_LabelIndex_UniqueValues verifies bounded growth when new values
// are added to the index across cycles. Growth is expected but must stay within
// the index's own eviction or capacity limit (10 MB bound for 500 unique values).
func TestMemLeak_LabelIndex_UniqueValueSets(t *testing.T) {
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

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		// Each cycle introduces one new service name to the index.
		values := []string{fmt.Sprintf("svc-%d", i), "nginx", "gateway"}
		p.updateLabelValuesIndex("", "service_name", values)
	}
	mlAssert(t, "label-index/unique-value-sets", before, mlHeapAfter(), cycles, boundMB)
}

// ── background refresh goroutines ─────────────────────────────────────────────

// TestMemLeak_BackgroundRefresh_GoroutinesBounded verifies that firing background
// label-refresh goroutines via refreshLabelsCacheAsync does not accumulate goroutines
// (i.e., singleflight deduplication prevents unbounded goroutine spawning).
func TestMemLeak_BackgroundRefresh_GoroutinesBounded(t *testing.T) {
	const (
		fires   = 200
		boundMB = 10
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vlStreamFieldNamesBody())
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	cacheKey := "labels::end=1700003600000000000&query=%2A&start=1700000000000000000"

	goroutinesBefore := runtime.NumGoroutine()
	before := mlHeapBefore()

	for i := 0; i < fires; i++ {
		p.refreshLabelsCacheAsync("", cacheKey, "*", "1700000000000000000", "1700003600000000000", "", nil)
	}
	// Allow goroutines to finish.
	time.Sleep(100 * time.Millisecond)

	goroutinesAfter := runtime.NumGoroutine()
	mlAssert(t, "background-refresh/labels-goroutine-heap", before, mlHeapAfter(), fires, boundMB)

	// Goroutine count must not grow indefinitely — singleflight deduplicates.
	if goroutinesAfter > goroutinesBefore+10 {
		t.Errorf("background refresh goroutine leak: before=%d after=%d (delta>10)",
			goroutinesBefore, goroutinesAfter)
	}
}

// ── bucket metadata time ──────────────────────────────────────────────────────

// TestMemLeak_BucketMetadataTime_HighVolume verifies that repeated bucketMetadataTime
// calls (a pure function operating on int64) produce zero net allocation.
func TestMemLeak_BucketMetadataTime_HighVolume(t *testing.T) {
	const (
		cycles  = 100_000
		boundMB = 2
	)
	before := mlHeapBefore()

	nowNs := int64(1700000000000000000)
	for i := 0; i < cycles; i++ {
		start := nowNs - int64(i%7)*int64(time.Hour)
		end := nowNs
		_, _ = bucketMetadataTime(start, end)
	}
	mlAssert(t, "cache-keys/bucket-metadata-time", before, mlHeapAfter(), cycles, boundMB)
}

// ── capMetadataTimeRange ──────────────────────────────────────────────────────

// TestMemLeak_CapMetadataTimeRange_HighVolume verifies that repeated capMetadataTimeRange
// calls on the same params produce bounded allocations. The function copies url.Values
// only when capping is required; the copy path must not leak.
func TestMemLeak_CapMetadataTimeRange_HighVolume(t *testing.T) {
	const (
		cycles  = 10_000
		boundMB = 5
	)
	params := url.Values{
		"start": {"1700000000000000000"},
		"end":   {"1700090000000000000"}, // 25h → gets capped to 1h
		"query": {`{app="nginx"}`},
	}

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		_ = capMetadataTimeRange(params, metadataMaxFieldNamesWindow)
	}
	mlAssert(t, "label-metadata/cap-time-range", before, mlHeapAfter(), cycles, boundMB)
}

// ── metrics / build-info ──────────────────────────────────────────────────────

// TestMemLeak_BuildInfo_RepeatedRequests covers the build-info and config-stub
// handlers (static JSON responses, no caching). They should be allocation-minimal.
func TestMemLeak_BuildInfo_RepeatedRequests(t *testing.T) {
	const (
		cycles  = 500
		boundMB = 3
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{}`)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	before := mlHeapBefore()
	for i := 0; i < cycles; i++ {
		p.handleBuildInfo(httptest.NewRecorder(), httptest.NewRequest("GET", "/loki/api/v1/status/buildinfo", nil))
		p.handleConfigStub(httptest.NewRecorder(), httptest.NewRequest("GET", "/config", nil))
	}
	mlAssert(t, "handler/build-info-config-stub", before, mlHeapAfter(), cycles, boundMB)
}

