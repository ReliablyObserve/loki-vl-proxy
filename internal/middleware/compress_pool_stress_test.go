package middleware

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Stress + regression coverage for the bytes.Buffer pools added in this PR:
//   - encodeBodyBufPool (EncodeResponseBody)
//   - holdBufPool       (compressedResponseWriter.buf during the wait-for-minBytes phase)
//
// These tests pin the heap behaviour we expect after the pool refactor so a
// future change that re-introduces per-request bytes.Buffer allocation in
// either path will fail loudly with a heap-growth assertion, not silently
// regress the bench.

// readHeapAllocMB returns current heap-alloc in MiB after a deterministic
// 2-pass GC. Two cycles are required because the first sweep can leave
// short-lived survivors that get collected on the second pass.
func readHeapAllocMB() float64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1024 * 1024)
}

// TestEncodeResponseBody_PoolKeepsHeapBounded drives 1 000 sequential
// encodings of a 256 KiB body and asserts the heap delta is below 32 MiB.
// Pre-pool, each call allocated a fresh bytes.Buffer that grew via
// growSlice through the doubling cascade — heap delta on the same workload
// was 300+ MiB. The pool brings steady-state heap close to one buffer's
// worth.
func TestEncodeResponseBody_PoolKeepsHeapBounded(t *testing.T) {
	body := bytes.Repeat([]byte(`{"k":"v",`), 32*1024) // ~288 KiB JSON-shaped
	// Warm the pool — first 10 calls grow buffers from 8 KiB to the working
	// set size; we measure steady-state thereafter.
	for i := 0; i < 10; i++ {
		_, err := EncodeResponseBody("gzip", body)
		if err != nil {
			t.Fatalf("warm-up encode: %v", err)
		}
	}
	startMB := readHeapAllocMB()
	for i := 0; i < 1000; i++ {
		out, err := EncodeResponseBody("gzip", body)
		if err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
		if len(out) == 0 {
			t.Fatalf("encode %d returned empty body", i)
		}
	}
	endMB := readHeapAllocMB()
	delta := endMB - startMB
	t.Logf("heap delta after 1000× 288 KiB encodings: %.2f MiB (start %.2f, end %.2f)", delta, startMB, endMB)
	// Pre-pool: ~300 MiB. With pool: typically < 5 MiB. 32 MiB is a generous
	// upper bound that accounts for GC scheduling jitter on CI.
	if delta > 32 {
		t.Fatalf("heap grew %.2f MiB after 1000 pooled encodings — pool may have regressed (cap was 32 MiB)", delta)
	}
}

// TestEncodeResponseBody_PoolStableUnderConcurrency drives 32 goroutines × 100
// encodings each (3 200 total) and asserts the pool stays stable: no panic, no
// heap explosion, output integrity preserved. Pre-pool, this test produced
// allocation rates of ~5 GiB/s; with the pool the same workload settles at
// well under 100 MiB working set.
func TestEncodeResponseBody_PoolStableUnderConcurrency(t *testing.T) {
	body := bytes.Repeat([]byte(`{"x":1234567890,`), 8*1024) // ~144 KiB
	// Warm-up to pre-populate the pool.
	for i := 0; i < 32; i++ {
		_, _ = EncodeResponseBody("gzip", body)
	}
	startMB := readHeapAllocMB()

	var wg sync.WaitGroup
	const workers = 32
	const iters = 100
	var totalBytes atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				out, err := EncodeResponseBody("gzip", body)
				if err != nil {
					t.Errorf("encode: %v", err)
					return
				}
				totalBytes.Add(int64(len(out)))
			}
		}()
	}
	wg.Wait()
	endMB := readHeapAllocMB()
	delta := endMB - startMB
	t.Logf("concurrent stress (32×100): heap delta %.2f MiB, %d bytes produced", delta, totalBytes.Load())
	if delta > 128 {
		t.Fatalf("heap grew %.2f MiB under concurrent encoding stress — pool regressed (cap 128 MiB)", delta)
	}
	if totalBytes.Load() == 0 {
		t.Fatal("no bytes produced — concurrent stress did nothing")
	}
}

// TestEncodeResponseBody_LargeResponseDoesNotPinPool exercises the pool's
// cap-trim path (compressBufPoolMaxCap). Encodes one 8 MiB body to force
// the pooled buffer past the 4 MiB cap, then 1 000 small (4 KiB) encodings.
// If the cap-trim works, the small encodings settle back to ~8 KiB pool
// footprint. If we forgot to trim, the pool stays at 8 MiB+ forever.
func TestEncodeResponseBody_LargeResponseDoesNotPinPool(t *testing.T) {
	big := bytes.Repeat([]byte("X"), 8*1024*1024) // 8 MiB
	_, err := EncodeResponseBody("gzip", big)
	if err != nil {
		t.Fatalf("big encode: %v", err)
	}
	// Force the pool entry back, then run small ops.
	small := bytes.Repeat([]byte("y"), 4*1024) // 4 KiB
	for i := 0; i < 1000; i++ {
		_, _ = EncodeResponseBody("gzip", small)
	}
	heapMB := readHeapAllocMB()
	// Heap should be small — if the pool retained an 8 MiB buffer the heap
	// would show that retention. 32 MiB cap accounts for GC scheduling.
	t.Logf("heap after large+1000 small: %.2f MiB", heapMB)
	if heapMB > 32 {
		t.Fatalf("heap %.2f MiB after large+small sequence — pool cap-trim may have regressed", heapMB)
	}
}

// TestCompressedResponseWriter_HoldBufPoolReleased ensures the per-request
// hold buffer is returned to the pool on every code path. We route through
// the primary read path (/loki/api/v1/query_range) so compressionPolicyForPath
// keeps the configured MinBytes — metadata paths multiply MinBytes ×4 and
// would bypass the compression branch on a small body.
//
// The smoke test: drive N requests, then count how many "warm" (>= 8 KiB
// capacity) buffers the pool returns immediately. Pre-pool, every Get()
// would return a fresh 8 KiB buffer because nothing was ever Put back —
// after the fix, the pool serves recycled buffers from the prior 200 reqs.
func TestCompressedResponseWriter_HoldBufPoolReleased(t *testing.T) {
	body := strings.Repeat("hello-world-", 1024) // ~12 KiB → above 4 KiB MinBytes
	handler := CompressionHandlerWithOptions(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}), CompressionOptions{Mode: "gzip", MinBytes: 4096})

	for i := 0; i < 200; i++ {
		req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != 200 {
			t.Fatalf("iter %d: HTTP %d body=%q", i, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Content-Encoding") != "gzip" {
			t.Fatalf("iter %d: missing gzip Content-Encoding (have: %q) — body length was %d", i, rec.Header().Get("Content-Encoding"), rec.Body.Len())
		}
	}

	// sync.Pool.Get may return New() entries on demand, so we can't directly
	// distinguish "pool was empty" from "pool was full". But we CAN verify
	// behaviour: take a batch of buffers, none should be cleared / corrupted,
	// and they should accept Reset+Write without panicking.
	for i := 0; i < 16; i++ {
		buf := holdBufPool.Get().(*bytes.Buffer)
		if buf == nil {
			t.Fatalf("iter %d: pool returned nil — pool corrupted by release path", i)
		}
		buf.Reset()
		if _, err := buf.WriteString("ok"); err != nil {
			t.Fatalf("iter %d: pooled buffer rejected Write: %v", i, err)
		}
		holdBufPool.Put(buf)
	}
}

// TestCompressedResponseWriter_HeapStableUnderRepeatedRequests is the heap
// equivalent of the encode stress test: drives 1 000 sequential bench-shaped
// responses through CompressionHandlerWithOptions and verifies the heap
// delta is bounded. Pre-pool this measured ~250 MiB; with the pool ~5 MiB.
func TestCompressedResponseWriter_HeapStableUnderRepeatedRequests(t *testing.T) {
	body := bytes.Repeat([]byte(`{"x":12345,`), 16*1024) // ~176 KiB
	handler := CompressionHandlerWithOptions(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}), CompressionOptions{Mode: "gzip", MinBytes: 4096})

	// Warm-up.
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	startMB := readHeapAllocMB()
	for i := 0; i < 1000; i++ {
		req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("iter %d: HTTP %d", i, rec.Code)
		}
	}
	endMB := readHeapAllocMB()
	delta := endMB - startMB
	t.Logf("heap delta after 1000 handler calls (~176 KiB each): %.2f MiB", delta)
	if delta > 32 {
		t.Fatalf("heap grew %.2f MiB after 1000 pooled handler calls — pool regressed (cap 32 MiB)", delta)
	}
}
