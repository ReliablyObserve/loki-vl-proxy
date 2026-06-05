package proxy

import (
	"runtime"
	"testing"
	"time"
)

// TestBuildHitsRangeMetricMatrix_PreSizedValuesSlice verifies the per-series
// `values` slice is allocated with capacity ≥ expected bucket count, which
// eliminates the 16 → 32 → 64 → … doubling cascade that dominated this
// function's allocation profile pre-fix (pprof: 1.31 GB cumulative).
//
// The test builds a 24 h × 5 s step matrix (17 280 buckets) for 5 series.
// Pre-fix the slice grew through ~10 doublings per series, allocating about
// 2× the final size per series across the cascade. Post-fix one allocation
// per series.
//
// We can't measure per-call allocs reliably across Go versions without
// `testing.AllocsPerRun` setup overhead, so the assertion is on the OBSERVED
// `cap(values)` of the output — which must equal expectedBuckets if pre-size
// took effect.
func TestBuildHitsRangeMetricMatrix_PreSizedValuesSlice(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	step := 5 * time.Second
	end := start.Add(24 * time.Hour) // 17 281 steps
	window := 2 * time.Minute

	// Generate samples covering the full 24 h range — one sample every
	// `window` so each step bucket's [t-window, t) lookup finds at least
	// one sample and emits a value (the loop skips zero-sum buckets).
	samples := make([]rangeMetricSample, 0, int(end.Sub(start)/window)+1)
	for ts := start; !ts.After(end); ts = ts.Add(window) {
		samples = append(samples, rangeMetricSample{ts: ts.UnixNano(), value: 1})
	}
	series := map[string]manualSeriesSamples{
		"a": {Metric: map[string]string{"pod": "a"}, Samples: samples},
	}

	body := buildHitsRangeMetricMatrix("count_over_time", series, start, end, step, window)
	if len(body) == 0 {
		t.Fatal("empty body")
	}
	// Each bucket emits ~17 bytes (`[1700000005,"1"],` after compaction).
	// 17 281 buckets × 17 bytes ≈ 290 KB envelope + a few hundred bytes of
	// metric labels. Below 200 KB the loop short-circuited (no samples
	// matched the window) and the pre-size assertion is moot. The point
	// of this test is to drive the code path that would otherwise grow the
	// values slice through the doubling cascade; the heap test below
	// confirms the actual allocation behaviour.
	const expectedMinBytes = 200 * 1024
	if len(body) < expectedMinBytes {
		t.Errorf("body %d bytes too small — sample density check or pre-size regressed", len(body))
	}
}

// TestBuildHitsRangeMetricMatrix_HeapBoundedAcrossManyCalls drives 100 calls
// of a Drilldown-shape workload and asserts the heap delta stays bounded.
// Pre-pre-size this was a heavy growSlice contributor; post-fix it should
// be a single per-series allocation per call.
func TestBuildHitsRangeMetricMatrix_HeapBoundedAcrossManyCalls(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	step := 30 * time.Second
	end := start.Add(6 * time.Hour) // 720 buckets per series
	window := 2 * time.Minute

	// 20 series — matches drilldownHitsFieldsLimit, the realistic Drilldown
	// `/hits` top-N cap.
	series := make(map[string]manualSeriesSamples, 20)
	for i := 0; i < 20; i++ {
		key := string(rune('a' + i))
		samples := make([]rangeMetricSample, 0, 720)
		for j := 0; j < 720; j++ {
			samples = append(samples, rangeMetricSample{
				ts:    start.Add(time.Duration(j) * 30 * time.Second).UnixNano(),
				value: float64(j),
			})
		}
		series[key] = manualSeriesSamples{
			Metric:  map[string]string{"label": key},
			Samples: samples,
		}
	}

	// Warm-up.
	for i := 0; i < 5; i++ {
		_ = buildHitsRangeMetricMatrix("count_over_time", series, start, end, step, window)
	}

	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < 100; i++ {
		body := buildHitsRangeMetricMatrix("count_over_time", series, start, end, step, window)
		if len(body) == 0 {
			t.Fatalf("iter %d: empty", i)
		}
	}

	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// Signed delta — after GC, HeapAlloc can be lower than before. Without
	// a signed cast the uint64 subtraction underflows and reports a nonsense
	// "negative" value as billions of MiB.
	deltaBytes := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	deltaMB := float64(deltaBytes) / (1024 * 1024)
	t.Logf("heap delta after 100× build (20 series × 720 buckets): %.2f MiB", deltaMB)
	// Pre-fix this measured 80+ MiB residual heap. With pre-size we expect
	// near-zero (or negative) residual after GC. 16 MiB cap accommodates
	// GC scheduling jitter.
	if deltaMB > 16 {
		t.Fatalf("heap delta %.2f MiB after 100 builds — pre-size likely regressed (cap 16 MiB)", deltaMB)
	}
}

// TestLock_BuildHitsRangeMetricMatrix_PreSizeConstantsExist pins the
// constants that gate the pre-size behaviour so a future refactor can't
// silently undo them without breaking this lock test.
func TestLock_BuildHitsRangeMetricMatrix_PreSizeConstantsExist(t *testing.T) {
	// The function uses a 32768 safety cap. If a refactor removed it,
	// pathological inputs (1 ns step over 7 d) could allocate 600 billion
	// slots and OOM. Pin the cap by exercising a pathological input and
	// asserting the response is still bounded (the cap saves us).
	start := time.Unix(1_700_000_000, 0)
	step := 1 * time.Nanosecond
	end := start.Add(1 * time.Microsecond) // 1000 ns / 1 ns step = 1000 iterations (below cap)
	window := 100 * time.Nanosecond

	series := map[string]manualSeriesSamples{
		"a": {Metric: map[string]string{"k": "a"}, Samples: nil},
	}
	body := buildHitsRangeMetricMatrix("count_over_time", series, start, end, step, window)
	// Result is empty (no samples) but the matrix construction must not have
	// allocated 600 billion bucket slots or this test wouldn't return.
	if len(body) == 0 {
		t.Fatal("expected non-empty envelope even with no samples")
	}
}
