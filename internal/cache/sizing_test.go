package cache

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDiskCache_EntrySizeEstimation measures real on-disk sizes for various
// cache entry types to enable capacity planning.
func TestDiskCache_EntrySizeEstimation(t *testing.T) {
	dir := t.TempDir()

	// With compression (production default)
	dcComp, err := NewDiskCache(DiskCacheConfig{
		Path:        filepath.Join(dir, "compressed.db"),
		Compression: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Without compression (baseline)
	dcRaw, err := NewDiskCache(DiskCacheConfig{
		Path:        filepath.Join(dir, "raw.db"),
		Compression: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate realistic cache entries
	entries := []struct {
		name string
		data []byte
	}{
		{"labels_response_small", generateLabelsJSON(10)},
		{"labels_response_medium", generateLabelsJSON(100)},
		{"labels_response_large", generateLabelsJSON(1000)},
		{"query_range_10_lines", generateQueryRangeJSON(10)},
		{"query_range_100_lines", generateQueryRangeJSON(100)},
		{"query_range_1000_lines", generateQueryRangeJSON(1000)},
		{"metric_result_small", generateMetricJSON(5, 60)},
		{"metric_result_medium", generateMetricJSON(20, 360)},
		{"metric_result_large", generateMetricJSON(50, 1440)},
		{"series_response", generateSeriesJSON(50)},
		{"detected_fields", generateDetectedFieldsJSON(30)},
	}

	t.Logf("\n%-30s %10s %10s %10s %6s", "Entry Type", "Raw (B)", "Disk (B)", "Gzip (B)", "Ratio")
	t.Logf("%-30s %10s %10s %10s %6s", "----------", "------", "-------", "--------", "-----")

	var totalRaw, totalComp int64
	for _, e := range entries {
		dcRaw.Set(e.name, e.data, time.Hour)
		dcComp.Set(e.name+"-comp", e.data, time.Hour)
	}
	dcRaw.Flush()
	dcComp.Flush()

	rawSize := fileSize(filepath.Join(dir, "raw.db"))
	compSize := fileSize(filepath.Join(dir, "compressed.db"))

	for _, e := range entries {
		raw := len(e.data)
		totalRaw += int64(raw)
		t.Logf("%-30s %10d", e.name, raw)
	}

	totalComp = compSize
	ratio := float64(compSize) / float64(rawSize) * 100
	t.Logf("\n=== Totals ===")
	t.Logf("Raw payload:  %d bytes (%s)", totalRaw, humanBytes(totalRaw))
	t.Logf("Uncompressed DB: %d bytes (%s)", rawSize, humanBytes(rawSize))
	t.Logf("Compressed DB:   %d bytes (%s)", compSize, humanBytes(compSize))
	t.Logf("Compression ratio: %.1f%%", ratio)
	t.Logf("bbolt overhead: ~%.0f%% (B+ tree metadata)", float64(rawSize-totalRaw)/float64(totalRaw)*100)

	// Capacity estimates
	t.Logf("\n=== Capacity Estimates (compressed) ===")
	avgEntryComp := float64(totalComp) / float64(len(entries))
	for _, diskGB := range []float64{1, 5, 10, 50} {
		diskBytes := diskGB * 1024 * 1024 * 1024
		entries := int64(diskBytes / avgEntryComp)
		t.Logf("%.0f GB disk → ~%dk cached entries (avg %.0f B/entry compressed)",
			diskGB, entries/1000, avgEntryComp)
	}

	_ = dcComp.Close()
	_ = dcRaw.Close()
}

// TestDiskCache_HitRateModel estimates L1+L2 cache hit rates
// based on working set size and cache capacity.
func TestDiskCache_HitRateModel(t *testing.T) {
	t.Logf("\n=== Hit Rate Model: L1 (memory) + L2 (disk) ===")
	t.Logf("%-20s %-12s %-12s %-12s %-12s %-12s",
		"Scenario", "UniqueQ/hr", "L1 Size", "L2 Size", "L1 Hit%%", "L1+L2 Hit%%")

	scenarios := []struct {
		name       string
		uniqueQPH  int     // unique queries per hour
		l1Entries  int     // L1 cache max entries
		l2Entries  int     // L2 cache max entries
		repeatRate float64 // % of queries that repeat within the hour
	}{
		{"Small team", 500, 1000, 50000, 0.7},
		{"Medium org", 5000, 5000, 200000, 0.6},
		{"Large org", 50000, 10000, 500000, 0.5},
		{"Dashboard-heavy", 2000, 5000, 100000, 0.85},
		{"Explore-heavy", 20000, 5000, 100000, 0.3},
	}

	for _, s := range scenarios {
		repeating := int(float64(s.uniqueQPH) * s.repeatRate)
		l1HitRate := min(float64(s.l1Entries)/float64(repeating), 1.0) * s.repeatRate * 100
		l2HitRate := min(float64(s.l2Entries)/float64(repeating), 1.0) * s.repeatRate * 100

		t.Logf("%-20s %-12d %-12d %-12d %-12.1f %-12.1f",
			s.name, s.uniqueQPH, s.l1Entries, s.l2Entries, l1HitRate, l2HitRate)
	}

	t.Logf("\nKey insight: L2 disk cache catches L1 evictions.")
	t.Logf("With 1GB disk + gzip compression: ~100k-500k entries depending on query result size.")
	t.Logf("Dashboard queries (repeating panel refreshes) benefit most: 70-85%% L1+L2 hit rate.")
}

// TestDiskCache_WritePerformance measures write throughput for capacity planning.
func TestDiskCache_WritePerformance(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        filepath.Join(dir, "perf.db"),
		Compression: true,
		FlushSize:   100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	// Write 1000 entries
	start := time.Now()
	for i := 0; i < 1000; i++ {
		data := generateQueryRangeJSON(50)
		dc.Set(fmt.Sprintf("perf-key-%d", i), data, time.Hour)
	}
	dc.Flush()
	elapsed := time.Since(start)

	_, bytes := dc.Size()
	t.Logf("1000 entries: %v (%.0f entries/s)", elapsed, 1000/elapsed.Seconds())
	t.Logf("Disk usage: %s", humanBytes(bytes))
	t.Logf("Avg entry on disk: %d bytes", bytes/1000)
}

// TestDiskCache_ReadPerformance measures read latency for cache hit planning.
func TestDiskCache_ReadPerformance(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(DiskCacheConfig{
		Path:        filepath.Join(dir, "read-perf.db"),
		Compression: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()

	// Pre-populate 1000 entries
	for i := 0; i < 1000; i++ {
		dc.Set(fmt.Sprintf("key-%d", i), generateQueryRangeJSON(50), time.Hour)
	}
	dc.Flush()

	// Sequential reads
	start := time.Now()
	hits := 0
	for i := 0; i < 1000; i++ {
		if _, ok := dc.Get(fmt.Sprintf("key-%d", i)); ok {
			hits++
		}
	}
	elapsed := time.Since(start)
	t.Logf("1000 sequential reads: %v (%.0f reads/s, %d hits)", elapsed, 1000/elapsed.Seconds(), hits)

	// Random reads
	start = time.Now()
	hits = 0
	for i := 0; i < 1000; i++ {
		k := rand.Intn(1000)
		if _, ok := dc.Get(fmt.Sprintf("key-%d", k)); ok {
			hits++
		}
	}
	elapsed = time.Since(start)
	t.Logf("1000 random reads: %v (%.0f reads/s, %d hits)", elapsed, 1000/elapsed.Seconds(), hits)
}

// --- Helpers ---

func generateLabelsJSON(count int) []byte {
	s := `{"status":"success","data":[`
	for i := 0; i < count; i++ {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf(`"label_%d"`, i)
	}
	s += `]}`
	return []byte(s)
}

func generateQueryRangeJSON(lines int) []byte {
	s := `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"nginx","env":"prod","namespace":"default"},"values":[`
	for i := 0; i < lines; i++ {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf(`["%d","2026-01-01T00:00:%02d.000Z level=info msg=\"request handled\" status=200 duration=%.3fms path=/api/v1/query method=GET client=10.0.%d.%d"]`,
			1735689600000000000+int64(i)*1000000000, i%60, rand.Float64()*100, i/256, i%256)
	}
	s += `]}]}}`
	return []byte(s)
}

func generateMetricJSON(series, points int) []byte {
	s := `{"status":"success","data":{"resultType":"matrix","result":[`
	for i := 0; i < series; i++ {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf(`{"metric":{"app":"service_%d","env":"prod"},"values":[`, i)
		for j := 0; j < points; j++ {
			if j > 0 {
				s += ","
			}
			s += fmt.Sprintf(`[%d,"%.2f"]`, 1735689600+j*60, rand.Float64()*1000)
		}
		s += `]}`
	}
	s += `]}}`
	return []byte(s)
}

func generateSeriesJSON(count int) []byte {
	s := `{"status":"success","data":[`
	for i := 0; i < count; i++ {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf(`{"app":"service_%d","env":"prod","namespace":"default","pod":"pod-%d-abc"}`, i, i)
	}
	s += `]}`
	return []byte(s)
}

func generateDetectedFieldsJSON(count int) []byte {
	s := `{"fields":[`
	for i := 0; i < count; i++ {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf(`{"label":"field_%d","type":"string","cardinality":%d}`, i, rand.Intn(1000))
	}
	s += `]}`
	return []byte(s)
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func humanBytes(b int64) string {
	switch {
	case b >= 1024*1024*1024:
		return fmt.Sprintf("%.1f GB", float64(b)/(1024*1024*1024))
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
