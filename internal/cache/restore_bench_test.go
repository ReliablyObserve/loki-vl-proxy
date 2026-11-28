package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Throughput of the start-up warm-up at real size: mixed-size, compressible
// cached answers on disk, restored into memory by WarmFromHandover.
//
//	LVP_RESTORE_BENCH_MB      decoded bytes restored, in MiB (default 128)
//	LVP_RESTORE_FIXTURE_DIR   where the template files are built once and kept
//	                          (default: a temporary directory per run)
//	LVP_RESTORE_COLD=1        drop the file from the page cache before each open
//
// MB/s is decoded bytes restored into the memory cache per second of warm-up;
// disk-MB/s is the stored (encoded) bytes read in the same time. rebuild-ms is
// the disk index rebuild NewDiskCache runs before the warm-up (not timed in
// ns/op), and peak-heap-MB the sampled live heap.

const restoreFixtureVersion = 1

// restoreSizes is the mixed size distribution of the cached answers: the share
// of entries (per mille) and the size range.
var restoreSizes = []struct {
	perMille, min, max int
}{
	{600, 1 << 10, 8 << 10},
	{300, 16 << 10, 128 << 10},
	{90, 256 << 10, 1 << 20},
	{10, 2 << 20, 4 << 20},
}

// restoreCorpus is log-like JSON: repeated structure with random identifiers,
// compressing about as cached query_range answers do.
func restoreCorpus(n int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	out := make([]byte, 0, n+512)
	paths := []string{"/api/v1/query_range", "/api/v1/push", "/ready", "/loki/api/v1/labels", "/api/v2/search"}
	levels := []string{"info", "warn", "error", "debug"}
	for len(out) < n {
		out = fmt.Appendf(out, `["%d","{\"level\":\"%s\",\"trace_id\":\"%016x%016x\",\"path\":\"%s\",\"status\":%d,\"duration_ms\":%d,\"msg\":\"request completed\",\"user\":\"u-%06d\"}"],`,
			1790000000000000000+r.Int64N(1e15), levels[r.IntN(len(levels))], r.Uint64(), r.Uint64(), paths[r.IntN(len(paths))], 200+r.IntN(4)*100, r.IntN(30000), r.IntN(1000000))
	}
	return out[:n]
}

func restoreKey(i int) string {
	return fmt.Sprintf("query_range:v7:tenant-%02d:%s:%08d", i%16,
		strings.Repeat(`sum by (level) (count_over_time({app="checkout"} | json [5m]))|`, 3), i)
}

type restoreFixture struct {
	path             string
	entries          int
	decoded, encoded int64
}

func restoreDiskConfig(path, codec string) DiskCacheConfig {
	return DiskCacheConfig{
		Path: path, Codec: codec, Compression: codec != "raw", FlushInterval: time.Hour,
		MaxEntries: 2_000_000, MaxIndexBytes: 1 << 30,
		FlushSize: 1 << 20, FlushBytes: 1 << 30, MaxBufferBytes: 1 << 30,
	}
}

// buildRestoreFixture writes decodedBytes of records and the ranked hand-over a
// graceful shutdown would leave for them, without ever holding them in memory.
func buildRestoreFixture(tb testing.TB, path, codec string, decodedBytes int64) restoreFixture {
	tb.Helper()
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	dc, err := NewDiskCache(restoreDiskConfig(tmp, codec))
	if err != nil {
		tb.Fatal(err)
	}
	corpus := restoreCorpus(32 << 20)
	r := rand.New(rand.NewPCG(3, 4))
	f := restoreFixture{path: path}
	var pending int
	var records []handoverRecord
	for f.decoded < decodedBytes {
		pick, size := r.IntN(1000), 0
		for _, class := range restoreSizes {
			if pick < class.perMille {
				size = class.min + r.IntN(class.max-class.min)
				break
			}
			pick -= class.perMille
		}
		key := restoreKey(f.entries)
		if left := decodedBytes - f.decoded - int64(len(key)); int64(size) > left {
			size = int(max(left, 1))
		}
		at := r.IntN(len(corpus) - size)
		dc.Set(key, corpus[at:at+size:at+size], 240*time.Hour)
		records = append(records, handoverRecord{key: key, class: handoverClassDisk, rank: uint32(f.entries)})
		f.entries++
		f.decoded += int64(size + len(key))
		if pending += size; pending > 64<<20 {
			dc.Flush()
			pending = 0
		}
	}
	dc.Flush()
	gen, err := dc.beginHandover()
	if err != nil {
		tb.Fatal(err)
	}
	for start := 0; start < len(records); start += handoverChunkRecords {
		if err := dc.putHandoverRecords(gen, records[start:min(start+handoverChunkRecords, len(records))]); err != nil {
			tb.Fatal(err)
		}
	}
	if !dc.setHandoverManifest(gen, HandoverManifest{Scope: "s", FlushedAt: time.Now().UnixNano(), Complete: true, Ranked: len(records)}) {
		tb.Fatal("manifest refused")
	}
	dc.writeMu.Lock()
	for _, e := range dc.index {
		f.encoded += e.size
	}
	held := len(dc.index)
	dc.writeMu.Unlock()
	if held != f.entries {
		tb.Fatalf("disk holds %d of %d records", held, f.entries)
	}
	if err := dc.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		tb.Fatal(err)
	}
	return f
}

func openRestoreFixture(tb testing.TB, codec string, decodedBytes int64) restoreFixture {
	tb.Helper()
	dir := os.Getenv("LVP_RESTORE_FIXTURE_DIR")
	if dir == "" {
		dir = tb.TempDir()
	}
	path := filepath.Join(dir, fmt.Sprintf("restore-v%d-%s-%d.db", restoreFixtureVersion, codec, decodedBytes>>20))
	meta := path + ".meta"
	if raw, err := os.ReadFile(meta); err == nil {
		f := restoreFixture{path: path}
		if _, err := fmt.Sscan(string(raw), &f.entries, &f.decoded, &f.encoded); err == nil {
			if _, err := os.Stat(path); err == nil {
				return f
			}
		}
	}
	f := buildRestoreFixture(tb, path, codec, decodedBytes)
	if err := os.WriteFile(meta, []byte(fmt.Sprintln(f.entries, f.decoded, f.encoded)), 0o600); err != nil {
		tb.Fatal(err)
	}
	return f
}

func copyRestoreFile(tb testing.TB, from, to string, cold bool) {
	tb.Helper()
	in, err := os.Open(from)
	if err != nil {
		tb.Fatal(err)
	}
	defer in.Close()
	_ = os.Remove(to)
	out, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	if cold {
		if err := writeUncached(out); err != nil {
			tb.Skipf("cold page cache not available: %v", err)
		}
	}
	if _, err := io.CopyBuffer(struct{ io.Writer }{out}, struct{ io.Reader }{in}, make([]byte, 4<<20)); err != nil {
		tb.Fatal(err)
	}
	if cold {
		if err := dropFromPageCache(out); err != nil {
			tb.Fatal(err)
		}
	}
	if err := out.Close(); err != nil {
		tb.Fatal(err)
	}
}

func heapPeakSampler() (stop func() uint64) {
	var peak atomic.Uint64
	done, finished := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		for {
			metrics.Read(s)
			if v := s[0].Value.Uint64(); v > peak.Load() {
				peak.Store(v)
			}
			select {
			case <-done:
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	}()
	return func() uint64 { close(done); <-finished; return peak.Load() }
}

func restoreBenchSize(tb testing.TB) int64 {
	mib := 128
	if v := os.Getenv("LVP_RESTORE_BENCH_MB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			tb.Fatalf("LVP_RESTORE_BENCH_MB=%q", v)
		}
		mib = n
	}
	return int64(mib) << 20
}

// quietRestoreLogs keeps the disk cache's start-up lines out of the results.
func quietRestoreLogs(tb testing.TB) {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	tb.Cleanup(func() { slog.SetDefault(previous) })
}

func BenchmarkRestoreWarm(b *testing.B) {
	quietRestoreLogs(b)
	size := restoreBenchSize(b)
	cold := os.Getenv("LVP_RESTORE_COLD") == "1"
	for _, codec := range []string{"s2", "raw"} {
		b.Run("codec="+codec, func(b *testing.B) {
			b.StopTimer() // building the fixture is not measured
			f := openRestoreFixture(b, codec, size)
			path := filepath.Join(filepath.Dir(f.path), "restore-run.db")
			defer os.Remove(path)
			var rebuild time.Duration
			var peak uint64
			b.SetBytes(f.decoded)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				debug.FreeOSMemory()
				copyRestoreFile(b, f.path, path, cold)
				opened := time.Now()
				dc, err := NewDiskCache(restoreDiskConfig(path, codec))
				if err != nil {
					b.Fatal(err)
				}
				rebuild += time.Since(opened)
				c := NewWithMaxBytes(time.Hour, f.entries+10, int(2*f.decoded+(1<<20)))
				c.SetL2(dc)
				runtime.GC()
				stop := heapPeakSampler()
				b.StartTimer()
				report := c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: f.decoded})
				b.StopTimer()
				peak = max(peak, stop())
				if report.DiskEntries != f.entries || report.Bytes != f.decoded {
					b.Fatalf("restored %+v of %+v", report, f)
				}
				c.Close()
				if err := dc.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			seconds := b.Elapsed().Seconds()
			b.ReportMetric(float64(f.entries*b.N)/seconds, "entries/s")
			b.ReportMetric(float64(f.encoded*int64(b.N))/1e6/seconds, "disk-MB/s")
			b.ReportMetric(float64(f.encoded)/1e6, "disk-MB")
			b.ReportMetric(float64(f.entries), "entries")
			b.ReportMetric(float64(rebuild.Milliseconds())/float64(b.N), "rebuild-ms")
			b.ReportMetric(float64(peak)/1e6, "peak-heap-MB")
		})
	}
}

// BenchmarkRestoreShutdownFlush is the other side of a restart at the same
// size: the flush a graceful shutdown runs over a memory tier holding
// LVP_RESTORE_BENCH_MB, and the disk tier's close that makes it valid.
// disk=holds is the usual case (the disk has every record: ranks only),
// disk=empty writes every record at shutdown.
func BenchmarkRestoreShutdownFlush(b *testing.B) {
	quietRestoreLogs(b)
	size := restoreBenchSize(b)
	for _, disk := range []string{"holds", "empty"} {
		b.Run("disk="+disk, func(b *testing.B) {
			b.StopTimer()
			f := openRestoreFixture(b, "s2", size)
			path := filepath.Join(filepath.Dir(f.path), "restore-flush.db")
			defer os.Remove(path)
			b.SetBytes(f.decoded)
			b.ResetTimer()
			var closing time.Duration
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				debug.FreeOSMemory()
				copyRestoreFile(b, f.path, path, false)
				dc, err := NewDiskCache(restoreDiskConfig(path, "s2"))
				if err != nil {
					b.Fatal(err)
				}
				c := NewWithMaxBytes(time.Hour, f.entries+10, int(2*f.decoded+(1<<20)))
				c.SetL2(dc)
				if report := c.WarmFromHandover(context.Background(), HandoverWarmOptions{Scope: "s", MaxBytes: f.decoded}); report.DiskEntries != f.entries {
					b.Fatalf("restored %+v", report)
				}
				if disk == "empty" {
					dc.Purge()
				}
				b.StartTimer()
				report, err := c.FlushHandover(context.Background(), HandoverFlushOptions{Scope: "s"})
				if err != nil || report.Ranked != f.entries {
					b.Fatalf("flush %+v %v", report, err)
				}
				flushed := time.Now()
				if err := dc.Close(); err != nil {
					b.Fatal(err)
				}
				closing += time.Since(flushed)
				b.StopTimer()
				c.Close()
				b.StartTimer()
			}
			b.ReportMetric(float64(f.entries), "entries")
			b.ReportMetric(float64(closing.Milliseconds())/float64(b.N), "close-ms")
		})
	}
}

// BenchmarkRestoreIndexRebuild is the disk index rebuild NewDiskCache runs
// before the proxy listens, over LVP_RESTORE_REBUILD_ENTRIES records (default
// 10,000) of LVP_RESTORE_BENCH_MB MiB in all, stored raw.
func BenchmarkRestoreIndexRebuild(b *testing.B) {
	quietRestoreLogs(b)
	size := restoreBenchSize(b)
	entries := 10_000
	if v := os.Getenv("LVP_RESTORE_REBUILD_ENTRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			b.Fatalf("LVP_RESTORE_REBUILD_ENTRIES=%q", v)
		}
		entries = n
	}
	cold := os.Getenv("LVP_RESTORE_COLD") == "1"
	b.StopTimer()
	dir := os.Getenv("LVP_RESTORE_FIXTURE_DIR")
	if dir == "" {
		dir = b.TempDir()
	}
	template := filepath.Join(dir, fmt.Sprintf("rebuild-v%d-%d-%d.db", restoreFixtureVersion, entries, size>>20))
	if _, err := os.Stat(template); err != nil {
		dc, err := NewDiskCache(restoreDiskConfig(template+".tmp", "raw"))
		if err != nil {
			b.Fatal(err)
		}
		corpus := restoreCorpus(32 << 20)
		each := int(size) / entries
		for i := 0; i < entries; i++ {
			at := (i * 997) % (len(corpus) - each)
			dc.Set(restoreKey(i), corpus[at:at+each:at+each], 240*time.Hour)
			if i%4096 == 4095 {
				dc.Flush()
			}
		}
		if err := dc.Close(); err != nil {
			b.Fatal(err)
		}
		if err := os.Rename(template+".tmp", template); err != nil {
			b.Fatal(err)
		}
	}
	path := filepath.Join(dir, "rebuild-run.db")
	defer os.Remove(path)
	for _, readahead := range []bool{false, true} {
		b.Run(fmt.Sprintf("readahead=%t", readahead), func(b *testing.B) {
			cfg := restoreDiskConfig(path, "raw")
			cfg.RebuildReadahead = readahead
			var readBytes, fileBytes int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				copyRestoreFile(b, template, path, cold)
				b.StartTimer()
				dc, err := NewDiskCache(cfg)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if held, _ := dc.Size(); held != entries {
					b.Fatalf("rebuilt %d of %d", held, entries)
				}
				readBytes += dc.rebuildReadahead
				if info, err := os.Stat(path); err == nil {
					fileBytes += info.Size()
				}
				_ = dc.Close()
				b.StartTimer()
			}
			b.ReportMetric(float64(entries), "entries")
			b.ReportMetric(float64(readBytes)/float64(max(fileBytes, 1)), "readahead-share")
		})
	}
}
