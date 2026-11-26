package cache

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

// Throughput of the peer bulk restore over loopback: one serving replica holds
// LVP_RESTORE_BENCH_MB of mixed-size cached answers on its disk (the fixture of
// restore_bench_test.go), and a replica with an empty memory tier pulls all of
// them through POST /_cache/bulk into its memory, and with disk=on also queues
// them to its own new disk. Loopback has no network limit: the numbers are the
// CPU and disk cost of the two ends, not what a pod network carries.
//
//	LVP_RESTORE_BENCH_MB      decoded bytes restored, in MiB (default 128)
//	LVP_RESTORE_FIXTURE_DIR   where the fixture files are built once and kept
//	LVP_PEER_RESTORE_RATE_MB  the serving replica's rate limit in MiB/s
//	                          (default: none)
//
// MB/s is decoded bytes restored per second, wire-MB/s the key and record
// bytes the stream carried in the same time (records go as stored, so s2
// records are compressed on the wire).

type peerBenchServer struct {
	srv *httptest.Server
	c   *Cache
	dc  *DiskCache
	pc  *PeerCache
}

func (s *peerBenchServer) close() {
	s.srv.Close()
	s.pc.Close()
	s.c.Close()
	_ = s.dc.Close()
}

// openPeerBenchServer serves the fixture from its disk tier (its memory tier is
// small, as on a replica whose hot set is a fraction of its disk).
func openPeerBenchServer(tb testing.TB, f restoreFixture, codec string, rate int64) *peerBenchServer {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "serving.db")
	copyRestoreFile(tb, f.path, path, false)
	dc, err := NewDiskCache(restoreDiskConfig(path, codec))
	if err != nil {
		tb.Fatal(err)
	}
	s := &peerBenchServer{dc: dc, c: NewWithMaxBytes(time.Hour, 1000, 64<<20)}
	s.c.SetL2(dc)
	handler := new(http.Handler)
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { (*handler).ServeHTTP(w, r) }))
	addr := s.srv.Listener.Addr().String()
	s.pc = NewPeerCache(PeerConfig{SelfAddr: addr, DiscoveryType: "static", StaticPeers: addr + ",127.0.0.1:1", ReplicationFactor: 2, WriteThrough: true, Timeout: 5 * time.Second, BulkServeMaxBytesPerSecond: rate})
	s.c.SetL3(s.pc)
	// The benchmark measures the transfer: the governor sends at the given
	// rate (or none) from the start instead of ramping up under signals.
	s.pc.bulk.gov.fixed = math.Inf(1)
	if rate > 0 {
		s.pc.bulk.gov.fixed = float64(rate)
	}
	*handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.pc.ServeHTTP(w, r, s.c) })
	return s
}

// restoreFromBenchServer restores everything the server holds into a new
// replica at 127.0.0.1:1, with or without a disk of its own.
func restoreFromBenchServer(tb testing.TB, s *peerBenchServer, f restoreFixture, codec string, disk bool) (PeerRestoreReport, func()) {
	tb.Helper()
	c := NewWithMaxBytes(time.Hour, f.entries+10, int(2*f.decoded+(1<<20)))
	var dc *DiskCache
	if disk {
		cfg := restoreDiskConfig(filepath.Join(tb.TempDir(), fmt.Sprintf("restoring-%d.db", time.Now().UnixNano())), codec)
		cfg.FlushBytes, cfg.MaxBufferBytes, cfg.FlushInterval = 0, 0, 0 // the defaults a replica runs with
		var err error
		if dc, err = NewDiskCache(cfg); err != nil {
			tb.Fatal(err)
		}
		c.SetL2(dc)
	}
	pc := NewPeerCache(PeerConfig{SelfAddr: "127.0.0.1:1", DiscoveryType: "static", StaticPeers: s.srv.Listener.Addr().String() + ",127.0.0.1:1", ReplicationFactor: 2, WriteThrough: true, Timeout: 5 * time.Second})
	c.SetL3(pc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	report := c.RestoreFromPeers(ctx, PeerRestoreOptions{MaxBytes: f.decoded})
	return report, func() {
		pc.Close()
		c.Close()
		if dc != nil {
			_ = dc.Close()
			_ = os.Remove(dc.db.Path())
		}
	}
}

func peerBenchRate(tb testing.TB) int64 {
	if v := os.Getenv("LVP_PEER_RESTORE_RATE_MB"); v != "" {
		var mib int64
		if _, err := fmt.Sscan(v, &mib); err != nil || mib <= 0 {
			tb.Fatalf("LVP_PEER_RESTORE_RATE_MB=%q", v)
		}
		return mib << 20
	}
	return -1
}

func BenchmarkPeerBulkRestore(b *testing.B) {
	quietRestoreLogs(b)
	size := restoreBenchSize(b)
	rate := peerBenchRate(b)
	for _, codec := range []string{"s2", "raw"} {
		for _, disk := range []bool{false, true} {
			name := fmt.Sprintf("codec=%s/disk=%v", codec, map[bool]string{false: "off", true: "on"}[disk])
			b.Run(name, func(b *testing.B) {
				b.StopTimer()
				f := openRestoreFixture(b, codec, size)
				s := openPeerBenchServer(b, f, codec, rate)
				defer s.close()
				var wire, skipped int64
				var peak uint64
				b.SetBytes(f.decoded)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					debug.FreeOSMemory()
					runtime.GC()
					stop := heapPeakSampler()
					b.StartTimer()
					report, done := restoreFromBenchServer(b, s, f, codec, disk)
					b.StopTimer()
					peak = max(peak, stop())
					if report.Outcome != RestoreDone || report.Progress.RestoredEntries != int64(f.entries) {
						b.Fatalf("restored %d of %d: %q %+v", report.Progress.RestoredEntries, f.entries, report.Outcome, report.Peers)
					}
					if disk && report.DiskWrites+report.DiskSkipped != int64(f.entries) {
						b.Fatalf("disk writes %d + skipped %d of %d", report.DiskWrites, report.DiskSkipped, f.entries)
					}
					wire += report.Peers[0].Bytes
					skipped += report.DiskSkipped
					done()
					b.StartTimer()
				}
				seconds := b.Elapsed().Seconds()
				b.ReportMetric(float64(f.entries*b.N)/seconds, "entries/s")
				b.ReportMetric(float64(wire)/1e6/seconds, "wire-MB/s")
				b.ReportMetric(float64(wire)/float64(b.N)/1e6, "wire-MB")
				b.ReportMetric(float64(f.entries), "entries")
				b.ReportMetric(float64(peak)/1e6, "peak-heap-MB")
				b.ReportMetric(float64(skipped)/float64(b.N), "disk-skipped")
			})
		}
	}
}

// TestPeerRestore_ThroughputGate is the opt-in regression gate of the peer
// bulk restore over loopback: 2 GiB (LVP_RESTORE_BENCH_MB) must restore at
// 200 MiB/s of decoded bytes or more, with and without the receiver's disk.
func TestPeerRestore_ThroughputGate(t *testing.T) {
	if os.Getenv("LVP_PEER_RESTORE_GATE") != "1" {
		t.Skip("set LVP_PEER_RESTORE_GATE=1 to run the real-size peer restore gate")
	}
	if os.Getenv("LVP_RESTORE_BENCH_MB") == "" {
		t.Setenv("LVP_RESTORE_BENCH_MB", "2048")
	}
	quietRestoreLogs(t)
	const floor = 200 << 20 // decoded bytes per second
	size := restoreBenchSize(t)
	for _, codec := range []string{"s2", "raw"} {
		f := openRestoreFixture(t, codec, size)
		s := openPeerBenchServer(t, f, codec, -1)
		for _, disk := range []bool{false, true} {
			report, done := restoreFromBenchServer(t, s, f, codec, disk)
			done()
			rate := report.Progress.BytesPerSecond()
			t.Logf("codec=%s disk=%v: %d entries, %d MiB (%d MiB on the wire) in %v = %.0f MiB/s, disk writes %d skipped %d",
				codec, disk, report.Progress.RestoredEntries, report.Progress.RestoredBytes>>20, report.Peers[0].Bytes>>20, report.Progress.Elapsed, rate/(1<<20), report.DiskWrites, report.DiskSkipped)
			if report.Progress.RestoredEntries != int64(f.entries) || rate < floor {
				t.Fatalf("codec=%s disk=%v: %d of %d entries at %.0f MiB/s, want all at 200 MiB/s or more", codec, disk, report.Progress.RestoredEntries, f.entries, rate/(1<<20))
			}
		}
		s.close()
	}
}

// TestWritePeerLoadFixture writes the disk cache file the peer restore latency
// proof (test/e2e-fleet/peer_restore_load_test.go) serves from: LVP_RESTORE_BENCH_MB
// of mixed-size s2 records keyed restoreKey(0..n-1), at LVP_PEER_LOAD_FIXTURE,
// and the record count to LVP_PEER_LOAD_FIXTURE.entries. Opt-in.
func TestWritePeerLoadFixture(t *testing.T) {
	path := os.Getenv("LVP_PEER_LOAD_FIXTURE")
	if path == "" {
		t.Skip("set LVP_PEER_LOAD_FIXTURE to write the peer restore load fixture")
	}
	quietRestoreLogs(t)
	f := buildRestoreFixture(t, path, "s2", restoreBenchSize(t))
	if err := os.WriteFile(path+".entries", []byte(fmt.Sprintln(f.entries)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d entries, %d MiB decoded, %d MiB stored", f.entries, f.decoded>>20, f.encoded>>20)
}
