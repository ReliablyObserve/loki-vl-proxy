package cache

import (
	"fmt"
	"syscall"
	"testing"
	"time"
)

// cpuTime is the process's user plus system CPU time so far.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// The peer replication benchmarks run a three-replica in-process ring
// (newReplFleet) over loopback HTTP. Report them with -benchmem; the wire
// metrics count request bytes the replicas received.

// A local memory hit with a peer ring attached costs what it costs without
// one: replication only adds work on misses and writes.
func BenchmarkPeerReplication_GetLocalHit(b *testing.B) {
	f := newReplFleet(b, 3, 2, true, time.Second)
	key := f.keyWith(0, 1)
	f.nodes[0].c.SetWithTTL(key, []byte(`{"status":"success","data":["a","b"]}`), time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, ok := f.nodes[0].c.Get(key); !ok {
			b.Fatal("miss")
		}
	}
}

// One write-through Set by the key's owner: the local store and the pushes
// to the other replicas (RF 1: none, RF 2: one, RF 3: two), the pushes
// waited for so their CPU is inside the measurement.
func BenchmarkPeerReplication_SetOwner(b *testing.B) {
	value := make([]byte, 4096)
	for rf := 1; rf <= 3; rf++ {
		b.Run(fmt.Sprintf("rf=%d", rf), func(b *testing.B) {
			f := newReplFleet(b, 3, rf, true, time.Second)
			keys := make([]string, 0, 64)
			for i := 0; len(keys) < cap(keys); i++ {
				key := fmt.Sprintf("set-%d", i)
				if f.replicaIdx(key)[0] == 0 {
					keys = append(keys, key)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			n := 0
			cpu0 := cpuTime()
			for b.Loop() {
				f.nodes[0].c.SetWithTTL(keys[n%len(keys)], value, time.Hour)
				n++
				want := int64(n * (rf - 1))
				for {
					ok, failed := f.pushes()
					if ok+failed >= want {
						break
					}
					time.Sleep(10 * time.Microsecond)
				}
			}
			b.StopTimer()
			cpu := cpuTime() - cpu0
			var wire int64
			for _, nd := range f.nodes {
				wire += nd.wire.Load()
			}
			b.ReportMetric(float64(wire)/float64(n), "wire-B/op")
			b.ReportMetric(float64(cpu.Nanoseconds())/float64(n), "cpu-ns/op")
		})
	}
}

// A lookup answered by the key's owner (no local copy kept between
// iterations: the peer fetch alone).
func BenchmarkPeerReplication_FetchOwner(b *testing.B) {
	f := newReplFleet(b, 3, 2, true, time.Second)
	key := f.keyWith(0, 1)
	f.nodes[0].c.SetWithTTL(key, make([]byte, 4096), time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, ok := f.nodes[2].pc.Get(key); !ok {
			b.Fatal("miss")
		}
	}
}

// A lookup answered by the backup: the owner down (its breaker open after
// the first failures), and the owner itself after losing its disk.
func BenchmarkPeerReplication_FetchBackup(b *testing.B) {
	b.Run("owner_down", func(b *testing.B) {
		f := newReplFleet(b, 3, 2, true, time.Second)
		key := f.keyWith(0, 1)
		f.nodes[0].c.SetWithTTL(key, make([]byte, 4096), time.Hour)
		waitFor(b, "the backup copy", func() bool { _, _, ok := f.nodes[1].c.GetWithTTL(key); return ok })
		f.stopNode(0)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if _, _, ok := f.nodes[2].pc.Get(key); !ok {
				b.Fatal("miss")
			}
		}
	})
	b.Run("owner_lost_disk", func(b *testing.B) {
		f := newReplFleet(b, 3, 2, true, time.Second)
		key := f.keyWith(0, 1)
		f.nodes[0].c.SetWithTTL(key, make([]byte, 4096), time.Hour)
		waitFor(b, "the backup copy", func() bool { _, _, ok := f.nodes[1].c.GetWithTTL(key); return ok })
		f.restart(0, true)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if _, _, ok := f.nodes[0].pc.Get(key); !ok {
				b.Fatal("miss")
			}
		}
	})
}

// What holding the copies costs: 1,000 keys of 4 KiB written through one
// replica, then the memory and disk all replicas hold, per key, for RF 1-3.
func BenchmarkPeerReplication_Footprint(b *testing.B) {
	for rf := 1; rf <= 3; rf++ {
		b.Run(fmt.Sprintf("rf=%d", rf), func(b *testing.B) {
			const keys = 1000
			value := make([]byte, 4096)
			for i := range value {
				value[i] = byte(i * 7)
			}
			var mem, disk int64
			for b.Loop() {
				f := newReplFleet(b, 3, rf, true, time.Second)
				for i := 0; i < keys; i++ {
					key := fmt.Sprintf("footprint-%d", i)
					f.nodes[f.replicaIdx(key)[0]].c.SetWithTTL(key, value, time.Hour)
				}
				waitFor(b, "the pushes", func() bool { ok, failed := f.pushes(); return ok+failed >= int64(keys*(rf-1)) })
				mem, disk = 0, 0
				for _, nd := range f.nodes {
					_, m := nd.c.Size()
					nd.dc.Flush()
					_, d := nd.dc.Size()
					mem += int64(m)
					disk += d
				}
				f.close()
			}
			b.ReportMetric(float64(mem)/keys, "mem-B/key")
			b.ReportMetric(float64(disk)/keys, "disk-B/key")
		})
	}
}

// The refill after a lost disk: an owner that restarted without its disk
// looks up 1,000 keys of 4 KiB; each comes from the backup once and is
// stored back in memory and on disk.
func BenchmarkPeerReplication_RefillAfterDiskLoss(b *testing.B) {
	const keys = 1000
	value := make([]byte, 4096)
	var fetched int64
	for b.Loop() {
		b.StopTimer()
		f := newReplFleet(b, 3, 2, true, time.Second)
		var owned []string
		for i := 0; len(owned) < keys; i++ {
			key := fmt.Sprintf("refill-%d", i)
			if f.replicaIdx(key)[0] == 0 {
				owned = append(owned, key)
				f.nodes[0].c.SetWithTTL(key, value, time.Hour)
			}
		}
		waitFor(b, "the pushes", func() bool { ok, _ := f.pushes(); return ok >= keys })
		f.restart(0, true)
		b.StartTimer()
		for _, key := range owned {
			if _, _, _, tier, ok := f.nodes[0].c.GetSharedWithAge(key); !ok || tier != "l3_peer" {
				b.Fatalf("%s: %v %q", key, ok, tier)
			}
		}
		b.StopTimer()
		fetched = f.nodes[0].pc.PeerHits.Load()
		f.close()
		b.StartTimer()
	}
	b.ReportMetric(float64(fetched), "keys-from-backup")
	b.ReportMetric(float64(fetched*int64(len(value))), "bytes-fetched")
}

// A lookup of a key no replica holds (a cold request's miss before it
// computes): a replica that is not the owner asks every other replica, an
// owner asks its backups; with factor 1 an owner asks nobody.
func BenchmarkPeerReplication_ColdMiss(b *testing.B) {
	for rf := 1; rf <= 2; rf++ {
		for _, who := range []string{"owner", "other"} {
			b.Run(fmt.Sprintf("rf=%d/%s", rf, who), func(b *testing.B) {
				f := newReplFleet(b, 3, rf, true, time.Second)
				asker := 0
				if who == "other" {
					asker = 2
				}
				var keys []string
				for i := 0; len(keys) < 256; i++ {
					key := fmt.Sprintf("absent-%d", i)
					if r := f.replicaIdx(key); r[0] == 0 && (rf == 1 || r[1] == 1) {
						keys = append(keys, key)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				n := 0
				cpu0 := cpuTime()
				for b.Loop() {
					if _, _, ok := f.nodes[asker].pc.Get(keys[n%len(keys)]); ok {
						b.Fatal("hit")
					}
					n++
				}
				b.ReportMetric(float64((cpuTime()-cpu0).Nanoseconds())/float64(n), "cpu-ns/op")
			})
		}
	}
}
