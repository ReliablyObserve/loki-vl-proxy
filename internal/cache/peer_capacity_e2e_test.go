package cache

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const capacityFleetBytes = 16 << 10

// The fixture uses the production caches, HTTP handlers and peer workers. Only
// the final backend is replaced with a counter and deterministic query results.
type capacityFleet struct {
	*replFleet
	entries int
	bytes   int
	alive   []bool
	backend atomic.Int64
}

func newCapacityFleet(t testing.TB, rf, entries, maxBytes int) *capacityFleet {
	t.Helper()
	f := &capacityFleet{replFleet: &replFleet{t: t, timeout: time.Second, wt: true, rf: rf}, entries: entries, bytes: maxBytes, alive: make([]bool, 3)}
	for i := 0; i < 3; i++ {
		node := &replNode{disk: filepath.Join(t.TempDir(), "cache.db")}
		node.handler.Store(http.Handler(http.NotFoundHandler()))
		node.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { node.handler.Load().(http.Handler).ServeHTTP(w, r) }))
		f.nodes = append(f.nodes, node)
	}
	for i := range f.nodes {
		f.startBounded(i)
	}
	t.Cleanup(f.close)
	return f
}

func (f *capacityFleet) startBounded(i int) {
	f.t.Helper()
	node := f.nodes[i]
	peers := make([]string, len(f.nodes))
	for j := range peers {
		peers[j] = f.addr(j)
	}
	node.c = NewWithMaxBytes(10*time.Minute, f.entries, f.bytes)
	disk, err := NewDiskCache(DiskCacheConfig{Path: node.disk, MaxEntries: f.entries, MaxBytes: int64(f.bytes), MaxIndexBytes: 64 << 10, MaxBufferBytes: 64 << 10, FlushSize: 64, FlushInterval: time.Hour})
	if err != nil {
		f.t.Fatal(err)
	}
	node.dc = disk
	node.c.SetL2(disk)
	node.pc = NewPeerCache(PeerConfig{SelfAddr: f.addr(i), DiscoveryType: "static", StaticPeers: strings.Join(peers, ","), ReplicationFactor: f.rf, WriteThrough: true, WriteThroughMinTTL: time.Second, Timeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	node.pc.SetPushFilter(func(key string) bool { return !strings.Contains(key, ":auth:user.") })
	node.c.SetL3(node.pc)
	c, peer := node.c, node.pc
	node.handler.Store(http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { peer.ServeHTTP(w, r, c) })))
	f.alive[i] = true
}

func (f *capacityFleet) stop(i int) {
	if f.alive[i] {
		f.stopNode(i)
		f.alive[i] = false
	}
}

func (f *capacityFleet) restartBounded(i int) { f.stop(i); f.startBounded(i) }

func (f *capacityFleet) quiesce() {
	f.t.Helper()
	waitFor(f.t, "peer publication to quiesce", func() bool {
		for i, node := range f.nodes {
			if !f.alive[i] {
				continue
			}
			if node.pc.pushers.Load() != 0 || node.pc.pushQueued.Load() != 0 || len(node.pc.pushQueue) != 0 {
				return false
			}
			pending := false
			node.pc.breakers.Range(func(_, value any) bool {
				breaker := value.(*peerBreaker)
				breaker.mu.Lock()
				pending = len(breaker.pending) != 0
				breaker.mu.Unlock()
				return !pending
			})
			if pending {
				return false
			}
		}
		return true
	})
	for i, node := range f.nodes {
		if f.alive[i] {
			node.dc.Flush()
		}
	}
}

func capacityValue(key string) []byte {
	return []byte("backend:" + key + ":" + strings.Repeat("v", 64))
}

func (f *capacityFleet) query(frontend int, key string) []byte {
	f.t.Helper()
	if value, _, _, _, ok := f.nodes[frontend].c.GetSharedWithAge(key); ok {
		return value
	}
	f.backend.Add(1)
	value := capacityValue(key)
	f.nodes[frontend].c.SetWithTTL(key, value, 10*time.Minute)
	return value
}

func (f *capacityFleet) balancedKeys() []string {
	var keys []string
	if f.rf == 1 {
		for owner := 0; owner < 3; owner++ {
			keys = append(keys, f.keysWith(50, owner)...)
		}
	} else {
		for _, pair := range [][2]int{{0, 1}, {1, 2}, {2, 0}} {
			keys = append(keys, f.keysWith(30, pair[0], pair[1])...)
		}
	}
	return keys
}

func (f *capacityFleet) assertBudgets() {
	f.t.Helper()
	for i, node := range f.nodes {
		if !f.alive[i] {
			continue
		}
		node.c.mu.RLock()
		entries, size, shadows, shadowBytes := len(node.c.entries), node.c.curBytes, node.c.shadowEntries, node.c.shadowBytes
		node.c.mu.RUnlock()
		if entries > f.entries || size > f.bytes || shadows > f.entries/10 || shadowBytes > f.bytes/10 {
			f.t.Fatalf("node%d L1 entries=%d/%d bytes=%d/%d shadows=%d/%d shadowBytes=%d/%d", i, entries, f.entries, size, f.bytes, shadows, f.entries/10, shadowBytes, f.bytes/10)
		}
		node.dc.writeMu.Lock()
		diskEntries, diskBytes := len(node.dc.index), node.dc.logicalBytes
		node.dc.writeMu.Unlock()
		if diskEntries > f.entries || diskBytes > int64(f.bytes) {
			f.t.Fatalf("node%d disk entries=%d/%d bytes=%d/%d", i, diskEntries, f.entries, diskBytes, f.bytes)
		}
	}
}

func (f *capacityFleet) assertPlacement(keys []string) {
	f.t.Helper()
	for _, key := range keys {
		copies := 0
		for i, node := range f.nodes {
			_, diskHit := node.dc.Get(key)
			want := node.pc.IsReplica(key)
			if diskHit {
				copies++
			}
			if diskHit != want {
				f.t.Fatalf("node%d key%s disk=%v replica=%v", i, key, diskHit, want)
			}
			if want {
				if value, _, ok := node.c.GetWithTTL(key); !ok || !bytes.Equal(value, capacityValue(key)) {
					f.t.Fatalf("owned value evicted by frontend shadows: node%d key%s", i, key)
				}
			}
		}
		if copies != f.rf {
			f.t.Fatalf("key%s copies=%d wantRF%d", key, copies, f.rf)
		}
	}
	f.assertBudgets()
}

func TestPeerCapacityDatasetExceedsSingleNode(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			f := newCapacityFleet(t, rf, 64, capacityFleetBytes)
			keys := f.balancedKeys()
			if len(keys) <= 64 {
				t.Fatal("dataset did not exceed a single node")
			}
			for i, key := range keys {
				if got := f.query(i%3, key); !bytes.Equal(got, capacityValue(key)) {
					t.Fatal("wrong cold result")
				}
			}
			f.quiesce()
			if got := f.backend.Load(); got != int64(len(keys)) {
				t.Fatalf("cold backend reads=%d want%d", got, len(keys))
			}
			f.assertPlacement(keys)
			before := f.backend.Load()
			for frontend := range f.nodes {
				for _, key := range keys {
					if got := f.query(frontend, key); !bytes.Equal(got, capacityValue(key)) {
						t.Fatal("wrong warm result")
					}
				}
			}
			f.quiesce()
			if got := f.backend.Load() - before; got != 0 {
				t.Fatalf("warm backend reads=%d want0", got)
			}
			f.assertPlacement(keys)
			t.Logf("RF%d: %d distinct keys, 3 nodes x64 entries, exact persistent copies=%d; warm %d requests caused0 backend reads", rf, len(keys), rf*len(keys), 3*len(keys))
		})
	}
}

func TestPeerCapacityOwnerRestartPromotesDiskOnce(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			f := newCapacityFleet(t, rf, 64, capacityFleetBytes)
			var key string
			if rf == 2 {
				key = f.keyWith(0, 1)
			} else {
				key = f.keyWith(0)
			}
			f.query(0, key)
			f.quiesce()
			f.restartBounded(0)
			before := f.nodes[0].dc.Hits.Load()
			for _, frontend := range []int{2, 1, 2, 1} {
				f.nodes[frontend].c.Invalidate(key)
				if got := f.query(frontend, key); !bytes.Equal(got, capacityValue(key)) {
					t.Fatal("restart changed value")
				}
			}
			if reads := f.nodes[0].dc.Hits.Load() - before; reads != 1 {
				t.Fatalf("owner read disk%d times after restart, want1 followed byL1 reuse", reads)
			}
			if got := f.backend.Load(); got != 1 {
				t.Fatalf("restart caused additional backend reads: %d", got)
			}
		})
	}
}

func TestPeerCapacityOwnerLossHasExplicitRFContract(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			f := newCapacityFleet(t, rf, 64, capacityFleetBytes)
			var key string
			if rf == 2 {
				key = f.keyWith(0, 1)
			} else {
				key = f.keyWith(0)
			}
			f.query(0, key)
			f.quiesce()
			f.stop(0)
			f.nodes[2].c.Invalidate(key)
			before := f.backend.Load()
			if got := f.query(2, key); !bytes.Equal(got, capacityValue(key)) {
				t.Fatal("fault returned incorrect value")
			}
			wantReads := int64(0)
			if rf == 1 {
				wantReads = 1
			}
			if reads := f.backend.Load() - before; reads != wantReads {
				t.Fatalf("RF%d owner-loss backend reads=%d want%d", rf, reads, wantReads)
			}
			if rf == 2 && f.nodes[2].pc.BackupHits.Load() == 0 {
				t.Fatal("owner loss was not answered by backup")
			}
		})
	}
}

func TestPeerCapacityFilteredAndMemoryOnlyIsolation(t *testing.T) {
	f := newCapacityFleet(t, 2, 64, capacityFleetBytes)
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		key := "query:" + tenant + ":auth:user.credential"
		f.nodes[0].c.SetWithTTL(key, []byte(tenant), time.Minute)
		f.quiesce()
		for i, node := range f.nodes {
			value, ok := node.dc.Get(key)
			if i == 0 {
				if !ok || string(value) != tenant {
					t.Fatal("filtered result not retained at writer")
				}
			} else if ok {
				t.Fatalf("tenant/authfiltered value reached peer%d", i)
			}
		}
	}
	key := f.keyWith(0, 1)
	f.nodes[0].c.SetLocalOnlyWithTTL(key, []byte("private parsed window"), time.Minute)
	if !f.nodes[1].pc.prefetchHotKey(f.nodes[1].c, hotIndexEntry{Key: key, RemainingTTLMS: 60000}) {
		t.Fatal("memory-only prefetch failed")
	}
	f.quiesce()
	for i, node := range f.nodes {
		if _, ok := node.dc.Get(key); ok {
			t.Fatalf("memory-only window persisted at node%d", i)
		}
	}
	if pushes, errors := f.pushes(); pushes != 0 || errors != 0 {
		t.Fatalf("private data published: pushes%d errors%d", pushes, errors)
	}
}
