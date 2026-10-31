package cache

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type nonMemberReader struct {
	cache   *Cache
	disk    *DiskCache
	peer    *PeerCache
	server  *httptest.Server
	client  *http.Client
	backend atomic.Int64
}

type readerOwnerContacts struct {
	mu    sync.Mutex
	byKey map[string][]int
}

func observeReaderOwnerContacts(pool *capacityFleet) *readerOwnerContacts {
	contacts := &readerOwnerContacts{byKey: make(map[string][]int)}
	for index, node := range pool.nodes {
		next := node.handler.Load().(http.Handler)
		node.handler.Store(http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/_cache/get" {
				key := r.URL.Query().Get("key")
				contacts.mu.Lock()
				contacts.byKey[key] = append(contacts.byKey[key], index)
				contacts.mu.Unlock()
			}
			next.ServeHTTP(w, r)
		})))
	}
	return contacts
}

func (contacts *readerOwnerContacts) snapshot(key string) []int {
	contacts.mu.Lock()
	defer contacts.mu.Unlock()
	return slices.Clone(contacts.byKey[key])
}

func (contacts *readerOwnerContacts) assertAfter(t *testing.T, key string, start int, want ...int) {
	t.Helper()
	got := contacts.snapshot(key)[start:]
	if !slices.Equal(got, want) {
		t.Fatalf("key%s owner HTTP contacts=%v, want%v", key, got, want)
	}
}

// The frontend wrapper supplies only the final backend counter. Cache lookup,
// peer HTTP requests/handlers, ownership, admission and disk storage are the
// production implementation. Reader addresses are excluded from every ring.
func newNonMemberReader(t *testing.T, pool *capacityFleet) *nonMemberReader {
	t.Helper()
	r := &nonMemberReader{cache: NewWithMaxBytes(10*time.Minute, 20, 4096), client: &http.Client{Timeout: 5 * time.Second}}
	disk, err := NewDiskCache(DiskCacheConfig{Path: filepath.Join(t.TempDir(), "reader.db"), MaxEntries: 20, MaxBytes: 4096, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	r.disk = disk
	r.cache.SetL2(disk)
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		key := request.URL.Query().Get("key")
		value, ttl, _, tier, hit := r.cache.GetSharedWithAge(key)
		if !hit {
			r.backend.Add(1)
			value, ttl, tier = capacityValue(key), 10*time.Minute, "backend"
			r.cache.SetWithTTL(key, value, ttl)
		}
		w.Header().Set("X-Test-Cache-Tier", tier)
		w.Header().Set("X-Test-Source-TTL-Ns", strconv.FormatInt(int64(ttl), 10))
		_, _ = w.Write(value)
	}))
	owners := make([]string, len(pool.nodes))
	for i := range owners {
		owners[i] = pool.addr(i)
	}
	r.peer = NewPeerCache(PeerConfig{SelfAddr: strings.TrimPrefix(r.server.URL, "http://"), DiscoveryType: "static", StaticPeers: strings.Join(owners, ","), ReplicationFactor: pool.rf, WriteThrough: true, WriteThroughMinTTL: time.Second, Timeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	r.peer.SetPushFilter(func(key string) bool { return !strings.Contains(key, ":auth:user.") })
	r.cache.SetL3(r.peer)
	t.Cleanup(func() {
		r.server.Close()
		r.client.CloseIdleConnections()
		r.peer.client.CloseIdleConnections()
		r.peer.Close()
		r.cache.Close()
		_ = r.disk.Close()
	})
	return r
}

func (r *nonMemberReader) query(t *testing.T, key string) ([]byte, time.Duration, string) {
	t.Helper()
	response, err := r.client.Get(r.server.URL + "/?key=" + url.QueryEscape(key))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	value, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("reader response status%d", response.StatusCode)
	}
	ttl, err := strconv.ParseInt(response.Header.Get("X-Test-Source-TTL-Ns"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value, time.Duration(ttl), response.Header.Get("X-Test-Cache-Tier")
}

func assertNonMemberReaderStorage(t *testing.T, r *nonMemberReader) {
	t.Helper()
	r.disk.Flush()
	if entries, _ := r.disk.Size(); entries != 0 || r.disk.Writes.Load() != 0 {
		t.Fatalf("reader added SSD replicas: entries%d writes%d", entries, r.disk.Writes.Load())
	}
	r.cache.mu.RLock()
	defer r.cache.mu.RUnlock()
	if len(r.cache.entries) != r.cache.shadowEntries || r.cache.shadowEntries > r.cache.maxEntries/10 || r.cache.shadowBytes > r.cache.maxBytes/10 {
		t.Fatalf("unbounded/non-shadow reader residency: entries%d shadows%d bytes%d", len(r.cache.entries), r.cache.shadowEntries, r.cache.shadowBytes)
	}
	if r.peer.WTPushes.Load() != 0 || r.peer.WTErrors.Load() != 0 {
		t.Fatalf("reader republished fetched values: pushes%d errors%d", r.peer.WTPushes.Load(), r.peer.WTErrors.Load())
	}
}

// Requests are sequential: reader counts exercise ownership and storage
// invariants, rather than making a concurrent-throughput or scaling claim.
func TestPeerReaderNonMembersReuseHotOwnerPool(t *testing.T) {
	for _, rf := range []int{1, 2} {
		for _, readerCount := range []int{1, 3, 6} {
			t.Run(fmt.Sprintf("rf=%d/readers=%d", rf, readerCount), func(t *testing.T) {
				pool := newCapacityFleet(t, rf, 64, capacityFleetBytes)
				var keys []string
				if rf == 1 {
					for owner := range pool.nodes {
						keys = append(keys, pool.keysWith(4, owner)...)
					}
				} else {
					for _, pair := range [][2]int{{0, 1}, {1, 2}, {2, 0}} {
						keys = append(keys, pool.keysWith(4, pair[0], pair[1])...)
					}
				}
				for i, key := range keys {
					pool.query(i%len(pool.nodes), key)
				}
				pool.quiesce()
				pool.assertPlacement(keys)
				contacts := observeReaderOwnerContacts(pool)
				primedReads := pool.backend.Load()
				readers := make([]*nonMemberReader, readerCount)
				for i := range readers {
					readers[i] = newNonMemberReader(t, pool)
				}
				for _, reader := range readers {
					if entries, _ := reader.cache.Size(); entries != 0 {
						t.Fatal("reader was not cold")
					}
					if len(reader.peer.Peers()) != len(pool.nodes) {
						t.Fatal("reader discovery changed owner membership")
					}
					for _, key := range keys {
						if reader.peer.IsReplica(key) {
							t.Fatal("reader unexpectedly joined ownership ring")
						}
						start := len(contacts.snapshot(key))
						value, ttl, tier := reader.query(t, key)
						contacts.assertAfter(t, key, start, pool.replicaIdx(key)[0])
						if !bytes.Equal(value, capacityValue(key)) || tier != "l3_peer" {
							t.Fatalf("cold reader missed hot pool: key%s tier%s value%q", key, tier, value)
						}
						if ttl <= nonOwnerShadowMaxTTL || ttl > 10*time.Minute {
							t.Fatalf("source TTL was lost or extended: %s", ttl)
						}
					}
					last := keys[len(keys)-1]
					start := len(contacts.snapshot(last))
					_, sourceTTL, tier := reader.query(t, last)
					contacts.assertAfter(t, last, start)
					if tier != "l1_memory" || sourceTTL <= nonOwnerShadowMaxTTL {
						t.Fatalf("reader shadow lost source freshness: tier%s ttl%s", tier, sourceTTL)
					}
					if _, resident, hit := reader.cache.GetWithTTL(last); !hit || resident > nonOwnerShadowMaxTTL {
						t.Fatalf("reader shadow residence exceeds bound: ttl%s hit%v", resident, hit)
					}
					if reader.backend.Load() != 0 || reader.peer.PeerHits.Load() != int64(len(keys)) {
						t.Fatalf("reader backend%d peer hits%d, want0/%d", reader.backend.Load(), reader.peer.PeerHits.Load(), len(keys))
					}
					assertNonMemberReaderStorage(t, reader)
					for _, owner := range pool.nodes {
						for _, address := range owner.pc.Peers() {
							if address == reader.peer.SelfAddr() {
								t.Fatal("owner discovery included reader")
							}
						}
					}
				}
				if pool.backend.Load() != primedReads {
					t.Fatal("hot owner pool required backend recomputation")
				}
				pool.assertPlacement(keys)
				t.Logf("RF%d: %d cold nonmember readers reused12 hot keys with0 backend reads; exactly%d owner SSD copies; no reader SSD copies", rf, readerCount, rf*len(keys))
			})
		}
	}
}

func TestPeerReaderNonMemberPreservesMemoryOnlyAndPrivateKeys(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			pool := newCapacityFleet(t, rf, 64, capacityFleetBytes)
			contacts := observeReaderOwnerContacts(pool)
			reader := newNonMemberReader(t, pool)
			var key string
			if rf == 1 {
				key = pool.keyWith(0)
			} else {
				key = pool.keyWith(0, 1)
			}
			pool.nodes[0].c.SetLocalOnlyWithTTL(key, []byte("memory-only window"), time.Minute)
			value, _, tier := reader.query(t, key)
			contacts.assertAfter(t, key, 0, 0)
			if string(value) != "memory-only window" || tier != "l3_peer" {
				t.Fatalf("memory-only lookup: value%q tier%s", value, tier)
			}
			if _, _, memoryOnly, hit := reader.cache.getForPeer(key); !hit || !memoryOnly {
				t.Fatal("reader lost memory-only flag")
			}
			assertNonMemberReaderStorage(t, reader)
			for i, node := range pool.nodes {
				node.dc.Flush()
				if _, hit := node.dc.Get(key); hit {
					t.Fatalf("memory-only value persisted at owner%d", i)
				}
			}
			for _, tenant := range []string{"tenant-a", "tenant-b"} {
				privateKey := "query:" + tenant + ":auth:user.credential"
				reader.cache.SetWithTTL(privateKey, []byte(tenant+" private"), time.Minute)
				value, _, tier := reader.query(t, privateKey)
				contacts.assertAfter(t, privateKey, 0)
				if string(value) != tenant+" private" || tier != "l1_memory" {
					t.Fatalf("private tenant cache identity lost: value%q tier%s", value, tier)
				}
				for i, node := range pool.nodes {
					if _, _, _, hit := node.c.getForPeer(privateKey); hit {
						t.Fatalf("filtered private key reached owner%d", i)
					}
				}
			}
			if reader.backend.Load() != 0 || reader.peer.WTPushes.Load() != 0 {
				t.Fatal("reader recomputed or published private/memory-only values")
			}
			// Filtered private entries intentionally retain local storage. They
			// are not part of the public owner pool or its replica count.
			reader.disk.Flush()
			if _, hit := reader.disk.Get(key); hit {
				t.Fatal("private local writes persisted unrelated memory-only window")
			}
		})
	}
}

func TestPeerReaderNonMemberRF2MissContactsOnlyBackup(t *testing.T) {
	pool := newCapacityFleet(t, 2, 64, capacityFleetBytes)
	key := pool.keyWith(0, 1)
	pool.query(0, key)
	pool.quiesce()
	// Purge only the owner through its production API; the backup stays hot.
	pool.nodes[0].c.PurgeAll()
	contacts := observeReaderOwnerContacts(pool)
	reader := newNonMemberReader(t, pool)
	value, _, tier := reader.query(t, key)
	if !bytes.Equal(value, capacityValue(key)) || tier != "l3_peer" || reader.backend.Load() != 0 || reader.peer.BackupHits.Load() != 1 {
		t.Fatalf("backup did not satisfy owner miss: tier%s backend%d backupHits%d", tier, reader.backend.Load(), reader.peer.BackupHits.Load())
	}
	contacts.assertAfter(t, key, 0, 0, 1)
	assertNonMemberReaderStorage(t, reader)
}

func TestPeerReaderNonMemberAllMissContactsOnlyRFReplicas(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			pool := newCapacityFleet(t, rf, 64, capacityFleetBytes)
			var key string
			if rf == 1 {
				key = pool.keyWith(0)
			} else {
				key = pool.keyWith(0, 1)
			}
			contacts := observeReaderOwnerContacts(pool)
			reader := newNonMemberReader(t, pool)
			value, _, tier := reader.query(t, key)
			if !bytes.Equal(value, capacityValue(key)) || tier != "backend" || reader.backend.Load() != 1 {
				t.Fatalf("all misses did not compute once: tier%s backend%d", tier, reader.backend.Load())
			}
			contacts.assertAfter(t, key, 0, pool.replicaIdx(key)...)
			waitFor(t, "reader's RF publication", func() bool { return reader.peer.WTPushes.Load() == int64(rf) })
			pool.quiesce()
			pool.assertPlacement([]string{key})
			// The backend result was published to its RF designated owners;
			// the nonmember writer still has no persistent copy of that result.
			reader.disk.Flush()
			if entries, _ := reader.disk.Size(); entries != 0 || reader.disk.Writes.Load() != 0 {
				t.Fatal("nonmember miss created a reader SSD replica")
			}
		})
	}
}

func TestPeerReaderLocalSSDPrecedesPeerAndPromotesToRAM(t *testing.T) {
	for _, tc := range []struct{ rf, localNode int }{{1, 0}, {2, 0}, {2, 1}} {
		t.Run(fmt.Sprintf("rf=%d/local-node=%d", tc.rf, tc.localNode), func(t *testing.T) {
			pool := newCapacityFleet(t, tc.rf, 64, capacityFleetBytes)
			var key string
			if tc.rf == 1 {
				key = pool.keyWith(0)
			} else {
				key = pool.keyWith(0, 1)
			}
			pool.query(0, key)
			pool.quiesce()
			pool.restartBounded(tc.localNode)
			local := pool.nodes[tc.localNode]
			if entries, _ := local.c.Size(); entries != 0 {
				t.Fatal("restart did not leave RAM cold")
			}
			contacts := observeReaderOwnerContacts(pool)
			diskReads, diskWrites := local.dc.Hits.Load(), local.dc.Writes.Load()
			value, ttl, tier, hit := local.c.GetSharedWithTTL(key)
			if !hit || tier != "l2_disk" || !bytes.Equal(value, capacityValue(key)) || ttl <= 0 {
				t.Fatalf("cold RAM did not use local SSD: tier%s hit%v ttl%s value%q", tier, hit, ttl, value)
			}
			if reads := local.dc.Hits.Load() - diskReads; reads != 1 {
				t.Fatalf("initial local SSD reads%d, want1", reads)
			}
			contacts.assertAfter(t, key, 0)
			for repeat := 0; repeat < 3; repeat++ {
				value, _, tier, hit = local.c.GetSharedWithTTL(key)
				if !hit || tier != "l1_memory" || !bytes.Equal(value, capacityValue(key)) {
					t.Fatalf("promoted RAM lookup: tier%s hit%v value%q", tier, hit, value)
				}
				if reads := local.dc.Hits.Load() - diskReads; reads != 1 {
					t.Fatalf("RAM repeat reread SSD: reads%d", reads)
				}
				contacts.assertAfter(t, key, 0)
			}
			if value, _, hit := local.c.GetWithTTL(key); !hit || !bytes.Equal(value, capacityValue(key)) {
				t.Fatal("promoted value not available through RAM TTL lookup")
			}
			if local.dc.Writes.Load() != diskWrites || local.c.Stats().L3.Requests != 0 {
				t.Fatalf("local hits wrote SSD or consulted peers: writes%d peer requests%d", local.dc.Writes.Load()-diskWrites, local.c.Stats().L3.Requests)
			}
			contacts.assertAfter(t, key, 0)
		})
	}
}
