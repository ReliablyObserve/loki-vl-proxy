package cache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPeerPublicationEligibilityHasNoSideEffects(t *testing.T) {
	tooLarge := make([]byte, maxPeerSetBodyBytes+1)
	for _, writeThrough := range []bool{false, true} {
		peer := NewPeerCache(PeerConfig{WriteThrough: writeThrough, WriteThroughMinTTL: 30 * time.Second})
		peer.SetPushFilter(func(key string) bool { return !strings.Contains(key, ":auth:user.") })
		for _, tc := range []struct {
			name, key   string
			value       []byte
			ttl         time.Duration
			force, want bool
		}{
			{"normal", "query:tenant:a", []byte("value"), time.Minute, false, writeThrough},
			{"unknownTTL", "query:tenant:a", []byte("value"), 0, false, writeThrough},
			{"negativeTTL", "query:tenant:a", []byte("value"), -time.Second, false, writeThrough},
			{"shortTTL", "query:tenant:a", []byte("value"), time.Second, false, false},
			{"minimumTTL", "query:tenant:a", []byte("value"), 30 * time.Second, false, writeThrough},
			{"forcedShortTTL", "query:tenant:a", []byte("value"), time.Second, true, true},
			{"filtered", "query:tenant:a:auth:user.secret", []byte("value"), time.Minute, false, false},
			{"forcedFiltered", "query:tenant:a:auth:user.secret", []byte("value"), time.Minute, true, false},
			{"oversize", "query:tenant:a", tooLarge, time.Minute, false, false},
			{"forcedOversize", "query:tenant:a", tooLarge, time.Minute, true, false},
			{"maximumSize", "query:tenant:a", tooLarge[:maxPeerSetBodyBytes], time.Minute, true, true},
			{"empty", "query:tenant:a", nil, time.Minute, true, false},
		} {
			t.Run(fmt.Sprintf("writeThrough=%v/%s", writeThrough, tc.name), func(t *testing.T) {
				if got := peer.publicationEligible(tc.key, tc.value, tc.ttl, tc.force); got != tc.want {
					t.Fatalf("eligibility=%v want=%v", got, tc.want)
				}
				if peer.PushTooLarge.Load() != 0 || peer.WTPushes.Load() != 0 || peer.WTErrors.Load() != 0 {
					t.Fatal("eligibility check updated publication counters")
				}
			})
		}
		peer.Close()
	}
	var peer *PeerCache
	if peer.publicationEligible("key", []byte("value"), time.Minute, true) {
		t.Fatal("nil peer publication eligible")
	}
}

func TestPeerPublicationStillCountsRejectedOversize(t *testing.T) {
	peer := NewPeerCache(PeerConfig{WriteThrough: true, WriteThroughMinTTL: time.Second})
	defer peer.Close()
	value := make([]byte, maxPeerSetBodyBytes+1)
	if peer.publicationEligible("key", value, time.Minute, true) {
		t.Fatal("oversize eligible")
	}
	if peer.PushTooLarge.Load() != 0 {
		t.Fatal("eligibility changed counter")
	}
	peer.Replicate("key", value, time.Minute)
	if peer.PushTooLarge.Load() != 1 {
		t.Fatal("actual rejected publication did not update counter")
	}
}

func TestReadAheadPreservesMemoryOnlyPlacement(t *testing.T) {
	for _, tc := range []struct{ rf, requester int }{{1, 1}, {2, 1}, {2, 2}} {
		for _, writeThrough := range []bool{false, true} {
			t.Run(fmt.Sprintf("rf=%d/requester=%d/writeThrough=%v", tc.rf, tc.requester, writeThrough), func(t *testing.T) {
				fleet := newReplFleet(t, 3, tc.rf, writeThrough, time.Second)
				var key string
				if tc.rf == 1 {
					key = fleet.keyWith(0)
				} else {
					key = fleet.keyWith(0, 1)
				}
				fleet.nodes[0].c.SetLocalOnlyWithTTL(key, []byte("memory-only parsed window"), time.Minute)
				node := fleet.nodes[tc.requester]
				if !node.pc.prefetchHotKey(node.c, hotIndexEntry{Key: key, RemainingTTLMS: 60000}) {
					t.Fatal("read-ahead did not fetch owner value")
				}
				value, ttl, memOnly, ok := node.c.getForPeer(key)
				if !ok || string(value) != "memory-only parsed window" || ttl <= 0 || ttl > time.Minute || !memOnly {
					t.Fatalf("read-ahead lost metadata: value=%q ttl=%s memOnly=%v ok=%v", value, ttl, memOnly, ok)
				}
				for i, replica := range fleet.nodes {
					if onDisk(replica, key) {
						t.Fatalf("memory-only value persisted at node%d", i)
					}
				}
				if pushes, failed := fleet.pushes(); pushes != 0 || failed != 0 {
					t.Fatalf("read-ahead published value: pushes=%d failed=%d", pushes, failed)
				}
			})
		}
	}
}

func TestReadAheadRejectsUnknownTTL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("unknown owner expiry")) }))
	defer server.Close()
	peer := NewPeerCache(PeerConfig{SelfAddr: "self:3100", DiscoveryType: "static", StaticPeers: strings.TrimPrefix(server.URL, "http://"), ReadAheadMinTTL: time.Second})
	defer peer.Close()
	local := New(time.Hour, 10)
	defer local.Close()
	if peer.prefetchHotKey(local, hotIndexEntry{Key: "unknown", RemainingTTLMS: 60000}) {
		t.Fatal("read-ahead extended unknown owner TTL from older hot index")
	}
	if _, _, ok := local.GetWithTTL("unknown"); ok {
		t.Fatal("unknown TTL value admitted")
	}
}

func TestReadAheadAccountsForFetchTime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Cache-Ttl-Ms", "60000")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(40 * time.Millisecond)
		_, _ = w.Write([]byte("owner expiry"))
	}))
	defer server.Close()
	peer := NewPeerCache(PeerConfig{SelfAddr: "self:3100", DiscoveryType: "static", StaticPeers: strings.TrimPrefix(server.URL, "http://"), ReadAheadMinTTL: time.Second})
	defer peer.Close()
	local := New(time.Hour, 10)
	defer local.Close()
	if !peer.prefetchHotKey(local, hotIndexEntry{Key: "delayed", RemainingTTLMS: 60000}) {
		t.Fatal("read-ahead failed")
	}
	if _, ttl, ok := local.GetWithTTL("delayed"); !ok || ttl > 59960*time.Millisecond {
		t.Fatalf("fetch time extended expiry: ttl=%s hit=%v", ttl, ok)
	}
}

func TestPeerPublicationKeepsConfiguredReplicaCount(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			fleet := newReplFleet(t, 3, rf, true, time.Second)
			var key string
			if rf == 1 {
				key = fleet.keyWith(0)
			} else {
				key = fleet.keyWith(0, 1)
			}
			fleet.nodes[0].c.SetWithTTL(key, []byte("exact durable replica count"), time.Minute)
			waitFor(t, "replica publication", func() bool { pushes, _ := fleet.pushes(); return pushes == int64(rf-1) })
			copies := 0
			for _, node := range fleet.nodes {
				if onDisk(node, key) {
					copies++
				}
			}
			if copies != rf {
				t.Fatalf("disk copies=%d want configuredRF%d", copies, rf)
			}
		})
	}
}
