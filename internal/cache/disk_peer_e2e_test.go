package cache

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercise persisted records through the real HTTP peer protocol after losing
// all owner memory. Changing the configured codec must not reinterpret old data.
func TestDiskPeerRestartCodecCompatibility(t *testing.T) {
	for _, codec := range []string{"raw", "gzip", "s2", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache.db")
			cfg := DiskCacheConfig{Path: path, Compression: true, Codec: codec, FlushInterval: time.Hour}
			dc, err := NewDiskCache(cfg)
			if err != nil {
				t.Fatal(err)
			}
			key := "query_range:tenant=A:auth=caller1:+/?&=雪"
			other := "query_range:tenant=B:auth=caller2:+/?&=雪"
			value := bytes.Repeat([]byte(`{"status":"success","data":{"result":[]}}`), 200)
			owner := NewWithMaxBytes(time.Hour, 100, 8<<20)
			owner.SetL2(dc)
			owner.SetLocalAndDiskWithTTL(key, value, time.Hour)
			owner.SetLocalAndDiskWithTTL(other, []byte("other tenant"), time.Hour)
			owner.Close()
			if err := dc.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.Codec = "raw"
			cfg.Compression = false
			dc, err = NewDiskCache(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer dc.Close()
			owner = NewWithMaxBytes(time.Hour, 100, 8<<20)
			defer owner.Close()
			owner.SetL2(dc)
			serverPeer := NewPeerCache(PeerConfig{})
			defer serverPeer.Close()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serverPeer.ServeHTTP(w, r, owner) }))
			defer srv.Close()
			requester := NewPeerCache(PeerConfig{SelfAddr: "requester:3100", DiscoveryType: "static", StaticPeers: strings.TrimPrefix(srv.URL, "http://"), Timeout: time.Second})
			defer requester.Close()
			for _, item := range []struct {
				key   string
				value []byte
			}{{key, value}, {other, []byte("other tenant")}} {
				got, ttl, ok := requester.Get(item.key)
				if !ok || !bytes.Equal(got, item.value) || ttl <= 0 || ttl > time.Hour {
					t.Fatalf("peer after restart: hit=%v ttl=%s equal=%v", ok, ttl, bytes.Equal(got, item.value))
				}
			}
			if dc.Hits.Load() < 2 {
				t.Fatalf("expected real L2 reads, hits=%d", dc.Hits.Load())
			}
			if _, _, ok := requester.Get(strings.Replace(key, "auth=caller1", "auth=caller2", 1)); ok {
				t.Fatal("credential-distinct key unexpectedly reused")
			}
			owner.PurgeAll()
			// Peer disk hits promote into owner memory; purge both local tiers.
			if _, _, ok := requester.Get(key); ok {
				t.Fatal("purged disk value reappeared through peer")
			}
		})
	}
}
