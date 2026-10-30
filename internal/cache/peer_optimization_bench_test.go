package cache

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// BenchmarkPeerCacheFetchPayload measures a peer network lookup with an owner L1
// hit and no requester shadow copy. Both HTTP sides run in this process, so the
// reported allocations include client and server. No disk/cache flush runs in
// this fixture. The connection and codecs are warmed before measurement.
func BenchmarkPeerCacheFetchPayload(b *testing.B) {
	for _, size := range []int{128, 4096, 256 << 10} {
		for _, random := range []bool{false, true} {
			entropy := "repetitive"
			if random {
				entropy = "incompressible"
			}
			b.Run(fmt.Sprintf("bytes=%d/%s", size, entropy), func(b *testing.B) {
				payload := bytes.Repeat([]byte("x"), size)
				if random {
					_, _ = rand.New(rand.NewSource(42)).Read(payload)
				}
				local := NewWithMaxBytes(time.Hour, 100, 64<<20)
				defer local.Close()
				owner := NewPeerCache(PeerConfig{})
				defer owner.Close()
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { owner.ServeHTTP(w, r, local) }))
				defer srv.Close()
				addr := strings.TrimPrefix(srv.URL, "http://")
				peer := NewPeerCache(PeerConfig{SelfAddr: "requester:3100", DiscoveryType: "static", StaticPeers: addr, Timeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
				defer peer.Close()
				defer peer.client.CloseIdleConnections()
				local.SetLocalOnlyWithTTL("payload", payload, time.Hour)
				got, _, ok := peer.Get("payload")
				if !ok || !bytes.Equal(got, payload) {
					b.Fatal("warmup peer response mismatch")
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				b.ResetTimer()
				for b.Loop() {
					got, _, ok := peer.Get("payload")
					if !ok || len(got) != len(payload) {
						b.Fatal("peer response mismatch")
					}
				}
			})
		}
	}
}
