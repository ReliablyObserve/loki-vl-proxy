package cache

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Close leaves no connection of the peer cache open: idle ones are closed, and
// a dial after Close is refused, so no connection is kept for later calls.
//
// conformance: shutdown-releases-background-work
func TestPeerCache_CloseLeavesNoConnection(t *testing.T) {
	var open atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			open.Add(1)
		case http.StateClosed, http.StateHijacked:
			open.Add(-1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	pc := NewPeerCache(PeerConfig{SelfAddr: "127.0.0.1:1", DiscoveryType: "static", StaticPeers: "127.0.0.1:1," + addr, Timeout: 2 * time.Second})
	t.Cleanup(pc.Close)
	key := ""
	for i := 0; i < 1000 && key == ""; i++ {
		if k := "close-conn-" + string(rune('a'+i%26)) + string(rune('a'+i/26)); pc.replicasOf(k)[0] == addr {
			key = k
		}
	}
	if key == "" {
		t.Fatal("no key owned by the peer")
	}
	if _, _, ok := pc.Get(key); ok {
		t.Fatal("a miss answered")
	}
	waitFor(t, "an idle connection to the peer", func() bool { return open.Load() == 1 })
	pc.Close()
	waitFor(t, "the idle connection to close", func() bool { return open.Load() == 0 })
	if _, err := pc.dialPeer(context.Background(), "tcp", addr); err == nil {
		t.Fatal("a closed peer cache dialled")
	}
}
