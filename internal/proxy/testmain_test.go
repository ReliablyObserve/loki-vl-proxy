package proxy

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

// TestMain closes the test binary's outbound connections with an RST instead
// of a FIN handshake. Thousands of tests build a proxy and a fake backend, send
// a few requests and shut the proxy down; each connection the proxy closes
// first leaves a socket in TIME_WAIT for the next 30 seconds, and a full run
// piles up more than the ~16k ephemeral ports of the host, so later dials fail
// with "can't assign requested address". A connection closed with SO_LINGER 0
// skips TIME_WAIT and frees its port at once.
//
// The proxy builds its backend transport by cloning http.DefaultTransport, so
// setting the dialer there covers every proxy under test, and the default
// client used by tests that call a server directly.
func TestMain(m *testing.M) {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	http.DefaultTransport.(*http.Transport).DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		return conn, err
	}
	os.Exit(m.Run())
}
