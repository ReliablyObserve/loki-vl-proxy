package querycorpus_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"
)

// The fake VictoriaLogs of the replay harnesses is served in memory. Every replayed request builds a fresh proxy in
// front of a fresh fake, and over TCP each pair cost a listening port and at least one client connection in
// TIME_WAIT: a few thousand runs of a differential (and the next test's, within the same half minute) used up the
// ephemeral ports of the machine (macOS holds one for 30 s), and the proxy answered 500 "can't assign requested
// address" for the rows that came after, in whatever test ran then. Nothing reaches the network now: the proxy's
// transport is a clone of http.DefaultTransport, whose dialer hands a fake's address to its in-memory listener.

// frontDialRegistry maps the loopback address of each running fake to its listener.
var frontDialRegistry = struct {
	sync.Mutex
	byAddr map[string]*memListener
}{byAddr: map[string]*memListener{}}

// realFrontDials counts the dials of the process that were not for a fake: the network is used only by them.
var realFrontDials atomic.Int64

// frontAddrCounter numbers the fakes: an address is used once in the process, so a request that a stopped proxy
// sends late finds its own fake closed and never the next run's (over TCP it was refused the same way).
var frontAddrCounter atomic.Uint32

// isFrontAddr is true for the addresses of the fakes: loopback ones from 127.64.0.0/10 that nothing binds. A proxy
// from the environment is skipped for them, and a dial of one that is not (or no longer) registered fails at once.
func isFrontAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host).To4()
	return ip != nil && ip[0] == 127 && ip[1]&0xC0 == 0x40
}

func init() {
	// Before any test starts: proxy.New clones this transport, so every proxy built later dials through it.
	tr := http.DefaultTransport.(*http.Transport)
	next := tr.DialContext
	if next == nil {
		next = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		frontDialRegistry.Lock()
		ln := frontDialRegistry.byAddr[addr]
		frontDialRegistry.Unlock()
		if ln != nil {
			return ln.dial()
		}
		if isFrontAddr(addr) {
			return nil, errors.New("memory front closed")
		}
		realFrontDials.Add(1)
		return next(ctx, network, addr)
	}
}

// memListener is a net.Listener whose connections are net.Pipe pairs.
type memListener struct {
	addr   memAddr
	conns  chan net.Conn
	done   chan struct{}
	closed sync.Once
}

type memAddr string

func (a memAddr) Network() string { return "tcp" }
func (a memAddr) String() string  { return string(a) }

func (l *memListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *memListener) Close() error {
	l.closed.Do(func() {
		close(l.done)
		frontDialRegistry.Lock()
		delete(frontDialRegistry.byAddr, string(l.addr))
		frontDialRegistry.Unlock()
	})
	return nil
}

func (l *memListener) Addr() net.Addr { return l.addr }

func (l *memListener) dial() (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.done:
		_ = client.Close()
		_ = server.Close()
		return nil, errors.New("memory front closed")
	}
}

// newMemoryFront starts h as a test server on an in-memory listener, at an address no other fake of the process has had.
func newMemoryFront(h http.Handler) *httptest.Server {
	n := frontAddrCounter.Add(1)
	addr := fmt.Sprintf("127.%d.%d.%d:80", 64+n>>16&0x3F, n>>8&0xFF, n&0xFF)
	ln := &memListener{addr: memAddr(addr), conns: make(chan net.Conn), done: make(chan struct{})}
	frontDialRegistry.Lock()
	frontDialRegistry.byAddr[addr] = ln
	frontDialRegistry.Unlock()
	srv := &httptest.Server{Listener: ln, Config: &http.Server{Handler: h}}
	srv.Start()
	return srv
}
