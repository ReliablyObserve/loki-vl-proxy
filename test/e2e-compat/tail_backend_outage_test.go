//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// outageForwarder forwards to the stack's VictoriaLogs and, on demand, takes
// it away the way a restart does: every open /select/logsql/ stream is cut and
// every new call is refused with a reset connection. Nothing on the stack is
// stopped, so the stack's other tests and the lines a test pushes straight to
// VictoriaLogs are unaffected.
type outageForwarder struct {
	url     string
	down    atomic.Bool
	mu      sync.Mutex
	streams map[int]context.CancelFunc
	next    int
}

func newOutageForwarder(t *testing.T) *outageForwarder {
	t.Helper()
	target, err := url.Parse(vlURL)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.FlushInterval = -1 // a tail is a stream: forward each write at once
	f := &outageForwarder{streams: map[int]context.CancelFunc{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/select/logsql/") {
			forward.ServeHTTP(w, r)
			return
		}
		if f.down.Load() {
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					if tc, ok := conn.(*net.TCPConn); ok {
						_ = tc.SetLinger(0)
					}
					_ = conn.Close()
				}
			}
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		f.mu.Lock()
		id := f.next
		f.next++
		f.streams[id] = cancel
		f.mu.Unlock()
		defer func() {
			f.mu.Lock()
			delete(f.streams, id)
			f.mu.Unlock()
			cancel()
		}()
		forward.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(server.Close)
	f.url = server.URL
	return f
}

// outage starts the restart: the open streams are cut and new calls refused.
func (f *outageForwarder) outage() {
	f.down.Store(true)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, cancel := range f.streams {
		cancel()
	}
}

// recover ends it: VictoriaLogs answers again.
func (f *outageForwarder) recover() { f.down.Store(false) }

// tailLines collects the lines a tail client receives, and how its connection ended.
type tailLines struct {
	mu       sync.Mutex
	seen     map[string]int
	closeErr *websocket.CloseError
	ended    bool
}

func collectTail(conn *websocket.Conn) *tailLines {
	c := &tailLines{seen: map[string]int{}}
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				c.mu.Lock()
				c.ended = true
				if ce, ok := err.(*websocket.CloseError); ok {
					c.closeErr = ce
				}
				c.mu.Unlock()
				return
			}
			var frame struct {
				Streams []struct {
					Values [][]interface{} `json:"values"`
				} `json:"streams"`
			}
			if json.Unmarshal(msg, &frame) != nil {
				continue
			}
			c.mu.Lock()
			for _, s := range frame.Streams {
				for _, v := range s.Values {
					if len(v) > 1 {
						if line, ok := v[1].(string); ok {
							c.seen[line]++
						}
					}
				}
			}
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *tailLines) waitFor(t *testing.T, d time.Duration, lines ...string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		c.mu.Lock()
		missing := ""
		for _, l := range lines {
			if c.seen[l] == 0 {
				missing = l
				break
			}
		}
		ended, closeErr := c.ended, c.closeErr
		c.mu.Unlock()
		if missing == "" {
			return
		}
		if ended {
			t.Fatalf("the tail ended before %q arrived: close frame %v", missing, closeErr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("line %q did not arrive within %v", missing, d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFeature_Tail_SurvivesBackendOutage tails through the proxy while the
// VictoriaLogs behind it is taken away and brought back (a fault-injecting
// forwarder in front of the stack's VictoriaLogs, as TestCompat_ErrorLayerLikeLoki
// uses; no container is restarted). The websocket must stay open, and the
// lines pushed before, during and after the outage must each arrive exactly
// once, for the native tail and for the polling tail.
// conformance: semantics/tail-survives-backend-outage, loki_api_v1_tail
func TestFeature_Tail_SurvivesBackendOutage(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode proxy.TailMode
	}{{"native", proxy.TailModeNative}, {"polling", proxy.TailModeSynthetic}} {
		t.Run(tc.name, func(t *testing.T) {
			app := "tail-outage-" + tc.name + "-" + strings.ReplaceAll(time.Now().Format("150405.000000"), ".", "")
			fwd := newOutageForwarder(t)
			route := newErrLayerProxy(t, fwd.url, func(cfg *proxy.Config) {
				cfg.TailMode = tc.mode
				cfg.TailBackendRetryWindow = 30 * time.Second
			})
			params := url.Values{"query": {`{app="` + app + `"}`}}
			conn, resp, err := (&websocket.Dialer{HandshakeTimeout: 5 * time.Second}).Dial("ws"+strings.TrimPrefix(route, "http")+"/loki/api/v1/tail?"+params.Encode(), http.Header{"X-Scope-OrgID": {"0"}})
			if err != nil {
				t.Fatalf("tail dial: %v (resp %v)", err, resp)
			}
			defer conn.Close()
			lines := collectTail(conn)

			push := func(msg string) {
				pushCustomToVL(t, time.Now(), map[string]string{"app": app, "env": "test", "level": "info"}, []logLine{{Msg: msg, Level: "info"}}, []string{"app", "env", "level"})
			}
			before := "before-outage-" + app
			push(before)
			lines.waitFor(t, 20*time.Second, before)

			fwd.outage()
			during1, during2 := "during-outage-1-"+app, "during-outage-2-"+app
			push(during1)
			time.Sleep(1500 * time.Millisecond)
			push(during2)
			time.Sleep(2500 * time.Millisecond) // several retries, none answered
			lines.mu.Lock()
			ended, closeErr := lines.ended, lines.closeErr
			lines.mu.Unlock()
			if ended {
				t.Fatalf("the tail ended during an outage shorter than -tail.backend-retry-window: close frame %v", closeErr)
			}
			fwd.recover()

			after := "after-outage-" + app
			lines.waitFor(t, 30*time.Second, during1, during2)
			push(after)
			lines.waitFor(t, 20*time.Second, after)
			time.Sleep(2500 * time.Millisecond) // a repeated line would arrive in this time

			lines.mu.Lock()
			defer lines.mu.Unlock()
			for _, l := range []string{before, during1, during2, after} {
				if lines.seen[l] != 1 {
					t.Errorf("line %q delivered %d times, want once (all: %v)", l, lines.seen[l], lines.seen)
				}
			}
			if lines.ended {
				t.Errorf("the tail ended after the backend came back: close frame %v", lines.closeErr)
			}
		})
	}
}
