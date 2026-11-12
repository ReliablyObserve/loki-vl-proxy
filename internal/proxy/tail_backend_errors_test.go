package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// tailOutcome is what a tail client received: the HTTP answer of a refused
// upgrade, or the frames and the close frame of an upgraded connection.
type tailOutcome struct {
	httpStatus int // 0: the upgrade succeeded
	httpBody   string
	frames     int
	closeCode  int // 0: no close frame (the client read timed out or the TCP stream just ended)
	closeText  string
}

// tailErrProxy starts a proxy against a fake VictoriaLogs that fails every
// call in the given mode, and returns the websocket URL of its tail route.
func tailErrProxy(t *testing.T, mode string, tailMode TailMode) (string, *errLayerBackend) {
	t.Helper()
	backend := &errLayerBackend{mode: mode}
	srv := httptest.NewServer(backend)
	if mode == "conn_refused" {
		srv.Close()
	} else {
		t.Cleanup(srv.Close)
	}
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	cfg := Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second, TailMode: tailMode}
	if mode == "timeout" {
		cfg.BackendTimeout = 250 * time.Millisecond
	}
	if mode == "oversize" {
		cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = 4096
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	front := httptest.NewServer(mux)
	t.Cleanup(front.Close)
	return "ws" + strings.TrimPrefix(front.URL, "http") + `/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D`, backend
}

// readTail dials the tail and reads until the connection ends.
func readTail(t *testing.T, wsURL string, readFor time.Duration) tailOutcome {
	t.Helper()
	dialer := websocket.Dialer{HandshakeTimeout: 8 * time.Second}
	conn, resp, err := dialer.Dial(wsURL, http.Header{"X-Scope-OrgID": {"0"}})
	if err != nil {
		if resp == nil {
			t.Fatalf("tail dial: %v", err)
		}
		defer resp.Body.Close()
		var b strings.Builder
		buf := make([]byte, 1024)
		for {
			n, rerr := resp.Body.Read(buf)
			b.Write(buf[:n])
			if rerr != nil {
				break
			}
		}
		return tailOutcome{httpStatus: resp.StatusCode, httpBody: b.String()}
	}
	defer conn.Close()
	out := tailOutcome{}
	_ = conn.SetReadDeadline(time.Now().Add(readFor))
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			var ce *websocket.CloseError
			if errors.As(err, &ce) && ce.Code != websocket.CloseAbnormalClosure {
				out.closeCode, out.closeText = ce.Code, ce.Text
			}
			return out
		}
		out.frames++
	}
}

// TestTail_BackendFailureModes drives the tail route, native and polling,
// through each of the 14 backend failure modes and asserts what the client
// receives: an HTTP error before the upgrade, or frames and then Loki's close
// (1011 with the error layer's text), never a bare disconnect, a normal close,
// or Go reader wording. A polling tail stops calling VictoriaLogs once closed.
// conformance: semantics/error-layer-internal-go-text, semantics/error-layer-cancel-deadline, semantics/error-layer-silent-success, loki_api_v1_tail
func TestTail_BackendFailureModes(t *testing.T) {
	const (
		internal = lokiErrInternal
		parse    = lokiErrParse
	)
	type want struct {
		httpStatus int    // the upgrade is refused with this status
		text       string // refused body, or the close frame text
		closeCode  int    // 1011 after the upgrade
		minFrames  int
		open       bool // the tail delivers frames and is still open when the client stops reading
	}
	closed := func(text string, minFrames int) want {
		return want{closeCode: websocket.CloseInternalServerErr, text: text, minFrames: minFrames}
	}
	refused := func(status int, text string) want { return want{httpStatus: status, text: text} }
	// The native and the polling tail share the probe before the upgrade, so
	// every mode the probe sees fails alike; the modes only the stream (or the
	// poll) meets differ.
	cases := []struct {
		mode            string
		native, polling want
	}{
		{"vl400_parse", refused(400, parse), refused(400, parse)},
		{"vl400_limit", refused(500, internal), refused(500, internal)},
		{"vl422", refused(500, internal), refused(500, internal)},
		{"vl500", refused(500, internal), refused(500, internal)},
		{"vl502_front", refused(500, internal), refused(500, internal)},
		{"vl503_front", refused(500, internal), refused(500, internal)},
		{"vl429", refused(429, lokiQueueFull), refused(429, lokiQueueFull)},
		{"conn_refused", closed(internal, 0), closed(internal, 0)},
		{"conn_reset", closed(internal, 0), closed(internal, 0)},
		{"timeout", refused(504, lokiDeadlineExceeded), refused(504, lokiDeadlineExceeded)},
		{"truncated", closed(internal, 0), closed(internal, 0)},
		{"malformed", closed(internal, 0), closed(internal, 0)},
		// A polled body is read as a stream with no byte cap, so its rows are
		// delivered and the tail stays open; the native stream ends after its
		// rows, which is a failed call.
		{"oversize", closed(internal, 1), want{open: true, minFrames: 1}},
	}
	for _, tc := range cases {
		for _, m := range []struct {
			name string
			mode TailMode
			want want
		}{{"native", TailModeNative, tc.native}, {"polling", TailModeSynthetic, tc.polling}} {
			t.Run(m.name+"/"+tc.mode, func(t *testing.T) {
				t.Parallel()
				wsURL, backend := tailErrProxy(t, tc.mode, m.mode)
				readFor := 8 * time.Second
				if m.want.open {
					readFor = 3 * time.Second
				}
				got := readTail(t, wsURL, readFor)
				t.Logf("got %+v", got)
				w := m.want
				if w.httpStatus != 0 {
					if got.httpStatus != w.httpStatus || strings.TrimSpace(got.httpBody) != w.text {
						t.Fatalf("want HTTP %d %q before the upgrade, got %d %q (close %d %q)", w.httpStatus, w.text, got.httpStatus, got.httpBody, got.closeCode, got.closeText)
					}
					return
				}
				if got.httpStatus != 0 {
					t.Fatalf("want an upgraded tail closed with %d, got HTTP %d %q", w.closeCode, got.httpStatus, got.httpBody)
				}
				if w.open {
					if got.closeCode != 0 || got.frames < w.minFrames {
						t.Fatalf("want an open tail with %d frames or more, got close %d %q after %d frames", w.minFrames, got.closeCode, got.closeText, got.frames)
					}
					return
				}
				if got.closeCode != w.closeCode || got.closeText != w.text {
					t.Fatalf("want close %d %q, got %d %q after %d frames", w.closeCode, w.text, got.closeCode, got.closeText, got.frames)
				}
				if got.frames < w.minFrames {
					t.Fatalf("want at least %d frames before the close, got %d", w.minFrames, got.frames)
				}
				if m.mode == TailModeSynthetic {
					// The poll loop ended with the close: VictoriaLogs is not called again.
					hits := backend.hits.Load()
					time.Sleep(2500 * time.Millisecond)
					if after := backend.hits.Load(); after != hits {
						t.Fatalf("polling continued after the close: %d calls, then %d", hits, after)
					}
				}
			})
		}
	}
}

// tailLongLineBackend answers the probe and the poll with one row above the
// tail scanner's 1 MiB line cap, and the native tail stream with the same row
// after a valid one.
func tailLongLineBackend(t *testing.T) *httptest.Server {
	t.Helper()
	long := fmt.Sprintf(`{"_time":"%s","_stream":"{app=\"api\"}","app":"api","_msg":"%s"}`+"\n",
		time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano), strings.Repeat("x", 1100*1024))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/select/logsql/tail"):
			fmt.Fprint(w, errLayerShapedBody(r.URL.Path, 3))
			w.(http.Flusher).Flush()
			fmt.Fprint(w, long)
			<-r.Context().Done()
		case r.URL.Query().Get("limit") == "1":
			w.WriteHeader(http.StatusNoContent) // the probe
		default:
			fmt.Fprint(w, long)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A VictoriaLogs line above the scanner's cap ends the tail with Loki's 1011,
// native and polling alike, instead of dropping the connection.
// conformance: semantics/error-layer-internal-go-text, loki_api_v1_tail
func TestTail_OversizeLineClosesWithLokiText(t *testing.T) {
	for _, mode := range []TailMode{TailModeNative, TailModeSynthetic} {
		t.Run(string(mode), func(t *testing.T) {
			srv := tailLongLineBackend(t)
			c := cache.New(time.Minute, 100)
			t.Cleanup(c.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", TailMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			front := httptest.NewServer(mux)
			t.Cleanup(front.Close)
			got := readTail(t, "ws"+strings.TrimPrefix(front.URL, "http")+`/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D`, 8*time.Second)
			if got.closeCode != websocket.CloseInternalServerErr || got.closeText != lokiErrInternal {
				t.Fatalf("want close 1011 %q, got HTTP %d, close %d %q", lokiErrInternal, got.httpStatus, got.closeCode, got.closeText)
			}
		})
	}
}

// A client that disconnects while VictoriaLogs is still answering gets nothing
// (it is gone); the proxy drops its VictoriaLogs call, native stream and poll
// alike, and the tail handler returns.
// conformance: semantics/error-layer-cancel-deadline, backend-deadlines-and-cancellation, loki_api_v1_tail
func TestTail_ClientDisconnectDropsBackendCall(t *testing.T) {
	for _, mode := range []TailMode{TailModeNative, TailModeSynthetic} {
		t.Run(string(mode), func(t *testing.T) {
			// The query names this test's stream, so a call from another test's
			// proxy that reached a reused port never counts.
			label := fmt.Sprintf("tc-%s-%d", mode, time.Now().UnixNano())
			ours := func(r *http.Request) bool { return strings.Contains(r.URL.Query().Get("query"), label) }
			hit := make(chan struct{})
			released := make(chan struct{})
			var hitOnce, releaseOnce sync.Once
			var probed atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !ours(r) {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if r.URL.Query().Get("limit") == "1" && probed.CompareAndSwap(false, true) {
					w.WriteHeader(http.StatusNoContent) // the probe before the upgrade
					return
				}
				// The native tail call, or the first poll: it never answers.
				hitOnce.Do(func() { close(hit) })
				<-r.Context().Done()
				releaseOnce.Do(func() { close(released) })
			}))
			t.Cleanup(srv.Close)
			c := cache.New(time.Minute, 100)
			t.Cleanup(c.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", TailMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			var inflight atomic.Int64
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				inflight.Add(1)
				defer inflight.Add(-1)
				mux.ServeHTTP(w, r)
			}))
			t.Cleanup(front.Close)
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(front.URL, "http")+`/loki/api/v1/tail?query=`+url.QueryEscape(`{app="`+label+`"}`), http.Header{"X-Scope-OrgID": {"0"}})
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			select {
			case <-hit:
			case <-time.After(5 * time.Second):
				t.Fatal("the proxy never called VictoriaLogs")
			}
			if inflight.Load() != 1 {
				t.Fatalf("tail handlers running while the client is connected: %d, want 1", inflight.Load())
			}
			_ = conn.Close()
			select {
			case <-released:
			case <-time.After(3 * time.Second):
				t.Fatal("the proxy kept its VictoriaLogs call after the client left")
			}
			deadline := time.Now().Add(3 * time.Second)
			for inflight.Load() > 0 && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
			}
			if n := inflight.Load(); n > 0 {
				t.Fatalf("%d tail handlers still running after the client left", n)
			}
		})
	}
}
