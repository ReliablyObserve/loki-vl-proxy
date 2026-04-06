package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/szibis/Loki-VL-proxy/internal/cache"
)

func TestTailHardening_RejectsBrowserOriginsByDefault(t *testing.T) {
	var backendCalls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"test log line","app":"nginx"}`)
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?query={app%3D%22nginx%22}"
	header := http.Header{}
	header.Set("Origin", "https://grafana.example.com")
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err == nil {
		t.Fatal("expected websocket dial to fail for untrusted origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for untrusted origin, got resp=%v err=%v", resp, err)
	}
	if backendCalls != 0 {
		t.Fatalf("expected origin rejection before backend call, got %d backend calls", backendCalls)
	}
}

func TestTailHardening_AllowsConfiguredOrigin(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"test log line","app":"nginx"}`)
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:         vlBackend.URL,
		Cache:              c,
		LogLevel:           "error",
		TailAllowedOrigins: []string{"https://grafana.example.com"},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?query={app%3D%22nginx%22}"
	header := http.Header{}
	header.Set("Origin", "https://grafana.example.com")
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer ws.Close()

	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("websocket read failed: %v", err)
	}

	var frame map[string]interface{}
	if err := json.Unmarshal(msg, &frame); err != nil {
		t.Fatalf("invalid JSON frame: %v", err)
	}
	if _, ok := frame["streams"]; !ok {
		t.Fatalf("expected Loki tail frame, got %v", frame)
	}
}

func TestTailHardening_BackendFailureReturnsHTTPStatusBeforeUpgrade(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend unauthorized", http.StatusUnauthorized)
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?query={app%3D%22nginx%22}"
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected websocket dial to fail on upstream auth failure")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected upstream 401 to be returned before upgrade, got resp=%v err=%v", resp, err)
	}
}

func TestTailHardening_UsesDedicatedStreamingClient(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	if p.tailClient == nil {
		t.Fatal("expected dedicated tail client to be configured")
	}
	if p.tailClient.Timeout != 0 {
		t.Fatalf("expected tail client to disable overall timeout, got %s", p.tailClient.Timeout)
	}
	if p.client.Timeout == 0 {
		t.Fatal("expected regular backend client to retain bounded timeout")
	}
}

func TestTailHardening_FallsBackWhenNativeTailUnavailable(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/tail":
			http.NotFound(w, r)
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"test log line","app":"nginx"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?query={app%3D%22nginx%22}"
	dialer := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	ws, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer ws.Close()
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))

	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("websocket read failed: %v", err)
	}

	var frame map[string]interface{}
	if err := json.Unmarshal(msg, &frame); err != nil {
		t.Fatalf("invalid JSON frame: %v", err)
	}
	if _, ok := frame["streams"]; !ok {
		t.Fatalf("expected Loki tail frame from synthetic fallback, got %v", frame)
	}
}

func TestTailHardening_UpgradeDoesNotWaitForNativeTailHeaders(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/tail":
			time.Sleep(3 * time.Second)
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(http.StatusOK)
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"test log line","app":"nginx"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?query={app%3D%22nginx%22}"
	dialer := websocket.Dialer{HandshakeTimeout: 1 * time.Second}
	ws, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer ws.Close()
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))

	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("websocket read failed: %v", err)
	}

	var frame map[string]interface{}
	if err := json.Unmarshal(msg, &frame); err != nil {
		t.Fatalf("invalid JSON frame: %v", err)
	}
	if _, ok := frame["streams"]; !ok {
		t.Fatalf("expected Loki tail frame from synthetic fallback, got %v", frame)
	}
}

func TestTailHardening_ForcedSyntheticModeSkipsNativeTail(t *testing.T) {
	var nativeTailCalls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/tail":
			nativeTailCalls++
			http.Error(w, "native tail should not be used in synthetic mode", http.StatusInternalServerError)
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"test log line","app":"nginx"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error", TailMode: TailModeSynthetic})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?query={app%3D%22nginx%22}"
	dialer := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	ws, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer ws.Close()
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))

	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("websocket read failed: %v", err)
	}
	if nativeTailCalls != 0 {
		t.Fatalf("expected synthetic mode to skip native tail calls, got %d", nativeTailCalls)
	}

	var frame map[string]interface{}
	if err := json.Unmarshal(msg, &frame); err != nil {
		t.Fatalf("invalid JSON frame: %v", err)
	}
	if _, ok := frame["streams"]; !ok {
		t.Fatalf("expected Loki tail frame from synthetic mode, got %v", frame)
	}
}

func TestTailHardening_NativeModeReturnsBackendFailureReason(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/tail":
			http.Error(w, "upstream tail forbidden", http.StatusForbidden)
		case "/select/logsql/query":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error", TailMode: TailModeNative})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?query={app%3D%22nginx%22}"
	dialer := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	ws, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v (resp=%v)", err, resp)
	}
	defer ws.Close()
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))

	_, _, err = ws.ReadMessage()
	if err == nil {
		t.Fatal("expected websocket close on native backend failure")
	}
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("expected websocket close error, got %v", err)
	}
	if closeErr.Code != websocket.CloseInternalServerErr {
		t.Fatalf("expected internal server close code, got %d", closeErr.Code)
	}
	if !strings.Contains(closeErr.Text, "upstream tail forbidden") {
		t.Fatalf("expected backend failure reason in close message, got %q", closeErr.Text)
	}
}
