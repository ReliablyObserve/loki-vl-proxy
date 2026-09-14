package proxy

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestNew_RespectsConfiguredLogLevelOverDefaultLogger(t *testing.T) {
	var buf lockedLogBuffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(orig)

	p, err := New(Config{
		BackendURL: "http://example.com",
		Cache:      cache.New(60*time.Second, 10),
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if p.log.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("expected info level to be disabled")
	}

	p.log.Info("suppressed info log")
	// Other tests in this package run proxies whose background probes log
	// through slog.Default(), which this test temporarily points at buf. Only
	// this proxy's own message proves the configured level took precedence;
	// asserting an empty buffer races with those goroutines and flakes.
	if strings.Contains(buf.String(), "suppressed info log") {
		t.Fatalf("expected info log to be suppressed, got %q", buf.String())
	}
}

func TestDeriveRequestType_UsesKnownUpstreamRouteMapping(t *testing.T) {
	if got := deriveRequestType("", "/select/logsql/query"); got != "select_logsql_query" {
		t.Fatalf("deriveRequestType() = %q, want %q", got, "select_logsql_query")
	}
	if got := deriveRequestType("query_range", "/select/logsql/stats_query_range"); got != "query_range" {
		t.Fatalf("deriveRequestType() should prefer explicit endpoint, got %q", got)
	}
}
