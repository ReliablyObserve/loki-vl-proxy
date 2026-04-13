package proxy

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestNew_RespectsConfiguredLogLevelOverDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
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
	if buf.Len() != 0 {
		t.Fatalf("expected info log to be suppressed, got %q", buf.String())
	}
}
