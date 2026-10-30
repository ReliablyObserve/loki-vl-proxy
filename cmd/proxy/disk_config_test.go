package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

func TestDiskFlagsPassStorageBudgets(t *testing.T) {
	for _, tc := range []struct {
		name         string
		args         []string
		codec        string
		batch        int
		bytes, queue int64
		entries      int
		index        int64
	}{
		{"defaults", nil, "s2", 1000, 4 << 20, 16 << 20, 100000, 16 << 20},
		{"explicit", []string{"-disk-cache-codec=zstd", "-disk-cache-flush-size=25", "-disk-cache-flush-bytes=8192", "-disk-cache-max-buffer-bytes=32768", "-disk-cache-max-entries=40", "-disk-cache-max-index-bytes=16384"}, "zstd", 25, 8192, 32768, 40, 16384},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sentinel := errors.New("captured config")
			var got runtimeOptions
			args := append([]string{"-server.admin-auth-token=test-token"}, tc.args...)
			err := run(args, func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} }, func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
				got = opts
				return nil, sentinel
			}, nil, nil)
			if !errors.Is(err, sentinel) {
				t.Fatalf("run: %v", err)
			}
			cfg := got.diskCfg
			if cfg.Codec != tc.codec || cfg.FlushSize != tc.batch || cfg.FlushBytes != tc.bytes || cfg.MaxBufferBytes != tc.queue || cfg.MaxEntries != tc.entries || cfg.MaxIndexBytes != tc.index {
				t.Fatalf("disk config = %+v", cfg)
			}
		})
	}
}
