package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

func TestPeerReplicationFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want int
	}{
		{"default_owner_and_backup", nil, 2},
		{"owner_only", []string{"-peer-replication-factor=1"}, 1},
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
			if got.proxyCfg.peerReplicationFactor != tc.want {
				t.Fatalf("replication factor = %d, want %d", got.proxyCfg.peerReplicationFactor, tc.want)
			}
		})
	}
}

// The peer push buffer flags default to the cache's defaults, 0 turns
// buffering off, and a negative value is refused at start.
//
// conformance: peer-cache-push-replay
func TestPeerPushBufferFlags(t *testing.T) {
	capture := func(t *testing.T, args ...string) (runtimeOptions, error) {
		t.Helper()
		sentinel := errors.New("captured config")
		var got runtimeOptions
		err := run(append([]string{"-server.admin-auth-token=test-token"}, args...), func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} }, func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
			got = opts
			return nil, sentinel
		}, nil, nil)
		if errors.Is(err, sentinel) {
			err = nil
		}
		return got, err
	}
	got, err := capture(t)
	if err != nil {
		t.Fatal(err)
	}
	if got.proxyCfg.peerPushBufferMaxEntries != cache.DefaultPushBufferMaxEntries || got.proxyCfg.peerPushBufferMaxBytes != cache.DefaultPushBufferMaxBytes {
		t.Fatalf("defaults %d / %d, want the cache's %d / %d", got.proxyCfg.peerPushBufferMaxEntries, got.proxyCfg.peerPushBufferMaxBytes, cache.DefaultPushBufferMaxEntries, cache.DefaultPushBufferMaxBytes)
	}
	if peerPushBufferBound(0) != -1 || peerPushBufferBound(int64(0)) != -1 || peerPushBufferBound(5) != 5 {
		t.Fatal("0 must turn the buffer off in the peer cache config, other values pass through")
	}
	for _, arg := range []string{"-peer-push-buffer-max-entries=-1", "-peer-push-buffer-max-bytes=-1"} {
		got, err := capture(t, arg)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateExecutionLimits(got.proxyCfg); err == nil || !strings.Contains(err.Error(), "peer-push-buffer") {
			t.Fatalf("%s: %v", arg, err)
		}
	}
	if err := validateExecutionLimits(got.proxyCfg); err != nil {
		t.Fatalf("defaults refused: %v", err)
	}
}

// -peer-read-hedge-max-delay defaults to the peer cache's default, 0 turns hedged reads off, a negative value is
// refused.
//
// conformance: peer-cache-read-hedge
func TestPeerReadHedgeFlag(t *testing.T) {
	sentinel := errors.New("stop")
	capture := func(t *testing.T, args ...string) (runtimeOptions, error) {
		t.Helper()
		var got runtimeOptions
		err := run(append([]string{"-server.admin-auth-token=test-token"}, args...), func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} }, func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
			got = opts
			return nil, sentinel
		}, nil, nil)
		if errors.Is(err, sentinel) {
			err = nil
		}
		return got, err
	}
	got, err := capture(t)
	if err != nil {
		t.Fatal(err)
	}
	if got.proxyCfg.peerReadHedgeMaxDelay != cache.DefaultPeerReadHedgeMaxDelay || peerReadHedgeBound(got.proxyCfg.peerReadHedgeMaxDelay) != cache.DefaultPeerReadHedgeMaxDelay {
		t.Fatalf("default %s, want the cache's %s", got.proxyCfg.peerReadHedgeMaxDelay, cache.DefaultPeerReadHedgeMaxDelay)
	}
	if peerReadHedgeBound(0) != -1 {
		t.Fatal("0 must turn hedged reads off in the peer cache config")
	}
	got, err = capture(t, "-peer-read-hedge-max-delay=-1s")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateExecutionLimits(got.proxyCfg); err == nil || !strings.Contains(err.Error(), "peer-read-hedge-max-delay") {
		t.Fatalf("a negative delay: %v", err)
	}
}
