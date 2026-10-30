package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

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
