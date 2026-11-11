package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// The delete API is closed unless the admin token is set: enabling it without
// one is refused with the flags named.
//
// conformance: delete-api-requires-admin-token
func TestValidateDeleteAPI(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		token   string
		wantErr bool
	}{
		{false, "", false},
		{false, "secret", false},
		{true, "secret", false},
		{true, "", true},
		{true, "  \t", true},
	} {
		err := validateDeleteAPI(tc.enabled, tc.token)
		if (err != nil) != tc.wantErr {
			t.Fatalf("enabled=%v token=%q: err %v, want error %v", tc.enabled, tc.token, err, tc.wantErr)
		}
		if err != nil && (!strings.Contains(err.Error(), "-delete-api-enabled") || !strings.Contains(err.Error(), "-server.admin-auth-token")) {
			t.Fatalf("error must name both flags: %v", err)
		}
		if err != nil && strings.Contains(err.Error(), tc.token) && strings.TrimSpace(tc.token) != "" {
			t.Fatalf("error must not echo the token: %v", err)
		}
	}
}

// conformance: delete-api-requires-admin-token
func TestRun_DeleteAPIWithoutAdminTokenRefusesToStart(t *testing.T) {
	err := run([]string{"-delete-api-enabled=true"}, func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return &fakeOTLPPusher{}
	}, func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error) {
		t.Fatal("runtime builder must not run when the delete API has no admin token")
		return nil, nil
	}, func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {
		t.Fatal("the listener must not start when the delete API has no admin token")
	}, func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {
		t.Fatal("shutdown handler must not run when the delete API has no admin token")
	})
	if err == nil || !strings.Contains(err.Error(), "-delete-api-enabled=true requires -server.admin-auth-token") {
		t.Fatalf("expected the delete API without an admin token to be refused, got %v", err)
	}
}
