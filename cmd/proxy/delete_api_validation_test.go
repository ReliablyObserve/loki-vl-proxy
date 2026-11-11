package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// The delete API is closed unless its token is set: enabling it without
// one is refused with the flags named.
//
// conformance: delete-api-requires-token
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
		if err != nil && (!strings.Contains(err.Error(), "-delete-api-enabled") || !strings.Contains(err.Error(), "-delete-api-auth-token")) {
			t.Fatalf("error must name both flags: %v", err)
		}
		if err != nil && strings.Contains(err.Error(), tc.token) && strings.TrimSpace(tc.token) != "" {
			t.Fatalf("error must not echo the token: %v", err)
		}
	}
}

// conformance: delete-api-requires-token
func TestRun_DeleteAPIWithoutTokenRefusesToStart(t *testing.T) {
	err := run([]string{"-delete-api-enabled=true"}, func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return &fakeOTLPPusher{}
	}, func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error) {
		t.Fatal("runtime builder must not run when the delete API has no delete token")
		return nil, nil
	}, func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {
		t.Fatal("the listener must not start when the delete API has no delete token")
	}, func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {
		t.Fatal("shutdown handler must not run when the delete API has no delete token")
	})
	if err == nil || !strings.Contains(err.Error(), "-delete-api-enabled=true requires -delete-api-auth-token") {
		t.Fatalf("expected the delete API without a delete token to be refused, got %v", err)
	}
}

// Enabling deletes changes nothing about the admin surfaces: without
// -server.admin-auth-token they stay on the loopback -admin-listen exactly as
// before, the delete token reaches only the delete API, and the admin token is
// not set from it.
//
// conformance: delete-api-requires-token
func TestRun_DeleteAPIKeepsAdminListenerOnLoopback(t *testing.T) {
	for _, args := range [][]string{
		{"-delete-api-enabled=true", "-delete-api-auth-token=delete-secret", "-server.register-instrumentation=true", "-server.enable-pprof=true"},
		{"-delete-api-enabled=true", "-delete-api-auth-token=delete-secret", "-server.register-instrumentation=true", "-admin-listen=127.0.0.1:3999"},
	} {
		reloadCh := make(chan os.Signal)
		close(reloadCh)
		shutdownCh := make(chan os.Signal, 1)
		shutdownCh <- syscall.SIGTERM
		var captured runtimeOptions
		err := run(args, func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {},
			func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
			func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
				captured = opts
				return &runtimeState{proxy: &proxy.Proxy{}, server: &fakeHTTPServer{}, cacheCleanup: func() {}, stopOTLP: func() {}, reloadCh: reloadCh, shutdownCh: shutdownCh}, nil
			},
			func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {},
			func(ch <-chan os.Signal, _ httpServer, _ time.Duration, _ *slog.Logger) { <-ch })
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		want := "127.0.0.1:3101"
		if strings.Contains(strings.Join(args, " "), "-admin-listen=") {
			want = "127.0.0.1:3999"
		}
		if captured.adminListenAddr != want {
			t.Fatalf("%v: admin listener %q, want the loopback %q as without deletes", args, captured.adminListenAddr, want)
		}
		if captured.proxyCfg.adminAuthToken != "" || captured.proxyCfg.deleteAPIAuthToken != "delete-secret" || !captured.proxyCfg.deleteAPIEnabled {
			t.Fatalf("%v: admin token %q, delete token %q, enabled %v", args, captured.proxyCfg.adminAuthToken, captured.proxyCfg.deleteAPIAuthToken, captured.proxyCfg.deleteAPIEnabled)
		}
	}
}
