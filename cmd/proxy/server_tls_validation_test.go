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

func TestValidateServerTLS(t *testing.T) {
	for _, tc := range []struct {
		name                string
		cert, key, clientCA string
		requireClientCert   bool
		wantErr             string
	}{
		{name: "plain HTTP"},
		{name: "HTTPS", cert: "tls.crt", key: "tls.key"},
		{name: "HTTPS with optional client certificates", cert: "tls.crt", key: "tls.key", clientCA: "ca.pem"},
		{name: "HTTPS with required client certificates", cert: "tls.crt", key: "tls.key", clientCA: "ca.pem", requireClientCert: true},
		{name: "certificate without key", cert: "tls.crt", wantErr: "must be set together"},
		{name: "key without certificate", key: "tls.key", wantErr: "must be set together"},
		{name: "client CA without certificate and key", clientCA: "ca.pem", wantErr: "need -tls-cert-file and -tls-key-file"},
		{name: "required client certificates without certificate and key", clientCA: "ca.pem", requireClientCert: true, wantErr: "need -tls-cert-file and -tls-key-file"},
		{name: "required client certificates alone", requireClientCert: true, wantErr: "need -tls-cert-file and -tls-key-file"},
		{name: "client CA with only a certificate", cert: "tls.crt", clientCA: "ca.pem", wantErr: "must be set together"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateServerTLS(tc.cert, tc.key, tc.clientCA, tc.requireClientCert)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

// A client CA without a server certificate used to start a plain HTTP
// listener that checked no client certificate; startup now fails before the
// runtime or the listener is built.
func TestRun_ClientCAWithoutServerCertificateRefusesToStart(t *testing.T) {
	err := run([]string{"-tls-client-ca-file=/etc/proxy/ca.pem", "-tls-require-client-cert=true"}, func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return &fakeOTLPPusher{}
	}, func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error) {
		t.Fatal("runtime builder must not run when the TLS settings are refused")
		return nil, nil
	}, func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {
		t.Fatal("the listener must not start when the TLS settings are refused")
	}, func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {
		t.Fatal("shutdown handler must not run when the TLS settings are refused")
	})
	if err == nil || !strings.Contains(err.Error(), "-tls-cert-file and -tls-key-file") {
		t.Fatalf("expected the TLS settings to be refused, got %v", err)
	}
}
