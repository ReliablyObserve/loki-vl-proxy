package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
	"github.com/szibis/Loki-VL-proxy/internal/metrics"
	"github.com/szibis/Loki-VL-proxy/internal/proxy"
)

type fakeReloadableProxy struct {
	tenantMap     map[string]proxy.TenantMapping
	fieldMappings []proxy.FieldMapping
}

type fakeHTTPServer struct {
	listenErr      error
	listenTLSErr   error
	shutdownErr    error
	listenCalls    int
	listenTLSCalls int
	shutdownCalls  int
}

type fakeOTLPPusher struct {
	started bool
	stopped bool
}

type runtimeRecorder struct {
	options runtimeOptions
	called  bool
	runtime *runtimeState
	err     error
}

type exitRecorder struct {
	code  int
	calls int
}

func (f *fakeReloadableProxy) ReloadTenantMap(m map[string]proxy.TenantMapping) {
	f.tenantMap = m
}

func (f *fakeReloadableProxy) ReloadFieldMappings(m []proxy.FieldMapping) {
	f.fieldMappings = m
}

func (f *fakeHTTPServer) ListenAndServe() error {
	f.listenCalls++
	return f.listenErr
}

func (f *fakeHTTPServer) ListenAndServeTLS(_, _ string) error {
	f.listenTLSCalls++
	return f.listenTLSErr
}

func (f *fakeHTTPServer) Shutdown(context.Context) error {
	f.shutdownCalls++
	return f.shutdownErr
}

func (f *fakeOTLPPusher) Start() { f.started = true }

func (f *fakeOTLPPusher) Stop() { f.stopped = true }

func (r *runtimeRecorder) build(_ runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
	r.called = true
	return r.runtime, r.err
}

func (r *exitRecorder) exit(code int) {
	r.calls++
	r.code = code
}

func TestBuildLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := buildLogger(buf, loggerConfig{
		level:                 "debug",
		serviceName:           "proxy",
		serviceNamespace:      "platform",
		serviceVersion:        "v1.2.3",
		serviceInstanceID:     "proxy-1",
		deploymentEnvironment: "prod",
	})
	if logger == nil {
		t.Fatal("expected logger")
	}
	logger.Info("hello")
	logs := buf.String()
	for _, want := range []string{"\"service.name\":\"proxy\"", "\"service.version\":\"v1.2.3\"", "\"deployment.environment.name\":\"prod\"", "\"body\":\"hello\""} {
		if !strings.Contains(logs, want) {
			t.Fatalf("expected %q in %s", want, logs)
		}
	}
}

func TestRun_Success(t *testing.T) {
	reloadCh := make(chan os.Signal)
	close(reloadCh)
	shutdownCh := make(chan os.Signal, 1)
	shutdownCh <- syscall.SIGTERM
	srv := &fakeHTTPServer{}
	recorder := &runtimeRecorder{
		runtime: &runtimeState{
			proxy:        &proxy.Proxy{},
			server:       srv,
			cacheCleanup: func() {},
			stopOTLP:     func() {},
			reloadCh:     reloadCh,
			shutdownCh:   shutdownCh,
		},
	}
	var gotLoopOpts serverLoopOptions
	var shutdownCalled bool
	loopDone := make(chan struct{})

	err := run([]string{
		"-listen", ":9999",
		"-backend", "http://backend.test",
		"-response-gzip=false",
		"-tail.mode=synthetic",
	}, func(key string) string {
		if key == "OTEL_SERVICE_NAME" {
			return "custom-proxy"
		}
		return ""
	}, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return &fakeOTLPPusher{}
	}, func(opts runtimeOptions, logger *slog.Logger, notify signalNotifier, newPusher otlpPusherFactory) (*runtimeState, error) {
		recorder.options = opts
		return recorder.build(opts, logger, notify, newPusher)
	}, func(_ httpServer, opts serverLoopOptions, _ *slog.Logger, _ func(string, ...any)) {
		gotLoopOpts = opts
		close(loopDone)
	}, func(ch <-chan os.Signal, _ httpServer, _ time.Duration, _ *slog.Logger) {
		shutdownCalled = true
		<-ch
	})
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}
	if !recorder.called {
		t.Fatal("expected runtime builder to be called")
	}
	if recorder.options.proxyCfg.backendURL != "http://backend.test" {
		t.Fatalf("unexpected backend URL: %+v", recorder.options.proxyCfg)
	}
	if recorder.options.proxyCfg.tailMode != "synthetic" || recorder.options.enableGzip {
		t.Fatalf("unexpected parsed runtime options: %+v", recorder.options)
	}
	if recorder.options.serverOpts.listenAddr != ":9999" {
		t.Fatalf("unexpected listen addr: %+v", recorder.options.serverOpts)
	}
	if recorder.options.otlpCfg.serviceName != "custom-proxy" {
		t.Fatalf("expected env override to apply, got %+v", recorder.options.otlpCfg)
	}
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("expected server loop to be invoked")
	}
	if gotLoopOpts.listenAddr != ":9999" || gotLoopOpts.backendURL != "http://backend.test" {
		t.Fatalf("unexpected server loop options: %+v", gotLoopOpts)
	}
	if !shutdownCalled {
		t.Fatal("expected shutdown handler to be called")
	}
}

func TestRun_ParseError(t *testing.T) {
	err := run([]string{"-unknown-flag"}, func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return &fakeOTLPPusher{}
	}, func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error) {
		t.Fatal("runtime builder should not be called on parse error")
		return nil, nil
	}, func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {
		t.Fatal("server loop should not be called on parse error")
	}, func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {
		t.Fatal("shutdown handler should not be called on parse error")
	})
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestRun_RuntimeInitError(t *testing.T) {
	wantErr := errors.New("boom")
	err := run([]string{}, func(string) string { return "" }, io.Discard, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return &fakeOTLPPusher{}
	}, func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error) {
		return nil, wantErr
	}, func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {
		t.Fatal("server loop should not run when runtime init fails")
	}, func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {
		t.Fatal("shutdown handler should not run when runtime init fails")
	})
	if err == nil || !strings.Contains(err.Error(), "failed to initialize runtime") {
		t.Fatalf("expected wrapped runtime init error, got %v", err)
	}
}

func TestRunMain_WritesErrorAndExits(t *testing.T) {
	stderr := &bytes.Buffer{}
	exits := &exitRecorder{}

	runMain(
		[]string{"-unknown-flag"},
		func(string) string { return "" },
		io.Discard,
		stderr,
		func(chan<- os.Signal, ...os.Signal) {},
		exits.exit,
		func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
		func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error) {
			t.Fatal("runtime builder should not be called on parse error")
			return nil, nil
		},
		func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {
			t.Fatal("server loop should not be called on parse error")
		},
		func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {
			t.Fatal("shutdown handler should not be called on parse error")
		},
	)

	if exits.calls != 1 || exits.code != 1 {
		t.Fatalf("expected one exit(1), got calls=%d code=%d", exits.calls, exits.code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Fatalf("expected parse error on stderr, got %q", stderr.String())
	}
}

func TestRunMain_SuccessDoesNotExit(t *testing.T) {
	reloadCh := make(chan os.Signal)
	close(reloadCh)
	shutdownCh := make(chan os.Signal, 1)
	shutdownCh <- syscall.SIGTERM
	exits := &exitRecorder{}

	runMain(
		nil,
		func(string) string { return "" },
		io.Discard,
		&bytes.Buffer{},
		func(chan<- os.Signal, ...os.Signal) {},
		exits.exit,
		func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
		func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error) {
			return &runtimeState{
				proxy:        &proxy.Proxy{},
				server:       &fakeHTTPServer{},
				cacheCleanup: func() {},
				stopOTLP:     func() {},
				reloadCh:     reloadCh,
				shutdownCh:   shutdownCh,
			}, nil
		},
		func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {},
		func(ch <-chan os.Signal, _ httpServer, _ time.Duration, _ *slog.Logger) {
			<-ch
		},
	)

	if exits.calls != 0 {
		t.Fatalf("expected no exit on success, got %d calls", exits.calls)
	}
}

func TestBuildServerTLSConfig_RequiresCAWhenClientCertsRequired(t *testing.T) {
	cfg, err := buildServerTLSConfig("", true)
	if err == nil {
		t.Fatal("expected error when client cert auth is required without a CA file")
	}
	if cfg != nil {
		t.Fatal("expected nil TLS config on error")
	}
}

func TestBuildServerTLSConfig_LoadsClientCAPool(t *testing.T) {
	caPath := writeTestCA(t)

	cfg, err := buildServerTLSConfig(caPath, true)
	if err != nil {
		t.Fatalf("expected TLS config, got error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected TLS config to be returned")
	}
	if cfg.ClientCAs == nil {
		t.Fatal("expected client CA pool to be configured")
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected RequireAndVerifyClientCert, got %v", cfg.ClientAuth)
	}
}

func TestBuildServerTLSConfig_NilWithoutClientAuth(t *testing.T) {
	cfg, err := buildServerTLSConfig("", false)
	if err != nil {
		t.Fatalf("expected nil config without error, got %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil TLS config when client CA is not configured")
	}
}

func TestBuildServerTLSConfig_InvalidPEM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(path, []byte("not-a-cert"), 0o600); err != nil {
		t.Fatalf("write bad pem: %v", err)
	}
	cfg, err := buildServerTLSConfig(path, false)
	if err == nil {
		t.Fatal("expected invalid PEM error")
	}
	if cfg != nil {
		t.Fatal("expected nil config on invalid PEM")
	}
}

func TestBuildServerTLSConfig_VerifyIfGivenWhenOptional(t *testing.T) {
	caPath := writeTestCA(t)
	cfg, err := buildServerTLSConfig(caPath, false)
	if err != nil {
		t.Fatalf("expected TLS config, got error: %v", err)
	}
	if cfg.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatalf("expected VerifyClientCertIfGiven, got %v", cfg.ClientAuth)
	}
}

func TestBuildServerTLSConfig_MissingFile(t *testing.T) {
	cfg, err := buildServerTLSConfig(filepath.Join(t.TempDir(), "missing.pem"), false)
	if err == nil {
		t.Fatal("expected missing file error")
	}
	if cfg != nil {
		t.Fatal("expected nil config on missing file")
	}
}

func TestParseCSV(t *testing.T) {
	got := parseCSV(" foo,bar ,, baz ")
	want := []string{"foo", "bar", "baz"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
	if parseCSV("") != nil {
		t.Fatal("expected nil for empty CSV")
	}
}

func TestParseHeaderMapCSV(t *testing.T) {
	got := parseHeaderMapCSV("Authorization=Bearer abc, X-Scope-OrgID = team-a, broken, empty= ")
	if len(got) != 2 {
		t.Fatalf("expected 2 parsed headers, got %+v", got)
	}
	if got["Authorization"] != "Bearer abc" {
		t.Fatalf("unexpected authorization header: %+v", got)
	}
	if got["X-Scope-OrgID"] != "team-a" {
		t.Fatalf("unexpected scope header: %+v", got)
	}
	if parseHeaderMapCSV("") != nil {
		t.Fatal("expected nil for empty header map")
	}
}

func TestParseHeaderMapCSV_DuplicateKeysLastWins(t *testing.T) {
	got := parseHeaderMapCSV("X-Scope-OrgID=team-a, X-Scope-OrgID=team-b")
	if got["X-Scope-OrgID"] != "team-b" {
		t.Fatalf("expected last duplicate header value to win, got %+v", got)
	}
}

func TestMaxBodyHandler(t *testing.T) {
	var seenErr error
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, seenErr = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	})

	body := strings.Repeat("a", 8)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	maxBodyHandler(4, next).ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected next handler status, got %d", w.Code)
	}
	if seenErr == nil {
		t.Fatal("expected body read to fail when it exceeds limit")
	}
}

func TestWrapHandler_GzipEnabled(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("hello", 20)))
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	wrapHandler(next, 1024, true).ServeHTTP(w, req)

	if got := w.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip response, got %q", got)
	}
	gr, err := gzip.NewReader(bytes.NewReader(w.Body.Bytes()))
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	defer gr.Close()
	body, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}
	if !strings.Contains(string(body), "hello") {
		t.Fatalf("expected decompressed body, got %q", string(body))
	}
}

func TestBuildOTLPConfig(t *testing.T) {
	cfg := buildOTLPConfig(otlpRuntimeConfig{
		endpoint:              "http://collector:4318/v1/metrics",
		interval:              15 * time.Second,
		headers:               "Authorization=Bearer abc, X-Scope-OrgID=team-a",
		compression:           "gzip",
		timeout:               5 * time.Second,
		tlsSkipVerify:         true,
		serviceName:           "proxy",
		serviceNamespace:      "platform",
		serviceVersion:        "v1.2.3",
		serviceInstanceID:     "proxy-1",
		deploymentEnvironment: "prod",
	})

	if cfg.Endpoint != "http://collector:4318/v1/metrics" || cfg.Interval != 15*time.Second {
		t.Fatalf("unexpected endpoint/interval: %+v", cfg)
	}
	if cfg.Headers["Authorization"] != "Bearer abc" || cfg.Headers["X-Scope-OrgID"] != "team-a" {
		t.Fatalf("unexpected headers: %+v", cfg.Headers)
	}
	if cfg.Compression != "gzip" || !cfg.TLSSkipVerify {
		t.Fatalf("unexpected compression/tls config: %+v", cfg)
	}
	if cfg.ServiceName != "proxy" || cfg.ServiceNamespace != "platform" || cfg.ServiceVersion != "v1.2.3" || cfg.ServiceInstanceID != "proxy-1" || cfg.DeploymentEnvironment != "prod" {
		t.Fatalf("unexpected resource attributes: %+v", cfg)
	}
}

func TestStartOTLPMetricsPusher_NoEndpointIsNoop(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	called := false

	stop := startOTLPMetricsPusher(otlpRuntimeConfig{}, metrics.NewMetrics(), logger, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		called = true
		return &fakeOTLPPusher{}
	})
	stop()

	if called {
		t.Fatal("expected no pusher to be created when OTLP endpoint is empty")
	}
}

func TestStartOTLPMetricsPusher_StartsAndStops(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	m := metrics.NewMetrics()
	var gotCfg metrics.OTLPConfig
	fake := &fakeOTLPPusher{}

	stop := startOTLPMetricsPusher(otlpRuntimeConfig{
		endpoint:              "http://collector:4318/v1/metrics",
		interval:              12 * time.Second,
		headers:               "Authorization=Bearer abc",
		compression:           "gzip",
		timeout:               5 * time.Second,
		tlsSkipVerify:         true,
		serviceName:           "proxy",
		serviceNamespace:      "platform",
		serviceVersion:        "v1.2.3",
		serviceInstanceID:     "proxy-1",
		deploymentEnvironment: "prod",
	}, m, logger, func(cfg metrics.OTLPConfig, gotM *metrics.Metrics) otlpMetricsPusher {
		gotCfg = cfg
		if gotM != m {
			t.Fatalf("expected metrics pointer to be preserved")
		}
		return fake
	})
	if !fake.started {
		t.Fatal("expected OTLP pusher to start")
	}
	stop()
	if !fake.stopped {
		t.Fatal("expected OTLP pusher stop func to stop the pusher")
	}
	if gotCfg.Endpoint != "http://collector:4318/v1/metrics" || gotCfg.Compression != "gzip" {
		t.Fatalf("unexpected OTLP config: %+v", gotCfg)
	}
	if !strings.Contains(buf.String(), "otlp metrics push enabled") {
		t.Fatalf("expected OTLP startup log, got %s", buf.String())
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	cfg := envConfig{
		listenAddr:        ":3100",
		backendURL:        "http://backend",
		rulerBackendURL:   "http://ruler-flag",
		alertsBackendURL:  "http://alerts-flag",
		otlpCompression:   "none",
		labelStyle:        "passthrough",
		metadataFieldMode: "hybrid",
	}
	env := map[string]string{
		"LISTEN_ADDR":              ":9999",
		"VL_BACKEND_URL":           "http://other",
		"RULER_BACKEND_URL":        "http://ruler-env",
		"ALERTS_BACKEND_URL":       "http://alerts-env",
		"TENANT_MAP":               `{"team":{"account_id":"1","project_id":"0"}}`,
		"OTLP_ENDPOINT":            "http://otel",
		"OTLP_COMPRESSION":         "gzip",
		"LABEL_STYLE":              "underscores",
		"FIELD_MAPPING":            `[{"vl_field":"service.name","loki_label":"service_name"}]`,
		"METADATA_FIELD_MODE":      "native",
		"OTEL_SERVICE_NAME":        "custom-proxy",
		"OTEL_SERVICE_NAMESPACE":   "platform",
		"OTEL_SERVICE_INSTANCE_ID": "proxy-1",
		"DEPLOYMENT_ENVIRONMENT":   "prod",
	}
	got := applyEnvOverrides(cfg, func(key string) string { return env[key] })
	if got.listenAddr != ":9999" || got.backendURL != "http://other" || got.otlpEndpoint != "http://otel" {
		t.Fatalf("unexpected env override result: %+v", got)
	}
	if got.rulerBackendURL != "http://ruler-flag" || got.alertsBackendURL != "http://alerts-flag" {
		t.Fatalf("expected explicit ruler/alerts flag values to win, got %+v", got)
	}
	if got.tenantMapJSON == "" || got.fieldMappingJSON == "" {
		t.Fatalf("expected JSON env overrides, got %+v", got)
	}
	if got.otlpCompression != "gzip" || got.labelStyle != "underscores" || got.metadataFieldMode != "native" {
		t.Fatalf("unexpected style/compression override result: %+v", got)
	}
	if got.serviceName != "custom-proxy" || got.serviceNamespace != "platform" || got.serviceInstanceID != "proxy-1" || got.deploymentEnv != "prod" {
		t.Fatalf("unexpected observability env overrides: %+v", got)
	}
}

func TestApplyEnvOverrides_PreservesExplicitFlags(t *testing.T) {
	cfg := envConfig{
		tenantMapJSON:     `{}`,
		otlpEndpoint:      "http://flag",
		otlpCompression:   "zstd",
		labelStyle:        "underscores",
		fieldMappingJSON:  `[]`,
		metadataFieldMode: "translated",
	}
	env := map[string]string{
		"TENANT_MAP":          `{"ignored":{}}`,
		"OTLP_ENDPOINT":       "http://env",
		"OTLP_COMPRESSION":    "gzip",
		"LABEL_STYLE":         "passthrough",
		"FIELD_MAPPING":       `[{"ignored":true}]`,
		"METADATA_FIELD_MODE": "native",
	}
	got := applyEnvOverrides(cfg, func(key string) string { return env[key] })
	if got != cfg {
		t.Fatalf("expected explicit values to win, got %+v", got)
	}
}

func TestReloadDynamicConfig(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	fake := &fakeReloadableProxy{}
	env := map[string]string{
		"TENANT_MAP":    `{"team-a":{"account_id":"1","project_id":"2"}}`,
		"FIELD_MAPPING": `[{"vl_field":"service.name","loki_label":"service_name"}]`,
	}

	reloadDynamicConfig(fake, func(key string) string { return env[key] }, logger)

	if fake.tenantMap["team-a"] != (proxy.TenantMapping{AccountID: "1", ProjectID: "2"}) {
		t.Fatalf("unexpected tenant map reload: %+v", fake.tenantMap)
	}
	if len(fake.fieldMappings) != 1 || fake.fieldMappings[0].VLField != "service.name" {
		t.Fatalf("unexpected field mapping reload: %+v", fake.fieldMappings)
	}
	logs := buf.String()
	if !strings.Contains(logs, "reloaded tenant mappings") || !strings.Contains(logs, "reloaded field mappings") {
		t.Fatalf("expected reload logs, got %s", logs)
	}
}

func TestReloadDynamicConfig_InvalidJSON(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	fake := &fakeReloadableProxy{}
	env := map[string]string{
		"TENANT_MAP":    "{",
		"FIELD_MAPPING": "{",
	}

	reloadDynamicConfig(fake, func(key string) string { return env[key] }, logger)

	if fake.tenantMap != nil || fake.fieldMappings != nil {
		t.Fatalf("expected no reloads on invalid JSON, got %+v %+v", fake.tenantMap, fake.fieldMappings)
	}
	logs := buf.String()
	if !strings.Contains(logs, "failed to reload tenant map") || !strings.Contains(logs, "failed to reload field mappings") {
		t.Fatalf("expected reload errors, got %s", logs)
	}
}

func TestParseTenantMapJSON(t *testing.T) {
	got, err := parseTenantMapJSON(`{"team-a":{"account_id":"1","project_id":"2"}}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["team-a"] != (proxy.TenantMapping{AccountID: "1", ProjectID: "2"}) {
		t.Fatalf("unexpected tenant map: %+v", got)
	}
	if _, err := parseTenantMapJSON("{"); err == nil {
		t.Fatal("expected invalid tenant map JSON error")
	}
}

func TestParseFieldMappingsJSON(t *testing.T) {
	got, err := parseFieldMappingsJSON(`[{"vl_field":"service.name","loki_label":"service_name"}]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0].VLField != "service.name" || got[0].LokiLabel != "service_name" {
		t.Fatalf("unexpected field mappings: %+v", got)
	}
	if _, err := parseFieldMappingsJSON("{"); err == nil {
		t.Fatal("expected invalid field mapping JSON error")
	}
}

func TestParseDerivedFieldsJSON(t *testing.T) {
	got, err := parseDerivedFieldsJSON(`[{"name":"traceID","matcherRegex":"trace_id=(\\w+)","url":"http://tempo/${__value.raw}"}]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0].Name != "traceID" {
		t.Fatalf("unexpected derived fields: %+v", got)
	}
	if _, err := parseDerivedFieldsJSON("{"); err == nil {
		t.Fatal("expected invalid derived field JSON error")
	}
}

func TestParseLabelModes(t *testing.T) {
	ls, mfm, err := parseLabelModes("underscores", "native")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ls != proxy.LabelStyleUnderscores || mfm != proxy.MetadataFieldModeNative {
		t.Fatalf("unexpected parsed modes: %v %v", ls, mfm)
	}
	if _, _, err := parseLabelModes("bad", "native"); err == nil {
		t.Fatal("expected invalid label style error")
	}
	if _, _, err := parseLabelModes("underscores", "bad"); err == nil {
		t.Fatal("expected invalid metadata field mode error")
	}
}

func TestBuildProxyConfig(t *testing.T) {
	registerInstrumentation := true
	c := proxyRuntimeConfig{
		backendURL:               "http://backend",
		rulerBackendURL:          "http://ruler",
		alertsBackendURL:         "http://alerts",
		cache:                    nil,
		logLevel:                 "debug",
		tenantMapJSON:            `{"team-a":{"account_id":"1","project_id":"2"}}`,
		maxLines:                 123,
		backendTimeout:           5 * time.Second,
		backendBasicAuth:         "user:pass",
		backendTLSSkip:           true,
		forwardHeaders:           "Authorization, X-Scope-OrgID",
		forwardCookies:           "session, csrf",
		derivedFieldsJSON:        `[{"name":"traceID","matcherRegex":"trace_id=(\\w+)","url":"http://tempo/${__value.raw}"}]`,
		streamResponse:           true,
		authEnabled:              true,
		allowGlobalTenant:        true,
		registerInstrumentation:  &registerInstrumentation,
		enablePprof:              true,
		enableQueryAnalytics:     true,
		adminAuthToken:           "secret",
		tailAllowedOrigins:       "https://grafana.example.com",
		tailMode:                 "synthetic",
		metricsMaxTenants:        11,
		metricsMaxClients:        12,
		metricsTrustProxyHeaders: true,
		labelStyle:               "underscores",
		metadataFieldMode:        "hybrid",
		fieldMappingJSON:         `[{"vl_field":"service.name","loki_label":"service_name"}]`,
		streamFieldsCSV:          "app,namespace",
		peerSelf:                 "10.0.0.1:3100",
		peerDiscovery:            "static",
		peerStatic:               "10.0.0.2:3100,10.0.0.3:3100",
		peerAuthToken:            "peer-secret",
	}

	got, err := buildProxyConfig(c)
	if err != nil {
		t.Fatalf("unexpected buildProxyConfig error: %v", err)
	}
	if got.BackendURL != "http://backend" || got.MaxLines != 123 || got.BackendBasicAuth != "user:pass" || !got.BackendTLSSkip {
		t.Fatalf("unexpected proxy config basics: %+v", got)
	}
	if got.RulerBackendURL != "http://ruler" || got.AlertsBackendURL != "http://alerts" {
		t.Fatalf("unexpected alerting backend urls: %+v", got)
	}
	if len(got.TenantMap) != 1 || got.TenantMap["team-a"].AccountID != "1" {
		t.Fatalf("unexpected tenant map: %+v", got.TenantMap)
	}
	if len(got.ForwardHeaders) != 2 || got.ForwardHeaders[0] != "Authorization" {
		t.Fatalf("unexpected forward headers: %+v", got.ForwardHeaders)
	}
	if len(got.ForwardCookies) != 2 || got.ForwardCookies[1] != "csrf" {
		t.Fatalf("unexpected forward cookies: %+v", got.ForwardCookies)
	}
	if got.LabelStyle != proxy.LabelStyleUnderscores || got.MetadataFieldMode != proxy.MetadataFieldModeHybrid {
		t.Fatalf("unexpected label modes: %v %v", got.LabelStyle, got.MetadataFieldMode)
	}
	if len(got.FieldMappings) != 1 || got.FieldMappings[0].VLField != "service.name" {
		t.Fatalf("unexpected field mappings: %+v", got.FieldMappings)
	}
	if len(got.DerivedFields) != 1 || got.DerivedFields[0].Name != "traceID" {
		t.Fatalf("unexpected derived fields: %+v", got.DerivedFields)
	}
	if len(got.StreamFields) != 2 || got.StreamFields[0] != "app" {
		t.Fatalf("unexpected stream fields: %+v", got.StreamFields)
	}
	if got.TailMode != proxy.TailModeSynthetic {
		t.Fatalf("unexpected tail mode: %v", got.TailMode)
	}
	if got.PeerCache == nil {
		t.Fatal("expected peer cache to be created")
	}
	if got.PeerAuthToken != "peer-secret" {
		t.Fatalf("unexpected peer auth token: %q", got.PeerAuthToken)
	}
}

func TestBuildProxyConfig_DefaultsAlertsBackendToRuler(t *testing.T) {
	got, err := buildProxyConfig(proxyRuntimeConfig{
		backendURL:        "http://backend",
		rulerBackendURL:   "http://ruler",
		cache:             cache.New(60*time.Second, 1000),
		logLevel:          "error",
		labelStyle:        "passthrough",
		metadataFieldMode: "hybrid",
	})
	if err != nil {
		t.Fatalf("unexpected buildProxyConfig error: %v", err)
	}
	if got.RulerBackendURL != "http://ruler" {
		t.Fatalf("expected ruler backend URL to be preserved, got %q", got.RulerBackendURL)
	}
	if got.AlertsBackendURL != "http://ruler" {
		t.Fatalf("expected alerts backend to default to ruler backend, got %q", got.AlertsBackendURL)
	}
}

func TestBuildProxyConfig_InvalidInputs(t *testing.T) {
	cases := []proxyRuntimeConfig{
		{tenantMapJSON: "{", labelStyle: "passthrough", metadataFieldMode: "hybrid"},
		{fieldMappingJSON: "{", labelStyle: "passthrough", metadataFieldMode: "hybrid"},
		{derivedFieldsJSON: "{", labelStyle: "passthrough", metadataFieldMode: "hybrid"},
		{labelStyle: "bad", metadataFieldMode: "hybrid"},
		{labelStyle: "passthrough", metadataFieldMode: "bad"},
		{labelStyle: "passthrough", metadataFieldMode: "hybrid", tailMode: "bad"},
	}
	for _, tc := range cases {
		if _, err := buildProxyConfig(tc); err == nil {
			t.Fatalf("expected buildProxyConfig to reject %+v", tc)
		}
	}
}

func TestBuildHTTPServer(t *testing.T) {
	srv, err := buildHTTPServer(serverRuntimeOptions{
		listenAddr:     ":9999",
		handler:        http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		readTimeout:    2 * time.Second,
		writeTimeout:   3 * time.Second,
		idleTimeout:    4 * time.Second,
		maxHeaderBytes: 8192,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.Addr != ":9999" || srv.ReadTimeout != 2*time.Second || srv.WriteTimeout != 3*time.Second || srv.IdleTimeout != 4*time.Second || srv.MaxHeaderBytes != 8192 {
		t.Fatalf("unexpected server config: %+v", srv)
	}
}

func TestBuildHTTPServer_WithTLSClientCA(t *testing.T) {
	caPath := writeTestCA(t)
	srv, err := buildHTTPServer(serverRuntimeOptions{
		listenAddr:           ":9999",
		handler:              http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		tlsClientCAFile:      caPath,
		tlsRequireClientCert: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.TLSConfig == nil || srv.TLSConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected client-auth TLS config, got %+v", srv.TLSConfig)
	}
}

func TestBuildCacheLayer_WithoutDiskCache(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	c, cleanup, err := buildCacheLayer(15*time.Second, 123, cache.DiskCacheConfig{}, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()

	if c == nil {
		t.Fatal("expected in-memory cache")
	}
	if got := buf.String(); strings.Contains(got, "disk cache enabled") {
		t.Fatalf("did not expect disk cache log, got %s", got)
	}
}

func TestBuildCacheLayer_WithDiskCache(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	c, cleanup, err := buildCacheLayer(15*time.Second, 123, cache.DiskCacheConfig{
		Path:          filepath.Join(t.TempDir(), "cache.db"),
		Compression:   true,
		FlushSize:     7,
		FlushInterval: 2 * time.Second,
	}, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()

	if c == nil {
		t.Fatal("expected cache with disk layer")
	}
	logs := buf.String()
	if !strings.Contains(logs, "disk cache enabled") || !strings.Contains(logs, "\"flush_size\":7") {
		t.Fatalf("expected disk cache startup log, got %s", logs)
	}
}

func TestBuildCacheLayer_InvalidDiskCache(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if _, cleanup, err := buildCacheLayer(15*time.Second, 123, cache.DiskCacheConfig{
		Path:          t.TempDir(),
		Compression:   true,
		FlushSize:     7,
		FlushInterval: 2 * time.Second,
	}, logger); err == nil {
		if cleanup != nil {
			cleanup()
		}
		t.Fatal("expected invalid disk cache path error")
	}
}

func TestRunServerLoop_UsesPlainHTTPByDefault(t *testing.T) {
	srv := &fakeHTTPServer{}
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	var fatalCalls int

	runServerLoop(srv, serverLoopOptions{
		listenAddr: ":3100",
		backendURL: "http://backend",
	}, logger, func(string, ...any) {
		fatalCalls++
	})

	if srv.listenCalls != 1 || srv.listenTLSCalls != 0 {
		t.Fatalf("expected plain HTTP listen path, got listen=%d tls=%d", srv.listenCalls, srv.listenTLSCalls)
	}
	if fatalCalls != 0 {
		t.Fatalf("expected no fatal calls, got %d", fatalCalls)
	}
	if !strings.Contains(buf.String(), `"tls":false`) {
		t.Fatalf("expected non-TLS startup log, got %s", buf.String())
	}
}

func TestRunServerLoop_UsesTLSWhenCertAndKeyConfigured(t *testing.T) {
	srv := &fakeHTTPServer{}
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	var fatalCalls int

	runServerLoop(srv, serverLoopOptions{
		listenAddr:  ":3100",
		backendURL:  "http://backend",
		tlsCertFile: "server.crt",
		tlsKeyFile:  "server.key",
	}, logger, func(string, ...any) {
		fatalCalls++
	})

	if srv.listenCalls != 0 || srv.listenTLSCalls != 1 {
		t.Fatalf("expected TLS listen path, got listen=%d tls=%d", srv.listenCalls, srv.listenTLSCalls)
	}
	if fatalCalls != 0 {
		t.Fatalf("expected no fatal calls, got %d", fatalCalls)
	}
	if !strings.Contains(buf.String(), `"tls":true`) {
		t.Fatalf("expected TLS startup log, got %s", buf.String())
	}
}

func TestRunServerLoop_ReportsServerFailure(t *testing.T) {
	srv := &fakeHTTPServer{listenErr: errors.New("boom")}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	var fatalMsg string

	runServerLoop(srv, serverLoopOptions{
		listenAddr: ":3100",
		backendURL: "http://backend",
	}, logger, func(msg string, _ ...any) {
		fatalMsg = msg
	})

	if fatalMsg != "server failed" {
		t.Fatalf("expected server failure message, got %q", fatalMsg)
	}
}

func TestRunServerLoop_IgnoresServerClosed(t *testing.T) {
	srv := &fakeHTTPServer{listenErr: http.ErrServerClosed}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	var fatalCalls int

	runServerLoop(srv, serverLoopOptions{
		listenAddr: ":3100",
		backendURL: "http://backend",
	}, logger, func(string, ...any) {
		fatalCalls++
	})

	if fatalCalls != 0 {
		t.Fatalf("expected http.ErrServerClosed to be ignored, got %d fatal calls", fatalCalls)
	}
}

func TestRunServerLoop_IgnoresTLSServerClosed(t *testing.T) {
	srv := &fakeHTTPServer{listenTLSErr: http.ErrServerClosed}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	var fatalCalls int

	runServerLoop(srv, serverLoopOptions{
		listenAddr:  ":3100",
		backendURL:  "http://backend",
		tlsCertFile: "server.crt",
		tlsKeyFile:  "server.key",
	}, logger, func(string, ...any) {
		fatalCalls++
	})

	if fatalCalls != 0 {
		t.Fatalf("expected tls http.ErrServerClosed to be ignored, got %d fatal calls", fatalCalls)
	}
}

func TestBuildSignalChannels(t *testing.T) {
	type notifyCall struct {
		ch      chan<- os.Signal
		signals []os.Signal
	}
	var calls []notifyCall

	reloadCh, shutdownCh := buildSignalChannels(func(ch chan<- os.Signal, sigs ...os.Signal) {
		calls = append(calls, notifyCall{ch: ch, signals: sigs})
	})

	if reloadCh == nil || shutdownCh == nil {
		t.Fatal("expected signal channels")
	}
	if len(calls) != 2 {
		t.Fatalf("expected 2 notify calls, got %d", len(calls))
	}
	if len(calls[0].signals) != 1 || calls[0].signals[0] != syscall.SIGHUP {
		t.Fatalf("unexpected reload signals: %+v", calls[0].signals)
	}
	if len(calls[1].signals) != 2 || calls[1].signals[0] != syscall.SIGTERM || calls[1].signals[1] != syscall.SIGINT {
		t.Fatalf("unexpected shutdown signals: %+v", calls[1].signals)
	}
}

func TestBuildRuntime_Success(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	var notifyCalls int
	fake := &fakeOTLPPusher{}

	rt, err := buildRuntime(runtimeOptions{
		cacheTTL: 10 * time.Second,
		cacheMax: 50,
		proxyCfg: proxyRuntimeConfig{
			backendURL:               "http://example.com",
			logLevel:                 "info",
			registerInstrumentation:  boolPtr(true),
			labelStyle:               "passthrough",
			metadataFieldMode:        "hybrid",
			metricsMaxTenants:        10,
			metricsMaxClients:        10,
			metricsTrustProxyHeaders: false,
		},
		otlpCfg: otlpRuntimeConfig{
			endpoint:    "http://collector:4318/v1/metrics",
			interval:    5 * time.Second,
			compression: "gzip",
			serviceName: "proxy",
		},
		maxBodyBytes: 1024,
		enableGzip:   true,
		serverOpts: serverRuntimeOptions{
			listenAddr:     ":0",
			readTimeout:    time.Second,
			writeTimeout:   2 * time.Second,
			idleTimeout:    3 * time.Second,
			maxHeaderBytes: 4096,
		},
	}, logger, func(ch chan<- os.Signal, _ ...os.Signal) {
		notifyCalls++
	}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return fake
	})
	if err != nil {
		t.Fatalf("unexpected buildRuntime error: %v", err)
	}
	defer rt.cacheCleanup()
	defer rt.stopOTLP()

	if rt.proxy == nil || rt.server == nil {
		t.Fatalf("expected initialized runtime, got %+v", rt)
	}
	if rt.reloadCh == nil || rt.shutdownCh == nil {
		t.Fatalf("expected signal channels, got %+v", rt)
	}
	if notifyCalls != 2 {
		t.Fatalf("expected 2 signal registrations, got %d", notifyCalls)
	}
	if !fake.started {
		t.Fatal("expected OTLP pusher to be started")
	}
	if !strings.Contains(buf.String(), "proxy listening") && !strings.Contains(buf.String(), "otlp metrics push enabled") {
		t.Fatalf("expected startup logs, got %s", buf.String())
	}
}

func TestBuildRuntime_ProxyConfigError(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	_, err := buildRuntime(runtimeOptions{
		cacheTTL: 10 * time.Second,
		cacheMax: 50,
		proxyCfg: proxyRuntimeConfig{
			backendURL:        "http://example.com",
			labelStyle:        "bad",
			metadataFieldMode: "hybrid",
		},
		serverOpts: serverRuntimeOptions{listenAddr: ":0"},
	}, logger, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return &fakeOTLPPusher{}
	})
	if err == nil || !strings.Contains(err.Error(), "build proxy config") {
		t.Fatalf("expected build proxy config error, got %v", err)
	}
}

func TestBuildRuntime_HTTPServerErrorStopsOTLP(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	fake := &fakeOTLPPusher{}

	_, err := buildRuntime(runtimeOptions{
		cacheTTL: 10 * time.Second,
		cacheMax: 50,
		proxyCfg: proxyRuntimeConfig{
			backendURL:               "http://example.com",
			logLevel:                 "info",
			registerInstrumentation:  boolPtr(true),
			labelStyle:               "passthrough",
			metadataFieldMode:        "hybrid",
			metricsMaxTenants:        10,
			metricsMaxClients:        10,
			metricsTrustProxyHeaders: false,
		},
		otlpCfg: otlpRuntimeConfig{
			endpoint: "http://collector:4318/v1/metrics",
		},
		serverOpts: serverRuntimeOptions{
			listenAddr:           ":0",
			tlsRequireClientCert: true,
		},
	}, logger, func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher {
		return fake
	})
	if err == nil || !strings.Contains(err.Error(), "build http server") {
		t.Fatalf("expected build http server error, got %v", err)
	}
	if !fake.started || !fake.stopped {
		t.Fatalf("expected OTLP pusher start+stop on server build error, got started=%v stopped=%v", fake.started, fake.stopped)
	}
}

func TestHandleShutdown(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	srv := &fakeHTTPServer{}
	shutdownCh := make(chan os.Signal, 1)
	done := make(chan struct{})

	go func() {
		handleShutdown(shutdownCh, srv, time.Second, logger)
		close(done)
	}()

	shutdownCh <- syscall.SIGTERM
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleShutdown did not return")
	}

	if srv.shutdownCalls != 1 {
		t.Fatalf("expected shutdown to be called once, got %d", srv.shutdownCalls)
	}
	logs := buf.String()
	if !strings.Contains(logs, "shutdown requested") || !strings.Contains(logs, "shutdown complete") {
		t.Fatalf("expected shutdown logs, got %s", logs)
	}
}

func TestHandleShutdown_LogsShutdownError(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	srv := &fakeHTTPServer{shutdownErr: errors.New("boom")}
	shutdownCh := make(chan os.Signal, 1)
	done := make(chan struct{})

	go func() {
		handleShutdown(shutdownCh, srv, time.Second, logger)
		close(done)
	}()

	shutdownCh <- syscall.SIGINT
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleShutdown did not return")
	}

	if !strings.Contains(buf.String(), "http shutdown error") {
		t.Fatalf("expected shutdown error log, got %s", buf.String())
	}
}

func TestWatchReloadSignals(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	fake := &fakeReloadableProxy{}
	reloadCh := make(chan os.Signal, 1)
	done := make(chan struct{})

	go func() {
		watchReloadSignals(reloadCh, fake, func(key string) string {
			switch key {
			case "TENANT_MAP":
				return `{"team-a":{"account_id":"1","project_id":"2"}}`
			case "FIELD_MAPPING":
				return `[{"vl_field":"service.name","loki_label":"service_name"}]`
			default:
				return ""
			}
		}, logger)
		close(done)
	}()

	reloadCh <- syscall.SIGHUP
	time.Sleep(50 * time.Millisecond)
	signal.Stop(reloadCh)
	close(reloadCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchReloadSignals did not stop")
	}

	if fake.tenantMap["team-a"] != (proxy.TenantMapping{AccountID: "1", ProjectID: "2"}) {
		t.Fatalf("unexpected tenant map reload: %+v", fake.tenantMap)
	}
	if len(fake.fieldMappings) != 1 || fake.fieldMappings[0].VLField != "service.name" {
		t.Fatalf("unexpected field mapping reload: %+v", fake.fieldMappings)
	}
	if !strings.Contains(buf.String(), "received sighup, reloading configuration") {
		t.Fatalf("expected reload signal log, got %s", buf.String())
	}
}

func TestLogProxyStartup(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	c := cache.New(30*time.Second, 100)
	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "10.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "10.0.0.2:3100",
		Port:          3100,
	})
	cfg := proxy.Config{
		TenantMap:         map[string]proxy.TenantMapping{"team-a": {AccountID: "1", ProjectID: "2"}},
		FieldMappings:     []proxy.FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}},
		LabelStyle:        proxy.LabelStyleUnderscores,
		MetadataFieldMode: proxy.MetadataFieldModeHybrid,
		DerivedFields:     []proxy.DerivedField{{Name: "traceID"}},
		PeerCache:         pc,
	}

	logProxyStartup(logger, cfg, "10.0.0.1:3100", "static", c)

	logs := buf.String()
	for _, want := range []string{
		"loaded tenant mappings",
		"loaded field mappings",
		"label translation enabled",
		"loaded derived fields",
		"peer cache enabled",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("expected log %q in %s", want, logs)
		}
	}
}

func writeTestCA(t *testing.T) string {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test CA key: %v", err)
	}

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "loki-vl-proxy-test-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create test CA certificate: %v", err)
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	path := filepath.Join(t.TempDir(), "client-ca.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("failed to write CA PEM: %v", err)
	}
	return path
}

func boolPtr(v bool) *bool { return &v }
