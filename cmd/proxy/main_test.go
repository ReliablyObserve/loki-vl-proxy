package main

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
	"github.com/szibis/Loki-VL-proxy/internal/proxy"
)

type fakeReloadableProxy struct {
	tenantMap     map[string]proxy.TenantMapping
	fieldMappings []proxy.FieldMapping
}

func (f *fakeReloadableProxy) ReloadTenantMap(m map[string]proxy.TenantMapping) {
	f.tenantMap = m
}

func (f *fakeReloadableProxy) ReloadFieldMappings(m []proxy.FieldMapping) {
	f.fieldMappings = m
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
