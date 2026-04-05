package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/proxy"
)

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

func TestApplyEnvOverrides(t *testing.T) {
	cfg := envConfig{
		listenAddr:        ":3100",
		backendURL:        "http://backend",
		otlpCompression:   "none",
		labelStyle:        "passthrough",
		metadataFieldMode: "hybrid",
	}
	env := map[string]string{
		"LISTEN_ADDR":              ":9999",
		"VL_BACKEND_URL":           "http://other",
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
