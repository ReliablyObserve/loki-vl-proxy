package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLogger_EmitsOTelFriendlyJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, LoggerConfig{
		Level:                 "debug",
		ServiceName:           "test-proxy",
		ServiceNamespace:      "edge",
		ServiceVersion:        "1.2.3",
		ServiceInstanceID:     "proxy-1",
		DeploymentEnvironment: "prod",
	})

	logger.Info("request finished", "http.response.status_code", 200, "endpoint", "query_range")

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("invalid json log: %v", err)
	}

	if payload["body"] != "request finished" {
		t.Fatalf("expected body field, got %#v", payload["body"])
	}
	if payload["service.name"] != "test-proxy" {
		t.Fatalf("expected service.name, got %#v", payload["service.name"])
	}
	if payload["service.namespace"] != "edge" {
		t.Fatalf("expected service.namespace, got %#v", payload["service.namespace"])
	}
	if payload["service.version"] != "1.2.3" {
		t.Fatalf("expected service.version, got %#v", payload["service.version"])
	}
	if payload["service.instance.id"] != "proxy-1" {
		t.Fatalf("expected service.instance.id, got %#v", payload["service.instance.id"])
	}
	if payload["deployment.environment.name"] != "prod" {
		t.Fatalf("expected deployment.environment.name, got %#v", payload["deployment.environment.name"])
	}
	severity, ok := payload["severity"].(map[string]any)
	if !ok {
		t.Fatalf("expected grouped severity field, got %#v", payload["severity"])
	}
	if severity["text"] != "INFO" {
		t.Fatalf("expected severity text INFO, got %#v", severity["text"])
	}
	if severity["number"] != float64(9) {
		t.Fatalf("expected severity number 9, got %#v", severity["number"])
	}
	if payload["timestamp"] == nil {
		t.Fatal("expected timestamp field")
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"info":    slog.LevelInfo,
		"other":   slog.LevelInfo,
	}
	for in, want := range cases {
		if got := parseLevel(in); got != want {
			t.Fatalf("parseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestSeverityNumber(t *testing.T) {
	if severityNumber(slog.LevelDebug) != 5 {
		t.Fatal("expected debug severity number 5")
	}
	if severityNumber(slog.LevelInfo) != 9 {
		t.Fatal("expected info severity number 9")
	}
	if severityNumber(slog.LevelWarn) != 13 {
		t.Fatal("expected warn severity number 13")
	}
	if severityNumber(slog.LevelError) != 17 {
		t.Fatal("expected error severity number 17")
	}
}

func TestValueOrDefault(t *testing.T) {
	if got := valueOrDefault(" custom ", "fallback"); got != "custom" {
		t.Fatalf("unexpected trimmed value: %q", got)
	}
	if got := valueOrDefault("   ", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback, got %q", got)
	}
}

func TestDefaultInstanceID(t *testing.T) {
	got := defaultInstanceID()
	if strings.TrimSpace(got) == "" {
		t.Fatal("expected non-empty instance id")
	}
	if !strings.Contains(got, "-") {
		t.Fatalf("expected host-pid shaped instance id, got %q", got)
	}
}
