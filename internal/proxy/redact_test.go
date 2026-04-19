package proxy

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactSecrets_BearerToken(t *testing.T) {
	input := `Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U`
	result := RedactSecrets(input)
	if strings.Contains(result, "eyJhbGci") {
		t.Errorf("bearer token not redacted: %s", result)
	}
	if !strings.Contains(result, "***REDACTED***") {
		t.Errorf("expected redaction marker, got: %s", result)
	}
}

func TestRedactSecrets_APIKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"api_key query param", "query=rate(http_requests_total[5m])&api_key=sk-1234567890abcdef"},
		{"apiKey camelCase", "apiKey=my-super-secret-key-12345"},
		{"api-token", "api-token=ghp_1234567890abcdefghijklmnopqrstuvwxyz"},
		{"access_token", "access_token=ya29.a0ARrdaM-something-long"},
		{"secret_key", "secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
		{"auth_token", "auth_token=xoxb-1234-5678-abcdefgh"},
		{"token param", "token=supersecrettoken123456"},
		{"password param", "password=MyP@ssw0rd!123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RedactSecrets(tt.input)
			// The key name should still be visible, but the value should be redacted
			if !strings.Contains(result, "***REDACTED***") {
				t.Errorf("expected redaction in %q, got: %s", tt.name, result)
			}
		})
	}
}

func TestRedactSecrets_AWSKeys(t *testing.T) {
	input := "AKIA" + "IOSFODNN7EXAMPLE"
	result := RedactSecrets(input)
	if strings.Contains(result, "AKIAIOSFODNN7") {
		t.Errorf("AWS access key not redacted: %s", result)
	}
}

func TestRedactSecrets_URLCredentials(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"basic auth URL", "http://admin:secretpass123@localhost:9428/api/v1/query"},
		{"postgres connection", "postgres://user:p@ssw0rd@db.example.com:5432/mydb"},
		{"redis connection", "redis://default:mysecret@cache.example.com:6379"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RedactSecrets(tt.input)
			if !strings.Contains(result, "***REDACTED***") {
				t.Errorf("URL credentials not redacted: %s", result)
			}
		})
	}
}

func TestRedactSecrets_NoFalsePositives(t *testing.T) {
	safe := []string{
		`{app="nginx"} |= "error"`,
		`rate(http_requests_total{job="api"}[5m])`,
		`sum by (status) (rate(requests[1m]))`,
		`{namespace="prod"} | json | line_format "{{.msg}}"`,
		`count_over_time({app="web"}[24h])`,
		"method=GET status=200 duration_ms=42",
		"tenant=org-123 endpoint=query_range",
	}

	for _, s := range safe {
		result := RedactSecrets(s)
		if strings.Contains(result, "***REDACTED***") {
			t.Errorf("false positive redaction in: %q → %q", s, result)
		}
	}
}

func TestRedactSecrets_MixedContent(t *testing.T) {
	input := `query={app="web"}&api_key=sk-secret123&start=1234567890`
	result := RedactSecrets(input)
	// Query and start should survive
	if !strings.Contains(result, `query={app="web"}`) {
		t.Errorf("query was incorrectly redacted: %s", result)
	}
	if !strings.Contains(result, "start=1234567890") {
		t.Errorf("start was incorrectly redacted: %s", result)
	}
	// api_key value should be redacted
	if strings.Contains(result, "sk-secret123") {
		t.Errorf("api_key value not redacted: %s", result)
	}
}

func TestRedactSecrets_HeaderValues(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"x-api-key header", "x-api-key=abcdef0123456789abcdef0123456789ab"},
		{"x-auth-token header", "x-auth-token=abcdef0123456789abcdef0123456789ab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RedactSecrets(tt.input)
			if !strings.Contains(result, "***REDACTED***") {
				t.Errorf("header value not redacted: %s", result)
			}
		})
	}
}

func TestRedactSecrets_EmptyAndShort(t *testing.T) {
	if RedactSecrets("") != "" {
		t.Error("empty string should remain empty")
	}
	if RedactSecrets("hello") != "hello" {
		t.Error("short safe string should be unchanged")
	}
}

// =============================================================================
// Redacting slog handler
// =============================================================================

func TestRedactingHandler_RedactsAttributes(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(NewRedactingHandler(inner))

	logger.Info("request", "query", `{app="web"}&api_key=sk-secret123`, "status", 200)

	output := buf.String()
	if strings.Contains(output, "sk-secret123") {
		t.Errorf("secret leaked in log output: %s", output)
	}
	if !strings.Contains(output, "***REDACTED***") {
		t.Errorf("expected redaction marker in output: %s", output)
	}
	// Non-secret attributes should be preserved
	if !strings.Contains(output, "200") {
		t.Errorf("status should be preserved: %s", output)
	}
}

func TestRedactingHandler_RedactsSensitiveKeys(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(NewRedactingHandler(inner))

	logger.Info("auth", "password", "super-secret", "authorization", "Bearer xyz")

	output := buf.String()
	if strings.Contains(output, "super-secret") {
		t.Errorf("password value leaked: %s", output)
	}
	if strings.Contains(output, "Bearer xyz") {
		t.Errorf("authorization value leaked: %s", output)
	}
}

func TestRedactingHandler_RedactsMessage(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(NewRedactingHandler(inner))

	logger.Info("failed with token=mysecrettoken123 in request")

	output := buf.String()
	if strings.Contains(output, "mysecrettoken123") {
		t.Errorf("token in message not redacted: %s", output)
	}
}

func TestRedactingHandler_WithAttrs(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(NewRedactingHandler(inner)).With("api_token", "persistent-secret-value")

	logger.Info("test")

	output := buf.String()
	if strings.Contains(output, "persistent-secret-value") {
		t.Errorf("persistent attr secret leaked: %s", output)
	}
}

func TestRedactingHandler_WithGroup(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(NewRedactingHandler(inner)).WithGroup("request")

	logger.Info("incoming", "url", "http://admin:secret@localhost/query")

	output := buf.String()
	if strings.Contains(output, "secret@") {
		t.Errorf("URL password leaked in group: %s", output)
	}
}

func TestRedactingHandler_Enabled(t *testing.T) {
	inner := slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelWarn})
	h := NewRedactingHandler(inner)

	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("debug should be disabled when inner level is warn")
	}
	if !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("warn should be enabled")
	}
}

func TestLevelFilterHandler_WithGroupPreservesMinimumLevel(t *testing.T) {
	inner := slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := (&levelFilterHandler{inner: inner, min: slog.LevelWarn}).WithGroup("request")

	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("info should remain disabled after WithGroup")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("error should remain enabled after WithGroup")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkRedactSecrets_Safe(b *testing.B) {
	s := `{app="nginx"} |= "error" | rate(requests[5m])`
	for b.Loop() {
		RedactSecrets(s)
	}
}

func BenchmarkRedactSecrets_WithSecret(b *testing.B) {
	s := `query={app="web"}&api_key=sk-1234567890abcdef&start=1234567890`
	for b.Loop() {
		RedactSecrets(s)
	}
}

func BenchmarkRedactingHandler(b *testing.B) {
	inner := slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(NewRedactingHandler(inner))
	for b.Loop() {
		logger.Info("request", "query", `rate(http_requests[5m])`, "status", 200)
	}
}
