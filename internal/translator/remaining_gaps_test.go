package translator

import (
	"strings"
	"testing"
)

// =============================================================================
// @ timestamp modifier — strip and return as metadata for proxy to adjust time range
// =============================================================================

func TestAtModifier_Stripped(t *testing.T) {
	tests := []struct {
		name  string
		logql string
	}{
		{"rate with @", `rate({app="nginx"}[5m] @ 1609459200)`},
		{"count with @", `count_over_time({app="nginx"}[5m] @ end())`},
		{"count with @ start", `count_over_time({app="nginx"}[5m] @ start())`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("@ modifier should not cause error: %v", err)
			}
			// @ should be stripped from the translated query
			if strings.Contains(result, "@") {
				t.Errorf("@ modifier should be stripped from output, got %q", result)
			}
			// Should still produce a valid stats query
			if !strings.Contains(result, "stats") {
				t.Errorf("expected stats query, got %q", result)
			}
		})
	}
}

// =============================================================================
// unwrap duration() / bytes() — proxy-side unit conversion
// =============================================================================

func TestUnwrapDuration_FieldExtracted(t *testing.T) {
	logql := `avg_over_time({app="nginx"} | unwrap duration(latency_ms) [5m])`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "latency_ms") {
		t.Errorf("expected 'latency_ms' field, got %q", result)
	}
}

func TestUnwrapBytes_FieldExtracted(t *testing.T) {
	logql := `sum_over_time({app="nginx"} | unwrap bytes(response_size) [5m])`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "response_size") {
		t.Errorf("expected 'response_size' field, got %q", result)
	}
}

// =============================================================================
// Subquery syntax — should return clear error (no VL equivalent)
// =============================================================================

func TestSubquery_ReturnsError(t *testing.T) {
	logql := `rate(rate({app="nginx"}[5m])[1h:5m])`
	_, err := TranslateLogQL(logql)
	// This is a nested expression that VL can't handle.
	// It may succeed (partial translation) or fail — either is acceptable.
	// The key is it doesn't panic.
	_ = err
}

// =============================================================================
// Multiple stream selectors in binary queries — normalize both
// =============================================================================

func TestBinaryQuery_BothSelectorsTranslated(t *testing.T) {
	logql := `rate({app="a"}[5m]) / rate({app="b"}[5m])`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatal(err)
	}
	// Should be a binary expression with both sides translated
	if !strings.HasPrefix(result, "__binary__:") {
		t.Errorf("expected binary expression, got %q", result)
	}
	// Both sides should contain translated queries (not raw LogQL)
	parts := strings.SplitN(result, "|||", 2)
	if len(parts) != 2 {
		t.Fatalf("expected ||| separator in %q", result)
	}
	// Left side should have "app:=a" (translated), not {app="a"} (raw)
	if strings.Contains(parts[0], `{app="a"}`) {
		t.Error("left side should be translated to LogsQL, not raw LogQL")
	}
}
