package translator

import (
	"strings"
	"testing"
)

// =============================================================================
// bool modifier on comparison operators
// =============================================================================

func TestBoolModifier_Preserved(t *testing.T) {
	// "rate(...) > bool 0" should translate and preserve the bool marker
	// so the proxy's binary evaluation returns 1/0 instead of filtering
	tests := []struct {
		name  string
		logql string
	}{
		{"greater_bool", `rate({app="nginx"}[5m]) > bool 0`},
		{"equal_bool", `rate({app="nginx"}[5m]) == bool 0`},
		{"less_bool", `rate({app="nginx"}[5m]) < bool 100`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Should produce a binary expression (not fail)
			if !strings.HasPrefix(result, "__binary__:") {
				t.Errorf("expected binary expression, got %q", result)
			}
		})
	}
}

// =============================================================================
// offset modifier
// =============================================================================

func TestOffsetModifier_Recognized(t *testing.T) {
	tests := []struct {
		name    string
		logql   string
		wantErr bool
	}{
		{
			name:    "rate with offset",
			logql:   `rate({app="nginx"}[5m] offset 1h)`,
			wantErr: false,
		},
		{
			name:    "count with offset",
			logql:   `count_over_time({app="nginx"}[5m] offset 30m)`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TranslateLogQL(tt.logql)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err == nil && result == "" {
				t.Error("expected non-empty result")
			}
		})
	}
}

// =============================================================================
// unwrap duration()/bytes() conversion
// =============================================================================

func TestUnwrapConversion_DurationStripped(t *testing.T) {
	// unwrap duration(field) — the "duration" wrapper is stripped, field name extracted
	logql := `avg_over_time({app="nginx"} | unwrap duration(response_time) [5m])`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should use the field name directly
	if !strings.Contains(result, "response_time") {
		t.Errorf("expected field name 'response_time' in result, got %q", result)
	}
}

func TestUnwrapConversion_BytesStripped(t *testing.T) {
	logql := `sum_over_time({app="nginx"} | unwrap bytes(body_size) [5m])`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "body_size") {
		t.Errorf("expected field name 'body_size' in result, got %q", result)
	}
}

// =============================================================================
// field-specific JSON parser: | json field1, field2
// =============================================================================

func TestFieldSpecificJSON_TranslatesToUnpackJSON(t *testing.T) {
	// | json field1, field2 should still translate to | unpack_json
	// (VL extracts all fields; field-specific extraction is a client optimization)
	logql := `{app="nginx"} | json status, duration`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "unpack_json") {
		t.Errorf("expected 'unpack_json' for field-specific json, got %q", result)
	}
}

func TestFieldSpecificLogfmt_TranslatesToUnpackLogfmt(t *testing.T) {
	logql := `{app="nginx"} | logfmt status, method`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "unpack_logfmt") {
		t.Errorf("expected 'unpack_logfmt' for field-specific logfmt, got %q", result)
	}
}

// =============================================================================
// backslash-escaped quotes in stream selectors
// =============================================================================

func TestBackslashQuotes_InStreamSelector(t *testing.T) {
	// Label value with escaped quote: {msg="foo\"bar"}
	logql := `{msg="foo\"bar"}`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should handle the escaped quote in the value
	if result == "" {
		t.Error("expected non-empty result")
	}
}

func TestBackslashQuotes_InFilter(t *testing.T) {
	// Line filter with escaped quote
	logql := `{app="nginx"} |= "path=\"/api\""`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result")
	}
}
