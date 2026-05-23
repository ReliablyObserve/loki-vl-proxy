package translator

import (
	"testing"
)

func TestNewDropCondition_EqOp(t *testing.T) {
	dc, err := NewDropCondition("level", "=", "debug")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dc.Field != "level" || dc.Op != "=" || dc.Value != "debug" {
		t.Errorf("got %+v, want {level = debug}", dc)
	}
	// = operator: Matches works without regex
	if !dc.Matches("debug") {
		t.Error("Matches(debug) should be true")
	}
	if dc.Matches("info") {
		t.Error("Matches(info) should be false")
	}
}

func TestNewDropCondition_NeqOp(t *testing.T) {
	dc, err := NewDropCondition("level", "!=", "debug")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dc.Matches("info") {
		t.Error("Matches(info) should be true for !=debug")
	}
	if dc.Matches("debug") {
		t.Error("Matches(debug) should be false for !=debug")
	}
}

func TestNewDropCondition_ReMatchOp(t *testing.T) {
	dc, err := NewDropCondition("level", "=~", "debug|info")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dc.Matches("debug") {
		t.Error("Matches(debug) should be true for =~ debug|info")
	}
	if !dc.Matches("info") {
		t.Error("Matches(info) should be true for =~ debug|info")
	}
	if dc.Matches("error") {
		t.Error("Matches(error) should be false for =~ debug|info")
	}
}

func TestNewDropCondition_ReMatchAnchoredRegex(t *testing.T) {
	// Regex is anchored: "^(?:debug.*)$", so "debug_extra" should match but "xdebug" should not.
	dc, err := NewDropCondition("level", "=~", "debug.*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dc.Matches("debug") {
		t.Error("Matches(debug) should be true")
	}
	if !dc.Matches("debugx") {
		t.Error("Matches(debugx) should be true")
	}
	if dc.Matches("xdebug") {
		t.Error("Matches(xdebug) should be false — regex anchored at start")
	}
}

func TestNewDropCondition_ReNotMatchOp(t *testing.T) {
	dc, err := NewDropCondition("level", "!~", "debug.*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dc.Matches("debug") {
		t.Error("Matches(debug) should be false for !~ debug.*")
	}
	if dc.Matches("debugx") {
		t.Error("Matches(debugx) should be false for !~ debug.*")
	}
	if !dc.Matches("error") {
		t.Error("Matches(error) should be true for !~ debug.*")
	}
}

func TestNewDropCondition_InvalidRegex(t *testing.T) {
	_, err := NewDropCondition("level", "=~", "[invalid")
	if err == nil {
		t.Error("expected error for invalid regex, got nil")
	}
}

func TestNewDropCondition_InvalidRegex_ReNotMatch(t *testing.T) {
	_, err := NewDropCondition("level", "!~", "[invalid")
	if err == nil {
		t.Error("expected error for invalid regex, got nil")
	}
}

func TestNewDropCondition_NoRegexForEq(t *testing.T) {
	// = and != operators must not attempt to compile regex
	dc, err := NewDropCondition("status", "=", "not-a-regex-[[[")
	if err != nil {
		t.Fatalf("= operator should not compile regex, but got error: %v", err)
	}
	if !dc.Matches("not-a-regex-[[[") {
		t.Error("Matches should work for = operator even with non-regex value")
	}
}

func TestNewDropCondition_NoRegexForNeq(t *testing.T) {
	dc, err := NewDropCondition("status", "!=", "not-a-regex-[[[")
	if err != nil {
		t.Fatalf("!= operator should not compile regex, but got error: %v", err)
	}
	if dc.Matches("not-a-regex-[[[") {
		t.Error("Matches should be false for matching != value")
	}
}

func TestNewDropCondition_EmptyValue(t *testing.T) {
	dc, err := NewDropCondition("level", "=", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dc.Matches("") {
		t.Error("empty string should match empty value")
	}
	if dc.Matches("debug") {
		t.Error("non-empty should not match empty value")
	}
}

func TestNewDropCondition_EmptyRegex(t *testing.T) {
	dc, err := NewDropCondition("level", "=~", "")
	if err != nil {
		t.Fatalf("unexpected error for empty regex: %v", err)
	}
	// ^(?:)$ matches only empty string
	if !dc.Matches("") {
		t.Error("empty regex should match empty string")
	}
	if dc.Matches("x") {
		t.Error("empty regex ^(?:)$ should not match non-empty")
	}
}

func TestNewDropCondition_AllOpsTable(t *testing.T) {
	tests := []struct {
		field, op, value string
		match, noMatch   string
		wantErr          bool
	}{
		{"f", "=", "v", "v", "w", false},
		{"f", "!=", "v", "w", "v", false},
		{"f", "=~", "a|b", "a", "c", false},
		{"f", "!~", "a|b", "c", "a", false},
		{"f", "=~", "[bad", "", "", true},
		{"f", "!~", "[bad", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			dc, err := NewDropCondition(tt.field, tt.op, tt.value)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.match != "" && !dc.Matches(tt.match) {
				t.Errorf("Matches(%q) should be true", tt.match)
			}
			if tt.noMatch != "" && dc.Matches(tt.noMatch) {
				t.Errorf("Matches(%q) should be false", tt.noMatch)
			}
		})
	}
}
