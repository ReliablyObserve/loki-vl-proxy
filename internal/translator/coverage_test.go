package translator

import "testing"

// Coverage gap: extractQuotedValue edge cases
func TestCoverage_ExtractQuotedValue(t *testing.T) {
	tests := []struct {
		input     string
		wantVal   string
		wantRest  string
	}{
		{`"hello" world`, `"hello"`, "world"},
		{`unquoted rest`, `"unquoted"`, "rest"},
		{`single`, `"single"`, ""},
		{`word | pipe`, `"word"`, "| pipe"},
		{`  "spaced"  `, `"spaced"`, ""},
	}
	for _, tc := range tests {
		val, rest := extractQuotedValue(tc.input)
		if val != tc.wantVal {
			t.Errorf("extractQuotedValue(%q) val = %q, want %q", tc.input, val, tc.wantVal)
		}
		if rest != tc.wantRest {
			t.Errorf("extractQuotedValue(%q) rest = %q, want %q", tc.input, rest, tc.wantRest)
		}
	}
}

// Coverage gap: extractUnwrapField edge cases
func TestCoverage_ExtractUnwrapField(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"no unwrap here", ""},
		{"| unwrap duration", "duration"},
		{"| unwrap duration | other", "duration"},
		{"| unwrap bytes_total [5m]", "bytes_total"},
		{"| unwrap  spaced_field ", "spaced_field"},
	}
	for _, tc := range tests {
		got := extractUnwrapField(tc.input)
		if got != tc.want {
			t.Errorf("extractUnwrapField(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// Coverage gap: translateBareFilter
func TestCoverage_TranslateBareFilter(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"hello", `"hello"`},
		{"  spaced  ", `"spaced"`},
	}
	for _, tc := range tests {
		got := translateBareFilter(tc.input)
		if got != tc.want {
			t.Errorf("translateBareFilter(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// Coverage gap: addByClause when no stats pipe exists
func TestCoverage_AddByClause_NoStats(t *testing.T) {
	got := addByClause("app:=nginx", "app")
	if got != "app:=nginx | stats by (app)" {
		t.Errorf("got %q", got)
	}
}
