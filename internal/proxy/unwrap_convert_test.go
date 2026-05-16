package proxy

import (
	"math"
	"testing"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		want  float64
		ok    bool
	}{
		{"100ms", 0.1, true},
		{"1.5s", 1.5, true},
		{"2m", 120, true},
		{"1h", 3600, true},
		{"1d", 86400, true},
		{"100ns", 0.0000001, true},
		{"500us", 0.0005, true},
		{"1h30m", 5400, true}, // compound
		{"2m30s", 150, true},  // compound
		{"42", 42, true},      // plain number = seconds
		{"3.14", 3.14, true},  // float = seconds
		{"", 0, false},
		{"abc", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := parseDuration(tt.input)
			if ok != tt.ok {
				t.Fatalf("parseDuration(%q) ok=%v, want ok=%v", tt.input, ok, tt.ok)
			}
			if ok && math.Abs(got-tt.want) > 0.001 {
				t.Errorf("parseDuration(%q) = %f, want %f", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseBytes(t *testing.T) {
	tests := []struct {
		input string
		want  float64
		ok    bool
	}{
		{"1024", 1024, true}, // plain number = bytes
		{"1B", 1, true},
		{"1KB", 1000, true},
		{"1KiB", 1024, true},
		{"1.5KiB", 1536, true},
		{"100MB", 1e8, true},
		{"1MiB", 1048576, true},
		{"1GB", 1e9, true},
		{"1GiB", 1073741824, true},
		{"1TB", 1e12, true},
		{"", 0, false},
		{"abc", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := parseBytes(tt.input)
			if ok != tt.ok {
				t.Fatalf("parseBytes(%q) ok=%v, want ok=%v", tt.input, ok, tt.ok)
			}
			if ok && math.Abs(got-tt.want) > 1 {
				t.Errorf("parseBytes(%q) = %f, want %f", tt.input, got, tt.want)
			}
		})
	}
}
