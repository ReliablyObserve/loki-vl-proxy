package main

import "testing"

func TestResponseCompressionHelpers(t *testing.T) {
	t.Run("resolve response compression", func(t *testing.T) {
		tests := []struct {
			name          string
			explicit      string
			legacyEnabled bool
			want          string
			wantErr       bool
		}{
			{name: "legacy auto", explicit: "", legacyEnabled: true, want: "auto"},
			{name: "legacy disabled none", explicit: "", legacyEnabled: false, want: "none"},
			{name: "explicit gzip", explicit: " gzip ", want: "gzip"},
			{name: "explicit none", explicit: "none", want: "none"},
			{name: "explicit zstd aliases to gzip", explicit: "zstd", want: "gzip"},
			{name: "invalid explicit", explicit: "brotli", wantErr: true},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := resolveResponseCompression(tc.explicit, tc.legacyEnabled)
				if tc.wantErr {
					if err == nil {
						t.Fatal("expected error")
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got != tc.want {
					t.Fatalf("resolveResponseCompression(%q, %v) = %q, want %q", tc.explicit, tc.legacyEnabled, got, tc.want)
				}
			})
		}
	})

	t.Run("normalize frontend compression setting", func(t *testing.T) {
		tests := []struct {
			input   string
			want    string
			wantErr bool
		}{
			{input: "", want: "auto"},
			{input: "AUTO", want: "auto"},
			{input: "none", want: "none"},
			{input: "gzip", want: "gzip"},
			{input: "zstd", want: "gzip"},
			{input: "brotli", wantErr: true},
		}
		for _, tc := range tests {
			got, err := normalizeFrontendCompressionSetting(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				continue
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("normalizeFrontendCompressionSetting(%q) = %q, want %q", tc.input, got, tc.want)
			}
		}
	})

	t.Run("normalize backend compression setting", func(t *testing.T) {
		tests := []struct {
			input   string
			want    string
			wantErr bool
		}{
			{input: "", want: "auto"},
			{input: "auto", want: "auto"},
			{input: "none", want: "none"},
			{input: "gzip", want: "gzip"},
			{input: "zstd", want: "zstd"},
			{input: "brotli", wantErr: true},
		}
		for _, tc := range tests {
			got, err := normalizeCompressionSetting(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				continue
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("normalizeCompressionSetting(%q) = %q, want %q", tc.input, got, tc.want)
			}
		}
	})
}

func TestIsLoopbackListenAddr_AdditionalCoverage(t *testing.T) {
	tests := []struct {
		addr string
		want bool
	}{
		{addr: "localhost:3100", want: true},
		{addr: "127.0.0.1:3100", want: true},
		{addr: "[::1]:3100", want: true},
		{addr: "0.0.0.0:3100", want: false},
		{addr: "[2001:db8::1]:3100", want: false},
		{addr: "example.com:3100", want: false},
		{addr: "", want: false},
		{addr: " :3100 ", want: false},
	}

	for _, tc := range tests {
		if got := isLoopbackListenAddr(tc.addr); got != tc.want {
			t.Fatalf("isLoopbackListenAddr(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}
