package proxy

import (
	"regexp"
	"strings"
	"testing"
)

// sanitizeLabelNameRef is the original regex-based implementation kept here as
// a reference for parity testing. It must never be called in production.
var sanitizeLabelNameRefRE = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func sanitizeLabelNameRef(name string) string {
	sanitized := sanitizeLabelNameRefRE.ReplaceAllString(name, "_")
	for strings.Contains(sanitized, "__") {
		sanitized = strings.ReplaceAll(sanitized, "__", "_")
	}
	sanitized = strings.TrimRight(sanitized, "_")
	if len(sanitized) == 0 {
		return "key_empty"
	}
	if sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "key_" + sanitized
	}
	return sanitized
}

var sanitizeParityInputs = []string{
	"",
	"a",
	"abc",
	"service.name",
	"k8s.pod.uid",
	"a.b.c",
	"a..b",
	"a...b",
	"__leading",
	"trailing__",
	"middle__underscore",
	"123abc",
	"1",
	"___",
	"a___b___c",
	"a!@#$%^b",
	"valid_name_already",
	strings.Repeat(".", 50),
	strings.Repeat("a.b", 20),
	"a" + strings.Repeat(".", 100) + "b",
	"über",
	"café",
	"http.request.method",
	"deployment.environment.name",
	"telemetry.sdk.version",
}

func TestOPT4_SanitizeLabelName_ParityWithReference(t *testing.T) {
	for _, tc := range sanitizeParityInputs {
		want := sanitizeLabelNameRef(tc)
		got := SanitizeLabelName(tc)
		if got != want {
			t.Errorf("SanitizeLabelName(%q): got %q, want %q", tc, got, want)
		}
	}
}

func BenchmarkOPT4_SanitizeLabelName_OnePass(b *testing.B) {
	inputs := []string{"service.name", "k8s.pod.uid", "a.b.c.d.e.f.g", "valid_name_already"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, n := range inputs {
			_ = SanitizeLabelName(n)
		}
	}
}

func BenchmarkOPT4_SanitizeLabelName_Reference(b *testing.B) {
	inputs := []string{"service.name", "k8s.pod.uid", "a.b.c.d.e.f.g", "valid_name_already"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, n := range inputs {
			_ = sanitizeLabelNameRef(n)
		}
	}
}

// BenchmarkOPT4_SanitizeLabelName_Pathological guards against O(n²) regression.
// The old loop was quadratic on strings with many consecutive invalid chars.
// Threshold: must stay under 5000 ns/op on any modern hardware.
func BenchmarkOPT4_SanitizeLabelName_Pathological(b *testing.B) {
	name := strings.Repeat(".", 200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SanitizeLabelName(name)
	}
}
