package proxy

import (
	"net/http/httptest"
	"testing"
)

// Loki's text error body for a valid message (every error the proxy writes but a quoted invalid byte) and for one
// that quotes invalid bytes.
func BenchmarkWriteLokiErrorBody(b *testing.B) {
	for _, bc := range []struct{ name, msg string }{
		{"valid", "parse error : stage '|~ \"a(\"' : error parsing regexp: missing closing ): `a(`"},
		{"invalid_utf8", "parse error : stage '|~ \"a\\xff\\xfeb\"' : error parsing regexp: invalid UTF-8: `\xff\xfeb`"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				writeLokiErrorBody(httptest.NewRecorder(), 400, bc.msg)
			}
		})
	}
}
