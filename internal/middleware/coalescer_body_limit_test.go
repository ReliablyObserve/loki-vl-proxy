package middleware

import (
	"strings"
	"testing"
)

type limitedBody struct {
	*strings.Reader
	limit int64
}

func (b limitedBody) BodyLimit() int64 { return b.limit }

// A reader carrying an operator-configured cap above the coalescer default
// raises the coalescer's read limit, so the default never hides it; a smaller
// cap leaves the default in place (the reader enforces its own).
// conformance: limits/label-values-response-cap
func TestBodyReadLimitHonoursLargerConfiguredCap(t *testing.T) {
	if got := bodyReadLimit(strings.NewReader("x")); got != defaultBodyReadLimit {
		t.Fatalf("plain reader limit = %d, want %d", got, defaultBodyReadLimit)
	}
	if got := bodyReadLimit(limitedBody{strings.NewReader("x"), 1 << 30}); got != 1<<30 {
		t.Fatalf("larger cap = %d, want %d", got, 1<<30)
	}
	if got := bodyReadLimit(limitedBody{strings.NewReader("x"), 4096}); got != defaultBodyReadLimit {
		t.Fatalf("smaller cap = %d, want the default %d", got, defaultBodyReadLimit)
	}
}
