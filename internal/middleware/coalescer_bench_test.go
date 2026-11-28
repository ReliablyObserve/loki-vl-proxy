package middleware

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// BenchmarkCoalescerCall is one shared call with no other caller: the flight the coalescer builds for every call.
func BenchmarkCoalescerCall(b *testing.B) {
	c := NewCoalescer()
	fn := func(context.Context) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("answer"))}, nil
	}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, err := c.DoWithGuardCtx(ctx, "key", allow, fn); err != nil {
			b.Fatal(err)
		}
	}
}
