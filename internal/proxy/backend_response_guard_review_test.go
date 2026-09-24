package proxy

import (
	"context"
	"errors"
	"io"
	"testing"
)

// A body read that failed on the request deadline stays visible to
// errors.Is, so retry and graceful-timeout paths keyed on
// context.DeadlineExceeded still take it.
//
// conformance: semantics/backend-aborted-response
func TestVLResponseAbortedError_UnwrapsCause(t *testing.T) {
	err := error(&vlResponseAbortedError{timedOut: true, cause: context.DeadlineExceeded})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("aborted error hides context.DeadlineExceeded")
	}
	if !errors.Is(&vlResponseAbortedError{cause: io.ErrUnexpectedEOF}, io.ErrUnexpectedEOF) {
		t.Fatal("aborted error hides its transport cause")
	}
	if statusFromUpstreamErr(err) != 504 {
		t.Fatalf("status = %d, want 504", statusFromUpstreamErr(err))
	}
}

// Coalesced VictoriaLogs reads with different response caps never share a
// flight: a capped label values request must not receive an uncapped
// background refresh's body, nor fail an uncapped waiter with its limit.
//
// conformance: limits/label-values-response-cap
func TestResponseCapKeySuffix_SeparatesCaps(t *testing.T) {
	plain := responseCapKeySuffix(context.Background())
	capped := responseCapKeySuffix(withLabelValuesResponseCap(context.Background(), 1024))
	other := responseCapKeySuffix(withLabelValuesResponseCap(context.Background(), 2048))
	if plain != "" || capped == "" || capped == other {
		t.Fatalf("cap key suffixes not distinct: %q %q %q", plain, capped, other)
	}
}
