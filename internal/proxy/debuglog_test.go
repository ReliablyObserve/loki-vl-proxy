package proxy

import (
	"strings"
	"testing"
)

func TestRedactQuery_DefaultRedacts(t *testing.T) {
	got := redactQuery("{job=\"nginx\"} |= \"customer-1234\"", false)
	if !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("expected sha256 prefix, got %q", got)
	}
	if !strings.Contains(got, "len=") {
		t.Fatalf("expected len= field, got %q", got)
	}
	if strings.Contains(got, "customer-1234") {
		t.Fatalf("literal leaked into redacted output: %q", got)
	}
}

func TestRedactQuery_RawPassthrough(t *testing.T) {
	in := "{job=\"nginx\"} |= \"customer-1234\""
	if got := redactQuery(in, true); got != in {
		t.Fatalf("raw flag should return input verbatim, got %q", got)
	}
}

func TestRedactQuery_StableForSameInput(t *testing.T) {
	a := redactQuery("foo", false)
	b := redactQuery("foo", false)
	if a != b {
		t.Fatalf("expected stable hash, got %q vs %q", a, b)
	}
}

func TestRedactQuery_DifferentInputsDifferentHash(t *testing.T) {
	a := redactQuery("foo", false)
	b := redactQuery("foo ", false)
	if a == b {
		t.Fatalf("expected different hashes for different inputs, both %q", a)
	}
}

func TestRedactQuery_EmptyInput(t *testing.T) {
	got := redactQuery("", false)
	if !strings.Contains(got, "len=0") {
		t.Fatalf("expected len=0 for empty input, got %q", got)
	}
}
