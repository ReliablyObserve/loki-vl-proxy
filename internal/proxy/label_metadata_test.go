package proxy

import (
	"context"
	"testing"
	"time"
)

// TestMetadataDefaultLookback_InjectsWhenBlank asserts that when the client
// supplies neither start nor end, metadataQueryParams injects a default window
// of now-metadataDefaultLookback..now so /labels, /label/{name}/values, and
// /series do not trigger a full-retention VL scan.
func TestMetadataDefaultLookback_InjectsWhenBlank(t *testing.T) {
	p := &Proxy{metadataDefaultLookback: time.Hour}
	params, err := p.metadataQueryParams(context.Background(), "*", "", "", "", "")
	if err != nil {
		t.Fatalf("metadataQueryParams: %v", err)
	}
	if params.Get("start") == "" {
		t.Fatalf("expected start to be injected, got empty")
	}
	if params.Get("end") == "" {
		t.Fatalf("expected end to be injected, got empty")
	}
}

// TestMetadataDefaultLookback_DoesNotOverrideClient asserts that a client that
// supplies start (or end) wins over the default lookback — the proxy never
// silently rewrites a client-supplied bound.
func TestMetadataDefaultLookback_DoesNotOverrideClient(t *testing.T) {
	p := &Proxy{metadataDefaultLookback: time.Hour}
	params, err := p.metadataQueryParams(context.Background(), "*", "12345", "", "", "")
	if err != nil {
		t.Fatalf("metadataQueryParams: %v", err)
	}
	if got := params.Get("start"); got != "12345" {
		t.Fatalf("client start should win, got %q", got)
	}
}

// TestMetadataDefaultLookback_ZeroDisables asserts that setting the lookback to
// 0 restores prior behavior — no injection, unbounded scan.
func TestMetadataDefaultLookback_ZeroDisables(t *testing.T) {
	p := &Proxy{metadataDefaultLookback: 0}
	params, err := p.metadataQueryParams(context.Background(), "*", "", "", "", "")
	if err != nil {
		t.Fatalf("metadataQueryParams: %v", err)
	}
	if params.Get("start") != "" || params.Get("end") != "" {
		t.Fatalf("expected no injection when lookback=0, got start=%q end=%q",
			params.Get("start"), params.Get("end"))
	}
}
