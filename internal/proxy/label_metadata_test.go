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
// supplies either start or end wins over the default lookback — the proxy never
// silently rewrites a client-supplied bound, and never "half-injects" the
// missing bound (which would change the semantics of a half-open range).
//
// Asymmetric coverage was a concrete regression risk: an earlier draft of this
// test only covered start-only; flipping the guard from `&&` to `||` would have
// passed unnoticed. Both subtests are required to lock the guard.
func TestMetadataDefaultLookback_DoesNotOverrideClient(t *testing.T) {
	t.Run("start only — end stays blank", func(t *testing.T) {
		p := &Proxy{metadataDefaultLookback: time.Hour}
		params, err := p.metadataQueryParams(context.Background(), "*", "12345", "", "", "")
		if err != nil {
			t.Fatalf("metadataQueryParams: %v", err)
		}
		if got := params.Get("start"); got != "12345" {
			t.Fatalf("client start should win, got %q", got)
		}
		if got := params.Get("end"); got != "" {
			t.Fatalf("end must stay blank when only start is supplied, got %q", got)
		}
	})

	t.Run("end only — start stays blank", func(t *testing.T) {
		p := &Proxy{metadataDefaultLookback: time.Hour}
		params, err := p.metadataQueryParams(context.Background(), "*", "", "67890", "", "")
		if err != nil {
			t.Fatalf("metadataQueryParams: %v", err)
		}
		if got := params.Get("end"); got != "67890" {
			t.Fatalf("client end should win, got %q", got)
		}
		if got := params.Get("start"); got != "" {
			t.Fatalf("start must stay blank when only end is supplied, got %q", got)
		}
	})
}

// TestApplyDefaultMetadataLookback exercises the shared helper that both
// metadataQueryParams and handleSeries call to inject the default-lookback
// window. Keeping the guard in one place eliminates drift risk between the
// two near-identical inline blocks that previously existed.
func TestApplyDefaultMetadataLookback(t *testing.T) {
	const lookback = time.Hour

	t.Run("injects when both blank", func(t *testing.T) {
		start, end := applyDefaultMetadataLookback("", "", lookback)
		if start == "" || end == "" {
			t.Fatalf("expected both bounds injected, got start=%q end=%q", start, end)
		}
	})

	t.Run("does not override start-only", func(t *testing.T) {
		start, end := applyDefaultMetadataLookback("12345", "", lookback)
		if start != "12345" {
			t.Fatalf("client start should win, got %q", start)
		}
		if end != "" {
			t.Fatalf("end must stay blank when only start is supplied, got %q", end)
		}
	})

	t.Run("does not override end-only", func(t *testing.T) {
		start, end := applyDefaultMetadataLookback("", "67890", lookback)
		if end != "67890" {
			t.Fatalf("client end should win, got %q", end)
		}
		if start != "" {
			t.Fatalf("start must stay blank when only end is supplied, got %q", start)
		}
	})

	t.Run("does not override both supplied", func(t *testing.T) {
		start, end := applyDefaultMetadataLookback("12345", "67890", lookback)
		if start != "12345" || end != "67890" {
			t.Fatalf("client bounds should win, got start=%q end=%q", start, end)
		}
	})

	t.Run("zero lookback disables injection", func(t *testing.T) {
		start, end := applyDefaultMetadataLookback("", "", 0)
		if start != "" || end != "" {
			t.Fatalf("expected no injection when lookback=0, got start=%q end=%q", start, end)
		}
	})

	t.Run("whitespace-only treated as blank", func(t *testing.T) {
		start, end := applyDefaultMetadataLookback("   ", "\t", lookback)
		if start == "" || end == "" {
			t.Fatalf("whitespace bounds should be treated as blank and trigger injection, got start=%q end=%q", start, end)
		}
	})
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
