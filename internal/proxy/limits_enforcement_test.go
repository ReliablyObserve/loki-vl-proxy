package proxy

import (
	"testing"
	"time"
)

func TestEffectiveMaxQueryLength_ZeroDefault(t *testing.T) {
	p := &Proxy{defaultMaxQueryLength: 0}
	got := p.effectiveMaxQueryLength("tenant-a")
	if got != 0 {
		t.Errorf("want 0 (unlimited), got %v", got)
	}
}

func TestEffectiveMaxQueryLength_FlagDefault(t *testing.T) {
	p := &Proxy{defaultMaxQueryLength: 7 * 24 * time.Hour}
	p.tenantLimits = map[string]map[string]any{}
	p.tenantDefaultLimits = map[string]any{}
	got := p.effectiveMaxQueryLength("tenant-a")
	if got != 7*24*time.Hour {
		t.Errorf("want 168h, got %v", got)
	}
}

func TestEffectiveMaxQueryLength_TenantOverride(t *testing.T) {
	p := &Proxy{defaultMaxQueryLength: 7 * 24 * time.Hour}
	p.tenantLimits = map[string]map[string]any{
		"tenant-a": {"max_query_length": "1h"},
	}
	p.tenantDefaultLimits = map[string]any{}
	got := p.effectiveMaxQueryLength("tenant-a")
	if got != time.Hour {
		t.Errorf("want 1h, got %v", got)
	}
}

func TestEffectiveMaxQueryLength_DefaultLimitOverride(t *testing.T) {
	p := &Proxy{defaultMaxQueryLength: 0}
	p.tenantLimits = map[string]map[string]any{}
	p.tenantDefaultLimits = map[string]any{"max_query_length": "24h"}
	got := p.effectiveMaxQueryLength("")
	if got != 24*time.Hour {
		t.Errorf("want 24h, got %v", got)
	}
}
