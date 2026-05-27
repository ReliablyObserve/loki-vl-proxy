package proxy

import (
	"context"
	"time"
)

// effectiveMaxQueryLength returns the maximum allowed query time range for orgID.
// Precedence (highest first): per-tenant tenantLimits → tenantDefaultLimits → defaultMaxQueryLength flag.
// Returns 0 when unlimited.
func (p *Proxy) effectiveMaxQueryLength(orgID string) time.Duration {
	p.configMu.RLock()
	var tenantOverride string
	if p.tenantLimits != nil {
		if m, ok := p.tenantLimits[orgID]; ok {
			tenantOverride, _ = m["max_query_length"].(string)
		}
	}
	var defaultOverride string
	if p.tenantDefaultLimits != nil {
		defaultOverride, _ = p.tenantDefaultLimits["max_query_length"].(string)
	}
	p.configMu.RUnlock()

	if tenantOverride != "" {
		if d := parseLokiDuration(tenantOverride); d > 0 {
			return d
		}
	}
	if defaultOverride != "" {
		if d := parseLokiDuration(defaultOverride); d > 0 {
			return d
		}
	}
	return p.defaultMaxQueryLength
}

// checkQueryRangeLength returns an error string if the requested range exceeds
// the enforced max query length for orgID. Returns "" when within limit or unlimited.
func (p *Proxy) checkQueryRangeLength(ctx context.Context, startNs, endNs int64) string {
	maxLen := p.effectiveMaxQueryLength(getOrgID(ctx))
	if maxLen == 0 {
		return ""
	}
	rangeNs := endNs - startNs
	if rangeNs <= 0 {
		return ""
	}
	rangeDur := time.Duration(rangeNs)
	if rangeDur > maxLen {
		return "query length " + rangeDur.String() + " exceeds limit " + maxLen.String()
	}
	return ""
}
