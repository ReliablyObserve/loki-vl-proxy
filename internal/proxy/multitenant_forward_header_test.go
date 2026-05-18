package proxy

import (
	"context"
	"net/http"
	"testing"
)

func makeProxyForForwardHeaderTest(enabled bool) *Proxy {
	return &Proxy{
		forwardTenantHeader: enabled,
	}
}

// TestForwardTenantHeader_Enabled verifies that a non-alias, non-numeric orgID
// (which falls through all early-returns) gets X-Scope-OrgID forwarded when enabled.
func TestForwardTenantHeader_Enabled(t *testing.T) {
	p := makeProxyForForwardHeaderTest(true)
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("X-Scope-OrgID", "prod-team-eu_staging")
	req = withOrgID(req)
	// Clear the header so we test only what forwardTenantHeaders sets
	req.Header.Del("X-Scope-OrgID")

	p.forwardTenantHeaders(req)

	got := req.Header.Get("X-Scope-OrgID")
	if got != "prod-team-eu_staging" {
		t.Errorf("X-Scope-OrgID = %q, want %q", got, "prod-team-eu_staging")
	}
}

// TestForwardTenantHeader_Disabled verifies that X-Scope-OrgID is NOT forwarded when flag is false.
func TestForwardTenantHeader_Disabled(t *testing.T) {
	p := makeProxyForForwardHeaderTest(false)
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("X-Scope-OrgID", "prod-team-eu_staging")
	req = withOrgID(req)
	req.Header.Del("X-Scope-OrgID")

	p.forwardTenantHeaders(req)

	got := req.Header.Get("X-Scope-OrgID")
	if got != "" {
		t.Errorf("X-Scope-OrgID = %q, want empty (flag disabled)", got)
	}
}

// TestForwardTenantHeader_WithTenantMap verifies that the tenant map path sets
// AccountID/ProjectID. The tenant map branch returns early, so X-Scope-OrgID
// forwarding does not apply (tenant map overrides with numeric VL coordinates).
func TestForwardTenantHeader_WithTenantMap(t *testing.T) {
	p := makeProxyForForwardHeaderTest(true)
	p.tenantMap = map[string]TenantMapping{
		"prod-team-eu_staging": {AccountID: "42", ProjectID: "3"},
	}
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("X-Scope-OrgID", "prod-team-eu_staging")
	req = withOrgID(req)
	req.Header.Del("X-Scope-OrgID")

	p.forwardTenantHeaders(req)

	if got := req.Header.Get("AccountID"); got != "42" {
		t.Errorf("AccountID = %q, want %q", got, "42")
	}
	if got := req.Header.Get("ProjectID"); got != "3" {
		t.Errorf("ProjectID = %q, want %q", got, "3")
	}
	// Tenant map path returns early; X-Scope-OrgID is NOT forwarded by this path.
	if got := req.Header.Get("X-Scope-OrgID"); got != "" {
		t.Errorf("X-Scope-OrgID = %q, want empty (tenant map returns early)", got)
	}
}

// TestForwardTenantHeader_DefaultAliasSkipped verifies that default alias orgIDs
// ("fake", "0", "default") trigger early return before X-Scope-OrgID forwarding.
func TestForwardTenantHeader_DefaultAliasSkipped(t *testing.T) {
	p := makeProxyForForwardHeaderTest(true)
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("X-Scope-OrgID", "fake")
	req = withOrgID(req)
	req.Header.Del("X-Scope-OrgID")

	p.forwardTenantHeaders(req)

	got := req.Header.Get("X-Scope-OrgID")
	if got != "" {
		t.Errorf("X-Scope-OrgID = %q, want empty (default alias returns early)", got)
	}
}

// TestForwardTenantHeader_EmptyOrgIDSkipped verifies that requests with no orgID
// in context (no X-Scope-OrgID header) do not set the header on the upstream request.
func TestForwardTenantHeader_EmptyOrgIDSkipped(t *testing.T) {
	p := makeProxyForForwardHeaderTest(true)
	req, _ := http.NewRequest("GET", "/", nil)
	req = req.WithContext(context.Background())

	p.forwardTenantHeaders(req)

	got := req.Header.Get("X-Scope-OrgID")
	if got != "" {
		t.Errorf("X-Scope-OrgID = %q, want empty (no orgID in context)", got)
	}
}
