package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProxyPolicyHelpers_ValidateTenantHeader(t *testing.T) {
	t.Run("missing_header_rejected_when_auth_enabled", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		p.authEnabled = true
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query", nil)

		err := p.validateTenantHeader(req)
		if err == nil {
			t.Fatal("expected missing tenant header to be rejected")
		}
		rpe, ok := err.(*requestPolicyError)
		if !ok || rpe.status != http.StatusUnauthorized {
			t.Fatalf("expected unauthorized policy error, got %#v", err)
		}
	})

	t.Run("multi_tenant_header_rejected_on_non_query_path", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/tail", nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")

		err := p.validateTenantHeader(req)
		if err == nil {
			t.Fatal("expected non-query multi-tenant header to be rejected")
		}
		rpe := err.(*requestPolicyError)
		if rpe.status != http.StatusBadRequest {
			t.Fatalf("expected bad request, got %#v", err)
		}
	})

	t.Run("multi_tenant_header_requires_two_distinct_ids", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query", nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a | tenant-a")

		err := p.validateTenantHeader(req)
		if err == nil {
			t.Fatal("expected duplicate-only multi-tenant header to be rejected")
		}
		rpe := err.(*requestPolicyError)
		if rpe.status != http.StatusBadRequest {
			t.Fatalf("expected bad request, got %#v", err)
		}
	})

	t.Run("wildcard_inside_multi_tenant_header_is_rejected", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		p.ReloadTenantMap(map[string]TenantMapping{
			"tenant-a": {AccountID: "1", ProjectID: "0"},
		})
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a|*")

		err := p.validateTenantHeader(req)
		if err == nil {
			t.Fatal("expected wildcard multi-tenant header to be rejected")
		}
		rpe := err.(*requestPolicyError)
		if rpe.status != http.StatusBadRequest {
			t.Fatalf("expected bad request, got %#v", err)
		}
	})

	t.Run("known_multi_tenant_header_is_allowed", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		p.ReloadTenantMap(map[string]TenantMapping{
			"tenant-a": {AccountID: "1", ProjectID: "0"},
			"tenant-b": {AccountID: "2", ProjectID: "0"},
		})
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")

		if err := p.validateTenantHeader(req); err != nil {
			t.Fatalf("expected mapped multi-tenant header to be allowed, got %v", err)
		}
	})

	t.Run("wildcard_single_tenant_header_requires_explicit_opt_in", func(t *testing.T) {
		p := newTestProxy(t, "http://unused")
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query", nil)
		req.Header.Set("X-Scope-OrgID", "*")

		err := p.validateTenantHeader(req)
		if err == nil {
			t.Fatal("expected wildcard tenant header to be rejected without explicit opt-in")
		}
		rpe := err.(*requestPolicyError)
		if rpe.status != http.StatusForbidden {
			t.Fatalf("expected forbidden, got %#v", err)
		}
	})
}

func TestProxyPolicyHelpers_SplitMultiTenantOrgIDs(t *testing.T) {
	got := splitMultiTenantOrgIDs(" tenant-a | tenant-b | tenant-a || tenant-c ")
	want := []string{"tenant-a", "tenant-b", "tenant-c"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestProxyPolicyHelpers_TailOriginPolicy(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	if !p.isAllowedTailOrigin("") {
		t.Fatal("expected empty origin to be allowed for non-browser clients")
	}

	p.tailAllowedOrigins = map[string]struct{}{
		"https://grafana.example": {},
	}
	if !p.isAllowedTailOrigin("https://grafana.example") {
		t.Fatal("expected exact origin match to be allowed")
	}
	if p.isAllowedTailOrigin("https://other.example") {
		t.Fatal("expected unknown origin to be rejected")
	}

	p.tailAllowedOrigins["*"] = struct{}{}
	if !p.isAllowedTailOrigin("https://other.example") {
		t.Fatal("expected wildcard origin allowlist to allow any browser origin")
	}
}

func TestProxyPolicyHelpers_SplitLeadingSelector(t *testing.T) {
	selector, rest, ok := splitLeadingSelector(`{app="api", msg="brace } and \\\"quote\\\""} |= "error"`)
	if !ok {
		t.Fatal("expected leading selector to be parsed")
	}
	if selector != `{app="api", msg="brace } and \\\"quote\\\""}` {
		t.Fatalf("unexpected selector %q", selector)
	}
	if rest != `|= "error"` {
		t.Fatalf("unexpected rest %q", rest)
	}

	if _, _, ok := splitLeadingSelector(`sum(rate({app="api"}[5m]))`); ok {
		t.Fatal("expected query without leading selector to be rejected")
	}
	if _, _, ok := splitLeadingSelector(`{app="api"`); ok {
		t.Fatal("expected unmatched selector to be rejected")
	}
}

func TestProxyPolicyHelpers_FindMatchingBraceLocal(t *testing.T) {
	if got := findMatchingBraceLocal(`{app="api", msg="brace } in string"}`); got != len(`{app="api", msg="brace } in string"}`)-1 {
		t.Fatalf("unexpected matching brace index %d", got)
	}
	if got := findMatchingBraceLocal(`{app="api", msg="unterminated"`); got != -1 {
		t.Fatalf("expected unmatched brace to return -1, got %d", got)
	}
}

func TestProxyPolicyHelpers_NewSyntheticTailSeen_DefaultLimit(t *testing.T) {
	seen := newSyntheticTailSeen(0)
	if seen.limit != maxSyntheticTailSeenEntries {
		t.Fatalf("expected zero limit to default to %d, got %d", maxSyntheticTailSeenEntries, seen.limit)
	}
}
