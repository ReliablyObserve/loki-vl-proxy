package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// Tenant mapping tests — string org IDs to VL numeric AccountID
// =============================================================================

func TestTenant_StringMapping(t *testing.T) {
	var receivedAccountID, receivedProjectID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		receivedProjectID = r.Header.Get("ProjectID")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	tenantMap := map[string]TenantMapping{
		"team-alpha": {AccountID: "100", ProjectID: "1"},
		"team-beta":  {AccountID: "200", ProjectID: "2"},
		"ops-prod":   {AccountID: "300", ProjectID: "0"},
	}

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap:  tenantMap,
	})

	// Test mapped string tenant
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "team-alpha")
	p.handleLabels(w, r)

	if receivedAccountID != "100" {
		t.Errorf("expected AccountID=100, got %q", receivedAccountID)
	}
	if receivedProjectID != "1" {
		t.Errorf("expected ProjectID=1, got %q", receivedProjectID)
	}
}

func TestTenant_UnmappedStringRejected(t *testing.T) {
	var backendCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalled = true
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap:  map[string]TenantMapping{"known": {AccountID: "1", ProjectID: "0"}},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "unknown-tenant")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for unknown tenant, got %d body=%s", w.Code, w.Body.String())
	}
	if backendCalled {
		t.Fatal("backend should not be called for unknown tenant")
	}
}

func TestTenant_NumericPassthrough(t *testing.T) {
	var receivedAccountID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "42")
	p.handleLabels(w, r)

	if receivedAccountID != "42" {
		t.Errorf("expected AccountID=42 for numeric org, got %q", receivedAccountID)
	}
}

func TestTenant_NoHeader(t *testing.T) {
	var receivedAccountID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	p.handleLabels(w, r)

	if receivedAccountID != "" {
		t.Errorf("expected no AccountID when no OrgID, got %q", receivedAccountID)
	}
}

func TestTenant_AuthEnabledRequiresHeader(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("backend should not be called when tenant header is required")
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL:  vlBackend.URL,
		Cache:       c,
		LogLevel:    "error",
		AuthEnabled: true,
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when auth.enabled=true and X-Scope-OrgID is missing, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestTenant_DrilldownLimitsDoesNotRequireHeaderWhenAuthEnabled(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("backend should not be called for drilldown-limits")
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL:  vlBackend.URL,
		Cache:       c,
		LogLevel:    "error",
		AuthEnabled: true,
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for drilldown-limits without X-Scope-OrgID when auth.enabled=true, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestTenant_GlobalBypassDisabledWhenMappingsConfigured(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("backend should not be called when global tenant bypass is disabled")
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap:  map[string]TenantMapping{"known": {AccountID: "1", ProjectID: "0"}},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "*")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for wildcard tenant bypass, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestTenant_DefaultTenantAliasesUseBackendDefaultWithoutMappings(t *testing.T) {
	tests := []struct {
		name  string
		orgID string
	}{
		{name: "zero", orgID: "0"},
		{name: "fake", orgID: "fake"},
		{name: "default", orgID: "default"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var receivedAccountID, receivedProjectID string
			vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedAccountID = r.Header.Get("AccountID")
				receivedProjectID = r.Header.Get("ProjectID")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"values": []map[string]interface{}{},
				})
			}))
			defer vlBackend.Close()

			c := cache.New(60*time.Second, 1000)
			p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
			r.Header.Set("X-Scope-OrgID", tc.orgID)
			mux.ServeHTTP(w, r)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 for OrgID=%q without tenant mappings, got %d body=%s", tc.orgID, w.Code, w.Body.String())
			}
			if receivedAccountID != "" || receivedProjectID != "" {
				t.Fatalf("expected OrgID=%q to use backend default tenant, got AccountID=%q ProjectID=%q", tc.orgID, receivedAccountID, receivedProjectID)
			}
		})
	}
}

func TestTenant_WildcardGlobalBypassRequiresOptInWithoutMappings(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("backend should not be called when wildcard tenant bypass is disabled")
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "*")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for wildcard org without explicit opt-in, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestTenant_DefaultTenantAliasesAllowedWhenMappingsConfigured(t *testing.T) {
	tests := []struct {
		name  string
		orgID string
	}{
		{name: "zero", orgID: "0"},
		{name: "fake", orgID: "fake"},
		{name: "default", orgID: "default"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var receivedAccountID, receivedProjectID string
			vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedAccountID = r.Header.Get("AccountID")
				receivedProjectID = r.Header.Get("ProjectID")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"values": []map[string]interface{}{},
				})
			}))
			defer vlBackend.Close()

			c := cache.New(60*time.Second, 1000)
			p, _ := New(Config{
				BackendURL: vlBackend.URL,
				Cache:      c,
				LogLevel:   "error",
				TenantMap:  map[string]TenantMapping{"known": {AccountID: "1", ProjectID: "0"}},
			})

			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
			r.Header.Set("X-Scope-OrgID", tc.orgID)
			mux.ServeHTTP(w, r)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 for OrgID=%q when using default-tenant alias, got %d body=%s", tc.orgID, w.Code, w.Body.String())
			}
			if receivedAccountID != "" || receivedProjectID != "" {
				t.Fatalf("expected OrgID=%q to use backend default tenant, got AccountID=%q ProjectID=%q", tc.orgID, receivedAccountID, receivedProjectID)
			}
		})
	}
}

func TestTenant_WildcardGlobalBypassRequiresOptInWhenMappingsConfigured(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("backend should not be called when tenant mappings are configured and global bypass is disabled")
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap:  map[string]TenantMapping{"known": {AccountID: "1", ProjectID: "0"}},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "*")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for wildcard org when tenant mappings are configured, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestTenant_WildcardGlobalBypassAllowedWhenMappingsConfiguredAndOptedIn(t *testing.T) {
	var receivedAccountID, receivedProjectID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		receivedProjectID = r.Header.Get("ProjectID")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             c,
		LogLevel:          "error",
		TenantMap:         map[string]TenantMapping{"known": {AccountID: "1", ProjectID: "0"}},
		AllowGlobalTenant: true,
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "*")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for wildcard org when global bypass is enabled, got %d body=%s", w.Code, w.Body.String())
	}
	if receivedAccountID != "" || receivedProjectID != "" {
		t.Fatalf("expected wildcard org to use backend default tenant, got AccountID=%q ProjectID=%q", receivedAccountID, receivedProjectID)
	}
}

func TestTenant_ExplicitMappingOverridesDefaultTenantAlias(t *testing.T) {
	var receivedAccountID, receivedProjectID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		receivedProjectID = r.Header.Get("ProjectID")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"0":    {AccountID: "10", ProjectID: "20"},
			"fake": {AccountID: "11", ProjectID: "21"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	for _, tc := range []struct {
		orgID     string
		accountID string
		projectID string
	}{
		{orgID: "0", accountID: "10", projectID: "20"},
		{orgID: "fake", accountID: "11", projectID: "21"},
	} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
		r.Header.Set("X-Scope-OrgID", tc.orgID)
		mux.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 for OrgID=%q, got %d body=%s", tc.orgID, w.Code, w.Body.String())
		}
		if receivedAccountID != tc.accountID || receivedProjectID != tc.projectID {
			t.Fatalf("expected OrgID=%q to use explicit mapping %s/%s, got %s/%s", tc.orgID, tc.accountID, tc.projectID, receivedAccountID, receivedProjectID)
		}
	}
}

func TestTenant_MultiTenantQueryAllowedOnLabels(t *testing.T) {
	var seenAccountIDs []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAccountIDs = append(seenAccountIDs, r.Header.Get("AccountID"))
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": "app", "hits": 10},
			},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for multi-tenant labels query, got %d body=%s", w.Code, w.Body.String())
	}
	if len(seenAccountIDs) != 2 {
		t.Fatalf("expected two tenant fanout backend calls, got %v", seenAccountIDs)
	}
	if !strings.Contains(w.Body.String(), "__tenant_id__") {
		t.Fatalf("expected synthetic __tenant_id__ label in multi-tenant labels response, got %s", w.Body.String())
	}
}

func TestTenant_MultiTenantQueryRangeInjectsTenantLabel(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("AccountID") {
		case "10":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"line-a","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
		case "20":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:01Z","_msg":"line-b","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
		default:
			t.Fatalf("unexpected AccountID %q", r.Header.Get("AccountID"))
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api"}&start=1&end=2&limit=10`, nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for multi-tenant query_range, got %d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Data struct {
			Result []struct {
				Stream map[string]string `json:"stream"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("expected two stream results after multi-tenant fanout, got %d body=%s", len(resp.Data.Result), w.Body.String())
	}
	seenTenants := map[string]struct{}{}
	for _, item := range resp.Data.Result {
		seenTenants[item.Stream["__tenant_id__"]] = struct{}{}
	}
	if len(seenTenants) != 2 {
		t.Fatalf("expected synthetic __tenant_id__ labels for both tenants, got %v", seenTenants)
	}
}

func TestTenant_MultiTenantQueryRangeFiltersByTenantSelector(t *testing.T) {
	var seenAccountIDs []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAccountIDs = append(seenAccountIDs, r.Header.Get("AccountID"))
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"line","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api",__tenant_id__="tenant-b"}&start=1&end=2&limit=10`, nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for filtered multi-tenant query_range, got %d body=%s", w.Code, w.Body.String())
	}
	if len(seenAccountIDs) != 1 || seenAccountIDs[0] != "20" {
		t.Fatalf("expected tenant filter to narrow fanout to tenant-b only, got %v", seenAccountIDs)
	}
	if strings.Contains(w.Body.String(), "tenant-a") {
		t.Fatalf("expected tenant-a to be absent after __tenant_id__ filtering, got %s", w.Body.String())
	}
}

func TestTenant_MultiTenantQueryRangeSortsStreamsAcrossTenants(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		switch r.Header.Get("AccountID") {
		case "10":
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"older","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
		case "20":
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:05Z","_msg":"newer","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
		default:
			t.Fatalf("unexpected AccountID %q", r.Header.Get("AccountID"))
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api"}&start=1&end=2&limit=10`, nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	mux.ServeHTTP(w, r)

	var resp struct {
		Data struct {
			Result []struct {
				Values [][]string        `json:"values"`
				Stream map[string]string `json:"stream"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("expected two merged streams, got %d body=%s", len(resp.Data.Result), w.Body.String())
	}
	if got := resp.Data.Result[0].Stream["__tenant_id__"]; got != "tenant-b" {
		t.Fatalf("expected newest tenant-b stream first after sort, got %q", got)
	}
}

func TestTenant_MultiTenantLabelsUsesMergedCache(t *testing.T) {
	var calls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": "app", "hits": 10},
			},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
		r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
		}
	}

	if calls != 2 {
		t.Fatalf("expected first request to fan out twice and second request to hit merged cache, got %d backend calls", calls)
	}
}

func TestTenant_MultiTenantHeaderRejectedForTail(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("backend should not be called for multi-tenant header values")
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/tail", nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for multi-tenant X-Scope-OrgID, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "query endpoints") {
		t.Fatalf("expected multi-tenant rejection message, got %s", w.Body.String())
	}
}

func TestTenant_MultiTenantQueryRangeRejectsExcessiveTenantCount(t *testing.T) {
	tenantMap := make(map[string]TenantMapping, maxMultiTenantFanout+1)
	tenants := make([]string, 0, maxMultiTenantFanout+1)
	for i := 0; i < maxMultiTenantFanout+1; i++ {
		tenantID := fmt.Sprintf("tenant-%02d", i)
		tenantMap[tenantID] = TenantMapping{AccountID: fmt.Sprintf("%d", i+1), ProjectID: "0"}
		tenants = append(tenants, tenantID)
	}

	p, _ := New(Config{
		BackendURL: "http://example.com",
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
		TenantMap:  tenantMap,
	})
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="api"}&start=1&end=2&limit=10`, nil)
	r.Header.Set("X-Scope-OrgID", strings.Join(tenants, "|"))
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized query_range fanout, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "fanout exceeds limit") {
		t.Fatalf("expected fanout limit message, got %s", w.Body.String())
	}
}

func TestTenant_MultiTenantDetectedFieldsUsesExactValueUnion(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"method=GET status=200","_stream":"{app=\"api\",cluster=\"c1\"}","app":"api","cluster":"c1"}` + "\n"))
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:01Z","_msg":"method=POST status=500","_stream":"{app=\"api\",cluster=\"c1\"}","app":"api","cluster":"c1"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/detected_fields?query={app="api"}&start=1&end=2&limit=50`, nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for multi-tenant detected_fields, got %d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Fields []struct {
			Label       string `json:"label"`
			Cardinality int    `json:"cardinality"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	got := map[string]int{}
	for _, field := range resp.Fields {
		got[field.Label] = field.Cardinality
	}
	if got["method"] != 2 {
		t.Fatalf("expected merged method cardinality to stay exact at 2, got %d body=%s", got["method"], w.Body.String())
	}
	if got["status"] != 2 {
		t.Fatalf("expected merged status cardinality to stay exact at 2, got %d body=%s", got["status"], w.Body.String())
	}
}

func TestTenant_MultiTenantDetectedFieldsKeepsNativeCardinalityFloor(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/select/logsql/field_names"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"native.only","hits":1}]}`))
		case strings.HasPrefix(r.URL.Path, "/select/logsql/query"):
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(""))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/detected_fields?query={app="api"}&start=1&end=2&limit=50`, nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for multi-tenant detected_fields, got %d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Fields []struct {
			Label       string `json:"label"`
			Cardinality int    `json:"cardinality"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	foundNativeOnly := false
	for _, field := range resp.Fields {
		if field.Label != "native.only" {
			continue
		}
		foundNativeOnly = true
		if field.Cardinality != 1 {
			t.Fatalf("expected native.only cardinality floor=1, got %d body=%s", field.Cardinality, w.Body.String())
		}
	}
	if !foundNativeOnly {
		t.Fatalf("expected native.only field in detected_fields response, got %v", resp.Fields)
	}
}

func TestTenant_MultiTenantDetectedLabelsUsesExactValueUnion(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"line","_stream":"{app=\"api\",cluster=\"us-east-1\",namespace=\"prod\"}","app":"api","cluster":"us-east-1","namespace":"prod"}` + "\n"))
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:01Z","_msg":"line","_stream":"{app=\"api\",cluster=\"us-east-1\",namespace=\"prod\"}","app":"api","cluster":"us-east-1","namespace":"prod"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, _ := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "10", ProjectID: "0"},
			"tenant-b": {AccountID: "20", ProjectID: "0"},
		},
	})

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/detected_labels?query={app="api"}&start=1&end=2&limit=50`, nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for multi-tenant detected_labels, got %d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		DetectedLabels []struct {
			Label       string `json:"label"`
			Cardinality int    `json:"cardinality"`
		} `json:"detectedLabels"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	got := map[string]int{}
	for _, field := range resp.DetectedLabels {
		got[field.Label] = field.Cardinality
	}
	if got["cluster"] != 1 {
		t.Fatalf("expected merged cluster cardinality to stay exact at 1, got %d body=%s", got["cluster"], w.Body.String())
	}
	if got["namespace"] != 1 {
		t.Fatalf("expected merged namespace cardinality to stay exact at 1, got %d body=%s", got["namespace"], w.Body.String())
	}
	if got["__tenant_id__"] != 2 {
		t.Fatalf("expected synthetic __tenant_id__ cardinality 2, got %d body=%s", got["__tenant_id__"], w.Body.String())
	}
}

func TestTenant_HasMultiTenantOrgID(t *testing.T) {
	cases := []struct {
		value string
		want  bool
	}{
		{value: "", want: false},
		{value: "tenant-a", want: false},
		{value: "tenant-a|tenant-b", want: true},
		{value: "tenant-a | tenant-b", want: true},
	}
	for _, tc := range cases {
		if got := hasMultiTenantOrgID(tc.value); got != tc.want {
			t.Fatalf("hasMultiTenantOrgID(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestTenant_QueryRangeCacheKeyUsesRawQueryWhenAvailable(t *testing.T) {
	p, _ := New(Config{BackendURL: "http://example.com", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?end=2&query={app="nginx"}&start=1&step=1`, nil)
	r.Header.Set("X-Scope-OrgID", "tenant-a")

	got := p.queryRangeCacheKey(r, `{app="nginx"}`)
	want := `query_range:tenant-a:end=2&query={app="nginx"}&start=1&step=1:default_2tuple`
	if got != want {
		t.Fatalf("queryRangeCacheKey() = %q, want %q", got, want)
	}
}
