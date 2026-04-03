package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
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
		"team-alpha":  {AccountID: "100", ProjectID: "1"},
		"team-beta":   {AccountID: "200", ProjectID: "2"},
		"ops-prod":    {AccountID: "300", ProjectID: "0"},
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

func TestTenant_UnmappedStringDefaultsToZero(t *testing.T) {
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
		TenantMap:  map[string]TenantMapping{"known": {AccountID: "1", ProjectID: "0"}},
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "unknown-tenant")
	p.handleLabels(w, r)

	if receivedAccountID != "0" {
		t.Errorf("expected AccountID=0 for unknown tenant, got %q", receivedAccountID)
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
