package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
)

// Tests that ALL handlers forward tenant headers to VL.
// Critical bug: 7 handlers were missing withOrgID(r), causing cross-tenant data exposure.

func newTenantTestProxy(t *testing.T, backendURL string) *Proxy {
	t.Helper()
	c := cache.New(1*time.Second, 1000) // short TTL for cache tests
	p, err := New(Config{
		BackendURL: backendURL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"team-a": {AccountID: "100", ProjectID: "1"},
			"team-b": {AccountID: "200", ProjectID: "2"},
		},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

// testTenantForwarded is a helper that verifies a handler forwards X-Scope-OrgID as AccountID.
func testTenantForwarded(t *testing.T, handler func(*Proxy) http.HandlerFunc, path string) {
	t.Helper()
	var receivedAccountID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		// Return valid VL response for any endpoint
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []interface{}{},
			"hits":   []interface{}{},
		})
	}))
	defer vlBackend.Close()

	p := newTenantTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("X-Scope-OrgID", "team-a")
	handler(p)(w, r)

	if receivedAccountID != "100" {
		t.Errorf("%s: expected AccountID=100 for team-a, got %q", path, receivedAccountID)
	}
}

func TestTenantScoping_HandleSeries(t *testing.T) {
	testTenantForwarded(t, func(p *Proxy) http.HandlerFunc { return p.handleSeries },
		`/loki/api/v1/series?match[]={app="nginx"}&start=1&end=2`)
}

func TestTenantScoping_HandleIndexStats(t *testing.T) {
	testTenantForwarded(t, func(p *Proxy) http.HandlerFunc { return p.handleIndexStats },
		`/loki/api/v1/index/stats?query={app="nginx"}&start=1&end=2`)
}

func TestTenantScoping_HandleVolume(t *testing.T) {
	testTenantForwarded(t, func(p *Proxy) http.HandlerFunc { return p.handleVolume },
		`/loki/api/v1/index/volume?query={app="nginx"}&start=1&end=2`)
}

func TestTenantScoping_HandleVolumeRange(t *testing.T) {
	testTenantForwarded(t, func(p *Proxy) http.HandlerFunc { return p.handleVolumeRange },
		`/loki/api/v1/index/volume_range?query={app="nginx"}&start=1&end=2&step=60`)
}

func TestTenantScoping_HandleDetectedFields(t *testing.T) {
	testTenantForwarded(t, func(p *Proxy) http.HandlerFunc { return p.handleDetectedFields },
		`/loki/api/v1/detected_fields?query={app="nginx"}&start=1&end=2`)
}

func TestTenantScoping_HandleDetectedFieldValues(t *testing.T) {
	testTenantForwarded(t, func(p *Proxy) http.HandlerFunc { return p.handleDetectedFieldValues },
		`/loki/api/v1/detected_field/level/values?query={app="nginx"}&start=1&end=2`)
}

func TestTenantScoping_HandlePatterns(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("AccountID"); got != "11" {
			t.Fatalf("expected AccountID 11, got %q", got)
		}
		if got := r.Header.Get("ProjectID"); got != "22" {
			t.Fatalf("expected ProjectID 22, got %q", got)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"nginx","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	p.tenantMap = map[string]TenantMapping{
		"team-a": {AccountID: "11", ProjectID: "22"},
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/patterns?query={app="nginx"}&start=1&end=2`, nil)
	r.Header.Set("X-Scope-OrgID", "team-a")
	p.handlePatterns(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected patterns endpoint to preserve tenant scoping, got %d", w.Code)
	}
}

// Test that cache keys include tenant — different tenants must not share cached labels.
func TestTenantScoping_CacheIsolation_Labels(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		accountID := r.Header.Get("AccountID")
		// Return different labels per tenant
		if accountID == "100" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "team_a_label", "hits": 1},
				},
			})
		} else {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "team_b_label", "hits": 1},
				},
			})
		}
	}))
	defer vlBackend.Close()

	p := newTenantTestProxy(t, vlBackend.URL)

	// Request labels for team-a
	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r1.Header.Set("X-Scope-OrgID", "team-a")
	p.handleLabels(w1, r1)

	var resp1 map[string]interface{}
	json.Unmarshal(w1.Body.Bytes(), &resp1)

	// Request labels for team-b (should NOT get team-a's cached response)
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r2.Header.Set("X-Scope-OrgID", "team-b")
	p.handleLabels(w2, r2)

	var resp2 map[string]interface{}
	json.Unmarshal(w2.Body.Bytes(), &resp2)

	// Backend must have been called twice (once per tenant)
	if callCount < 2 {
		t.Errorf("expected 2 backend calls (one per tenant), got %d — cache leaked across tenants", callCount)
	}

	// Verify different responses
	data1, _ := resp1["data"].([]interface{})
	data2, _ := resp2["data"].([]interface{})

	if len(data1) > 0 && len(data2) > 0 {
		if data1[0] == data2[0] {
			t.Errorf("tenant A and B got same labels — cache not isolated: A=%v B=%v", data1, data2)
		}
	}
}

func TestTenantScoping_CacheIsolation_LabelValues(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		accountID := r.Header.Get("AccountID")
		if accountID == "100" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{{"value": "prod", "hits": 1}},
			})
		} else {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{{"value": "staging", "hits": 1}},
			})
		}
	}))
	defer vlBackend.Close()

	p := newTenantTestProxy(t, vlBackend.URL)

	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest("GET", "/loki/api/v1/label/env/values", nil)
	r1.Header.Set("X-Scope-OrgID", "team-a")
	p.handleLabelValues(w1, r1)

	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/loki/api/v1/label/env/values", nil)
	r2.Header.Set("X-Scope-OrgID", "team-b")
	p.handleLabelValues(w2, r2)

	if callCount < 2 {
		t.Errorf("expected 2 backend calls for label values, got %d — cache leaked", callCount)
	}
}
