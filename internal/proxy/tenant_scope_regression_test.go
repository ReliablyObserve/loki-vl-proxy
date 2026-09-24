package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTenantScoping_NamespaceTracksReloadAndCopiesBackendConfig(t *testing.T) {
	headers := map[string]string{"Authorization": "Bearer original"}
	p, err := New(Config{BackendURL: "http://unused", BackendHeaders: headers, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	r := p.withRequestScope(httptest.NewRequest("GET", "/", nil))
	before := p.fingerprintFromCtx(r.Context(), r)
	headers["Authorization"] = "Bearer changed"
	if p.backendHeaders["Authorization"] != "Bearer original" {
		t.Fatal("caller mutated immutable backend configuration")
	}
	p.ReloadTenantMap(map[string]TenantMapping{"a": {AccountID: "1", ProjectID: "0"}})
	if p.forwardedAuthFingerprint(httptest.NewRequest("GET", "/", nil)) == before {
		t.Fatal("reload did not replace cached namespace")
	}
	if p.forwardedAuthFingerprint(r) != before {
		t.Fatal("reload changed admitted request scope")
	}
}

func TestTenantScoping_ReloadDuringInflightAndBackgroundWork(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		account := r.Header.Get("AccountID")
		if account == "10" {
			close(started)
			<-release
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"values": []map[string]any{{"value": "account_" + account, "hits": 1}}})
	}))
	defer backend.Close()
	p := newCompatTestProxy(t, backend.URL)
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "team-a")
	r = p.withRequestScope(r)
	oldKey := p.canonicalReadCacheKey("labels", "team-a", r)
	saved := p.snapshotForwardedAuth(r)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- doCompatProxyRequest(p, r.URL.String(), map[string]string{"X-Scope-OrgID": "team-a"}) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("old request did not start")
	}
	p.ReloadTenantMap(map[string]TenantMapping{"team-a": {AccountID: "20", ProjectID: "0"}})
	fresh := doCompatProxyRequest(p, r.URL.String(), map[string]string{"X-Scope-OrgID": "team-a"})
	close(release)
	old := <-done
	if !strings.Contains(old.Body.String(), "account_10") || !strings.Contains(fresh.Body.String(), "account_20") {
		t.Fatalf("old=%s new=%s", old.Body, fresh.Body)
	}
	if p.canonicalReadCacheKey("labels", "team-a", r) != oldKey {
		t.Fatal("in-flight cache namespace changed")
	}
	latest := doCompatProxyRequest(p, r.URL.String(), map[string]string{"X-Scope-OrgID": "team-a"})
	if strings.Contains(latest.Body.String(), "account_10") || !strings.Contains(latest.Body.String(), "account_20") {
		t.Fatalf("late write contaminated new mapping: %s", latest.Body)
	}
	ctx := context.WithValue(context.Background(), origRequestKey, saved)
	ctx = context.WithValue(ctx, orgIDKey, "team-a")
	upstream := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	p.forwardTenantHeaders(upstream)
	if upstream.Header.Get("AccountID") != "10" {
		t.Fatal("background work lost routing snapshot")
	}
}

func TestTenantScoping_TrustedIdentityCacheCollision(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"values": []map[string]any{{"value": fmt.Sprintf(`{owner=%q}`, r.Header.Get("X-Grafana-User")), "hits": 1}}})
	}))
	defer backend.Close()
	p := newCompatTestProxy(t, backend.URL)
	p.metricsTrustProxyHeaders = true
	a := doCompatProxyRequest(p, "/loki/api/v1/series?match[]=*", map[string]string{"X-Scope-OrgID": "team-a", "X-Grafana-User": "alice"})
	b := doCompatProxyRequest(p, "/loki/api/v1/series?match[]=*", map[string]string{"X-Scope-OrgID": "team-a", "X-Grafana-User": "bob"})
	if a.Code != 200 || b.Code != 200 || !strings.Contains(b.Body.String(), "bob") || strings.Contains(b.Body.String(), "alice") {
		t.Fatalf("isolation regression: %d %s / %d %s", a.Code, a.Body, b.Code, b.Body)
	}
}

func TestTenantScoping_TenantReloadRetainsOldLabels(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"values": []map[string]any{{"value": "account_" + r.Header.Get("AccountID"), "hits": 1}}})
	}))
	defer backend.Close()
	p := newCompatTestProxy(t, backend.URL)
	headers := map[string]string{"X-Scope-OrgID": "team-a"}
	a := doCompatProxyRequest(p, "/loki/api/v1/labels", headers)
	p.ReloadTenantMap(map[string]TenantMapping{"team-a": {AccountID: "20", ProjectID: "0"}})
	b := doCompatProxyRequest(p, "/loki/api/v1/labels", headers)
	if a.Code != 200 || b.Code != 200 || !strings.Contains(b.Body.String(), "account_20") || strings.Contains(b.Body.String(), "account_10") {
		t.Fatalf("isolation regression: %s / %s", a.Body, b.Body)
	}
}

func TestTenantScoping_ColdRoutingDropsTenant(t *testing.T) {
	seen := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("X-Scope-OrgID") + "/" + r.Header.Get("AccountID") + "/" + r.Header.Get("ProjectID")
		fmt.Fprintln(w, `{"_time":"2026-01-01T00:00:01Z","_msg":"cold-default-tenant-secret","_stream":"{app=\"a\"}"}`)
	}))
	defer backend.Close()
	p := newTestProxy(t, "http://unused")
	cr, err := NewColdRouter(ColdBackendConfig{Enabled: true, URL: backend.URL}, p.log)
	if err != nil {
		t.Fatal(err)
	}
	p.coldRouter = cr
	p.tenantMap = map[string]TenantMapping{"team-a": {AccountID: "10", ProjectID: "20"}}
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?direction=forward&start=1767225600&end=1767225660", nil)
	r.Header.Set("X-Scope-OrgID", "team-a")
	w := httptest.NewRecorder()
	p.proxyLogQueryCold(w, p.withRequestScope(r), `{app="a"}`)
	got := <-seen
	if got != "team-a/10/20" || !strings.Contains(w.Body.String(), "cold-default-tenant-secret") {
		t.Fatalf("isolation regression: %s %s", got, w.Body)
	}
}
