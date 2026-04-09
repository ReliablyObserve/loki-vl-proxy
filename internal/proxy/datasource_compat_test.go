package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestDatasourceCompat_ForwardsConfiguredCookies(t *testing.T) {
	var gotCookie string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cookie, err := r.Cookie("grafana_session"); err == nil {
			gotCookie = cookie.Value
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:     vlBackend.URL,
		Cache:          c,
		LogLevel:       "error",
		ForwardCookies: []string{"grafana_session"},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	req.AddCookie(&http.Cookie{Name: "grafana_session", Value: "abc123"})
	req.AddCookie(&http.Cookie{Name: "ignored_cookie", Value: "skip-me"})
	w := httptest.NewRecorder()
	p.handleLabels(w, req)

	if gotCookie != "abc123" {
		t.Fatalf("expected configured cookie to be forwarded, got %q", gotCookie)
	}
}

func TestDatasourceCompat_BackendTimeoutIsConfigurable(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:     "http://unused",
		Cache:          c,
		LogLevel:       "error",
		BackendTimeout: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	if p.client.Timeout != 10*time.Minute {
		t.Fatalf("expected backend timeout to be configurable, got %s", p.client.Timeout)
	}
}

func TestDatasourceCompat_ForwardsTrustedGrafanaUserContext(t *testing.T) {
	var gotGrafanaUser, gotClientID, gotClientSource string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotGrafanaUser = r.Header.Get("X-Grafana-User")
		gotClientID = r.Header.Get("X-Loki-VL-Client-ID")
		gotClientSource = r.Header.Get("X-Loki-VL-Client-Source")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:               vlBackend.URL,
		Cache:                    c,
		LogLevel:                 "error",
		MetricsTrustProxyHeaders: true,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	req.Header.Set("X-Grafana-User", "grafana-user@example.com")
	req.RemoteAddr = "198.51.100.20:1234"
	w := httptest.NewRecorder()
	p.handleLabels(w, req)

	if gotGrafanaUser != "grafana-user@example.com" {
		t.Fatalf("expected trusted X-Grafana-User to be forwarded, got %q", gotGrafanaUser)
	}
	if gotClientID != "grafana-user@example.com" {
		t.Fatalf("expected resolved client id header, got %q", gotClientID)
	}
	if gotClientSource != "grafana_user" {
		t.Fatalf("expected client source to describe trusted grafana user, got %q", gotClientSource)
	}
}

func TestDatasourceCompat_ForwardsTrustedUserAndProxyChainHeadersAndSeparatesAuthUser(t *testing.T) {
	var gotForwardedUser, gotForwardedFor, gotAuthUser, gotAuthSource string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotForwardedUser = r.Header.Get("X-Forwarded-User")
		gotForwardedFor = r.Header.Get("X-Forwarded-For")
		gotAuthUser = r.Header.Get("X-Loki-VL-Auth-User")
		gotAuthSource = r.Header.Get("X-Loki-VL-Auth-Source")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:               vlBackend.URL,
		Cache:                    c,
		LogLevel:                 "error",
		MetricsTrustProxyHeaders: true,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	req.Header.Set("X-Forwarded-User", "idp-user@example.com")
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	req.Header.Set("Authorization", "Basic ZGF0YXNvdXJjZS11c2VyOnNlY3JldA==")
	req.RemoteAddr = "198.51.100.20:1234"
	w := httptest.NewRecorder()
	p.handleLabels(w, req)

	if gotForwardedUser != "idp-user@example.com" {
		t.Fatalf("expected trusted forwarded user header to be passed upstream, got %q", gotForwardedUser)
	}
	if gotForwardedFor != "203.0.113.10" {
		t.Fatalf("expected trusted forwarded-for header to be passed upstream, got %q", gotForwardedFor)
	}
	if gotAuthUser != "datasource-user" || gotAuthSource != "basic_auth" {
		t.Fatalf("expected datasource auth principal to be forwarded separately, got user=%q source=%q", gotAuthUser, gotAuthSource)
	}
}
