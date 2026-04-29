package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"gopkg.in/yaml.v3"
)

func newAlertProxyMux(t *testing.T, cfg Config) (*Proxy, *http.ServeMux) {
	t.Helper()
	if cfg.Cache == nil {
		cfg.Cache = cache.New(60*time.Second, 1000)
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "error"
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

func TestRulerProxy_LegacyRulesYAML(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/rules" {
			t.Fatalf("expected /api/v1/rules, got %s", r.URL.Path)
		}
		if got := r.URL.Query()["file[]"]; len(got) != 1 || got[0] != "prod" {
			t.Fatalf("expected file[] filter for legacy namespace, got %v", r.URL.Query())
		}
		if got := r.Header.Get("AccountID"); got != "41" {
			t.Fatalf("expected AccountID to be forwarded, got %q", got)
		}
		if got := r.Header.Get("ProjectID"); got != "7" {
			t.Fatalf("expected ProjectID to be forwarded, got %q", got)
		}
		// VL backend credentials must NOT be forwarded to the ruler backend.
		if got := r.Header.Get("Authorization"); got != "" {
			t.Fatalf("VL backend credentials must not be forwarded to ruler backend, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"groups": []map[string]any{
					{
						"name":     "api",
						"file":     "rules/api.yaml",
						"interval": 60,
						"rules": []map[string]any{
							{
								"name":   "HighErrorRate",
								"query":  `sum(rate({app="api"} |= "error"[5m]))`,
								"health": "ok",
								"type":   "alerting",
							},
							{
								"name":  "api_requests_total:rate5m",
								"query": `sum(rate({app="api"}[5m]))`,
								"type":  "recording",
							},
						},
					},
				},
			},
		})
	}))
	defer backend.Close()

	_, mux := newAlertProxyMux(t, Config{
		BackendURL:       "http://unused",
		RulerBackendURL:  backend.URL,
		BackendBasicAuth: "user:pass",
		TenantMap: map[string]TenantMapping{
			"tenant-a": {AccountID: "41", ProjectID: "7"},
		},
		AuthEnabled: true,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/rules/prod", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "application/yaml" {
		t.Fatalf("expected YAML response, got %q", got)
	}

	var resp map[string][]legacyRuleGroup
	if err := yaml.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid YAML: %v", err)
	}
	groups := resp["rules/api.yaml"]
	if len(groups) != 1 {
		t.Fatalf("expected 1 YAML group, got %d", len(groups))
	}
	if groups[0].Name != "api" {
		t.Fatalf("expected group name api, got %q", groups[0].Name)
	}
	if len(groups[0].Rules) != 2 {
		t.Fatalf("expected 2 YAML rules, got %+v", groups[0].Rules)
	}
	if groups[0].Rules[0].Alert != "HighErrorRate" {
		t.Fatalf("expected alert rule conversion, got %+v", groups[0].Rules[0])
	}
	if groups[0].Rules[1].Record != "api_requests_total:rate5m" {
		t.Fatalf("expected recording rule conversion, got %+v", groups[0].Rules[1])
	}
}

func TestRulerProxy_PrometheusRulesJSONPassthrough(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/rules" {
			t.Fatalf("expected /api/v1/rules, got %s", r.URL.Path)
		}
		if got := r.URL.RawQuery; got != "namespace=prod" {
			t.Fatalf("expected query string to be forwarded, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"groups": []map[string]any{
					{"name": "api"},
				},
			},
		})
	}))
	defer backend.Close()

	_, mux := newAlertProxyMux(t, Config{
		BackendURL:      "http://unused",
		RulerBackendURL: backend.URL,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/rules?namespace=prod", nil)
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	data := resp["data"].(map[string]any)
	groups := data["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
}

func TestRulerProxy_AlertsPassthrough(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/alerts" {
			t.Fatalf("expected /api/v1/alerts, got %s", r.URL.Path)
		}
		if got := r.URL.RawQuery; got != "filter=firing" {
			t.Fatalf("expected query string to be forwarded, got %q", got)
		}
		if got := r.Header.Get("AccountID"); got != "" {
			t.Fatalf("expected global tenant request to omit AccountID, got %q", got)
		}
		if got := r.Header.Get("ProjectID"); got != "" {
			t.Fatalf("expected global tenant request to omit ProjectID, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"alerts": []map[string]any{
					{
						"labels": map[string]string{
							"alertname": "HighErrorRate",
							"severity":  "page",
						},
						"state": "firing",
					},
				},
			},
		})
	}))
	defer backend.Close()

	_, mux := newAlertProxyMux(t, Config{
		BackendURL:       "http://unused",
		AlertsBackendURL: backend.URL,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/alerts?filter=firing", nil)
	req.Header.Set("X-Scope-OrgID", "0")
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	data := resp["data"].(map[string]any)
	alerts := data["alerts"].([]any)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
}

func TestRulerProxy_LegacyRuleGroupFiltersToSingleGroup(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query()["file[]"]; len(got) != 1 || got[0] != "prod" {
			t.Fatalf("expected file[] filter, got %v", r.URL.Query())
		}
		if got := r.URL.Query()["rule_group[]"]; len(got) != 1 || got[0] != "api" {
			t.Fatalf("expected rule_group[] filter, got %v", r.URL.Query())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"groups": []map[string]any{
					{
						"name":     "api",
						"file":     "prod",
						"interval": 30,
						"rules": []map[string]any{
							{
								"name":     "api_requests_total:rate5m",
								"query":    `sum(rate({app="api"}[5m]))`,
								"type":     "recording",
								"duration": 0,
							},
						},
					},
				},
			},
		})
	}))
	defer backend.Close()

	_, mux := newAlertProxyMux(t, Config{
		BackendURL:      "http://unused",
		RulerBackendURL: backend.URL,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/prom/rules/prod/api", nil)
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var group legacyRuleGroup
	if err := yaml.Unmarshal(w.Body.Bytes(), &group); err != nil {
		t.Fatalf("invalid YAML: %v", err)
	}
	if group.Name != "api" {
		t.Fatalf("expected single group response, got %q", group.Name)
	}
	if len(group.Rules) != 1 || group.Rules[0].Record != "api_requests_total:rate5m" {
		t.Fatalf("expected recording rule conversion, got %+v", group.Rules)
	}
}

func TestRulerProxy_ParseLegacyRulesPathRejectsTraversal(t *testing.T) {
	if _, _, err := parseLegacyRulesPath("/loki/api/v1/rules/%2e%2e/escape"); err == nil {
		t.Fatal("expected path traversal to be rejected")
	}
}

func TestRulerProxy_ParseLegacyRulesPathAcceptsEncodedSegments(t *testing.T) {
	namespace, group, err := parseLegacyRulesPath("/loki/api/v1/rules/team%2Fa/api%20alerts")
	if err != nil {
		t.Fatalf("expected encoded path to parse: %v", err)
	}
	if namespace != "team/a" {
		t.Fatalf("expected decoded namespace, got %q", namespace)
	}
	if group != "api alerts" {
		t.Fatalf("expected decoded group, got %q", group)
	}
}

func TestRulerProxy_LegacyRuleGroupFromBackend(t *testing.T) {
	out := legacyRuleGroupFromBackend(alertingRuleGroup{
		Name:     "api",
		Interval: 15,
		Rules: []alertingBackendRule{
			{
				Name:     "HighErrorRate",
				Query:    `sum(rate({app="api"} |= "error"[5m]))`,
				Type:     "alerting",
				Duration: 120,
				Labels:   map[string]string{"severity": "page"},
			},
			{
				Name:  "api_requests_total:rate5m",
				Query: `sum(rate({app="api"}[5m]))`,
				Type:  "recording",
			},
		},
	})

	if out.Interval != "15s" {
		t.Fatalf("expected 15s interval, got %q", out.Interval)
	}
	if len(out.Rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(out.Rules))
	}
	if out.Rules[0].Alert != "HighErrorRate" || out.Rules[0].For != "2m0s" {
		t.Fatalf("expected alert rule conversion, got %+v", out.Rules[0])
	}
	if out.Rules[1].Record != "api_requests_total:rate5m" {
		t.Fatalf("expected recording rule conversion, got %+v", out.Rules[1])
	}
}

func TestRulerProxy_BackendErrorPropagates(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, `{"status":"error","error":"backend unavailable"}`, http.StatusBadGateway)
	}))
	defer backend.Close()

	_, mux := newAlertProxyMux(t, Config{
		BackendURL:      "http://unused",
		RulerBackendURL: backend.URL,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/rules", nil)
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got == "" {
		t.Fatal("expected backend content-type to be forwarded on error responses")
	}
}

func TestRulerProxy_NoBackendFallsBackToStub(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/rules", nil)
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/yaml" {
		t.Fatalf("expected YAML fallback, got %q", got)
	}
	var resp map[string][]legacyRuleGroup
	if err := yaml.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid YAML: %v", err)
	}
	if len(resp) != 0 {
		t.Fatalf("expected empty fallback groups, got %v", resp)
	}
}

func TestRulerProxy_NoBackendPrometheusRulesFallsBackToJSONStub(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/rules", nil)
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	data := resp["data"].(map[string]any)
	groups := data["groups"].([]any)
	if len(groups) != 0 {
		t.Fatalf("expected empty JSON fallback groups, got %d", len(groups))
	}
}

func TestRulerProxy_InvalidBackendURLs(t *testing.T) {
	_, err := New(Config{
		BackendURL:      "http://unused",
		RulerBackendURL: "://bad",
		Cache:           cache.New(60*time.Second, 1000),
		LogLevel:        "error",
	})
	if err == nil {
		t.Fatal("expected invalid ruler backend URL error")
	}

	_, err = New(Config{
		BackendURL:       "http://unused",
		RulerBackendURL:  "http://ruler",
		AlertsBackendURL: "://bad",
		Cache:            cache.New(60*time.Second, 1000),
		LogLevel:         "error",
	})
	if err == nil {
		t.Fatal("expected invalid alerts backend URL error")
	}
}

func TestRulerProxy_AlertingBackendGetRequiresBackend(t *testing.T) {
	p, err := New(Config{
		BackendURL: "http://unused",
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/rules", nil)
	if _, err := p.alertingBackendGet(withOrgID(req), nil, "/api/v1/rules"); err == nil {
		t.Fatal("expected nil backend to be rejected")
	}
}
