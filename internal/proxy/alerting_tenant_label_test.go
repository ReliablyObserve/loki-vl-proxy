package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// alertingFake is a ruler/alerts backend that answers for every tenant, as
// vmalert does: rules and alerts of tenants A and B plus one with no tenant label.
type alertingFake struct {
	mu      sync.Mutex
	headers []http.Header
}

func (f *alertingFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.headers = append(f.headers, r.Header.Clone())
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	rule := func(name, tenant string) string {
		labels := `{}`
		if tenant != "" {
			labels = `{"org_id":"` + tenant + `"}`
		}
		return `{"name":"` + name + `","query":"count_over_time({app=\"x\"}[1m])","type":"alerting","duration":0,"labels":` + labels + `,"annotations":{}}`
	}
	alert := func(name, tenant string) string {
		labels := `{"alertname":"` + name + `"}`
		if tenant != "" {
			labels = `{"alertname":"` + name + `","org_id":"` + tenant + `"}`
		}
		return `{"labels":` + labels + `,"state":"firing","value":"1"}`
	}
	switch r.URL.Path {
	case "/api/v1/rules":
		groups := []struct{ name, file, rules string }{
			{"g1", "ns1", rule("rule-A", "A") + "," + rule("rule-B", "B") + "," + rule("rule-U", "")},
			{"g2", "ns2", rule("rule-B2", "B")},
		}
		var out []string
		for _, g := range groups {
			if f := r.URL.Query().Get("file[]"); f != "" && f != g.file {
				continue
			}
			if n := r.URL.Query().Get("rule_group[]"); n != "" && n != g.name {
				continue
			}
			out = append(out, `{"name":"`+g.name+`","file":"`+g.file+`","interval":60,"rules":[`+g.rules+`]}`)
		}
		_, _ = io.WriteString(w, `{"status":"success","data":{"groups":[`+strings.Join(out, ",")+`]}}`)
	case "/api/v1/alerts":
		_, _ = io.WriteString(w, `{"status":"success","data":{"alerts":[`+alert("alert-A", "A")+","+alert("alert-B", "B")+","+alert("alert-U", "")+`]}}`)
	default:
		http.NotFound(w, r)
	}
}

// Every rules and alerts read route the proxy registers.
var alertingReadRoutes = []struct {
	path    string
	kind    string // "rules" or "alerts"
	yaml    bool
	grouped bool // names a rule group: an empty answer is 404
}{
	{"/prometheus/api/v1/rules", "rules", false, false},
	{"/loki/api/v1/rules", "rules", true, false},
	{"/loki/api/v1/rules/ns1", "rules", true, false},
	{"/loki/api/v1/rules/ns1/g1", "rules", true, true},
	{"/api/prom/rules", "rules", true, false},
	{"/api/prom/rules/ns1", "rules", true, false},
	{"/api/prom/rules/ns1/g1", "rules", true, true},
	{"/loki/api/v1/alerts", "alerts", false, false},
	{"/api/prom/alerts", "alerts", false, false},
	{"/prometheus/api/v1/alerts", "alerts", false, false},
}

func (f *alertingFake) reset() {
	f.mu.Lock()
	f.headers = nil
	f.mu.Unlock()
}

// Tenant-label mode: the ruler and alerts backends are not tenant aware, so a
// read is confined to the rules and alerts that carry the org in the tenant
// label, shown whole only to allow-global, and sent with the mapping's tenant
// headers for a mapped org.
//
// conformance: tenant-label-isolation, semantics/tenant-label-default-aliases, prometheus_api_v1_rules, prometheus_api_v1_alerts, loki_api_v1_rules, loki_api_v1_rules_namespace, loki_api_v1_rules_namespace_groupname, api_prom_rules, api_prom_rules_namespace, api_prom_rules_namespace_groupname
func TestTenantLabelRouting_RulesAndAlertsAreConfinedToTheOrg(t *testing.T) {
	fake := &alertingFake{}
	backend := httptest.NewServer(fake)
	defer backend.Close()

	type outcome struct {
		org         string
		allowGlobal bool
		mapped      bool
		see, hide   []string // markers (rule-/alert- suffix) present and absent
	}
	for _, tc := range []outcome{
		{org: "A", see: []string{"A"}, hide: []string{"B", "U"}},
		{org: "B", see: []string{"B"}, hide: []string{"A", "U"}},
		{org: "C", hide: []string{"A", "B", "U"}},
		{org: "*", allowGlobal: true, see: []string{"A", "B", "U"}},
		{org: "fake", allowGlobal: true, see: []string{"A", "B", "U"}},
		{org: "M", mapped: true, see: []string{"A", "B", "U"}},
		{org: "A", allowGlobal: true, see: []string{"A"}, hide: []string{"B", "U"}},
	} {
		for _, rt := range alertingReadRoutes {
			name := rt.path + "/org=" + tc.org
			if tc.allowGlobal {
				name += "/global"
			}
			t.Run(name, func(t *testing.T) {
				fake.reset()
				p := newTestProxyWithOptions(t, withTenantLabel("org_id"), func(c *Config) {
					c.RulerBackendURL = backend.URL
					c.AlertsBackendURL = backend.URL
					c.AllowGlobalTenant = tc.allowGlobal
					if tc.mapped {
						c.TenantMap = map[string]TenantMapping{"M": {AccountID: "7", ProjectID: "8"}}
					}
				})
				req := httptest.NewRequest("GET", rt.path, nil)
				req.Header.Set("X-Scope-OrgID", tc.org)
				rec := httptest.NewRecorder()
				serveProxy(p, rec, req)
				body := rec.Body.String()

				empty := len(tc.see) == 0
				if rt.grouped && empty {
					if rec.Code != http.StatusNotFound {
						t.Fatalf("status %d, want 404 for an org without that group: %s", rec.Code, body)
					}
				} else if rec.Code != http.StatusOK {
					t.Fatalf("status %d: %s", rec.Code, body)
				}
				prefix := "rule-"
				if rt.kind == "alerts" {
					prefix = "alert-"
				}
				for _, m := range tc.see {
					if !strings.Contains(body, prefix+m) {
						t.Fatalf("%s%s missing from %s", prefix, m, body)
					}
				}
				for _, m := range tc.hide {
					if strings.Contains(body, prefix+m) {
						t.Fatalf("%s%s leaked: %s", prefix, m, body)
					}
				}
				if empty && !rt.grouped {
					if rt.yaml && strings.TrimSpace(body) != "{}" {
						t.Fatalf("empty YAML answer expected: %q", body)
					}
					if !rt.yaml && !strings.Contains(body, `"status":"success"`) {
						t.Fatalf("empty JSON answer expected: %q", body)
					}
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				if len(fake.headers) != 1 {
					t.Fatalf("%d backend requests", len(fake.headers))
				}
				h := fake.headers[0]
				if tc.mapped {
					if h.Get("AccountID") != "7" || h.Get("ProjectID") != "8" {
						t.Fatalf("mapped headers not forwarded: %v", h)
					}
				} else if h.Get("AccountID") != "" || h.Get("X-Scope-OrgID") != "" {
					t.Fatalf("tenant headers sent for an unmapped label-mode org: %v", h)
				}
			})
		}
	}
}

// Without the tenant label the backends still get the tenant headers and answer
// for the org themselves; the proxy does not filter.
//
// conformance: tenant-label-isolation, prometheus_api_v1_rules, prometheus_api_v1_alerts, loki_api_v1_rules
func TestRulesAndAlerts_NonLabelModesForwardTenantHeadersUnfiltered(t *testing.T) {
	fake := &alertingFake{}
	backend := httptest.NewServer(fake)
	defer backend.Close()
	for _, rt := range alertingReadRoutes {
		fake.reset()
		p := newTestProxyWithOptions(t, func(c *Config) {
			c.RulerBackendURL = backend.URL
			c.AlertsBackendURL = backend.URL
		})
		req := httptest.NewRequest("GET", rt.path, nil)
		req.Header.Set("X-Scope-OrgID", "42")
		rec := httptest.NewRecorder()
		serveProxy(p, rec, req)
		prefix := "rule-"
		if rt.kind == "alerts" {
			prefix = "alert-"
		}
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), prefix+"A") || !strings.Contains(rec.Body.String(), prefix+"U") {
			t.Fatalf("%s: %d %s", rt.path, rec.Code, rec.Body.String())
		}
		fake.mu.Lock()
		if len(fake.headers) != 1 || fake.headers[0].Get("AccountID") != "42" {
			t.Fatalf("%s: tenant headers: %v", rt.path, fake.headers)
		}
		fake.mu.Unlock()
	}
}

// An answer the proxy cannot attribute is an error, never passed on unfiltered.
//
// conformance: tenant-label-isolation, prometheus_api_v1_rules
func TestTenantLabelRouting_UnattributableAlertingAnswerIsRefused(t *testing.T) {
	for _, body := range []string{`not json`, `{"status":"success","data":{"other":[]}}`} {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, body)
		}))
		p := newTestProxyWithOptions(t, withTenantLabel("org_id"), func(c *Config) { c.RulerBackendURL = backend.URL })
		req := httptest.NewRequest("GET", "/prometheus/api/v1/rules", nil)
		req.Header.Set("X-Scope-OrgID", "A")
		rec := httptest.NewRecorder()
		serveProxy(p, rec, req)
		backend.Close()
		if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "other") {
			t.Fatalf("%q: %d %s", body, rec.Code, rec.Body.String())
		}
	}
}

func labelModeRulerProxy(t *testing.T, status int, body string, allowGlobal bool) *Proxy {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(backend.Close)
	return newTestProxyWithOptions(t, withTenantLabel("org_id"), func(c *Config) {
		c.RulerBackendURL = backend.URL
		c.AlertsBackendURL = backend.URL
		c.AllowGlobalTenant = allowGlobal
	})
}

func getAlerting(p *Proxy, path, org string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", path, nil)
	req.Header.Set("X-Scope-OrgID", org)
	rec := httptest.NewRecorder()
	serveProxy(p, rec, req)
	return rec
}

const mixedRulesBody = `{"status":"success","warnings":["warn-other-tenant"],"infos":["info-other-tenant"],"data":{"groupNextToken":"token-other-tenant","groups":[
{"name":"g","file":"ns1","interval":60,"evaluationTime":0.1,"params":{"p":["params-other-tenant"]},"headers":["header-other-tenant"],"labels":{"group":"labels-other-tenant"},"rules":[
 {"name":"rule-A","query":"a>b&c","type":"alerting","labels":{"org_id":"A"},"alerts":[
   {"labels":{"alertname":"alert-A-1","org_id":"A"},"state":"firing"},
   {"labels":{"alertname":"alert-B-in-A","org_id":"B"},"state":"firing"},
   {"labels":{"alertname":"alert-nolabel-in-A"},"state":"firing"}]},
 {"name":"rule-T","query":"q","type":"alerting","labels":{"org_id":"{{ $labels.org_id }}"},"alerts":[
   {"labels":{"alertname":"alert-A-in-T","org_id":"A"},"state":"firing"},
   {"labels":{"alertname":"alert-B-in-T","org_id":"B"},"state":"firing"}]},
 {"name":"rule-B","query":"q","type":"alerting","labels":{"org_id":"B"},"alerts":null}]}]}}`

// conformance: tenant-label-isolation, prometheus_api_v1_rules, loki_api_v1_rules
func TestTenantLabelRouting_RulesNestedAlertsTemplatesAndGroupFields(t *testing.T) {
	p := labelModeRulerProxy(t, http.StatusOK, mixedRulesBody, false)
	for _, path := range []string{"/prometheus/api/v1/rules", "/loki/api/v1/rules", "/api/prom/rules"} {
		body := getAlerting(p, path, "A").Body.String()
		if !strings.Contains(body, "rule-A") {
			t.Fatalf("%s: rule-A missing: %s", path, body)
		}
		for _, leaked := range []string{"rule-T", "rule-B", "alert-B-in-A", "alert-A-in-T", "alert-B-in-T", "warn-other", "info-other", "token-other", "params-other", "header-other", "labels-other"} {
			if strings.Contains(body, leaked) {
				t.Fatalf("%s: %q leaked: %s", path, leaked, body)
			}
		}
		if path == "/prometheus/api/v1/rules" {
			for _, want := range []string{"alert-A-1", `"query":"a>b&c"`} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s: %q missing: %s", path, want, body)
				}
			}
			if strings.Contains(body, "alert-nolabel-in-A") {
				t.Fatalf("a nested alert without the tenant label leaked: %s", body)
			}
		}
		// An org named like the rule's template must not match it.
		for _, tmpl := range []string{"{{ $labels.org_id }}"} {
			rec := getAlerting(p, path, tmpl)
			if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "rule-") || strings.Contains(rec.Body.String(), "alert-") {
				t.Fatalf("%s: org %q: %d %s", path, tmpl, rec.Code, rec.Body.String())
			}
		}
	}
}

// conformance: tenant-label-isolation, prometheus_api_v1_alerts
func TestTenantLabelRouting_AlertsTemplatedTenantLabelNeverMatches(t *testing.T) {
	body := `{"status":"success","data":{"alerts":[{"labels":{"alertname":"alert-T","org_id":"{{ $labels.org_id }}"}},{"labels":{"alertname":"alert-A","org_id":"A"}}]}}`
	p := labelModeRulerProxy(t, http.StatusOK, body, false)
	if got := getAlerting(p, "/loki/api/v1/alerts", "{{ $labels.org_id }}").Body.String(); strings.Contains(got, "alert-") {
		t.Fatalf("%s", got)
	}
	if got := getAlerting(p, "/loki/api/v1/alerts", "A").Body.String(); !strings.Contains(got, "alert-A") || strings.Contains(got, "alert-T") {
		t.Fatalf("%s", got)
	}
}

// Any 2xx answer is filtered, not only 200.
//
// conformance: tenant-label-isolation, prometheus_api_v1_rules, loki_api_v1_rules
func TestTenantLabelRouting_AnyTwoHundredAnswerIsFiltered(t *testing.T) {
	p := labelModeRulerProxy(t, http.StatusNonAuthoritativeInfo, mixedRulesBody, false)
	for _, path := range []string{"/prometheus/api/v1/rules", "/loki/api/v1/rules", "/loki/api/v1/alerts"} {
		rec := getAlerting(p, path, "A")
		body := rec.Body.String()
		if rec.Code != http.StatusOK || strings.Contains(body, "rule-B") || strings.Contains(body, "rule-T") || strings.Contains(body, "other-tenant") {
			t.Fatalf("%s: %d %s", path, rec.Code, body)
		}
	}
}

// A backend error body is not attributable to the org, so it is not passed on.
//
// conformance: tenant-label-isolation, prometheus_api_v1_rules, loki_api_v1_rules
func TestTenantLabelRouting_BackendErrorBodyIsNotPassedOn(t *testing.T) {
	const secret = "rule-text-of-another-tenant"
	for _, path := range []string{"/prometheus/api/v1/rules", "/loki/api/v1/rules", "/loki/api/v1/rules/ns1/g", "/loki/api/v1/alerts"} {
		p := labelModeRulerProxy(t, http.StatusBadRequest, `{"error":"`+secret+`"}`, false)
		rec := getAlerting(p, path, "A")
		if rec.Code != http.StatusBadRequest || strings.Contains(rec.Body.String(), secret) || !strings.Contains(rec.Body.String(), "alerting backend error") {
			t.Fatalf("%s: %d %s", path, rec.Code, rec.Body.String())
		}
		// The global view, which is not confined, is unchanged.
		pg := labelModeRulerProxy(t, http.StatusBadRequest, `{"error":"`+secret+`"}`, true)
		if rec := getAlerting(pg, path, "*"); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("%s (global): %d %s", path, rec.Code, rec.Body.String())
		}
	}
}

// Shapes the proxy cannot attribute are refused; null lists are empty.
//
// conformance: tenant-label-isolation, prometheus_api_v1_rules, prometheus_api_v1_alerts
func TestTenantLabelRouting_AlertingAnswerShapes(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       int
	}{
		{"groups and alerts", `{"status":"success","data":{"groups":[],"alerts":[]}}`, http.StatusInternalServerError},
		{"groups not a list", `{"status":"success","data":{"groups":"x"}}`, http.StatusInternalServerError},
		{"no data", `{"status":"success"}`, http.StatusInternalServerError},
		{"null groups", `{"status":"success","data":{"groups":null}}`, http.StatusOK},
		{"null alerts", `{"status":"success","data":{"alerts":null}}`, http.StatusOK},
	} {
		p := labelModeRulerProxy(t, http.StatusOK, tc.body, false)
		for _, path := range []string{"/prometheus/api/v1/rules", "/loki/api/v1/alerts"} {
			if tc.name == "null alerts" && strings.Contains(path, "rules") || tc.name == "null groups" && strings.Contains(path, "alerts") {
				continue
			}
			if rec := getAlerting(p, path, "A"); rec.Code != tc.want {
				t.Fatalf("%s %s: %d %s", tc.name, path, rec.Code, rec.Body.String())
			}
		}
	}
}
