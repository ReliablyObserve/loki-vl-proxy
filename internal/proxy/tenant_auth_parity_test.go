package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Every request that differs only in X-Scope-OrgID gets Loki's answer: the
// recorded status, and for a refusal Loki's Content-Type and exact body (Loki
// 3.7.9, auth_enabled: true, multi-tenant queries on). With auth on, a missing
// or blank header is 401 "no org id"; an ID Loki's tenant check refuses ('.' or
// '..', longer than 150, a byte outside its set) is 400 with Loki's text on
// every route, before routing, the cache or any VictoriaLogs call.
//
// conformance: tenant-auth-follows-loki
func TestTenantAuth_AnswersLikeLoki(t *testing.T) {
	meta, rows := readTenantAuthLokiAnswers(t)
	if meta.Loki == "" || len(rows) < 300 {
		t.Fatalf("%s: %d rows recorded with Loki %q", tenantAuthLokiAnswersFile, len(rows), meta.Loki)
	}
	vl := httptest.NewServer(emptyVictoriaLogs())
	defer vl.Close()
	mux := newTenantAuthReplayProxy(t, rows, vl.URL)
	// The requests come from tenantAuthCases, not the file: JSON cannot carry
	// a header that is not valid UTF-8.
	cases := map[string]tenantAuthCase{}
	for _, c := range tenantAuthCases() {
		cases[c.ID] = c
	}
	if len(cases) != len(rows) {
		t.Fatalf("%d cases, %d recorded answers: record them again (TestRecordTenantAuthLokiAnswers)", len(cases), len(rows))
	}
	var diffs []string
	for _, row := range rows {
		c, ok := cases[row.ID]
		if !ok {
			t.Fatalf("recorded %s is no case: record the answers again", row.ID)
		}
		req, err := c.request("")
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(row.Method, row.Target, nil)
		r.Header = req.Header
		if strings.HasPrefix(row.ID, "delete/") {
			r.Header.Set("X-Delete-Confirmation", "true")
			r.Header.Set("X-Delete-Token", tenantAuthReplayToken)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		got := tenantAuthAnswer{Status: w.Code, ContentType: w.Header().Get("Content-Type")}
		if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
			got.setBody(w.Body.Bytes())
		} else if strings.HasPrefix(row.ID, "labels/") {
			got.TenantLabel = strings.Contains(w.Body.String(), `"__tenant_id__"`)
		}
		if got.Status != row.Status || got.body() != row.body() || got.TenantLabel != row.TenantLabel ||
			(row.body() != "" && got.ContentType != row.ContentType) {
			diffs = append(diffs, row.ID+": proxy "+describeTenantAuth(got)+"\n      loki  "+describeTenantAuth(row))
		}
	}
	if len(diffs) > 0 {
		t.Fatalf("%d of %d answers differ from Loki %s:\n  %s", len(diffs), len(rows), meta.Loki, strings.Join(diffs, "\n  "))
	}
}

func describeTenantAuth(a tenantAuthAnswer) string {
	s := strconv.Itoa(a.Status)
	if b := a.body(); b != "" {
		s += " " + strconv.Quote(b) + " (" + a.ContentType + ")"
	}
	if a.TenantLabel {
		s += " +__tenant_id__"
	}
	return s
}

// testTenantMapFile configures a tenant map file with no entry: the proxy
// routes on X-Scope-OrgID (tenants configured) with no mapping in force.
const testTenantMapFile = "/etc/loki-vl-proxy/tenants.yaml"

// emptyVictoriaLogs answers every VictoriaLogs read with an empty success, so a
// request the proxy admits answers 200 and only the tenant check can refuse it.
func emptyVictoriaLogs() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case "/select/logsql/stats_query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		case "/select/logsql/query", "/select/logsql/tail":
		case "/select/logsql/hits":
			_, _ = w.Write([]byte(`{"hits":[]}`))
		case "/select/logsql/delete":
			w.WriteHeader(http.StatusOK)
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	})
}

// With tenancy turned off (-tenant.numeric-passthrough=false and no other
// tenant configuration), the proxy is Loki's single-tenant mode
// (auth_enabled: false): X-Scope-OrgID is ignored on every route, whatever it
// holds, and every request reads the backend default tenant. The answers are
// recorded from the e2e stack's Loki (auth_enabled: false); the backend must
// receive no tenant at all.
//
// conformance: tenant-auth-follows-loki
func TestTenantNoAuth_HeaderIgnoredLikeLoki(t *testing.T) {
	meta, rows := readTenantAnswers(t, tenantNoAuthLokiAnswersFile)
	if meta.Loki == "" || len(rows) < 600 {
		t.Fatalf("%s: %d rows recorded with Loki %q", tenantNoAuthLokiAnswersFile, len(rows), meta.Loki)
	}
	var mu sync.Mutex
	var tenants []string
	inner := emptyVictoriaLogs()
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if v := r.Header.Get("AccountID") + r.Header.Get("ProjectID") + r.Header.Get("X-Scope-OrgID") + r.FormValue("extra_stream_filters"); v != "" {
			tenants = append(tenants, v)
		}
		mu.Unlock()
		inner.ServeHTTP(w, r)
	}))
	defer vl.Close()
	p := newTestProxyWithOptions(t, withBackendURL(vl.URL), withTenancyOff)
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	cases := map[string]tenantAuthCase{}
	for _, c := range tenantNoAuthCases() {
		cases[c.ID] = c
	}
	if len(cases) != len(rows) {
		t.Fatalf("%d cases, %d recorded answers: record them again (TestRecordTenantNoAuthLokiAnswers)", len(cases), len(rows))
	}
	var diffs []string
	for _, row := range rows {
		c, ok := cases[row.ID]
		if !ok {
			t.Fatalf("recorded %s is no case: record the answers again", row.ID)
		}
		req, err := c.request("")
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(row.Method, row.Target, nil)
		r.Header = req.Header
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		got := tenantAuthAnswer{Status: w.Code, ContentType: w.Header().Get("Content-Type")}
		if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
			got.setBody(w.Body.Bytes())
		} else if strings.HasPrefix(row.ID, "labels/") {
			got.TenantLabel = strings.Contains(w.Body.String(), `"__tenant_id__"`)
		}
		if got.Status != row.Status || got.body() != row.body() || got.TenantLabel != row.TenantLabel {
			diffs = append(diffs, row.ID+": proxy "+describeTenantAuth(got)+"\n      loki  "+describeTenantAuth(row))
		}
	}
	if len(diffs) > 0 {
		t.Fatalf("%d of %d answers differ from Loki %s (auth_enabled: false):\n  %s", len(diffs), len(rows), meta.Loki, strings.Join(diffs, "\n  "))
	}
	if len(tenants) > 0 {
		t.Fatalf("VictoriaLogs received a tenant from an ignored header: %q", tenants[:min(len(tenants), 5)])
	}
}

// withTenancyOff turns tenancy off: numeric passthrough off and no other
// tenant configuration.
func withTenancyOff(c *Config) { c.StrictTenantRouting = true }

// Tenancy is off only when numeric passthrough is off and nothing else names
// tenants (the binary's default, cmd/proxy TestResolveNumericPassthrough);
// numeric passthrough and every other form of tenant configuration keep
// routing on the header.
//
// conformance: tenant-auth-follows-loki, tenant-numeric-routing
func TestTenantHeaderIgnored_OnlyWithTenancyOff(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  func(*Config)
		ignored bool
	}{
		{"Config zero value (numeric passthrough on)", func(*Config) {}, false},
		{"numeric passthrough on with allow-global", func(c *Config) { c.AllowGlobalTenant = true }, false},
		{"tenancy off", withTenancyOff, true},
		{"tenancy off with allow-global", func(c *Config) { withTenancyOff(c); c.AllowGlobalTenant = true }, true},
		{"strict + -auth.enabled", func(c *Config) { withTenancyOff(c); c.AuthEnabled = true }, false},
		{"strict + -require-tenant-header", func(c *Config) { withTenancyOff(c); c.RequireTenantHeader = true }, false},
		{"strict + tenant map", func(c *Config) {
			withTenancyOff(c)
			c.TenantMap = map[string]TenantMapping{"a": {AccountID: "1", ProjectID: "0"}}
		}, false},
		{"strict + tenant map file, empty", func(c *Config) { withTenancyOff(c); c.TenantMapFile = testTenantMapFile }, false},
		{"strict + tenant label", func(c *Config) { withTenancyOff(c); c.TenantLabel = "org_id" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestProxyWithOptions(t, tc.config)
			if got := p.tenantHeaderIgnored(p.routingForContext(context.Background())); got != tc.ignored {
				t.Fatalf("ignored = %v, want %v", got, tc.ignored)
			}
		})
	}
	// A reload that adds a tenant map starts routing; one that empties it stops.
	p := newTestProxyWithOptions(t, withTenancyOff)
	p.ReloadTenantMap(map[string]TenantMapping{"a": {AccountID: "1", ProjectID: "0"}})
	if p.tenantHeaderIgnored(p.routingForContext(context.Background())) {
		t.Fatal("a reloaded tenant map is not routed")
	}
	p.ReloadTenantMap(nil)
	if !p.tenantHeaderIgnored(p.routingForContext(context.Background())) {
		t.Fatal("an emptied static tenant map still routes")
	}
}

// With numeric passthrough on (proxy.Config's zero value; the binary's
// -tenant.numeric-passthrough=true) numeric org IDs route as before: 7 reads
// account 7, a non-canonical number and an unmapped name are refused, a
// header Loki refuses gets Loki's 400, and 7|8 fans out with __tenant_id__.
// With tenancy off the same headers all read the default tenant and
// VictoriaLogs never sees a tenant.
//
// conformance: tenant-numeric-routing, tenant-auth-follows-loki
func TestTenantHeader_NumericPassthroughRoutesTenancyOffIgnores(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("AccountID")+"/"+r.Header.Get("X-Scope-OrgID"))
		mu.Unlock()
		emptyVictoriaLogs().ServeHTTP(w, r)
	}))
	defer vl.Close()
	for _, mode := range []struct {
		name string
		opt  testProxyOption
	}{{"numeric passthrough", func(*Config) {}}, {"tenancy off", withTenancyOff}} {
		p := newTestProxyWithOptions(t, withBackendURL(vl.URL), mode.opt)
		mux := http.NewServeMux()
		p.RegisterProxyRoutes(mux)
		for _, tc := range []struct {
			org      string
			status   int
			accounts []string // every AccountID/X-Scope-OrgID pair a backend call may carry
			tenantID bool     // the answer lists __tenant_id__
		}{
			{"7", 200, []string{"7/", "7/7"}, false},
			{"007", 403, nil, false},
			{"team-a", 403, nil, false},
			{"a b", 400, nil, false},
			{"7|8", 200, []string{"7/", "7/7", "8/", "8/8"}, true},
		} {
			if mode.name == "tenancy off" {
				tc.status, tc.accounts, tc.tenantID = 200, []string{"/"}, false
			}
			mu.Lock()
			seen = nil
			mu.Unlock()
			r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?start=1762000000&end=1762003600", nil)
			r.Header.Set("X-Scope-OrgID", tc.org)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != tc.status || strings.Contains(w.Body.String(), `"__tenant_id__"`) != tc.tenantID {
				t.Fatalf("%s %q: %d %s, want %d (__tenant_id__ %v)", mode.name, tc.org, w.Code, w.Body.String(), tc.status, tc.tenantID)
			}
			mu.Lock()
			got := append([]string(nil), seen...)
			mu.Unlock()
			// The first request of each mode reaches VictoriaLogs; with tenancy
			// off the later ones share its cache entry, as they share its tenant.
			if tc.org == "7" && len(got) == 0 {
				t.Fatalf("%s %q: no backend call", mode.name, tc.org)
			}
			for _, a := range got {
				if !slices.Contains(tc.accounts, a) {
					t.Fatalf("%s %q: backend tenant %q, want one of %q", mode.name, tc.org, a, tc.accounts)
				}
			}
		}
	}
}
