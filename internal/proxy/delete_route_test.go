package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Loki registers /loki/api/v1/delete only with compactor retention enabled
// (off by default); otherwise its router answers every method with
// http.NotFoundHandler before any auth middleware. Without -delete-api-enabled
// the proxy leaves the path unregistered and must give the same answer, with
// no tenant, rate-limit or parameter check answering first and nothing sent to
// VictoriaLogs.
//
// conformance: loki_api_v1_delete, semantics/delete-route-disabled-like-loki
func TestDeleteRoute_DisabledByDefaultAnswersLikeLoki(t *testing.T) {
	var backendHits atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer vl.Close()

	configs := map[string]testProxyOption{
		"default": func(*Config) {},
		"tenant-label": func(c *Config) {
			c.TenantLabel = "org_id"
		},
		"require-tenant-header": func(c *Config) {
			c.AuthEnabled = true
			c.RequireTenantHeader = true
		},
	}
	registrars := map[string]func(*Proxy, *http.ServeMux){
		"RegisterRoutes":      (*Proxy).RegisterRoutes,
		"RegisterProxyRoutes": (*Proxy).RegisterProxyRoutes,
	}
	headerSets := map[string]http.Header{
		"no-headers":              {},
		"tenant":                  {"X-Scope-OrgID": {"0"}},
		"tenant-and-confirmation": {"X-Scope-OrgID": {"team-a"}, "X-Delete-Confirmation": {"true"}},
	}
	const path = `/loki/api/v1/delete?query={app="nginx"}&start=1700000000&end=1700003600`

	for cfgName, opt := range configs {
		p := newTestProxyWithOptions(t, withBackendURL(vl.URL), opt)
		for regName, register := range registrars {
			mux := http.NewServeMux()
			register(p, mux)
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
				for hName, headers := range headerSets {
					t.Run(cfgName+"/"+regName+"/"+method+"/"+hName, func(t *testing.T) {
						r := httptest.NewRequest(method, path, nil)
						for k, v := range headers {
							r.Header.Set(k, v[0])
						}
						w := httptest.NewRecorder()
						mux.ServeHTTP(w, r)
						if w.Code != http.StatusNotFound {
							t.Fatalf("status = %d, want 404; body %q", w.Code, w.Body.String())
						}
						if got := w.Body.String(); got != "404 page not found\n" {
							t.Fatalf("body = %q, want Loki's %q", got, "404 page not found\n")
						}
						if got := w.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
							t.Fatalf("Content-Type = %q, want text/plain; charset=utf-8", got)
						}
					})
				}
			}
		}
	}
	if n := backendHits.Load(); n != 0 {
		t.Fatalf("backend received %d requests with the delete route disabled, want 0", n)
	}
}

// testDeleteToken is the delete token the delete API tests configure.
const testDeleteToken = "test-delete-api-token"

// enableDeleteAPIForTest opens the delete API on p as -delete-api-enabled with
// -delete-api-auth-token does.
func enableDeleteAPIForTest(p *Proxy) {
	p.deleteAPIEnabled = true
	p.deleteAPIAuthToken = testDeleteToken
}

// serveWithDeleteToken serves r with the delete API's token.
func serveWithDeleteToken(h http.Handler, w http.ResponseWriter, r *http.Request) {
	r.Header.Set("X-Delete-Token", testDeleteToken)
	h.ServeHTTP(w, r)
}

// With -delete-api-enabled the route is registered and the existing handler
// answers unchanged once the delete token is presented.
//
// conformance: loki_api_v1_delete, semantics/delete-route-disabled-like-loki
func TestDeleteRoute_EnabledRegistersExistingHandler(t *testing.T) {
	p := newTestProxyWithOptions(t, withBackendURL("http://unused"), func(c *Config) {
		c.DeleteAPIEnabled = true
		c.DeleteAPIAuthToken = testDeleteToken
	})
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)

	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/delete?query={app="nginx"}&start=1&end=2`, nil)
	w := httptest.NewRecorder()
	serveWithDeleteToken(mux, w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want the handler's 405; body %q", w.Code, w.Body.String())
	}

	r = httptest.NewRequest(http.MethodPost, `/loki/api/v1/delete?query={app="nginx"}&start=1&end=2`, nil)
	w = httptest.NewRecorder()
	serveWithDeleteToken(mux, w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("POST without confirmation status = %d, want the handler's 400; body %q", w.Code, w.Body.String())
	}
}

// The delete API needs its token: New refuses DeleteAPIEnabled without
// one, and a proxy whose delete route is open but holds no token (a field set
// after New) refuses every request.
//
// conformance: delete-api-requires-token
func TestDeleteAuth_ClosedWithoutToken(t *testing.T) {
	for _, token := range []string{"", "   "} {
		_, err := New(Config{BackendURL: "http://unused", DeleteAPIEnabled: true, DeleteAPIAuthToken: token, AdminAuthToken: "admin-token-is-not-the-delete-token", LogLevel: "error"})
		if err == nil || !strings.Contains(err.Error(), "-delete-api-enabled=true requires -delete-api-auth-token") {
			t.Fatalf("token %q: New error = %v, want the refusal naming both flags", token, err)
		}
	}
	var hits atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer vl.Close()
	p := newTestProxyWithOptions(t, withBackendURL(vl.URL))
	p.deleteAPIEnabled = true // open without a token: still closed
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	for _, h := range []http.Header{{}, {"X-Delete-Token": {""}}, {"Authorization": {"Bearer "}}} {
		r := httptest.NewRequest(http.MethodPost, `/loki/api/v1/delete?query={app="nginx"}&start=1700000000&end=1700003600`, nil)
		r.Header = h.Clone()
		r.Header.Set("X-Delete-Confirmation", "true")
		r.Header.Set("X-Scope-OrgID", "0")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized || w.Body.String() != "delete authentication required" {
			t.Fatalf("%v: %d %q, want 401 delete authentication required", h, w.Code, w.Body.String())
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("VictoriaLogs received %d delete requests with no delete token configured", n)
	}
}

// With the token configured, only a request carrying it reaches the delete
// handler: a missing or wrong token answers 401 before the method, tenant,
// confirmation and parameter checks; the right one (X-Delete-Token or
// Authorization: Bearer) passes to the existing safeguards, and the token never
// reaches VictoriaLogs even when Authorization is forwarded. The audit log
// still names the tenant and the query.
//
// conformance: delete-api-requires-token, loki_api_v1_delete
func TestDeleteAuth_TokenGatesTheHandler(t *testing.T) {
	var hits atomic.Int64
	var gotAuth, gotAdmin atomic.Value
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		gotAuth.Store(r.Header.Get("Authorization"))
		gotAdmin.Store(r.Header.Get("X-Delete-Token"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer vl.Close()
	var logs lockedBuffer
	p := newTestProxyWithOptions(t, withBackendURL(vl.URL), func(c *Config) {
		c.DeleteAPIEnabled = true
		c.DeleteAPIAuthToken = testDeleteToken
		c.ForwardHeaders = []string{"Authorization"}
		c.TenantMap = map[string]TenantMapping{"team-a": {AccountID: "100", ProjectID: "0"}}
	})
	p.log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	const ok = `/loki/api/v1/delete?query={app="nginx"}&start=1700000000&end=1700003600`
	type tc struct {
		name, method, target string
		header               http.Header
		status               int
		body                 string
	}
	full := func(extra ...string) http.Header {
		h := http.Header{"X-Delete-Confirmation": {"true"}, "X-Scope-Orgid": {"team-a"}}
		for i := 0; i+1 < len(extra); i += 2 {
			h.Set(extra[i], extra[i+1])
		}
		return h
	}
	const denied = "delete authentication required"
	for _, c := range []tc{
		{"no token", http.MethodPost, ok, full(), 401, denied},
		{"wrong token", http.MethodPost, ok, full("X-Delete-Token", "nope"), 401, denied},
		{"token prefix", http.MethodPost, ok, full("X-Delete-Token", testDeleteToken[:5]), 401, denied},
		{"wrong bearer", http.MethodPost, ok, full("Authorization", "Bearer nope"), 401, denied},
		{"basic auth is no token", http.MethodPost, ok, full("Authorization", "Basic dGVzdDp0ZXN0"), 401, denied},
		{"no token, GET", http.MethodGet, ok, http.Header{}, 401, denied},
		{"no token, no tenant", http.MethodPost, ok, http.Header{}, 401, denied},
		{"token, GET is the handler's 405", http.MethodGet, ok, full("X-Delete-Token", testDeleteToken), 405, ""},
		{"token, no confirmation", http.MethodPost, ok, http.Header{"X-Delete-Token": {testDeleteToken}, "X-Scope-Orgid": {"team-a"}}, 400, "delete requires X-Delete-Confirmation: true header for safety\n"},
		{"token, wildcard", http.MethodPost, `/loki/api/v1/delete?query={}&start=1700000000&end=1700003600`, full("X-Delete-Token", testDeleteToken), 400, "wildcard delete rejected — query must target specific streams (e.g., {app=\"nginx\"})\n"},
		{"token, range too wide", http.MethodPost, `/loki/api/v1/delete?query={app="nginx"}&start=1600000000&end=1700000000`, full("X-Delete-Token", testDeleteToken), 400, ""},
		{"token, unknown tenant", http.MethodPost, ok, http.Header{"X-Delete-Token": {testDeleteToken}, "X-Delete-Confirmation": {"true"}, "X-Scope-Orgid": {"nobody"}}, 403, ""},
		{"token, invalid tenant is Loki's", http.MethodPost, ok, http.Header{"X-Delete-Token": {testDeleteToken}, "X-Delete-Confirmation": {"true"}, "X-Scope-Orgid": {"a b"}}, 400, "tenant ID 'a b' contains unsupported character ' '\n"},
		{"token in X-Delete-Token", http.MethodPost, ok, full("X-Delete-Token", testDeleteToken), 204, ""},
		{"token as bearer", http.MethodPost, ok, full("Authorization", "Bearer "+testDeleteToken), 204, ""},
		{"token in both headers", http.MethodPost, ok, full("X-Delete-Token", testDeleteToken, "Authorization", "Bearer "+testDeleteToken), 204, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := hits.Load()
			r := httptest.NewRequest(c.method, c.target, nil)
			r.Header = c.header.Clone()
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != c.status || (c.body != "" && w.Body.String() != c.body) {
				t.Fatalf("%d %q, want %d %q", w.Code, w.Body.String(), c.status, c.body)
			}
			reached := hits.Load() != before
			if reached != (c.status == http.StatusNoContent) {
				t.Fatalf("VictoriaLogs reached = %v for status %d", reached, w.Code)
			}
			if reached {
				if a, _ := gotAuth.Load().(string); strings.Contains(a, testDeleteToken) {
					t.Fatalf("the delete token reached VictoriaLogs in Authorization: %q", a)
				}
				if a, _ := gotAdmin.Load().(string); a != "" {
					t.Fatalf("X-Delete-Token reached VictoriaLogs: %q", a)
				}
			}
		})
	}
	out := strings.Join(logs.lines(), "\n")
	if !strings.Contains(out, `"msg":"DELETE request"`) || !strings.Contains(out, `"msg":"DELETE completed"`) ||
		!strings.Contains(out, `"tenant":"team-a"`) || !strings.Contains(out, `app=\"nginx\"`) {
		t.Fatalf("audit log lines missing: %s", out)
	}
	if strings.Contains(out, testDeleteToken) {
		t.Fatalf("the delete token reached the log: %s", out)
	}
}

// The delete token and the admin token are separate credentials: the admin
// token does not open the delete API, and the delete token does not open the
// admin routes.
//
// conformance: delete-api-requires-token
func TestDeleteAuth_SeparateFromAdminToken(t *testing.T) {
	const adminToken = "admin-only-token"
	var hits atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer vl.Close()
	on := true
	p := newTestProxyWithOptions(t, withBackendURL(vl.URL), func(c *Config) {
		c.DeleteAPIEnabled = true
		c.DeleteAPIAuthToken = testDeleteToken
		c.AdminAuthToken = adminToken
		c.RegisterInstrumentation = &on
	})
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	del := func(h http.Header) int {
		r := httptest.NewRequest(http.MethodPost, `/loki/api/v1/delete?query={app="nginx"}&start=1700000000&end=1700003600`, nil)
		r.Header = h
		r.Header.Set("X-Delete-Confirmation", "true")
		r.Header.Set("X-Scope-OrgID", "0")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w.Code
	}
	if got := del(http.Header{"X-Admin-Token": {adminToken}}); got != http.StatusUnauthorized {
		t.Fatalf("admin token in X-Admin-Token opened the delete API: %d", got)
	}
	if got := del(http.Header{"Authorization": {"Bearer " + adminToken}}); got != http.StatusUnauthorized {
		t.Fatalf("admin token as bearer opened the delete API: %d", got)
	}
	if got := del(http.Header{"X-Delete-Token": {adminToken}}); got != http.StatusUnauthorized {
		t.Fatalf("admin token in X-Delete-Token opened the delete API: %d", got)
	}
	if hits.Load() != 0 {
		t.Fatal("a delete reached VictoriaLogs with the admin token")
	}
	if got := del(http.Header{"X-Delete-Token": {testDeleteToken}}); got != http.StatusNoContent {
		t.Fatalf("delete token: %d", got)
	}
	for _, h := range []http.Header{{"X-Delete-Token": {testDeleteToken}}, {"Authorization": {"Bearer " + testDeleteToken}}, {"X-Admin-Token": {testDeleteToken}}} {
		r := httptest.NewRequest(http.MethodPost, "/admin/cache/flush", nil)
		r.Header = h
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("delete token %v opened /admin/cache/flush: %d", h, w.Code)
		}
	}
}
