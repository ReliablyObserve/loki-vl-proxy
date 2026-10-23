package proxy

import (
	"net/http"
	"net/http/httptest"
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

// With -delete-api-enabled the route is registered and the existing handler
// answers unchanged.
//
// conformance: loki_api_v1_delete, semantics/delete-route-disabled-like-loki
func TestDeleteRoute_EnabledRegistersExistingHandler(t *testing.T) {
	p := newTestProxyWithOptions(t, withBackendURL("http://unused"), func(c *Config) {
		c.DeleteAPIEnabled = true
	})
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)

	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/delete?query={app="nginx"}&start=1&end=2`, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want the handler's 405; body %q", w.Code, w.Body.String())
	}

	r = httptest.NewRequest(http.MethodPost, `/loki/api/v1/delete?query={app="nginx"}&start=1&end=2`, nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("POST without confirmation status = %d, want the handler's 400; body %q", w.Code, w.Body.String())
	}
}
