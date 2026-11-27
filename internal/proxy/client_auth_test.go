package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/clientauth"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

const (
	authTestAdminToken  = "admin-token-for-tests"
	authTestDeleteToken = "delete-token-for-tests"
	authTestPeerToken   = "peer-token-for-tests"
)

// newTestClientAuth is grafana/secret (any tenant), pinned/pinned-pw (tenant
// team-a) and the bearer token ci-token (user ci).
func newTestClientAuth(t testing.TB) *clientauth.Authenticator {
	t.Helper()
	hash := func(pw string) string {
		h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		return string(h)
	}
	digest := sha256.Sum256([]byte("ci-token"))
	data := fmt.Appendf(nil, "users:\n  - name: grafana\n    password_hash: %q\n  - name: pinned\n    password_hash: %q\n    tenant: team-a\n  - name: ci\n    token_sha256: %q\n",
		hash("secret"), hash("pinned-pw"), hex.EncodeToString(digest[:]))
	users, err := clientauth.Parse(data, ValidateTenantPin)
	if err != nil {
		t.Fatal(err)
	}
	a, err := clientauth.New(users, clientauth.Options{CacheTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func basicAuth(user, pw string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pw))
}

// recordingMux records every pattern registered on it and registers it on a
// real mux.
type recordingMux struct {
	*http.ServeMux
	patterns []string
}

func newRecordingMux() *recordingMux { return &recordingMux{ServeMux: http.NewServeMux()} }

func (m *recordingMux) Handle(pattern string, h http.Handler) {
	m.patterns = append(m.patterns, pattern)
	m.ServeMux.Handle(pattern, h)
}

func (m *recordingMux) HandleFunc(pattern string, h func(http.ResponseWriter, *http.Request)) {
	m.patterns = append(m.patterns, pattern)
	m.ServeMux.HandleFunc(pattern, h)
}

// pathOf is a request path the pattern matches.
func pathOf(pattern string) string {
	p := strings.NewReplacer("{name}", "x", "{namespace}", "x").Replace(pattern)
	if strings.HasSuffix(p, "/") {
		p += "x"
	}
	return p
}

// stubVL answers every VictoriaLogs call with an empty success and records
// what it was sent.
type stubVL struct {
	mu       sync.Mutex
	calls    int
	auth     []string
	accounts []string
}

func (s *stubVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.calls++
		s.auth = append(s.auth, r.Header.Get("Authorization"))
		s.accounts = append(s.accounts, r.Header.Get("AccountID"))
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "field_names"), strings.Contains(r.URL.Path, "field_values"), strings.Contains(r.URL.Path, "streams"):
			_, _ = io.WriteString(w, `{"values":[{"value":"app","hits":1}]}`)
		default:
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (s *stubVL) snapshot() (int, []string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, slices.Clone(s.auth), slices.Clone(s.accounts)
}

// newAuthTestProxy turns every route on: peer cache, delete API, admin token,
// pprof, query analytics and instrumentation.
func newAuthTestProxy(t *testing.T, backend string, opts ...testProxyOption) *Proxy {
	t.Helper()
	pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: "127.0.0.1:3100", DiscoveryType: "static", StaticPeers: "127.0.0.1:3100"})
	t.Cleanup(pc.Close)
	on := true
	all := append([]testProxyOption{withBackendURL(backend), func(c *Config) {
		c.ClientAuth = newTestClientAuth(t)
		c.PeerCache = pc
		c.PeerAuthToken = authTestPeerToken
		c.DeleteAPIEnabled = true
		c.DeleteAPIAuthToken = authTestDeleteToken
		c.AdminAuthToken = authTestAdminToken
		c.RegisterInstrumentation = &on
		c.EnablePprof = true
		c.EnableQueryAnalytics = true
		c.MetricsExportSensitiveLabels = true
		c.TenantMap = map[string]TenantMapping{"team-a": {AccountID: "1"}, "team-b": {AccountID: "2"}}
	}}, opts...)
	p := newTestProxyWithOptions(t, all...)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func serveAuth(h http.Handler, method, path string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range header {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func isCallerAuthRefusal(rec *httptest.ResponseRecorder) bool {
	return rec.Code == http.StatusUnauthorized && len(rec.Header().Values("WWW-Authenticate")) > 0
}

// TestClientAuth_EveryRegisteredRouteIsProtectedOrExempt registers every route
// of every listener topology cmd/proxy builds (one listener; proxy, admin and
// metrics on their own listeners) with every optional route on, and checks
// each registered pattern: without a credential it is refused by caller
// authentication, unless it is in the exempt list; with one it reaches its
// own handler. A route added later is protected by default and this test
// lists it.
//
// conformance: client-authentication
func TestClientAuth_EveryRegisteredRouteIsProtectedOrExempt(t *testing.T) {
	vl := (&stubVL{}).server(t)
	p := newAuthTestProxy(t, vl.URL)

	listeners := map[string]func(RouteMux){
		"single":  p.RegisterRoutes,
		"proxy":   p.RegisterProxyRoutes,
		"admin":   p.RegisterAdminRoutes,
		"metrics": p.RegisterMetricsRoute,
	}
	seen := map[string]bool{}
	names := make([]string, 0, len(listeners))
	for name := range listeners {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		mux := newRecordingMux()
		listeners[name](mux)
		if len(mux.patterns) == 0 {
			t.Fatalf("%s: no routes registered", name)
		}
		h := p.WithClientAuth(mux.ServeMux)
		for _, pattern := range mux.patterns {
			seen[pattern] = true
			path := pathOf(pattern)
			t.Run(name+pattern, func(t *testing.T) {
				for _, method := range []string{http.MethodGet, http.MethodPost} {
					rec := serveAuth(h, method, path, nil)
					if ClientAuthExempt(pattern) {
						if isCallerAuthRefusal(rec) {
							t.Fatalf("%s %s is exempt but caller authentication refused it", method, path)
						}
						continue
					}
					if !isCallerAuthRefusal(rec) || strings.TrimSpace(rec.Body.String()) != "unauthorized" {
						t.Fatalf("%s %s without a credential: %d %q %v, want caller authentication's 401", method, path, rec.Code, rec.Body.String(), rec.Header())
					}
					ok := serveAuth(h, method, path, http.Header{"Authorization": {basicAuth("grafana", "secret")}, "X-Scope-Orgid": {"team-a"}})
					if isCallerAuthRefusal(ok) {
						t.Fatalf("%s %s with a valid credential was refused", method, path)
					}
				}
			})
		}
	}
	// The exempt list names only routes that exist.
	for pattern := range clientAuthExemptPatterns {
		if !seen[pattern] {
			t.Errorf("exempt pattern %s is registered by no listener", pattern)
		}
	}
	// A path no route holds is refused before the mux says 404, so the routes
	// cannot be listed without a credential.
	for _, path := range []string{"/nope", "/ready/", "/ready/../loki/api/v1/labels", "/%72eady/x", "//metrics"} {
		h := p.WithClientAuth(func() *http.ServeMux { m := http.NewServeMux(); p.RegisterRoutes(m); return m }())
		rec := serveAuth(h, http.MethodGet, path, nil)
		redirect := rec.Code >= 300 && rec.Code < 400
		if !isCallerAuthRefusal(rec) && !redirect {
			t.Errorf("GET %s: %d, want caller authentication's 401 or the mux's redirect to a cleaned path", path, rec.Code)
		}
		if redirect && strings.Contains(rec.Body.String(), "values") {
			t.Errorf("GET %s answered data on a redirect", path)
		}
	}
}

// TestClientAuth_OffLeavesTheMuxAsItIs: without a users file the handler is
// the mux itself, /metrics has no auth series, and Authorization is untouched.
//
// conformance: client-authentication
func TestClientAuth_OffLeavesTheMuxAsItIs(t *testing.T) {
	on := true
	p := newTestProxyWithOptions(t, func(c *Config) { c.RegisterInstrumentation = &on })
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	if h := p.WithClientAuth(mux); h != http.Handler(mux) {
		t.Fatal("WithClientAuth wrapped the mux with caller authentication off")
	}
	rec := serveAuth(mux, http.MethodGet, "/metrics", nil)
	if strings.Contains(rec.Body.String(), "loki_vl_proxy_auth_") {
		t.Fatal("/metrics carries auth series with caller authentication off")
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.SetBasicAuth("u", "p")
	if user, src := p.resolveAuth(r); user != "u" || src != "basic_auth" {
		t.Fatalf("resolveAuth off: %q %q", user, src)
	}
	if id, src := p.resolveClient(r); src == "auth_user" || id == "u" {
		t.Fatalf("resolveClient off: %q %q", id, src)
	}
}

// TestClientAuth_AnswersAndMetrics: every failure reason answers 401
// "unauthorized" with WWW-Authenticate and counts its reason; the same text
// for every reason, so a wrong password and an unknown user look alike.
//
// conformance: client-authentication
func TestClientAuth_AnswersAndMetrics(t *testing.T) {
	vl := (&stubVL{}).server(t)
	p := newAuthTestProxy(t, vl.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	h := p.WithClientAuth(mux)
	cases := map[string]struct {
		header http.Header
		reason string
	}{
		"missing":        {http.Header{}, "missing"},
		"malformed":      {http.Header{"Authorization": {"Digest x"}}, "malformed"},
		"wrong password": {http.Header{"Authorization": {basicAuth("grafana", "nope")}}, "invalid"},
		"unknown user":   {http.Header{"Authorization": {basicAuth("nobody", "secret")}}, "invalid"},
		"wrong token":    {http.Header{"Authorization": {"Bearer nope"}}, "invalid"},
		"pinned elsewhere": {http.Header{"Authorization": {basicAuth("pinned", "pinned-pw")},
			"X-Scope-Orgid": {"team-b"}}, "tenant_denied"},
	}
	var bodies []string
	for name, c := range cases {
		rec := serveAuth(h, http.MethodGet, "/loki/api/v1/labels", c.header)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: %d", name, rec.Code)
		}
		ch := rec.Header().Values("WWW-Authenticate")
		if len(ch) != 2 || !strings.HasPrefix(ch[0], `Basic realm="loki-vl-proxy"`) || !strings.HasPrefix(ch[1], `Bearer realm="loki-vl-proxy"`) {
			t.Fatalf("%s: WWW-Authenticate %q", name, ch)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
			t.Fatalf("%s: content type %q", name, ct)
		}
		bodies = append(bodies, rec.Body.String())
	}
	for _, b := range bodies {
		if b != bodies[0] || strings.TrimSpace(b) != "unauthorized" {
			t.Fatalf("bodies differ by reason: %q", bodies)
		}
	}
	out := serveAuth(h, http.MethodGet, "/metrics", nil).Body.String()
	for _, c := range cases {
		if !strings.Contains(out, fmt.Sprintf(`loki_vl_proxy_auth_failures_total{reason=%q} `, c.reason)) {
			t.Fatalf("/metrics lacks reason %s", c.reason)
		}
	}
	for _, want := range []string{`loki_vl_proxy_auth_failures_total{reason="invalid"} 3`, `loki_vl_proxy_auth_failures_total{reason="busy"} 0`, "loki_vl_proxy_auth_users_reload_failures_total 0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("/metrics lacks %q", want)
		}
	}
	// The reasons the metric knows are the authenticator's.
	if got, want := strings.Join(metrics.AuthFailureReasons[:], ","), strings.Join(clientauth.Reasons(), ","); got != want {
		t.Fatalf("metric reasons %s, authenticator reasons %s", got, want)
	}
	// No credential reaches /metrics or the request log.
	if strings.Contains(out, "secret") || strings.Contains(out, "pinned-pw") {
		t.Fatal("/metrics carries a password")
	}
}

// TestClientAuth_TenantPinAndCacheIsolation: a pinned user reads its own
// tenant whatever it sends (another tenant, a|b, two header lines are
// refused), so it never receives an answer cached for another tenant.
//
// conformance: client-authentication, tenant-auth-follows-loki
func TestClientAuth_TenantPinAndCacheIsolation(t *testing.T) {
	stub := &stubVL{}
	vl := stub.server(t)
	p := newAuthTestProxy(t, vl.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	h := p.WithClientAuth(mux)
	const path = "/loki/api/v1/labels?start=1700000000&end=1700003600"

	// grafana (no pin) fills team-b's cache.
	if rec := serveAuth(h, http.MethodGet, path, http.Header{"Authorization": {basicAuth("grafana", "secret")}, "X-Scope-Orgid": {"team-b"}}); rec.Code != http.StatusOK {
		t.Fatalf("grafana team-b: %d %s", rec.Code, rec.Body.String())
	}
	if rec := serveAuth(h, http.MethodGet, path, http.Header{"Authorization": {basicAuth("grafana", "secret")}, "X-Scope-Orgid": {"team-b"}}); rec.Code != http.StatusOK {
		t.Fatalf("grafana team-b repeat: %d", rec.Code)
	}
	before, _, accounts := stub.snapshot()
	for _, a := range accounts {
		if a != "2" {
			t.Fatalf("grafana's team-b calls went to account %q", a)
		}
	}
	// pinned (team-a) cannot ask for team-b in any spelling.
	for _, orgs := range [][]string{{"team-b"}, {"team-a|team-b"}, {"team-b|team-a"}, {"team-a", "team-b"}, {"team-a "}, {"*"}} {
		rec := serveAuth(h, http.MethodGet, path, http.Header{"Authorization": {basicAuth("pinned", "pinned-pw")}, "X-Scope-Orgid": orgs})
		if !isCallerAuthRefusal(rec) {
			t.Fatalf("pinned with X-Scope-OrgID %q: %d %s", orgs, rec.Code, rec.Body.String())
		}
	}
	if after, _, _ := stub.snapshot(); after != before {
		t.Fatalf("refused requests reached VictoriaLogs: %d calls, before %d", after, before)
	}
	// Without a header, or with its own tenant, pinned reads team-a: a call to
	// account 1, not team-b's cached answer.
	for _, orgs := range [][]string{nil, {"team-a"}} {
		hdr := http.Header{"Authorization": {basicAuth("pinned", "pinned-pw")}}
		if orgs != nil {
			hdr["X-Scope-Orgid"] = orgs
		}
		if rec := serveAuth(h, http.MethodGet, path, hdr); rec.Code != http.StatusOK {
			t.Fatalf("pinned %q: %d %s", orgs, rec.Code, rec.Body.String())
		}
	}
	after, _, accounts := stub.snapshot()
	if after == before {
		t.Fatal("pinned user was answered from another tenant's cache")
	}
	for _, a := range accounts[before:] {
		if a != "1" {
			t.Fatalf("pinned user's calls went to account %q, want 1", a)
		}
	}
	// The pin also holds on the routes outside the tenant middleware.
	for _, route := range []string{"/config/tenant/v1/limits", "/loki/api/v1/drilldown-limits"} {
		rec := serveAuth(h, http.MethodGet, route, http.Header{"Authorization": {basicAuth("pinned", "pinned-pw")}, "X-Scope-Orgid": {"team-b"}})
		if !isCallerAuthRefusal(rec) {
			t.Fatalf("%s pinned elsewhere: %d", route, rec.Code)
		}
	}
}

// TestClientAuth_CredentialIsNotForwarded: the caller's Authorization never
// reaches VictoriaLogs unless the operator forwards it, and then cache keys
// carry the credential's fingerprint (no entry shared between credentials,
// none pushed to peers), as for any forwarded credential.
//
// conformance: client-authentication, credential-passthrough-as-configured
func TestClientAuth_CredentialIsNotForwarded(t *testing.T) {
	for _, forward := range []bool{false, true} {
		t.Run(fmt.Sprint("forward=", forward), func(t *testing.T) {
			stub := &stubVL{}
			vl := stub.server(t)
			p := newAuthTestProxy(t, vl.URL, func(c *Config) {
				if forward {
					c.ForwardHeaders = []string{"authorization"}
				}
			})
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			var fingerprint string
			inner := http.NewServeMux()
			inner.HandleFunc("/loki/api/v1/labels", func(w http.ResponseWriter, r *http.Request) {
				fingerprint = p.forwardedAuthFingerprint(r)
				mux.ServeHTTP(w, r)
			})
			h := p.WithClientAuth(inner)
			cred := basicAuth("grafana", "secret")
			rec := serveAuth(h, http.MethodGet, "/loki/api/v1/labels?start=1700000000&end=1700003600", http.Header{"Authorization": {cred}, "X-Scope-Orgid": {"team-a"}})
			if rec.Code != http.StatusOK {
				t.Fatalf("%d %s", rec.Code, rec.Body.String())
			}
			calls, auths, _ := stub.snapshot()
			if calls == 0 {
				t.Fatal("no VictoriaLogs call")
			}
			for _, a := range auths {
				if forward && a != cred || !forward && a != "" {
					t.Fatalf("VictoriaLogs got Authorization %q (forward=%v)", a, forward)
				}
			}
			if forward != strings.HasPrefix(fingerprint, identityFingerprintPrefix) {
				t.Fatalf("cache fingerprint %q (forward=%v)", fingerprint, forward)
			}
		})
	}
}

// TestClientAuth_AdminAndDeleteTokens: with caller authentication on, admin
// and delete routes need both the caller's credential and their own token,
// which is then read from X-Admin-Token or X-Delete-Token only (Authorization
// is the caller's).
//
// conformance: client-authentication, delete-api-requires-token
func TestClientAuth_AdminAndDeleteTokens(t *testing.T) {
	vl := (&stubVL{}).server(t)
	for _, forward := range []bool{false, true} {
		p := newAuthTestProxy(t, vl.URL, func(c *Config) {
			if forward {
				c.ForwardHeaders = []string{"Authorization"}
			}
		})
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		h := p.WithClientAuth(mux)
		caller := basicAuth("grafana", "secret")
		cases := []struct {
			name   string
			method string
			path   string
			header http.Header
			check  func(*httptest.ResponseRecorder) bool
		}{
			{"admin: caller and X-Admin-Token", http.MethodGet, "/admin/capabilities",
				http.Header{"Authorization": {caller}, "X-Admin-Token": {authTestAdminToken}}, func(r *httptest.ResponseRecorder) bool { return r.Code == http.StatusOK }},
			{"admin: admin token as Bearer only", http.MethodGet, "/admin/capabilities",
				http.Header{"Authorization": {"Bearer " + authTestAdminToken}}, isCallerAuthRefusal},
			{"admin: caller only", http.MethodGet, "/admin/capabilities",
				http.Header{"Authorization": {caller}}, func(r *httptest.ResponseRecorder) bool {
					return r.Code == http.StatusUnauthorized && !isCallerAuthRefusal(r)
				}},
			{"admin: bearer caller and no admin token", http.MethodGet, "/admin/capabilities",
				http.Header{"Authorization": {"Bearer ci-token"}}, func(r *httptest.ResponseRecorder) bool {
					return r.Code == http.StatusUnauthorized && !isCallerAuthRefusal(r)
				}},
			{"delete: caller and X-Delete-Token", http.MethodGet, "/loki/api/v1/delete",
				http.Header{"Authorization": {caller}, "X-Delete-Token": {authTestDeleteToken}, "X-Scope-Orgid": {"team-a"}}, func(r *httptest.ResponseRecorder) bool {
					return r.Code != http.StatusUnauthorized
				}},
			{"delete: delete token as Bearer only", http.MethodGet, "/loki/api/v1/delete",
				http.Header{"Authorization": {"Bearer " + authTestDeleteToken}}, isCallerAuthRefusal},
			{"delete: caller only", http.MethodGet, "/loki/api/v1/delete",
				http.Header{"Authorization": {caller}, "X-Scope-Orgid": {"team-a"}}, func(r *httptest.ResponseRecorder) bool {
					return r.Code == http.StatusUnauthorized && strings.TrimSpace(r.Body.String()) == "no org id"
				}},
		}
		for _, c := range cases {
			rec := serveAuth(h, c.method, c.path, c.header)
			if !c.check(rec) {
				t.Fatalf("forward=%v %s: %d %q %v", forward, c.name, rec.Code, rec.Body.String(), rec.Header())
			}
		}
	}
}

// TestClientAuth_TailWebsocket: the live tail's websocket handshake is refused
// without a credential and proceeds with one.
//
// conformance: client-authentication
func TestClientAuth_TailWebsocket(t *testing.T) {
	vl := (&stubVL{}).server(t)
	p := newAuthTestProxy(t, vl.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	srv := httptest.NewServer(p.WithClientAuth(mux))
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + `/loki/api/v1/tail?query=%7Bapp%3D%22x%22%7D`
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}

	_, resp, err := dialer.Dial(url, http.Header{"X-Scope-OrgID": {"team-a"}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("tail without a credential: err=%v resp=%v", err, resp)
	}
	_ = resp.Body.Close()

	ws, resp, err := dialer.Dial(url, http.Header{"X-Scope-OrgID": {"team-a"}, "Authorization": {basicAuth("grafana", "secret")}})
	if resp != nil && resp.StatusCode == http.StatusUnauthorized {
		t.Fatalf("tail with a credential refused: %v", err)
	}
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("tail with a credential: err=%v resp=%v", err, resp)
	}
	_ = ws.Close()
}

// TestClientAuth_IdentityLabelsTheClient: the authenticated name is the
// client label of the client metrics (bounded by -metrics.max-clients like any
// client identity) and enduser.id of the caller.
//
// conformance: client-authentication
func TestClientAuth_IdentityLabelsTheClient(t *testing.T) {
	vl := (&stubVL{}).server(t)
	p := newAuthTestProxy(t, vl.URL, func(c *Config) { c.MetricsTrustProxyHeaders = true })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	h := p.WithClientAuth(mux)
	for _, hdr := range []http.Header{
		{"Authorization": {basicAuth("grafana", "secret")}, "X-Scope-Orgid": {"team-a"}, "X-Grafana-User": {"spoofed"}},
		{"Authorization": {"Bearer ci-token"}, "X-Scope-Orgid": {"team-a"}},
	} {
		if rec := serveAuth(h, http.MethodGet, "/loki/api/v1/labels?start=1700000000&end=1700003600", hdr); rec.Code != http.StatusOK {
			t.Fatalf("%d %s", rec.Code, rec.Body.String())
		}
	}
	out := serveAuth(h, http.MethodGet, "/metrics", nil).Body.String()
	for _, want := range []string{`client="grafana"`, `client="ci"`} {
		if !strings.Contains(out, "loki_vl_proxy_client_requests_total{") || !strings.Contains(out, want) {
			t.Fatalf("/metrics lacks %s", want)
		}
	}
	if strings.Contains(out, `client="spoofed"`) || strings.Contains(out, `client="team-a"`) {
		t.Fatal("a header named the client instead of the authenticated user")
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r = r.WithContext(clientauth.WithIdentity(r.Context(), clientauth.Identity{Name: "ci", Method: clientauth.MethodBearer}))
	if u, src := p.resolveAuth(r); u != "ci" || src != "bearer_token" {
		t.Fatalf("resolveAuth %q %q", u, src)
	}
	if id, src := p.resolveClient(r); id != "ci" || src != "auth_user" {
		t.Fatalf("resolveClient %q %q", id, src)
	}
	ctx := context.WithValue(r.Context(), requestRouteMetaKey, requestRouteMeta{req: r})
	if c := p.requestCallerFromContext(ctx); c.user != "ci" || c.userSource != "auth_user" {
		t.Fatalf("caller %+v", c)
	}
}

// TestClientAuth_BusyAnswersQueueFull: when every verification slot stays
// taken (clientauth's TestAuthenticate_BusyWhenEverySlotIsTaken), the request
// gets Loki's 429 too many outstanding requests, without WWW-Authenticate,
// and the busy reason is counted.
//
// conformance: client-authentication
func TestClientAuth_BusyAnswersQueueFull(t *testing.T) {
	p := newTestProxyWithOptions(t, func(c *Config) { c.ClientAuth = newTestClientAuth(t) })
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	rec := httptest.NewRecorder()
	p.refuseClientAuth(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil), clientauth.ReasonBusy)
	if rec.Code != http.StatusTooManyRequests || strings.TrimSpace(rec.Body.String()) != "too many outstanding requests" || rec.Header().Get("WWW-Authenticate") != "" {
		t.Fatalf("%d %q %v", rec.Code, rec.Body.String(), rec.Header())
	}
	mux := http.NewServeMux()
	on := true
	p.registerInstrumentation = on
	p.RegisterMetricsRoute(mux)
	if out := serveAuth(mux, http.MethodGet, "/metrics", nil).Body.String(); !strings.Contains(out, `loki_vl_proxy_auth_failures_total{reason="busy"} 1`) {
		t.Fatal("busy not counted")
	}
}

// TestValidateTenantPin uses Loki's tenant ID rules.
//
// conformance: client-authentication
func TestValidateTenantPin(t *testing.T) {
	for id, ok := range map[string]bool{"team-a": true, "0": true, "a|b": false, "..": false, "a b": false, strings.Repeat("a", 151): false} {
		if err := ValidateTenantPin(id); (err == nil) != ok {
			t.Errorf("%q: %v", id, err)
		}
	}
	if !forwardsAuthorizationHeader([]string{" AUTHORIZATION "}) || forwardsAuthorizationHeader([]string{"X-Other"}) {
		t.Fatal("forwardsAuthorizationHeader")
	}
}

// TestClientAuth_ChartProbesAndHealthcheckAreExempt: the probe paths the chart
// configures by default and the image's healthcheck (/ready) stay open, so
// turning caller authentication on never fails a probe.
//
// conformance: client-authentication
func TestClientAuth_ChartProbesAndHealthcheckAreExempt(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "charts", "loki-vl-proxy", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		Probe map[string]struct {
			Path string `yaml:"path"`
		} `yaml:"probe"`
	}
	if err := yaml.Unmarshal(data, &values); err != nil {
		t.Fatal(err)
	}
	if len(values.Probe) < 3 {
		t.Fatalf("probes %v", values.Probe)
	}
	for name, probe := range values.Probe {
		if !ClientAuthExempt(probe.Path) {
			t.Errorf("chart probe %s path %q needs a credential", name, probe.Path)
		}
	}
	src, err := os.ReadFile(filepath.Join("..", "..", "cmd", "healthcheck", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), `"/ready"`) || !ClientAuthExempt("/ready") {
		t.Fatal("the healthcheck's /ready is not exempt")
	}
}

// BenchmarkClientAuthLabelsWarm is a cached /labels request through the whole
// handler chain with caller authentication off (WithClientAuth returns the mux
// itself), with a verified Basic credential (a cache hit: one HMAC, no bcrypt)
// and with a bearer token (one SHA-256).
func BenchmarkClientAuthLabelsWarm(b *testing.B) {
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeVLFieldNames(w, []fieldHit{{"app", 100}, {"env", 50}})
	}))
	b.Cleanup(vl.Close)
	endNs := perfBaseTimeNs
	path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d&query=%%2A", endNs-int64(time.Hour), endNs)
	for _, c := range []struct {
		name   string
		auth   bool
		header string
	}{
		{"off", false, ""},
		{"basic_cached", true, basicAuth("grafana", "secret")},
		{"bearer", true, "Bearer ci-token"},
	} {
		b.Run(c.name, func(b *testing.B) {
			cfg := Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 10000), LogLevel: "error"}
			if c.auth {
				cfg.ClientAuth = newTestClientAuth(b)
			}
			p, err := New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			h := p.WithClientAuth(mux)
			req := func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, path, nil)
				if c.header != "" {
					r.Header.Set("Authorization", c.header)
				}
				return r
			}
			h.ServeHTTP(httptest.NewRecorder(), req())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req())
				if rec.Code != http.StatusOK {
					b.Fatal(rec.Code)
				}
			}
		})
	}
}
