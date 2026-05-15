package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func serveProxy(p *Proxy, w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)
}

// ---------------------------------------------------------------------------
// Unit tests for injectTenantLabelFilter
// ---------------------------------------------------------------------------

func TestInjectTenantLabelFilter_InjectsIntoQueryParam(t *testing.T) {
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", "2026-01-01T00:00:00Z")

	result := injectTenantLabelFilter(params, "org_id", "production")

	got := result.Get("query")
	want := `{app="api-gateway"} {org_id="production"}`
	if got != want {
		t.Fatalf("query param after injection:\n  got:  %q\n  want: %q", got, want)
	}
	// Original must be unmodified
	if params.Get("query") != `{app="api-gateway"}` {
		t.Fatal("injectTenantLabelFilter must not mutate the original params")
	}
}

func TestInjectTenantLabelFilter_InjectsIntoQParam(t *testing.T) {
	params := url.Values{}
	params.Set("q", `{service_name="svc"}`)
	params.Set("step", "60")

	result := injectTenantLabelFilter(params, "account_id", "42")

	got := result.Get("q")
	want := `{service_name="svc"} {account_id="42"}`
	if got != want {
		t.Fatalf("q param after injection:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestInjectTenantLabelFilter_NoopWhenNoQueryParam(t *testing.T) {
	params := url.Values{}
	params.Set("start", "2026-01-01T00:00:00Z")

	result := injectTenantLabelFilter(params, "org_id", "production")

	if result.Get("start") != "2026-01-01T00:00:00Z" {
		t.Fatal("non-query params must be preserved unchanged")
	}
	if result.Get("query") != "" || result.Get("q") != "" {
		t.Fatal("no query/q param must be added when none existed")
	}
}

func TestInjectTenantLabelFilter_EscapesDoubleQuotesInOrgID(t *testing.T) {
	params := url.Values{}
	params.Set("query", `{app="x"}`)

	result := injectTenantLabelFilter(params, "org", `tenant"with"quotes`)

	got := result.Get("query")
	want := `{app="x"} {org="tenant\"with\"quotes"}`
	if got != want {
		t.Fatalf("double-quote escaping:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestInjectTenantLabelFilter_QueryParamTakesPriorityOverQParam(t *testing.T) {
	params := url.Values{}
	params.Set("query", `{app="a"}`)
	params.Set("q", `{app="b"}`)

	result := injectTenantLabelFilter(params, "org_id", "x")

	if result.Get("query") != `{app="a"} {org_id="x"}` {
		t.Fatalf("expected injection into query, got %q", result.Get("query"))
	}
	if result.Get("q") != `{app="b"}` {
		t.Fatalf("expected q param unchanged, got %q", result.Get("q"))
	}
}

// ---------------------------------------------------------------------------
// Integration tests for tenant label routing end-to-end dispatch
// ---------------------------------------------------------------------------

type testProxyOption func(*Config)

func withBackendURL(u string) testProxyOption {
	return func(cfg *Config) {
		cfg.BackendURL = u
	}
}

func withTenantLabel(label string) testProxyOption {
	return func(cfg *Config) {
		cfg.TenantLabel = label
	}
}

func newTestProxyWithOptions(t *testing.T, opts ...testProxyOption) *Proxy {
	t.Helper()
	c := cache.New(60*time.Second, 1000)
	cfg := Config{
		BackendURL: "http://unused",
		Cache:      c,
		LogLevel:   "error",
	}
	for _, o := range opts {
		o(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

func TestTenantLabelRouting_InjectsLabelFilterIntoVLQuery(t *testing.T) {
	var receivedQuery string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		receivedQuery = r.FormValue("query")
		if receivedQuery == "" {
			receivedQuery = r.URL.Query().Get("query")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"streams","result":[],"stats":{}}}`)
	}))
	defer vl.Close()

	p := newTestProxyWithOptions(t, withBackendURL(vl.URL), withTenantLabel("org_id"))

	req := httptest.NewRequest("GET",
		`/loki/api/v1/query_range?query={app="api-gateway"}&start=0&end=1000000000000&limit=10`,
		nil)
	req.Header.Set("X-Scope-OrgID", "production")

	rec := httptest.NewRecorder()
	serveProxy(p, rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(receivedQuery, `{org_id="production"}`) {
		t.Fatalf("expected VL query to contain {org_id=\"production\"}, got: %q", receivedQuery)
	}
}

func TestTenantLabelRouting_DefaultAliasSkipsLabelFilter(t *testing.T) {
	var receivedQuery string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		receivedQuery = r.FormValue("query")
		if receivedQuery == "" {
			receivedQuery = r.URL.Query().Get("query")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"streams","result":[],"stats":{}}}`)
	}))
	defer vl.Close()

	p := newTestProxyWithOptions(t, withBackendURL(vl.URL), withTenantLabel("org_id"))

	for _, alias := range []string{"0", "fake", "default"} {
		receivedQuery = ""
		req := httptest.NewRequest("GET",
			`/loki/api/v1/query_range?query={app="x"}&start=0&end=1000000000000&limit=10`,
			nil)
		req.Header.Set("X-Scope-OrgID", alias)

		rec := httptest.NewRecorder()
		serveProxy(p, rec, req)

		if strings.Contains(receivedQuery, "org_id") {
			t.Fatalf("alias %q must not inject label filter, got query: %q", alias, receivedQuery)
		}
	}
}
