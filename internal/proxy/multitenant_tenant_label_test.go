package proxy

import (
	"context"
	"encoding/json"
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

func TestInjectTenantLabelFilter_ServerConstraintPreservesQuery(t *testing.T) {
	for _, org := range []string{"production", "42", `tenant"with"quotes`, `tenant\path`, `tenant\"`} {
		for _, queryKey := range []string{"query", "q", ""} {
			params := url.Values{"start": {"2026-01-01T00:00:00Z"}, "extra_stream_filters": {`{"org":"attacker"}`}}
			if queryKey != "" {
				params.Set(queryKey, `* | sort by (_time desc)`)
			}
			before := params.Encode()
			result := injectTenantLabelFilter(params, "org", org)
			var filter map[string]string
			if err := json.Unmarshal([]byte(result.Get("extra_stream_filters")), &filter); err != nil {
				t.Fatal(err)
			}
			if filter["org"] != org {
				t.Fatalf("constraint: %v", filter)
			}
			if result.Get(queryKey) != params.Get(queryKey) || before != params.Encode() {
				t.Fatal("query or original parameters mutated")
			}
		}
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
		receivedQuery = r.FormValue("extra_stream_filters")
		if receivedQuery == "" {
			receivedQuery = r.URL.Query().Get("extra_stream_filters")
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
	if !strings.Contains(receivedQuery, `{"org_id":"production"}`) {
		t.Fatalf("expected VL query to contain {org_id=\"production\"}, got: %q", receivedQuery)
	}
}

func TestTenantLabelRouting_TailInjectsLabelFilter(t *testing.T) {
	var receivedQuery string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.URL.Query().Get("extra_stream_filters")
		// Respond with a streaming body that closes immediately so openNativeTailStream
		// gets a non-200 from a minimal handler — we only care about the URL it called.
		w.WriteHeader(http.StatusOK)
	}))
	defer vl.Close()

	p := newTestProxyWithOptions(t, withBackendURL(vl.URL), withTenantLabel("org_id"))

	// Build a context that carries the orgID, mirroring what withOrgID does for normal requests.
	ctx := context.WithValue(context.Background(), orgIDKey, "production")

	// Call openNativeTailStream directly — this is the function that builds the VL URL by
	// hand and therefore bypasses vlGetInner's automatic tenant-label injection.
	resp, _, _ := p.openNativeTailStream(ctx, `{app="api-gateway"}`)
	if resp != nil {
		_ = resp.Body.Close()
	}

	if !strings.Contains(receivedQuery, `"org_id":"production"`) {
		t.Fatalf("expected tail VL query to contain tenant label filter, got: %q", receivedQuery)
	}
}

func TestTenantLabelRouting_DefaultAliasSkipsLabelFilter(t *testing.T) {
	var receivedQuery string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		receivedQuery = r.FormValue("extra_stream_filters")
		if receivedQuery == "" {
			receivedQuery = r.URL.Query().Get("extra_stream_filters")
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
