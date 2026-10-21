package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/gorilla/websocket"
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

// tenantLabelRecorder is a fake VictoriaLogs that records the tenant routing of
// every query it receives.
type tenantLabelRecorder struct {
	mu   sync.Mutex
	seen []url.Values
	acct []string
}

func (rec *tenantLabelRecorder) handler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	if strings.HasPrefix(r.URL.Path, "/select/logsql/") {
		rec.mu.Lock()
		rec.seen = append(rec.seen, r.Form)
		rec.acct = append(rec.acct, r.Header.Get("AccountID"))
		rec.mu.Unlock()
	}
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/select/logsql/hits":
		_, _ = io.WriteString(w, `{"hits":[]}`)
	case "/select/logsql/field_names", "/select/logsql/field_values", "/select/logsql/stream_field_names", "/select/logsql/stream_field_values":
		_, _ = io.WriteString(w, `{"values":[]}`)
	case "/select/logsql/tail":
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}
}

// wait polls until the backend has recorded a request or the deadline passes.
func (rec *tenantLabelRecorder) wait(d time.Duration) {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		rec.mu.Lock()
		n := len(rec.seen)
		rec.mu.Unlock()
		if n > 0 {
			return
		}
	}
}

// conformance: tenant-label-isolation, semantics/tenant-label-default-aliases
func TestTenantLabelRouting_DefaultAliasesNeedAllowGlobalOrMapping(t *testing.T) {
	const rng = "start=0&end=1000000000000"
	routes := []struct {
		name, method, path string
		multi, noBackend   bool
	}{
		{"query_range", "GET", `/loki/api/v1/query_range?query={app="x"}&` + rng + `&limit=10`, true, false},
		{"query", "GET", `/loki/api/v1/query?query=count_over_time({app="x"}[5m])`, true, false},
		{"labels", "GET", `/loki/api/v1/labels?` + rng, true, false},
		{"label_values", "GET", `/loki/api/v1/label/app/values?` + rng, true, false},
		{"series", "GET", `/loki/api/v1/series?match[]={app="x"}&` + rng, true, false},
		{"index_volume", "GET", `/loki/api/v1/index/volume?query={app="x"}&` + rng, true, false},
		{"detected_fields", "GET", `/loki/api/v1/detected_fields?query={app="x"}&` + rng, true, false},
		{"detected_labels", "GET", `/loki/api/v1/detected_labels?query={app="x"}&` + rng, true, false},
		{"patterns", "GET", `/loki/api/v1/patterns?query={app="x"}&` + rng, true, false},
		{"delete", "POST", `/loki/api/v1/delete?query={app="x"}&start=1700000000&end=1700003600`, false, false},
		{"rules", "GET", `/loki/api/v1/rules`, false, true},
		{"tail", "GET", `/loki/api/v1/tail?query={app="x"}`, false, false},
	}
	mappedTenant := TenantMapping{AccountID: "7", ProjectID: "8"}
	onOff := map[bool]string{false: "off", true: "on"}
	for _, org := range []string{"", "0", "fake", "default", "*", "production", "production|fake", "production|staging"} {
		for _, allowGlobal := range []bool{false, true} {
			for _, mapped := range []bool{false, true} {
				// Every tenant of the header is admitted or refused on its own.
				refused := false
				wantFilters := map[string]bool{}
				for _, tenant := range splitMultiTenantOrgIDs(org) {
					bypass := tenant == "*" || isDefaultTenantAlias(tenant)
					if !bypass {
						wantFilters[`{"org_id":"`+tenant+`"}`] = true
						continue
					}
					// A map entry admits an alias but never an absent org.
					switch {
					case allowGlobal || mapped:
						wantFilters[""] = true
					default:
						refused = true
					}
				}
				if org == "" {
					wantFilters = map[string]bool{"": true}
					refused = !allowGlobal // an entry under the empty key does not admit it
				}
				for _, rt := range routes {
					if strings.Contains(org, "|") && !rt.multi {
						continue
					}
					name := rt.name + "/org=" + org + "/allowGlobal=" + onOff[allowGlobal] + "/mapped=" + map[bool]string{false: "no", true: "yes"}[mapped]
					t.Run(name, func(t *testing.T) {
						// One backend per case: a late background request from another
						// case must not be recorded here.
						rec := &tenantLabelRecorder{}
						vl := httptest.NewServer(http.HandlerFunc(rec.handler))
						defer vl.Close()
						p := newTestProxyWithOptions(t, withBackendURL(vl.URL), withTenantLabel("org_id"), func(c *Config) {
							c.AllowGlobalTenant = allowGlobal
							if mapped {
								c.TenantMap = map[string]TenantMapping{"0": mappedTenant, "fake": mappedTenant, "default": mappedTenant, "*": mappedTenant, "": mappedTenant}
							}
						})
						mux := http.NewServeMux()
						p.RegisterRoutes(mux)
						srv := httptest.NewServer(mux)
						defer srv.Close()
						header := http.Header{"X-Delete-Confirmation": {"true"}}
						if org != "" {
							header.Set("X-Scope-OrgID", org)
						}

						wantStatus := http.StatusOK
						switch {
						case refused:
							wantStatus = http.StatusForbidden
						case rt.name == "tail":
							wantStatus = http.StatusSwitchingProtocols
						case rt.name == "delete":
							wantStatus = http.StatusNoContent
						}
						status := 0
						if rt.name == "tail" {
							ws, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+rt.path, header)
							if err == nil {
								status = resp.StatusCode
								// Keep the socket open until the backend call is seen.
								rec.wait(3 * time.Second)
								_ = ws.Close()
							} else if resp != nil {
								status = resp.StatusCode
							} else {
								t.Fatal(err)
							}
						} else {
							req, _ := http.NewRequest(rt.method, srv.URL+rt.path, nil)
							req.Header = header
							resp, err := http.DefaultClient.Do(req)
							if err != nil {
								t.Fatal(err)
							}
							body, _ := io.ReadAll(resp.Body)
							_ = resp.Body.Close()
							status = resp.StatusCode
							if status == http.StatusForbidden && !strings.Contains(string(body), "is disabled") && !strings.Contains(string(body), "not allowed in tenant-label mode") {
								t.Fatalf("refusal body: %s", body)
							}
						}
						if status != wantStatus {
							t.Fatalf("status %d, want %d", status, wantStatus)
						}

						if !refused && !rt.noBackend {
							rec.wait(3 * time.Second)
						}
						rec.mu.Lock()
						defer rec.mu.Unlock()
						if refused && len(rec.seen) != 0 {
							t.Fatalf("a refused request reached the backend: %v", rec.seen)
						}
						if !refused && !rt.noBackend && len(rec.seen) == 0 {
							t.Fatal("no backend request recorded")
						}
						for i, form := range rec.seen {
							got := form.Get("extra_stream_filters")
							if !wantFilters[got] {
								t.Fatalf("extra_stream_filters %q, want one of %v", got, wantFilters)
							}
							if got == "" && mapped && org != "" && !strings.Contains(org, "|") && rec.acct[i] != mappedTenant.AccountID && !allowGlobal {
								t.Fatalf("mapped tenant not applied: AccountID %q", rec.acct[i])
							}
						}
					})
				}
			}
		}
	}
}

// conformance: tenant-label-isolation, semantics/tenant-label-default-aliases
func TestTenantLabelRouting_RefusedAliasKeepsRequiredHeaderRule(t *testing.T) {
	p := newTestProxyWithOptions(t, withTenantLabel("org_id"), func(c *Config) {
		c.AuthEnabled = true
		c.RequireTenantHeader = true
	})
	for org, want := range map[string]int{"": http.StatusUnauthorized, "fake": http.StatusForbidden, "0": http.StatusForbidden, "default": http.StatusForbidden} {
		req := httptest.NewRequest("GET", `/loki/api/v1/labels`, nil)
		if org != "" {
			req.Header.Set("X-Scope-OrgID", org)
		}
		rec := httptest.NewRecorder()
		serveProxy(p, rec, req)
		if rec.Code != want {
			t.Fatalf("org %q: status %d, want %d", org, rec.Code, want)
		}
	}
}

// conformance: tenant-label-isolation, semantics/tenant-label-default-aliases
func TestTenantLabelRouting_ScopedParamsNeverUnfilteredWithoutGlobal(t *testing.T) {
	p := newTestProxyWithOptions(t, withTenantLabel("org_id"))
	for _, org := range []string{"", "0", "fake", "default", "*"} {
		ctx := context.Background()
		if org != "" {
			ctx = context.WithValue(ctx, orgIDKey, org)
		}
		got := p.scopedTenantParams(ctx, url.Values{"query": {"*"}}).Get("extra_stream_filters")
		// The value is one no org ID can carry, and not "", which would match
		// streams that lack the label.
		var filter map[string]string
		if err := json.Unmarshal([]byte(got), &filter); err != nil || len(filter) != 1 || filter["org_id"] != "\n" {
			t.Fatalf("org %q: filter %q", org, got)
		}
		if !strings.HasPrefix(got, `{"`) {
			t.Fatalf("org %q: VictoriaLogs reads only a filter that starts with {\" as JSON: %q", org, got)
		}
	}
	if err := p.validateSingleTenantOrgID("a\nb"); err == nil {
		t.Fatal("an org ID with a newline must be refused")
	}
}

// conformance: tenant-label-isolation, semantics/tenant-label-default-aliases
func TestTenantLabelRouting_EmptyOrgRefusalMessage(t *testing.T) {
	p := newTestProxyWithOptions(t, withTenantLabel("org_id"))
	rec := httptest.NewRecorder()
	serveProxy(p, rec, httptest.NewRequest("GET", `/loki/api/v1/labels`, nil))
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "missing X-Scope-OrgID is not allowed in tenant-label mode") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

// The other routing modes keep treating the default aliases as the default tenant.
// conformance: tenant-label-isolation, semantics/tenant-label-default-aliases
func TestTenantRouting_NonLabelModesStillAcceptDefaultAliases(t *testing.T) {
	modes := map[string]func(*Config){
		"tenant_map":   func(c *Config) { c.TenantMap = map[string]TenantMapping{"team-a": {AccountID: "1", ProjectID: "2"}} },
		"passthrough":  func(c *Config) {},
		"allow_global": func(c *Config) { c.AllowGlobalTenant = true },
	}
	for mode, opt := range modes {
		for _, org := range []string{"", "0", "fake", "default"} {
			t.Run(mode+"/org="+org, func(t *testing.T) {
				rec := &tenantLabelRecorder{}
				vl := httptest.NewServer(http.HandlerFunc(rec.handler))
				defer vl.Close()
				p := newTestProxyWithOptions(t, withBackendURL(vl.URL), opt)
				req := httptest.NewRequest("GET", `/loki/api/v1/labels?start=0&end=1000000000000`, nil)
				if org != "" {
					req.Header.Set("X-Scope-OrgID", org)
				}
				rr := httptest.NewRecorder()
				serveProxy(p, rr, req)
				if rr.Code != http.StatusOK {
					t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
				}
				rec.wait(3 * time.Second)
				rec.mu.Lock()
				defer rec.mu.Unlock()
				if len(rec.seen) == 0 {
					t.Fatal("no backend request recorded")
				}
				for _, form := range rec.seen {
					if form.Get("extra_stream_filters") != "" {
						t.Fatalf("unexpected filter %q", form.Get("extra_stream_filters"))
					}
				}
			})
		}
	}
}
