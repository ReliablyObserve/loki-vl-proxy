package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// tenantFailure is how the backend answers tenant-a (AccountID 10) while a
// failure is injected: an outage, a timeout or a query rejection.
type tenantFailure struct {
	name       string
	status     int
	body       string
	wantStatus int
}

var tenantFailureModes = []tenantFailure{
	{name: "backend_failure", status: http.StatusServiceUnavailable, body: "backend unavailable", wantStatus: http.StatusInternalServerError},
	{name: "timeout", status: http.StatusGatewayTimeout, body: "timeout while executing the query", wantStatus: http.StatusGatewayTimeout},
	{name: "query_rejected", status: http.StatusBadRequest, body: vlBodyParsePattern, wantStatus: http.StatusBadRequest},
}

// newTenantFailureBackend serves VictoriaLogs responses for two tenants. While
// fail holds a mode, every request for tenant-a fails with that mode's answer.
func newTenantFailureBackend(t *testing.T, fail *atomic.Pointer[tenantFailure]) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		name := "beta"
		if r.Header.Get("AccountID") == "10" {
			if mode := fail.Load(); mode != nil {
				http.Error(w, mode.body, mode.status)
				return
			}
			name = "alpha"
		}
		switch r.URL.Path {
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/stream+json")
			for _, id := range []string{"1", "2", "3", "4"} {
				_, _ = w.Write([]byte(`{"_time":"2024-01-01T00:00:0` + id + `Z","_stream":"{app=\"api\"}","app":"api","_msg":"` + name + `_pattern request id=` + id + ` done","` + name + `_field":"v"}` + "\n"))
			}
		case "/select/logsql/stats_query_range":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"` + name + `-app"},"values":[[1704063600,"5"],[1704063900,"7"]]}]}}`))
		case "/select/logsql/hits":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"hits":[{"fields":{"app":"` + name + `-app"},"timestamps":["2024-01-01T00:00:00Z"],"values":[5],"total":5}]}`))
		case "/select/logsql/streams":
			writeVLFieldNames(w, []fieldHit{{`{app="api",` + name + `_label="x"}`, 1}})
		case "/select/logsql/stream_field_names":
			writeVLFieldNames(w, []fieldHit{{"app", 1}, {name + "_label", 1}})
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			writeVLFieldValues(w, []fieldHit{{name + "-value", 1}})
		default:
			writeVLFieldNames(w, []fieldHit{{"app", 1}, {name + "_field", 1}})
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Loki fails a multi-tenant request as a whole when any tenant fails. The
// proxy answers with Loki's status (500 for a backend failure, 504 for a
// timeout, 400 for a rejected query), merges nothing, caches nothing in the
// multi-tenant merge cache or the compatibility-edge cache, and returns the
// complete result once the tenant recovers.
func TestMultiTenantFanout_TenantFailureFailsWholeRequest(t *testing.T) {
	const window = "start=1703980800000000000&end=1704067200000000000"
	const selector = "query=%7Bapp%3D%22api%22%7D&"
	families := []struct {
		name     string
		endpoint string
		path     string
		alpha    string
		beta     string
		// rejectOnly marks endpoints whose single-tenant handler answers a
		// backend outage or timeout with an empty 200 (patterns falls back to
		// its last snapshot), so only a rejected query fails a tenant there.
		rejectOnly bool
	}{
		{name: "generic", endpoint: "label_values", path: "/loki/api/v1/label/app/values?" + window, alpha: "alpha-value", beta: "beta-value"},
		{name: "detected_fields", endpoint: "detected_fields", path: "/loki/api/v1/detected_fields?" + selector + window, alpha: "alpha_field", beta: "beta_field"},
		{name: "detected_labels", endpoint: "detected_labels", path: "/loki/api/v1/detected_labels?" + selector + window, alpha: "alpha_label", beta: "beta_label"},
		{name: "volume", endpoint: "volume", path: "/loki/api/v1/index/volume?" + selector + window, alpha: "alpha-app", beta: "beta-app"},
		{name: "patterns", endpoint: "patterns", path: "/loki/api/v1/patterns?" + selector + window, alpha: "alpha_pattern", beta: "beta_pattern", rejectOnly: true},
	}
	for _, family := range families {
		for _, mode := range tenantFailureModes {
			if family.rejectOnly && mode.wantStatus != http.StatusBadRequest {
				continue
			}
			t.Run(family.name+"/"+mode.name, func(t *testing.T) {
				var fail atomic.Pointer[tenantFailure]
				fail.Store(&mode)
				srv := newTenantFailureBackend(t, &fail)
				p, compat, mux := newTwoTenantProxy(t, srv.URL)

				newReq := func() *http.Request {
					req := httptest.NewRequest(http.MethodGet, family.path, nil)
					req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
					return req
				}
				mtKey, mtCacheable := p.multiTenantCacheKey(newReq(), family.endpoint)
				compatKey, compatCacheable := p.compatCacheKey(family.endpoint, newReq())
				if !mtCacheable || !compatCacheable {
					t.Fatalf("request not cacheable: merge cache %v, compatibility-edge cache %v", mtCacheable, compatCacheable)
				}

				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, newReq())
				body := rec.Body.String()
				if rec.Code != mode.wantStatus {
					t.Fatalf("status = %d, want %d; body=%s", rec.Code, mode.wantStatus, body)
				}
				if strings.Contains(body, family.beta) || strings.Contains(body, `"warnings"`) || rec.Header().Get("X-Multi-Tenant-Partial-Failures") != "" {
					t.Fatalf("failed request carries merged or partial data: headers=%v body=%s", rec.Header(), body)
				}
				if !strings.Contains(body, `"status":"error"`) {
					t.Fatalf("failure body is not the proxy error envelope: %s", body)
				}
				if _, cached := p.cache.Get(mtKey); cached {
					t.Fatal("failed request stored in the multi-tenant merge cache")
				}
				if _, cached := compat.Get(compatKey); cached {
					t.Fatal("failed request captured by the compatibility-edge cache")
				}

				fail.Store(nil)
				rec = httptest.NewRecorder()
				mux.ServeHTTP(rec, newReq())
				body = rec.Body.String()
				if rec.Code != http.StatusOK || !strings.Contains(body, family.alpha) || !strings.Contains(body, family.beta) {
					t.Fatalf("after recovery: want 200 with %s and %s, got %d %s", family.alpha, family.beta, rec.Code, body)
				}
				// Positive control: the complete merge is cached under the same
				// keys, so the absence checks above are not vacuous.
				if _, cached := p.cache.Get(mtKey); !cached {
					t.Fatal("complete merged response not stored in the multi-tenant merge cache")
				}
				if _, cached := compat.Get(compatKey); !cached {
					t.Fatal("complete merged response not captured by the compatibility-edge cache")
				}
			})
		}
	}
}

// A Grafana-sourced stats failure on one tenant is answered by that tenant's
// handler with a 200 Drilldown partial-results reply. It is still a failed
// tenant: the whole multi-tenant request fails with Loki's status and nothing
// is merged or cached.
func TestMultiTenantFanout_DrilldownPartialTenantFailsWholeRequest(t *testing.T) {
	const path = "/loki/api/v1/query_range?query=sum+by+%28app%29+%28count_over_time%28%7Bapp%3D%22api%22%7D%5B5m%5D%29%29&start=1704063600000000000&end=1704067200000000000&step=300"
	for _, mode := range tenantFailureModes {
		if mode.wantStatus == http.StatusBadRequest {
			// A rejected query is a 400, never a partial reply; covered above.
			continue
		}
		t.Run(mode.name, func(t *testing.T) {
			var fail atomic.Pointer[tenantFailure]
			fail.Store(&mode)
			srv := newTenantFailureBackend(t, &fail)
			p, compat, mux := newTwoTenantProxy(t, srv.URL)

			newReq := func(orgID string) *http.Request {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Header.Set("X-Scope-OrgID", orgID)
				req.Header.Set("User-Agent", "Grafana/12.0.0")
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
				return req
			}
			// Precondition: the failing tenant alone gets the partial reply.
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, newReq("tenant-a"))
			if rec.Code != http.StatusOK || rec.Header().Get("X-Proxy-Upstream-Status") == "" || !strings.Contains(rec.Body.String(), `"warnings"`) {
				t.Fatalf("single-tenant failure is not a Drilldown partial reply: %d headers=%v body=%s", rec.Code, rec.Header(), rec.Body.String())
			}

			mtKey, mtCacheable := p.multiTenantCacheKey(newReq("tenant-a|tenant-b"), "query_range")
			compatKey, compatCacheable := p.compatCacheKey("query_range", newReq("tenant-a|tenant-b"))
			if !mtCacheable || !compatCacheable {
				t.Fatalf("request not cacheable: merge cache %v, compatibility-edge cache %v", mtCacheable, compatCacheable)
			}
			rec = httptest.NewRecorder()
			mux.ServeHTTP(rec, newReq("tenant-a|tenant-b"))
			body := rec.Body.String()
			if rec.Code != mode.wantStatus || strings.Contains(body, "beta-app") || !strings.Contains(body, `"status":"error"`) {
				t.Fatalf("want the whole request to fail with %d, got %d body=%s", mode.wantStatus, rec.Code, body)
			}
			if _, cached := p.cache.Get(mtKey); cached {
				t.Fatal("failed request stored in the multi-tenant merge cache")
			}
			if _, cached := compat.Get(compatKey); cached {
				t.Fatal("failed request captured by the compatibility-edge cache")
			}

			fail.Store(nil)
			rec = httptest.NewRecorder()
			mux.ServeHTTP(rec, newReq("tenant-a|tenant-b"))
			body = rec.Body.String()
			if rec.Code != http.StatusOK || !strings.Contains(body, "alpha-app") || !strings.Contains(body, "beta-app") || strings.Contains(body, `"warnings"`) {
				t.Fatalf("after recovery: want 200 with both tenants, got %d %s", rec.Code, body)
			}
			if _, cached := p.cache.Get(mtKey); !cached {
				t.Fatal("complete merged response not stored in the multi-tenant merge cache")
			}
			if _, cached := compat.Get(compatKey); !cached {
				t.Fatal("complete merged response not captured by the compatibility-edge cache")
			}
		})
	}
}

// Loki validates the query before splitting by tenant, so a query rejected by
// one tenant is a 400 even when an earlier tenant hit a backend failure.
func TestMultiTenantFanout_RejectedQueryWinsOverBackendFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("AccountID") {
		case "10":
			http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
		case "20":
			http.Error(w, vlBodyParsePattern, http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)
	const window = "start=1703980800000000000&end=1704067200000000000"
	for _, path := range []string{
		"/loki/api/v1/label/app/values?" + window,
		"/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&" + window,
		"/loki/api/v1/detected_labels?query=%7Bapp%3D%22api%22%7D&" + window,
	} {
		_, _, mux := newTwoTenantProxy(t, srv.URL)
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400; body=%s", path, rec.Code, rec.Body.String())
		}
	}
}
