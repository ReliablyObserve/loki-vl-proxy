package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestLabelsAliasPreservesTenantAdmission(t *testing.T) {
	backend, calls := fieldNamesServer(t, []string{"app"})
	p, err := New(Config{BackendURL: backend.URL, AuthEnabled: true, RequireTenantHeader: true, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	var canonical int
	for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/label"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if canonical == 0 {
			canonical = w.Code
		}
		if w.Code < 400 || w.Code != canonical {
			t.Fatalf("%s bypassed tenant admission: status=%d canonical=%d", path, w.Code, canonical)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("rejected request reached backend: %d calls", got)
	}
}

// conformance: loki_api_v1_label, loki_api_v1_labels
func TestLabelsAliasSharesCanonicalHandlerAndMetadataCache(t *testing.T) {
	for _, first := range []string{"/loki/api/v1/labels", "/loki/api/v1/label"} {
		t.Run(first, func(t *testing.T) {
			backend, calls := fieldNamesServer(t, []string{"app", "env"})
			_, mux := newBehaviorProxy(t, backend.URL, 0)
			params := url.Values{"start": {"1790871720000000000"}, "end": {"1790875320000000000"}}
			get := func(method, path string) *httptest.ResponseRecorder {
				t.Helper()
				req := httptest.NewRequest(method, path+"?"+params.Encode(), nil)
				if method == http.MethodPost {
					req = httptest.NewRequest(method, path, strings.NewReader(params.Encode()))
					req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				}
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, req)
				if w.Code != http.StatusOK {
					t.Fatalf("%s %s: status=%d body=%s", method, path, w.Code, w.Body.String())
				}
				if w.Header().Get("Location") != "" {
					t.Fatalf("alias must serve directly, got Location=%q", w.Header().Get("Location"))
				}
				return w
			}
			initial := get(http.MethodGet, first)
			baseline := calls.Load()
			if baseline == 0 {
				t.Fatal("cold request did not reach backend")
			}
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				for _, path := range []string{"/loki/api/v1/label", "/loki/api/v1/labels"} {
					got := get(method, path)
					if got.Body.String() != initial.Body.String() {
						t.Fatalf("%s %s response differs: %s vs %s", method, path, got.Body.String(), initial.Body.String())
					}
					if got := calls.Load(); got != baseline {
						t.Fatalf("alias duplicated backend read: calls=%d want=%d", got, baseline)
					}
				}
			}
		})
	}
}
