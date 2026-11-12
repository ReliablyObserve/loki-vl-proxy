package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// conformance: backend-url-path-prefix
func TestBackendRequestURLKeepsTheConfiguredPath(t *testing.T) {
	for _, tc := range []struct{ base, path, want string }{
		{"http://vl:9428", "/select/logsql/query", "http://vl:9428/select/logsql/query"},
		{"http://vl:9428/", "/health", "http://vl:9428/health"},
		{"https://o11y.example.com/logs", "/select/logsql/query", "https://o11y.example.com/logs/select/logsql/query"},
		{"https://o11y.example.com/logs/", "/health", "https://o11y.example.com/logs/health"},
		{"https://o11y.example.com/a/b", "/metrics", "https://o11y.example.com/a/b/metrics"},
		{"https://user:pw@o11y.example.com/logs", "/health", "https://user:pw@o11y.example.com/logs/health"},
		{"https://o11y.example.com/a%2Fb", "/health", "https://o11y.example.com/a%2Fb/health"},
	} {
		base, err := url.Parse(tc.base)
		if err != nil {
			t.Fatal(err)
		}
		u := backendRequestURL(base, tc.path)
		if got := u.String(); got != tc.want {
			t.Errorf("backendRequestURL(%q, %q) = %q, want %q", tc.base, tc.path, got, tc.want)
		}
		if base.String() != tc.base {
			t.Errorf("backendRequestURL changed its base to %q", base.String())
		}
	}
}

// prefixedBackend is a VictoriaLogs (and ruler, and cold backend) served under prefix only, as behind a reverse
// proxy that shares one hostname: every request outside the prefix is recorded and answered 404.
func prefixedBackend(t *testing.T, prefix string) (*httptest.Server, func() (seen, outside []string)) {
	t.Helper()
	var mu sync.Mutex
	var seen, outside []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.Path)
		mu.Unlock()
		path, ok := strings.CutPrefix(r.URL.Path, prefix)
		if !ok || !strings.HasPrefix(path, "/") {
			mu.Lock()
			outside = append(outside, r.URL.Path)
			mu.Unlock()
			http.NotFound(w, r)
			return
		}
		switch {
		case path == "/health":
			_, _ = io.WriteString(w, "OK")
		case path == "/select/logsql/query":
			_, _ = io.WriteString(w, `{"_time":"2026-11-12T08:00:00Z","_msg":"under the prefix","_stream":"{app=\"x\"}","app":"x"}`+"\n")
		case path == "/api/v1/rules":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"success","data":{"groups":[]}}`)
		case path == "/manifest/range":
			_, _ = io.WriteString(w, `{"minTime":0,"maxTime":0}`)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"values":[]}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() ([]string, []string) {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...), append([]string(nil), outside...)
	}
}

// A backend URL with a path (-backend=https://host/logs) keeps that path on every request the proxy sends:
// queries, label lookups, the readiness probe and the ruler, with or without a trailing slash.
//
// conformance: backend-url-path-prefix
func TestBackendUnderAPathPrefixReceivesEveryRequest(t *testing.T) {
	for _, suffix := range []string{"/logs", "/logs/"} {
		t.Run(suffix, func(t *testing.T) {
			srv, requests := prefixedBackend(t, "/logs")
			ruler, rulerRequests := prefixedBackend(t, "/vmalert")
			_, mux := newAlertProxyMux(t, Config{BackendURL: srv.URL + suffix, RulerBackendURL: ruler.URL + "/vmalert"})
			now := time.Date(2026, 11, 12, 8, 30, 0, 0, time.UTC)
			window := url.Values{"start": {now.Add(-time.Hour).Format(time.RFC3339)}, "end": {now.Format(time.RFC3339)}}
			for _, target := range []string{
				"/loki/api/v1/query_range?" + url.Values{"query": {`{app="x"}`}, "start": window["start"], "end": window["end"]}.Encode(),
				"/loki/api/v1/labels?" + window.Encode(),
				"/ready",
				"/loki/api/v1/rules",
			} {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
				if rec.Code != http.StatusOK {
					t.Errorf("%s: status %d, body %.300s", target, rec.Code, rec.Body.String())
				}
				if strings.HasPrefix(target, "/loki/api/v1/query_range") && !strings.Contains(rec.Body.String(), "under the prefix") {
					t.Errorf("%s: the answer does not carry the backend's line: %.300s", target, rec.Body.String())
				}
			}
			seen, outside := requests()
			if len(outside) > 0 {
				t.Errorf("requests outside the backend's path: %v", outside)
			}
			for _, want := range []string{"/logs/select/logsql/query", "/logs/health"} {
				if !containsString(seen, want) {
					t.Errorf("the backend never saw %s (saw %v)", want, seen)
				}
			}
			rulerSeen, rulerOutside := rulerRequests()
			if len(rulerOutside) > 0 || !containsString(rulerSeen, "/vmalert/api/v1/rules") {
				t.Errorf("ruler requests %v, outside its path %v", rulerSeen, rulerOutside)
			}
		})
	}
}

// The cold backend keeps the path of its URL too, for its manifest and both query routes.
//
// conformance: backend-url-path-prefix
func TestColdBackendUnderAPathPrefix(t *testing.T) {
	srv, requests := prefixedBackend(t, "/cold")
	cr, err := NewColdRouter(ColdBackendConfig{Enabled: true, URL: srv.URL + "/cold/", ManifestRefresh: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || cr == nil {
		t.Fatalf("new cold router: %v", err)
	}
	ctx := context.Background()
	cr.refreshManifest(ctx)
	for _, do := range []func() (*http.Response, error){
		func() (*http.Response, error) {
			return cr.ColdGet(ctx, "/select/logsql/query", url.Values{"query": {"*"}})
		},
		func() (*http.Response, error) {
			return cr.ColdPost(ctx, "/select/logsql/query", url.Values{"query": {"*"}})
		},
	} {
		resp, err := do()
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("cold query status %d", resp.StatusCode)
		}
	}
	seen, outside := requests()
	if len(outside) > 0 {
		t.Errorf("cold requests outside the backend's path: %v", outside)
	}
	for _, want := range []string{"/cold/manifest/range", "/cold/select/logsql/query"} {
		if !containsString(seen, want) {
			t.Errorf("the cold backend never saw %s (saw %v)", want, seen)
		}
	}
}
