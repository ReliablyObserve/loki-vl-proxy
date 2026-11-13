package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// FuzzLokiRouter drives the shipped router (the mux behind
// SecurityHeadersMiddleware) with any method, path, query string, Content-Type
// and body, and holds its routing decisions to Loki's: a path Loki would clean
// is a bodyless 301 to a clean path; a method other than GET and POST on a
// Loki read route is a bodyless 405 whatever else the request holds; no
// answer is a server error from the router itself, and the router never
// panics.
func FuzzLokiRouter(f *testing.F) {
	for _, s := range [][5]string{
		{"GET", "/loki/api/v1/labels", "", "", ""},
		{"PUT", "/loki/api/v1/query_range", "query=%7Ba%3D%22b%22%7D", "", ""},
		{"HEAD", "/loki/api/v1/label/app/values", "", "", ""},
		{"GET", "/loki//api/v1/labels", "start=1", "", ""},
		{"GET", "/loki/api/v1/./labels", "", "", ""},
		{"GET", "/loki/api/v1/label/a%2Fb/values", "", "", ""},
		{"POST", "/loki/api/v1/query_range", "", "application/x-www-form-urlencoded", "query=%zz"},
		{"POST", "/loki/api/v1/labels", "", "application/json", "{}"},
		{"GET", "/loki/api/v1/labels", "query=a;b", "", ""},
		{"GET", "/loki/api/v1/labels", "query=%", "", ""},
		{"OPTIONS", "/loki/api/v1/push", "", "", ""},
		{"DELETE", "/", "", "", ""},
		{"GET", "/loki/api/v1/detected_field", "", "", ""},
		{"GET", "/loki/api/v1/format_query", "query=%7B", "", ""},
	} {
		f.Add(s[0], s[1], s[2], s[3], s[4])
	}
	backend := httptest.NewServer(&errLayerBackend{failSig: "\x00none", okBody: "empty"})
	f.Cleanup(backend.Close)
	c := cache.New(time.Minute, 1000)
	f.Cleanup(c.Close)
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error", BackendTimeout: 2 * time.Second})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	handler := SecurityHeadersMiddleware(mux)
	gated := map[string]bool{}
	for _, route := range gatedRoutes {
		gated[route.target[:strings.IndexAny(route.target+"?", "?")]] = true
	}

	f.Fuzz(func(t *testing.T, method, path, rawQuery, contentType, body string) {
		if !validToken(method) || len(path) > 512 || len(rawQuery) > 512 || len(body) > 4096 {
			return
		}
		req := &http.Request{
			Method:     method,
			URL:        &url.URL{Path: path, RawQuery: rawQuery},
			Proto:      "HTTP/1.1",
			ProtoMajor: 1, ProtoMinor: 1,
			Header:        http.Header{},
			Body:          http.NoBody,
			ContentLength: 0,
			RemoteAddr:    "192.0.2.1:1",
			Host:          "proxy",
		}
		if contentType != "" && !strings.ContainsAny(contentType, "\r\n") {
			req.Header.Set("Content-Type", contentType)
		}
		if body != "" {
			req.Body = readCloser(body)
			req.ContentLength = int64(len(body))
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		resp := rec.Result()

		switch {
		case path != "" && path[0] == '/' && !pathIsClean(path):
			if resp.StatusCode != http.StatusMovedPermanently || rec.Body.Len() != 0 || resp.Header.Get("Content-Type") != "" {
				t.Fatalf("%s %q: %d with body %q, want Loki's bodyless 301", method, path, resp.StatusCode, rec.Body.String())
			}
			location, _, _ := strings.Cut(resp.Header.Get("Location"), "?")
			if location == "" || location[0] != '/' || !pathIsClean(location) {
				t.Fatalf("%s %q: Location %q is not a clean path", method, path, resp.Header.Get("Location"))
			}
		case gated[path] && method != http.MethodGet && method != http.MethodPost && !strings.HasPrefix(path, "/loki/api/v1/rules"):
			if resp.StatusCode != http.StatusMethodNotAllowed || rec.Body.Len() != 0 || resp.Header.Get("Content-Type") != "" {
				t.Fatalf("%s %q: %d with body %q, want Loki's bodyless 405", method, path, resp.StatusCode, rec.Body.String())
			}
		}
	})
}

// validToken reports whether s is an HTTP method token.
func validToken(s string) bool {
	if s == "" || len(s) > 16 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
}

func readCloser(s string) *bodyReader { return &bodyReader{Reader: strings.NewReader(s)} }

type bodyReader struct{ *strings.Reader }

func (bodyReader) Close() error { return nil }
