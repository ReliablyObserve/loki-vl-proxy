package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// The tables below hold Loki 3.7.9's answers recorded over HTTP (auth
// disabled, 2026-11-13): the request as sent and the status, Content-Type,
// X-Content-Type-Options and body a client read. A row names the recorded
// request it comes from (rNNNN in internal/lokierr/testdata/
// loki_error_answers.jsonl, or its catalogue run). Each is replayed against
// the proxy as shipped and its answer must equal Loki's byte for byte.

// bodyless is Loki's router answer to a method the route does not serve.
var bodyless405 = lokiErrorReply{Status: http.StatusMethodNotAllowed}

// notFound is Loki's answer to a path no route matches.
var lokiNotFound = lokiErrorReply{Status: http.StatusNotFound, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: "404 page not found\n"}

// movedPermanently is Loki's answer to a path it would clean.
var lokiMovedPermanently = lokiErrorReply{Status: http.StatusMovedPermanently}

func lokiFrontendError(status int, text string) lokiErrorReply {
	return lokiErrorReply{Status: status, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: text}
}

const formType = "Content-Type: application/x-www-form-urlencoded"

// gatedRoutes are the Loki read routes with a path and a valid request
// target for each, by the proxy's endpoint name.
var gatedRoutes = []struct {
	endpoint string
	target   string
	// recorded is false for a route Loki's methods were not recorded on: it
	// is registered beside the recorded ones.
	recorded bool
}{
	{"query_range", "/loki/api/v1/query_range?query=%7Bapp%3D%22a%22%7D", true},
	{"query", "/loki/api/v1/query?query=%7Bapp%3D%22a%22%7D", true},
	{"labels", "/loki/api/v1/labels", true},
	{"labels", "/loki/api/v1/label", false},
	{"label_values", "/loki/api/v1/label/app/values", true},
	{"series", "/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22a%22%7D", true},
	{"index_stats", "/loki/api/v1/index/stats?query=%7Bapp%3D%22a%22%7D", true},
	{"volume", "/loki/api/v1/index/volume?query=%7Bapp%3D%22a%22%7D", true},
	{"volume_range", "/loki/api/v1/index/volume_range?query=%7Bapp%3D%22a%22%7D", false},
	{"patterns", "/loki/api/v1/patterns?query=%7Bapp%3D%22a%22%7D", true},
	{"detected_fields", "/loki/api/v1/detected_fields?query=%7Bapp%3D%22a%22%7D", true},
	{"detected_field_values", "/loki/api/v1/detected_field/level/values?query=%7Bapp%3D%22a%22%7D", false},
	{"detected_labels", "/loki/api/v1/detected_labels?query=%7Bapp%3D%22a%22%7D", true},
	{"tail", "/loki/api/v1/tail?query=%7Bapp%3D%22a%22%7D", true},
	{"format_query", "/loki/api/v1/format_query?query=%7Bapp%3D%22a%22%7D", true},
}

// TestLokiRouterMethods: every Loki read route serves GET and POST, and
// answers every other method with the router's 405: no body, no Content-Type,
// no nosniff (r1205-r1282: PUT, DELETE, PATCH, HEAD and OPTIONS on each
// recorded route). GET and POST reach the handler.
// conformance: semantics/router-method-not-allowed-like-loki, status-405
func TestLokiRouterMethods(t *testing.T) {
	addr := newLokiErrorReplayProxy(t)
	for _, route := range gatedRoutes {
		for _, method := range []string{"PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "FOO"} {
			name := route.endpoint + " " + route.target + " " + method
			if !route.recorded {
				name += " (not recorded: registered beside its siblings)"
			}
			t.Run(name, func(t *testing.T) {
				row := lokiErrorAnswer{ID: name, Method: method, Target: route.target, Headers: []string{"X-Scope-OrgID: 0", formType}}
				if got := row.replay(t, addr); got != bodyless405 {
					t.Errorf("answer %+v, want Loki's bodyless 405", got)
				}
			})
		}
		for _, method := range []string{"GET", "POST"} {
			t.Run(route.endpoint+" "+method+" is served", func(t *testing.T) {
				row := lokiErrorAnswer{ID: route.endpoint, Method: method, Target: route.target, Headers: []string{"X-Scope-OrgID: 0", formType}}
				if got := row.replay(t, addr); got.Status == http.StatusMethodNotAllowed || got.Status == http.StatusNotFound {
					t.Errorf("%s answered %d", method, got.Status)
				}
			})
		}
	}
}

// TestLokiRouterRulesAreReadOnly: the ruler's read routes serve GET alone
// (POST to /rules is Loki's bodyless 405, r1199); the proxy has no ruler to
// write to, so the other methods on the nested and alerts routes answer the
// same (a documented limit: Loki creates and deletes rule groups there).
// conformance: semantics/router-method-not-allowed-like-loki, status-405
func TestLokiRouterRulesAreReadOnly(t *testing.T) {
	addr := newLokiErrorReplayProxy(t)
	for _, target := range []string{"/loki/api/v1/rules", "/loki/api/v1/rules/ns", "/loki/api/v1/rules/ns/grp", "/api/prom/rules", "/prometheus/api/v1/rules", "/loki/api/v1/alerts", "/api/prom/alerts", "/prometheus/api/v1/alerts"} {
		for _, method := range []string{"POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"} {
			row := lokiErrorAnswer{ID: target, Method: method, Target: target, Headers: []string{"X-Scope-OrgID: 0", formType}}
			if got := row.replay(t, addr); got != bodyless405 {
				t.Errorf("%s %s answered %+v, want Loki's bodyless 405", method, target, got)
			}
		}
		row := lokiErrorAnswer{ID: target, Method: "GET", Target: target, Headers: []string{"X-Scope-OrgID: 0"}}
		if got := row.replay(t, addr); got.Status == http.StatusMethodNotAllowed {
			t.Errorf("GET %s answered 405", target)
		}
	}
}

// TestLokiRouterPaths: a path no route matches is 404 `404 page not found`,
// and a path Loki would clean is its empty 301, before the method is looked
// at.
// conformance: semantics/router-unknown-paths-and-redirects-like-loki, status-404
func TestLokiRouterPaths(t *testing.T) {
	addr := newLokiErrorReplayProxy(t)
	rows := []struct {
		id, method, target string
		want               lokiErrorReply
	}{
		// Unknown paths (r1343-r1358, r1363-r1368).
		{"r1343", "GET", "/loki/api/v1/nosuch", lokiNotFound},
		{"r1344", "GET", "/loki/api/v1/", lokiNotFound},
		{"r1345", "GET", "/loki/api/v1", lokiNotFound},
		{"r1346", "GET", "/loki/nosuch", lokiNotFound},
		{"r1347", "GET", "/nosuch", lokiNotFound},
		{"r1348", "GET", "/loki/api/v1/labels/", lokiNotFound},
		{"r1351", "GET", "/loki/api/v1/query_range/", lokiNotFound},
		{"r1352", "GET", "/LOKI/API/V1/LABELS", lokiNotFound},
		{"r1353", "GET", "/loki/api/v1/query_range/extra", lokiNotFound},
		{"r1354", "GET", "/loki/api/v1/index", lokiNotFound},
		{"r1355", "GET", "/loki/api/v1/index/", lokiNotFound},
		{"r1356", "GET", "/loki/api/v1/index/nosuch", lokiNotFound},
		{"r1363", "GET", "/loki/api/v2/labels", lokiNotFound},
		{"r1364", "GET", "/loki/api/v1/query_range%00", lokiNotFound},
		{"r1365", "GET", "/", lokiNotFound},
		{"r1367", "POST", "/loki/api/v1/nosuch", lokiNotFound},
		{"r1368", "DELETE", "/loki/api/v1/nosuch", lokiNotFound},
		// The name routes: a missing, repeated or extra element is not the route.
		{"r1159", "GET", "/loki/api/v1/label/a/b/values?start=1794567075&end=1794570675", lokiNotFound},
		{"r1349", "GET", "/loki/api/v1/label/", lokiNotFound},
		{"r1357", "GET", "/loki/api/v1/detected_field/", lokiNotFound},
		{"r1358", "GET", "/loki/api/v1/detected_field/a", lokiNotFound},
		{"detected_field without a slash", "GET", "/loki/api/v1/detected_field", lokiNotFound},
		{"label name with an encoded slash", "GET", "/loki/api/v1/label/a%2Fb/values", lokiNotFound},
		{"detected field name with an encoded slash", "GET", "/loki/api/v1/detected_field/a%2Fb/values", lokiNotFound},
		{"label values with a trailing slash", "GET", "/loki/api/v1/label/app/values/", lokiNotFound},
		{"label values method", "PUT", "/loki/api/v1/label/a/b/values", lokiNotFound},
		// Paths Loki cleans (r1151, r1163, r1350, r1359-r1361, r1366): the redirect
		// comes before the route and the method.
		{"r1151", "GET", "/loki/api/v1/label//values?start=1794567075&end=1794570675", lokiMovedPermanently},
		{"r1163", "GET", "/loki/api/v1/detected_field//values?query=%7Bapp%3D%22a%22%7D", lokiMovedPermanently},
		{"r1350", "GET", "/loki/api/v1/label//values", lokiMovedPermanently},
		{"r1359", "GET", "/loki//api/v1/labels", lokiMovedPermanently},
		{"r1360", "GET", "/loki/api/v1/./labels", lokiMovedPermanently},
		{"r1361", "GET", "/loki/api/v1/../v1/labels", lokiMovedPermanently},
		{"r1366", "GET", "//", lokiMovedPermanently},
		{"clean before method", "PUT", "/loki//api/v1/labels", lokiMovedPermanently},
		{"clean before 404", "GET", "/loki/api/v1//nosuch", lokiMovedPermanently},
		{"trailing dot element", "GET", "/loki/api/v1/labels/.", lokiMovedPermanently},
		{"trailing dot-dot element", "GET", "/loki/api/v1/labels/..", lokiMovedPermanently},
		// A percent-encoded letter is the letter (r1362).
		{"r1362", "GET", "/loki/api/v1/%6cabels", lokiErrorReply{Status: http.StatusOK}},
	}
	for _, row := range rows {
		t.Run(row.id, func(t *testing.T) {
			req := lokiErrorAnswer{ID: row.id, Method: row.method, Target: row.target, Headers: []string{"X-Scope-OrgID: 0"}}
			got := req.replay(t, addr)
			if row.want.Status == http.StatusOK {
				if got.Status != http.StatusOK {
					t.Errorf("answer %+v, want a 200", got)
				}
				return
			}
			if got != row.want {
				t.Errorf("answer %+v, want Loki's %+v", got, row.want)
			}
		})
	}
}

// TestLokiRouterRedirectLocation: the 301 points at the cleaned path and keeps
// the query string, and the cleaned path is not redirected again.
// conformance: semantics/router-unknown-paths-and-redirects-like-loki, status-404
func TestLokiRouterRedirectLocation(t *testing.T) {
	handler := SecurityHeadersMiddleware(http.NotFoundHandler())
	for _, tc := range []struct{ target, location string }{
		{"/loki//api/v1/labels?start=1", "/loki/api/v1/labels?start=1"},
		{"/loki/api/v1/./labels", "/loki/api/v1/labels"},
		{"/loki/api/v1/../v1/labels", "/loki/api/v1/labels"},
		{"//", "/"},
		{"/a/b/..", "/a"},
		{"/a/b/../", "/a/"},
		{"/a//b/", "/a/b/"},
		{"/loki/api/v1/label//values", "/loki/api/v1/label/values"},
		{"/../..", "/"},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.target, nil))
		if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != tc.location || rec.Body.Len() != 0 {
			t.Errorf("%s: %d Location %q body %q, want 301 to %q with no body", tc.target, rec.Code, rec.Header().Get("Location"), rec.Body.String(), tc.location)
		}
		if ct, ns := rec.Header().Get("Content-Type"), rec.Header().Get("X-Content-Type-Options"); ct != "" || ns != "" {
			t.Errorf("%s: Content-Type %q and nosniff %q, Loki's redirect has neither", tc.target, ct, ns)
		}
		again := httptest.NewRecorder()
		handler.ServeHTTP(again, httptest.NewRequest(http.MethodGet, tc.location, nil))
		if again.Code == http.StatusMovedPermanently {
			t.Errorf("%s: its Location %s is redirected again", tc.target, tc.location)
		}
	}
}

// TestLokiRouterNeverRedirectsOffHost: a path whose cleaned form a browser
// would read as another host ("//host" or a backslash) is never redirected,
// whatever its dot and slash elements; every Location the router writes starts
// with a single slash and holds no backslash.
// conformance: semantics/router-unknown-paths-and-redirects-like-loki
func TestLokiRouterNeverRedirectsOffHost(t *testing.T) {
	handler := SecurityHeadersMiddleware(http.NotFoundHandler())
	for _, target := range []string{
		"/./\\evil.example/",
		"//\\evil.example",
		"/a/../\\evil.example/x",
		"/%5C/evil.example/./x",
		"/\\/evil.example//x",
	} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.URL = &url.URL{Path: strings.ReplaceAll(target, "%5C", "\\"), RawPath: target}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, r)
		loc := rec.Header().Get("Location")
		if rec.Code == http.StatusMovedPermanently && !localRedirectPath(loc) {
			t.Errorf("%q: 301 to %q, a Location off this host", target, loc)
		}
	}
	for _, tc := range []struct {
		p    string
		want bool
	}{{"/", true}, {"/a/b", true}, {"//evil", false}, {"/\\evil", false}, {"/a\\b", false}, {"", false}, {"evil", false}} {
		if got := localRedirectPath(tc.p); got != tc.want {
			t.Errorf("localRedirectPath(%q) = %v, want %v", tc.p, got, tc.want)
		}
	}
}

// TestPathIsCleanMatchesPathClean: pathIsClean agrees with path.Clean (with a
// trailing slash kept) on every path of a small alphabet up to length 7.
// conformance: semantics/router-unknown-paths-and-redirects-like-loki
func TestPathIsCleanMatchesPathClean(t *testing.T) {
	alphabet := []byte{'/', '.', 'a'}
	var walk func(prefix []byte)
	walk = func(prefix []byte) {
		if len(prefix) > 0 && prefix[0] == '/' {
			p := string(prefix)
			want := path.Clean(p)
			if p[len(p)-1] == '/' && want != "/" {
				want += "/"
			}
			if got := pathIsClean(p); got != (want == p) {
				t.Errorf("pathIsClean(%q) = %v, but the cleaned path is %q", p, got, want)
			}
		}
		if len(prefix) == 7 {
			return
		}
		for _, c := range alphabet {
			walk(append(prefix[:len(prefix):len(prefix)], c))
		}
	}
	walk(nil)
}

// TestLokiRouterFormDecode: the URL query and a form body are decoded before
// any parameter is read, as Loki's handlers do: an invalid percent escape,
// a semicolon and a form body over 10 MiB are 400 with Loki's text; a body is
// read only for application/x-www-form-urlencoded; the body wins over the URL.
// conformance: semantics/request-form-decode-like-loki, status-400
func TestLokiRouterFormDecode(t *testing.T) {
	addr := newLokiErrorReplayProxy(t)
	const validQuery = "query=%7Bapp%3D%22api-gateway%22%7D&start=1794567075&end=1794570675&step=60"
	syntaxEnd := lokiFrontendError(400, "parse error : syntax error: unexpected $end")
	rows := []struct {
		id, method, target, form string
		headers                  []string
		want                     lokiErrorReply
	}{
		// URL query (r1369-r1374).
		{"r1369", "GET", "/loki/api/v1/query_range?query=%zz", "", nil, lokiFrontendError(400, `invalid URL escape "%zz"`)},
		{"r1370", "GET", "/loki/api/v1/labels?query=%zz", "", nil, lokiFrontendError(400, `invalid URL escape "%zz"`)},
		{"r1371", "GET", "/loki/api/v1/query_range?query=%", "", nil, lokiFrontendError(400, `invalid URL escape "%"`)},
		{"r1372", "GET", "/loki/api/v1/labels?query=%", "", nil, lokiFrontendError(400, `invalid URL escape "%"`)},
		{"r1373", "GET", "/loki/api/v1/query_range?query=a;b=c", "", nil, lokiFrontendError(400, "invalid semicolon separator in query")},
		{"r1374", "GET", "/loki/api/v1/labels?query=a;b=c", "", nil, lokiFrontendError(400, "invalid semicolon separator in query")},
		// The same decode on every route that reads a form.
		{"series", "GET", "/loki/api/v1/series?match%5B%5D=%zz", "", nil, lokiFrontendError(400, `invalid URL escape "%zz"`)},
		{"index_stats", "GET", "/loki/api/v1/index/stats?query=a;b", "", nil, lokiFrontendError(400, "invalid semicolon separator in query")},
		{"label_values", "GET", "/loki/api/v1/label/app/values?query=%zz", "", nil, lokiFrontendError(400, `invalid URL escape "%zz"`)},
		{"tail", "GET", "/loki/api/v1/tail?query=%zz", "", nil, lokiFrontendError(400, `invalid URL escape "%zz"`)},
		// Harmless query strings stay harmless (r1375-r1380).
		{"r1375", "GET", "/loki/api/v1/query_range?query", "", nil, syntaxEnd},
		{"r1377", "GET", "/loki/api/v1/query_range?", "", nil, syntaxEnd},
		{"r1379", "GET", "/loki/api/v1/query_range?&&&", "", nil, syntaxEnd},
		// A form body (urlencoded only) carries the same errors.
		{"form body escape", "POST", "/loki/api/v1/query_range", "query=%zz", []string{formType}, lokiFrontendError(400, `invalid URL escape "%zz"`)},
		{"form body semicolon", "POST", "/loki/api/v1/labels", "a=1;b=2", []string{formType}, lokiFrontendError(400, "invalid semicolon separator in query")},
		// Content types (r1404-r1411): the body is read for the form type only.
		{"r1404 no type", "POST", "/loki/api/v1/query_range", validQuery, nil, syntaxEnd},
		{"r1405 json type", "POST", "/loki/api/v1/query_range", validQuery, []string{"Content-Type: application/json"}, syntaxEnd},
		{"r1407 text type", "POST", "/loki/api/v1/query_range", validQuery, []string{"Content-Type: text/plain"}, syntaxEnd},
		{"r1408 multipart type", "POST", "/loki/api/v1/query_range", validQuery, []string{"Content-Type: multipart/form-data; boundary=x"}, syntaxEnd},
		{"r1409 garbage type", "POST", "/loki/api/v1/query_range", validQuery, []string{"Content-Type: garbage"}, syntaxEnd},
		{"a body that is not a form under a form type", "POST", "/loki/api/v1/query_range", "%zz=1", []string{"Content-Type: application/json"}, syntaxEnd},
		{"r1411 empty form body", "POST", "/loki/api/v1/query_range", "", []string{formType}, syntaxEnd},
		{"GET with a form body (r1422)", "GET", "/loki/api/v1/query_range", validQuery, []string{formType}, syntaxEnd},
		// A body that is read can fail only on its size.
		{"r1401 over 10 MiB", "POST", "/loki/api/v1/query_range", strings.Repeat("a", maxFormBodyBytes+1), []string{formType}, lokiFrontendError(400, "http: POST too large")},
		{"r1402 over 10 MiB on series", "POST", "/loki/api/v1/series", strings.Repeat("a", maxFormBodyBytes+1), []string{formType}, lokiFrontendError(400, "http: POST too large")},
		{"over 10 MiB on a non-form type is not read", "POST", "/loki/api/v1/query_range", strings.Repeat("a", maxFormBodyBytes+1), []string{"Content-Type: text/plain"}, syntaxEnd},
	}
	for _, row := range rows {
		t.Run(row.id, func(t *testing.T) {
			req := lokiErrorAnswer{ID: row.id, Method: row.method, Target: row.target, Form: row.form, Headers: append([]string{"X-Scope-OrgID: 0"}, row.headers...)}
			if got := req.replay(t, addr); got != row.want {
				t.Errorf("answer %+v, want Loki's %+v", got, row.want)
			}
		})
	}

	// A form of exactly 10 MiB is read (r1403: Loki answers the parse error of
	// its query, not the size error); body and URL both name a query: the body wins
	// (r1412), where the URL's alone is a syntax error.
	t.Run("r1403 exactly 10 MiB", func(t *testing.T) {
		body := "query=" + strings.Repeat("a", maxFormBodyBytes-len("query="))
		req := lokiErrorAnswer{ID: "r1403", Method: "POST", Target: "/loki/api/v1/query_range", Form: body, Headers: []string{"X-Scope-OrgID: 0", formType}}
		if got := req.replay(t, addr); got.Status != 400 || strings.Contains(got.Body, "POST too large") {
			t.Errorf("answer %+v, want the parse error of the query", got)
		}
	})
	t.Run("r1412 body wins over the URL", func(t *testing.T) {
		req := lokiErrorAnswer{ID: "r1412", Method: "POST", Target: "/loki/api/v1/query_range?query=%7B", Form: validQuery, Headers: []string{"X-Scope-OrgID: 0", formType}}
		if got := req.replay(t, addr); got.Status != http.StatusOK {
			t.Errorf("answer %+v, want 200: the body's query is the one read", got)
		}
	})
	t.Run("r1410 form type with a charset", func(t *testing.T) {
		req := lokiErrorAnswer{ID: "r1410", Method: "POST", Target: "/loki/api/v1/query_range", Form: validQuery, Headers: []string{"X-Scope-OrgID: 0", "Content-Type: application/x-www-form-urlencoded; charset=utf-8"}}
		if got := req.replay(t, addr); got.Status != http.StatusOK {
			t.Errorf("answer %+v, want 200", got)
		}
	})
}

// countingBody is a request body that never ends and counts what is read.
type countingBody struct{ n int64 }

func (b *countingBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	b.n += int64(len(p))
	return len(p), nil
}

func (b *countingBody) Close() error { return nil }

// TestDecodeRequestFormBoundsTheBody: the form decode never reads more than
// 10 MiB of a body (plus the reader's last buffer), whatever its length, and
// reads none of a body that is not a form.
// conformance: semantics/request-form-decode-like-loki
func TestDecodeRequestFormBoundsTheBody(t *testing.T) {
	for _, tc := range []struct {
		name, contentType string
		wantRead          bool
	}{
		{"form", "application/x-www-form-urlencoded", true},
		{"form with a charset", "application/x-www-form-urlencoded; charset=utf-8", true},
		{"json", "application/json", false},
		{"multipart", "multipart/form-data; boundary=x", false},
		{"none", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &countingBody{}
			req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/labels", body)
			req.Header.Set("Content-Type", tc.contentType)
			req.ContentLength = -1
			err := decodeRequestForm(httptest.NewRecorder(), req)
			if tc.wantRead {
				e, ok := lokierr.As(err)
				if !ok || e.Msg() != lokierr.MsgPostTooLarge || e.Text() != "http: POST too large" {
					t.Errorf("error %v, want the form size error", err)
				}
				if body.n > maxFormBodyBytes+64<<10 {
					t.Errorf("read %d bytes of an endless body, want at most 10 MiB plus one buffer", body.n)
				}
				return
			}
			if err != nil || body.n != 0 {
				t.Errorf("error %v after reading %d bytes, want none read", err, body.n)
			}
		})
	}
}

// TestParseFormSemicolonError holds the text net/http gives a semicolon in a
// query string (it has no type) to the one decodeRequestForm tells it by, and
// the typed answer to Loki's.
// conformance: semantics/request-form-decode-like-loki
func TestParseFormSemicolonError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/x?a=1;b=2", nil)
	err := req.ParseForm()
	if err == nil || err.Error() != semicolonQueryError {
		t.Fatalf("net/http's error for a semicolon is %v, decodeRequestForm tells it by %q", err, semicolonQueryError)
	}
	var typed error = lokierr.Semicolon()
	if got := decodeRequestForm(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x?a=1;b=2", nil)); got == nil || got.Error() != typed.Error() {
		t.Errorf("decodeRequestForm = %v, want %v", got, typed)
	}
	var escape url.EscapeError
	got := decodeRequestForm(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x?a=%zz", nil))
	var te *lokierr.Error
	if !errors.As(got, &te) || te.Msg() != lokierr.MsgURLEscape || !errors.As(te.Cause(), &escape) {
		t.Errorf("an invalid escape is %v, want the typed URL escape with its cause", got)
	}
}

// TestLokiFormatQuery: format_query parses the query and answers a query it
// cannot parse 400 as JSON {"status":"invalid-query","error":<message>}; a
// query it can parse keeps its answer.
// conformance: semantics/format-query-parse-like-loki, status-400
func TestLokiFormatQuery(t *testing.T) {
	addr := newLokiErrorReplayProxy(t)
	invalid := func(msg string) lokiErrorReply {
		return lokiErrorReply{Status: 400, ContentType: "application/json; charset=utf-8", Body: `{"status":"invalid-query","error":` + jsonString(msg) + "}\n"}
	}
	const matchers = `parse error : queries require at least one regexp or equality matcher that does not have an empty-compatible value. For instance, app=~".*" does not meet this requirement, but app=~".+" will`
	// Two recorded rejections are left out because the parser's text or
	// grammar differs from Loki's on every route, not on this one: a selector
	// regexp that does not compile (r0325: the proxy's wording differs) and
	// `unwrap` in a log query (r0423: the proxy's parser accepts it).
	rejected := []struct{ id, query, msg string }{
		{"r0185", ``, `parse error : syntax error: unexpected $end`},
		{"r0199", `{`, `parse error at line 1, col 2: syntax error: unexpected $end, expecting IDENTIFIER or }`},
		{"r0213", `{app="api-gateway}`, `parse error at line 1, col 6: literal not terminated`},
		{"r0227", `{app=api-gateway}`, `parse error at line 1, col 6: syntax error: unexpected IDENTIFIER, expecting STRING`},
		{"r0241", `{}`, matchers},
		{"r0255", `{app!="x"}`, matchers},
		{"r0269", `{app=~".*"}`, matchers},
		{"r0283", `foo`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{"r0339", `{app="api-gateway"} | regexp "("`, "parse error : invalid regexp parser: error parsing regexp: missing closing ): `(`"},
		{"r0353", `{app="api-gateway"} | pattern "<_>"`, `parse error : invalid pattern parser: at least one capture is required`},
		{"r0381", `rate({app="api-gateway"}[5x])`, `parse error at line 0, col 25: unknown unit "x" in duration "5x"`},
		{"r0395", `unknownfn({app="api-gateway"}[5m])`, `parse error at line 1, col 1: syntax error: unexpected IDENTIFIER`},
		{"r0409", `sum({app="api-gateway"})`, `parse error at line 1, col 24: syntax error: unexpected )`},
		{"r0437", `topk(0, rate({app="api-gateway"}[5m]))`, `parse error : invalid parameter (must be greater than 0) topk(0`},
	}
	for _, row := range rejected {
		for _, method := range []string{"GET", "POST"} {
			t.Run(row.id+" "+method, func(t *testing.T) {
				req := lokiErrorAnswer{ID: row.id, Method: method, Target: "/loki/api/v1/format_query", Headers: []string{"X-Scope-OrgID: 0", formType}}
				if method == "GET" {
					req.Target += "?query=" + url.QueryEscape(row.query)
				} else {
					req.Form = "query=" + url.QueryEscape(row.query)
				}
				if got := req.replay(t, addr); got != invalid(row.msg) {
					t.Errorf("answer %+v, want Loki's %+v", got, invalid(row.msg))
				}
			})
		}
	}
	// Missing query (r1166, r1167).
	for _, method := range []string{"GET", "POST"} {
		req := lokiErrorAnswer{ID: "r1166", Method: method, Target: "/loki/api/v1/format_query", Headers: []string{"X-Scope-OrgID: 0", formType}}
		if got := req.replay(t, addr); got != invalid("parse error : syntax error: unexpected $end") {
			t.Errorf("%s without a query: %+v", method, got)
		}
	}
	// A query Loki parses: its answer is the query, as before. A stage whose
	// error only the pipeline build raises (a line filter regexp, a template)
	// parses (r0311, r0465), as does a repeated matcher (r0367).
	for _, query := range []string{
		`{app="api-gateway"}`,
		`{app="api-gateway"} |= "x"`,
		`rate({app="api-gateway"}[5m])`,
		`count_over_time({app="api-gateway"}[5m])`,
		`{app="x", app="y"}`,
		`{app="api-gateway"} |~ "("`,
		`{app="api-gateway"} | line_format "{{"`,
		`{app="api-gateway"} | label_format a=b, a=c`,
	} {
		req := lokiErrorAnswer{ID: query, Method: "GET", Target: "/loki/api/v1/format_query?query=" + url.QueryEscape(query), Headers: []string{"X-Scope-OrgID: 0"}}
		got := req.replay(t, addr)
		if got.Status != 200 || !strings.Contains(got.Body, `"status":"success"`) || !strings.Contains(got.Body, jsonString(query)) && !strings.Contains(got.Body, strings.ReplaceAll(jsonString(query), `<`, "<")) {
			t.Errorf("%s: answer %+v, want 200 with the query", query, got)
		}
	}
}

// jsonString is s as a JSON string literal, escaped as Go's encoder does (the
// way both sides write a message).
func jsonString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '<':
			b.WriteString(`<`)
		case '>':
			b.WriteString(`>`)
		case '&':
			b.WriteString(`&`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// TestLokiPushRoute: the push route is not served. Loki answers a method
// other than POST with the router's bodyless 405 (r1191), which the proxy
// matches. A POST would be an ingest, which a read-only proxy cannot do: it
// answers 405 in the shape Loki writes its push errors in (http.Error's),
// with the reason, and names no backend (Loki's own POST answer, 204 or 400
// `snappy: corrupt input` for an empty body, r1200, has no read-only
// counterpart).
// conformance: semantics/push-route-read-only-answer, status-405
func TestLokiPushRoute(t *testing.T) {
	addr := newLokiErrorReplayProxy(t)
	for _, method := range []string{"GET", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"} {
		req := lokiErrorAnswer{ID: "r1191", Method: method, Target: "/loki/api/v1/push", Headers: []string{"X-Scope-OrgID: 0"}}
		if got := req.replay(t, addr); got != bodyless405 {
			t.Errorf("%s: %+v, want Loki's bodyless 405", method, got)
		}
	}
	want := lokiErrorReply{Status: 405, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: "write operations are not supported, this is a read-only proxy\n"}
	for _, ct := range []string{"application/json", "application/x-protobuf", formType[len("Content-Type: "):]} {
		req := lokiErrorAnswer{ID: "r1200", Method: "POST", Target: "/loki/api/v1/push", Form: `{"streams":[]}`, Headers: []string{"X-Scope-OrgID: 0", "Content-Type: " + ct}}
		got := req.replay(t, addr)
		if got != want {
			t.Errorf("POST %s: %+v, want %+v", ct, got, want)
		}
		if strings.Contains(strings.ToLower(got.Body), "victoria") {
			t.Errorf("the push answer names the backend: %q", got.Body)
		}
	}
}

// TestRouteGateCoversEveryReadRoute: every route in the table above is gated
// when it is reached through the registered mux, as the server reaches it, so
// a route registered later without the gate fails here. The delete API is the
// proxy's own route (it checks its method behind its token) and is not listed.
// conformance: semantics/router-method-not-allowed-like-loki
func TestRouteGateCoversEveryReadRoute(t *testing.T) {
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	h := SecurityHeadersMiddleware(mux)
	for _, route := range gatedRoutes {
		if _, ok := lokiRouteMethods[route.endpoint]; !ok {
			t.Errorf("%s is not in lokiRouteMethods", route.endpoint)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("PUT", route.target, nil))
		if rec.Code != http.StatusMethodNotAllowed || rec.Body.Len() != 0 {
			t.Errorf("PUT %s answered %d %q", route.target, rec.Code, rec.Body.String())
		}
	}
	if _, ok := lokiRouteMethods["delete"]; ok {
		t.Error("the delete API checks its own method behind its token and is not gated")
	}
}
