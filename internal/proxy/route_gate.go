package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// Loki's router answers before any handler runs, in this order: a path it
// would clean is redirected (301, no body); a path no route matches is 404;
// a method the route does not serve is 405 (no body, no Content-Type); then
// the handler decodes the URL and the form body (invalid URL escape,
// semicolon, a form over 10 MiB) before it reads any parameter. This file is
// the proxy's copy of that order, ahead of tenant, rate limit, cache and
// parameter checks, so none of them answers a request Loki's router refuses.

// methodMask is a set of the methods a route serves.
type methodMask uint8

const (
	methodGet methodMask = 1 << iota
	methodPost
)

// methodBit is the mask of one HTTP method (0 for a method no route serves).
func methodBit(method string) methodMask {
	switch method {
	case http.MethodGet:
		return methodGet
	case http.MethodPost:
		return methodPost
	}
	return 0
}

// lokiRouteMethods are the methods Loki's router serves on each Loki read
// route, by the endpoint name the route registers with: GET and POST on the
// query, metadata, index, patterns, detected-field, tail and format_query
// routes (recorded for every route but volume_range, detected_field_values
// and the label values route's PATCH, HEAD and OPTIONS, which Loki registers
// beside its siblings); GET alone on the ruler's read routes. Any other
// method is 405 with no body. Routes that are not listed here (the delete
// API, which has its own method check behind its token, the drilldown limits
// and tenant limits views, the health and peer routes) keep their own rules.
var lokiRouteMethods = map[string]methodMask{
	"query_range":           methodGet | methodPost,
	"query":                 methodGet | methodPost,
	"series":                methodGet | methodPost,
	"labels":                methodGet | methodPost,
	"label_values":          methodGet | methodPost,
	"detected_fields":       methodGet | methodPost,
	"detected_field_values": methodGet | methodPost,
	"detected_labels":       methodGet | methodPost,
	"index_stats":           methodGet | methodPost,
	"volume":                methodGet | methodPost,
	"volume_range":          methodGet | methodPost,
	"patterns":              methodGet | methodPost,
	"tail":                  methodGet | methodPost,
	"format_query":          methodGet | methodPost,
	"rules":                 methodGet,
	"rules_nested":          methodGet,
	"rules_prom":            methodGet,
	"rules_prom_nested":     methodGet,
	"rules_prometheus":      methodGet,
	"alerts":                methodGet,
	"alerts_prom":           methodGet,
	"alerts_prometheus":     methodGet,
}

// pathSlashes holds, for the routes whose path carries a name, the number of
// slashes of the path as Loki's router matches it (decoded): a name that
// holds an encoded slash is another path, which no route matches.
var pathSlashes = map[string]int{
	"label_values":          6, // /loki/api/v1/label/{name}/values
	"detected_field_values": 6, // /loki/api/v1/detected_field/{name}/values
}

// maxFormBodyBytes is the largest form body Loki reads: a bigger one is
// answered "http: POST too large".
const maxFormBodyBytes = 10 << 20

// routeGate wraps next, the handler chain of the route registered under
// endpoint, with Loki's router checks. An endpoint that is not in
// lokiRouteMethods is returned as it is.
func (p *Proxy) routeGate(endpoint, route string, next http.Handler) http.Handler {
	allowed, ok := lokiRouteMethods[endpoint]
	if !ok {
		return next
	}
	slashes := pathSlashes[endpoint]
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slashes > 0 && strings.Count(r.URL.Path, "/") != slashes {
			p.failBeforeRoute(w, r, endpoint, route, lokierr.NotFound())
			return
		}
		if allowed&methodBit(r.Method) == 0 {
			p.failBeforeRoute(w, r, endpoint, route, lokierr.MethodNotAllowed())
			return
		}
		if err := decodeRequestForm(w, r); err != nil {
			p.failBeforeRoute(w, r, endpoint, route, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// failBeforeRoute writes err for a request the route's own middleware has not
// seen: the body shape is the route's, and the log line names the route.
func (p *Proxy) failBeforeRoute(w http.ResponseWriter, r *http.Request, endpoint, route string, err error) {
	ctx := context.WithValue(r.Context(), requestRouteMetaKey, requestRouteMeta{endpoint: endpoint, route: route, req: r})
	p.writeFailure(w, ctx, err, endpointErrorShape(endpoint))
}

// decodeRequestForm reads the URL query and, for a POST, the form body once,
// as Loki's handlers do before they read a parameter. It returns Loki's error
// for what Loki rejects at that point (an invalid percent escape, a semicolon
// in the query, a form body over 10 MiB) and nil otherwise; the parsed form
// stays on the request for every later reader. Only a body sent as
// application/x-www-form-urlencoded is read; with any other Content-Type the
// body is not read at all, and a body of any size is bounded by 10 MiB.
func decodeRequestForm(w http.ResponseWriter, r *http.Request) error {
	if r.Method == http.MethodPost && r.Body != nil && r.Body != http.NoBody {
		r.Body = http.MaxBytesReader(w, r.Body, maxFormBodyBytes)
	}
	err := r.ParseForm()
	if err == nil {
		return nil
	}
	var tooLarge *http.MaxBytesError
	var escape url.EscapeError
	switch {
	case errors.As(err, &tooLarge):
		return lokierr.PostTooLarge().WithCause(err)
	case errors.As(err, &escape):
		return lokierr.URLEscape(escape).WithCause(err)
	case err.Error() == semicolonQueryError:
		return lokierr.Semicolon().WithCause(err)
	}
	// A body that cannot be read, or a Content-Type that is not a media
	// type: Loki's answer to them is not recorded, and the handlers have
	// always read the query string alone then.
	return nil
}

// semicolonQueryError is net/http's error for a semicolon in a query string or
// form body (it has no type); TestParseFormSemicolonError holds it to the
// standard library's text.
const semicolonQueryError = "invalid semicolon separator in query"

// redirectUncleanPath answers a request whose path Loki's router would clean
// (a repeated slash or a . or .. element) with Loki's 301 to the cleaned
// path: no body, no Content-Type. It reports whether it answered. It runs
// before any handler or mux, so it answers every method on every route. The
// path is read as it was sent, escapes undecoded (as the mux reads it), so an
// encoded dot element is not one here: the handlers that take a path element
// refuse it themselves.
func redirectUncleanPath(w http.ResponseWriter, r *http.Request) bool {
	p := r.URL.EscapedPath()
	if p == "" || p[0] != '/' || pathIsClean(p) {
		return false
	}
	clean := path.Clean(p)
	if p[len(p)-1] == '/' && clean != "/" {
		clean += "/"
	}
	if r.URL.RawQuery != "" {
		clean += "?" + r.URL.RawQuery
	}
	w.Header().Set("Location", clean)
	w.WriteHeader(http.StatusMovedPermanently)
	return true
}

// pathIsClean reports whether p, which starts with a slash, has no empty
// element and no . or .. element (a trailing slash is clean). It does not
// allocate.
func pathIsClean(p string) bool {
	for i := 0; i < len(p); i++ {
		if p[i] != '/' {
			continue
		}
		rest := p[i+1:]
		switch {
		case rest == "":
			return true
		case rest[0] == '/':
			return false
		case rest[0] == '.':
			if len(rest) == 1 || rest[1] == '/' {
				return false
			}
			if rest[1] == '.' && (len(rest) == 2 || rest[2] == '/') {
				return false
			}
		}
	}
	return true
}
