package proxy

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// requestOnRoute is a request as the proxy's route middleware hands it to a
// handler: its context names the endpoint.
func requestOnRoute(endpoint, path string, ctx context.Context) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	return r.WithContext(context.WithValue(ctx, requestRouteMetaKey, requestRouteMeta{endpoint: endpoint, route: path, req: r}))
}

// errorRoutes is every endpoint name the proxy registers a Loki route with,
// with a path of it, and the shape writer the route's handlers use for an
// error today.
var errorRoutes = []struct {
	endpoint, path string
	shape          errorShape
}{
	{"query_range", "/loki/api/v1/query_range", writeLokiErrorBody},
	{"query", "/loki/api/v1/query", writeLokiErrorBody},
	{"series", "/loki/api/v1/series", writeLokiErrorBody},
	{"labels", "/loki/api/v1/labels", writeLokiErrorBody},
	{"label_values", "/loki/api/v1/label/app/values", writeLokiErrorBody},
	{"detected_fields", "/loki/api/v1/detected_fields", writeLokiErrorBody},
	{"detected_field_values", "/loki/api/v1/detected_field/app/values", writeLokiErrorBody},
	{"index_stats", "/loki/api/v1/index/stats", writeLokiErrorBody},
	{"volume", "/loki/api/v1/index/volume", writeLokiErrorBody},
	{"volume_range", "/loki/api/v1/index/volume_range", writeLokiErrorBody},
	{"patterns", "/loki/api/v1/patterns", writeLokiErrorBody},
	{"tail", "/loki/api/v1/tail", writeLokiErrorBody},
	{"format_query", "/loki/api/v1/format_query", writeLokiFormatQueryError},
	{"detected_labels", "/loki/api/v1/detected_labels", writeLokiErrorBody},
	{"drilldown_limits", "/loki/api/v1/drilldown-limits", writeLokiErrorBody},
	{"tenant_limits", "/config/tenant/v1/limits", writeLokiHTTPError},
	{"delete", "/loki/api/v1/delete", writeLokiHTTPError},
	{"rules", "/loki/api/v1/rules", writeLokiHTTPError},
	{"rules_nested", "/loki/api/v1/rules/ns", writeLokiHTTPError},
	{"rules_prom", "/api/prom/rules", writeLokiHTTPError},
	{"rules_prom_nested", "/api/prom/rules/ns", writeLokiHTTPError},
	{"rules_prometheus", "/prometheus/api/v1/rules", writeLokiRulerError},
	{"alerts", "/loki/api/v1/alerts", writeLokiAlertsError},
	{"alerts_prom", "/api/prom/alerts", writeLokiAlertsError},
	{"alerts_prometheus", "/prometheus/api/v1/alerts", writeLokiAlertsError},
}

// TestFailRouteTableCoversEveryRoute: every endpoint name RegisterProxyRoutes
// registers a Loki route with is in errorRoutes, and every name in the route
// table is one of them, so a new route gets its shape decided.
// conformance: semantics/error-layer-body-shape
func TestFailRouteTableCoversEveryRoute(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "proxy.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	registered := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "RegisterProxyRoutes" {
			return true
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) < 3 {
				return true
			}
			var name string
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				name = fun.Name
			case *ast.SelectorExpr:
				name = fun.Sel.Name
			}
			if name != "routeHandler" && name != "alertRead" && name != "rlNoTenant" {
				return true
			}
			if lit, ok := call.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
				endpoint, _ := strconv.Unquote(lit.Value)
				registered[endpoint] = true
			}
			return true
		})
		return false
	})
	if len(registered) < 20 {
		t.Fatalf("found %d registered endpoints in proxy.go; the scan is broken", len(registered))
	}
	listed := map[string]bool{}
	for _, rt := range errorRoutes {
		listed[rt.endpoint] = true
		if !registered[rt.endpoint] {
			t.Errorf("errorRoutes names %q, which proxy.go does not register", rt.endpoint)
		}
	}
	for endpoint := range registered {
		if !listed[endpoint] {
			t.Errorf("route %q has no entry in errorRoutes: decide its error shape (routeErrorShapes)", endpoint)
		}
	}
	for endpoint := range routeErrorShapes {
		if !registered[endpoint] {
			t.Errorf("routeErrorShapes names %q, which proxy.go does not register", endpoint)
		}
	}
}

// TestFailWritesWhatTheRouteWritesToday: for every route and every failure
// kind of the error writers' golden cases, p.fail answers byte for byte what
// the route's handlers answer with the error layer today (writeErr, or
// writeErrShaped with the route's shape).
// conformance: semantics/error-layer-body-shape, semantics/error-layer-backend-text, semantics/error-layer-cancel-deadline
func TestFailWritesWhatTheRouteWritesToday(t *testing.T) {
	p := &Proxy{}
	for _, rt := range errorRoutes {
		for _, tc := range errorWriterCases() {
			want := httptest.NewRecorder()
			wantStatus := p.writeErrShaped(want, tc.ctx(), tc.err, rt.shape)
			got := httptest.NewRecorder()
			gotStatus := p.fail(got, requestOnRoute(rt.endpoint, rt.path, tc.ctx()), tc.err)
			if gotStatus != wantStatus || got.Code != want.Code || got.Body.String() != want.Body.String() ||
				got.Header().Get("Content-Type") != want.Header().Get("Content-Type") ||
				got.Header().Get("X-Content-Type-Options") != want.Header().Get("X-Content-Type-Options") {
				t.Errorf("%s %s: fail wrote %d %q %v, the route writes %d %q %v", rt.endpoint, tc.name,
					got.Code, got.Body.String(), got.Header(), want.Code, want.Body.String(), want.Header())
			}
		}
	}
}

// TestFailWritesTypedErrors: a *lokierr.Error is written with its status and
// text in the route's shape, or in its own shape for the messages Loki's
// router writes the same on every route; its cause never reaches the body; a
// cancelled or expired request is 499 or 504 whatever the error.
// conformance: semantics/error-layer-body-shape, semantics/error-layer-cancel-deadline, semantics/error-layer-backend-text, status-404, status-405
func TestFailWritesTypedErrors(t *testing.T) {
	p := &Proxy{}
	cause := errors.New(`VictoriaLogs at 10.0.0.9:9428: cannot parse query [_time:5m | x]`)
	cases := []struct {
		name        string
		endpoint    string
		ctx         func() context.Context
		err         error
		status      int
		contentType string
		nosniff     string
		body        string
	}{
		{"param on query_range", "query_range", liveCtx, lokierr.StepNotPositive().WithCause(cause), 400, "text/plain; charset=utf-8", "nosniff",
			"zero or negative query resolution step widths are not accepted. Try a positive integer"},
		{"wrapped param", "labels", liveCtx, fmt.Errorf("decode: %w", lokierr.Direction("bad")), 400, "text/plain; charset=utf-8", "nosniff", "invalid direction 'bad'"},
		{"parse on format_query", "format_query", liveCtx, lokierr.Parse(0, 0, "syntax error: unexpected $end"), 400, "application/json; charset=utf-8", "",
			`{"status":"invalid-query","error":"parse error : syntax error: unexpected $end"}` + "\n"},
		{"not found", "query_range", liveCtx, lokierr.NotFound(), 404, "text/plain; charset=utf-8", "nosniff", "404 page not found\n"},
		{"not found on ruler JSON", "rules_prometheus", liveCtx, lokierr.NotFound(), 404, "text/plain; charset=utf-8", "nosniff", "404 page not found\n"},
		{"method not allowed", "series", liveCtx, lokierr.MethodNotAllowed(), 405, "", "", ""},
		{"tail not a websocket", "tail", liveCtx, lokierr.TailNotWebSocket(), 400, "text/plain; charset=utf-8", "nosniff", "Bad Request\n"},
		{"backend failed", "volume", liveCtx, lokierr.BackendFailed(cause), 500, "text/plain; charset=utf-8", "nosniff", "Internal Server Error"},
		{"index series limit", "volume", liveCtx, lokierr.IndexSeriesLimit(1000), 500, "text/plain; charset=utf-8", "nosniff",
			"rpc error: code = Unknown desc = the query hit the max number of series limit (limit: 1000 series)"},
		{"delete http.Error", "delete", liveCtx, lokierr.EndBeforeStart(), 400, "text/plain; charset=utf-8", "nosniff", "end timestamp must not be before or equal to start time\n"},
		{"ruler JSON", "rules_prometheus", liveCtx, lokierr.Internal(cause), 500, "text/plain; charset=utf-8", "",
			`{"status":"error","data":null,"errorType":"server_error","error":"Internal Server Error"}`},
		{"typed under a cancelled request", "query_range", canceledCtx, lokierr.StepNotPositive(), 499, "text/plain; charset=utf-8", "nosniff", "the request was cancelled by the client"},
		{"typed under an expired request", "query_range", expiredCtx, lokierr.StepNotPositive(), 504, "text/plain; charset=utf-8", "nosniff", lokiErrDeadlineExceeded},
		{"invalid UTF-8 in a typed text", "query_range", liveCtx, lokierr.Direction("b\xffd"), 400, "text/plain; charset=utf-8", "nosniff", "invalid direction 'b�d'"},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		status := p.fail(rec, requestOnRoute(tc.endpoint, "/x", tc.ctx()), tc.err)
		if status != tc.status || rec.Code != tc.status || rec.Header().Get("Content-Type") != tc.contentType ||
			rec.Header().Get("X-Content-Type-Options") != tc.nosniff || rec.Body.String() != tc.body {
			t.Errorf("%s: %d (returned %d) %q nosniff=%q %q; want %d %q %q %q", tc.name, rec.Code, status,
				rec.Header().Get("Content-Type"), rec.Header().Get("X-Content-Type-Options"), rec.Body.String(),
				tc.status, tc.contentType, tc.nosniff, tc.body)
		}
		if strings.Contains(rec.Body.String(), "10.0.0.9") || strings.Contains(rec.Body.String(), "cannot parse query") {
			t.Errorf("%s: the cause reached the client: %q", tc.name, rec.Body.String())
		}
	}
}

// lokiAnswerStatuses are the statuses Loki answers a failed read with,
// its router's 404 and 405 included.
var lokiAnswerStatuses = map[int]bool{400: true, 404: true, 405: true, 429: true, 499: true, 500: true, 504: true}

// FuzzFail drives the error layer's writer with typed errors built from
// arbitrary inputs and with the untyped failures the proxy meets, on every
// route, under a live, cancelled or expired request: the status is one of
// Loki's, a typed error is written with its own status and text, an untyped
// one exactly as the route writes it today, and no cause reaches the body.
// conformance: semantics/error-layer-backend-text, semantics/error-layer-body-shape, semantics/error-layer-cancel-deadline
func FuzzFail(f *testing.F) {
	f.Add(uint8(0), uint8(0), uint8(0), "abc", 1, 4)
	f.Add(uint8(3), uint8(1), uint8(12), "{app=\"x\"} |= \"\xff\"", 0, 0)
	f.Add(uint8(9), uint8(2), uint8(21), "cannot parse query arg: x", 2, 7)
	f.Add(uint8(13), uint8(0), uint8(24), "a; narrow the query or increase -flag", -1, 1)
	f.Add(uint8(14), uint8(0), uint8(1), "x", 4097, 4096)
	f.Add(uint8(16), uint8(0), uint8(3), "adaptive limit 2", 2, 1000)
	f.Add(uint8(17), uint8(2), uint8(0), "{app=\"x\"}", 7, 512)
	f.Fuzz(func(t *testing.T, kind, ctxState, route uint8, text string, line, col int) {
		p := &Proxy{}
		const marker = "\x00cause-marker"
		cause := errors.New(text + marker)
		var err error
		var typed *lokierr.Error
		const limitFlag = "fz-limit-flag"
		switch kind % 18 {
		case 0:
			typed = lokierr.Direction(text)
		case 1:
			typed = lokierr.Parse(line, col, text)
		case 2:
			typed = lokierr.ParamDuration("since", text)
		case 3:
			typed = lokierr.StepDuration(text)
		case 4:
			typed = lokierr.BackendFailed(cause)
		case 5:
			typed = lokierr.NotFound().WithCause(cause)
		case 6:
			typed = lokierr.MethodNotAllowed()
		case 7:
			typed = lokierr.ParserMessage(text)
		case 8:
			err = errors.New(text)
		case 9:
			err = &upstreamStatusError{status: 400 + int(uint8(line)%110), msg: text, class: vlErrorClass(uint8(col) % 5)}
		case 10:
			err = fmt.Errorf("%s: %w", text, context.Canceled)
		case 11:
			err = &orderedJSONPipelineError{message: text}
		case 12:
			err = nil
		case 13:
			err = errors.New(text + "; narrow the query or increase -flag")
		case 14:
			typed = lokierr.ResponseTooLarge(limitFlag, int64(line), int64(col), cause)
		case 15:
			typed = lokierr.LimitReached(limitFlag, int64(col), cause)
		case 16:
			typed, _ = lokierr.As(newHeavyQueueFull(&heavyQueryQueueFullError{limitFlag: "-" + limitFlag, work: text, maxConcurrent: line, queueWait: time.Duration(col), adaptive: text}))
		case 17:
			typed = lokierr.InputTooLong(len(text), col).WithLimit(limitFlag, int64(col)).WithCause(cause)
		}
		if typed != nil {
			err = typed
		}
		ctx := []func() context.Context{liveCtx, canceledCtx, expiredCtx}[ctxState%3]
		rt := errorRoutes[int(route)%len(errorRoutes)]

		rec := httptest.NewRecorder()
		status := p.fail(rec, requestOnRoute(rt.endpoint, rt.path, ctx()), err)
		// The ruler's JSON shape answers every status but 400 as 500 on the
		// wire, as Loki's ruler does; the returned status is the error's.
		if !lokiAnswerStatuses[status] || !lokiAnswerStatuses[rec.Code] {
			t.Fatalf("status %d (wrote %d) is not one of Loki's", status, rec.Code)
		}
		if strings.Contains(rec.Body.String(), marker) {
			t.Fatalf("a cause reached the client: %q", rec.Body.String())
		}
		if kind%18 >= 14 && strings.Contains(rec.Body.String(), limitFlag) {
			t.Fatalf("a limit's flag reached the client: %q", rec.Body.String())
		}
		if typed != nil && ctxState%3 == 0 {
			if status != typed.Status() {
				t.Fatalf("typed %s answered %d, want %d", typed.Msg().ID(), status, typed.Status())
			}
			return
		}
		want := httptest.NewRecorder()
		wantStatus := p.writeErrShaped(want, ctx(), err, rt.shape)
		if typed == nil && (wantStatus != status || want.Body.String() != rec.Body.String()) {
			t.Fatalf("%s: fail wrote %d %q, the route writes %d %q", rt.endpoint, status, rec.Body.String(), wantStatus, want.Body.String())
		}
	})
}

// BenchmarkFailTyped is the writer for typed errors on a frontend route and
// on a route with its own shape (format_query), log line on.
func BenchmarkFailTyped(b *testing.B) {
	p := &Proxy{log: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	for _, bc := range []struct {
		name     string
		endpoint string
		err      error
	}{
		{"param", "query_range", lokierr.StepNotPositive()},
		{"backend_failed", "query_range", lokierr.BackendFailed(errors.New("VL backend returned 500"))},
		{"parse_format_query", "format_query", lokierr.Parse(1, 2, "syntax error: unexpected $end")},
	} {
		r := requestOnRoute(bc.endpoint, "/x", context.Background())
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				p.fail(httptest.NewRecorder(), r, bc.err)
			}
		})
	}
}
