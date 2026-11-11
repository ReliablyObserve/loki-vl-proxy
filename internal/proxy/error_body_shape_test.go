package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// errBodyCase is one route answering one error class with the exact status,
// Content-Type, nosniff and body bytes Loki v3.7.9 writes there.
type errBodyCase struct {
	name        string
	method      string
	path        string
	query       url.Values
	status      int
	contentType string // "" = Loki sets none
	nosniff     bool
	body        string // the exact bytes
}

const (
	lokiPlain = "text/plain; charset=utf-8"
	// pkg/util/validation/validate.go:5 with a 3h range over a 1h limit.
	lokiQueryTooLong = "the query time range exceeds the limit (query length: 3h0m0s, limit: 1h)"
)

func runErrBodyCases(t *testing.T, p *Proxy, cases []errBodyCase) {
	t.Helper()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	handler := shippedChain(mux)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.path
			if len(tc.query) > 0 {
				target += "?" + tc.query.Encode()
			}
			method := tc.method
			if method == "" {
				method = http.MethodGet
			}
			req := httptest.NewRequest(method, target, nil)
			req.Header.Set("X-Scope-OrgID", "0")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status %d, want %d; body %q", rec.Code, tc.status, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != tc.contentType {
				t.Fatalf("Content-Type %q, want %q", got, tc.contentType)
			}
			if got := rec.Header().Get("X-Content-Type-Options") == "nosniff"; got != tc.nosniff {
				t.Fatalf("nosniff = %v, want %v", got, tc.nosniff)
			}
			if rec.Body.String() != tc.body {
				t.Fatalf("body %q, want %q", rec.Body.String(), tc.body)
			}
		})
	}
}

// Every route that Loki serves through its query frontend answers an error with
// the message alone: status, text/plain, nosniff, no envelope, no newline
// (pkg/util/server/error.go WriteError). The classes are the ones a client can
// cause or a backend can raise: a query Loki rejects, a range over
// max_query_length, a failed backend call (500), a cancelled request (499) and
// a deadline (504).
// conformance: semantics/error-layer-body-shape, status-400, status-500
func TestErrorBody_QueryFrontendRoutes(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "vl private text 10.0.0.9:9428 _stream", http.StatusBadGateway)
	}))
	defer failing.Close()
	p, err := New(Config{BackendURL: failing.URL, Cache: cache.New(time.Minute, 100), LogLevel: "error", DefaultMaxQueryLength: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	now := time.Now().UTC()
	recent := url.Values{"start": {now.Add(-time.Minute).Format(time.RFC3339)}, "end": {now.Format(time.RFC3339)}}
	with := func(kv ...string) url.Values {
		v := url.Values{}
		for k, vs := range recent {
			v[k] = vs
		}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return v
	}
	long := url.Values{"start": {"0"}, "end": {"10800"}}
	frontend := func(name, path string, q url.Values, status int, body string) errBodyCase {
		return errBodyCase{name: name, path: path, query: q, status: status, contentType: lokiPlain, nosniff: true, body: body}
	}
	var cases []errBodyCase
	// A backend that fails every call: Loki's failed-call 500, nothing of the backend.
	for _, route := range []struct {
		name, path string
		q          url.Values
	}{
		{"query_range", "/loki/api/v1/query_range", with("query", `{app="a"}`)},
		{"query", "/loki/api/v1/query", with("query", `sum(count_over_time({app="a"}[1m]))`, "time", "1")},
		{"labels", "/loki/api/v1/labels", with()},
		{"label_values", "/loki/api/v1/label/app/values", with()},
		{"series", "/loki/api/v1/series", with("match[]", `{app="a"}`)},
		{"index_volume", "/loki/api/v1/index/volume", with("query", `{app="a"}`)},
	} {
		cases = append(cases, frontend(route.name+"/backend_500", route.path, route.q, http.StatusInternalServerError, lokiErrInternal))
	}
	// A range past max_query_length is Loki's 400 text on every route that checks it.
	for _, route := range []struct {
		name, path string
		q          url.Values
	}{
		{"labels", "/loki/api/v1/labels", long},
		{"label_values", "/loki/api/v1/label/app/values", long},
		{"series", "/loki/api/v1/series", url.Values{"match[]": {`{app="a"}`}, "start": {"0"}, "end": {"10800"}}},
	} {
		cases = append(cases, frontend(route.name+"/max_query_length", route.path, route.q, http.StatusBadRequest, lokiQueryTooLong))
	}
	// A log query is not an instant query (pkg/logql/engine.go).
	cases = append(cases, frontend("query/log_query_as_instant", "/loki/api/v1/query", with("query", `{app="a"}`, "time", "1"), http.StatusBadRequest,
		"log queries are not supported as an instant query type, please change your query to a range query type"))
	// Routes Loki does not serve: gorilla/mux's 404.
	cases = append(cases, errBodyCase{name: "unknown/404", path: "/loki/api/v1/nope", status: http.StatusNotFound, contentType: lokiPlain, nosniff: true, body: "404 page not found\n"})
	runErrBodyCases(t, p, cases)
}

// A cancelled request is Loki's 499 and an expired deadline its 504, with Loki's
// fixed texts, as the message alone.
// conformance: semantics/error-layer-body-shape, semantics/error-layer-cancel-deadline
func TestErrorBody_CancelAndDeadline(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	defer slow.Close()
	p := newTestProxy(t, slow.URL)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil).WithContext(canceled)
	rec := httptest.NewRecorder()
	p.handleLabels(rec, req)
	if rec.Code != 499 || rec.Header().Get("Content-Type") != lokiPlain || rec.Body.String() != "the request was cancelled by the client" {
		t.Fatalf("cancel: %d %q %q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}

	p.client.Timeout = 20 * time.Millisecond
	rec = httptest.NewRecorder()
	p.handleLabels(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
	if rec.Code != http.StatusGatewayTimeout || rec.Header().Get("Content-Type") != lokiPlain || rec.Body.String() != lokiErrDeadlineExceeded {
		t.Fatalf("deadline: %d %q %q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

// The rules and alerts API is the ruler's: Prometheus JSON in the order
// status, data, errorType, error, with no X-Content-Type-Options; the rules
// route sets no Content-Type (Go sniffs text/plain) and PrometheusAlerts sets
// application/json before it asks the ruler. The legacy rule views and the
// delete API use http.Error (text, newline, nosniff), and an unregistered
// method is gorilla/mux's bodyless 405 with no headers of its own. The cases
// run through the middleware cmd/proxy ships, which presets nosniff and a
// default Content-Type that the writers must remove or override.
// (pkg/ruler/base/api.go:106-123, :166-179, :286, :523-572; pkg/loki/modules.go:1936-1938.)
// conformance: semantics/error-layer-body-shape, status-400, status-500
func TestErrorBody_RulerAndDeleteShapes(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Query().Get("rule_group[]"), "g-empty"):
			_, _ = w.Write([]byte(`{"status":"success","data":{"groups":[]}}`))
		case r.URL.Query().Get("type") == "alert":
			http.Error(w, "vmalert private text", http.StatusBadRequest)
		default:
			http.Error(w, "vmalert private text", http.StatusBadGateway)
		}
	}))
	defer failing.Close()
	_, mux := newAlertProxyMux(t, Config{BackendURL: "http://unused", RulerBackendURL: failing.URL, AlertsBackendURL: failing.URL, DeleteAPIEnabled: true, DeleteAPIAuthToken: testDeleteToken})
	const plain, json = "text/plain; charset=utf-8", "application/json"
	const serverErr = `{"status":"error","data":null,"errorType":"server_error","error":"Internal Server Error"}`
	cases := []struct {
		name, method, path string
		status             int
		contentType        string
		nosniff            bool
		body               string
	}{
		{"prometheus_rules/backend_500", "GET", "/prometheus/api/v1/rules", 500, plain, false, serverErr},
		{"prometheus_rules/backend_4xx_is_500", "GET", "/prometheus/api/v1/rules?type=alert", 500, plain, false, serverErr},
		{"prometheus_rules/unknown_type", "GET", "/prometheus/api/v1/rules?type=xyz", 400, plain, false, `{"status":"error","data":null,"errorType":"bad_data","error":"not supported value \"xyz\""}`},
		{"prometheus_alerts/backend_500", "GET", "/prometheus/api/v1/alerts", 500, json, false, serverErr},
		{"loki_alerts_alias/backend_500", "GET", "/loki/api/v1/alerts", 500, json, false, serverErr},
		{"loki_rules/backend_500", "GET", "/loki/api/v1/rules", 500, plain, true, "Internal Server Error\n"},
		{"prom_rules/backend_500", "GET", "/api/prom/rules", 500, plain, true, "Internal Server Error\n"},
		{"loki_rules/group_does_not_exist", "GET", "/loki/api/v1/rules/ns/g-empty", 404, plain, true, "group does not exist\n"},
		{"loki_rules/more_segments_than_the_route", "GET", "/loki/api/v1/rules/a/b/c", 404, plain, true, "404 page not found\n"},
		{"loki_rules/traversal_is_a_client_error", "GET", "/loki/api/v1/rules/%2e%2e/escape", 400, plain, true, "invalid namespace: path traversal not allowed\n"},
		{"loki_rules/escape_cannot_be_decoded", "GET", "/loki/api/v1/rules/%25zz", 500, plain, false, `{"status":"error","data":null,"errorType":"server_error","error":"invalid namespace: invalid URL escape \"%zz\""}`},
		{"delete/missing_confirmation_post", "POST", "/loki/api/v1/delete?query=%7Bapp%3D%22a%22%7D&start=1&end=2", 400, plain, true, "delete requires X-Delete-Confirmation: true header for safety\n"},
		{"delete/missing_confirmation_put", "PUT", "/loki/api/v1/delete?query=%7Bapp%3D%22a%22%7D&start=1&end=2", 400, plain, true, "delete requires X-Delete-Confirmation: true header for safety\n"},
		// Loki serves GET (list) and DELETE (cancel) here; the proxy does not
		// (open gap semantics/error-request-validation-parity).
		{"delete/get_list_not_implemented", "GET", "/loki/api/v1/delete", 405, "", false, ""},
		{"delete/delete_cancel_not_implemented", "DELETE", "/loki/api/v1/delete", 405, "", false, ""},
	}
	handler := shippedChain(mux)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("X-Scope-OrgID", "0")
			req.Header.Set("X-Delete-Token", testDeleteToken) // the delete API's credential
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.status || rec.Header().Get("Content-Type") != tc.contentType || (rec.Header().Get("X-Content-Type-Options") == "nosniff") != tc.nosniff || rec.Body.String() != tc.body {
				t.Fatalf("got %d %q nosniff=%q %q, want %d %q nosniff=%v %q", rec.Code, rec.Header().Get("Content-Type"), rec.Header().Get("X-Content-Type-Options"), rec.Body.String(), tc.status, tc.contentType, tc.nosniff, tc.body)
			}
			if strings.Contains(rec.Body.String(), "private") {
				t.Fatalf("the backend's text reached the client: %q", rec.Body.String())
			}
		})
	}
}

// Removing the presets from an error must not change a success: through the
// shipped middleware a result keeps its JSON Content-Type and nosniff.
// conformance: semantics/error-layer-body-shape
func TestErrorBody_SuccessKeepsItsHeaders(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1}]}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	rec := httptest.NewRecorder()
	shippedChain(mux).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("success: %d %v", rec.Code, rec.Header())
	}
}

// FuzzErrorBodyShapes writes any status and message through the three Loki
// error shapes and asserts the bytes a client reads: the frontend shape is the
// message alone (valid UTF-8 as Loki's: U+FFFD per invalid byte), http.Error's is the message and a newline, and the ruler's is
// JSON that round-trips the message in Loki's key order. A message with
// quotes, newlines, control bytes or invalid UTF-8 must not change the framing.
//
// conformance: semantics/error-layer-body-shape
func FuzzErrorBodyShapes(f *testing.F) {
	f.Add(400, "parse error at line 1, col 2: syntax error: unexpected $end, expecting IDENTIFIER or }")
	f.Add(500, lokiErrInternal)
	f.Add(499, "line one\nline two\r\n\"quoted\" <tag> & \x00\xff")
	f.Add(504, "")
	f.Fuzz(func(t *testing.T, status int, msg string) {
		if status < 400 || status > 599 {
			status = 400 + (status%200+200)%200
		}
		frontend := httptest.NewRecorder()
		writeLokiErrorBody(frontend, status, msg)
		if frontend.Code != status || frontend.Header().Get("Content-Type") != lokiPlain || frontend.Header().Get("X-Content-Type-Options") != "nosniff" || frontend.Body.String() != string([]rune(msg)) {
			t.Fatalf("frontend shape: %d %v %q", frontend.Code, frontend.Header(), frontend.Body.String())
		}
		httpErr := httptest.NewRecorder()
		writeLokiHTTPError(httpErr, status, msg)
		if httpErr.Code != status || httpErr.Header().Get("Content-Type") != lokiPlain || httpErr.Body.String() != msg+"\n" {
			t.Fatalf("http.Error shape: %d %v %q", httpErr.Code, httpErr.Header(), httpErr.Body.String())
		}
		ruler := httptest.NewRecorder()
		writeLokiRulerError(ruler, status, msg)
		var decoded struct {
			Status    string `json:"status"`
			Data      any    `json:"data"`
			ErrorType string `json:"errorType"`
			Error     string `json:"error"`
		}
		// The ruler answers only 400 (bad_data) and 500 (server_error).
		wantStatus, wantType := http.StatusInternalServerError, "server_error"
		if status == http.StatusBadRequest {
			wantStatus, wantType = http.StatusBadRequest, "bad_data"
		}
		if ruler.Code != wantStatus || ruler.Header().Get("Content-Type") != lokiPlain || ruler.Header().Get("X-Content-Type-Options") != "" ||
			!strings.HasPrefix(ruler.Body.String(), `{"status":"error","data":null,"errorType":"`+wantType+`","error":`) ||
			json.Unmarshal(ruler.Body.Bytes(), &decoded) != nil || decoded.Status != "error" || decoded.Data != nil || decoded.ErrorType != wantType ||
			decoded.Error != string([]rune(msg)) {
			t.Fatalf("ruler shape: %d %v %q", ruler.Code, ruler.Header(), ruler.Body.String())
		}
	})
}

// The window stats byte guard is the row limit's own budget (rows times the
// largest row), so it is the same limit error: Loki's limit status 400 with the
// text naming the one flag that raises it, never a 500 that names a flag.
// conformance: semantics/error-layer-body-shape, limits/row-limit-error-names-flag
func TestSlidingStatsByteBudgetIsTheRowLimitError(t *testing.T) {
	err := slidingStatsByteBudgetError(1000)
	status, _, text := lokiClientError(context.Background(), err)
	const want = "manual range metric row limit exceeded (1000 stats rows of up to 65536 bytes in one bucket grid); narrow the query or increase -manual-range-metric-row-limit"
	if status != http.StatusBadRequest || text != want {
		t.Fatalf("got %d %q, want 400 %q", status, text, want)
	}
}

// A stats call that VictoriaLogs answers 200 with an error body is a failed
// call, not a result: the route answers Loki's 500 text, never the body with
// VictoriaLogs' message under a success status.
// conformance: semantics/error-layer-body-shape, semantics/error-layer-silent-success
func TestErrorBody_StatsErrorBodyUnderSuccessIsAFailedCall(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":"cannot execute query [_stream:{app=\"a\"}]: VLSENTINEL"}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	for _, target := range []string{
		"/loki/api/v1/query?" + url.Values{"query": {`sum(count_over_time({app="a"}[1m]))`}, "time": {"1700000000"}}.Encode(),
		"/loki/api/v1/query_range?" + url.Values{"query": {`sum(count_over_time({app="a"}[1m]))`}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}}.Encode(),
	} {
		rec := httptest.NewRecorder()
		shippedChain(mux).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusInternalServerError || rec.Header().Get("Content-Type") != lokiPlain || rec.Body.String() != lokiErrInternal {
			t.Fatalf("%s: %d %q %q, want 500 text %q", target, rec.Code, rec.Header().Get("Content-Type"), rec.Body.String(), lokiErrInternal)
		}
	}
}
