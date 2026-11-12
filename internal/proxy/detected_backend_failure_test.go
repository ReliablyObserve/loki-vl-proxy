package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// detected_fields, detected_field/{name}/values and detected_labels answer a failed VictoriaLogs call as the
// proxy's one error layer does on every other route: Loki's status, errorType and fixed text, never an empty or a
// partial 200. A body that is empty because the range holds no lines is still an answer.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedRoutesBackendFailureAnswersLokisError(t *testing.T) {
	type want struct {
		status    int
		errorType string
		text      string
	}
	wants := map[string]want{
		// A query VictoriaLogs rejects is the client's mistake: Loki's 400 with its own text.
		"vl400_parse":   {http.StatusBadRequest, "bad_data", lokiErrParse},
		"vl400_limit":   {http.StatusInternalServerError, "internal", lokiErrInternal},
		"vl422":         {http.StatusInternalServerError, "internal", lokiErrInternal},
		"vl500":         {http.StatusInternalServerError, "internal", lokiErrInternal},
		"vl502_front":   {http.StatusInternalServerError, "internal", lokiErrInternal},
		"vl503_front":   {http.StatusInternalServerError, "internal", lokiErrInternal},
		"vl429":         {http.StatusTooManyRequests, "bad_data", lokiQueueFull},
		"conn_refused":  {http.StatusInternalServerError, "internal", lokiErrInternal},
		"conn_reset":    {http.StatusInternalServerError, "internal", lokiErrInternal},
		"timeout":       {http.StatusGatewayTimeout, "timeout", lokiDeadlineExceeded},
		"client_cancel": {499, "canceled", lokiClientCanceled},
		"truncated":     {http.StatusInternalServerError, "internal", lokiErrInternal},
		"malformed":     {http.StatusInternalServerError, "internal", lokiErrInternal},
		// The scan streams its body whole whatever its size, so the buffered-body cap does not apply to it.
		"oversize": {http.StatusOK, "", ""},
		// A range without lines is an answer, not a failure.
		"empty_ok": {http.StatusOK, "", ""},
	}
	routes := []struct {
		name string
		path string
		ok   string // what an answer starts with
		// selectorOnly: the route takes label matchers only, so it has no strict and relaxed query.
		selectorOnly bool
	}{
		{"detected_fields", "/loki/api/v1/detected_fields", `{"data":[`, false},
		{"detected_field_values", "/loki/api/v1/detected_field/level/values", `{"data":[`, false},
		{"detected_labels", "/loki/api/v1/detected_labels", `{"data":[`, true},
	}
	queries := []struct {
		name  string
		query string
	}{
		// One query candidate.
		{"selector", `{app="api"}`},
		// A strict and a relaxed candidate: the relaxed one is read after the strict one fails.
		{"parser_filter", `{app="api"} | json | level="info"`},
	}
	modes := append(append([]string{}, errLayerModes...), "empty_ok")
	for _, rt := range routes {
		for _, q := range queries {
			if rt.selectorOnly && q.name != "selector" {
				continue
			}
			for _, mode := range modes {
				t.Run(rt.name+"/"+q.name+"/"+mode, func(t *testing.T) {
					w := wants[mode]
					rec := runDetectedFailureCell(t, rt.path, q.query, mode)
					body := rec.Body.String()
					if mode == "oversize" && rec.Code != http.StatusOK {
						// A route that buffers its body is held to the cap and answers Loki's 500.
						w = want{http.StatusInternalServerError, "internal", lokiErrInternal}
					}
					if rec.Code != w.status {
						t.Fatalf("status %d, want %d; body %.200q", rec.Code, w.status, body)
					}
					if w.status == http.StatusOK {
						if !strings.HasPrefix(body, rt.ok) {
							t.Fatalf("body %.200q, want the success answer", body)
						}
						if mode == "empty_ok" && strings.Contains(body, `"label"`) {
							t.Fatalf("a range without lines must answer no entries, got %.200q", body)
						}
						return
					}
					if got := lokiErrorType(rec.Code); got != w.errorType {
						t.Fatalf("errorType %q, want %q", got, w.errorType)
					}
					if body != w.text {
						t.Fatalf("body %q, want Loki's %q", body, w.text)
					}
					if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
						t.Fatalf("Content-Type %q, want Loki's text/plain", ct)
					}
					if strings.Contains(body, errLayerSentinel) || strings.Contains(body, "logsql") {
						t.Fatalf("body leaks the backend: %q", body)
					}
				})
			}
		}
	}
}

func runDetectedFailureCell(t *testing.T, path, query, mode string) *httptest.ResponseRecorder {
	t.Helper()
	backend := &errLayerBackend{mode: mode}
	if mode == "empty_ok" {
		backend.mode = "" // answers 200 with no rows
	}
	srv := httptest.NewServer(backend)
	if mode == "conn_refused" {
		srv.Close()
	} else {
		// A handler that waits out a timeout must not hold the close.
		t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
	}
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	cfg := Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second}
	if mode == "timeout" {
		cfg.BackendTimeout = 40 * time.Millisecond
	}
	if mode == "oversize" {
		cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = 4096
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if mode == "client_cancel" {
		backend.mu.Lock()
		backend.cancel = cancel
		backend.mu.Unlock()
	}
	now := time.Now().Truncate(time.Minute)
	q := url.Values{"query": {query}, "start": {fmt.Sprint(now.Add(-6 * time.Hour).Unix())}, "end": {fmt.Sprint(now.Unix())}}
	req := httptest.NewRequest(http.MethodGet, path+"?"+q.Encode(), nil).WithContext(ctx)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	shippedChain(mux).ServeHTTP(rec, req)
	return rec
}

// The Drilldown scan budget (-drilldown-scan-timeout) is the one documented bound that keeps a partial answer: a
// scan that outlasts it answers no fields, while the backend and the request are still alive. A failure of the
// backend inside that budget is an error.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedFieldsScanBudgetKeepsItsPartialAnswer(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{
		BackendURL: srv.URL, Cache: c, LogLevel: "error",
		BackendTimeout: 5 * time.Second, DrilldownScanTimeout: 60 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	for _, path := range []string{"/loki/api/v1/detected_fields", "/loki/api/v1/detected_field/level/values"} {
		req := httptest.NewRequest(http.MethodGet, path+`?query={app="api"}&start=1705314600&end=1705318200`, nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		shippedChain(mux).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Body.String(), `{"data":[]`) {
			t.Fatalf("%s: a spent scan budget answers no entries, got %d %.200q", path, rec.Code, rec.Body.String())
		}
	}
	if calls.Load() == 0 {
		t.Fatal("the backend was never called")
	}
}

// A scan that stalls after some rows ends the same way as one that stalls before its headers: the scan budget
// running out keeps the documented answer of no fields, and the backend timeout is the error of every other route.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedFieldsScanStalledMidBody(t *testing.T) {
	for _, tc := range []struct {
		name       string
		scanBudget time.Duration
		backendTO  time.Duration
		wantStatus int
	}{
		{"scan_budget_spent", 80 * time.Millisecond, 5 * time.Second, http.StatusOK},
		{"backend_timeout", 0, 80 * time.Millisecond, http.StatusGatewayTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if !strings.HasPrefix(r.URL.Path, "/select/logsql/query") {
					// The native lookups stall before their headers.
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
					}
					return
				}
				_, _ = io.WriteString(w, errLayerShapedBody("/select/logsql/query", 3))
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			}))
			t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
			c := cache.New(time.Minute, 100)
			t.Cleanup(c.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: tc.backendTO, DrilldownScanTimeout: tc.scanBudget})
			if err != nil {
				t.Fatalf("new proxy: %v", err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			req := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_fields?query={app="api"}&start=1705314600&end=1705318200`, nil)
			req.Header.Set("X-Scope-OrgID", "0")
			rec := httptest.NewRecorder()
			shippedChain(mux).ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; body %.200q", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusOK && !strings.HasPrefix(rec.Body.String(), `{"data":[]`) {
				t.Fatalf("a spent scan budget answers no fields, got %.200q", rec.Body.String())
			}
			if tc.wantStatus != http.StatusOK && rec.Body.String() != lokiDeadlineExceeded {
				t.Fatalf("body %q, want Loki's %q", rec.Body.String(), lokiDeadlineExceeded)
			}
		})
	}
}

// The stale-on-error helper is documented and marked (X-Proxy-Stale-Response): an expired answer of the same
// request stands in for a failed read, on these routes as on the label routes. It serves nothing when no expired
// answer exists (the table above), and it serves the new failure classes the way it serves a 502.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedRoutesStaleAnswerStandsInForFailedRead(t *testing.T) {
	for _, tc := range []struct {
		endpoint, label, path, body string
	}{
		{"detected_fields", "", "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=1705314600&end=1705318200", `{"status":"success","data":[{"label":"cached"}],"fields":[{"label":"cached"}]}`},
		{"detected_field_values", "level", "/loki/api/v1/detected_field/level/values?query=%7Bapp%3D%22api%22%7D&start=1705314600&end=1705318200", `{"status":"success","data":["cached"],"values":["cached"],"limit":100}`},
		{"detected_labels", "", "/loki/api/v1/detected_labels?query=%7Bapp%3D%22api%22%7D&start=1705314600&end=1705318200", `{"status":"success","data":[{"label":"cached"}],"detectedLabels":[{"label":"cached"}],"limit":100}`},
	} {
		for _, mode := range []string{"timeout", "truncated", "malformed"} {
			t.Run(tc.endpoint+"/"+mode, func(t *testing.T) {
				backend := &errLayerBackend{mode: mode}
				srv := httptest.NewServer(backend)
				t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
				c := cache.New(time.Minute, 100)
				t.Cleanup(c.Close)
				cfg := Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second}
				if mode == "timeout" {
					cfg.BackendTimeout = 40 * time.Millisecond
				}
				p, err := New(cfg)
				if err != nil {
					t.Fatalf("new proxy: %v", err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				mux := http.NewServeMux()
				p.RegisterRoutes(mux)
				newReq := func() *http.Request {
					r := httptest.NewRequest(http.MethodGet, tc.path, nil)
					r.Header.Set("X-Scope-OrgID", "0")
					return r
				}
				var key string
				if tc.label != "" {
					key = p.canonicalReadCacheKey(tc.endpoint, "0", newReq(), tc.label)
				} else {
					key = p.canonicalReadCacheKey(tc.endpoint, "0", newReq())
				}
				p.setEndpointReadCacheWithTTL(tc.endpoint, key, []byte(tc.body), time.Millisecond)
				time.Sleep(5 * time.Millisecond)

				rec := httptest.NewRecorder()
				shippedChain(mux).ServeHTTP(rec, newReq())
				if backend.hits.Load() == 0 {
					t.Fatal("the backend was never called: the expired answer must be tried against a fresh read first")
				}
				if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "cached") {
					t.Fatalf("want the expired answer, got %d %.200q", rec.Code, rec.Body.String())
				}
				if rec.Header().Get(staleResponseHeader) != "true" || rec.Header().Get("Cache-Control") != "no-store" {
					t.Fatalf("stale answer headers = %v, want %s: true and Cache-Control: no-store", rec.Header(), staleResponseHeader)
				}
			})
		}
	}
}

// One label whose values cannot be read makes detected_labels fail over to the streams and, when those fail too,
// answer the error: a list with some of the labels is never the answer.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedLabelsFailedLabelValuesIsNotAPartialList(t *testing.T) {
	var streamsCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch {
		case strings.HasSuffix(r.URL.Path, "/stream_field_names"):
			_, _ = fmt.Fprint(w, `{"values":[{"value":"app","hits":3},{"value":"env","hits":3}]}`)
		case strings.HasSuffix(r.URL.Path, "/field_values") && r.Form.Get("field") == "app":
			_, _ = fmt.Fprint(w, `{"values":[{"value":"api","hits":3}]}`)
		case strings.HasSuffix(r.URL.Path, "/field_values"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprint(w, "boom "+errLayerSentinel)
		case strings.HasSuffix(r.URL.Path, "/streams"):
			streamsCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	// VictoriaLogs v1.50 and later read stream label values from the column index.
	p.backendVersionMu.Lock()
	p.backendSupportsColumnFieldValues = true
	p.backendVersionMu.Unlock()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_labels?query={app="api"}&start=1705314600&end=1705318200`, nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	shippedChain(mux).ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError || rec.Body.String() != lokiErrInternal {
		t.Fatalf("a label that failed must fail the answer, got %d %.200q", rec.Code, rec.Body.String())
	}
	if streamsCalls.Load() == 0 {
		t.Fatal("the failed label values must fail over to the streams")
	}
}
