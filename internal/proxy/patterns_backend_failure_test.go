package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The patterns route answers a failed VictoriaLogs call as the proxy's one error layer does on every other
// route: Loki's status, errorType and fixed text, never an empty 200. A body that is empty because the range holds
// no lines, or a body streamed whole, is still an answer.
//
// conformance: semantics/error-layer-silent-success
func TestPatternsBackendFailureAnswersLokisError(t *testing.T) {
	type want struct {
		status    int
		errorType string
		text      string
	}
	wants := map[string]want{
		// A query VictoriaLogs rejects is the client's mistake: Loki's 400 with its own text.
		"vl400_parse": {http.StatusBadRequest, "bad_data", lokiErrParse},
		// A memory limit hit while executing is a failed call, not a rejected query.
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
		// The streamed body is read whole whatever its size: the buffered-body cap does not apply.
		"oversize": {http.StatusOK, "", ""},
		// A range without lines is an answer, not a failure.
		"empty_ok": {http.StatusOK, "", ""},
	}
	ranges := []struct {
		name string
		span time.Duration
	}{
		{"6h", 6 * time.Hour},
		// Long enough to be read window by window.
		{"7d", 7 * 24 * time.Hour},
	}
	tiers := []struct {
		name string
		cfg  func(*Config)
	}{
		{"sampled", func(*Config) {}},
		{"exact_and_counts", func(c *Config) { c.PatternsExactMaxLines = 1000; c.PatternsCounts = true }},
	}
	modes := append(append([]string{}, errLayerModes...), "empty_ok")
	for _, tier := range tiers {
		for _, rg := range ranges {
			for _, mode := range modes {
				t.Run(tier.name+"/"+rg.name+"/"+mode, func(t *testing.T) {
					w := wants[mode]
					backend := &errLayerBackend{mode: mode}
					if mode == "empty_ok" {
						backend.mode = "" // answers 200 with no rows
					}
					srv := httptest.NewServer(backend)
					if mode == "conn_refused" {
						srv.Close()
					} else {
						// A handler that waits out a timeout must not hold the close.
						defer func() { srv.CloseClientConnections(); srv.Close() }()
					}
					c := cache.New(time.Minute, 100)
					defer c.Close()
					cfg := Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second}
					if mode == "timeout" {
						cfg.BackendTimeout = 40 * time.Millisecond
					}
					if mode == "oversize" {
						cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = 4096
					}
					tier.cfg(&cfg)
					p, err := New(cfg)
					if err != nil {
						t.Fatalf("new proxy: %v", err)
					}
					defer func() { _ = p.Shutdown(context.Background()) }()
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
					q := url.Values{"query": {`{app="api"}`}, "start": {fmt.Sprint(now.Add(-rg.span).Unix())}, "end": {fmt.Sprint(now.Unix())}, "step": {"60"}}
					req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns?"+q.Encode(), nil).WithContext(ctx)
					req.Header.Set("X-Scope-OrgID", "0")
					rec := httptest.NewRecorder()
					shippedChain(mux).ServeHTTP(rec, req)

					body := rec.Body.String()
					if rec.Code != w.status {
						t.Fatalf("status %d, want %d; body %.200q", rec.Code, w.status, body)
					}
					if w.status == http.StatusOK {
						if !strings.HasPrefix(body, `{"status":"success","data":[`) {
							t.Fatalf("body %.200q, want the success answer", body)
						}
					} else {
						if got := lokiErrorType(rec.Code); got != w.errorType {
							t.Fatalf("errorType %q, want %q", got, w.errorType)
						}
						if body != w.text {
							t.Fatalf("body %q, want Loki's %q", body, w.text)
						}
					}
					if rec.Code >= 400 {
						if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
							t.Fatalf("Content-Type %q, want Loki's text/plain", ct)
						}
						if strings.Contains(body, errLayerSentinel) || strings.Contains(body, "logsql") {
							t.Fatalf("body leaks the backend: %q", body)
						}
					}
					if mode == "empty_ok" && !strings.Contains(body, `"data":[]`) {
						t.Fatalf("a range without lines must answer no patterns, got %q", body)
					}
				})
			}
		}
	}
}
