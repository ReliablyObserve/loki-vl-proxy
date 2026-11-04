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

// TestCapturedErrorAnswerIsTheInnerAnswer checks that a metric query the
// proxy post-processes after its handler answered (label_replace, a without
// fold) fails exactly as the query inside it does. Loki raises the failure
// where it falls and the outer function never rewrites it; the handler's
// answer is the error layer's already (Loki's status and text, no
// VictoriaLogs text), so it is sent as written: same status, same body, same
// Content-Type and nosniff, on query and query_range. Every backend failure
// mode of the error-layer gate that fails the call is driven through it, and
// the answer must hold the gate's classes, so no VictoriaLogs, transport or
// Go text reaches the client on this path; the series limit is the proxy's
// own failure after the call.
// conformance: semantics/error-layer-backend-text, semantics/error-layer-transport-text, semantics/error-layer-body-shape, semantics/error-layer-cancel-deadline, limits/series-limit-error
func TestCapturedErrorAnswerIsTheInnerAnswer(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	ts := now.Add(-30 * time.Minute).Unix()
	const inner = `sum by (level, pod) (count_over_time({app="api"}[1m]))`
	outers := map[string]string{
		"label_replace": `label_replace(` + inner + `, "dst", "$1", "pod", "(.*)")`,
		"without":       `sum without (pod) (` + inner + `)`,
	}
	type failure struct {
		mode   string   // an errLayerModes mode, or "series_limit"
		outers []string // the outer forms that keep the inner failure
	}
	var failures []failure
	for _, mode := range errLayerModes {
		switch mode {
		case "truncated", "malformed", "oversize":
			// A 200 body: the gate's own cells cover what the stats route
			// makes of it; nothing failed before the outer function.
			continue
		}
		failures = append(failures, failure{mode, []string{"label_replace", "without"}})
	}
	// The without fold counts the folded series, so only label_replace keeps
	// the inner query's series count.
	failures = append(failures, failure{"series_limit", []string{"label_replace"}})
	for _, fc := range failures {
		backend := &errLayerBackend{mode: fc.mode}
		var handler http.Handler = backend
		if fc.mode == "series_limit" {
			handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(r.URL.Path, "/stats_query") {
					fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"level":"info","pod":"a"},"value":[%d,"1"]},{"metric":{"level":"error","pod":"b"},"value":[%d,"2"]}]}}`, ts, ts)
					return
				}
				fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info","pod":"a"},"values":[[%d,"1"]]},{"metric":{"level":"error","pod":"b"},"values":[[%d,"2"]]}]}}`, ts, ts)
			})
		}
		srv := httptest.NewServer(handler)
		backendURL := srv.URL
		if fc.mode == "conn_refused" {
			srv.Close()
		}
		cfg := Config{BackendURL: backendURL, Cache: cache.NewDisabled(), LogLevel: "error", BackendTimeout: 5 * time.Second}
		switch fc.mode {
		case "timeout":
			cfg.BackendTimeout = 250 * time.Millisecond
		case "series_limit":
			cfg.MaxStatsQuerySeries = 1
		}
		p, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		get := func(mode, query string) *httptest.ResponseRecorder {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			backend.mu.Lock()
			backend.cancel = cancel
			backend.mu.Unlock()
			v := url.Values{"query": {query}, "start": {fmt.Sprint(now.Add(-time.Hour).Unix())}, "end": {fmt.Sprint(now.Unix())}, "time": {fmt.Sprint(now.Unix())}, "step": {"60"}}
			w := httptest.NewRecorder()
			shippedChain(mux).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+mode+"?"+v.Encode(), nil).WithContext(ctx))
			return w
		}
		host := strings.TrimPrefix(backendURL, "http://")
		for _, mode := range []string{"query_range", "query"} {
			want := get(mode, inner)
			if want.Code < http.StatusBadRequest {
				t.Fatalf("%s %s: the inner query did not fail: %d %q", fc.mode, mode, want.Code, want.Body)
			}
			for _, name := range fc.outers {
				t.Run(fc.mode+"/"+mode+"/"+name, func(t *testing.T) {
					got := get(mode, outers[name])
					if got.Code != want.Code || got.Body.String() != want.Body.String() {
						t.Fatalf("got %d %q, want the inner answer %d %q", got.Code, got.Body, want.Code, want.Body)
					}
					for _, h := range []string{"Content-Type", "X-Content-Type-Options"} {
						if got.Header().Get(h) != want.Header().Get(h) {
							t.Fatalf("%s = %q, want %q", h, got.Header().Get(h), want.Header().Get(h))
						}
					}
					if fc.mode != "series_limit" {
						outcome := errLayerOutcome{status: got.Code, body: got.Body.String(), headers: got.Header()}
						if classes := errLayerClasses(errLayerRoute{name: "capture"}, fc.mode, outcome, host); len(classes) > 0 {
							t.Fatalf("error-layer leak %v: %d %q", classes, got.Code, got.Body)
						}
					}
				})
			}
		}
		_ = p.Shutdown(context.Background())
		if fc.mode != "conn_refused" {
			srv.Close()
		}
	}
}
