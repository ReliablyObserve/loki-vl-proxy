//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// Loki's fixed answers (v3.7.9 pkg/util/server/error.go:26-27,
// pkg/querier/queryrange/limits.go:46).
const (
	e2eLokiDeadlineExceeded = "request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed"
	e2eLokiSeriesLimitTmpl  = "maximum number of series (%d) reached for a single query; consider reducing query cardinality by adding more specific stream selectors, reducing the time range, or aggregating results with functions like sum(), count() or topk()"
)

// Text no client may see: VictoriaLogs messages, LogsQL, Go transport and
// decoder errors.
var (
	e2eErrLayerBackendMarkers   = []string{"cannot parse `query`", "cannot parse query", "cannot execute", "_stream", "_msg", "| stats", "-search.", "VictoriaLogs", "VL ", "backend returned", "logsql"}
	e2eErrLayerTransportMarkers = []string{"dial tcp", "read tcp", "connection reset", "connection refused", `Post "http`, `Get "http`, "Client.Timeout", "127.0.0.1:", "victorialogs:"}
	e2eErrLayerInternalMarkers  = []string{"unexpected EOF", "invalid character", "scanning", "context canceled", "deadline exceeded", "body exceeds configured limit"}
)

// errLayerFaultBackend forwards to the stack's VictoriaLogs and injects one
// failure into every query call (the /select/logsql/ paths); the version probe
// and /metrics pass through, so the in-process proxy starts as on the stack.
func errLayerFaultBackend(t *testing.T, fault string) string {
	t.Helper()
	target, err := url.Parse(vlURL)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/select/logsql/") {
			forward.ServeHTTP(w, r)
			return
		}
		switch fault {
		case "stopped":
			// VictoriaLogs goes away: the connection closes before any answer.
			_, _ = io.Copy(io.Discard, r.Body)
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
				}
			}
		case "stopped_mid_body":
			// VictoriaLogs answers 200 and stops halfway through the body.
			rec := httptest.NewRecorder()
			forward.ServeHTTP(rec, r)
			body := rec.Body.Bytes()
			hj, ok := w.(http.Hijacker)
			if !ok {
				return
			}
			conn, buf, err := hj.Hijack()
			if err != nil {
				return
			}
			_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n", rec.Header().Get("Content-Type"), len(body)+64)
			_, _ = buf.Write(body[:len(body)/2])
			_ = buf.Flush()
			_ = conn.Close()
		case "slow":
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		default:
			forward.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// newErrLayerProxy starts an in-process proxy against backendURL.
func newErrLayerProxy(t *testing.T, backendURL string, set func(*proxy.Config)) string {
	t.Helper()
	cfg := proxy.Config{BackendURL: backendURL, Cache: cache.NewDisabled(), LogLevel: "error", BackendTimeout: 30 * time.Second}
	if set != nil {
		set(&cfg)
	}
	p, err := proxy.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.ValidateBackendVersionCompatibility(ctx); err != nil {
		t.Fatalf("backend version probe: %v", err)
	}
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL
}

// errLayerE2EText is the error text a client reads: the envelope's error field
// or the raw body.
func errLayerE2EText(body []byte) string {
	var env struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &env) == nil && env.Error != "" {
		return env.Error
	}
	return strings.TrimSpace(string(body))
}

func errLayerE2EContains(s string, markers []string) bool {
	for _, m := range markers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

// errLayerE2EClasses names the registry cases an answer fails against Loki's
// (wantStatus, and wantText when Loki has a fixed text).
func errLayerE2EClasses(status int, body []byte, header http.Header, wantStatus int, wantText string, timeout, limit bool) []string {
	classes := map[string]bool{}
	switch {
	case status == http.StatusOK:
		classes["semantics/error-layer-silent-success"] = true
	case status != wantStatus && timeout:
		classes["semantics/error-layer-cancel-deadline"] = true
	case status != wantStatus && limit:
		classes["semantics/error-layer-proxy-limit-status"] = true
	case status != wantStatus:
		classes["semantics/error-layer-backend-failure-status"] = true
	}
	visible := string(body)
	for k, vs := range header {
		visible += "\n" + k + ": " + strings.Join(vs, ",")
	}
	if errLayerE2EContains(visible, e2eErrLayerTransportMarkers) {
		classes["semantics/error-layer-transport-text"] = true
	} else if errLayerE2EContains(visible, e2eErrLayerInternalMarkers) {
		classes["semantics/error-layer-internal-go-text"] = true
	}
	if errLayerE2EContains(visible, e2eErrLayerBackendMarkers) {
		classes["semantics/error-layer-backend-text"] = true
	}
	if limit && status >= 400 && !strings.Contains(errLayerE2EText(body), "increase -") {
		// A proxy limit names the flag that sets it.
		classes["semantics/error-layer-proxy-limit-status"] = true
	}
	if wantText != "" && status == wantStatus && errLayerE2EText(body) != wantText {
		if timeout {
			classes["semantics/error-layer-cancel-deadline"] = true
		} else {
			classes["semantics/error-layer-backend-text"] = true
		}
	}
	out := make([]string, 0, len(classes))
	for c := range classes {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// errLayerE2EVerdict fails on a leak class the case does not list, skips with
// the registry ids while any listed class still fails, and fails once none
// does, so a fixed case must drop its list. The list is the classes the case
// may fail on any route today (they vary by route and VictoriaLogs version).
func errLayerE2EVerdict(t *testing.T, got, known []string, status int, body []byte) {
	t.Helper()
	listed := map[string]bool{}
	for _, id := range known {
		listed[id] = true
	}
	var unlisted []string
	for _, id := range got {
		if !listed[id] {
			unlisted = append(unlisted, id)
		}
	}
	switch {
	case len(unlisted) > 0:
		t.Fatalf("error-layer leak %v not listed: status %d, body %.400s", unlisted, status, body)
	case len(got) == 0 && len(known) > 0:
		t.Fatalf("known leak fixed, remove %v from the case (status %d, body %.300s)", known, status, body)
	case len(got) > 0:
		t.Skipf("known leak %s: status %d, text %.200q", strings.Join(got, ", "), status, errLayerE2EText(body))
	}
}

// TestCompat_ErrorLayerLikeLoki drives real VictoriaLogs failures through the
// proxy and compares the client's answer with Loki's for the same request:
// an invalid LogsQL the proxy cannot pre-validate (asked of Loki directly), the
// series limit and the buffered-response limit (Loki's limit message), and
// VictoriaLogs stopping or answering after -backend-timeout (Loki's 500 and
// 504 from pkg/util/server/error.go, since a stack Loki cannot be stopped per
// request). The in-process proxies reach the stack's VictoriaLogs through a
// fault-injecting forwarder; nothing on the stack is stopped.
// conformance: semantics/error-layer-transport-text, semantics/error-layer-backend-text, semantics/error-layer-internal-go-text, semantics/error-layer-backend-failure-status, semantics/error-layer-cancel-deadline, semantics/error-layer-silent-success, semantics/error-layer-proxy-limit-status, backend-deadlines-and-cancellation, status-400, status-500, status-502
func TestCompat_ErrorLayerLikeLoki(t *testing.T) {
	end := time.Now()
	start := end.Add(-time.Hour)
	window := func(params url.Values) url.Values {
		params = cloneQueryValues(params)
		params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
		params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
		return params
	}
	// Both sides answer the plain metric with data before any failure is compared;
	// Loki's range metrics are empty for a few minutes after a fresh stack starts.
	waitForLokiMetricData(t)
	const levels = `sum by (level) (count_over_time({app="api-gateway"}[1m]))`
	for name, base := range map[string]string{"loki": lokiURL, "proxy": proxyURL} {
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", window(url.Values{"query": {levels}, "step": {"60"}}), "0", nil)
		// The series limit case needs at least two level series.
		if status != http.StatusOK || strings.Count(string(body), `"metric"`) < 2 {
			t.Fatalf("%s does not answer the baseline metric with two or more series: %d %.300s", name, status, body)
		}
	}

	t.Run("invalid_logsql_like_loki", func(t *testing.T) {
		// VictoriaLogs rejects the repetition on stats_query_range; the proxy's
		// LogQL parser accepts it.
		for _, tc := range []struct {
			name, path string
			params     url.Values
		}{
			{"query_range_line_regex", "/loki/api/v1/query_range", url.Values{"query": {`sum(count_over_time({app="api-gateway"} |~ "a{2,1}" [1m]))`}, "step": {"60"}}},
			{"query_range_pattern", "/loki/api/v1/query_range", url.Values{"query": {`sum by (app) (count_over_time({app="api-gateway"} | pattern "<_>" [1m]))`}, "step": {"60"}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, tc.path, window(tc.params), "0", nil)
				if lokiStatus != http.StatusBadRequest {
					t.Fatalf("Loki fixture drifted: want 400, got %d %s", lokiStatus, lokiBody)
				}
				status, body := rejectedQueryGet(t, proxyURL, tc.path, window(tc.params), "0", nil)
				got := errLayerE2EClasses(status, body, nil, lokiStatus, strings.TrimSpace(string(lokiBody)), false, false)
				errLayerE2EVerdict(t, got, []string{"semantics/error-layer-backend-text"}, status, body)
			})
		}
	})

	t.Run("series_limit_like_loki", func(t *testing.T) {
		route := newErrLayerProxy(t, errLayerFaultBackend(t, "pass"), func(cfg *proxy.Config) { cfg.MaxStatsQuerySeries = 1 })
		status, body := rejectedQueryGet(t, route, "/loki/api/v1/query_range", window(url.Values{"query": {levels}, "step": {"60"}}), "0", nil)
		got := errLayerE2EClasses(status, body, nil, http.StatusBadRequest, fmt.Sprintf(e2eLokiSeriesLimitTmpl, 1), false, false)
		errLayerE2EVerdict(t, got, nil, status, body)
	})

	t.Run("buffered_response_limit", func(t *testing.T) {
		// rate_counter always reads raw rows into the proxy's buffered-bytes
		// budget (internal/proxy/range_metric_compat.go shouldUseManualRangeMetricCompat),
		// whatever the VictoriaLogs version; a 64-byte budget holds no row.
		const rawRows = `sum(rate_counter({app="api-gateway"} | json | __error__="" | duration!="" | unwrap duration [5m]))`
		params := window(url.Values{"query": {rawRows}, "step": {"60"}})
		lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/query_range", params, "0", nil)
		if lokiStatus != http.StatusOK || !strings.Contains(string(lokiBody), `"values"`) {
			t.Fatalf("Loki fixture drifted: want 200 with samples for %s, got %d %.300s", rawRows, lokiStatus, lokiBody)
		}
		route := newErrLayerProxy(t, errLayerFaultBackend(t, "pass"), func(cfg *proxy.Config) {
			cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = 64
		})
		status, body := rejectedQueryGet(t, route, "/loki/api/v1/query_range", params, "0", nil)
		if status == http.StatusOK {
			t.Fatalf("the 64-byte buffered-response limit was not reached; the case proves nothing: %.300s", body)
		}
		got := errLayerE2EClasses(status, body, nil, http.StatusInternalServerError, "", false, true)
		errLayerE2EVerdict(t, got, []string{"semantics/error-layer-proxy-limit-status"}, status, body)
	})

	failures := []struct {
		name, fault string
		timeout     bool
		wantStatus  int
		wantText    string
		known       []string
	}{
		{"vl_stopped", "stopped", false, http.StatusInternalServerError, "", []string{"semantics/error-layer-backend-failure-status", "semantics/error-layer-transport-text"}},
		{"vl_stopped_mid_body", "stopped_mid_body", false, http.StatusInternalServerError, "", []string{"semantics/error-layer-backend-failure-status", "semantics/error-layer-internal-go-text", "semantics/error-layer-backend-text"}},
		{"backend_timeout", "slow", true, http.StatusGatewayTimeout, e2eLokiDeadlineExceeded, []string{"semantics/error-layer-cancel-deadline", "semantics/error-layer-transport-text"}},
	}
	for _, fc := range failures {
		t.Run(fc.name, func(t *testing.T) {
			route := newErrLayerProxy(t, errLayerFaultBackend(t, fc.fault), func(cfg *proxy.Config) {
				if fc.timeout {
					cfg.BackendTimeout = 300 * time.Millisecond
				}
			})
			for _, rc := range []struct {
				name, path string
				params     url.Values
			}{
				{"query_range_log", "/loki/api/v1/query_range", url.Values{"query": {`{app="api-gateway"}`}, "limit": {"10"}}},
				{"query_range_stats", "/loki/api/v1/query_range", url.Values{"query": {levels}, "step": {"60"}}},
				{"labels", "/loki/api/v1/labels", url.Values{}},
			} {
				t.Run(rc.name, func(t *testing.T) {
					status, body := rejectedQueryGet(t, route, rc.path, window(rc.params), "0", nil)
					got := errLayerE2EClasses(status, body, nil, fc.wantStatus, fc.wantText, fc.timeout, false)
					errLayerE2EVerdict(t, got, fc.known, status, body)
				})
			}
		})
	}
}
