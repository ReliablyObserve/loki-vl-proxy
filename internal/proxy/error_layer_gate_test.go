package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The error-layer gate drives every client-facing route that reaches
// VictoriaLogs (or the ruler backend) through every backend failure mode and
// asserts Loki's answer: Loki's status and errorType, Loki's fixed texts where
// Loki has one, and no VictoriaLogs status, body, LogsQL, host:port or internal
// Go error in the body or the headers.
//
// Loki's answers (v3.7.9, pkg/util/server/error.go ClientHTTPStatusAndError):
//   - client cancel: 499 "the request was cancelled by the client" (:26, :103-105);
//   - deadline: 504 "request timed out, decrease the duration ..." (:27, :106-107);
//   - any other failed backend call: 500 with Loki's own text (:124-128);
//   - a full query queue: 429 "too many outstanding requests"
//     (pkg/queue/queue.go:21, pkg/lokifrontend/frontend/v1/frontend.go:32);
//   - max_query_series: 400 with limitErrTmpl (pkg/querier/queryrange/limits.go:46, :448);
//   - too many points: 400 errStepTooSmall (pkg/querier/queryrange/queryrangebase/query_range.go:43).
//
// Cells that still differ are listed in conformance/registry/error-layer-known-leaks.json
// with the registry case of every class they fail; a listed cell is skipped with
// its case ids while it still fails exactly those classes, and fails the test once
// it no longer does, so the list can only shrink. scripts/ci/check_conformance.py
// holds the list to its ceiling. Set ERROR_LAYER_RECORD=<file> to write the
// current list after a fix.

const (
	errLayerSentinel     = "VLSENTINEL"
	errLayerKnownLeakRel = "../../conformance/registry/error-layer-known-leaks.json"
	lokiClientCanceled   = "the request was cancelled by the client"
	lokiDeadlineExceeded = "request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed"
	lokiQueueFull        = "too many outstanding requests"
)

// Leak classes: one registry case each.
const (
	leakTransportText  = "semantics/error-layer-transport-text"
	leakBackendText    = "semantics/error-layer-backend-text"
	leakInternalText   = "semantics/error-layer-internal-go-text"
	leakFailureStatus  = "semantics/error-layer-backend-failure-status"
	leakCancelDeadline = "semantics/error-layer-cancel-deadline"
	leakSilentSuccess  = "semantics/error-layer-silent-success"
	leakLimitStatus    = "semantics/error-layer-proxy-limit-status"
)

// errLayerModes are the backend failure modes, in report order.
var errLayerModes = []string{
	"vl400_parse",   // VictoriaLogs rejects the LogsQL (parse error class)
	"vl400_limit",   // VictoriaLogs 400: memory limit while executing
	"vl422",         // VictoriaLogs 422 from a stats endpoint
	"vl500",         // VictoriaLogs 500
	"vl502_front",   // a proxy in front of VictoriaLogs answers 502
	"vl503_front",   // a proxy in front of VictoriaLogs answers 503
	"vl429",         // VictoriaLogs -search.maxConcurrentRequests queue full
	"conn_refused",  // nothing listens
	"conn_reset",    // the connection is reset before a response
	"timeout",       // VictoriaLogs answers after -backend-timeout
	"client_cancel", // the client goes away while VictoriaLogs runs
	"truncated",     // a 200 body cut off mid-stream
	"malformed",     // a 200 body that is not the endpoint's JSON
	"oversize",      // a valid body above -backend-max-buffered-response-bytes
}

type errLayerRoute struct {
	name    string
	path    string
	params  url.Values
	headers map[string]string
}

func errLayerRoutes(now time.Time) []errLayerRoute {
	start := fmt.Sprint(now.Add(-6 * time.Hour).Unix())
	end := fmt.Sprint(now.Unix())
	q := func(kv ...string) url.Values {
		v := url.Values{}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return v
	}
	sel := `{app="api"}`
	drilldown := map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app", "User-Agent": "Grafana/12.0.0"}
	return []errLayerRoute{
		{name: "query_range_log", path: "/loki/api/v1/query_range", params: q("query", sel, "start", start, "end", end, "limit", "10")},
		{name: "query_range_rate", path: "/loki/api/v1/query_range", params: q("query", `sum(rate({app="api"}[5m]))`, "start", start, "end", end, "step", "60")},
		{name: "query_range_stats", path: "/loki/api/v1/query_range", params: q("query", `sum by (level) (count_over_time({app="api"}[1m]))`, "start", start, "end", end, "step", "60")},
		{name: "query_range_stats_drilldown", path: "/loki/api/v1/query_range", params: q("query", `sum by (level) (count_over_time({app="api"}[1m]))`, "start", start, "end", end, "step", "60"), headers: drilldown},
		{name: "query_range_unwrap_rate_counter", path: "/loki/api/v1/query_range", params: q("query", `sum(rate_counter({app="api"} | logfmt | unwrap amount [5m]))`, "start", start, "end", end, "step", "60")},
		{name: "query_range_json", path: "/loki/api/v1/query_range", params: q("query", `sum by (level) (count_over_time({app="api"} | json [5m]))`, "start", start, "end", end, "step", "60")},
		{name: "query_instant", path: "/loki/api/v1/query", params: q("query", `sum(count_over_time({app="api"}[5m]))`, "time", end)},
		{name: "series", path: "/loki/api/v1/series", params: q("match[]", sel, "start", start, "end", end)},
		{name: "labels", path: "/loki/api/v1/labels", params: q("start", start, "end", end)},
		{name: "label_values", path: "/loki/api/v1/label/app/values", params: q("start", start, "end", end)},
		{name: "detected_fields", path: "/loki/api/v1/detected_fields", params: q("query", sel, "start", start, "end", end)},
		{name: "detected_field_values", path: "/loki/api/v1/detected_field/level/values", params: q("query", sel, "start", start, "end", end)},
		{name: "detected_labels", path: "/loki/api/v1/detected_labels", params: q("query", sel, "start", start, "end", end)},
		{name: "index_stats", path: "/loki/api/v1/index/stats", params: q("query", sel, "start", start, "end", end)},
		{name: "index_volume", path: "/loki/api/v1/index/volume", params: q("query", sel, "start", start, "end", end)},
		{name: "index_volume_range", path: "/loki/api/v1/index/volume_range", params: q("query", sel, "start", start, "end", end, "step", "60")},
		{name: "index_volume_range_drilldown", path: "/loki/api/v1/index/volume_range", params: q("query", sel, "start", start, "end", end, "step", "60"), headers: drilldown},
		{name: "patterns", path: "/loki/api/v1/patterns", params: q("query", sel, "start", start, "end", end, "step", "60")},
		{name: "tail", path: "/loki/api/v1/tail", params: q("query", sel)},
		{name: "rules", path: "/loki/api/v1/rules"},
		{name: "alerts", path: "/prometheus/api/v1/alerts"},
	}
}

// errLayerBackend is a fake VictoriaLogs (and ruler) that fails every call in
// one mode. Bodies are shaped per endpoint so a size or decode failure is the
// endpoint's own, not a shape mismatch.
type errLayerBackend struct {
	mode   string
	hits   atomic.Int64
	mu     sync.Mutex
	cancel context.CancelFunc // client_cancel: cancels the client request once VictoriaLogs is called
}

// errLayerQueryPath reports whether path is a query call of a route, as opposed
// to the proxy's own version probe or warm-up: only those fail.
func errLayerQueryPath(path string) bool {
	return strings.HasPrefix(path, "/select/logsql/") || strings.HasPrefix(path, "/api/v1/")
}

func (b *errLayerBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Read the request body first: the server notices a client that went away
	// (and cancels r.Context()) only once the body is consumed.
	_, _ = io.Copy(io.Discard, r.Body)
	if !errLayerQueryPath(r.URL.Path) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	b.hits.Add(1)
	switch b.mode {
	case "vl400_parse":
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error":"cannot parse query arg [_stream:{app=\"api\"} | stats count() hits]: unexpected token %s near _time:5m"}`, errLayerSentinel)
	case "vl400_limit":
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "cannot execute query [_stream:{app=\"api\"} | stats count()]: cannot calculate [stats by (_stream) count()], since it requires more than 100MB of memory %s; possible solutions: to increase -search.maxQueryDuration", errLayerSentinel)
	case "vl422":
		w.WriteHeader(http.StatusUnprocessableEntity)
		fmt.Fprintf(w, `{"status":"error","errorType":"422","error":"cannot execute query [_stream:{app=\"api\"}]: %s -search.maxQueryDuration exceeded"}`, errLayerSentinel)
	case "vl500":
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error":"unexpected error at lib/logstorage/storage.go:123 %s"}`, errLayerSentinel)
	case "vl502_front":
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(w, "<html><body>502 Bad Gateway nginx/1.25 %s</body></html>", errLayerSentinel)
	case "vl503_front":
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "<html><body>503 Service Unavailable envoy %s</body></html>", errLayerSentinel)
	case "vl429":
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintf(w, "couldn't start executing the request in 10.000 seconds, since -search.maxConcurrentRequests=16 concurrent requests are executed %s", errLayerSentinel)
	case "conn_reset":
		if hj, ok := w.(http.Hijacker); ok {
			if c, _, err := hj.Hijack(); err == nil {
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.SetLinger(0)
				}
				_ = c.Close()
			}
		}
	case "timeout":
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	case "client_cancel":
		b.mu.Lock()
		cancel := b.cancel
		b.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	case "truncated":
		body := errLayerShapedBody(r.URL.Path, 64)
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		c, buf, err := hj.Hijack()
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", len(body))
		_, _ = buf.WriteString(body[:len(body)/2])
		_ = buf.Flush()
		_ = c.Close()
	case "malformed":
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"success","data":{"result":[%s`+"\n", errLayerSentinel)
	case "oversize":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(errLayerShapedBody(r.URL.Path, 400)))
	}
}

// errLayerShapedBody is a valid answer of the VictoriaLogs (or ruler) endpoint
// at path, with n entries.
func errLayerShapedBody(path string, n int) string {
	var b strings.Builder
	ts := time.Now().Add(-time.Hour).Unix()
	switch {
	case strings.HasSuffix(path, "/select/logsql/query"), strings.HasSuffix(path, "/select/logsql/tail"):
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, `{"_time":"%s","_stream":"{app=\"api\"}","app":"api","level":"info","_msg":"{\"amount\":%d}","amount":"%d"}`+"\n",
				time.Unix(ts+int64(i), 0).UTC().Format(time.RFC3339Nano), i, i)
		}
	case strings.HasSuffix(path, "/stats_query_range"):
		b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `[%d,"%d"]`, ts+int64(i*60), i)
		}
		b.WriteString(`]}]}}`)
	case strings.HasSuffix(path, "/stats_query"):
		b.WriteString(`{"status":"success","data":{"resultType":"vector","result":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"metric":{"level":"l%d"},"value":[%d,"1"]}`, i, ts)
		}
		b.WriteString(`]}}`)
	case strings.HasSuffix(path, "/hits"):
		b.WriteString(`{"hits":[{"fields":{},"timestamps":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"%s"`, time.Unix(ts+int64(i*60), 0).UTC().Format(time.RFC3339))
		}
		b.WriteString(`],"values":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString("1")
		}
		fmt.Fprintf(&b, `],"total":%d}]}`, n)
	case strings.HasPrefix(path, "/select/logsql/"):
		b.WriteString(`{"values":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"value":"value-%04d","hits":1}`, i)
		}
		b.WriteString(`]}`)
	default:
		b.WriteString(`{"status":"success","data":{"groups":[],"alerts":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"labels":{"alertname":"a%04d"},"state":"firing"}`, i)
		}
		b.WriteString(`]}}`)
	}
	return b.String()
}

// errLayerOutcome is what a client saw.
type errLayerOutcome struct {
	status  int
	body    string
	headers http.Header
	// closeCode and closeText hold the websocket close frame of a tail
	// connection that was upgraded; status is then 101.
	closeCode int
	closeText string
}

// errLayerExpect is Loki's answer to a failure mode.
func errLayerExpect(mode string) (status int, text string) {
	switch mode {
	case "vl400_parse":
		return http.StatusBadRequest, ""
	case "vl429":
		return http.StatusTooManyRequests, lokiQueueFull
	case "timeout":
		return http.StatusGatewayTimeout, lokiDeadlineExceeded
	case "client_cancel":
		return 499, lokiClientCanceled
	case "oversize":
		// A buffered-size cap is a documented proxy limit: Loki's ResourceExhausted
		// is a 500 (pkg/util/server/error.go:93-100); a route that streams answers 200.
		return http.StatusInternalServerError, ""
	}
	return http.StatusInternalServerError, ""
}

var (
	errLayerTransportMarkers = []string{"dial tcp", "read tcp", "write tcp", "connection reset", "connection refused", `Post "http`, `Get "http`, "Client.Timeout", "/select/logsql", "/api/v1/"}
	errLayerBackendMarkers   = []string{errLayerSentinel, "-search.", "cannot execute", "cannot parse", "cannot calculate", "logstorage", "_stream", "_msg", "| stats", "nginx", "envoy", "VictoriaLogs", "VL ", "backend returned", "maxConcurrentRequests", "X-Proxy-Upstream"}
	errLayerInternalMarkers  = []string{"unexpected EOF", "invalid character", "scanning", "body exceeds configured limit", "context canceled", "deadline exceeded", "looking for beginning", "after top-level value", "chunked"}
)

// errLayerText is the client-visible error text: the JSON envelope's error
// (and message) field, or the raw body, or the close frame text.
func errLayerText(o errLayerOutcome) string {
	if o.closeText != "" {
		return o.closeText
	}
	var env struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(o.body), &env) == nil && env.Error != "" {
		return env.Error
	}
	return strings.TrimSpace(o.body)
}

// errLayerClasses returns the leak classes an outcome fails for a mode.
func errLayerClasses(mode string, o errLayerOutcome, backendHost string) []string {
	classes := map[string]bool{}
	wantStatus, wantText := errLayerExpect(mode)
	status := o.status
	if status == http.StatusSwitchingProtocols {
		// An upgraded tail reports failures in its close frame: Loki closes with
		// 1011 and its error text (pkg/querier/http.go TailHandler).
		if o.closeCode == 0 || o.closeCode == websocket.CloseNormalClosure {
			classes[leakSilentSuccess] = true
		}
		status = wantStatus
	}
	switch {
	case status == http.StatusOK && mode == "oversize":
		// streamed within no limit: nothing to check
	case status == http.StatusOK || status == http.StatusNoContent:
		classes[leakSilentSuccess] = true
	case status != wantStatus:
		switch mode {
		case "timeout", "client_cancel":
			classes[leakCancelDeadline] = true
		case "oversize":
			classes[leakLimitStatus] = true
		default:
			classes[leakFailureStatus] = true
		}
	}
	if o.closeText == "" && status >= 400 && o.status != http.StatusSwitchingProtocols {
		var env struct {
			Status    string `json:"status"`
			ErrorType string `json:"errorType"`
		}
		if json.Unmarshal([]byte(o.body), &env) != nil || env.Status != "error" || env.ErrorType != lokiErrorType(status) {
			classes[leakFailureStatus] = true
		}
	}
	visible := o.body + "\n" + o.closeText
	for k, vs := range o.headers {
		visible += "\n" + k + ": " + strings.Join(vs, ",")
	}
	if strings.Contains(visible, backendHost) || containsAny(visible, errLayerTransportMarkers) {
		classes[leakTransportText] = true
	}
	if containsAny(visible, errLayerBackendMarkers) {
		classes[leakBackendText] = true
	}
	// A transport error carries Go's wording too ("context deadline exceeded" or
	// "timeout awaiting response headers", whichever fires first); the transport
	// class covers it, so the internal class stays deterministic.
	if !classes[leakTransportText] && containsAny(visible, errLayerInternalMarkers) {
		classes[leakInternalText] = true
	}
	if wantText != "" && status == wantStatus && o.status != http.StatusOK && errLayerText(o) != wantText {
		if mode == "timeout" || mode == "client_cancel" {
			classes[leakCancelDeadline] = true
		} else {
			classes[leakFailureStatus] = true
		}
	}
	if mode == "oversize" && o.status >= 400 && !strings.Contains(errLayerText(o), "increase -") {
		// A proxy limit names the flag that sets it.
		classes[leakLimitStatus] = true
	}
	out := make([]string, 0, len(classes))
	for c := range classes {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func errLayerKnownLeaks(t *testing.T) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.FromSlash(errLayerKnownLeakRel))
	if err != nil {
		t.Fatalf("read known leaks: %v", err)
	}
	var doc struct {
		Leaks map[string][]string `json:"leaks"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse known leaks: %v", err)
	}
	return doc.Leaks
}

// runErrLayerCell drives one route against a fake backend in one mode.
func runErrLayerCell(t *testing.T, rt errLayerRoute, mode string) (errLayerOutcome, string) {
	t.Helper()
	backend := &errLayerBackend{mode: mode}
	srv := httptest.NewServer(backend)
	backendURL := srv.URL
	if mode == "conn_refused" {
		srv.Close()
	} else {
		t.Cleanup(srv.Close)
	}
	c := cache.New(time.Minute, 100)
	t.Cleanup(c.Close)
	cfg := Config{
		BackendURL:       backendURL,
		RulerBackendURL:  backendURL,
		AlertsBackendURL: backendURL,
		Cache:            c,
		LogLevel:         "error",
		// Generous, so a slow scheduler never turns another mode into a timeout.
		BackendTimeout: 5 * time.Second,
		TailMode:       TailModeNative,
	}
	if mode == "timeout" {
		cfg.BackendTimeout = 250 * time.Millisecond
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
	host := strings.TrimPrefix(backendURL, "http://")

	if rt.name == "tail" {
		return runErrLayerTail(t, mux, rt), host
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if mode == "client_cancel" {
		backend.mu.Lock()
		backend.cancel = cancel
		backend.mu.Unlock()
	}
	target := rt.path
	if len(rt.params) > 0 {
		target += "?" + rt.params.Encode()
	}
	req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	req.Header.Set("X-Scope-OrgID", "0")
	for k, v := range rt.headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if backend.hits.Load() == 0 && mode != "conn_refused" {
		// Only query calls count (errLayerQueryPath): a route answered without
		// one never met the failure.
		t.Fatalf("route %s never called the backend; the cell proves nothing (status %d, body %.300q)", rt.name, w.Code, w.Body.String())
	}
	return errLayerOutcome{status: w.Code, body: w.Body.String(), headers: w.Header()}, host
}

// runErrLayerTail opens a real websocket tail (native mode, so a backend
// failure closes the connection instead of falling back to polling).
func runErrLayerTail(t *testing.T, mux *http.ServeMux, rt errLayerRoute) errLayerOutcome {
	t.Helper()
	front := httptest.NewServer(mux)
	t.Cleanup(front.Close)
	wsURL := "ws" + strings.TrimPrefix(front.URL, "http") + rt.path + "?" + rt.params.Encode()
	header := http.Header{"X-Scope-OrgID": {"0"}}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial(wsURL, header)
	if err != nil {
		if resp == nil {
			t.Fatalf("tail dial: %v", err)
		}
		defer resp.Body.Close()
		var b strings.Builder
		_, _ = bufio.NewReader(resp.Body).WriteTo(&b)
		return errLayerOutcome{status: resp.StatusCode, body: b.String(), headers: resp.Header}
	}
	defer conn.Close()
	out := errLayerOutcome{status: http.StatusSwitchingProtocols}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if ce, ok := err.(*websocket.CloseError); ok {
				out.closeCode, out.closeText = ce.Code, ce.Text
			}
			return out
		}
	}
}

// TestErrorLayer_NoVictoriaLogsLeak is the regression gate of the proxy's
// single Loki-shaped error layer (see the file comment).
// conformance: semantics/error-layer-transport-text, semantics/error-layer-backend-text, semantics/error-layer-internal-go-text, semantics/error-layer-backend-failure-status, semantics/error-layer-cancel-deadline, semantics/error-layer-silent-success, semantics/error-layer-proxy-limit-status, backend-deadlines-and-cancellation, status-500, status-502, status-503
func TestErrorLayer_NoVictoriaLogsLeak(t *testing.T) {
	known := errLayerKnownLeaks(t)
	recorded := map[string][]string{}
	var recordMu sync.Mutex
	now := time.Now().Truncate(time.Minute)
	for _, rt := range errLayerRoutes(now) {
		rt := rt
		t.Run(rt.name, func(t *testing.T) {
			for _, mode := range errLayerModes {
				mode := mode
				t.Run(mode, func(t *testing.T) {
					outcome, host := runErrLayerCell(t, rt, mode)
					got := errLayerClasses(mode, outcome, host)
					key := rt.name + " " + mode
					if len(got) > 0 {
						recordMu.Lock()
						recorded[key] = got
						recordMu.Unlock()
					}
					want := known[key]
					switch {
					case len(got) == 0 && len(want) == 0:
						return
					case strings.Join(got, ",") == strings.Join(want, ","):
						t.Skipf("known leak %s: status %d, text %.160q", strings.Join(want, ", "), outcome.status, errLayerText(outcome))
					case len(want) == 0:
						t.Errorf("new error-layer leak %v: status %d, body %.300q, headers %v", got, outcome.status, outcome.body, outcome.headers)
					default:
						t.Errorf("known leak list for %q is stale: listed %v, now %v (status %d, text %.200q); update %s (ERROR_LAYER_RECORD=<file> writes it)",
							key, want, got, outcome.status, errLayerText(outcome), errLayerKnownLeakRel)
					}
				})
			}
		})
	}
	if path := os.Getenv("ERROR_LAYER_RECORD"); path != "" {
		total := 0
		for _, ids := range recorded {
			total += len(ids)
		}
		doc := map[string]any{"ceiling": total, "leaks": recorded}
		raw, _ := json.MarshalIndent(doc, "", "  ")
		if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
			t.Fatalf("record: %v", err)
		}
	}
}

// TestErrorLayer_LokiLimitTexts locks the limits the proxy already answers
// with Loki's status and text: max_query_series on a stats-backed metric and
// the 11,000 points resolution cap.
// conformance: semantics/error-layer-backend-failure-status, status-400
func TestErrorLayer_LokiLimitTexts(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		ts := now.Add(-30 * time.Minute).Unix()
		switch {
		case strings.HasSuffix(r.URL.Path, "/stats_query_range"):
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[[%d,"1"]]},{"metric":{"level":"error"},"values":[[%d,"2"]]}]}}`, ts, ts)
		case strings.HasSuffix(r.URL.Path, "/select/logsql/query"):
			for i, level := range []string{"info", "error", "warn"} {
				fmt.Fprintf(w, `{"_time":"%s","_stream":"{app=\"api\",level=\"%s\"}","app":"api","level":"%s","_msg":"x"}`+"\n",
					time.Unix(ts+int64(i), 0).UTC().Format(time.RFC3339Nano), level, level)
			}
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	defer srv.Close()
	c := cache.New(time.Minute, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", MaxStatsQuerySeries: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	start := fmt.Sprint(now.Add(-time.Hour).Unix())
	end := fmt.Sprint(now.Unix())
	for _, tc := range []struct {
		name, query, step, want string
	}{
		{"max_query_series", `sum by (level) (count_over_time({app="api"}[1m]))`, "60",
			"maximum number of series (1) reached for a single query; consider reducing query cardinality by adding more specific stream selectors, reducing the time range, or aggregating results with functions like sum(), count() or topk()"},
		{"max_points", `sum(count_over_time({app="api"}[1m]))`, "0.1", errLokiStepTooSmall},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := url.Values{"query": {tc.query}, "start": {start}, "end": {end}, "step": {tc.step}}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+v.Encode(), nil))
			o := errLayerOutcome{status: w.Code, body: w.Body.String(), headers: w.Header()}
			if w.Code != http.StatusBadRequest || errLayerText(o) != tc.want {
				t.Fatalf("got %d %q, want 400 %q", w.Code, errLayerText(o), tc.want)
			}
		})
	}
}
