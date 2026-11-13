package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
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
// current list after a fix. A route that fails only after an earlier read worked
// is the mixed-failure gate's (error_layer_mixed_test.go).

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
	leakBodyShape      = "semantics/error-layer-body-shape"
)

// Error body shapes: what Loki writes on a route.
const (
	// The empty shape is pkg/util/server/error.go WriteError, the query
	// frontend's shape on every query, metadata and tail route.
	// errShapeRuler: pkg/ruler/base/api.go respondError, the Prometheus JSON of
	// the rules API (no Content-Type, so Go sniffs text/plain; no nosniff).
	errShapeRuler = "ruler"
	// errShapeAlerts: the same JSON from PrometheusAlerts, which sets
	// Content-Type: application/json first (api.go:286).
	errShapeAlerts = "alerts"
	// errShapeHTTPError: Go's http.Error, the legacy rule views and the delete API.
	errShapeHTTPError = "httperr"
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
	shape   string // the error body shape Loki writes on the route
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
		// A parser filter gives a strict and a relaxed query candidate, and a field outside the
		// stored ones reaches the unpack and label fallbacks.
		{name: "detected_fields_parser", path: "/loki/api/v1/detected_fields", params: q("query", `{app="api"} | json | level="info"`, "start", start, "end", end)},
		{name: "detected_field_values_parser", path: "/loki/api/v1/detected_field/status/values", params: q("query", `{app="api"} | json | level="info"`, "start", start, "end", end)},
		{name: "detected_field_values_field", path: "/loki/api/v1/detected_field/status/values", params: q("query", sel, "start", start, "end", end)},
		{name: "detected_labels", path: "/loki/api/v1/detected_labels", params: q("query", sel, "start", start, "end", end)},
		{name: "index_stats", path: "/loki/api/v1/index/stats", params: q("query", sel, "start", start, "end", end)},
		{name: "index_volume", path: "/loki/api/v1/index/volume", params: q("query", sel, "start", start, "end", end)},
		{name: "index_volume_range", path: "/loki/api/v1/index/volume_range", params: q("query", sel, "start", start, "end", end, "step", "60")},
		{name: "index_volume_range_drilldown", path: "/loki/api/v1/index/volume_range", params: q("query", sel, "start", start, "end", end, "step", "60"), headers: drilldown},
		{name: "patterns", path: "/loki/api/v1/patterns", params: q("query", sel, "start", start, "end", end, "step", "60")},
		{name: "tail", path: "/loki/api/v1/tail", params: q("query", sel)},
		{name: "rules", path: "/loki/api/v1/rules", shape: errShapeHTTPError},
		{name: "alerts", path: "/prometheus/api/v1/alerts", shape: errShapeAlerts},
	}
}

// errLayerBackend is a fake VictoriaLogs (and ruler) that fails every call in
// one mode. Bodies are shaped per endpoint so a size or decode failure is the
// endpoint's own, not a shape mismatch.
type errLayerBackend struct {
	mode string
	// stream runs the proxy with -stream-response, whose log queries send the
	// status line before the body is read (stream_response_broken_body_test.go).
	stream bool
	hits   atomic.Int64
	// failSig, okBody, seen and failHits drive the mixed-failure gate
	// (error_layer_mixed_test.go): see errLayerBackend.recordCall.
	failSig string
	// failContains selects the failing calls by a part of their signature instead of the whole of it.
	failContains []string
	okBody       string
	seen         map[string]struct{}
	order        []string
	failHits     atomic.Int64
	mu           sync.Mutex
	cancel       context.CancelFunc // client_cancel: cancels the client request once VictoriaLogs is called
	// tailHit and released serve a tail's client_cancel: the backend closes
	// tailHit when the tail stream call arrives and released once the proxy
	// has dropped that call (its request context is done).
	tailHit     chan struct{}
	released    chan struct{}
	hitOnce     sync.Once
	releaseOnce sync.Once
}

// errLayerQueryPath reports whether path is a query call of a route, as opposed
// to the proxy's own version probe or warm-up: only those fail.
func errLayerQueryPath(path string) bool {
	return strings.HasPrefix(path, "/select/logsql/") || strings.HasPrefix(path, "/api/v1/")
}

func (b *errLayerBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Read the request body first: the server notices a client that went away
	// (and cancels r.Context()) only once the body is consumed.
	raw, _ := io.ReadAll(r.Body)
	if !errLayerQueryPath(r.URL.Path) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	b.hits.Add(1)
	if b.recordCall(r, raw) {
		b.serveOK(w, r)
		return
	}
	b.fail(w, r)
}

// fail answers one call in the backend's failure mode.
func (b *errLayerBackend) fail(w http.ResponseWriter, r *http.Request) {
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
		if b.tailHit != nil {
			if !strings.HasSuffix(r.URL.Path, "/select/logsql/tail") {
				w.WriteHeader(http.StatusNoContent) // the preflight probe passes
				return
			}
			b.hitOnce.Do(func() { close(b.tailHit) })
			select {
			case <-r.Context().Done():
				b.releaseOnce.Do(func() { close(b.released) })
			case <-time.After(5 * time.Second):
			}
			return
		}
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

// errLayerRowsAt is the first timestamp of every shaped body: one instant for
// the whole run, an hour before it started, so a gate that compares a route's
// answer with a reference answer reads the same rows both times.
var errLayerRowsAt = time.Now().Add(-time.Hour).Truncate(time.Minute).Unix()

// errLayerShapedBody is a valid answer of the VictoriaLogs (or ruler) endpoint
// at path, with n entries.
func errLayerShapedBody(path string, n int) string {
	var b strings.Builder
	ts := errLayerRowsAt
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
	// released: a tail's client went away and the proxy dropped its
	// VictoriaLogs stream.
	released bool
	// aborted: the handler aborted the response (http.ErrAbortHandler) after
	// sending the status line, which a client sees as a broken response; body
	// is what was written before. streamed: the proxy ran with -stream-response,
	// the only setting where an abort is an answer.
	aborted, streamed bool
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

// errLayerText is the client-visible error text: the ruler JSON's error field,
// or the raw body, or the close frame text.
func errLayerText(o errLayerOutcome) string {
	if o.closeText != "" {
		return o.closeText
	}
	if strings.HasPrefix(o.body, `{"status":"error","data":null,`) {
		var env struct {
			Error string `json:"error"`
		}
		if json.Unmarshal([]byte(o.body), &env) == nil && env.Error != "" {
			return env.Error
		}
	}
	return strings.TrimSpace(o.body)
}

// errLayerBodyShapeProblem reports how an error answer differs from the body
// shape Loki writes on the route: its Content-Type, whether it carries nosniff,
// its framing (the raw message, the message and a newline, or the ruler's JSON
// in its key order with an errorType that matches the status) and no JSON
// envelope where Loki writes text.
func errLayerBodyShapeProblem(rt errLayerRoute, o errLayerOutcome) string {
	wantType := "text/plain; charset=utf-8"
	if rt.shape == errShapeAlerts {
		wantType = "application/json"
	}
	if got := o.headers.Get("Content-Type"); got != wantType {
		return fmt.Sprintf("Content-Type %q, want %s", got, wantType)
	}
	switch rt.shape {
	case errShapeRuler, errShapeAlerts:
		if o.headers.Get("X-Content-Type-Options") != "" {
			return "X-Content-Type-Options on the ruler's JSON, which Loki does not send"
		}
		wantErrorType := "server_error"
		if o.status == http.StatusBadRequest {
			wantErrorType = "bad_data"
		} else if o.status != http.StatusInternalServerError {
			return fmt.Sprintf("status %d, but the ruler answers only 400 and 500", o.status)
		}
		if !strings.HasPrefix(o.body, `{"status":"error","data":null,"errorType":"`+wantErrorType+`","error":`) || !strings.HasSuffix(o.body, `"}`) || strings.Contains(o.body, "\n") {
			return "body is not the ruler's {status,data,errorType,error} JSON with the errorType of its status"
		}
	case errShapeHTTPError:
		if o.headers.Get("X-Content-Type-Options") != "nosniff" || !strings.HasSuffix(o.body, "\n") || strings.HasPrefix(o.body, "{") {
			return "body is not http.Error's nosniff text and newline"
		}
	default:
		if o.headers.Get("X-Content-Type-Options") != "nosniff" {
			return "no X-Content-Type-Options: nosniff"
		}
		if strings.HasPrefix(o.body, "{") || strings.HasSuffix(o.body, "\n") {
			return "body is not the message alone"
		}
	}
	return ""
}

// errLayerClasses returns the leak classes an outcome fails for a mode.
func errLayerClasses(rt errLayerRoute, mode string, o errLayerOutcome, backendHost string) []string {
	classes := map[string]bool{}
	if o.aborted {
		// A streamed answer already sent as 200 cannot become an error. Only a
		// body that breaks off mid-answer may end this way, and what was sent
		// must never be a complete JSON document.
		if !o.streamed || mode != "truncated" || json.Valid([]byte(o.body)) {
			classes[leakSilentSuccess] = true
		}
		return errLayerClassList(classes)
	}
	if rt.name == "tail" && mode == "client_cancel" {
		// The client is gone, so Loki's 499 text has no reader: what a tail owes
		// a cancelled client is to stop its VictoriaLogs stream.
		if !o.released {
			classes[leakCancelDeadline] = true
		}
		return errLayerClassList(classes)
	}
	wantStatus, wantText := errLayerExpect(mode)
	switch rt.shape {
	case errShapeRuler, errShapeAlerts:
		// The ruler answers every GetRules failure with respondServerError, a
		// 500 (pkg/ruler/base/api.go:196-200, :301-305), whatever the cause.
		wantStatus = http.StatusInternalServerError
		if mode != "timeout" && mode != "client_cancel" {
			wantText = ""
		}
	case errShapeHTTPError:
		// A backend that answered an error status is a failed call, 500; a
		// transport timeout or cancel keeps Loki's 504 and 499.
		if mode != "timeout" && mode != "client_cancel" {
			wantStatus, wantText = http.StatusInternalServerError, ""
		}
	}
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
	if o.closeText == "" && status >= 400 && o.status != http.StatusSwitchingProtocols && errLayerBodyShapeProblem(rt, o) != "" {
		classes[leakBodyShape] = true
	}
	visible := o.body + "\n" + o.closeText
	for k, vs := range o.headers {
		if k == "Transfer-Encoding" {
			continue // framing of a streamed answer, not text
		}
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
	if mode == "oversize" && o.status >= 400 && !errLayerMessageSizeRE.MatchString(errLayerText(o)) {
		// A proxy buffer limit answers Loki's message size text, which carries
		// no flag: the flag that sets the limit is in the log line.
		classes[leakLimitStatus] = true
	}
	return errLayerClassList(classes)
}

// errLayerMessageSizeRE is Loki's text for a response above its message size.
var errLayerMessageSizeRE = regexp.MustCompile(`^rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max \(\d+ vs\. \d+\)$`)

func errLayerClassList(classes map[string]bool) []string {
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
	return runErrLayerBackendCell(t, rt, &errLayerBackend{mode: mode})
}

// runErrLayerBackendCell drives one route against the given fake backend.
func runErrLayerBackendCell(t *testing.T, rt errLayerRoute, backend *errLayerBackend) (errLayerOutcome, string) {
	t.Helper()
	mode := backend.mode
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
	if errLayerBackendRetries {
		cfg.BackendRetryAttempts = DefaultBackendRetryAttempts
		cfg.BackendRetryMaxWait = DefaultBackendRetryMaxWait
		cfg.BackendRetryBudgetRatio = DefaultBackendRetryBudgetRatio
	}
	if mode == "oversize" {
		cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = 4096
	}
	cfg.StreamResponse = backend.stream
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if p.backendRetry != nil {
		p.backendRetry.initial = time.Millisecond
		p.backendRetry.max = 4 * time.Millisecond
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	host := strings.TrimPrefix(backendURL, "http://")

	if rt.name == "tail" {
		return runErrLayerTail(t, mux, rt, backend), host
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
	aborted := serveRecoveringAbort(shippedChain(mux), w, req)
	if backend.hits.Load() == 0 && mode != "conn_refused" {
		// Only query calls count (errLayerQueryPath): a route answered without
		// one never met the failure.
		t.Fatalf("route %s never called the backend; the cell proves nothing (status %d, body %.300q)", rt.name, w.Code, w.Body.String())
	}
	return errLayerOutcome{status: w.Code, body: w.Body.String(), headers: w.Header(), aborted: aborted, streamed: backend.stream}, host
}

// serveRecoveringAbort serves req and reports whether the handler aborted the
// response with http.ErrAbortHandler, which net/http turns into a connection
// closed without the end of the body. Any other panic is not swallowed.
func serveRecoveringAbort(h http.Handler, w http.ResponseWriter, req *http.Request) (aborted bool) {
	defer func() {
		if v := recover(); v != nil {
			if v != http.ErrAbortHandler {
				panic(v)
			}
			aborted = true
		}
	}()
	h.ServeHTTP(w, req)
	return false
}

// runErrLayerTail opens a real websocket tail (native mode, so a backend
// failure closes the connection instead of falling back to polling).
func runErrLayerTail(t *testing.T, mux *http.ServeMux, rt errLayerRoute, backend *errLayerBackend) errLayerOutcome {
	t.Helper()
	if backend.mode == "client_cancel" {
		backend.tailHit = make(chan struct{})
		backend.released = make(chan struct{})
	}
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
	if backend.mode == "client_cancel" {
		// The client goes away once the proxy has called VictoriaLogs' tail.
		select {
		case <-backend.tailHit:
		case <-time.After(3 * time.Second):
			return out
		}
		_ = conn.Close()
		select {
		case <-backend.released:
			out.released = true
		case <-time.After(3 * time.Second):
		}
		return out
	}
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
	runErrLayerGate(t)
}

// errLayerBackendRetries runs the gate's proxies with the shared VictoriaLogs
// retry on (the gate's other runs send every read once, as the library default
// does).
var errLayerBackendRetries bool

// TestErrorLayer_NoVictoriaLogsLeakWithBackendRetries runs the gate again with
// the backend retries on. Every mode fails every call, so after the last try the
// client gets the answer it gets without retries: the same cells leak, and no
// new one does. (A transient failure that recovers is
// TestBackendRetry_ClientGetsTheAnswerLokiWouldGive.)
// conformance: semantics/transient-backend-failure-is-retried, semantics/error-layer-backend-failure-status, semantics/error-layer-transport-text, semantics/error-layer-backend-text
func TestErrorLayer_NoVictoriaLogsLeakWithBackendRetries(t *testing.T) {
	errLayerBackendRetries = true
	t.Cleanup(func() { errLayerBackendRetries = false })
	runErrLayerGate(t)
}

func runErrLayerGate(t *testing.T) {
	t.Helper()
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
					got := errLayerClasses(rt, mode, outcome, host)
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
		writeErrLayerRecord(t, path, recorded)
	}
}

// writeErrLayerRecord writes the leak list a gate run recorded, with its ceiling.
func writeErrLayerRecord(t *testing.T, path string, recorded map[string][]string) {
	t.Helper()
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

// TestErrorLayer_WindowRetryCancelDeadline drives the windowed log query_range
// path (-query-range-windowing) into its retry backoff: VictoriaLogs answers
// every window with a retryable 502, and the request ends while the proxy
// waits to retry, by a client cancel or by the tenant's query_timeout. Loki
// answers 499 "the request was cancelled by the client" and 504 with its
// deadline text (pkg/util/server/error.go:26-27, :103-107); a request whose
// context ended has no other answer. Both backoff waits are covered: the batch
// backoff (several windows fetched in parallel, then halved) and the
// single-window retry.
// conformance: semantics/error-layer-cancel-deadline, semantics/error-layer-internal-go-text, backend-deadlines-and-cancellation
func TestErrorLayer_WindowRetryCancelDeadline(t *testing.T) {
	end := time.Now().Truncate(time.Hour)
	start := end.Add(-4 * time.Hour)
	rt := errLayerRoute{name: "query_range_log_window_retry", path: "/loki/api/v1/query_range"}
	for _, wait := range []struct {
		name     string
		parallel int
	}{
		{"batch_backoff", 4},
		{"single_window_retry", 1},
	} {
		for _, mode := range []string{"client_cancel", "timeout"} {
			t.Run(wait.name+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var (
					queries  atomic.Int64
					mu       sync.Mutex
					attempts = map[string]int{}
					once     sync.Once
				)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_ = r.ParseForm()
					if r.URL.Path != "/select/logsql/query" {
						// The hits prefilter and the version probe fail
						// plainly: the windows are fetched without them.
						w.WriteHeader(http.StatusNotFound)
						return
					}
					queries.Add(1)
					mu.Lock()
					key := r.Form.Get("start") + ":" + r.Form.Get("end")
					attempts[key]++
					last := attempts[key] == queryRangeWindowFetchAttempts
					mu.Unlock()
					if last && mode == "client_cancel" {
						// The window's own retries are spent: the client goes
						// away while the proxy waits to retry the batch (the
						// first wait is 100ms). A cancel during a window's own
						// retries is answered by the fetch error path.
						once.Do(func() { time.AfterFunc(50*time.Millisecond, cancel) })
					}
					w.WriteHeader(http.StatusBadGateway)
					fmt.Fprintf(w, "<html><body>502 Bad Gateway nginx %s</body></html>", errLayerSentinel)
				}))
				defer srv.Close()
				c := cache.New(time.Minute, 100)
				defer c.Close()
				cfg := Config{
					BackendURL:                 srv.URL,
					Cache:                      c,
					LogLevel:                   "error",
					BackendTimeout:             5 * time.Second,
					QueryRangeWindowingEnabled: true,
					QueryRangeSplitInterval:    time.Hour,
					QueryRangeMaxParallel:      wait.parallel,
					QueryRangeParallelMin:      1,
					QueryRangeParallelMax:      wait.parallel,
					QueryRangeAdaptiveParallel: false,
					QueryRangeFreshness:        10 * time.Minute,
				}
				if mode == "timeout" {
					// Loki's per-tenant query_timeout, shorter than the first wait.
					cfg.TenantDefaultLimits = map[string]any{"query_timeout": "50ms"}
				}
				p, err := New(cfg)
				if err != nil {
					t.Fatalf("new proxy: %v", err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				mux := http.NewServeMux()
				p.RegisterRoutes(mux)
				v := url.Values{
					"query": {`{app="api"}`},
					"start": {fmt.Sprint(start.UnixNano())},
					"end":   {fmt.Sprint(end.UnixNano() - 1)},
					"limit": {"10"},
				}
				req := httptest.NewRequest(http.MethodGet, rt.path+"?"+v.Encode(), nil).WithContext(ctx)
				req.Header.Set("X-Scope-OrgID", "0")
				w := httptest.NewRecorder()
				shippedChain(mux).ServeHTTP(w, req)
				if queries.Load() == 0 {
					t.Fatalf("no window was fetched; the cell proves nothing (status %d, body %.300q)", w.Code, w.Body.String())
				}
				outcome := errLayerOutcome{status: w.Code, body: w.Body.String(), headers: w.Header()}
				if got := errLayerClasses(rt, mode, outcome, strings.TrimPrefix(srv.URL, "http://")); len(got) > 0 {
					t.Fatalf("error-layer leak %v: status %d, body %.300q", got, outcome.status, outcome.body)
				}
			})
		}
	}
}

// TestLimitAnswersAreLokisTextAndLogTheFlag: every proxy limit and queue answer
// is the typed error of its Loki class. A client reads Loki's status and text
// with no flag name in the body; the request error log line carries the flag
// and its value.
// conformance: semantics/error-layer-proxy-limit-status, status-400, status-429, status-500
func TestLimitAnswersAreLokisTextAndLogTheFlag(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		status    int
		text      string
		flag      string // the log line's limit_flag, without its dash
		value     string // the log line's limit_value, "" when the answer has none
		retryKind error  // a sentinel the typed error must still match
	}{
		{"series limit", newSeriesLimitError(context.Background(), 500), 400, lokierr.SeriesLimit(500).Text(), "max-stats-query-series", "500", nil},
		{"row limit", manualRowLimitError(3), 400, limitText, "manual-range-metric-row-limit", "3", nil},
		{"row limit of a count precheck", newRowLimitError(3, "", 5000), 400, limitText, "manual-range-metric-row-limit", "3", nil},
		{"row limit of a bucket grid", newRowLimitError(3, "stats rows in one bucket grid", 0), 400, limitText, "manual-range-metric-row-limit", "3", nil},
		{"series budget of a fold", newSeriesLimitError(foldingBudgetContext(7), 7), 400, limitText, "manual-range-metric-row-limit", "7", nil},
		{"binary arrays budget", newBinaryBudgetError(flagBinaryMetricMaxArrays), 400, limitText, "binary-metric-max-arrays", "", nil},
		{"binary operand bytes budget", fmt.Errorf("left query: %w", newBinaryBudgetError(flagBinaryMetricMaxOperandBytes)), 400, limitText, "binary-metric-max-operand-bytes", "", nil},
		{"backend body", fmt.Errorf("stats: %w", newBackendBodyTooLarge(4096)), 500, fmt.Sprintf(grpcSizeText, 4097, 4096), "backend-max-buffered-response-bytes", "4096", errBodyTooLarge},
		{"manual metric response", newManualMetricResponseTooLarge(64), 500, fmt.Sprintf(grpcSizeText, 65, 64), "backend-max-buffered-response-bytes", "64", errManualMetricResponseTooLarge},
		{"binary operand response", newBufferLimitError(flagBackendMaxBufferedResponseBytes, 17, 16, causeBinaryOperand), 500, fmt.Sprintf(grpcSizeText, 17, 16), "backend-max-buffered-response-bytes", "16", nil},
		{"ordered JSON metric output", newBufferLimitError(flagOrderedJSONMetricMaxBytes, 2048, 1024, causeOrderedOutput), 500, fmt.Sprintf(grpcSizeText, 2048, 1024), "ordered-json-metric-max-bytes", "1024", nil},
		{"label values", newLabelValuesTooLarge(2048, 1024), 500, fmt.Sprintf(grpcSizeText, 2048, 1024), "label-values-max-response-bytes", "1024", nil},
		{"heavy queue", newHeavyQueueFull(&heavyQueryQueueFullError{maxConcurrent: 4, queueWait: time.Second}), 429, queueText, "backend-max-concurrent-heavy-queries", "4", nil},
		{"metadata scan queue", newHeavyQueueFull(&heavyQueryQueueFullError{limitFlag: metadataScanLimitFlag, work: metadataScanLimitWork, maxConcurrent: 3, queueWait: time.Second, adaptive: "adaptive limit 2"}), 429, queueText, "backend-max-concurrent-metadata-scans", "3", nil},
		{"query length", queryLengthLimit(strings.Repeat("x", 600), 512), 400, "parse error : input size too long (600 > 512)", "max-query-length-bytes", "512", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, logs := logProxy()
			rec := httptest.NewRecorder()
			if status := p.fail(rec, requestOnRoute("query_range", "/loki/api/v1/query_range", context.Background()), tc.err); status != tc.status {
				t.Fatalf("status %d, want %d", status, tc.status)
			}
			if rec.Code != tc.status || rec.Body.String() != tc.text {
				t.Fatalf("answer %d %q, want %d %q", rec.Code, rec.Body.String(), tc.status, tc.text)
			}
			if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
				t.Errorf("Content-Type %q", got)
			}
			if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options %q", got)
			}
			body := rec.Body.String()
			for _, leak := range []string{"-" + tc.flag, tc.flag, "VictoriaLogs", "backend", "narrow the query", "increase -", "raise -"} {
				if strings.Contains(body, leak) {
					t.Errorf("client body %q carries %q", body, leak)
				}
			}
			line := logs.String()
			if !strings.Contains(line, "limit_flag=-"+tc.flag) {
				t.Errorf("log line %q lacks limit_flag=-%s", line, tc.flag)
			}
			if tc.value != "" && !strings.Contains(line, "limit_value="+tc.value) {
				t.Errorf("log line %q lacks limit_value=%s", line, tc.value)
			}
			if tc.retryKind != nil && !errors.Is(tc.err, tc.retryKind) {
				t.Errorf("the typed error no longer matches %v", tc.retryKind)
			}
			typed, ok := lokierr.As(tc.err)
			if !ok {
				t.Fatalf("%v is not typed", tc.err)
			}
			if kind := typed.Kind(); kind != lokierr.KindLimit && kind != lokierr.KindParse && kind != lokierr.KindQueueFull {
				t.Errorf("kind %v", kind)
			}
			if got := statusFromUpstreamErr(tc.err); typed.Kind() != lokierr.KindParse && got != tc.status {
				t.Errorf("statusFromUpstreamErr = %d, want %d", got, tc.status)
			}
		})
	}
}

// foldingBudgetContext is the context of a request that folds its series
// afterwards and reads them under a raw-row budget.
func foldingBudgetContext(budget int) context.Context {
	ctx := context.WithValue(context.Background(), foldedSumKey{}, true)
	return context.WithValue(ctx, seriesLimitScopeKey{}, &seriesLimitScope{foldBudget: budget})
}

// TestLimitCausesStayInTheLog: the operator wording of each limit, flag and
// all, is the typed error's cause: it reaches the log line as upstream_error and
// never the body.
func TestLimitCausesStayInTheLog(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		err        error
	}{
		{"manual metric", "manual metric response exceeds -backend-max-buffered-response-bytes; narrow the query or increase it", newManualMetricResponseTooLarge(64)},
		{"backend body", "backend response exceeds 4096 bytes; narrow the query or increase -backend-max-buffered-response-bytes", newBackendBodyTooLarge(4096)},
		{"row limit", "manual range metric row limit exceeded (3); narrow the query or increase -manual-range-metric-row-limit", manualRowLimitError(3)},
		{"binary budget", "binary expression exceeds its evaluation budget; narrow the query or increase -binary-metric-max-arrays", newBinaryBudgetError(flagBinaryMetricMaxArrays)},
		{"label values", "raise -label-values-max-response-bytes", newLabelValuesTooLarge(2048, 1024)},
		{"heavy queue", "heavy VictoriaLogs queries are limited to -backend-max-concurrent-heavy-queries=4 per replica", newHeavyQueueFull(&heavyQueryQueueFullError{maxConcurrent: 4, queueWait: time.Second})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, logs := logProxy()
			rec := httptest.NewRecorder()
			p.fail(rec, requestOnRoute("query", "/x", context.Background()), tc.err)
			if !strings.Contains(logs.String(), tc.want) {
				t.Errorf("log %q lacks %q", logs.String(), tc.want)
			}
			if strings.Contains(rec.Body.String(), "-") && strings.Contains(rec.Body.String(), "flag") {
				t.Errorf("body %q", rec.Body.String())
			}
		})
	}
}
