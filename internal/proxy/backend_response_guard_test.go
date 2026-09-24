package proxy

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// wideLabelVL answers every field_values / stream_field_values call with n
// values and counts the calls.
func wideLabelVL(t *testing.T, n int, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	values := make([]fieldHit, n)
	for i := range values {
		values[i] = fieldHit{Value: fmt.Sprintf("pod-%06d", i), Hits: 1}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			writeVLFieldNames(w, []fieldHit{{"pod", int64(n)}})
		case "/select/logsql/field_values", "/select/logsql/stream_field_values":
			calls.Add(1)
			writeVLFieldValues(w, values)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newGuardTestProxy(t *testing.T, cfg Config) *Proxy {
	t.Helper()
	cfg.Cache = cache.New(60*time.Second, 1000)
	cfg.LogLevel = "error"
	cfg.LabelValuesIndexedCache = true
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func getLabelValues(p *Proxy, tenant string) *httptest.ResponseRecorder {
	now := time.Now()
	target := fmt.Sprintf("/loki/api/v1/label/pod/values?start=%d&end=%d", now.Add(-time.Hour).UnixNano(), now.UnixNano())
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("X-Scope-OrgID", tenant)
	rec := httptest.NewRecorder()
	p.handleLabelValues(rec, req)
	return rec
}

func lokiErrorText(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", bodyHead(rec), err)
	}
	if body.Status != "error" {
		t.Fatalf("status field = %q, want error", body.Status)
	}
	return body.Error
}

// bodyHead keeps failure messages short when a body holds thousands of values.
func bodyHead(rec *httptest.ResponseRecorder) string {
	if s := rec.Body.String(); len(s) > 300 {
		return s[:300] + "..."
	}
	return rec.Body.String()
}

func labelValuesIndexSize(p *Proxy) int {
	p.labelValuesIndexMu.Lock()
	defer p.labelValuesIndexMu.Unlock()
	return len(p.labelValuesIndex)
}

var resourceExhaustedRE = regexp.MustCompile(`^rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max \((\d+) vs\. (\d+)\); raise -label-values-max-response-bytes or narrow the query$`)

// A label values response above -label-values-max-response-bytes fails with
// Loki's querier error for a message above grpc_server_max_send_msg_size, the
// read stops one byte past the limit, and nothing is cached or indexed.
// conformance: operator-configurable-limits, limits/label-values-response-cap
func TestLabelValuesResponseCap_OverLimitFailsWithLokiResourceExhausted(t *testing.T) {
	var calls atomic.Int64
	vl := wideLabelVL(t, 2000, &calls)
	p := newGuardTestProxy(t, Config{BackendURL: vl.URL, ExecutionLimits: ExecutionLimitsConfig{LabelValuesMaxResponseBytes: 4096}})

	for attempt := 1; attempt <= 2; attempt++ {
		rec := getLabelValues(p, "0")
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("attempt %d: status = %d, want 500; body %s", attempt, rec.Code, bodyHead(rec))
		}
		msg := lokiErrorText(t, rec)
		m := resourceExhaustedRE.FindStringSubmatch(msg)
		if m == nil {
			t.Fatalf("attempt %d: error %q is not Loki's ResourceExhausted text naming the flag", attempt, msg)
		}
		if m[1] != "4097" || m[2] != "4096" {
			t.Fatalf("attempt %d: sizes (%s vs. %s), want (4097 vs. 4096)", attempt, m[1], m[2])
		}
	}
	// Not cached: the second request reached VictoriaLogs again.
	if got := calls.Load(); got != 2 {
		t.Fatalf("values calls = %d, want 2 (an over-limit answer must not be cached)", got)
	}
	if n := labelValuesIndexSize(p); n != 0 {
		t.Fatalf("label values index has %d entries, want 0 (an over-limit answer must not be indexed)", n)
	}
}

// conformance: operator-configurable-limits, limits/label-values-response-cap
func TestLabelValuesResponseCap_UnderLimitAnswersInFull(t *testing.T) {
	var calls atomic.Int64
	vl := wideLabelVL(t, 2000, &calls)
	p := newGuardTestProxy(t, Config{BackendURL: vl.URL})

	rec := getLabelValues(p, "0")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rec.Code, bodyHead(rec))
	}
	var body struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 2000 {
		t.Fatalf("values = %d, want all 2000", len(body.Data))
	}
}

// label_values_max_response_bytes in -tenant-limits overrides the flag for
// that tenant only; a multi-tenant request is held to the smallest value, and
// the key is enforced but not published (Loki has no such tenant limit).
// conformance: operator-configurable-limits, limits/label-values-response-cap
func TestLabelValuesResponseCap_PerTenantOverride(t *testing.T) {
	var calls atomic.Int64
	vl := wideLabelVL(t, 2000, &calls)
	p := newGuardTestProxy(t, Config{
		BackendURL:   vl.URL,
		TenantLimits: map[string]map[string]any{"small": {limitLabelValuesMaxResponseBytes: 4096}},
	})

	if rec := getLabelValues(p, "small"); rec.Code != http.StatusInternalServerError || !resourceExhaustedRE.MatchString(lokiErrorText(t, rec)) {
		t.Fatalf("tenant small: status %d body %s, want 500 ResourceExhausted", rec.Code, bodyHead(rec))
	}
	if rec := getLabelValues(p, "big"); rec.Code != http.StatusOK {
		t.Fatalf("tenant big: status %d body %s, want 200 under the flag default", rec.Code, bodyHead(rec))
	}
	if got := p.labelValuesMaxResponseBytes("big|small"); got != 4096 {
		t.Fatalf("multi-tenant limit = %d, want the smallest (4096)", got)
	}
	if got := p.labelValuesMaxResponseBytes("big"); got != DefaultLabelValuesMaxResponseBytes {
		t.Fatalf("default limit = %d, want %d", got, DefaultLabelValuesMaxResponseBytes)
	}
	if published := p.publishedTenantLimitsForOrgID("small"); published[limitLabelValuesMaxResponseBytes] != nil {
		t.Fatalf("published limits carry %s; it is not a Loki limit", limitLabelValuesMaxResponseBytes)
	}
	for _, bad := range []any{0, -1, "lots"} {
		err := validateTenantLimitOverrides(nil, map[string]map[string]any{"t": {limitLabelValuesMaxResponseBytes: bad}}, 0)
		if err == nil || !strings.Contains(err.Error(), limitLabelValuesMaxResponseBytes) {
			t.Fatalf("override %v: err = %v, want a startup error naming the key", bad, err)
		}
	}
}

// abortingVL imitates VictoriaLogs' lib/httpserver abort(): a 200 response
// starts, then the connection is hijacked, a raw (unchunked) abort line is
// written and the connection is closed. With gzip the partial body is
// compressed as VictoriaLogs does. deadlineText adds VictoriaLogs' own
// deadline error before the abort; abortAfter is how long the query "ran".
type abortingVL struct {
	gzip         bool
	deadlineText bool
	abortAfter   func(timeoutArg time.Duration) time.Duration
	calls        atomic.Int64
}

func (a *abortingVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			writeVLFieldNames(w, []fieldHit{{"pod", 1}})
			return
		case "/select/logsql/field_values", "/select/logsql/stream_field_values":
		default:
			w.WriteHeader(http.StatusOK)
			return
		}
		a.calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		var out io.Writer = w
		var zw *gzip.Writer
		if a.gzip {
			w.Header().Set("Content-Encoding", "gzip")
			zw = gzip.NewWriter(w)
			out = zw
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(out, `{"values":[{"value":"pod-1","hits":1},`)
		if zw != nil {
			_ = zw.Flush()
		}
		w.(http.Flusher).Flush()
		if a.abortAfter != nil {
			timeoutArg, _ := time.ParseDuration(r.URL.Query().Get("timeout"))
			time.Sleep(a.abortAfter(timeoutArg))
		}
		if a.deadlineText {
			_, _ = fmt.Fprintf(out, "\n%s\n", "the request couldn't be executed in 60.012 seconds; possible solutions: to increase -search.maxQueryDuration=1m0s; to pass bigger value to 'timeout' query arg")
			w.(http.Flusher).Flush()
		}
		conn, bw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_, _ = bw.WriteString("\nthe connection has been aborted; see the last line in the response and/or in the server log for the reason\n")
		_ = bw.Flush()
		_ = conn.Close()
	}))
	t.Cleanup(srv.Close)
	return srv
}

var goTransportNoise = []string{"bare LF", "unexpected EOF", "flate", "gzip", "chunked", "malformed"}

// A VictoriaLogs body that aborts after its 200 headers is never answered with
// Go's transport error: 504 with Loki's timeout text when the query ran out of
// the budget sent to VictoriaLogs, a sanitized 502 otherwise. Nothing partial
// is cached or indexed.
// conformance: backend-deadlines-and-cancellation, semantics/backend-aborted-response
func TestVLResponseAbortedAfterHeaders(t *testing.T) {
	cases := []struct {
		name       string
		vl         *abortingVL
		timeout    time.Duration
		wantStatus int
	}{
		{"early abort, identity", &abortingVL{}, 0, http.StatusBadGateway},
		{"early abort, gzip", &abortingVL{gzip: true}, 0, http.StatusBadGateway},
		{"VictoriaLogs deadline text, identity", &abortingVL{deadlineText: true}, 0, http.StatusGatewayTimeout},
		{"abort at the timeout argument, identity", &abortingVL{abortAfter: func(d time.Duration) time.Duration { return d * 97 / 100 }}, 400 * time.Millisecond, http.StatusGatewayTimeout},
		{"abort at the timeout argument, gzip", &abortingVL{gzip: true, deadlineText: true, abortAfter: func(d time.Duration) time.Duration { return d * 97 / 100 }}, 400 * time.Millisecond, http.StatusGatewayTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.vl.server(t)
			// -backend-compression=gzip so the proxy accepts the gzip body even
			// though the fake backend is on loopback.
			p := newGuardTestProxy(t, Config{BackendURL: srv.URL, BackendTimeout: tc.timeout, BackendCompression: "gzip"})
			for attempt := 1; attempt <= 2; attempt++ {
				rec := getLabelValues(p, "0")
				if rec.Code != tc.wantStatus {
					t.Fatalf("attempt %d: status = %d, want %d; body %s", attempt, rec.Code, tc.wantStatus, bodyHead(rec))
				}
				msg := lokiErrorText(t, rec)
				if tc.wantStatus == http.StatusGatewayTimeout && msg != lokiErrDeadlineExceeded {
					t.Fatalf("attempt %d: error %q, want Loki's %q", attempt, msg, lokiErrDeadlineExceeded)
				}
				if tc.wantStatus == http.StatusBadGateway && !strings.HasPrefix(msg, "VictoriaLogs aborted the response after ") {
					t.Fatalf("attempt %d: error %q, want the sanitized abort message", attempt, msg)
				}
				for _, noise := range goTransportNoise {
					if strings.Contains(msg, noise) {
						t.Fatalf("attempt %d: error %q leaks transport detail %q", attempt, msg, noise)
					}
				}
			}
			if got := tc.vl.calls.Load(); got != 2 {
				t.Fatalf("values calls = %d, want 2 (a partial body must not be cached)", got)
			}
			if n := labelValuesIndexSize(p); n != 0 {
				t.Fatalf("label values index has %d entries, want 0", n)
			}
		})
	}
}

// A client that disconnects mid-body keeps its cancellation (499), not an
// abort error.
// conformance: backend-deadlines-and-cancellation, semantics/backend-aborted-response
func TestBackendBodyGuard_ClientCancelIsNotAnAbort(t *testing.T) {
	p := newGuardTestProxy(t, Config{BackendURL: "http://127.0.0.1:1"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := p.guardBackendBody(ctx, io.NopCloser(errReader{context.Canceled}), "/select/logsql/field_values", url.Values{}, time.Now())
	_, err := io.ReadAll(g)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := statusFromUpstreamErr(err); got != 499 {
		t.Fatalf("status = %d, want 499", got)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// conformance: semantics/backend-aborted-response, limits/label-values-response-cap
func TestStatusFromUpstreamErr_GuardErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
		text string
	}{
		{"aborted past its deadline", &vlResponseAbortedError{elapsed: time.Minute, timedOut: true}, http.StatusGatewayTimeout, lokiErrDeadlineExceeded},
		{"aborted early", &vlResponseAbortedError{elapsed: 1500 * time.Millisecond}, http.StatusBadGateway, "VictoriaLogs aborted the response after 1.5s, before it was complete; it ends a response this way when a query exceeds -search.maxQueryDuration or fails while its result is being written"},
		{"wrapped abort", fmt.Errorf("fetch: %w", &vlResponseAbortedError{timedOut: true}), http.StatusGatewayTimeout, "fetch: " + lokiErrDeadlineExceeded},
		{"label values too large", &labelValuesResponseTooLargeError{read: 67108865, limit: 67108864}, http.StatusInternalServerError, "rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (67108865 vs. 67108864); raise -label-values-max-response-bytes or narrow the query"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := statusFromUpstreamErr(tc.err); got != tc.want {
				t.Fatalf("status = %d, want %d", got, tc.want)
			}
			if got := tc.err.Error(); got != tc.text {
				t.Fatalf("text = %q, want %q", got, tc.text)
			}
		})
	}
}
