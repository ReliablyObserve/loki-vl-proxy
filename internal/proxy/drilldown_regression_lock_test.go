package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// recorderBackend records every request the proxy sends to VL and returns a
// response provided by the per-path handler map. Unknown paths return a
// success-shaped empty Loki matrix.
type recorderBackend struct {
	mu       sync.Mutex
	calls    map[string]int      // path -> count
	queries  map[string][]string // path -> received query strings
	steps    map[string][]string // path -> received step strings
	handlers map[string]func(http.ResponseWriter, *http.Request)
}

func newRecorderBackend() *recorderBackend {
	return &recorderBackend{
		calls:    map[string]int{},
		queries:  map[string][]string{},
		steps:    map[string][]string{},
		handlers: map[string]func(http.ResponseWriter, *http.Request){},
	}
}

func (b *recorderBackend) on(path string, h func(http.ResponseWriter, *http.Request)) {
	b.handlers[path] = h
}

func (b *recorderBackend) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		b.mu.Lock()
		b.calls[r.URL.Path]++
		b.queries[r.URL.Path] = append(b.queries[r.URL.Path], r.Form.Get("query"))
		b.steps[r.URL.Path] = append(b.steps[r.URL.Path], r.Form.Get("step"))
		h := b.handlers[r.URL.Path]
		b.mu.Unlock()
		if h != nil {
			h(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
}

// drilldownRequest builds an http.Request shaped like a Grafana stats query.
// source=="" leaves X-Query-Tags unset (Explore / direct API).
// source=="drilldown" sets X-Query-Tags: Source=grafana-lokiexplore-app.
// source=="grafana-ua" sets User-Agent: Grafana/11.5.0 (dashboard panels).
// source=="grafana-hdr" sets X-Grafana-Org-Id (backend-routed Explore).
func drilldownRequest(t *testing.T, query string, startSec, endSec int64, step, source string) *http.Request {
	t.Helper()
	form := url.Values{}
	form.Set("query", query)
	form.Set("start", strconv.FormatInt(startSec, 10))
	form.Set("end", strconv.FormatInt(endSec, 10))
	form.Set("step", step)
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	r = r.WithContext(context.WithValue(r.Context(), orgIDKey, "default"))
	r.Header.Set("X-Scope-OrgID", "default")
	switch source {
	case "drilldown":
		r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	case "grafana-ua":
		r.Header.Set("User-Agent", "Grafana/11.5.0")
	case "grafana-hdr":
		r.Header.Set("X-Grafana-Org-Id", "1")
	}
	return r
}

// TestLock_LeftoverChunkSuppressedInHits pins the rule:
//
//	end - start < step AND Drilldown source signal → empty matrix
//
// When Grafana loads a long range in pieces, the last piece Logs Drilldown asks
// for can be shorter than one step: its answer is a one-bucket frame, and the
// merged chart showed it as a spike on one edge (observed in Logs Drilldown at
// 24h and longer). Returning an empty matrix for that piece leaves the merged
// chart without the spike, and the chart loses at most one step on its edge. The response header keeps its
// historical value (X-Proxy-Drilldown-Path: hits-leftover-suppressed).
//
// Plain Grafana, non-Grafana callers and any range >= one full step are served
// normally.
func TestLock_LeftoverChunkSuppressedInHits(t *testing.T) {
	cases := []struct {
		name         string
		startSec     int64
		endSec       int64
		stepRaw      string
		source       string
		wantSuppress bool
	}{
		// Grafana, sub-step residual (range < step) → SUPPRESS.
		{"drilldown_60s_step120", 1700000000, 1700000060, "120", "drilldown", true},
		{"drilldown_119s_step120", 1700000000, 1700000119, "120", "drilldown", true},
		{"grafana_ua_60s", 1700000000, 1700000060, "120", "grafana-ua", false},
		{"grafana_hdr_60s", 1700000000, 1700000060, "120", "grafana-hdr", false},
		// Grafana, range >= step (legitimate >=1-bucket query) → do NOT suppress.
		{"drilldown_120s_step120", 1700000000, 1700000120, "120", "drilldown", false},
		{"drilldown_240s_step120", 1700000000, 1700000240, "120", "drilldown", false},
		{"drilldown_1h_step120", 1700000000, 1700003600, "120", "drilldown", false},
		// Non-Grafana caller with sub-step range → do NOT suppress.
		{"raw_caller_60s_step120", 1700000000, 1700000060, "120", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := newRecorderBackend()
			served := func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[1700000000,"3"]]}]}}`))
			}
			backend.on("/select/logsql/stats_query_range", served)
			vl := backend.server()
			defer vl.Close()
			p := newTestProxy(t, vl.URL)

			r := drilldownRequest(t,
				`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
				tc.startSec, tc.endSec, tc.stepRaw, tc.source)
			w := httptest.NewRecorder()
			p.proxyStatsQueryRange(w, r,
				statsQueryOf(`namespace:="prod" | filter pod:!"" | stats by (pod) count()`))

			gotSuppressed := w.Header().Get("X-Proxy-Drilldown-Path") == "hits-leftover-suppressed"
			if gotSuppressed != tc.wantSuppress {
				t.Errorf("%s: suppressed=%v, want %v (body=%s)", tc.name, gotSuppressed, tc.wantSuppress, w.Body.String())
			}
			if tc.wantSuppress && !strings.Contains(w.Body.String(), `"result":[]`) {
				t.Errorf("%s: suppressed but body is not an empty matrix: %s", tc.name, w.Body.String())
			}
		})
	}
}

// TestLock_IsGrafanaSourcedRequest pins which client signals count as
// "Grafana source" for Grafana-only compatibility behavior such as
// partial-results conversion and high-cardinality optimization.
func TestLock_IsGrafanaSourcedRequest(t *testing.T) {
	cases := []struct {
		name string
		set  func(*http.Request)
		want bool
	}{
		{"drilldown_tag", func(r *http.Request) {
			r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		}, true},
		{"explore_tag", func(r *http.Request) {
			r.Header.Set("X-Query-Tags", "Source=lokiexplore")
		}, true},
		{"user_agent_grafana", func(r *http.Request) {
			r.Header.Set("User-Agent", "Grafana/11.5.0")
		}, true},
		{"x_grafana_org_id", func(r *http.Request) {
			r.Header.Set("X-Grafana-Org-Id", "1")
		}, true},
		{"x_grafana_request_id", func(r *http.Request) {
			r.Header.Set("X-Grafana-Request-Id", "abc")
		}, true},
		{"raw_curl", func(r *http.Request) {}, false},
		{"random_user_agent", func(r *http.Request) {
			r.Header.Set("User-Agent", "curl/8.4.0")
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
			tc.set(r)
			if got := isGrafanaSourcedRequest(r); got != tc.want {
				t.Errorf("isGrafanaSourcedRequest(%s) = %v, want %v",
					tc.name, got, tc.want)
			}
		})
	}
}

// Backend failures remain sanitized Loki errors for Grafana and direct API
// clients alike. Loki's Drilldown max_query_series partial-result exception
// does not turn resource exhaustion, an outage, a timeout or a full queue into
// an empty successful matrix. Actual series-limit partials are tested apart.
func TestLock_VLErrorsRemainErrorsForGrafana(t *testing.T) {
	cases := []struct {
		name           string
		vlStatus       int
		vlBody         string
		grafanaSourced bool
		expectStatus   int
		expectText     string
	}{
		{
			name: "Grafana_VL_502_parserpipe_overflow", vlStatus: http.StatusBadGateway,
			vlBody: `{"error":"too many rows scanned by | stats by (trace_id)"}`, grafanaSourced: true,
			expectStatus: http.StatusInternalServerError, expectText: lokiErrInternal,
		},
		{
			name: "Grafana_VL_503_queue_saturated", vlStatus: http.StatusServiceUnavailable,
			vlBody: `{"error":"too many concurrent queries"}`, grafanaSourced: true,
			expectStatus: http.StatusInternalServerError, expectText: lokiErrInternal,
		},
		{
			name: "Grafana_VL_500_internal", vlStatus: http.StatusInternalServerError,
			vlBody: `{"error":"out of memory"}`, grafanaSourced: true,
			expectStatus: http.StatusInternalServerError, expectText: lokiErrInternal,
		},
		{
			name: "Grafana_VL_429_queue_full", vlStatus: http.StatusTooManyRequests,
			vlBody: `{"error":"private backend queue full"}`, grafanaSourced: true,
			expectStatus: http.StatusTooManyRequests, expectText: lokiTooManyOutstandingRequests,
		},
		{
			name: "Grafana_VL_504_timeout", vlStatus: http.StatusGatewayTimeout,
			vlBody: `{"error":"private query execution timeout"}`, grafanaSourced: true,
			expectStatus: http.StatusGatewayTimeout, expectText: lokiErrDeadlineExceeded,
		},
		{
			name: "NonGrafana_VL_502", vlStatus: http.StatusBadGateway,
			vlBody:       `{"error":"too many rows scanned"}`,
			expectStatus: http.StatusInternalServerError, expectText: lokiErrInternal,
		},
		{
			name: "NonGrafana_VL_500", vlStatus: http.StatusInternalServerError,
			vlBody:       `{"error":"oom"}`,
			expectStatus: http.StatusInternalServerError, expectText: lokiErrInternal,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.vlStatus)
				_, _ = w.Write([]byte(tc.vlBody))
			}))
			defer vlSrv.Close()

			p := newTestProxy(t, vlSrv.URL)

			req := httptest.NewRequest("POST", "/loki/api/v1/query_range",
				strings.NewReader(`query=sum(count_over_time({app="x"}[1m]))&start=1700000000000000000&end=1700001000000000000&step=60s`))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if tc.grafanaSourced {
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			}
			req.Header.Set("X-Scope-OrgID", "test-"+tc.name)
			_ = req.ParseForm()

			w := httptest.NewRecorder()
			p.proxyStatsQueryRangeDirect(w, req, `app:="x" | stats count() as v`)

			if w.Code != tc.expectStatus {
				t.Errorf("status: got %d, want %d (body=%q)", w.Code, tc.expectStatus, w.Body.String())
			}
			if w.Header().Get("Warning") != "" || w.Header().Get(drilldownPartialUpstreamStatusHeader) != "" || w.Header().Get("X-Proxy-Upstream-Error") != "" {
				t.Errorf("backend details or fabricated partial headers: %v", w.Header())
			}
			if envelope := readErrorRec(t, w); envelope.Error != tc.expectText {
				t.Errorf("expected sanitized Loki failure, got: %s", w.Body.String())
			}
		})
	}
}
