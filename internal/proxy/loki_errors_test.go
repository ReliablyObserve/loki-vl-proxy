package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// Loki error response format — must match exactly what Grafana expects
// =============================================================================

func TestLokiErrorType_Mapping(t *testing.T) {
	tests := []struct {
		code     int
		wantType string
	}{
		{400, "bad_data"},
		{422, "execution"},
		{429, "bad_data"}, // 429 is not a standard Loki errorType; falls through to client error default
		{499, "canceled"},
		{500, "internal"},
		{502, "unavailable"},
		{503, "timeout"},
		{504, "timeout"},
		{404, "not_found"},
		{501, "internal"}, // server errors default to internal
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.code), func(t *testing.T) {
			got := lokiErrorType(tt.code)
			if got != tt.wantType {
				t.Errorf("lokiErrorType(%d) = %q, want %q", tt.code, got, tt.wantType)
			}
		})
	}
}

func TestLokiError_ResponseFormat(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// Send a request with a query that has an unmatched brace — triggers parse error
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={unclosed&start=1&end=2&step=1`, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
	body := w.Body.String()
	// Loki returns plain text "parse error ..." for syntax errors.
	// Accept either plain text parse error or JSON error response.
	if !strings.Contains(body, "parse error") && !strings.Contains(body, "bad_data") {
		t.Errorf("expected parse error or bad_data in response, got %q", body)
	}
}

func TestLokiError_BackendDown_ReturnsUnavailable(t *testing.T) {
	// Use a backend URL that will fail
	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: "http://unused", Cache: c, LogLevel: "error"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse: %v\nbody: %s", err, w.Body.String())
	}
	if resp.ErrorType != "unavailable" {
		t.Errorf("backend down should return errorType=unavailable, got %q", resp.ErrorType)
	}
}

// =============================================================================
// VL error body rewriting — VL uses "unavailable" for all server errors;
// the proxy must translate to Loki-compliant errorType strings.
// =============================================================================

// TestVLError_OOMBodyIsRewritten verifies that when VL returns a memory-error
// JSON body with "errorType":"unavailable", the proxy rewrites it to a
// Loki-compliant error response (e.g. "internal" for 500) and extracts only
// the "error" field value as the error message. Without this fix, Grafana
// receives VL's raw JSON as the "error" field value (double-encoded).
func TestVLError_OOMBodyIsRewritten(t *testing.T) {
	vlBody := `{"error":"cannot execute query since it requires more than 819MB of memory","errorType":"unavailable","status":"error"}`
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(vlBody))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	params := url.Values{}
	params.Set("query", `{app="nginx"}`)
	params.Set("start", "1700000000")
	params.Set("end", "1700086400") // 24h range
	params.Set("step", "60")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
	mux.ServeHTTP(w, r)

	var resp struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse error response: %v\nbody: %s", err, w.Body.String())
	}
	if resp.Status != "error" {
		t.Errorf("expected status=error, got %q", resp.Status)
	}
	// The error field must be the VL message string, NOT VL's raw JSON body.
	if strings.Contains(resp.Error, `"errorType"`) || strings.Contains(resp.Error, `"status"`) {
		t.Errorf("error field must not contain VL JSON envelope fields; got %q", resp.Error)
	}
	if !strings.Contains(resp.Error, "819MB") && !strings.Contains(resp.Error, "memory") {
		t.Errorf("error field should contain the VL error message; got %q", resp.Error)
	}
	// errorType must be a valid Loki errorType string (not "unavailable" from VL for 500).
	validTypes := map[string]bool{"internal": true, "execution": true, "bad_data": true, "timeout": true, "not_found": true, "canceled": true, "unavailable": true}
	if !validTypes[resp.ErrorType] {
		t.Errorf("errorType %q is not a valid Loki errorType", resp.ErrorType)
	}
	// For a 500, should NOT be "unavailable" (that's Loki's 502 errorType).
	if resp.ErrorType == "unavailable" {
		t.Errorf("500 from VL should map to 'internal' not 'unavailable'; VL's raw errorType leaked through")
	}
}

// TestExtractVLErrorMsg verifies that the helper correctly extracts the "error"
// field value from VL JSON error bodies and falls back to raw string when needed.
func TestExtractVLErrorMsg(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			`{"error":"cannot execute query since it requires more than 819MB of memory","errorType":"unavailable","status":"error"}`,
			"cannot execute query since it requires more than 819MB of memory",
		},
		{
			`{"error":"query too complex","status":"error"}`,
			"query too complex",
		},
		{
			`plain text error`,
			"plain text error",
		},
		{
			``,
			"",
		},
		{
			`{"status":"error"}`, // no "error" field
			`{"status":"error"}`,
		},
	}
	for _, tt := range tests {
		got := extractVLErrorMsg([]byte(tt.input))
		if got != tt.want {
			t.Errorf("extractVLErrorMsg(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// =============================================================================
// Long-range sums with unused JSON parsing retain native stats_query_range
// =============================================================================

// A label-free sum may skip JSON under Loki's parser hints. Keep wide queries
// on native stats when this optimization is semantically valid.
func TestLongRange_SummedCountWithUnusedJSONUsesStats(t *testing.T) {
	var statsQueryCalled, logQueryCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			statsQueryCalled = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case "/select/logsql/query":
			logQueryCalled = true
			if r.FormValue("limit") == "1000000" || r.FormValue("limit") == "1000001" || strings.HasSuffix(r.FormValue("query"), " | limit 1000001") {
				t.Error("1M-limit raw log fetch must not be used for long-range count_over_time with range==step; use stats_query_range")
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
		default:
			fmt.Fprintf(w, `{}`)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	// Simulate Grafana sending $__auto range (step=15m) for a 24h window.
	base := time.Unix(1699999200, 0) // epoch-aligned: no known VL version, so no offset arg
	step := 15 * 60                  // 15m step
	params := url.Values{}
	params.Set("query", `sum(count_over_time({service_name="api-gateway"} | json [15m]))`) // range=step=15m
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(24*time.Hour).Unix(), 10))
	params.Set("step", strconv.Itoa(step))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected response: %d %s", w.Code, w.Body)
	}

	if logQueryCalled {
		t.Error("1M-limit log query was called — this causes OOM for long-range queries; stats_query_range should be used")
	}
	if !statsQueryCalled {
		t.Error("stats_query_range was not called — long-range count_over_time must route to stats, not raw log fetch")
	}
}

// Summed byte queries with unused JSON must retain native stats aggregation
// for both tumbling and sliding windows.
func TestLongRange_SummedBytesWithUnusedJSONUsesStats(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rangeW   string
		step     int
		tumbling bool
	}{
		{"tumbling_window_range_eq_step", "15m", 900, true},
		{"sliding_window_range_gt_step", "15m", 60, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var statsQueryCalled bool
			vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/select/logsql/stats_query_range":
					statsQueryCalled = true
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
				case "/select/logsql/query":
					if r.FormValue("limit") == "1000000" || r.FormValue("limit") == "1000001" || strings.HasSuffix(r.FormValue("query"), " | limit 1000001") {
						t.Errorf("1M-limit log fetch must not be used for bytes_over_time; use stats_query_range (%s)", tc.name)
					}
					w.Header().Set("Content-Type", "application/x-ndjson")
				default:
					fmt.Fprintf(w, `{}`)
				}
			}))
			defer vlBackend.Close()

			c := cache.New(60*time.Second, 10000)
			p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
			p.storeBackendVersion("v1.50.0", "v1.50.0") // anchored stats buckets need offset support (v1.45+)
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			base := time.Unix(1700000000, 0)
			params := url.Values{}
			params.Set("query", fmt.Sprintf(`sum(bytes_over_time({service_name="api-gateway"} | json [%s]))`, tc.rangeW))
			params.Set("start", strconv.FormatInt(base.Unix(), 10))
			params.Set("end", strconv.FormatInt(base.Add(24*time.Hour).Unix(), 10))
			params.Set("step", strconv.Itoa(tc.step))
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("unexpected response: %d %s", w.Code, w.Body)
			}

			if !statsQueryCalled {
				t.Errorf("stats_query_range was not called for bytes_over_time (%s) — long-range query would OOM with raw log fetch", tc.name)
			}
		})
	}
}

// Loki (pkg/loghttp.ParseRangeQuery) rejects (end-start)/step > 11000, using
// integer duration division, before any query evaluation. Log and metric
// queries are treated alike, and no backend call is made for a rejected range.
func TestLokiError_QueryRangeResolutionLimit(t *testing.T) {
	var backendCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/select/") {
			backendCalls.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer vlBackend.Close()
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: cache.New(0, 0), LogLevel: "error"})
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	const start = int64(1700000000)
	at := func(offset int64) string { return strconv.FormatInt(start+offset, 10) }
	metric := `sum(count_over_time({app="x"}[1m]))`
	for _, tc := range []struct {
		name, query string
		params      map[string]string // start, end, step, since; omitted keys are not sent
		reject      bool
	}{
		{name: "just under", query: metric, params: map[string]string{"start": at(0), "end": at(10999), "step": "1"}},
		{name: "at the limit", query: metric, params: map[string]string{"start": at(0), "end": at(11000), "step": "1"}},
		{name: "fraction above the limit truncates", query: `{app="x"}`, params: map[string]string{"start": at(0), "end": at(11000) + ".5", "step": "1"}},
		{name: "over the limit metric", query: metric, params: map[string]string{"start": at(0), "end": at(11001), "step": "1"}, reject: true},
		{name: "over the limit log query", query: `{app="x"}`, params: map[string]string{"start": at(0), "end": at(11001), "step": "1s"}, reject: true},
		{name: "30d at 60s", query: `sum by (pod) (rate({app="x"}[1h]))`, params: map[string]string{"start": at(0), "end": at(30 * 86400), "step": "60"}, reject: true},
		{name: "missing step uses Loki default", query: metric, params: map[string]string{"start": at(0), "end": at(30 * 86400)}},
		// Loki defaults: end=now, start=min(end, now)-since, since=1h.
		{name: "missing start and end over 1h default since", query: metric, params: map[string]string{"step": "0.3"}, reject: true},
		{name: "missing start and end under 1h default since", query: metric, params: map[string]string{"step": "0.33"}},
		{name: "missing start with past end", query: metric, params: map[string]string{"end": at(0), "step": "0.3"}, reject: true},
		{name: "since over the limit", query: metric, params: map[string]string{"since": "10m", "step": "0.05"}, reject: true},
		{name: "since under the limit", query: metric, params: map[string]string{"since": "3h", "step": "1"}},
		// Inverted ranges and non-positive steps are reported by the existing handlers.
		{name: "inverted range", query: metric, params: map[string]string{"start": at(20000), "end": at(0), "step": "0.001"}},
		{name: "zero step", query: metric, params: map[string]string{"start": at(0), "end": at(30 * 86400), "step": "0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backendCalls.Store(0)
			params := url.Values{"query": {tc.query}}
			for k, v := range tc.params {
				params.Set(k, v)
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			var resp struct {
				Status    string `json:"status"`
				ErrorType string `json:"errorType"`
				Error     string `json:"error"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			rejected := w.Code == http.StatusBadRequest && resp.Error == errLokiStepTooSmall
			if rejected != tc.reject {
				t.Fatalf("rejected=%v, want %v: %d %s", rejected, tc.reject, w.Code, w.Body.String())
			}
			if tc.reject && (resp.Status != "error" || resp.ErrorType != "bad_data" || backendCalls.Load() != 0) {
				t.Fatalf("expected a Loki bad_data error without backend calls, got %+v and %d backend calls", resp, backendCalls.Load())
			}
		})
	}
}

func TestLokiError_BadQuery_ReturnsBadData(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={unclosed&start=1&end=2&step=1`, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("bad query should return 400, got %d", w.Code)
	}
	body := w.Body.String()
	// Loki returns plain text parse errors for syntax issues.
	// Accept either plain text "parse error" or JSON errorType=bad_data.
	var resp struct {
		ErrorType string `json:"errorType"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.ErrorType != "bad_data" && !strings.Contains(body, "parse error") {
		t.Errorf("bad query should return errorType=bad_data or parse error, got %q", body)
	}
}
