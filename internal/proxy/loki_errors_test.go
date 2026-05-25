package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
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
// Long-range metric query routing — must use stats_query_range, never 1M fetch
// =============================================================================

// TestLongRange_CountOverTimeUsesStats verifies that count_over_time with a
// parser stage and range==step routes to VL stats_query_range, not the 1M-limit
// raw log fetch. Before the fix, long-range queries (e.g. 24h) exhausted memory.
func TestLongRange_CountOverTimeUsesStats(t *testing.T) {
	var statsQueryCalled, logQueryCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			statsQueryCalled = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case "/select/logsql/query":
			logQueryCalled = true
			if r.FormValue("limit") == "1000000" {
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
	base := time.Unix(1700000000, 0)
	step := 15 * 60 // 15m step
	params := url.Values{}
	params.Set("query", `count_over_time({service_name="api-gateway"} | json [15m])`) // range=step=15m
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(24*time.Hour).Unix(), 10))
	params.Set("step", strconv.Itoa(step))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
	mux.ServeHTTP(w, r)

	if logQueryCalled {
		t.Error("1M-limit log query was called — this causes OOM for long-range queries; stats_query_range should be used")
	}
	if !statsQueryCalled {
		t.Error("stats_query_range was not called — long-range count_over_time must route to stats, not raw log fetch")
	}
}

// TestLongRange_BytesOverTimeUsesStats verifies bytes_over_time routes to stats
// for both tumbling-window (range==step) and sliding-window (range>step) cases.
func TestLongRange_BytesOverTimeUsesStats(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rangeW  string
		step    int
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
					if r.FormValue("limit") == "1000000" {
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
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)

			base := time.Unix(1700000000, 0)
			params := url.Values{}
			params.Set("query", fmt.Sprintf(`bytes_over_time({service_name="api-gateway"} | json [%s])`, tc.rangeW))
			params.Set("start", strconv.FormatInt(base.Unix(), 10))
			params.Set("end", strconv.FormatInt(base.Add(24*time.Hour).Unix(), 10))
			params.Set("step", strconv.Itoa(tc.step))
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
			mux.ServeHTTP(w, r)

			if !statsQueryCalled {
				t.Errorf("stats_query_range was not called for bytes_over_time (%s) — long-range query would OOM with raw log fetch", tc.name)
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
