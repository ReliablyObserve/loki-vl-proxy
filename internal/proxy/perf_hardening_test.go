package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleDetectedFieldsAndValuesReuseCachedScan(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"service.name","hits":1}]}`)
		case "/select/logsql/query":
			backendCalls++
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"{\"method\":\"GET\"}","service.name":"api"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)

	r1 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_fields?query={service_name="api"}&start=1&end=2&limit=25`, nil)
	w1 := httptest.NewRecorder()
	p.handleDetectedFields(w1, r1)
	if w1.Code != http.StatusOK {
		t.Fatalf("detected_fields code=%d body=%s", w1.Code, w1.Body.String())
	}

	r2 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_field/method/values?query={service_name="api"}&start=1&end=2&limit=25`, nil)
	w2 := httptest.NewRecorder()
	p.handleDetectedFieldValues(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("detected_field_values code=%d body=%s", w2.Code, w2.Body.String())
	}

	if backendCalls != 1 {
		t.Fatalf("expected one backend scan reused through cache, got %d calls", backendCalls)
	}
}

func TestHandleDetectedLabelsReuseCachedScan(t *testing.T) {
	var streamCalls int
	var scanCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			streamCalls++
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"{cluster=\"eu\",service.name=\"api\"}","hits":1}]}`)
		case "/select/logsql/query":
			scanCalls++
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"ok","_stream":"{cluster=\"eu\",service.name=\"api\"}","level":"info"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)

	r1 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_labels?query={service_name="api"}&start=1&end=2&limit=25`, nil)
	w1 := httptest.NewRecorder()
	p.handleDetectedLabels(w1, r1)
	if w1.Code != http.StatusOK {
		t.Fatalf("detected_labels code=%d body=%s", w1.Code, w1.Body.String())
	}

	r2 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_labels?query={service_name="api"}&start=1&end=2&limit=25`, nil)
	w2 := httptest.NewRecorder()
	p.handleDetectedLabels(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("detected_labels cached code=%d body=%s", w2.Code, w2.Body.String())
	}

	if streamCalls != 1 || scanCalls != 1 {
		t.Fatalf("expected detected_labels cache reuse after one native call and one scan supplement, got streamCalls=%d scanCalls=%d", streamCalls, scanCalls)
	}
}

func TestHandleDetectedLabels_BackendErrorReturnsEmptySuccess(t *testing.T) {
	backend := httptest.NewServer(http.NotFoundHandler())
	backendURL := backend.URL
	backend.Close()

	p := newTestProxy(t, backendURL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_labels?query={service_name="api"}&limit=17`, nil)

	p.handleDetectedLabels(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Status         string        `json:"status"`
		Data           []interface{} `json:"data"`
		DetectedLabels []interface{} `json:"detectedLabels"`
		Limit          int           `json:"limit"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "success" || len(resp.Data) != 0 || len(resp.DetectedLabels) != 0 || resp.Limit != 17 {
		t.Fatalf("expected empty detected_labels success response, got %#v", resp)
	}
}

func TestHandleDetectedFieldValues_LevelFallsBackToDetectedLevel(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[]}`)
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"level=error","detected_level":"error"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_field/level/values?query={app="api"}&limit=9`, nil)

	p.handleDetectedFieldValues(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Values []string `json:"values"`
		Limit  int      `json:"limit"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Values) != 1 || resp.Values[0] != "error" || resp.Limit != 9 {
		t.Fatalf("expected detected_level fallback, got %#v", resp)
	}
}

func TestHandlePatternsReuseCachedResponse(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		backendCalls++
		w.Header().Set("Content-Type", "application/x-ndjson")
		fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"GET /api/users 200 15ms"}`)
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)

	r1 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&step=1m&limit=10`, nil)
	w1 := httptest.NewRecorder()
	p.handlePatterns(w1, r1)
	if w1.Code != http.StatusOK {
		t.Fatalf("patterns code=%d body=%s", w1.Code, w1.Body.String())
	}

	r2 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&step=1m&limit=10`, nil)
	w2 := httptest.NewRecorder()
	p.handlePatterns(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("patterns cached code=%d body=%s", w2.Code, w2.Body.String())
	}

	if backendCalls != 1 {
		t.Fatalf("expected patterns cache reuse, got %d backend calls", backendCalls)
	}
}

func TestHandleMultiTenantFanoutRejectsExcessiveTenantCount(t *testing.T) {
	p := newTestProxy(t, "http://example.com")
	tenants := make([]string, 0, maxMultiTenantFanout+1)
	for i := 0; i < maxMultiTenantFanout+1; i++ {
		tenants = append(tenants, fmt.Sprintf("tenant-%02d", i))
	}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", strings.Join(tenants, "|"))
	w := httptest.NewRecorder()

	if !p.handleMultiTenantFanout(w, r, "labels", p.handleLabels) {
		t.Fatal("expected multi-tenant fanout path to handle oversized tenant set")
	}
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized fanout, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestSyntheticTailSeenBoundsMemory(t *testing.T) {
	seen := newSyntheticTailSeen(4)
	for i := 0; i < 6; i++ {
		seen.Add(fmt.Sprintf("k-%d", i))
	}
	if seen.Contains("k-0") || seen.Contains("k-1") {
		t.Fatal("expected oldest entries to be evicted from synthetic tail dedup set")
	}
	for _, key := range []string{"k-2", "k-3", "k-4", "k-5"} {
		if !seen.Contains(key) {
			t.Fatalf("expected key %s to remain in bounded dedup set", key)
		}
	}
}

func TestHandlePatterns_InvalidQueryReturnsEmptySuccess(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		t.Fatalf("backend should not be called for invalid query")
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"&step=1m`, nil)

	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if backendCalls != 0 {
		t.Fatalf("expected no backend calls, got %d", backendCalls)
	}
	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "success" || len(resp.Data) != 0 {
		t.Fatalf("expected empty success response, got %#v", resp)
	}
}

func TestHandlePatterns_BackendFailureReturnsEmptySuccess(t *testing.T) {
	backend := httptest.NewServer(http.NotFoundHandler())
	backendURL := backend.URL
	backend.Close()

	p := newTestProxy(t, backendURL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&step=1m`, nil)

	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "success" || len(resp.Data) != 0 {
		t.Fatalf("expected empty success response, got %#v", resp)
	}
}

func TestBufferedResponseWriterInitializesHeaderAndCapturesBody(t *testing.T) {
	bw := &bufferedResponseWriter{}
	bw.Header().Set("Content-Type", "application/json")
	if _, err := bw.Write([]byte(`{"status":"success"}`)); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if got := bw.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected content type to be preserved, got %q", got)
	}
	if got := string(bw.body); got != `{"status":"success"}` {
		t.Fatalf("unexpected buffered body %q", got)
	}
}

func TestParseDetectedLineLimit(t *testing.T) {
	cases := []struct {
		name string
		url  string
		want int
	}{
		{name: "default", url: "/loki/api/v1/detected_fields", want: 1000},
		{name: "line_limit", url: "/loki/api/v1/detected_fields?line_limit=25", want: 25},
		{name: "limit_overrides", url: "/loki/api/v1/detected_fields?line_limit=25&limit=11", want: 11},
		{name: "invalid_values", url: "/loki/api/v1/detected_fields?line_limit=bad&limit=-1", want: 1000},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, tc.url, nil)
			if got := parseDetectedLineLimit(r); got != tc.want {
				t.Fatalf("parseDetectedLineLimit() = %d, want %d", got, tc.want)
			}
		})
	}
}
