package proxy

import (
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
