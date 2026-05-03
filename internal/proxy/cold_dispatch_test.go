package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestColdRouteForRequest_NilRouter(t *testing.T) {
	p := &Proxy{}
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?start=1714521600&end=1714608000", nil)
	if d := p.coldRouteForRequest(r); d != RouteHotOnly {
		t.Errorf("nil router should route hot, got %s", d)
	}
}

func TestColdRouteForRequest_WithRouter(t *testing.T) {
	now := time.Now()
	cr := &ColdRouter{
		boundary: 7 * 24 * time.Hour,
		overlap:  time.Hour,
	}

	p := &Proxy{coldRouter: cr}

	// Recent query → hot only
	start := fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano())
	end := fmt.Sprintf("%d", now.UnixNano())
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?start="+start+"&end="+end, nil)
	if d := p.coldRouteForRequest(r); d != RouteHotOnly {
		t.Errorf("recent query should route hot, got %s", d)
	}

	// Old query → cold only
	start = fmt.Sprintf("%d", now.Add(-30*24*time.Hour).UnixNano())
	end = fmt.Sprintf("%d", now.Add(-20*24*time.Hour).UnixNano())
	r = httptest.NewRequest("GET", "/loki/api/v1/query_range?start="+start+"&end="+end, nil)
	if d := p.coldRouteForRequest(r); d != RouteColdOnly {
		t.Errorf("old query should route cold, got %s", d)
	}

	// Spanning query → both
	start = fmt.Sprintf("%d", now.Add(-10*24*time.Hour).UnixNano())
	end = fmt.Sprintf("%d", now.UnixNano())
	r = httptest.NewRequest("GET", "/loki/api/v1/query_range?start="+start+"&end="+end, nil)
	if d := p.coldRouteForRequest(r); d != RouteBoth {
		t.Errorf("spanning query should route both, got %s", d)
	}
}

func TestBuildLogQueryParams(t *testing.T) {
	p := &Proxy{maxLines: 1000}
	r := httptest.NewRequest("GET", "/?start=1714521600&end=1714608000&limit=500&direction=forward", nil)
	params := p.buildLogQueryParams(r, "_time:1h")

	if !strings.Contains(params.Get("query"), "sort by (_time)") {
		t.Error("forward direction should add sort by _time")
	}
	if params.Get("limit") != "500" {
		t.Errorf("limit = %q, want 500", params.Get("limit"))
	}
}

func TestBuildLogQueryParams_Backward(t *testing.T) {
	p := &Proxy{maxLines: 1000}
	r := httptest.NewRequest("GET", "/?direction=backward", nil)
	params := p.buildLogQueryParams(r, "_time:1h")

	if !strings.Contains(params.Get("query"), "sort by (_time desc)") {
		t.Error("backward direction should add sort by _time desc")
	}
}

func TestBuildLogQueryParams_DefaultLimit(t *testing.T) {
	p := &Proxy{maxLines: 1000}
	r := httptest.NewRequest("GET", "/", nil)
	params := p.buildLogQueryParams(r, "*")

	if params.Get("limit") != "1000" {
		t.Errorf("default limit = %q, want 1000", params.Get("limit"))
	}
}

func TestProxyLogQueryCold_FallbackOnError(t *testing.T) {
	// Hot backend that returns valid NDJSON
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		fmt.Fprintln(w, `{"_msg":"hot-line","_time":"2026-05-01T00:00:00Z","_stream":"{}"}`)
	}))
	defer hotSrv.Close()

	// Cold backend that returns an error
	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer coldSrv.Close()

	p, err := New(Config{
		BackendURL: hotSrv.URL,
		ColdBackend: ColdBackendConfig{
			Enabled:  true,
			URL:      coldSrv.URL,
			Boundary: 7 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?query=*&start=1714521600&end=1714608000", nil)

	p.proxyLogQueryCold(w, r, "*")

	// Should fall back to hot and succeed
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (fallback to hot)", w.Code)
	}
}

func TestProxyLogQueryBoth_MergesResults(t *testing.T) {
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/stream+json")
			fmt.Fprintln(w, `{"_msg":"hot-line","_time":"2026-05-01T00:00:00Z","_stream":"{}"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer hotSrv.Close()

	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/stream+json")
			fmt.Fprintln(w, `{"_msg":"cold-line","_time":"2026-04-01T00:00:00Z","_stream":"{}"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer coldSrv.Close()

	p, err := New(Config{
		BackendURL: hotSrv.URL,
		ColdBackend: ColdBackendConfig{
			Enabled:  true,
			URL:      coldSrv.URL,
			Boundary: 7 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?query=*&start=1714521600&end=1714608000", nil)

	p.proxyLogQueryBoth(w, r, "*")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	body := w.Body.String()
	var result struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("unmarshal response: %v\nbody: %s", err, body)
	}
	if result.Status != "success" {
		t.Errorf("status = %q, want success", result.Status)
	}

	// Should contain both hot and cold lines
	allValues := ""
	for _, stream := range result.Data.Result {
		for _, v := range stream.Values {
			if len(v) >= 2 {
				allValues += v[1]
			}
		}
	}
	if !strings.Contains(allValues, "hot-line") {
		t.Error("missing hot-line in merged results")
	}
	if !strings.Contains(allValues, "cold-line") {
		t.Error("missing cold-line in merged results")
	}
}

func TestProxyLogQueryBoth_ColdFails_ServesHot(t *testing.T) {
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		fmt.Fprintln(w, `{"_msg":"hot-only","_time":"2026-05-01T00:00:00Z","_stream":"{}"}`)
	}))
	defer hotSrv.Close()

	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "cold is down", http.StatusServiceUnavailable)
	}))
	defer coldSrv.Close()

	p, err := New(Config{
		BackendURL: hotSrv.URL,
		ColdBackend: ColdBackendConfig{
			Enabled:  true,
			URL:      coldSrv.URL,
			Boundary: 7 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?query=*&start=1714521600&end=1714608000", nil)

	p.proxyLogQueryBoth(w, r, "*")

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "hot-only") {
		t.Error("should contain hot results when cold fails")
	}
}

func TestProxyLogQueryBoth_HotFails_ServesCold(t *testing.T) {
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "hot is down", http.StatusServiceUnavailable)
	}))
	defer hotSrv.Close()

	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		fmt.Fprintln(w, `{"_msg":"cold-only","_time":"2026-04-01T00:00:00Z","_stream":"{}"}`)
	}))
	defer coldSrv.Close()

	p, err := New(Config{
		BackendURL: hotSrv.URL,
		ColdBackend: ColdBackendConfig{
			Enabled:  true,
			URL:      coldSrv.URL,
			Boundary: 7 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?query=*&start=1714521600&end=1714608000", nil)

	p.proxyLogQueryBoth(w, r, "*")

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "cold-only") {
		t.Error("should contain cold results when hot fails")
	}
}

func TestColdRouteForRequest_NoTimeRange(t *testing.T) {
	cr := &ColdRouter{
		boundary: 7 * 24 * time.Hour,
		overlap:  time.Hour,
	}
	p := &Proxy{coldRouter: cr}

	r := httptest.NewRequest("GET", "/", nil)
	if d := p.coldRouteForRequest(r); d != RouteHotOnly {
		t.Errorf("no time range should default to hot, got %s", d)
	}
}

