package proxy

import (
	"encoding/json"
	"fmt"
	"io"
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

func TestBuildColdQueryParams_NoSortClause(t *testing.T) {
	p := &Proxy{maxLines: 1000}

	for _, dir := range []string{"forward", "backward", ""} {
		r := httptest.NewRequest("GET", "/?direction="+dir+"&start=1714521600&end=1714608000&limit=200", nil)
		params := p.buildColdQueryParams(r, "*")

		if strings.Contains(params.Get("query"), "sort by") {
			t.Errorf("direction=%q: cold params must not contain sort clause, got %q", dir, params.Get("query"))
		}
		if params.Get("query") != "*" {
			t.Errorf("direction=%q: cold query modified to %q, want *", dir, params.Get("query"))
		}
		if params.Get("limit") != "200" {
			t.Errorf("direction=%q: limit = %q, want 200", dir, params.Get("limit"))
		}
	}
}

func TestProxyLogQueryCold_PropagatesError(t *testing.T) {
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		fmt.Fprintln(w, `{"_msg":"hot-line","_time":"2026-05-01T00:00:00Z","_stream":"{}"}`)
	}))
	defer hotSrv.Close()

	// Cold backend that returns an error.
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

	// RouteColdOnly means hot has no data for this range — cold failure must surface
	// as an error rather than a silent empty/hot response.
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 (cold error propagated)", w.Code)
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

	// Use timestamps that straddle the 7-day boundary so both hot and cold are queried.
	now := time.Now()
	startNs := fmt.Sprintf("%d", now.Add(-10*24*time.Hour).UnixNano()) // 10 days ago (cold range)
	endNs := fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano())       // 1 hour ago (hot range)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?query=*&start="+startNs+"&end="+endNs, nil)

	p.proxyLogQueryBoth(w, r, "*")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", w.Code, w.Body.String())
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

	// Should contain both hot and cold lines.
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

func TestProxyLogQueryBoth_ColdFails_PropagatesError(t *testing.T) {
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

	now := time.Now()
	startNs := fmt.Sprintf("%d", now.Add(-10*24*time.Hour).UnixNano())
	endNs := fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano())

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?query=*&start="+startNs+"&end="+endNs, nil)

	p.proxyLogQueryBoth(w, r, "*")

	// Cold failed for a RouteBoth range — returning hot-only would silently truncate the
	// result to [boundary, end] without the client knowing cold data is missing.
	if w.Code == http.StatusOK {
		t.Errorf("cold failure should not produce a 200 OK silent partial response")
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 (cold error propagated)", w.Code)
	}
}

func TestProxyLogQueryBoth_HotFails_PropagatesError(t *testing.T) {
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

	now := time.Now()
	startNs := fmt.Sprintf("%d", now.Add(-10*24*time.Hour).UnixNano())
	endNs := fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano())

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?query=*&start="+startNs+"&end="+endNs, nil)

	p.proxyLogQueryBoth(w, r, "*")

	// Hot covers [boundary, end]; serving cold alone silently truncates the requested range.
	// A hot failure in RouteBoth must propagate rather than return a partial 200.
	if w.Code == http.StatusOK {
		t.Errorf("hot failure should not produce a 200 OK silent partial response")
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 (hot error propagated)", w.Code)
	}
}

func TestReverseNDJSONLines(t *testing.T) {
	input := "line1\nline2\nline3\n"
	r := reverseNDJSONLines(strings.NewReader(input))
	got, _ := io.ReadAll(r)
	// Reversed: line3, line2, line1
	if !strings.Contains(string(got), "line3") || !strings.HasPrefix(string(got), "line3") {
		t.Errorf("reverseNDJSONLines output = %q, want line3 first", string(got))
	}
}

func TestLimitNDJSONLines(t *testing.T) {
	input := []byte("a\nb\nc\nd\ne\n")
	got := limitNDJSONLines(input, 3)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
	if len(lines) != 3 {
		t.Errorf("limitNDJSONLines returned %d lines, want 3: %q", len(lines), string(got))
	}
}

func TestProxyLogQueryBoth_BackwardReversesCold(t *testing.T) {
	// Cold returns entries in ascending order (oldest first, as Lakehouse always does).
	// For backward direction the proxy must reverse cold so the merge stream is newest-first.
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		fmt.Fprintln(w, `{"_msg":"hot-newer","_time":"2026-05-01T00:00:01Z","_stream":"{}"}`)
	}))
	defer hotSrv.Close()

	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		// Ascending order from cold (oldest first)
		fmt.Fprintln(w, `{"_msg":"cold-older","_time":"2026-04-01T00:00:00Z","_stream":"{}"}`)
		fmt.Fprintln(w, `{"_msg":"cold-newer","_time":"2026-04-02T00:00:00Z","_stream":"{}"}`)
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

	now := time.Now()
	startNs := fmt.Sprintf("%d", now.Add(-10*24*time.Hour).UnixNano())
	endNs := fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano())

	w := httptest.NewRecorder()
	// No direction param = backward (default)
	r := httptest.NewRequest("GET", "/?query=*&start="+startNs+"&end="+endNs, nil)

	p.proxyLogQueryBoth(w, r, "*")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	// merged NDJSON fed to processLogQueryResponse must have cold-newer before cold-older
	// (cold reversed) so that stream values are assembled newest-first within the cold block.
	idxNewer := strings.Index(body, "cold-newer")
	idxOlder := strings.Index(body, "cold-older")
	if idxNewer < 0 || idxOlder < 0 {
		t.Fatalf("both cold entries should appear in response: %s", body)
	}
	if idxNewer > idxOlder {
		t.Errorf("cold-newer (%d) should appear before cold-older (%d) in the merged stream for backward direction", idxNewer, idxOlder)
	}
}

func TestProxyLogQueryBoth_LimitEnforced(t *testing.T) {
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, `{"_msg":"hot-%d","_time":"2026-05-01T00:00:%02dZ","_stream":"{}"}`+"\n", i, i)
		}
	}))
	defer hotSrv.Close()

	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, `{"_msg":"cold-%d","_time":"2026-04-01T00:00:%02dZ","_stream":"{}"}`+"\n", i, i)
		}
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

	now := time.Now()
	startNs := fmt.Sprintf("%d", now.Add(-10*24*time.Hour).UnixNano())
	endNs := fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano())

	w := httptest.NewRecorder()
	// limit=4: cold has 3 lines, hot has 3 lines — merged should be capped at 4
	r := httptest.NewRequest("GET", "/?query=*&start="+startNs+"&end="+endNs+"&limit=4", nil)

	p.proxyLogQueryBoth(w, r, "*")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", w.Code, w.Body.String())
	}

	var result struct {
		Data struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, w.Body.String())
	}
	total := 0
	for _, s := range result.Data.Result {
		total += len(s.Values)
	}
	if total > 4 {
		t.Errorf("merged result has %d entries, want ≤ 4 (limit enforced)", total)
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

