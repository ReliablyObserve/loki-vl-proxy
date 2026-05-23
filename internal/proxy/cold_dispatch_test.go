package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	// The chunked fetch wraps upstream errors as 502 BadGateway.
	if w.Code < 400 {
		t.Errorf("status = %d, want >= 400 (cold error propagated)", w.Code)
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
	// The chunked backward path surfaces the upstream error as 502 BadGateway.
	if w.Code == http.StatusOK {
		t.Errorf("cold failure should not produce a 200 OK silent partial response")
	}
	if w.Code != http.StatusServiceUnavailable && w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502 or 503 (cold error propagated)", w.Code)
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

	// Hot failure in RouteBoth must propagate as an error — returning cold-only would
	// silently truncate the [boundary, end] range without the client knowing.
	if w.Code == http.StatusOK {
		t.Errorf("hot failure should not produce a 200 OK silent partial response")
	}
	if w.Code != http.StatusServiceUnavailable && w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502 or 503 (hot error propagated)", w.Code)
	}
}

func TestProxyLogQueryBoth_BackwardDirection_ChunkedColdAndMerge(t *testing.T) {
	// Backward RouteBoth: cold covers [start, boundary] and returns oldest-first per chunk.
	// The proxy must (a) use chunked backward scan so only the N newest cold rows are
	// accumulated, (b) reverse cold before appending to hot so the merged body is
	// newest-first across both halves, and (c) trim to the original client limit.
	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/stream+json")
			// Lakehouse returns ascending (oldest first) within cold range.
			fmt.Fprintln(w, `{"_msg":"cold-old","_time":"2026-04-01T00:00:01Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"cold-new","_time":"2026-04-02T00:00:01Z","_stream":"{}"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer coldSrv.Close()

	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/stream+json")
			// Hot returns newest-first (proxy adds sort by _time desc).
			fmt.Fprintln(w, `{"_msg":"hot-new","_time":"2026-05-01T00:00:02Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"hot-old","_time":"2026-05-01T00:00:01Z","_stream":"{}"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer hotSrv.Close()

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
	// limit=3: 4 total rows (2 hot + 2 cold); backward must return the 3 newest.
	r := httptest.NewRequest("GET", "/?query=*&start="+startNs+"&end="+endNs+"&limit=3", nil)
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

	var msgs []string
	for _, stream := range result.Data.Result {
		for _, v := range stream.Values {
			if len(v) >= 2 {
				msgs = append(msgs, v[1])
			}
		}
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 entries (limit=3), got %d: %v", len(msgs), msgs)
	}
	// Backward: hot-new, hot-old, cold-new; cold-old must be trimmed.
	if msgs[0] != "hot-new" {
		t.Errorf("msgs[0] = %q, want hot-new (newest overall)", msgs[0])
	}
	if msgs[2] != "cold-new" {
		t.Errorf("msgs[2] = %q, want cold-new (newest cold row after reversal)", msgs[2])
	}
	for _, m := range msgs {
		if m == "cold-old" {
			t.Errorf("cold-old appeared in results but must be trimmed by limit=3")
		}
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

func TestReverseNDJSONBody(t *testing.T) {
	input := []byte(`{"_time":"2026-04-01T00:00:01Z","_msg":"first"}` + "\n" +
		`{"_time":"2026-04-01T00:00:02Z","_msg":"second"}` + "\n" +
		`{"_time":"2026-04-01T00:00:03Z","_msg":"third"}`)

	got := string(reverseNDJSONBody(input))
	lines := strings.Split(got, "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %q", len(lines), got)
	}
	if !strings.Contains(lines[0], "third") {
		t.Errorf("line 0 should be third (newest), got %q", lines[0])
	}
	if !strings.Contains(lines[2], "first") {
		t.Errorf("line 2 should be first (oldest), got %q", lines[2])
	}
}

func TestReverseNDJSONBody_EmptyLines(t *testing.T) {
	input := []byte(`{"_msg":"a"}` + "\n\n" + `{"_msg":"b"}` + "\n")
	got := string(reverseNDJSONBody(input))
	lines := strings.Split(got, "\n")
	if len(lines) != 2 {
		t.Fatalf("empty lines should be stripped; got %d lines: %q", len(lines), got)
	}
	if !strings.Contains(lines[0], `"b"`) {
		t.Errorf("first line after reverse should be b, got %q", lines[0])
	}
}

func TestProxyLogQueryCold_BackwardDirection_ReversesOrder(t *testing.T) {
	// Cold backend returns entries in ascending order (oldest first).
	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/stream+json")
			fmt.Fprintln(w, `{"_msg":"oldest","_time":"2026-04-01T00:00:01Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"middle","_time":"2026-04-01T00:00:02Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"newest","_time":"2026-04-01T00:00:03Z","_stream":"{}"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer coldSrv.Close()

	// Hot backend: serve a dummy response (won't be called for RouteColdOnly).
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
	}))
	defer hotSrv.Close()

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
	// No direction param → defaults to backward (Loki default).
	// Use a 30-minute window (< 1-hour chunk size) so only one chunk is fetched.
	// 1714521600 = 2024-05-01T00:00:00Z; +1800s = 2024-05-01T00:30:00Z.
	r := httptest.NewRequest("GET", "/?query=*&start=1714521600000000000&end=1714523400000000000", nil)
	p.proxyLogQueryCold(w, r, "*")

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

	var msgs []string
	for _, stream := range result.Data.Result {
		for _, v := range stream.Values {
			if len(v) >= 2 {
				msgs = append(msgs, v[1])
			}
		}
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 log entries, got %d: %v", len(msgs), msgs)
	}
	// Backward direction: newest first.
	if msgs[0] != "newest" {
		t.Errorf("msgs[0] = %q, want newest (backward = newest-first)", msgs[0])
	}
	if msgs[2] != "oldest" {
		t.Errorf("msgs[2] = %q, want oldest (backward = newest-first)", msgs[2])
	}
}

func TestProxyLogQueryCold_ForwardDirection_KeepsOrder(t *testing.T) {
	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/stream+json")
			fmt.Fprintln(w, `{"_msg":"oldest","_time":"2026-04-01T00:00:01Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"newest","_time":"2026-04-01T00:00:02Z","_stream":"{}"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer coldSrv.Close()

	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
	}))
	defer hotSrv.Close()

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
	r := httptest.NewRequest("GET", "/?query=*&start=1714521600&end=1714608000&direction=forward", nil)
	p.proxyLogQueryCold(w, r, "*")

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

	var msgs []string
	for _, stream := range result.Data.Result {
		for _, v := range stream.Values {
			if len(v) >= 2 {
				msgs = append(msgs, v[1])
			}
		}
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 log entries, got %d: %v", len(msgs), msgs)
	}
	// Forward direction: oldest first (Lakehouse natural order, no reversal).
	if msgs[0] != "oldest" {
		t.Errorf("msgs[0] = %q, want oldest (forward = oldest-first)", msgs[0])
	}
}

func TestProxyLogQueryCold_BackwardLimit_ReturnsNewest(t *testing.T) {
	// Regression test: with limit=2 and 5 entries, backward direction must return
	// the 2 NEWEST rows, not the 2 oldest rows in reverse. The chunked fetch
	// iterates from newest to oldest in 1-hour slices, stopping once the limit is
	// reached. Using a < 1-hour time window ensures a single chunk is issued.
	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/stream+json")
			// 5 entries ascending oldest→newest; mock ignores the limit param.
			fmt.Fprintln(w, `{"_msg":"e1","_time":"2026-04-01T00:00:01Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"e2","_time":"2026-04-01T00:00:02Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"e3","_time":"2026-04-01T00:00:03Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"e4","_time":"2026-04-01T00:00:04Z","_stream":"{}"}`)
			fmt.Fprintln(w, `{"_msg":"e5","_time":"2026-04-01T00:00:05Z","_stream":"{}"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer coldSrv.Close()

	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
	}))
	defer hotSrv.Close()

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
	// 30-minute window (< 1-hour chunk) so only one chunk is issued.
	// 1714521600000000000 = 2024-05-01T00:00:00Z; +1800s = 2024-05-01T00:30:00Z.
	r := httptest.NewRequest("GET", "/?query=*&start=1714521600000000000&end=1714523400000000000&limit=2", nil)
	p.proxyLogQueryCold(w, r, "*")

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

	var msgs []string
	for _, stream := range result.Data.Result {
		for _, v := range stream.Values {
			if len(v) >= 2 {
				msgs = append(msgs, v[1])
			}
		}
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 log entries (limit=2), got %d: %v", len(msgs), msgs)
	}
	// Backward + limit=2 must return the 2 NEWEST entries, newest first.
	if msgs[0] != "e5" {
		t.Errorf("msgs[0] = %q, want e5 (newest entry in backward order)", msgs[0])
	}
	if msgs[1] != "e4" {
		t.Errorf("msgs[1] = %q, want e4 (second-newest in backward order)", msgs[1])
	}
}

// TestProxyLogQueryCold_BackwardLimit_LargeRange verifies that a backward query
// over a range containing more than maxLimitValue rows returns the N newest rows
// across the full range, not the N newest from only the oldest maxLimitValue matches.
//
// With the old non-chunked implementation the proxy would issue a single VL request
// with limit=10000. The Lakehouse returns ascending rows, so it delivers e1..e10000.
// Reversing that gives e10000..e1, and trimming to 5 yields e10000..e9996 — wrong.
// The chunked backward scan fetches newest-to-oldest 1-hour slices and stops once
// limit rows are accumulated, so it returns e15000..e14996 — correct.
func TestProxyLogQueryCold_BackwardLimit_LargeRange(t *testing.T) {
	const totalRows = 15000
	const clientLimit = 5
	// Base epoch in Unix seconds; each row i has timestamp baseEpochSec+(i-1) seconds.
	const baseEpochSec = int64(1_000_000_000) // 2001-09-09T01:46:40Z

	// Cold backend returns ALL rows in the requested time window (ascending), up to
	// the server's natural capacity. It honours start/end (nanoseconds) for filtering
	// but does NOT apply the client's limit — the chunked scan's stop condition handles
	// limiting. This matches how Victoria Lakehouse works: it can return many rows per
	// 1-hour partition, and the proxy stops fetching older chunks once it has enough.
	coldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			http.NotFound(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("cold server: parse form: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		startNs, _ := strconv.ParseInt(r.FormValue("start"), 10, 64)
		endNs, _ := strconv.ParseInt(r.FormValue("end"), 10, 64)

		w.Header().Set("Content-Type", "application/x-ndjson")
		for i := 1; i <= totalRows; i++ {
			tsNs := (baseEpochSec + int64(i-1)) * int64(time.Second)
			if tsNs < startNs || tsNs >= endNs {
				continue
			}
			ts := time.Unix(0, tsNs).UTC().Format(time.RFC3339Nano)
			fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":\"e%d\",\"_stream\":\"{app=\\\"api\\\"}\"}\n", ts, i)
		}
	}))
	defer coldSrv.Close()

	// Hot backend: never called for a RouteColdOnly query (old time range).
	hotSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
	}))
	defer hotSrv.Close()

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

	// 15000 rows span baseEpochSec to baseEpochSec+15000 (Unix seconds).
	// parseFlexTimestamp converts these to nanoseconds automatically.
	startSec := strconv.FormatInt(baseEpochSec, 10)
	endSec := strconv.FormatInt(baseEpochSec+int64(totalRows), 10)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet,
		"/?query=*&start="+startSec+"&end="+endSec+"&limit="+strconv.Itoa(clientLimit)+"&direction=backward",
		nil)
	p.proxyLogQueryCold(w, r, `app:="api"`)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Parse Loki JSON response (same format used by other tests in this file).
	var result struct {
		Data struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal response: %v\nbody: %s", err, w.Body.String())
	}

	var msgs []string
	for _, stream := range result.Data.Result {
		for _, v := range stream.Values {
			if len(v) >= 2 {
				msgs = append(msgs, v[1])
			}
		}
	}

	if len(msgs) != clientLimit {
		t.Fatalf("want %d rows, got %d: %v", clientLimit, len(msgs), msgs)
	}
	// Backward = newest first. The 5 newest out of 15000 are e15000..e14996.
	want := []string{"e15000", "e14999", "e14998", "e14997", "e14996"}
	for i, wantMsg := range want {
		if msgs[i] != wantMsg {
			t.Errorf("msgs[%d] = %q, want %q (full: %v)", i, msgs[i], wantMsg, msgs)
			break
		}
	}
}
