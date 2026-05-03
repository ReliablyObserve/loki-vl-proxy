package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// TestGroupQueryRangeWindowEntries_DeterministicOrder guards against the
// non-deterministic map iteration bug: stream order and per-stream value order
// must be identical across repeated calls with the same input.
func TestGroupQueryRangeWindowEntries_DeterministicOrder(t *testing.T) {
	entries := []queryRangeWindowEntry{
		{Stream: map[string]string{"app": "b", "env": "prod"}, Ts: "1700000000000000002", Msg: "line-b2"},
		{Stream: map[string]string{"app": "a", "env": "prod"}, Ts: "1700000000000000003", Msg: "line-a3"},
		{Stream: map[string]string{"app": "b", "env": "prod"}, Ts: "1700000000000000001", Msg: "line-b1"},
		{Stream: map[string]string{"app": "a", "env": "prod"}, Ts: "1700000000000000001", Msg: "line-a1"},
		{Stream: map[string]string{"app": "c", "env": "prod"}, Ts: "1700000000000000001", Msg: "line-c1"},
		{Stream: map[string]string{"app": "a", "env": "prod"}, Ts: "1700000000000000002", Msg: "line-a2"},
	}

	for _, dir := range []string{"forward", ""} {
		dir := dir
		t.Run("direction="+dir, func(t *testing.T) {
			// Run many times to expose any non-determinism.
			var first []map[string]interface{}
			for i := 0; i < 50; i++ {
				result := groupQueryRangeWindowEntries(entries, dir, false, false)
				if first == nil {
					first = result
					continue
				}
				if len(result) != len(first) {
					t.Fatalf("iteration %d: stream count changed: got %d want %d", i, len(result), len(first))
				}
				for s := range result {
					r := result[s]["stream"].(map[string]string)
					f := first[s]["stream"].(map[string]string)
					if !reflect.DeepEqual(r, f) {
						t.Fatalf("iteration %d: stream[%d] labels changed: got %v want %v", i, s, r, f)
					}
					rv := result[s]["values"].([]interface{})
					fv := first[s]["values"].([]interface{})
					if len(rv) != len(fv) {
						t.Fatalf("iteration %d: stream[%d] value count changed", i, s)
					}
				}
			}

			// Verify stream order is ascending by canonical key.
			var streamKeys []string
			for _, s := range first {
				streamKeys = append(streamKeys, canonicalLabelsKey(s["stream"].(map[string]string)))
			}
			if !sort.StringsAreSorted(streamKeys) {
				t.Errorf("streams not sorted by canonical key: %v", streamKeys)
			}

			// Verify per-stream values are sorted by timestamp in direction order.
			forward := dir == "forward"
			for _, s := range first {
				vals := s["values"].([]interface{})
				for i := 1; i < len(vals); i++ {
					prev := vals[i-1].([]interface{})[0].(string)
					cur := vals[i].([]interface{})[0].(string)
					if forward && prev > cur {
						t.Errorf("stream %v: value[%d] timestamp %s > value[%d] timestamp %s (not ascending)",
							s["stream"], i-1, prev, i, cur)
					}
					if !forward && prev < cur {
						t.Errorf("stream %v: value[%d] timestamp %s < value[%d] timestamp %s (not descending)",
							s["stream"], i-1, prev, i, cur)
					}
				}
			}
		})
	}
}

func TestQueryRangeWindow_ExpandingRangeReusesCachedWindows(t *testing.T) {
	var backendCalls atomic.Int64
	seenWindows := map[string]int{}
	var mu sync.Mutex

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		_ = r.ParseForm()
		start := r.Form.Get("start")
		end := r.Form.Get("end")
		key := start + ":" + end
		mu.Lock()
		seenWindows[key]++
		mu.Unlock()

		endNs, _ := strconv.ParseInt(end, 10, 64)
		msg := fmt.Sprintf("window=%s", key)
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":%q,\"_stream\":\"{app=\\\"nginx\\\"}\"}\n", time.Unix(0, endNs).UTC().Format(time.RFC3339Nano), msg)
	}))
	defer vlBackend.Close()

	p := newWindowingTestProxy(t, vlBackend.URL)
	start := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	// Use 3 windows initially, then expand to 4 windows to verify cached window reuse
	// with exactly one additional backend call for the newly covered window.
	const initialRangeDuration = 3 * time.Hour
	const expandedRangeDuration = 4 * time.Hour
	endA := start + int64(initialRangeDuration) - 1
	endB := start + int64(expandedRangeDuration) - 1

	reqA := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100", url.QueryEscape(`{app="nginx"}`), start, endA), nil)
	wA := httptest.NewRecorder()
	p.handleQueryRange(wA, reqA)
	if wA.Code != http.StatusOK {
		t.Fatalf("unexpected status for initial range: %d body=%s", wA.Code, wA.Body.String())
	}
	if got := backendCalls.Load(); got != 3 {
		t.Fatalf("expected 3 backend window calls for initial range, got %d", got)
	}
	mu.Lock()
	if len(seenWindows) != 3 {
		t.Fatalf("expected 3 unique initial windows, got %d", len(seenWindows))
	}
	mu.Unlock()

	reqB := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100", url.QueryEscape(`{app="nginx"}`), start, endB), nil)
	wB := httptest.NewRecorder()
	p.handleQueryRange(wB, reqB)
	if wB.Code != http.StatusOK {
		t.Fatalf("unexpected status for expanded range: %d body=%s", wB.Code, wB.Body.String())
	}
	if got := backendCalls.Load(); got != 4 {
		t.Fatalf("expected exactly one additional backend call after expansion, got %d", got)
	}
	mu.Lock()
	if len(seenWindows) != 4 {
		t.Fatalf("expected 4 unique windows after expansion, got %d", len(seenWindows))
	}
	mu.Unlock()
}

func TestQueryRangeWindow_LimitAndDirection(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		endNs, _ := strconv.ParseInt(r.Form.Get("end"), 10, 64)
		query := r.Form.Get("query")
		desc := strings.Contains(query, "_time desc")

		var tsA, tsB int64
		if desc {
			tsA, tsB = endNs, maxInt64(startNs, endNs-1)
		} else {
			tsA, tsB = startNs, minInt64(endNs, startNs+1)
		}
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":\"a\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n", time.Unix(0, tsA).UTC().Format(time.RFC3339Nano))
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":\"b\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n", time.Unix(0, tsB).UTC().Format(time.RFC3339Nano))
	}))
	defer vlBackend.Close()

	p := newWindowingTestProxy(t, vlBackend.URL)
	start := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(2*time.Hour) - 1
	oldestWindowStart := start
	newestWindowEnd := end

	backwardReq := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=2", url.QueryEscape(`{app="nginx"}`), start, end), nil)
	backwardResp := httptest.NewRecorder()
	p.handleQueryRange(backwardResp, backwardReq)
	assertQueryRangeFirstTimestamp(t, backwardResp, newestWindowEnd)

	forwardReq := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=2&direction=forward", url.QueryEscape(`{app="nginx"}`), start, end), nil)
	forwardResp := httptest.NewRecorder()
	p.handleQueryRange(forwardResp, forwardReq)
	assertQueryRangeFirstTimestamp(t, forwardResp, oldestWindowStart)
}

func TestQueryRangeWindow_ParserChainBraceHeavyLine(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		msg := `time="2026-01-01T00:00:00Z" level=error msg="Drop if no profiles matched {json_like=true}"`
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":%q,\"_stream\":\"{app=\\\"iptables\\\"}\"}\n", time.Unix(0, startNs).UTC().Format(time.RFC3339Nano), msg)
	}))
	defer vlBackend.Close()

	p := newWindowingTestProxy(t, vlBackend.URL)
	start := time.Now().Add(-12 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(2*time.Hour) - 1
	q := `{app="iptables"} | json | logfmt | drop __error__, __error_details__`
	req := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100", url.QueryEscape(q), start, end), nil)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.Code, resp.Body.String())
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to decode response: %v body=%s", err, resp.Body.String())
	}
	data, _ := parsed["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	if len(result) == 0 {
		t.Fatalf("expected non-empty result for parser-chain query")
	}
	streamObj, _ := result[0].(map[string]interface{})
	values, _ := streamObj["values"].([]interface{})
	if len(values) == 0 {
		t.Fatalf("expected non-empty values")
	}
	first, ok := values[0].([]interface{})
	if !ok || len(first) < 2 {
		t.Fatalf("expected Loki tuple shape []interface{} with at least 2 items, got %#v", values[0])
	}
}

func TestQueryRangeWindow_CategorizeLabelsUsesMetadataObjectMaps(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		msg := `time="2026-01-01T00:00:00Z" level=error status=500 component=ingester`
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":%q,\"_stream\":\"{app=\\\"iptables\\\"}\",\"http.status_code\":\"500\"}\n", time.Unix(0, startNs).UTC().Format(time.RFC3339Nano), msg)
	}))
	defer vlBackend.Close()

	p := newWindowingTestProxy(t, vlBackend.URL)
	p.emitStructuredMetadata = true

	start := time.Now().Add(-12 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(2*time.Hour) - 1
	q := `{app="iptables"} | json | logfmt | drop __error__, __error_details__`
	req := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100", url.QueryEscape(q), start, end), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.Code, resp.Body.String())
	}

	var payload struct {
		Data struct {
			EncodingFlags []string `json:"encodingFlags"`
			Result        []struct {
				Values []json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v body=%s", err, resp.Body.String())
	}
	if len(payload.Data.Result) == 0 || len(payload.Data.Result[0].Values) == 0 {
		t.Fatalf("expected non-empty stream values: %#v", payload.Data.Result)
	}
	hasCategorizedFlag := false
	for _, flag := range payload.Data.EncodingFlags {
		if flag == "categorize-labels" {
			hasCategorizedFlag = true
			break
		}
	}
	if !hasCategorizedFlag {
		t.Fatalf("expected encodingFlags to include categorize-labels, body=%s", resp.Body.String())
	}

	var tuple []json.RawMessage
	if err := json.Unmarshal(payload.Data.Result[0].Values[0], &tuple); err != nil {
		t.Fatalf("expected stream tuple array, got %s: %v", string(payload.Data.Result[0].Values[0]), err)
	}
	if len(tuple) != 3 {
		t.Fatalf("expected categorize-labels 3-tuple, got len=%d tuple=%s", len(tuple), string(payload.Data.Result[0].Values[0]))
	}

	var metadata struct {
		StructuredMetadata map[string]string `json:"structuredMetadata"`
		Parsed             map[string]string `json:"parsed"`
	}
	if err := json.Unmarshal(tuple[2], &metadata); err != nil {
		t.Fatalf("expected tuple[2] metadata object with Loki maps, got %s: %v", string(tuple[2]), err)
	}
	if len(metadata.Parsed) == 0 && len(metadata.StructuredMetadata) == 0 {
		t.Fatalf("expected parsed and/or structuredMetadata maps, got %s", string(tuple[2]))
	}
	for field := range metadata.StructuredMetadata {
		if field == "" {
			t.Fatalf("expected non-empty structuredMetadata key, got %s", string(tuple[2]))
		}
	}
	for field := range metadata.Parsed {
		if field == "" {
			t.Fatalf("expected non-empty parsed key, got %s", string(tuple[2]))
		}
	}
}

func TestQueryRangeWindow_WarmQueryRangeWindowsAsync_PrimesCache(t *testing.T) {
	var backendCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		_ = r.ParseForm()
		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":\"warm-cache-test message\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n", time.Unix(0, startNs).UTC().Format(time.RFC3339Nano))
	}))
	defer vlBackend.Close()

	p := newWindowingTestProxy(t, vlBackend.URL)
	p.queryRangeBackgroundWarmMaxWindows = 1

	start := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	window := queryRangeWindow{startNs: start, endNs: start + int64(time.Hour) - 1}
	req := httptest.NewRequest(
		"GET",
		fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100", url.QueryEscape(`{app="nginx"}`), window.startNs, window.endNs),
		nil,
	)
	if err := req.ParseForm(); err != nil {
		t.Fatalf("parse form: %v", err)
	}
	cacheKey := p.queryRangeWindowCacheKey(req, `{app="nginx"}`, "100", window, false, false)

	p.warmQueryRangeWindowsAsync(req, `{app="nginx"}`, "100", []queryRangeWindow{window}, false, false)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := p.cache.Get(cacheKey); ok {
			if got := backendCalls.Load(); got == 0 {
				t.Fatal("expected warmQueryRangeWindowsAsync to call backend at least once")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected warmQueryRangeWindowsAsync to populate cache key %q", cacheKey)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestQueryRangeWindow_MultiTenantCompatibility(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		tenant := r.Header.Get("X-Scope-OrgID")
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":%q,\"_stream\":\"{app=\\\"nginx\\\"}\"}\n", time.Unix(0, startNs).UTC().Format(time.RFC3339Nano), "tenant="+tenant)
	}))
	defer vlBackend.Close()

	p := newWindowingTestProxy(t, vlBackend.URL)
	start := time.Now().Add(-8 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(2*time.Hour) - 1
	req := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100", url.QueryEscape(`{app="nginx"}`), start, end), nil)
	req.Header.Set("X-Scope-OrgID", "1|2")
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.Code, resp.Body.String())
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	data, _ := parsed["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	seenTenants := map[string]bool{}
	for _, item := range result {
		streamObj, _ := item.(map[string]interface{})
		streamLabels, _ := streamObj["stream"].(map[string]interface{})
		if tenant, ok := streamLabels["__tenant_id__"].(string); ok {
			seenTenants[tenant] = true
		}
	}
	if !seenTenants["1"] || !seenTenants["2"] {
		t.Fatalf("expected merged response to include __tenant_id__ labels for both tenants, got=%v", seenTenants)
	}
}

func TestQueryRangeWindow_FetchStoresCacheLocallyWhenPeerWriteThroughEnabled(t *testing.T) {
	var remoteSetCalls atomic.Int64
	ownerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cache/set" {
			remoteSetCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer ownerSrv.Close()

	ownerURL, err := url.Parse(ownerSrv.URL)
	if err != nil {
		t.Fatalf("parse owner URL: %v", err)
	}

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		_ = r.ParseForm()
		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":\"window-cache\",\"_stream\":\"{app=\\\"api\\\"}\"}\n", time.Unix(0, startNs).UTC().Format(time.RFC3339Nano))
	}))
	defer backend.Close()

	window := queryRangeWindow{
		startNs: time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Hour).UnixNano(),
	}
	window.endNs = window.startNs + int64(time.Hour) - 1
	req := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&limit=100", url.QueryEscape(`{app="api"}`)), nil)
	if err := req.ParseForm(); err != nil {
		t.Fatalf("parse form: %v", err)
	}
	cacheKey := (&Proxy{}).queryRangeWindowCacheKey(req, `{app="api"}`, "100", window, false, false)

	pc := newNonOwnerPeerCacheForKey(t, ownerURL.Host, cacheKey)
	defer pc.Close()
	c := cache.New(60*time.Second, 1000)
	c.SetL3(pc)
	defer c.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      c,
		PeerCache:                  pc,
		LogLevel:                   "error",
		QueryRangeWindowingEnabled: true,
		QueryRangeFreshness:        10 * time.Minute,
		QueryRangeRecentCacheTTL:   0,
		QueryRangeHistoryCacheTTL:  time.Minute,
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	if _, err := p.fetchQueryRangeWindow(context.Background(), req, `{app="api"}`, "100", 100, window, false, false); err != nil {
		t.Fatalf("fetchQueryRangeWindow returned error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if got := remoteSetCalls.Load(); got != 0 {
		t.Fatalf("expected window cache entry to stay local, got %d remote /_cache/set calls", got)
	}
	if got := pc.WTPushes.Load(); got != 0 {
		t.Fatalf("expected local-only window cache write to skip write-through pushes, got %d", got)
	}
	if got := pc.WTErrors.Load(); got != 0 {
		t.Fatalf("expected local-only window cache write to skip write-through errors, got %d", got)
	}
	if _, ok := p.cache.Get(cacheKey); !ok {
		t.Fatalf("expected local cache key %q to be stored", cacheKey)
	}
}

func TestQueryRangeWindowHitEstimate_CachesLocallyWhenPeerWriteThroughEnabled(t *testing.T) {
	var remoteSetCalls atomic.Int64
	ownerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cache/set" {
			remoteSetCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer ownerSrv.Close()

	ownerURL, err := url.Parse(ownerSrv.URL)
	if err != nil {
		t.Fatalf("parse owner URL: %v", err)
	}

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"hits":[{"timestamps":["1710000000"],"values":[7]}]}`))
	}))
	defer backend.Close()

	window := queryRangeWindow{
		startNs: time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Hour).UnixNano(),
	}
	window.endNs = window.startNs + int64(time.Hour) - 1
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	cacheKey := (&Proxy{}).queryRangeWindowHasHitsCacheKey(req, `{app="api"}`, window)

	pc := newNonOwnerPeerCacheForKey(t, ownerURL.Host, cacheKey)
	defer pc.Close()
	c := cache.New(60*time.Second, 1000)
	c.SetL3(pc)
	defer c.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      c,
		PeerCache:                  pc,
		LogLevel:                   "error",
		QueryRangeWindowingEnabled: true,
		QueryRangeFreshness:        10 * time.Minute,
		QueryRangeRecentCacheTTL:   0,
		QueryRangeHistoryCacheTTL:  time.Minute,
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	got, err := p.queryRangeWindowHitEstimate(context.Background(), req, `{app="api"}`, window)
	if err != nil {
		t.Fatalf("queryRangeWindowHitEstimate returned error: %v", err)
	}
	if got != 7 {
		t.Fatalf("expected hit estimate 7, got %d", got)
	}
	time.Sleep(100 * time.Millisecond)

	if got := remoteSetCalls.Load(); got != 0 {
		t.Fatalf("expected hit-estimate cache entry to stay local, got %d remote /_cache/set calls", got)
	}
	if got := pc.WTPushes.Load(); got != 0 {
		t.Fatalf("expected local-only hit-estimate cache write to skip write-through pushes, got %d", got)
	}
	if got := pc.WTErrors.Load(); got != 0 {
		t.Fatalf("expected local-only hit-estimate cache write to skip write-through errors, got %d", got)
	}
	if _, ok := p.cache.Get(cacheKey); !ok {
		t.Fatalf("expected local prefilter cache key %q to be stored", cacheKey)
	}
}

func TestQueryRangeWindow_SevenDayRangeUsesParallelWindowFetch(t *testing.T) {
	var calls atomic.Int64
	var inFlight atomic.Int64
	var maxInFlight atomic.Int64

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		current := inFlight.Add(1)
		for {
			observed := maxInFlight.Load()
			if current <= observed || maxInFlight.CompareAndSwap(observed, current) {
				break
			}
		}
		defer inFlight.Add(-1)

		_ = r.ParseForm()
		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		time.Sleep(10 * time.Millisecond)
		_, _ = fmt.Fprintf(w, "{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n", time.Unix(0, startNs).UTC().Format(time.RFC3339Nano))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL:                      vlBackend.URL,
		Cache:                           c,
		LogLevel:                        "error",
		QueryRangeWindowingEnabled:      true,
		QueryRangeSplitInterval:         time.Hour,
		QueryRangeAdaptiveParallel:      true,
		QueryRangeParallelMin:           2,
		QueryRangeParallelMax:           8,
		QueryRangeLatencyTarget:         500 * time.Millisecond,
		QueryRangeLatencyBackoff:        5 * time.Second,
		QueryRangeAdaptiveCooldown:      0,
		QueryRangeErrorBackoffThreshold: 0.5,
		QueryRangeFreshness:             10 * time.Minute,
		QueryRangeRecentCacheTTL:        0,
		QueryRangeHistoryCacheTTL:       24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	start := time.Now().Add(-7 * 24 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(7*24*time.Hour) - 1
	req := httptest.NewRequest("GET", fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=5000", url.QueryEscape(`{app="nginx"}`), start, end), nil)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.Code, resp.Body.String())
	}

	const expectedWindows = int64(7 * 24)
	if got := calls.Load(); got != expectedWindows {
		t.Fatalf("expected %d window fetches for 7-day range, got %d", expectedWindows, got)
	}
	if got := maxInFlight.Load(); got < 2 {
		t.Fatalf("expected at least 2 concurrent window fetches for adaptive parallel mode, got %d", got)
	}
}

func TestQueryRangeWindow_PrefilterSkipsEmptyWindows(t *testing.T) {
	start := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(3*time.Hour) - 1
	emptyWindowStart := start + int64(time.Hour)

	var hitsCalls atomic.Int64
	var queryCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/hits":
			hitsCalls.Add(1)
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			hits := 1
			if windowStart == emptyWindowStart {
				hits = 0
			}
			_, _ = fmt.Fprintf(
				w,
				`{"hits":[{"fields":{"app":"nginx"},"timestamps":["%s"],"values":[%d]}]}`,
				time.Unix(0, windowStart).UTC().Format(time.RFC3339),
				hits,
			)
		case "/select/logsql/query":
			queryCalls.Add(1)
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			_, _ = fmt.Fprintf(
				w,
				"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
				time.Unix(0, windowStart).UTC().Format(time.RFC3339Nano),
			)
		default:
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL:                    vlBackend.URL,
		Cache:                         c,
		LogLevel:                      "error",
		QueryRangeWindowingEnabled:    true,
		QueryRangeSplitInterval:       time.Hour,
		QueryRangeMaxParallel:         1,
		QueryRangeAdaptiveParallel:    false,
		QueryRangeFreshness:           10 * time.Minute,
		QueryRangeRecentCacheTTL:      0,
		QueryRangeHistoryCacheTTL:     24 * time.Hour,
		QueryRangePrefilterIndexStats: true,
		QueryRangePrefilterMinWindows: 1,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(
		"GET",
		fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100", url.QueryEscape(`{app="nginx"} | json`), start, end),
		nil,
	)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.Code, resp.Body.String())
	}
	if got := hitsCalls.Load(); got != 3 {
		t.Fatalf("expected 3 prefilter hit checks, got %d", got)
	}
	if got := queryCalls.Load(); got != 2 {
		t.Fatalf("expected 2 window query calls after prefilter, got %d", got)
	}

	metricsBody := collectMetricsText(t, p.metrics)
	for _, snippet := range []string{
		"loki_vl_proxy_window_prefilter_attempt_total 1",
		"loki_vl_proxy_window_prefilter_error_total 0",
		"loki_vl_proxy_window_prefilter_kept_total 2",
		"loki_vl_proxy_window_prefilter_skipped_total 1",
	} {
		if !strings.Contains(metricsBody, snippet) {
			t.Fatalf("expected metrics to include %q\nmetrics:\n%s", snippet, metricsBody)
		}
	}
}

func TestQueryRangeWindow_PrefilterFailureFallsBackToAllWindows(t *testing.T) {
	start := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(3*time.Hour) - 1

	var hitsCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/hits":
			hitsCalls.Add(1)
			http.Error(w, "prefilter backend unavailable", http.StatusBadGateway)
		case "/select/logsql/query":
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			_, _ = fmt.Fprintf(
				w,
				"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
				time.Unix(0, windowStart).UTC().Format(time.RFC3339Nano),
			)
		default:
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL:                    vlBackend.URL,
		Cache:                         c,
		LogLevel:                      "error",
		QueryRangeWindowingEnabled:    true,
		QueryRangeSplitInterval:       time.Hour,
		QueryRangeMaxParallel:         1,
		QueryRangeAdaptiveParallel:    false,
		QueryRangeFreshness:           10 * time.Minute,
		QueryRangeRecentCacheTTL:      0,
		QueryRangeHistoryCacheTTL:     24 * time.Hour,
		QueryRangePrefilterIndexStats: true,
		QueryRangePrefilterMinWindows: 1,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(
		"GET",
		fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100", url.QueryEscape(`{app="nginx"} | logfmt`), start, end),
		nil,
	)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.Code, resp.Body.String())
	}
	if hitsCalls.Load() < 1 {
		t.Fatalf("expected prefilter /hits calls, got %d", hitsCalls.Load())
	}

	metricsBody := collectMetricsText(t, p.metrics)
	for _, snippet := range []string{
		"loki_vl_proxy_window_prefilter_attempt_total 1",
		"loki_vl_proxy_window_prefilter_error_total 1",
		"loki_vl_proxy_window_prefilter_kept_total 0",
		"loki_vl_proxy_window_prefilter_skipped_total 0",
		"loki_vl_proxy_window_count_count 1",
		"loki_vl_proxy_window_count_sum 3",
	} {
		if !strings.Contains(metricsBody, snippet) {
			t.Fatalf("expected metrics to include %q\nmetrics:\n%s", snippet, metricsBody)
		}
	}
}

func TestQueryRangeWindow_StreamAwareBatchingCapsExpensiveWindows(t *testing.T) {
	start := time.Now().Add(-6 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(6*time.Hour) - 1

	var inFlight atomic.Int64
	var maxInFlight atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/hits":
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			_, _ = fmt.Fprintf(
				w,
				`{"hits":[{"fields":{"app":"nginx"},"timestamps":["%s"],"values":[100000]}]}`,
				time.Unix(0, windowStart).UTC().Format(time.RFC3339),
			)
		case "/select/logsql/query":
			cur := inFlight.Add(1)
			for {
				prev := maxInFlight.Load()
				if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			_, _ = fmt.Fprintf(
				w,
				"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
				time.Unix(0, windowStart).UTC().Format(time.RFC3339Nano),
			)
		default:
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL:                      vlBackend.URL,
		Cache:                           c,
		LogLevel:                        "error",
		QueryRangeWindowingEnabled:      true,
		QueryRangeSplitInterval:         time.Hour,
		QueryRangeMaxParallel:           4,
		QueryRangeAdaptiveParallel:      false,
		QueryRangeFreshness:             10 * time.Minute,
		QueryRangeRecentCacheTTL:        0,
		QueryRangeHistoryCacheTTL:       24 * time.Hour,
		QueryRangePrefilterIndexStats:   true,
		QueryRangePrefilterMinWindows:   1,
		QueryRangeStreamAwareBatching:   true,
		QueryRangeExpensiveHitThreshold: 1,
		QueryRangeExpensiveMaxParallel:  1,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=1000", url.QueryEscape(`{app="nginx"}`), start, end),
		nil,
	)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.Code, resp.Body.String())
	}
	if got := maxInFlight.Load(); got > 1 {
		t.Fatalf("expected expensive windows to be processed with max parallel 1, got=%d", got)
	}
}

func TestQueryRangeWindow_AlignedWindowsImproveOverlapReuse(t *testing.T) {
	run := func(t *testing.T, aligned bool) int64 {
		t.Helper()
		var backendCalls atomic.Int64
		vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/select/logsql/query" {
				t.Fatalf("unexpected backend path: %s", r.URL.Path)
			}
			backendCalls.Add(1)
			_ = r.ParseForm()
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			_, _ = fmt.Fprintf(
				w,
				"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
				time.Unix(0, windowStart).UTC().Format(time.RFC3339Nano),
			)
		}))
		defer vlBackend.Close()

		p, err := New(Config{
			BackendURL:                 vlBackend.URL,
			Cache:                      cache.New(60*time.Second, 20000),
			LogLevel:                   "error",
			QueryRangeWindowingEnabled: true,
			QueryRangeSplitInterval:    time.Hour,
			QueryRangeMaxParallel:      1,
			QueryRangeAdaptiveParallel: false,
			QueryRangeFreshness:        10 * time.Minute,
			QueryRangeRecentCacheTTL:   0,
			QueryRangeHistoryCacheTTL:  24 * time.Hour,
			QueryRangeAlignWindows:     aligned,
		})
		if err != nil {
			t.Fatalf("failed to create proxy: %v", err)
		}

		base := time.Now().Add(-12 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
		start1 := base + int64(15*time.Minute)
		end1 := start1 + int64(4*time.Hour) - 1
		start2 := start1 + int64(10*time.Minute)
		end2 := end1 + int64(10*time.Minute)

		req1 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=1000", url.QueryEscape(`{app="nginx"}`), start1, end1), nil)
		w1 := httptest.NewRecorder()
		p.handleQueryRange(w1, req1)
		if w1.Code != http.StatusOK {
			t.Fatalf("unexpected status for request1: %d body=%s", w1.Code, w1.Body.String())
		}
		callsAfterFirst := backendCalls.Load()

		req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=1000", url.QueryEscape(`{app="nginx"}`), start2, end2), nil)
		w2 := httptest.NewRecorder()
		p.handleQueryRange(w2, req2)
		if w2.Code != http.StatusOK {
			t.Fatalf("unexpected status for request2: %d body=%s", w2.Code, w2.Body.String())
		}
		callsAfterSecond := backendCalls.Load()
		return callsAfterSecond - callsAfterFirst
	}

	deltaUnaligned := run(t, false)
	deltaAligned := run(t, true)
	if deltaAligned >= deltaUnaligned {
		t.Fatalf("expected aligned windows to require fewer additional backend calls: aligned=%d unaligned=%d", deltaAligned, deltaUnaligned)
	}
}

func TestQueryRangeWindow_PartialResponseOnRetryableFailure(t *testing.T) {
	start := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(3*time.Hour) - 1
	oldestWindowStart := start
	oldestWindowEnd := start + int64(time.Hour) - 1

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		_ = r.ParseForm()
		windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		windowEnd, _ := strconv.ParseInt(r.Form.Get("end"), 10, 64)
		if windowStart == oldestWindowStart && windowEnd == oldestWindowEnd {
			http.Error(w, "all backends unavailable", http.StatusBadGateway)
			return
		}
		_, _ = fmt.Fprintf(
			w,
			"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
			time.Unix(0, windowStart).UTC().Format(time.RFC3339Nano),
		)
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:                 vlBackend.URL,
		Cache:                      cache.New(60*time.Second, 10000),
		LogLevel:                   "error",
		QueryRangeWindowingEnabled: true,
		QueryRangeSplitInterval:    time.Hour,
		QueryRangeMaxParallel:      1,
		QueryRangeAdaptiveParallel: false,
		QueryRangeFreshness:        10 * time.Minute,
		QueryRangeRecentCacheTTL:   0,
		QueryRangeHistoryCacheTTL:  24 * time.Hour,
		QueryRangePartialResponses: true,
		QueryRangeBackgroundWarm:   false,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=5000", url.QueryEscape(`{app="nginx"}`), start, end),
		nil,
	)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected partial-success status=200, got=%d body=%s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("X-Loki-VL-Partial-Response"); got != "true" {
		t.Fatalf("expected X-Loki-VL-Partial-Response=true, got %q", got)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to decode response: %v body=%s", err, resp.Body.String())
	}
	if parsed["status"] != "success" {
		t.Fatalf("expected success body for partial response, got=%v", parsed["status"])
	}
	data, _ := parsed["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	if len(result) == 0 {
		t.Fatalf("expected non-empty partial result set")
	}

	metricsBody := collectMetricsText(t, p.metrics)
	for _, snippet := range []string{
		"loki_vl_proxy_window_partial_response_total 1",
		"loki_vl_proxy_window_retry_total ",
	} {
		if !strings.Contains(metricsBody, snippet) {
			t.Fatalf("expected metrics to include %q\nmetrics:\n%s", snippet, metricsBody)
		}
	}
}

func TestQueryRangeWindow_SevenDayRegressionSLO(t *testing.T) {
	const (
		sparseWindowModulo   = 8
		sparseWindowHitCount = 1000
	)
	start := time.Now().Add(-7 * 24 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(7*24*time.Hour) - 1
	stepNs := int64(time.Hour)

	var hitsCalls atomic.Int64
	var queryCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/hits":
			hitsCalls.Add(1)
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			idx := int((windowStart - start) / stepNs)
			val := 0
			// Sparse long-range history: only every 8th window has events.
			if idx%sparseWindowModulo == 0 {
				val = sparseWindowHitCount
			}
			_, _ = fmt.Fprintf(
				w,
				`{"hits":[{"fields":{"app":"nginx"},"timestamps":["%s"],"values":[%d]}]}`,
				time.Unix(0, windowStart).UTC().Format(time.RFC3339),
				val,
			)
		case "/select/logsql/query":
			queryCalls.Add(1)
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			time.Sleep(2 * time.Millisecond)
			_, _ = fmt.Fprintf(
				w,
				"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
				time.Unix(0, windowStart).UTC().Format(time.RFC3339Nano),
			)
		default:
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:                      vlBackend.URL,
		Cache:                           cache.New(60*time.Second, 100000),
		LogLevel:                        "error",
		QueryRangeWindowingEnabled:      true,
		QueryRangeSplitInterval:         time.Hour,
		QueryRangeMaxParallel:           4,
		QueryRangeAdaptiveParallel:      false,
		QueryRangeFreshness:             10 * time.Minute,
		QueryRangeRecentCacheTTL:        0,
		QueryRangeHistoryCacheTTL:       24 * time.Hour,
		QueryRangePrefilterIndexStats:   true,
		QueryRangePrefilterMinWindows:   1,
		QueryRangeStreamAwareBatching:   true,
		QueryRangeExpensiveHitThreshold: 500,
		QueryRangeExpensiveMaxParallel:  1,
		QueryRangeAlignWindows:          true,
		QueryRangeWindowTimeout:         10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=5000", url.QueryEscape(`{app="nginx"} | json`), start, end), nil)
	resp := httptest.NewRecorder()

	begin := time.Now()
	p.handleQueryRange(resp, req)
	duration := time.Since(begin)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", resp.Code, resp.Body.String())
	}
	if duration > 2*time.Second {
		t.Fatalf("expected 7d sparse-range regression test under 2s, got %s", duration)
	}
	if got := queryCalls.Load(); got > 30 {
		t.Fatalf("expected <=30 window query fanout calls after prefilter, got %d", got)
	}
	if got := hitsCalls.Load(); got < 168 {
		t.Fatalf("expected prefilter to inspect all 168 windows, got %d", got)
	}
}

func TestQueryRangeWindow_FallsBackToDirectQueryOnWindowFetchError(t *testing.T) {
	start := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(2*time.Hour) - 1

	var fullRangeCalls atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		_ = r.ParseForm()
		startParam := r.Form.Get("start")
		endParam := r.Form.Get("end")
		reqStart, _ := strconv.ParseInt(startParam, 10, 64)
		reqEnd, _ := strconv.ParseInt(endParam, 10, 64)

		if reqStart == start && reqEnd == end {
			fullRangeCalls.Add(1)
		}
		http.Error(w, "all backends unavailable", http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p := newWindowingTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest(
		"GET",
		fmt.Sprintf(
			"/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100",
			url.QueryEscape(`{app="nginx"}`),
			start,
			end,
		),
		nil,
	)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)

	if resp.Code != http.StatusBadGateway {
		t.Fatalf("expected windowed query failure status=502, got=%d body=%s", resp.Code, resp.Body.String())
	}
	if got := fullRangeCalls.Load(); got != 0 {
		t.Fatalf("expected no direct full-range fallback call, got %d", got)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to decode error response: %v body=%s", err, resp.Body.String())
	}
	if parsed["status"] != "error" {
		t.Fatalf("expected Loki error response status=error, body=%s", resp.Body.String())
	}
	if parsed["errorType"] != "unavailable" {
		t.Fatalf("expected Loki errorType=unavailable, body=%s", resp.Body.String())
	}
}

func TestQueryRangeWindow_RetriesTransientWindowFailures(t *testing.T) {
	start := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(2*time.Hour) - 1

	var (
		mu       sync.Mutex
		attempts = map[string]int{}
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		_ = r.ParseForm()
		startParam := r.Form.Get("start")
		endParam := r.Form.Get("end")
		key := startParam + ":" + endParam
		startNs, _ := strconv.ParseInt(startParam, 10, 64)

		mu.Lock()
		attempts[key]++
		n := attempts[key]
		mu.Unlock()

		if n == 1 {
			http.Error(w, "all backends unavailable", http.StatusBadGateway)
			return
		}
		_, _ = fmt.Fprintf(
			w,
			"{\"_time\":%q,\"_msg\":\"retry-ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
			time.Unix(0, startNs).UTC().Format(time.RFC3339Nano),
		)
	}))
	defer vlBackend.Close()

	p := newWindowingTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest(
		"GET",
		fmt.Sprintf(
			"/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100",
			url.QueryEscape(`{app="nginx"}`),
			start,
			end,
		),
		nil,
	)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected successful response after retries, got=%d body=%s", resp.Code, resp.Body.String())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 {
		t.Fatalf("expected two hourly windows, got %d windows: %+v", len(attempts), attempts)
	}
	for window, n := range attempts {
		if n < 2 {
			t.Fatalf("expected at least one retry for window %s, got %d attempts", window, n)
		}
	}
}

func TestQueryRangeWindow_DegradesBatchParallelismOnBackendUnavailable(t *testing.T) {
	start := time.Now().Add(-4 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	end := start + int64(4*time.Hour) - 1

	var (
		calls    atomic.Int64
		failures atomic.Int64
		inFlight atomic.Int64
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		calls.Add(1)
		current := inFlight.Add(1)
		defer inFlight.Add(-1)
		_ = r.ParseForm()
		time.Sleep(15 * time.Millisecond)

		// Simulate an overloaded backend that rejects parallel window fetches.
		if current > 1 {
			failures.Add(1)
			http.Error(w, "all the 1 backends for the user \"\" are unavailable for proxying the request", http.StatusBadGateway)
			return
		}

		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		_, _ = fmt.Fprintf(
			w,
			"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
			time.Unix(0, startNs).UTC().Format(time.RFC3339Nano),
		)
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL:                 vlBackend.URL,
		Cache:                      c,
		LogLevel:                   "error",
		QueryRangeWindowingEnabled: true,
		QueryRangeSplitInterval:    time.Hour,
		QueryRangeMaxParallel:      4,
		QueryRangeAdaptiveParallel: false,
		QueryRangeFreshness:        10 * time.Minute,
		QueryRangeRecentCacheTTL:   0,
		QueryRangeHistoryCacheTTL:  24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(
		"GET",
		fmt.Sprintf(
			"/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100",
			url.QueryEscape(`{app="nginx"}`),
			start,
			end,
		),
		nil,
	)
	resp := httptest.NewRecorder()
	p.handleQueryRange(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected successful response after batch degradation, got=%d body=%s", resp.Code, resp.Body.String())
	}
	if failures.Load() == 0 {
		t.Fatal("expected at least one backend-unavailable failure before degradation")
	}
	if calls.Load() <= 4 {
		t.Fatalf("expected additional backend attempts after degradation, got calls=%d", calls.Load())
	}
}

func TestQueryRangeWindow_ParseLokiTimeAndNormalization(t *testing.T) {
	t.Run("rfc3339", func(t *testing.T) {
		raw := "2026-04-10T12:00:00Z"
		got, ok := parseLokiTimeToUnixNano(raw)
		if !ok {
			t.Fatalf("expected %q to parse", raw)
		}
		want := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC).UnixNano()
		if got != want {
			t.Fatalf("unexpected unix nanos: got=%d want=%d", got, want)
		}
	})

	t.Run("now_relative", func(t *testing.T) {
		before := time.Now().UnixNano()
		gotNow, ok := parseLokiTimeToUnixNano("now")
		if !ok {
			t.Fatal("expected now to parse")
		}
		after := time.Now().UnixNano()
		if gotNow < before || gotNow > after {
			t.Fatalf("expected now timestamp between %d and %d, got %d", before, after, gotNow)
		}

		gotPast, ok := parseLokiTimeToUnixNano("now-2s")
		if !ok {
			t.Fatal("expected now-2s to parse")
		}
		if gotPast > gotNow {
			t.Fatalf("expected now-2s <= now, got past=%d now=%d", gotPast, gotNow)
		}

		gotFuture, ok := parseLokiTimeToUnixNano("now+2s")
		if !ok {
			t.Fatal("expected now+2s to parse")
		}
		if gotFuture < gotNow {
			t.Fatalf("expected now+2s >= now, got future=%d now=%d", gotFuture, gotNow)
		}
	})

	t.Run("numeric_units", func(t *testing.T) {
		const want = int64(1700000000500000000)
		cases := []string{
			"1700000000.5",        // seconds
			"1700000000500",       // milliseconds
			"1700000000500000",    // microseconds
			"1700000000500000000", // nanoseconds
		}
		for _, raw := range cases {
			got, ok := parseLokiTimeToUnixNano(raw)
			if !ok {
				t.Fatalf("expected %q to parse", raw)
			}
			if got != want {
				t.Fatalf("unexpected normalization for %q: got=%d want=%d", raw, got, want)
			}
		}
	})

	t.Run("invalid", func(t *testing.T) {
		if _, ok := parseLokiTimeToUnixNano("definitely-not-a-time"); ok {
			t.Fatal("expected invalid time parse to fail")
		}
	})

	if got := normalizeLokiIntTimeToUnixNano(-1700000000); got != -1700000000000000000 {
		t.Fatalf("unexpected int normalization: got=%d", got)
	}
	if got := normalizeLokiNumericTimeToUnixNano(1700000000.5); got != 1700000000500000000 {
		t.Fatalf("unexpected float normalization: got=%d", got)
	}
}

func TestQueryRangeWindow_ParseLokiTimeRange(t *testing.T) {
	start, end, ok := parseLokiTimeRangeToUnixNano("1700000000", "1700003600")
	if !ok {
		t.Fatal("expected valid range to parse")
	}
	if end <= start {
		t.Fatalf("expected end > start, got start=%d end=%d", start, end)
	}

	if _, _, ok := parseLokiTimeRangeToUnixNano("invalid", "1700003600"); ok {
		t.Fatal("expected invalid start to fail parsing")
	}
	if _, _, ok := parseLokiTimeRangeToUnixNano("1700003600", "1700000000"); ok {
		t.Fatal("expected end < start to fail parsing")
	}
}

func TestQueryRangeWindow_SplitAndTTLHelpers(t *testing.T) {
	start := int64(0)
	end := int64(3*time.Hour - 1)

	forward := splitQueryRangeWindows(start, end, time.Hour, "forward")
	if len(forward) != 3 {
		t.Fatalf("expected 3 forward windows, got %d", len(forward))
	}
	if forward[0].startNs != start || forward[0].endNs != int64(time.Hour)-1 {
		t.Fatalf("unexpected first forward window: %+v", forward[0])
	}

	backward := splitQueryRangeWindows(start, end, time.Hour, "backward")
	if len(backward) != 3 {
		t.Fatalf("expected 3 backward windows, got %d", len(backward))
	}
	if backward[0].startNs != int64(2*time.Hour) {
		t.Fatalf("unexpected first backward window: %+v", backward[0])
	}

	if got := splitQueryRangeWindows(10, 1, time.Hour, "forward"); got != nil {
		t.Fatalf("expected nil for end < start, got %+v", got)
	}
	if got := splitQueryRangeWindows(0, 10, 0, "forward"); got != nil {
		t.Fatalf("expected nil for zero interval, got %+v", got)
	}

	p := &Proxy{
		queryRangeFreshness:       time.Hour,
		queryRangeRecentCacheTTL:  2 * time.Minute,
		queryRangeHistoryCacheTTL: 24 * time.Hour,
	}
	oldWindowEnd := time.Now().Add(-2 * time.Hour).UnixNano()
	recentWindowEnd := time.Now().Add(-10 * time.Minute).UnixNano()

	if got := p.queryRangeWindowTTL(oldWindowEnd); got != 24*time.Hour {
		t.Fatalf("expected history ttl, got %s", got)
	}
	if got := p.queryRangeWindowTTL(recentWindowEnd); got != 2*time.Minute {
		t.Fatalf("expected recent ttl, got %s", got)
	}
}

func TestQueryRangeWindow_NoTailWindowOnExactMultiple(t *testing.T) {
	// When the query duration is an exact multiple of the split interval, the
	// loop must NOT produce a 1-ns [endNs, endNs] tail window.
	for _, tc := range []struct {
		name     string
		duration time.Duration
	}{
		{"1h/1h", time.Hour},
		{"2h/1h", 2 * time.Hour},
		{"3h/1h", 3 * time.Hour},
	} {
		startNs := int64(0)
		endNs := tc.duration.Nanoseconds()
		windows := splitQueryRangeWindows(startNs, endNs, time.Hour, "forward")
		expected := int(tc.duration / time.Hour)
		if len(windows) != expected {
			t.Errorf("%s: got %d windows, want %d", tc.name, len(windows), expected)
			for i, w := range windows {
				t.Logf("  window[%d]: [%d, %d] (len=%d)", i, w.startNs, w.endNs, w.endNs-w.startNs+1)
			}
			continue
		}
		last := windows[len(windows)-1]
		if last.endNs != endNs {
			t.Errorf("%s: last window endNs=%d, want %d", tc.name, last.endNs, endNs)
		}
	}
}

func TestQueryRangeWindow_AdaptiveParallelHelpers(t *testing.T) {
	nonAdaptive := &Proxy{queryRangeAdaptiveParallel: false, queryRangeMaxParallel: 0}
	if got := nonAdaptive.queryRangeWindowParallelLimit(); got != 1 {
		t.Fatalf("expected non-adaptive fallback limit=1, got %d", got)
	}
	nonAdaptive.queryRangeMaxParallel = 5
	if got := nonAdaptive.queryRangeWindowParallelLimit(); got != 5 {
		t.Fatalf("expected non-adaptive configured limit=5, got %d", got)
	}

	p := &Proxy{
		queryRangeAdaptiveParallel:      true,
		queryRangeParallelMin:           2,
		queryRangeParallelMax:           6,
		queryRangeParallelCurrent:       99, // force clamp/reset path
		queryRangeLatencyTarget:         100 * time.Millisecond,
		queryRangeLatencyBackoff:        300 * time.Millisecond,
		queryRangeAdaptiveCooldown:      0,
		queryRangeErrorBackoffThreshold: 0.2,
		metrics:                         metrics.NewMetrics(),
	}

	if got := p.queryRangeWindowParallelLimit(); got != 2 {
		t.Fatalf("expected adaptive reset to min=2, got %d", got)
	}

	p.queryRangeParallelCurrent = 4
	p.observeQueryRangeWindowFetch(500*time.Millisecond, false) // backoff by latency
	if got := p.queryRangeParallelCurrent; got != 2 {
		t.Fatalf("expected adaptive backoff to 2, got %d", got)
	}

	for i := 0; i < 20; i++ {
		p.observeQueryRangeWindowFetch(10*time.Millisecond, false) // increase path
	}
	if got := p.queryRangeParallelCurrent; got <= 2 {
		t.Fatalf("expected adaptive increase above 2, got %d", got)
	}

	p.queryRangeAdaptiveCooldown = time.Hour
	p.queryRangeAdaptiveLastAdjust = time.Now()
	current := p.queryRangeParallelCurrent
	p.observeQueryRangeWindowFetch(time.Second, true)
	if got := p.queryRangeParallelCurrent; got != current {
		t.Fatalf("expected cooldown to suppress adjustments, got=%d want=%d", got, current)
	}

	// Error-driven backoff path with latency below backoff threshold.
	p2 := &Proxy{
		queryRangeAdaptiveParallel:      true,
		queryRangeParallelMin:           1,
		queryRangeParallelMax:           6,
		queryRangeParallelCurrent:       4,
		queryRangeLatencyTarget:         100 * time.Millisecond,
		queryRangeLatencyBackoff:        10 * time.Second,
		queryRangeAdaptiveCooldown:      0,
		queryRangeErrorBackoffThreshold: 0.1,
		metrics:                         metrics.NewMetrics(),
	}
	p2.observeQueryRangeWindowFetch(10*time.Millisecond, true)
	if got := p2.queryRangeParallelCurrent; got >= 4 {
		t.Fatalf("expected error-driven backoff to reduce parallelism, got %d", got)
	}
	if math.IsNaN(p2.queryRangeErrorEWMA) {
		t.Fatal("expected finite error EWMA")
	}
}

func TestQueryRangeWindow_ForceAdaptiveBackoff(t *testing.T) {
	p := &Proxy{
		queryRangeAdaptiveParallel: true,
		queryRangeParallelMin:      2,
		queryRangeParallelCurrent:  8,
		metrics:                    metrics.NewMetrics(),
	}
	before := p.queryRangeAdaptiveLastAdjust
	p.forceQueryRangeParallelBackoff()
	if got := p.queryRangeParallelCurrent; got != 4 {
		t.Fatalf("expected forced backoff to halve parallelism, got=%d", got)
	}
	if !p.queryRangeAdaptiveLastAdjust.After(before) {
		t.Fatal("expected adaptive last-adjust timestamp to update")
	}

	p.queryRangeParallelCurrent = 2
	unchanged := p.queryRangeAdaptiveLastAdjust
	p.forceQueryRangeParallelBackoff()
	if got := p.queryRangeParallelCurrent; got != 2 {
		t.Fatalf("expected no backoff at min parallelism, got=%d", got)
	}
	if !p.queryRangeAdaptiveLastAdjust.Equal(unchanged) {
		t.Fatal("expected timestamp unchanged when no backoff applied")
	}

	disabled := &Proxy{
		queryRangeAdaptiveParallel: false,
		queryRangeParallelMin:      1,
		queryRangeParallelCurrent:  4,
		metrics:                    metrics.NewMetrics(),
	}
	disabled.forceQueryRangeParallelBackoff()
	if got := disabled.queryRangeParallelCurrent; got != 4 {
		t.Fatalf("expected no change when adaptive mode disabled, got=%d", got)
	}
}

func TestQueryRangeWindow_DownstreamConnectionPressure(t *testing.T) {
	p := &Proxy{
		queryRangeAdaptiveParallel:      true,
		queryRangeParallelMin:           2,
		queryRangeParallelCurrent:       2,
		queryRangeLatencyTarget:         100 * time.Millisecond,
		queryRangeLatencyBackoff:        300 * time.Millisecond,
		queryRangeErrorBackoffThreshold: 0.2,
		queryRangeLatencyEWMA:           350 * time.Millisecond,
	}
	if !p.DownstreamConnectionPressure() {
		t.Fatal("expected latency backoff to activate downstream connection pressure")
	}

	p.queryRangeLatencyEWMA = 50 * time.Millisecond
	p.queryRangeErrorEWMA = 0.25
	if !p.DownstreamConnectionPressure() {
		t.Fatal("expected error EWMA backoff to activate downstream connection pressure")
	}

	p.queryRangeErrorEWMA = 0
	p.queryRangeLatencyEWMA = 120 * time.Millisecond
	if !p.DownstreamConnectionPressure() {
		t.Fatal("expected min-parallel latency pressure to activate downstream shedding")
	}

	p.queryRangeAdaptiveParallel = false
	if p.DownstreamConnectionPressure() {
		t.Fatal("expected disabled adaptive mode to suppress downstream pressure")
	}
}

type timeoutNetErr struct{}

func (timeoutNetErr) Error() string   { return "timeout" }
func (timeoutNetErr) Timeout() bool   { return true }
func (timeoutNetErr) Temporary() bool { return true }

func TestQueryRangeWindow_RetryHelpers(t *testing.T) {
	if shouldRetryQueryRangeWindow(nil) {
		t.Fatal("expected nil error to be non-retryable")
	}
	if shouldRetryQueryRangeWindow(context.Canceled) {
		t.Fatal("expected canceled context to be non-retryable")
	}
	if !shouldRetryQueryRangeWindow(context.DeadlineExceeded) {
		t.Fatal("expected deadline exceeded to be retryable")
	}
	var nerr net.Error = timeoutNetErr{}
	if !shouldRetryQueryRangeWindow(nerr) {
		t.Fatal("expected timeout net error to be retryable")
	}
	if !shouldRetryQueryRangeWindow(&queryRangeWindowHTTPError{status: http.StatusBadGateway, msg: "bad gateway"}) {
		t.Fatal("expected 502 to be retryable")
	}
	if !shouldRetryQueryRangeWindow(errors.New("all the 1 backends for the user \"\" are unavailable for proxying the request")) {
		t.Fatal("expected backend unavailable message to be retryable")
	}
	if shouldRetryQueryRangeWindow(errors.New("validation error")) {
		t.Fatal("expected generic validation error to be non-retryable")
	}

	if got := statusFromQueryRangeWindowErr(&queryRangeWindowHTTPError{status: http.StatusServiceUnavailable, msg: "down"}); got != http.StatusServiceUnavailable {
		t.Fatalf("expected http status passthrough, got=%d", got)
	}

	if got := queryRangeWindowRetryBackoff(0); got != queryRangeRetryMinBackoff {
		t.Fatalf("expected attempt<=0 backoff to min, got=%s", got)
	}
	if got := queryRangeWindowRetryBackoff(2); got != 2*queryRangeRetryMinBackoff {
		t.Fatalf("expected second attempt backoff to double min, got=%s", got)
	}
	if got := queryRangeWindowRetryBackoff(30); got != queryRangeRetryMaxBackoff {
		t.Fatalf("expected capped max backoff, got=%s", got)
	}
}

func TestQueryRangePrefilterQuery(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "selector only",
			input: `{app="nginx"}`,
			want:  `{app="nginx"}`,
		},
		{
			name:  "strip pipeline for cheap prefilter",
			input: `{app="nginx"} | json | logfmt | drop __error__`,
			want:  `{app="nginx"}`,
		},
		{
			name:  "keep malformed prefix",
			input: `| json`,
			want:  `| json`,
		},
		{
			name:  "empty",
			input: ``,
			want:  ``,
		},
		// Regression: the prefilter must pass through translated VL field-filter
		// queries unchanged. Specifically, queries built from `{app="json-test"}`
		// and `{level="warn"}` translate to `app:=json-test` and `level:=warn`
		// respectively. The prefilter must NOT mangle them into the buggy
		// double-quoted form `"app="json-test""` / `"level="warn""` that was
		// observed against e2e-proxy-underscore.
		{
			name:  "translated app field filter passes through",
			input: `app:=json-test`,
			want:  `app:=json-test`,
		},
		{
			name:  "translated level field filter passes through",
			input: `level:=warn`,
			want:  `level:=warn`,
		},
		{
			name:  "translated field filter with pipeline strips pipe",
			input: `app:=json-test | unpack_json`,
			want:  `app:=json-test`,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := queryRangePrefilterQuery(tc.input); got != tc.want {
				t.Fatalf("queryRangePrefilterQuery(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
func newWindowingTestProxy(t *testing.T, backendURL string) *Proxy {
	t.Helper()
	c := cache.New(60*time.Second, 10000)
	p, err := New(Config{
		BackendURL:                      backendURL,
		Cache:                           c,
		LogLevel:                        "error",
		QueryRangeWindowingEnabled:      true,
		QueryRangeSplitInterval:         time.Hour,
		QueryRangeMaxParallel:           2,
		QueryRangeAdaptiveParallel:      false,
		QueryRangeParallelMin:           1,
		QueryRangeParallelMax:           2,
		QueryRangeLatencyTarget:         1500 * time.Millisecond,
		QueryRangeLatencyBackoff:        3 * time.Second,
		QueryRangeAdaptiveCooldown:      30 * time.Second,
		QueryRangeErrorBackoffThreshold: 0.02,
		QueryRangeFreshness:             10 * time.Minute,
		QueryRangeRecentCacheTTL:        0,
		QueryRangeHistoryCacheTTL:       24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

func newNonOwnerPeerCacheForKey(t *testing.T, ownerHost, key string) *cache.PeerCache {
	t.Helper()
	for i := 0; i < 1024; i++ {
		candidate := cache.NewPeerCache(cache.PeerConfig{
			SelfAddr:           fmt.Sprintf("self-%d:3100", i),
			DiscoveryType:      "static",
			StaticPeers:        fmt.Sprintf("%s,remote-a:3100,remote-b:3100,%s", fmt.Sprintf("self-%d:3100", i), ownerHost),
			Timeout:            50 * time.Millisecond,
			WriteThrough:       true,
			WriteThroughMinTTL: 5 * time.Second,
		})
		if !candidate.IsOwner(key) {
			return candidate
		}
		candidate.Close()
	}
	t.Fatalf("expected non-owner peer cache setup for key %q", key)
	return nil
}

func assertQueryRangeFirstTimestamp(t *testing.T, rec *httptest.ResponseRecorder, expectedTs int64) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to parse response: %v body=%s", err, rec.Body.String())
	}
	data, _ := parsed["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	if len(result) == 0 {
		t.Fatalf("expected non-empty result")
	}
	streamObj, _ := result[0].(map[string]interface{})
	values, _ := streamObj["values"].([]interface{})
	if len(values) == 0 {
		t.Fatalf("expected non-empty stream values")
	}
	first, _ := values[0].([]interface{})
	if len(first) < 1 {
		t.Fatalf("expected tuple in values[0], got %#v", values[0])
	}
	tsStr, _ := first[0].(string)
	tsInt, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		t.Fatalf("expected numeric timestamp, got %q", tsStr)
	}
	if tsInt != expectedTs {
		t.Fatalf("unexpected first timestamp: got=%d want=%d", tsInt, expectedTs)
	}
}

func collectMetricsText(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler(w, r)
	return w.Body.String()
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// TestQueryRange_DoesNotEmitDoubleQuotedSelectorToVL is the end-to-end regression
// guard for the production bug observed against e2e-proxy-underscore. The proxy
// MUST translate `{app="json-test"}` to the VL field filter `app:=json-test`
// (and `{level="warn"}` to `level:=warn`) before sending the query to VL —
// never the malformed phrase-filter form `"app="json-test""` / `"level="warn""`
// that VL rejected with parse errors on /select/logsql/hits and
// /select/logsql/query.
//
// This test wires up a fake VL backend that records the `query` parameter on
// every backend call (covering both the windowed-batch fetches against
// /select/logsql/query and the prefilter probes against /select/logsql/hits)
// and asserts that no recorded query is the buggy double-quoted form. It runs
// against an underscore-style proxy with hybrid metadata mode, matching the
// exact configuration of the proxy that emitted the bad queries.
func TestQueryRange_DoesNotEmitDoubleQuotedSelectorToVL(t *testing.T) {
	cases := []struct {
		name         string
		logqlQuery   string
		wantVLQuery  string // VL field filter we expect to see
		bannedSubstr string // the buggy form that must never appear
	}{
		{
			name:         "app matcher",
			logqlQuery:   `{app="json-test"}`,
			wantVLQuery:  `app:=json-test`,
			bannedSubstr: `"app="json-test""`,
		},
		{
			name:         "level matcher",
			logqlQuery:   `{level="warn"}`,
			wantVLQuery:  `level:=warn`,
			bannedSubstr: `"level="warn""`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var (
				mu               sync.Mutex
				recordedQueries  []string
				recordedPathsAll []string
			)

			vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				q := r.Form.Get("query")
				mu.Lock()
				recordedQueries = append(recordedQueries, q)
				recordedPathsAll = append(recordedPathsAll, r.URL.Path)
				mu.Unlock()

				switch r.URL.Path {
				case "/select/logsql/hits":
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprint(w, `{"hits":[{"timestamps":[],"values":[1]}]}`)
				case "/select/logsql/query":
					w.Header().Set("Content-Type", "application/x-ndjson")
					_, _ = fmt.Fprint(w, `{"_time":"2026-04-10T10:00:00Z","_msg":"hello","_stream":"{app=\"json-test\"}"}`+"\n")
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer vlBackend.Close()

			p, err := New(Config{
				BackendURL:                      vlBackend.URL,
				Cache:                           cache.New(60*time.Second, 1000),
				LogLevel:                        "error",
				LabelStyle:                      LabelStyleUnderscores,
				MetadataFieldMode:               MetadataFieldModeHybrid,
				EmitStructuredMetadata:          true,
				QueryRangeWindowingEnabled:      true,
				QueryRangeSplitInterval:         time.Hour,
				QueryRangeMaxParallel:           4,
				QueryRangeAdaptiveParallel:      false,
				QueryRangeParallelMin:           1,
				QueryRangeParallelMax:           4,
				QueryRangeLatencyTarget:         1500 * time.Millisecond,
				QueryRangeLatencyBackoff:        3 * time.Second,
				QueryRangeAdaptiveCooldown:      30 * time.Second,
				QueryRangeErrorBackoffThreshold: 0.02,
				QueryRangeFreshness:             10 * time.Minute,
				QueryRangeRecentCacheTTL:        0,
				QueryRangeHistoryCacheTTL:       24 * time.Hour,
			})
			if err != nil {
				t.Fatalf("failed to create proxy: %v", err)
			}

			// Use a multi-hour range so windowing splits into 8+ windows and
			// the prefilter (queryRangePrefilterMinWindows=8 default) actually
			// runs — the production bug surfaced via the prefilter call to
			// /select/logsql/hits.
			start := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
			end := start + int64(12*time.Hour) - 1

			req := httptest.NewRequest(
				http.MethodGet,
				fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=5",
					url.QueryEscape(tc.logqlQuery), start, end),
				nil,
			)
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
			}

			mu.Lock()
			defer mu.Unlock()

			if len(recordedQueries) == 0 {
				t.Fatal("expected at least one VL backend call, got none")
			}

			sawExpected := false
			for i, q := range recordedQueries {
				path := recordedPathsAll[i]
				// The query may have a trailing pipeline (e.g., "| sort by (_time desc)")
				// for /select/logsql/query, but it must START with the expected
				// VL field filter, not the buggy double-quoted form.
				if strings.Contains(q, tc.bannedSubstr) {
					t.Fatalf("BUG: VL backend at %s received malformed query containing %q\n  full query: %q\n  expected VL field filter: %q",
						path, tc.bannedSubstr, q, tc.wantVLQuery)
				}
				if strings.Contains(q, tc.wantVLQuery) {
					sawExpected = true
				}
			}
			if !sawExpected {
				t.Fatalf("VL backend never received expected field filter %q\n  recorded queries: %#v", tc.wantVLQuery, recordedQueries)
			}
		})
	}
}
