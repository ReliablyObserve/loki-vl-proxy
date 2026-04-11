package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

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
	endA := start + int64(3*time.Hour) - 1
	endB := start + int64(4*time.Hour) - 1

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
