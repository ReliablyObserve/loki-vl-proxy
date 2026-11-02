package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestPatternWindowCacheIdentityIsolatedByScopeAndMiningSettings(t *testing.T) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), ForwardHeaders: []string{"X-Auth-Token"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req.Header.Set("X-Auth-Token", "secret-a")
	key := func(r *http.Request) string {
		return p.patternWindowCacheKey(p.withRequestScope(r), `{app="web"}`, "30s", 50, 200, 100, 200)
	}
	base := key(req)
	if !strings.HasPrefix(base, "patterns_window:") {
		t.Fatalf("pattern source window must use disk-eligible cache namespace, got %q", base)
	}
	if equivalentStep := p.patternWindowCacheKey(p.withRequestScope(req), `{app="web"}`, "30000ms", 50, 200, 100, 200); equivalentStep != base {
		t.Error("equivalent Grafana step spelling missed the same pattern window")
	}
	for name, mutate := range map[string]func(*http.Request){
		"tenant": func(r *http.Request) { r.Header.Set("X-Scope-OrgID", "tenant-b") },
		"auth":   func(r *http.Request) { r.Header.Set("X-Auth-Token", "secret-b") },
	} {
		changed := req.Clone(req.Context())
		changed.Header = req.Header.Clone()
		mutate(changed)
		if got := key(changed); got == base {
			t.Errorf("%s reused another scope's pattern window", name)
		}
	}
	p.backendDefaultMsgValue = "backend-placeholder"
	if got := key(req); got == base {
		t.Error("different backend message interpretation reused a pattern window")
	}
}

func TestPatternsPriorQueryAutodetectionAnswersExactRangeWithoutVL(t *testing.T) {
	var backendCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	p := newPatternPersistenceProxy(t, backend.URL, filepath.Join(t.TempDir(), "patterns.json"), true)
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	req := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1710000000&end=1710000060&step=60s`, nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	scoped := p.withRequestScope(req)
	p.maybeAutodetectPatternsFromWindowEntries(
		"tenant-a", p.fingerprintFromCtx(scoped.Context(), scoped), `{app="web"}`, "1710000000", "1710000060", "60s",
		[]queryRangeWindowEntry{{Stream: map[string]string{"detected_level": "info"}, Ts: "1710000030000000000", Msg: "api request completed successfully from worker"}},
	)
	rec := httptest.NewRecorder()
	p.handlePatterns(rec, req)
	if rec.Code != http.StatusOK || patternCountFromPayload(rec.Body.Bytes()) == 0 || backendCalls.Load() != 0 {
		t.Fatalf("same-scope prior query did not warm patterns: status=%d patterns=%d VL calls=%d", rec.Code, patternCountFromPayload(rec.Body.Bytes()), backendCalls.Load())
	}
}

// A moving Drilldown range keeps most aligned source windows unchanged. The
// second answer must match a cold calculation while avoiding those VL reads.
func TestPatternsShiftedRangeReusesCompletedWindows(t *testing.T) {
	var backendCalls atomic.Int64
	start := time.Date(2026, 10, 1, 0, 0, 10, 0, time.UTC)
	end := start.Add(6 * time.Hour)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Errorf("unexpected backend path %s", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		startNs, startErr := strconv.ParseInt(r.FormValue("start"), 10, 64)
		endNs, endErr := strconv.ParseInt(r.FormValue("end"), 10, 64)
		if startErr != nil || endErr != nil {
			t.Errorf("invalid source bounds: %q %q", r.FormValue("start"), r.FormValue("end"))
			http.Error(w, "bad bounds", http.StatusBadRequest)
			return
		}
		backendCalls.Add(1)
		w.Header().Set("Content-Type", "application/x-ndjson")
		for ts := time.Unix(0, startNs).UTC().Truncate(time.Minute).Add(time.Minute); ts.UnixNano() <= endNs; ts = ts.Add(time.Minute) {
			_, _ = fmt.Fprintf(w, `{"_time":%q,"_msg":"api request completed successfully from worker","level":"info"}`+"\n", ts.Format(time.RFC3339Nano))
		}
		// Only the shifted request includes this new tail line.
		newTail := end.Add(15 * time.Second)
		if startNs <= newTail.UnixNano() && endNs >= newTail.UnixNano() {
			_, _ = fmt.Fprintf(w, `{"_time":%q,"_msg":"new tail event arrived from worker","level":"warn"}`+"\n", newTail.Format(time.RFC3339Nano))
		}
	}))
	defer backend.Close()

	request := func(p *Proxy, from, to time.Time) patternsResponse {
		t.Helper()
		url := fmt.Sprintf("/loki/api/v1/patterns?query=%%7Bapp%%3D%%22web%%22%%7D&start=%s&end=%s&step=30s", from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano))
		rec := httptest.NewRecorder()
		p.handlePatterns(rec, httptest.NewRequest(http.MethodGet, url, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("patterns status %d: %s", rec.Code, rec.Body.String())
		}
		var result patternsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode patterns: %v", err)
		}
		return result
	}

	p := newTestProxy(t, backend.URL)
	p.backendSupportsDensePatternWindowing = true
	p.queryRangeHistoryCacheTTL = 24 * time.Hour
	first := request(p, start, end)
	if len(first.Data) == 0 {
		t.Fatal("fixture did not produce patterns")
	}
	firstCalls := backendCalls.Load()
	if firstCalls < 10 {
		t.Fatalf("expected stratified first fetch, got %d VL reads", firstCalls)
	}
	scannedLines := func() int64 {
		t.Helper()
		rec := httptest.NewRecorder()
		p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		const prefix = "loki_vl_proxy_patterns_source_lines_scanned_total "
		for _, line := range strings.Split(rec.Body.String(), "\n") {
			if raw, ok := strings.CutPrefix(line, prefix); ok {
				value, err := strconv.ParseInt(raw, 10, 64)
				if err == nil {
					return value
				}
			}
		}
		t.Fatal("missing pattern source-scan counter")
		return 0
	}
	firstScanned := scannedLines()
	shifted := request(p, start.Add(30*time.Second), end.Add(30*time.Second))
	reusedCalls := backendCalls.Load() - firstCalls
	t.Logf("patterns 6h backend reads: cold=%d shifted=%d", firstCalls, reusedCalls)
	if reusedCalls > 4 {
		t.Fatalf("shifted 6h range re-read %d VL windows; expected only moving edges", reusedCalls)
	}
	if rereadLines := scannedLines() - firstScanned; rereadLines >= firstScanned/3 {
		t.Fatalf("pattern source-scan counter counted cached lines: first=%d shifted=%d", firstScanned, rereadLines)
	}
	requireProxyMetricsContain(t, p, `loki_vl_proxy_internal_operation_total{operation="patterns_window_cache",outcome="hit"}`)
	requireProxyMetricsContain(t, p, `loki_vl_proxy_cache_layer_hits_total{layer="loki",component="primary",tier="l1_memory"}`)

	cold := newTestProxy(t, backend.URL)
	cold.backendSupportsDensePatternWindowing = true
	expected := request(cold, start.Add(30*time.Second), end.Add(30*time.Second))
	gotJSON, _ := json.Marshal(shifted)
	wantJSON, _ := json.Marshal(expected)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("reused answer differs from cold calculation\nreused: %s\ncold: %s", gotJSON, wantJSON)
	}

	// The existing query-range policy forbids near-now cache reuse by default.
	// Those source windows must still be read while historical ones are reused.
	recentStart := time.Now().UTC().Add(-30*time.Minute - 30*time.Second).Truncate(30 * time.Second)
	recentEnd := recentStart.Add(30 * time.Minute)
	recent := newTestProxy(t, backend.URL)
	recent.backendSupportsDensePatternWindowing = true
	recent.queryRangeHistoryCacheTTL = 24 * time.Hour
	_ = request(recent, recentStart, recentEnd)
	recentFirstCalls := backendCalls.Load()
	_ = request(recent, recentStart.Add(30*time.Second), recentEnd.Add(30*time.Second))
	if recentReReads := backendCalls.Load() - recentFirstCalls; recentReReads <= 4 {
		t.Fatalf("near-now windows were cached despite disabled recent-window TTL; only %d VL reads", recentReReads)
	}
}

func TestPatternsShiftedRangeReusesSecondPassWindows(t *testing.T) {
	var mu sync.Mutex
	var phase atomic.Int64
	var calls [2]int
	var boosted [2][]int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
			return
		}
		limit, _ := strconv.Atoi(r.FormValue("limit"))
		startNs, _ := strconv.ParseInt(r.FormValue("start"), 10, 64)
		p := int(phase.Load())
		mu.Lock()
		calls[p]++
		if limit > 40 {
			boosted[p] = append(boosted[p], startNs)
		}
		mu.Unlock()
		ts := time.Unix(0, startNs).Add(time.Second).UTC().Format(time.RFC3339Nano)
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i := 0; i < 40; i++ {
			_, _ = fmt.Fprintf(w, `{"_time":%q,"_msg":"api request completed successfully from worker","level":"info"}`+"\n", ts)
		}
		if limit > 40 {
			_, _ = fmt.Fprintf(w, `{"_time":%q,"_msg":"audit operation completed with warning from worker","level":"warn"}`+"\n", ts)
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	p.backendSupportsDensePatternWindowing = true
	p.queryRangeHistoryCacheTTL = 24 * time.Hour
	start := time.Date(2026, 10, 1, 0, 0, 10, 0, time.UTC)
	request := func(proxy *Proxy, from time.Time) patternsResponse {
		t.Helper()
		url := fmt.Sprintf("/loki/api/v1/patterns?query=%%7Bapp%%3D%%22web%%22%%7D&start=%s&end=%s&step=30s&line_limit=40", from.Format(time.RFC3339Nano), from.Add(6*time.Hour).Format(time.RFC3339Nano))
		rec := httptest.NewRecorder()
		proxy.handlePatterns(rec, httptest.NewRequest(http.MethodGet, url, nil))
		if rec.Code != http.StatusOK || patternCountFromPayload(rec.Body.Bytes()) == 0 {
			t.Fatalf("patterns request failed: status=%d body=%s", rec.Code, rec.Body.String())
		}
		var response patternsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode patterns: %v", err)
		}
		return response
	}
	request(p, start)
	phase.Store(1)
	shifted := request(p, start.Add(30*time.Second))
	mu.Lock()
	coldCalls, shiftedCalls := calls[0], calls[1]
	boostedCold, boostedShifted := append([]int64(nil), boosted[0]...), append([]int64(nil), boosted[1]...)
	mu.Unlock()
	t.Logf("capped 6h backend reads: cold=%d shifted=%d boosted cold=%d shifted=%d", coldCalls, shiftedCalls, len(boostedCold), len(boostedShifted))
	if len(boostedCold) < 4 {
		t.Fatalf("fixture did not exercise bounded second pass: %v", boostedCold)
	}
	if shiftedCalls > 4 || len(boostedShifted) > 2 {
		t.Fatalf("shifted second pass rescanned settled windows: calls=%d boosted=%v", shiftedCalls, boostedShifted)
	}

	cold := newTestProxy(t, backend.URL)
	cold.backendSupportsDensePatternWindowing = true
	expected := request(cold, start.Add(30*time.Second))
	gotJSON, _ := json.Marshal(shifted)
	wantJSON, _ := json.Marshal(expected)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("reused second-pass answer differs from cold calculation\nreused: %s\ncold: %s", gotJSON, wantJSON)
	}
}

func TestPatternsIncompleteWindowReadIsNotCached(t *testing.T) {
	var backendCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
			return
		}
		startNs, _ := strconv.ParseInt(r.FormValue("start"), 10, 64)
		body := fmt.Sprintf(`{"_time":%q,"_msg":"api request completed successfully from worker","level":"info"}`+"\n", time.Unix(0, startNs).Add(time.Second).UTC().Format(time.RFC3339Nano))
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)+100))
		_, _ = w.Write([]byte(body))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	p.backendSupportsDensePatternWindowing = true
	p.queryRangeHistoryCacheTTL = 24 * time.Hour
	start := time.Date(2026, 10, 1, 0, 0, 10, 0, time.UTC)
	request := func(from time.Time) {
		t.Helper()
		url := fmt.Sprintf("/loki/api/v1/patterns?query=%%7Bapp%%3D%%22web%%22%%7D&start=%s&end=%s&step=30s", from.Format(time.RFC3339Nano), from.Add(6*time.Hour).Format(time.RFC3339Nano))
		rec := httptest.NewRecorder()
		p.handlePatterns(rec, httptest.NewRequest(http.MethodGet, url, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("patterns request status %d: %s", rec.Code, rec.Body.String())
		}
	}
	request(start)
	firstCalls := backendCalls.Load()
	request(start.Add(30 * time.Second))
	if shiftedCalls := backendCalls.Load() - firstCalls; shiftedCalls <= 4 {
		t.Fatalf("incomplete VL body was cached and reused after range shift: cold=%d shifted=%d", firstCalls, shiftedCalls)
	}
}
