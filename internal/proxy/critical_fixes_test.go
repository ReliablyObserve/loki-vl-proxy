package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Ensure imports are used
var (
	_ = url.Values{}
	_ = strings.Contains
)


// =============================================================================
// CRITICAL: Data race — concurrent reads + SIGHUP reload of tenantMap
// =============================================================================

func TestCritical_TenantMap_ConcurrentReloadAndRead(t *testing.T) {
	var requestCount atomic.Int64
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Return tenant info in response for verification
		json.NewEncoder(w).Encode(map[string]interface{}{
			"account": r.Header.Get("AccountID"),
			"project": r.Header.Get("ProjectID"),
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"team-a": {AccountID: "100", ProjectID: "1"},
		},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	// Simulate concurrent reads + SIGHUP reload — should not panic with -race
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer goroutine: reload tenant map repeatedly
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			p.ReloadTenantMap(map[string]TenantMapping{
				"team-a": {AccountID: fmt.Sprintf("%d", 100+i), ProjectID: "1"},
				"team-b": {AccountID: "200", ProjectID: "2"},
			})
		}
	}()

	// Reader goroutines: concurrent requests
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				select {
				case <-stop:
					return
				default:
				}
				w := httptest.NewRecorder()
				r := httptest.NewRequest("GET", `/loki/api/v1/labels`, nil)
				r.Header.Set("X-Scope-OrgID", "team-a")
				p.handleLabels(w, r)
			}
		}()
	}

	// Run for a short time then stop
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	if requestCount.Load() == 0 {
		t.Error("expected some requests to reach backend")
	}
}

// =============================================================================
// CRITICAL: Binary operators — applyOp must handle %, ^, comparisons
// =============================================================================

func TestCritical_ApplyOp_AllOperators(t *testing.T) {
	tests := []struct {
		a, b float64
		op   string
		want float64
	}{
		// Existing operators
		{10, 3, "/", 10.0 / 3.0},
		{10, 0, "/", 0}, // division by zero
		{6, 7, "*", 42},
		{10, 3, "+", 13},
		{10, 3, "-", 7},

		// Missing operators that must be added
		{10, 3, "%", 1},         // modulo
		{2, 10, "^", 1024},      // power
		{42, 42, "==", 1},       // comparison: equal → 1
		{42, 43, "==", 0},       // comparison: not equal → 0
		{42, 43, "!=", 1},       // not equal
		{42, 42, "!=", 0},       // equal → 0
		{10, 5, ">", 1},         // greater
		{5, 10, ">", 0},         // not greater
		{5, 5, ">", 0},          // equal not greater
		{5, 10, "<", 1},         // less
		{10, 5, "<", 0},         // not less
		{10, 5, ">=", 1},        // greater or equal
		{5, 5, ">=", 1},         // equal
		{4, 5, ">=", 0},         // less
		{5, 10, "<=", 1},        // less or equal
		{5, 5, "<=", 1},         // equal
		{10, 5, "<=", 0},        // greater
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%.0f %s %.0f", tt.a, tt.op, tt.b), func(t *testing.T) {
			got := applyOp(tt.a, tt.b, tt.op)
			if math.Abs(got-tt.want) > 1e-9 {
				t.Errorf("applyOp(%.1f, %.1f, %q) = %.1f, want %.1f", tt.a, tt.b, tt.op, got, tt.want)
			}
		})
	}
}

// =============================================================================
// CRITICAL: Binary metric query end-to-end with modulo operator
// =============================================================================

func TestCritical_BinaryMetricQuery_Modulo(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"metric": map[string]string{},
					"values": [][]interface{}{{1234567890.0, "10"}},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	// rate({app="nginx"}[5m]) % 3 — modulo with scalar
	q := url.Values{
		"query": {`rate({app="nginx"}[5m]) % 3`},
		"start": {"1234567890"},
		"end":   {"1234567900"},
		"step":  {"1"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	p.handleQueryRange(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Errorf("expected success, got %v", resp["status"])
	}
}

// =============================================================================
// CRITICAL: Delete API endpoint — safeguards
// =============================================================================

func TestCritical_DeleteEndpoint_RequiresConfirmationHeader(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/loki/api/v1/delete?query={app=\"nginx\"}&start=1234567890&end=1234567900", nil)
	// No X-Delete-Confirmation header
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 without confirmation header, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCritical_DeleteEndpoint_RequiresQuery(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/loki/api/v1/delete?start=1234567890&end=1234567900", nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 without query, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCritical_DeleteEndpoint_RequiresTimeRange(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", `/loki/api/v1/delete?query={app="nginx"}`, nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 without time range, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCritical_DeleteEndpoint_RejectsWildcardQuery(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", `/loki/api/v1/delete?query={}&start=1234567890&end=1234567900`, nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for wildcard query, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCritical_DeleteEndpoint_RejectsTooWideTimeRange(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	// 31 days — exceeds 30-day safety limit
	start := time.Now().Add(-31 * 24 * time.Hour).Unix()
	end := time.Now().Unix()
	r := httptest.NewRequest("POST",
		fmt.Sprintf(`/loki/api/v1/delete?query={app="nginx"}&start=%d&end=%d`, start, end), nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for >30 day range, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCritical_DeleteEndpoint_ScopesToTenant(t *testing.T) {
	var receivedAccountID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		TenantMap: map[string]TenantMapping{
			"team-a": {AccountID: "100", ProjectID: "1"},
		},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	start := time.Now().Add(-1 * time.Hour).Unix()
	end := time.Now().Unix()
	r := httptest.NewRequest("POST",
		fmt.Sprintf(`/loki/api/v1/delete?query={app="nginx"}&start=%d&end=%d`, start, end), nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	r.Header.Set("X-Scope-OrgID", "team-a")
	mux.ServeHTTP(w, r)

	if receivedAccountID != "100" {
		t.Errorf("expected AccountID=100, got %q", receivedAccountID)
	}
}

func TestCritical_DeleteEndpoint_SuccessfulDelete(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	start := time.Now().Add(-1 * time.Hour).Unix()
	end := time.Now().Unix()
	r := httptest.NewRequest("POST",
		fmt.Sprintf(`/loki/api/v1/delete?query={app="nginx"}&start=%d&end=%d`, start, end), nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d: %s", w.Code, w.Body.String())
	}

	if receivedQuery == "" {
		t.Error("expected translated query to be forwarded to VL")
	}
}

// =============================================================================
// P1: without() clause should return clear error
// =============================================================================

func TestP1_WithoutClause_Supported(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]interface{}{
				{"metric": map[string]string{"app": "nginx"}, "values": [][]interface{}{{1234567890.0, "42"}}},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`sum without (app) (rate({job="nginx"}[5m]))`},
		"start": {"1"},
		"end":   {"2"},
		"step":  {"1"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	p.handleQueryRange(w, r)

	// without() is now supported — should not return error
	if w.Code >= 400 {
		t.Errorf("without() should be supported, got status %d: %s", w.Code, w.Body.String())
	}
}

// =============================================================================
// P2: targetLabels forwarded on volume_range
// =============================================================================

func TestP2_VolumeRange_ForwardsTargetLabels(t *testing.T) {
	var receivedField string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedField = r.URL.Query().Get("field")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hits": []interface{}{},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET",
		`/loki/api/v1/index/volume_range?query={app="nginx"}&start=1&end=2&step=60&targetLabels=namespace`, nil)
	p.handleVolumeRange(w, r)

	if receivedField != "namespace" {
		t.Errorf("expected field=namespace forwarded, got %q", receivedField)
	}
}

// =============================================================================
// P2: IsScalar must handle negatives and scientific notation
// =============================================================================

func TestP2_ParseScalar_NegativeAndScientific(t *testing.T) {
	tests := []struct {
		input string
		want  float64
	}{
		{"42", 42},
		{"3.14", 3.14},
		{"-1", -1},
		{"1e5", 1e5},
		{"0", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseScalar(tt.input)
			if math.Abs(got-tt.want) > 1e-9 {
				t.Errorf("parseScalar(%q) = %f, want %f", tt.input, got, tt.want)
			}
		})
	}
}

// =============================================================================
// Metrics: CB half_open state registers as gauge value 2
// =============================================================================

func TestMetrics_CBHalfOpen_GaugeValue(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	p.Init()

	// Force circuit breaker into half-open: trip it, then wait for open duration to expire
	// The CB defaults: failThreshold=5, openDuration=10s — too slow for tests.
	// Instead, test the metrics handler directly with a mock state func.
	m := p.GetMetrics()
	m.SetCircuitBreakerFunc(func() string { return "half_open" })

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "loki_vl_proxy_circuit_breaker_state 2") {
		t.Errorf("expected CB state gauge=2 for half_open, body:\n%s", body)
	}
}

// =============================================================================
// Metrics: actual status code recorded for non-query endpoints
// =============================================================================

func TestMetrics_StatusCode_RecordedCorrectly(t *testing.T) {
	// Backend returns 502 for all requests
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`{"error":"backend down"}`))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET",
		`/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&step=1`, nil)
	p.handleQueryRange(w, r)

	// The proxy should NOT return 200 when backend returns 502
	if w.Code == http.StatusOK {
		t.Errorf("proxy returned 200 even though backend returned 502")
	}
}

// =============================================================================
// Streaming: statusCapture preserves Flusher/Hijacker interfaces
// =============================================================================

func TestStatusCapture_Flush(t *testing.T) {
	recorder := httptest.NewRecorder()
	sc := &statusCapture{ResponseWriter: recorder, code: 200}
	// Should not panic
	sc.Flush()
}

func TestStatusCapture_WriteHeader(t *testing.T) {
	recorder := httptest.NewRecorder()
	sc := &statusCapture{ResponseWriter: recorder, code: 200}
	sc.WriteHeader(http.StatusNotFound)
	if sc.code != 404 {
		t.Errorf("expected captured code 404, got %d", sc.code)
	}
}

// =============================================================================
// Delete endpoint: only POST method allowed
// =============================================================================

func TestCritical_DeleteEndpoint_OnlyPOST(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	// GET should be rejected
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/delete?query={app="nginx"}&start=1&end=2`, nil)
	r.Header.Set("X-Delete-Confirmation", "true")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET on delete, got %d", w.Code)
	}
}

// =============================================================================
// Write endpoint still blocked (regression test)
// =============================================================================

func TestRegression_PushStillBlocked(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"streams":[]}`)
	r := httptest.NewRequest("POST", "/loki/api/v1/push", body)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for push, got %d", w.Code)
	}
}

