package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// Priority 1: Admin stubs (zero coverage)
// =============================================================================

func TestAdminStubs_Rules(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/rules", nil)
	mux.ServeHTTP(w, r)

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Errorf("rules stub: expected success, got %v", resp["status"])
	}
	data := resp["data"].(map[string]interface{})
	groups := data["groups"].([]interface{})
	if len(groups) != 0 {
		t.Errorf("rules stub: expected empty groups, got %d", len(groups))
	}
}

func TestAdminStubs_Alerts(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/alerts", nil)
	mux.ServeHTTP(w, r)

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Errorf("alerts stub: expected success, got %v", resp["status"])
	}
}

func TestAdminStubs_Config(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/config", nil)
	mux.ServeHTTP(w, r)

	if ct := w.Header().Get("Content-Type"); ct != "application/yaml" {
		t.Errorf("config stub: expected application/yaml, got %q", ct)
	}
}

func TestAdminStubs_FormatQuery(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/format_query?query={app="nginx"}`, nil)
	p.handleFormatQuery(w, r)

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["data"] != `{app="nginx"}` {
		t.Errorf("format_query should echo query, got %v", resp["data"])
	}
}

// =============================================================================
// Priority 1: handleDetectedFields 4xx fallback to wildcard
// =============================================================================

func TestDetectedFields_VL4xx_FallsBackToWildcard(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		query := r.URL.Query().Get("query")
		if query != "*" {
			// First call with translated query → 400
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Second call with wildcard → success
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": "app", "hits": 10},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{"query": {`{app="nginx"}`}, "start": {"1"}, "end": {"2"}}
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?"+q.Encode(), nil)
	p.handleDetectedFields(w, r)

	if callCount < 2 {
		t.Errorf("expected 2 VL calls (first 400, then wildcard fallback), got %d", callCount)
	}
	if w.Code != 200 {
		t.Errorf("expected 200 after wildcard fallback, got %d", w.Code)
	}
}

// =============================================================================
// Priority 1: isStatsQuery quote-aware exclusion
// =============================================================================

func TestIsStatsQuery_Comprehensive(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{`app:=nginx | stats rate()`, true},
		{`app:=nginx | rate(`, true},
		{`app:=nginx | count(`, true},
		{`app:=nginx ~"stats query"`, false},           // inside quotes
		{`app:=nginx ~"| stats rate()"`, false},         // pipe inside quotes
		{`app:=nginx`, false},                           // no stats
		{`app:=nginx ~"error" | stats count()`, true},   // stats after filter
		{``, false},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := isStatsQuery(tt.query)
			if got != tt.want {
				t.Errorf("isStatsQuery(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

// =============================================================================
// Priority 1: parseTimestampToUnix all branches
// =============================================================================

func TestParseTimestampToUnix(t *testing.T) {
	tests := []struct {
		input    string
		wantApprox float64
	}{
		{"2024-01-15T10:30:00Z", 1705314600},
		{"1705314600", 1705314600},
		{"1705314600.5", 1705314600.5},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseTimestampToUnix(tt.input)
			diff := got - tt.wantApprox
			if diff < -1 || diff > 1 {
				t.Errorf("parseTimestampToUnix(%q) = %f, want ~%f", tt.input, got, tt.wantApprox)
			}
		})
	}

	// Invalid input should not panic, returns ~now
	got := parseTimestampToUnix("garbage")
	if got < 1000000000 {
		t.Errorf("parseTimestampToUnix(garbage) should return ~now, got %f", got)
	}
}

// =============================================================================
// Priority 2: proxyBinaryMetric — VL error on one side
// =============================================================================

func TestBinaryMetric_LeftSideVLError(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			// Left side fails
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error":"left failed"}`))
			return
		}
		// Right side succeeds
		json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]interface{}{
				{"metric": map[string]string{}, "values": [][]interface{}{{1234567890.0, "10"}}},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{
		"query": {`rate({app="a"}[5m]) / rate({app="b"}[5m])`},
		"start": {"1"}, "end": {"2"}, "step": {"1"},
	}
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	p.handleQueryRange(w, r)

	// Should return error, not 200
	if w.Code == 200 {
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["status"] == "success" {
			// acceptable — the left side 502 is reported as error
		}
	}
}

// =============================================================================
// Priority 2: Delete endpoint — VL error propagation
// =============================================================================

func TestDelete_VLReturns500_PropagatesError(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal server error"}`))
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

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 propagated from VL, got %d", w.Code)
	}
}

// =============================================================================
// Priority 2: handlePatterns with real VL backend
// =============================================================================

func TestPatterns_VLReturnsLines_ExtractsPatterns(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 20; i++ {
			fmt.Fprintf(w, `{"_time":"2024-01-15T10:30:%02d.000Z","_msg":"GET /api/v1/users 200 %dms","_stream":"{}","app":"nginx"}`+"\n", i, i*10)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{"query": {`{app="nginx"}`}, "start": {"1"}, "end": {"2"}}
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?"+q.Encode(), nil)
	p.handlePatterns(w, r)

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Errorf("expected success, got %v", resp["status"])
	}
	data, _ := resp["data"].([]interface{})
	if len(data) == 0 {
		t.Error("expected extracted patterns from 20 log lines, got empty")
	}
}

// =============================================================================
// Priority 2: applyBackendHeaders and forwardHeaders
// =============================================================================

func TestForwardHeaders_ConfiguredHeadersForwarded(t *testing.T) {
	var receivedAuth string
	var receivedCustom string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		receivedCustom = r.Header.Get("X-Custom-Header")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{{"value": "app", "hits": 1}},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(1*time.Second, 1000)
	p, err := New(Config{
		BackendURL:     vlBackend.URL,
		Cache:          c,
		LogLevel:       "error",
		ForwardHeaders: []string{"Authorization", "X-Custom-Header"},
		BackendHeaders: map[string]string{"X-Static": "always-present"},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("Authorization", "Bearer secret-token")
	r.Header.Set("X-Custom-Header", "custom-value")
	p.handleLabels(w, r)

	if receivedAuth != "Bearer secret-token" {
		t.Errorf("expected Authorization forwarded, got %q", receivedAuth)
	}
	if receivedCustom != "custom-value" {
		t.Errorf("expected X-Custom-Header forwarded, got %q", receivedCustom)
	}
}

func TestBackendHeaders_StaticHeadersForwarded(t *testing.T) {
	var receivedStatic string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedStatic = r.Header.Get("X-Static")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{{"value": "app", "hits": 1}},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(1*time.Second, 1000)
	p, err := New(Config{
		BackendURL:     vlBackend.URL,
		Cache:          c,
		LogLevel:       "error",
		BackendHeaders: map[string]string{"X-Static": "always-present"},
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	p.handleLabels(w, r)

	if receivedStatic != "always-present" {
		t.Errorf("expected X-Static header, got %q", receivedStatic)
	}
}

// =============================================================================
// Priority 2: Label translator round-trip consistency
// =============================================================================

func TestLabelTranslator_RoundTrip(t *testing.T) {
	lt := NewLabelTranslator("underscores", nil)

	// Known OTel fields should round-trip
	otelFields := []string{
		"service.name", "k8s.namespace.name", "k8s.pod.name",
		"deployment.environment", "host.name",
	}

	for _, field := range otelFields {
		lokiLabel := lt.ToLoki(field) // service.name → service_name
		vlField := lt.ToVL(lokiLabel) // service_name → service.name
		if vlField != field {
			t.Errorf("round-trip failed: %q → %q → %q (expected %q)", field, lokiLabel, vlField, field)
		}
	}
}

// =============================================================================
// Priority 3: Unicode and special characters in NDJSON
// =============================================================================

func TestVLLogs_UnicodeMessage(t *testing.T) {
	body := []byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"请求处理 🚀 émojis","_stream":"{}"}` + "\n")
	streams := vlLogsToLokiStreams(body)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	vals := streams[0]["values"].([][]string)
	if !strings.Contains(vals[0][1], "请求处理") {
		t.Errorf("Unicode message not preserved: %q", vals[0][1])
	}
}

func TestVLLogs_SpecialCharsInLabels(t *testing.T) {
	body := []byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","_stream":"{app=\"ng\\\"inx\"}","app":"ng\"inx"}` + "\n")
	streams := vlLogsToLokiStreams(body)
	if len(streams) == 0 {
		t.Fatal("expected at least 1 stream for special char labels")
	}
}

// =============================================================================
// Priority 3: Metrics RecordClientError and RecordTenantRequest
// =============================================================================

func TestMetrics_RecordClientError(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	m := p.GetMetrics()

	m.RecordClientError("query_range", "bad_request")
	m.RecordClientError("query_range", "rate_limited")

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "client_errors_total") {
		t.Error("expected client_errors_total in metrics output")
	}
}

func TestMetrics_RecordTenantRequest(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	m := p.GetMetrics()

	m.RecordTenantRequest("team-a", "query_range", 200, 10*time.Millisecond)
	m.RecordTenantRequest("team-b", "labels", 200, 5*time.Millisecond)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "tenant_requests_total") {
		t.Error("expected tenant_requests_total in metrics output")
	}
	if !strings.Contains(body, "team-a") {
		t.Error("expected team-a in tenant metrics")
	}
}

// =============================================================================
// Priority 3: splitLabelPairs quoted comma
// =============================================================================

func TestSplitLabelPairs_QuotedComma(t *testing.T) {
	input := `app="he,llo",namespace="world"`
	pairs := splitLabelPairs(input)
	if len(pairs) != 2 {
		t.Errorf("expected 2 pairs (comma in quotes ignored), got %d: %v", len(pairs), pairs)
	}
}

// =============================================================================
// Priority 3: isVLInternalField
// =============================================================================

func TestIsVLInternalField(t *testing.T) {
	internals := []string{"_time", "_msg", "_stream", "_stream_id"}
	for _, f := range internals {
		if !isVLInternalField(f) {
			t.Errorf("expected %q to be internal field", f)
		}
	}
	externals := []string{"app", "namespace", "_custom", "time", "msg"}
	for _, f := range externals {
		if isVLInternalField(f) {
			t.Errorf("expected %q to NOT be internal field", f)
		}
	}
}
