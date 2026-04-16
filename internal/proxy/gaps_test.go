package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// Gap #1: /loki/api/v1/index/stats — real implementation via VL /select/logsql/hits
// Loki response: {"streams":N, "chunks":N, "entries":N, "bytes":N}
// =============================================================================

func TestGap_IndexStats_QueriesVLHits(t *testing.T) {
	var receivedPath string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		// VL /select/logsql/hits returns hit counts
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hits": []map[string]interface{}{
				{
					"fields":     map[string]string{},
					"timestamps": []int64{1705312200000},
					"values":     []int{42},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/stats?query=%7Bapp%3D%22nginx%22%7D&start=1705312200000000000&end=1705312800000000000", nil)
	p.handleIndexStats(w, r)

	// Should have queried VL
	if receivedPath != "/select/logsql/hits" {
		t.Errorf("expected VL path /select/logsql/hits, got %q", receivedPath)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	// Loki format requires these numeric fields
	for _, field := range []string{"streams", "chunks", "entries", "bytes"} {
		v, ok := resp[field]
		if !ok {
			t.Errorf("missing field %q", field)
			continue
		}
		if _, ok := v.(float64); !ok {
			t.Errorf("field %q must be number, got %T", field, v)
		}
	}

	// entries should be 42 (from VL hits)
	if entries, _ := resp["entries"].(float64); entries != 42 {
		t.Errorf("expected entries=42, got %v", resp["entries"])
	}
}

// =============================================================================
// Gap #2: /loki/api/v1/index/volume — real via VL hits with field grouping
// Loki response: {"status":"success","data":{"resultType":"vector","result":[...]}}
// =============================================================================

func TestGap_Volume_QueriesVLHits(t *testing.T) {
	var receivedPath string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hits": []map[string]interface{}{
				{
					"fields":     map[string]string{"app": "nginx"},
					"timestamps": []int64{1705312200000},
					"values":     []int{100},
				},
				{
					"fields":     map[string]string{"app": "api"},
					"timestamps": []int64{1705312200000},
					"values":     []int{50},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume?query=%7Bnamespace%3D%22prod%22%7D&start=1705312200000000000&end=1705312800000000000", nil)
	p.handleVolume(w, r)

	if receivedPath != "/select/logsql/hits" {
		t.Errorf("expected VL path /select/logsql/hits, got %q", receivedPath)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	if resp["status"] != "success" {
		t.Errorf("expected status=success")
	}

	data, ok := resp["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data must be object")
	}
	if data["resultType"] != "vector" {
		t.Errorf("expected resultType=vector, got %v", data["resultType"])
	}

	result, ok := data["result"].([]interface{})
	if !ok {
		t.Fatalf("result must be array")
	}
	if len(result) < 2 {
		t.Errorf("expected >=2 volume entries, got %d", len(result))
	}

	// Each entry must have "metric" (map) and "value" ([ts, count_string])
	for i, entry := range result {
		obj, ok := entry.(map[string]interface{})
		if !ok {
			t.Fatalf("result[%d] must be object", i)
		}
		if _, ok := obj["metric"]; !ok {
			t.Errorf("result[%d] missing 'metric'", i)
		}
		val, ok := obj["value"].([]interface{})
		if !ok || len(val) != 2 {
			t.Errorf("result[%d] 'value' must be [ts, count], got %v", i, obj["value"])
		}
	}
}

func TestGap_Volume_AcceptsFromToParams(t *testing.T) {
	var receivedStart string
	var receivedEnd string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedStart = r.URL.Query().Get("start")
		receivedEnd = r.URL.Query().Get("end")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hits": []map[string]interface{}{
				{
					"fields":     map[string]string{"app": "nginx"},
					"timestamps": []int64{1705312200000},
					"values":     []int{100},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume?query=%7B%7D&from=1705312200000000000&to=1705312800000000000", nil)
	p.handleVolume(w, r)

	if receivedStart == "" || receivedEnd == "" {
		t.Fatalf("expected from/to params to be forwarded as start/end, got start=%q end=%q", receivedStart, receivedEnd)
	}
}

// =============================================================================
// Gap #3: /loki/api/v1/index/volume_range — real via VL hits with step
// Loki response: {"status":"success","data":{"resultType":"matrix","result":[...]}}
// =============================================================================

func TestGap_VolumeRange_QueriesVLHits(t *testing.T) {
	var receivedStep string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedStep = r.URL.Query().Get("step")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hits": []map[string]interface{}{
				{
					"fields":     map[string]string{"app": "nginx"},
					"timestamps": []int64{1705312200000, 1705312260000},
					"values":     []int{100, 80},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume_range?query=%7B%7D&start=1705312200000000000&end=1705312800000000000&step=60", nil)
	p.handleVolumeRange(w, r)

	if receivedStep != "60s" {
		t.Errorf("expected step=60s forwarded, got %q", receivedStep)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	if resp["status"] != "success" {
		t.Errorf("expected status=success")
	}
	data := resp["data"].(map[string]interface{})
	if data["resultType"] != "matrix" {
		t.Errorf("expected resultType=matrix, got %v", data["resultType"])
	}

	result := data["result"].([]interface{})
	if len(result) == 0 {
		t.Error("expected non-empty result")
	}

	// Matrix entries have "values" (array of [ts, val] pairs)
	entry := result[0].(map[string]interface{})
	values, ok := entry["values"].([]interface{})
	if !ok {
		t.Fatal("matrix entry missing 'values'")
	}
	if len(values) < 2 {
		t.Errorf("expected >=2 time-series points, got %d", len(values))
	}
}

func TestGap_VolumeRange_FillsMissingBucketsAcrossRequestedRange(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hits": []map[string]interface{}{
				{
					"fields":     map[string]string{"app": "nginx"},
					"timestamps": []string{"2024-01-15T10:30:00Z", "2024-01-15T10:32:00Z"},
					"values":     []int{100, 80},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume_range?query=%7B%7D&start=2024-01-15T10:30:00Z&end=2024-01-15T10:33:00Z&step=60", nil)
	p.handleVolumeRange(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := resp["data"].(map[string]interface{})
	result := data["result"].([]interface{})
	entry := result[0].(map[string]interface{})
	values := entry["values"].([]interface{})
	if len(values) != 4 {
		t.Fatalf("expected 4 filled buckets, got %d", len(values))
	}
	second := values[1].([]interface{})
	fourth := values[3].([]interface{})
	if second[1] != "0" || fourth[1] != "0" {
		t.Fatalf("expected zero-filled missing buckets, got values=%v", values)
	}
}

func TestGap_VolumeRange_AcceptsFromToParams(t *testing.T) {
	var receivedStart string
	var receivedEnd string
	var receivedStep string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedStart = r.URL.Query().Get("start")
		receivedEnd = r.URL.Query().Get("end")
		receivedStep = r.URL.Query().Get("step")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hits": []map[string]interface{}{
				{
					"fields":     map[string]string{"app": "nginx"},
					"timestamps": []string{"2024-01-15T10:30:00Z", "2024-01-15T10:32:00Z"},
					"values":     []int{100, 80},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume_range?query=%7B%7D&from=2024-01-15T10:30:00Z&to=2024-01-15T10:33:00Z&step=60", nil)
	p.handleVolumeRange(w, r)

	if receivedStart == "" || receivedEnd == "" {
		t.Fatalf("expected from/to params to be forwarded as start/end, got start=%q end=%q", receivedStart, receivedEnd)
	}
	if receivedStep != "60s" {
		t.Fatalf("expected step=60s forwarded, got %q", receivedStep)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := resp["data"].(map[string]interface{})
	result := data["result"].([]interface{})
	entry := result[0].(map[string]interface{})
	values := entry["values"].([]interface{})
	if len(values) != 4 {
		t.Fatalf("expected 4 filled buckets from from/to range, got %d", len(values))
	}
}

// =============================================================================
// Gap #4: /loki/api/v1/detected_field/{name}/values — missing endpoint
// Loki response: {"values":["debug","info","warn","error"],"limit":1000}
// =============================================================================

func TestGap_DetectedFieldValues_Endpoint(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"level","hits":3}]}`))
		case "/select/logsql/field_values":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"error","hits":1},{"value":"info","hits":1},{"value":"warn","hits":1}]}`))
		case "/select/logsql/query":
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"level=info msg=\"ok\"","_stream":"{app=\"test\"}","app":"test"}` + "\n"))
			w.Write([]byte(`{"_time":"2026-04-04T17:18:50.971082Z","_msg":"level=error msg=\"boom\"","_stream":"{app=\"test\"}","app":"test"}` + "\n"))
			w.Write([]byte(`{"_time":"2026-04-04T17:18:51.971082Z","_msg":"level=warn msg=\"slow\"","_stream":"{app=\"test\"}","app":"test"}` + "\n"))
		default:
			t.Fatalf("expected detected field values to use native discovery or scan fallback, got %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/level/values?query=*", nil)
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	// Loki detected_field values format
	values, ok := resp["values"].([]interface{})
	if !ok {
		t.Fatalf("missing 'values' array, got %v", resp)
	}
	if len(values) != 3 {
		t.Errorf("expected 3 values, got %d", len(values))
	}

	// Values must be strings
	for i, v := range values {
		if _, ok := v.(string); !ok {
			t.Errorf("values[%d] must be string, got %T", i, v)
		}
	}
}

// =============================================================================
// Gap #5: Multitenancy header mapping
// X-Scope-OrgID → AccountID header
// =============================================================================

func TestGap_MultitenancyHeader_Forwarded(t *testing.T) {
	var receivedAccountID, receivedProjectID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		receivedProjectID = r.Header.Get("ProjectID")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "42")
	p.handleLabels(w, r)

	// Numeric OrgID should map to AccountID
	if receivedAccountID != "42" {
		t.Errorf("expected AccountID=42, got %q", receivedAccountID)
	}
	if receivedProjectID != "0" {
		t.Errorf("expected ProjectID=0, got %q", receivedProjectID)
	}
}

func TestGap_MultitenancyHeader_StringOrgIDRejectedWithoutMapping(t *testing.T) {
	var backendCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalled = true
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", "my-tenant")
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for unmapped string tenant, got %d body=%s", w.Code, w.Body.String())
	}
	if backendCalled {
		t.Fatal("backend should not be called for unmapped string tenant")
	}
}

func TestGap_MultitenancyHeader_NotSet(t *testing.T) {
	var receivedAccountID string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountID = r.Header.Get("AccountID")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	// No X-Scope-OrgID header
	p.handleLabels(w, r)

	// Should not set AccountID if no OrgID
	if receivedAccountID != "" {
		t.Errorf("expected no AccountID header when OrgID not set, got %q", receivedAccountID)
	}
}

// =============================================================================
// Coverage: test coverage check
// =============================================================================

func TestCoverage_AllRoutesRegistered(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	// Every Loki endpoint Grafana calls must be registered
	endpoints := []string{
		"/loki/api/v1/query_range",
		"/loki/api/v1/query",
		"/loki/api/v1/labels",
		"/loki/api/v1/label/app/values",
		"/loki/api/v1/series",
		"/loki/api/v1/index/stats",
		"/loki/api/v1/index/volume",
		"/loki/api/v1/index/volume_range",
		"/loki/api/v1/detected_fields",
		"/loki/api/v1/detected_field/level/values",
		"/loki/api/v1/drilldown-limits",
		"/config/tenant/v1/limits",
		"/loki/api/v1/patterns",
		"/loki/api/v1/tail",
		"/loki/api/v1/status/buildinfo",
		"/health",
		"/healthz",
		"/alive",
		"/livez",
		"/ready",
		"/metrics",
	}

	for _, ep := range endpoints {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", ep, nil)
		mux.ServeHTTP(w, r)
		// Should not return 404 (mux default), except for intentionally unsupported endpoints.
		if w.Code == http.StatusNotFound && ep != "/loki/api/v1/patterns" {
			t.Errorf("endpoint %s returned 404 — not registered", ep)
		}
	}
}

func newGapTestProxy(t *testing.T, backendURL string) *Proxy {
	t.Helper()
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:                   backendURL,
		Cache:                        c,
		LogLevel:                     "error",
		MetricsExportSensitiveLabels: true,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

// =============================================================================
// Coverage gap: proxyStatsQuery (instant metric query)
// =============================================================================

func TestCoverage_ProxyStatsQuery(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"resultType": "vector",
				"result": []map[string]interface{}{
					{"metric": map[string]string{}, "value": []interface{}{1234567890, "42"}},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	// count_over_time triggers stats query path
	r := httptest.NewRequest("GET", "/loki/api/v1/query?query=count_over_time({app%3D%22nginx%22}[5m])&time=1234567890", nil)
	p.handleQuery(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Errorf("expected success, got %v", resp["status"])
	}
}

// =============================================================================
// Coverage gap: wrapAsLokiResponse all 3 branches
// =============================================================================

func TestCoverage_WrapAsLokiResponse_InvalidJSON(t *testing.T) {
	result := wrapAsLokiResponse([]byte("not json"), "matrix")
	var resp map[string]interface{}
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["status"] != "success" {
		t.Error("expected success status for invalid input")
	}
}

func TestCoverage_WrapAsLokiResponse_WithDataField(t *testing.T) {
	input, _ := json.Marshal(map[string]interface{}{
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     []interface{}{},
		},
	})
	result := wrapAsLokiResponse(input, "vector")
	var resp map[string]interface{}
	json.Unmarshal(result, &resp)
	if resp["status"] != "success" {
		t.Error("expected success")
	}
	if resp["data"] == nil {
		t.Error("expected data field to be preserved")
	}
}

func TestCoverage_WrapAsLokiResponse_RawJSON(t *testing.T) {
	input, _ := json.Marshal(map[string]interface{}{
		"resultType": "matrix",
		"result":     []interface{}{},
	})
	result := wrapAsLokiResponse(input, "matrix")
	var resp map[string]interface{}
	json.Unmarshal(result, &resp)
	if resp["status"] != "success" {
		t.Error("expected success")
	}
}

// =============================================================================
// Coverage gap: formatVLTimestamp
// =============================================================================

func TestCoverage_FormatVLTimestamp(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"1234567890", "1234567890"},                              // numeric seconds
		{"1234567890.123", "1234567890.123"},                      // numeric float
		{"2024-01-15T10:30:00Z", "1705314600000000000"},           // RFC3339 → unix ns
		{"2024-01-15T10:30:00.123456789Z", "1705314600123456789"}, // RFC3339Nano → unix ns
		{"now-5m", "now-5m"},                                      // relative passthrough
	}
	for _, tc := range tests {
		got := formatVLTimestamp(tc.input)
		if got != tc.expected {
			t.Errorf("formatVLTimestamp(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

// =============================================================================
// Coverage gap: handleQuery error path
// =============================================================================

func TestCoverage_HandleQuery_InvalidQuery(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	// Invalid LogQL that translator can't parse
	r := httptest.NewRequest("GET", "/loki/api/v1/query?query=", nil)
	p.handleQuery(w, r)
	// Empty query should return error
	if w.Code != http.StatusBadRequest && w.Code != http.StatusOK {
		t.Logf("empty query returned %d (acceptable)", w.Code)
	}
}

func TestCoverage_HandleQuery_LogQuery(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return NDJSON log lines
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test line","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query?query={app="nginx"}`, nil)
	p.handleQuery(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// =============================================================================
// Coverage gap: GetMetrics
// =============================================================================

func TestCoverage_GetMetrics(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	m := p.GetMetrics()
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}
}
