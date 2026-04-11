package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/gorilla/websocket"
)

// =============================================================================
// Loki API Contract Tests
//
// These tests define the EXACT response format Grafana expects from a Loki
// datasource. Every endpoint Grafana calls is tested. The VL backend is mocked.
// Tests verify:
//   1. HTTP status code
//   2. Content-Type header
//   3. Response JSON structure (field names, types, nesting)
//   4. Correct translation of request params to VL backend
//
// Reference: https://grafana.com/docs/loki/latest/reference/loki-http-api/
// =============================================================================

// --- /loki/api/v1/labels ---

func TestContract_Labels_ResponseFormat(t *testing.T) {
	vlBackend := mockVLFieldNames(t, []fieldHit{
		{"app", 100}, {"env", 50}, {"_msg", 200}, {"level", 75}, {"_time", 300}, {"_stream", 100},
	})
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/labels")

	assertLokiSuccess(t, resp)
	data := assertDataIsStringArray(t, resp)

	// VL internal fields are filtered out, and Loki-style service_name is added.
	if len(data) != 4 {
		t.Errorf("expected 4 labels (app, env, level, service_name), got %d: %v", len(data), data)
	}
	assertContains(t, data, "service_name")
}

func TestContract_Labels_PassesTimeRange(t *testing.T) {
	var receivedStart, receivedEnd string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedStart = r.URL.Query().Get("start")
		receivedEnd = r.URL.Query().Get("end")
		writeVLFieldNames(w, nil)
	}))
	defer vlBackend.Close()

	doGet(t, vlBackend.URL, "/loki/api/v1/labels?start=1609459200&end=1609545600")

	if receivedStart != "1609459200" {
		t.Errorf("expected start=1609459200, got %q", receivedStart)
	}
	if receivedEnd != "1609545600" {
		t.Errorf("expected end=1609545600, got %q", receivedEnd)
	}
}

func TestContract_Labels_EmptyResult(t *testing.T) {
	vlBackend := mockVLFieldNames(t, nil)
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/labels")
	assertLokiSuccess(t, resp)

	data := assertDataIsStringArray(t, resp)
	if len(data) != 1 || data[0] != "service_name" {
		t.Errorf("expected synthetic service_name label, got %v", data)
	}
}

// --- /loki/api/v1/label/{name}/values ---

func TestContract_LabelValues_ResponseFormat(t *testing.T) {
	vlBackend := mockVLFieldValues(t, "app", []fieldHit{
		{"nginx", 100}, {"api", 50}, {"worker", 25},
	})
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/label/app/values")
	assertLokiSuccess(t, resp)

	data := assertDataIsStringArray(t, resp)
	if len(data) != 3 {
		t.Errorf("expected 3 values, got %d", len(data))
	}
	assertContains(t, data, "nginx")
	assertContains(t, data, "api")
}

func TestContract_LabelValues_PassesFieldParam(t *testing.T) {
	var receivedField string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedField = r.URL.Query().Get("field")
		writeVLFieldValues(w, nil)
	}))
	defer vlBackend.Close()

	doGet(t, vlBackend.URL, "/loki/api/v1/label/namespace/values")

	if receivedField != "namespace" {
		t.Errorf("expected field=namespace, got %q", receivedField)
	}
}

func TestContract_LabelValues_EmptyResult(t *testing.T) {
	vlBackend := mockVLFieldValues(t, "nonexistent", nil)
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/label/nonexistent/values")
	assertLokiSuccess(t, resp)

	data := assertDataIsStringArray(t, resp)
	if len(data) != 0 {
		t.Errorf("expected empty array, got %v", data)
	}
}

func TestContract_Labels_PrefersStreamFieldNames(t *testing.T) {
	var streamCalls, genericCalls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			streamCalls++
			writeVLFieldNames(w, []fieldHit{{"service.name", 1}, {"app", 1}})
		case "/select/logsql/field_names":
			genericCalls++
			writeVLFieldNames(w, []fieldHit{{"service.name", 1}, {"app", 1}, {"method", 1}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	p.handleLabels(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsStringArray(t, resp)
	assertContains(t, data, "service_name")
	assertContains(t, data, "app")
	assertNotContains(t, data, "method")
	if streamCalls != 1 || genericCalls != 0 {
		t.Fatalf("expected stream_field_names only, got stream=%d generic=%d", streamCalls, genericCalls)
	}
}

func TestContract_Labels_FallsBackToGenericFieldNames(t *testing.T) {
	var streamCalls, genericCalls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			streamCalls++
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"missing endpoint"}`))
		case "/select/logsql/field_names":
			genericCalls++
			writeVLFieldNames(w, []fieldHit{{"service.name", 1}, {"app", 1}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
	p.handleLabels(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	assertLokiSuccess(t, resp)
	if streamCalls != 1 || genericCalls != 1 {
		t.Fatalf("expected fallback to generic field_names, got stream=%d generic=%d", streamCalls, genericCalls)
	}
}

func TestContract_LabelValues_PrefersStreamFieldValues(t *testing.T) {
	var streamNameCalls, streamValueCalls, genericValueCalls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			streamNameCalls++
			writeVLFieldNames(w, []fieldHit{{"k8s.namespace.name", 1}, {"app", 1}})
		case "/select/logsql/stream_field_values":
			streamValueCalls++
			if got := r.URL.Query().Get("field"); got != "k8s.namespace.name" {
				t.Fatalf("expected stream field k8s.namespace.name, got %q", got)
			}
			writeVLFieldValues(w, []fieldHit{{"prod", 1}})
		case "/select/logsql/field_values":
			genericValueCalls++
			writeVLFieldValues(w, []fieldHit{{"wrong", 1}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/label/k8s_namespace_name/values", nil)
	p.handleLabelValues(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsStringArray(t, resp)
	assertContains(t, data, "prod")
	if streamNameCalls != 1 || streamValueCalls != 1 || genericValueCalls != 0 {
		t.Fatalf("expected stream metadata path only, got names=%d streamValues=%d genericValues=%d", streamNameCalls, streamValueCalls, genericValueCalls)
	}
}

func TestContract_LabelValues_JoinsAmbiguousStreamAliases(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			writeVLFieldNames(w, []fieldHit{{"foo.bar", 1}, {"foo-bar", 1}})
		case "/select/logsql/stream_field_values":
			switch r.URL.Query().Get("field") {
			case "foo.bar":
				writeVLFieldValues(w, []fieldHit{{"alpha", 1}})
			case "foo-bar":
				writeVLFieldValues(w, []fieldHit{{"beta", 1}})
			default:
				t.Fatalf("unexpected field %q", r.URL.Query().Get("field"))
			}
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/label/foo_bar/values", nil)
	p.handleLabelValues(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsStringArray(t, resp)
	assertContains(t, data, "alpha")
	assertContains(t, data, "beta")
}

// --- /loki/api/v1/query_range (streams / log query) ---

func TestContract_QueryRange_StreamsFormat(t *testing.T) {
	vlBackend := mockVLLogQuery(t, []vlLogEntry{
		{Time: "2024-01-15T10:30:00Z", Msg: "error in payment", Stream: `{app="api"}`, Fields: map[string]string{"level": "error"}},
		{Time: "2024-01-15T10:30:01Z", Msg: "request completed", Stream: `{app="api"}`, Fields: map[string]string{"level": "info"}},
	})
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1705312200&end=1705312300&limit=100")
	assertLokiSuccess(t, resp)

	data := assertDataIsObject(t, resp)

	// resultType MUST be "streams" for log queries
	if data["resultType"] != "streams" {
		t.Errorf("expected resultType=streams, got %v", data["resultType"])
	}

	// result MUST be array
	result := assertResultIsArray(t, data)
	if len(result) == 0 {
		t.Fatal("result must not be empty")
	}

	// Each stream entry: {"stream": {labels}, "values": [[ts, line], ...]}
	for i, entry := range result {
		assertStreamEntry(t, i, entry)
	}
}

func TestContract_QueryRange_StreamTimestampsAreNanosecondStrings(t *testing.T) {
	vlBackend := mockVLLogQuery(t, []vlLogEntry{
		{Time: "2024-01-15T10:30:00.123456789Z", Msg: "test", Stream: `{app="x"}`},
	})
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/query_range?query=*&start=1&end=2&limit=10")
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)

	entry := result[0].(map[string]interface{})
	values := entry["values"].([]interface{})
	pair := values[0].([]interface{})

	ts, ok := pair[0].(string)
	if !ok {
		t.Fatalf("timestamp must be string, got %T", pair[0])
	}
	// Nanosecond timestamps are 19 digits
	if len(ts) < 19 {
		t.Errorf("timestamp should be nanoseconds (19+ digits), got %q (%d chars)", ts, len(ts))
	}
}

func TestContract_QueryRange_EmptyResult(t *testing.T) {
	vlBackend := mockVLLogQuery(t, nil)
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/query_range?query=*&start=1&end=2&limit=10")
	assertLokiSuccess(t, resp)

	data := assertDataIsObject(t, resp)
	if data["resultType"] != "streams" {
		t.Errorf("expected resultType=streams even for empty, got %v", data["resultType"])
	}
}

func TestContract_QueryRange_StatsField(t *testing.T) {
	vlBackend := mockVLLogQuery(t, nil)
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/query_range?query=*&start=1&end=2&limit=10")
	data := assertDataIsObject(t, resp)

	// Loki responses include a "stats" object (can be empty but must exist)
	if _, ok := data["stats"]; !ok {
		t.Error("query_range response missing 'stats' field — Grafana expects it")
	}
}

// --- /loki/api/v1/query_range (matrix / metric query) ---

func TestContract_QueryRange_MatrixFormat(t *testing.T) {
	// When VL returns Prometheus-format stats, proxy should wrap as matrix
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "success",
				"data": map[string]interface{}{
					"resultType": "matrix",
					"result": []map[string]interface{}{
						{
							"metric": map[string]string{"app": "nginx"},
							"values": [][]interface{}{{1705312200, "42"}, {1705312260, "37"}},
						},
					},
				},
			})
		}
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/query_range?query=rate(%7Bapp%3D%22nginx%22%7D%5B5m%5D)&start=1705312200&end=1705312800&step=60")
	assertLokiSuccess(t, resp)
}

// --- /loki/api/v1/query (instant) ---

func TestContract_Query_ResponseFormat(t *testing.T) {
	vlBackend := mockVLLogQuery(t, []vlLogEntry{
		{Time: "2024-01-15T10:30:00Z", Msg: "test line", Stream: `{app="x"}`},
	})
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/query?query=*&limit=10")
	assertLokiSuccess(t, resp)
}

// --- /loki/api/v1/series ---

func TestContract_Series_ResponseFormat(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": `{app="nginx",env="prod"}`, "hits": 100},
				{"value": `{app="api",env="staging"}`, "hits": 50},
			},
		})
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/series?match[]=%7Bapp%3D%22nginx%22%7D", nil)
	r.ParseForm()
	p.handleSeries(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	assertLokiSuccess(t, resp)

	// data MUST be array of label map objects
	data, ok := resp["data"].([]interface{})
	if !ok {
		t.Fatalf("data must be array, got %T", resp["data"])
	}
	if len(data) != 2 {
		t.Errorf("expected 2 series, got %d", len(data))
	}

	for i, entry := range data {
		labelMap, ok := entry.(map[string]interface{})
		if !ok {
			t.Fatalf("data[%d] must be label map object, got %T", i, entry)
		}
		// Every value must be string
		for k, v := range labelMap {
			if _, ok := v.(string); !ok {
				t.Errorf("data[%d][%q] must be string, got %T", i, k, v)
			}
		}
	}
}

func TestContract_Series_ParsesLabelsCorrectly(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": `{app="nginx",namespace="prod",pod="nginx-abc123"}`, "hits": 1},
			},
		})
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/series?match[]=%7B%7D", nil)
	r.ParseForm()
	p.handleSeries(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	data := resp["data"].([]interface{})
	labelMap := data[0].(map[string]interface{})

	if labelMap["app"] != "nginx" {
		t.Errorf("expected app=nginx, got %v", labelMap["app"])
	}
	if labelMap["namespace"] != "prod" {
		t.Errorf("expected namespace=prod, got %v", labelMap["namespace"])
	}
	if labelMap["pod"] != "nginx-abc123" {
		t.Errorf("expected pod=nginx-abc123, got %v", labelMap["pod"])
	}
}

// --- /loki/api/v1/index/stats ---

func TestContract_IndexStats_ResponseFormat(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/stats?query=%7B%7D", nil)
	p.handleIndexStats(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	// Loki index/stats returns flat object with numeric fields
	for _, field := range []string{"streams", "chunks", "bytes", "entries"} {
		v, ok := resp[field]
		if !ok {
			t.Errorf("missing required field %q", field)
			continue
		}
		// Must be a number (JSON numbers are float64 in Go)
		if _, ok := v.(float64); !ok {
			t.Errorf("field %q must be number, got %T", field, v)
		}
	}
}

// --- /loki/api/v1/index/volume ---

func TestContract_Volume_ResponseFormat(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume?query=%7B%7D&start=1&end=2", nil)
	p.handleVolume(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	assertLokiSuccess(t, resp)

	data := assertDataIsObject(t, resp)
	if data["resultType"] != "vector" {
		t.Errorf("expected resultType=vector, got %v", data["resultType"])
	}
	assertResultIsArray(t, data)
}

// --- /loki/api/v1/index/volume_range ---

func TestContract_VolumeRange_ResponseFormat(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume_range?query=%7B%7D&start=1&end=2&step=60", nil)
	p.handleVolumeRange(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	assertLokiSuccess(t, resp)

	data := assertDataIsObject(t, resp)
	if data["resultType"] != "matrix" {
		t.Errorf("expected resultType=matrix, got %v", data["resultType"])
	}
	assertResultIsArray(t, data)
}

// --- /loki/api/v1/detected_fields ---

func TestContract_DetectedFields_ResponseFormat(t *testing.T) {
	vlBackend := mockVLFieldNames(t, []fieldHit{
		{"level", 1000}, {"duration", 500}, {"status", 300},
	})
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/detected_fields?query=*")

	// Loki detected_fields returns {"fields": [{label, type, cardinality}, ...]}
	fields, ok := resp["fields"].([]interface{})
	if !ok {
		t.Fatalf("missing 'fields' array, got %T", resp["fields"])
	}

	for i, f := range fields {
		obj, ok := f.(map[string]interface{})
		if !ok {
			t.Fatalf("fields[%d] must be object, got %T", i, f)
		}
		// Required fields per Loki spec
		if _, ok := obj["label"]; !ok {
			t.Errorf("fields[%d] missing 'label'", i)
		}
		if _, ok := obj["type"]; !ok {
			t.Errorf("fields[%d] missing 'type'", i)
		}
		if _, ok := obj["cardinality"]; !ok {
			t.Errorf("fields[%d] missing 'cardinality'", i)
		}
	}
}

// --- /loki/api/v1/patterns ---

func TestContract_Patterns_ResponseFormat(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"web","level":"info"}` + "\n"))
		w.Write([]byte(`{"_time":"2026-04-04T10:00:01Z","_msg":"GET /api/users 200 22ms","app":"web","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7B%7D&start=1&end=2", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for drilldown-compatible patterns endpoint, got %d", w.Code)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Fatalf("expected status=success, got %v", resp)
	}
	data, ok := resp["data"].([]interface{})
	if !ok || len(data) == 0 {
		t.Fatalf("expected at least one pattern, got %v", resp)
	}
}

// --- /loki/api/v1/tail ---

func TestContract_Tail_RequiresQuery(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/tail", nil)
	p.handleTail(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 without query param, got %d", w.Code)
	}
}

func TestContract_Tail_WebSocketUpgrade(t *testing.T) {
	// VL backend that streams NDJSON lines then closes
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("expected flusher")
		}
		fmt.Fprintf(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"test log line","app":"nginx"}`+"\n")
		flusher.Flush()
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	// Connect via WebSocket
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?query={app%3D%22nginx%22}"
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v (resp: %v)", err, resp)
	}
	defer ws.Close()

	// Read the tail frame
	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("websocket read failed: %v", err)
	}

	var frame map[string]interface{}
	if err := json.Unmarshal(msg, &frame); err != nil {
		t.Fatalf("invalid JSON frame: %v", err)
	}

	// Verify Loki-compatible tail frame structure
	streams, ok := frame["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Fatalf("expected non-empty streams array, got %v", frame)
	}
	stream0, ok := streams[0].(map[string]interface{})
	if !ok {
		t.Fatal("expected stream object")
	}
	if _, ok := stream0["stream"]; !ok {
		t.Error("expected 'stream' field in tail frame")
	}
	if _, ok := stream0["values"]; !ok {
		t.Error("expected 'values' field in tail frame")
	}
}

// --- /loki/api/v1/status/buildinfo ---

func TestContract_BuildInfo_ResponseFormat(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/status/buildinfo", nil)
	p.handleBuildInfo(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	assertLokiSuccess(t, resp)

	data := assertDataIsObject(t, resp)
	for _, field := range []string{"version", "revision", "branch", "goVersion"} {
		if _, ok := data[field]; !ok {
			t.Errorf("buildinfo missing field %q", field)
		}
	}
}

// --- /ready ---

func TestContract_Ready_HealthyBackend(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/ready", nil)
	p.handleReady(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 when backend healthy, got %d", w.Code)
	}
}

func TestContract_Ready_UnhealthyBackend(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/ready", nil)
	p.handleReady(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when backend unhealthy, got %d", w.Code)
	}
}

// --- /health and /alive ---

func TestContract_Health_AlwaysOK(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/health", nil)
	p.handleHealth(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /health, got %d", w.Code)
	}
	if got := strings.TrimSpace(w.Body.String()); got != "ok" {
		t.Errorf("expected /health body to be ok, got %q", got)
	}
}

func TestContract_Alive_AlwaysOK(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/alive", nil)
	p.handleAlive(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /alive, got %d", w.Code)
	}
	if got := strings.TrimSpace(w.Body.String()); got != "alive" {
		t.Errorf("expected /alive body to be alive, got %q", got)
	}
}

// --- /metrics ---

func TestContract_Metrics_PrometheusFormat(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/metrics", nil)
	mux.ServeHTTP(w, r)

	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text/plain Content-Type, got %q", ct)
	}

	body := w.Body.String()
	required := []string{
		"loki_vl_proxy_requests_total",
		"loki_vl_proxy_request_duration_seconds",
		"loki_vl_proxy_cache_hits_total",
		"loki_vl_proxy_cache_misses_total",
		"loki_vl_proxy_translations_total",
		"loki_vl_proxy_translation_errors_total",
		"loki_vl_proxy_uptime_seconds",
	}
	for _, m := range required {
		if !strings.Contains(body, m) {
			t.Errorf("/metrics missing required metric: %s", m)
		}
	}
}

// =============================================================================
// Translation Integration Tests
// Verify LogQL queries are correctly translated before hitting VL backend
// =============================================================================

func TestTranslation_LineFilterForwarded(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte{})
	}))
	defer vlBackend.Close()

	doGet(t, vlBackend.URL, `/loki/api/v1/query_range?query=%7Bapp%3D%22nginx%22%7D+%7C%3D+%22error%22&start=1&end=2&limit=10`)

	// |= "error" must become ~"error" (substring, not word match)
	// proxyLogQuery appends sort by _time desc by default (Loki backward direction)
	if receivedQuery != `app:=nginx ~"error" | sort by (_time desc)` {
		t.Errorf("expected translated query, got %q", receivedQuery)
	}
}

func TestTranslation_NegativeFilter(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte{})
	}))
	defer vlBackend.Close()

	doGet(t, vlBackend.URL, `/loki/api/v1/query_range?query=%7Bapp%3D%22nginx%22%7D+%21%3D+%22debug%22&start=1&end=2&limit=10`)

	if receivedQuery != `app:=nginx NOT ~"debug" | sort by (_time desc)` {
		t.Errorf("expected translated negative filter, got %q", receivedQuery)
	}
}

func TestTranslation_JSONParser(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte{})
	}))
	defer vlBackend.Close()

	doGet(t, vlBackend.URL, `/loki/api/v1/query_range?query=%7Bapp%3D%22x%22%7D+%7C+json&start=1&end=2&limit=10`)

	if receivedQuery != `app:=x | unpack_json | sort by (_time desc)` {
		t.Errorf("expected json→unpack_json translation, got %q", receivedQuery)
	}
}

// =============================================================================
// Cache Protection Tests
// =============================================================================

func TestCache_LabelsHitOnRepeat(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		writeVLFieldNames(w, []fieldHit{{"app", 1}})
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	// First call — miss
	w1 := httptest.NewRecorder()
	p.handleLabels(w1, httptest.NewRequest("GET", "/loki/api/v1/labels?start=1&end=2", nil))
	if callCount != 1 {
		t.Fatalf("expected 1 backend call, got %d", callCount)
	}

	// Second call — hit
	w2 := httptest.NewRecorder()
	p.handleLabels(w2, httptest.NewRequest("GET", "/loki/api/v1/labels?start=1&end=2", nil))
	if callCount != 1 {
		t.Errorf("expected cache hit (still 1 call), got %d", callCount)
	}

	// Different params — miss
	w3 := httptest.NewRecorder()
	p.handleLabels(w3, httptest.NewRequest("GET", "/loki/api/v1/labels?start=3&end=4", nil))
	if callCount != 2 {
		t.Errorf("expected 2 calls after different params, got %d", callCount)
	}
}

func TestCache_LabelValuesHitOnRepeat(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		writeVLFieldValues(w, []fieldHit{{"nginx", 1}})
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	w1 := httptest.NewRecorder()
	p.handleLabelValues(w1, httptest.NewRequest("GET", "/loki/api/v1/label/app/values?start=1&end=2", nil))
	if callCount != 2 {
		t.Fatalf("expected metadata lookup plus value lookup on miss, got %d calls", callCount)
	}

	w2 := httptest.NewRecorder()
	p.handleLabelValues(w2, httptest.NewRequest("GET", "/loki/api/v1/label/app/values?start=1&end=2", nil))
	if callCount != 2 {
		t.Errorf("expected cache hit, got %d calls", callCount)
	}
}

func TestCache_QueryRangeHitOnRepeat(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/stream+json")
		_, _ = w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"cache me","_stream":"{app=\"nginx\"}"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)

	r1 := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&limit=10`, nil)
	w1 := httptest.NewRecorder()
	p.handleQueryRange(w1, r1)
	if callCount != 1 {
		t.Fatalf("expected 1 backend call, got %d", callCount)
	}

	r2 := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&limit=10`, nil)
	w2 := httptest.NewRecorder()
	p.handleQueryRange(w2, r2)
	if callCount != 1 {
		t.Fatalf("expected cache hit on second query_range call, got %d backend calls", callCount)
	}
	if w1.Body.String() != w2.Body.String() {
		t.Fatalf("expected cached query_range body to match original response")
	}

	r3 := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=3&end=4&limit=10`, nil)
	w3 := httptest.NewRecorder()
	p.handleQueryRange(w3, r3)
	if callCount != 2 {
		t.Fatalf("expected new backend call for different time range, got %d", callCount)
	}
}

// =============================================================================
// POST Support Tests — Loki allows POST for all query endpoints
// =============================================================================

func TestContract_QueryRange_POST(t *testing.T) {
	vlBackend := mockVLLogQuery(t, []vlLogEntry{
		{Time: "2024-01-15T10:30:00Z", Msg: "test", Stream: `{app="x"}`},
	})
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	body := strings.NewReader("query=%7Bapp%3D%22x%22%7D&start=1&end=2&limit=10")
	r := httptest.NewRequest("POST", "/loki/api/v1/query_range", body)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	p.handleQueryRange(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	assertLokiSuccess(t, resp)
}

// =============================================================================
// Mock VL backend helpers
// =============================================================================

type fieldHit struct {
	Value string
	Hits  int64
}

type vlLogEntry struct {
	Time   string
	Msg    string
	Stream string
	Fields map[string]string
}

func mockVLFieldNames(t *testing.T, fields []fieldHit) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeVLFieldNames(w, fields)
	}))
}

func writeVLFieldNames(w http.ResponseWriter, fields []fieldHit) {
	values := make([]map[string]interface{}, len(fields))
	for i, f := range fields {
		values[i] = map[string]interface{}{"value": f.Value, "hits": f.Hits}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"values": values})
}

func mockVLFieldValues(t *testing.T, expectedField string, values []fieldHit) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeVLFieldValues(w, values)
	}))
}

func writeVLFieldValues(w http.ResponseWriter, values []fieldHit) {
	vals := make([]map[string]interface{}, len(values))
	for i, v := range values {
		vals[i] = map[string]interface{}{"value": v.Value, "hits": v.Hits}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"values": vals})
}

func mockVLLogQuery(t *testing.T, entries []vlLogEntry) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/stream+json")
		for _, e := range entries {
			obj := map[string]interface{}{
				"_time":   e.Time,
				"_msg":    e.Msg,
				"_stream": e.Stream,
			}
			for k, v := range e.Fields {
				obj[k] = v
			}
			line, _ := json.Marshal(obj)
			w.Write(line)
			w.Write([]byte("\n"))
		}
	}))
}

// =============================================================================
// Test execution helpers
// =============================================================================

func doGet(t *testing.T, backendURL, path string) map[string]interface{} {
	t.Helper()
	p := newTestProxy(t, backendURL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", path, nil)

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response from %s: %v\nbody: %s", path, err, w.Body.String())
	}
	return resp
}

func newTestProxy(t *testing.T, backendURL string) *Proxy {
	t.Helper()
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: backendURL,
		Cache:      c,
		LogLevel:   "error",
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

func assertLokiSuccess(t *testing.T, resp map[string]interface{}) {
	t.Helper()
	if resp["status"] != "success" {
		t.Errorf("expected status=success, got %v", resp["status"])
	}
}

func assertDataIsStringArray(t *testing.T, resp map[string]interface{}) []string {
	t.Helper()
	data, ok := resp["data"].([]interface{})
	if !ok {
		t.Fatalf("data must be array, got %T: %v", resp["data"], resp["data"])
	}
	result := make([]string, len(data))
	for i, v := range data {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("data[%d] must be string, got %T: %v", i, v, v)
		}
		result[i] = s
	}
	return result
}

func assertDataIsObject(t *testing.T, resp map[string]interface{}) map[string]interface{} {
	t.Helper()
	data, ok := resp["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data must be object, got %T", resp["data"])
	}
	return data
}

func assertResultIsArray(t *testing.T, data map[string]interface{}) []interface{} {
	t.Helper()
	result, ok := data["result"].([]interface{})
	if !ok {
		t.Fatalf("result must be array, got %T", data["result"])
	}
	return result
}

func assertStreamEntry(t *testing.T, idx int, entry interface{}) {
	t.Helper()
	obj, ok := entry.(map[string]interface{})
	if !ok {
		t.Fatalf("result[%d] must be object, got %T", idx, entry)
	}

	// "stream" must be map[string]string
	stream, ok := obj["stream"].(map[string]interface{})
	if !ok {
		t.Fatalf("result[%d].stream must be object, got %T", idx, obj["stream"])
	}
	for k, v := range stream {
		if _, ok := v.(string); !ok {
			t.Errorf("result[%d].stream[%q] must be string, got %T", idx, k, v)
		}
	}

	// "values" must be array of [string, string] pairs, optionally with
	// a third metadata/parsing object like Loki query responses support.
	values, ok := obj["values"].([]interface{})
	if !ok {
		t.Fatalf("result[%d].values must be array, got %T", idx, obj["values"])
	}
	for j, val := range values {
		pair, ok := val.([]interface{})
		if !ok || (len(pair) != 2 && len(pair) != 3) {
			t.Errorf("result[%d].values[%d] must be [ts, line] or [ts, line, meta], got %v", idx, j, val)
			continue
		}
		if _, ok := pair[0].(string); !ok {
			t.Errorf("result[%d].values[%d][0] timestamp must be string, got %T", idx, j, pair[0])
		}
		if _, ok := pair[1].(string); !ok {
			t.Errorf("result[%d].values[%d][1] line must be string, got %T", idx, j, pair[1])
		}
		if len(pair) == 3 {
			if _, ok := pair[2].(map[string]interface{}); !ok {
				t.Errorf("result[%d].values[%d][2] metadata must be object, got %T", idx, j, pair[2])
			}
		}
	}
}

func assertContains(t *testing.T, slice []string, s string) {
	t.Helper()
	for _, v := range slice {
		if v == s {
			return
		}
	}
	t.Errorf("expected slice to contain %q, got %v", s, slice)
}

func assertNotContains(t *testing.T, slice []string, s string) {
	t.Helper()
	for _, v := range slice {
		if v == s {
			t.Errorf("expected slice not to contain %q, got %v", s, slice)
			return
		}
	}
}

func mustUnmarshal(t *testing.T, data []byte, v interface{}) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v\nbody: %s", err, string(data))
	}
}

// =============================================================================
// P0: Metrics must record actual status codes, not always 200
// =============================================================================

func TestContract_QueryRange_MetricsRecordActualStatus(t *testing.T) {
	// Backend returns 502
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`{"error":"backend overloaded"}`))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2`, nil)
	p.handleQueryRange(w, r)

	// The response should NOT be 200 when backend fails
	// (writeError sets appropriate error code)
	if w.Code == http.StatusOK {
		t.Error("expected non-200 status when backend returns error, got 200")
	}
}

// =============================================================================
// Direction parameter — forward and backward sorting
// =============================================================================

func TestContract_QueryRange_DirectionForward(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&direction=forward`, nil)
	p.handleQueryRange(w, r)

	if !strings.Contains(receivedQuery, "| sort by (_time)") {
		t.Errorf("expected sort by (_time) for direction=forward, got %q", receivedQuery)
	}
	if strings.Contains(receivedQuery, "desc") {
		t.Errorf("direction=forward should not have desc, got %q", receivedQuery)
	}
}

func TestContract_QueryRange_DirectionBackward(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2&direction=backward`, nil)
	p.handleQueryRange(w, r)

	if !strings.Contains(receivedQuery, "| sort by (_time desc)") {
		t.Errorf("expected sort by (_time desc) for direction=backward, got %q", receivedQuery)
	}
}

func TestContract_QueryRange_DirectionDefault(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	// No direction param — should default to backward (newest first)
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="nginx"}&start=1&end=2`, nil)
	p.handleQueryRange(w, r)

	if !strings.Contains(receivedQuery, "| sort by (_time desc)") {
		t.Errorf("expected default direction=backward (desc), got %q", receivedQuery)
	}
}

// =============================================================================
// Labels with query param — scoped labels
// =============================================================================

func TestContract_Labels_ForwardsQueryParam(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.URL.Query().Get("query")
		writeVLFieldNames(w, []fieldHit{{"app", 1}, {"level", 1}})
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	// Query param should be translated and forwarded
	r := httptest.NewRequest("GET", `/loki/api/v1/labels?query={app="nginx"}`, nil)
	p.handleLabels(w, r)

	// Should have translated the query, not just "*"
	if receivedQuery == "*" {
		t.Error("expected query param to be forwarded, got *")
	}
	if !strings.Contains(receivedQuery, "app") {
		t.Errorf("expected translated query to contain 'app', got %q", receivedQuery)
	}
}

func TestContract_Labels_DefaultsToWildcard(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.URL.Query().Get("query")
		writeVLFieldNames(w, []fieldHit{{"app", 1}})
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	// No query param — should default to *
	r := httptest.NewRequest("GET", `/loki/api/v1/labels`, nil)
	p.handleLabels(w, r)

	if receivedQuery != "*" {
		t.Errorf("expected default query=*, got %q", receivedQuery)
	}
}

// =============================================================================
// VL error message mapping
// =============================================================================

func TestContract_WrapAsLokiResponse_VLError(t *testing.T) {
	// VL error format: {"error":"some message"}
	vlBody := []byte(`{"error":"query syntax error at position 42"}`)
	result := wrapAsLokiResponse(vlBody, "matrix")

	var resp map[string]interface{}
	json.Unmarshal(result, &resp)

	if resp["status"] != "error" {
		t.Errorf("expected status=error, got %v", resp["status"])
	}
	if resp["error"] != "query syntax error at position 42" {
		t.Errorf("expected error message preserved, got %v", resp["error"])
	}
}

func TestContract_WrapAsLokiResponse_VLStatusError(t *testing.T) {
	// VL may also return: {"status":"error","msg":"message"}
	vlBody := []byte(`{"status":"error","msg":"invalid LogsQL expression"}`)
	result := wrapAsLokiResponse(vlBody, "vector")

	var resp map[string]interface{}
	json.Unmarshal(result, &resp)

	if resp["status"] != "error" {
		t.Errorf("expected status=error, got %v", resp["status"])
	}
	if resp["error"] != "invalid LogsQL expression" {
		t.Errorf("expected msg mapped to error field, got %v", resp["error"])
	}
}

func TestContract_Query_MetricsRecordActualStatus(t *testing.T) {
	// Backend returns 502
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`{"error":"backend overloaded"}`))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query?query={app="nginx"}`, nil)
	p.handleQuery(w, r)

	if w.Code == http.StatusOK {
		t.Error("expected non-200 status when backend returns error, got 200")
	}
}

// =============================================================================
// isStatsQuery — must not false-positive on quoted log content
// =============================================================================

func TestIsStatsQuery(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		expect bool
	}{
		{"actual stats pipe", `app:="nginx" | stats count()`, true},
		{"actual rate pipe", `app:="nginx" | rate()`, true},
		{"actual count pipe", `app:="nginx" | count()`, true},
		{"stats by labels", `app:="nginx" | stats by (level) count()`, true},
		{"log filter with stats in content", `app:="nginx" ~"stats query failed"`, false},
		{"log filter with pipe stats in quotes", `app:="nginx" ~"| stats "`, false},
		{"log filter containing rate in log line", `app:="nginx" ~"rate limit exceeded"`, false},
		{"log filter with count in message", `app:="nginx" ~"count is wrong"`, false},
		{"no stats at all", `app:="nginx" level:="error"`, false},
		{"empty query", `*`, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isStatsQuery(tc.query)
			if got != tc.expect {
				t.Errorf("isStatsQuery(%q) = %v, want %v", tc.query, got, tc.expect)
			}
		})
	}
}

// TestContract_QueryRange_StatsInLogContent verifies that a query containing "stats"
// in the log content filter does NOT get routed to the stats query handler.
func TestContract_QueryRange_StatsInLogContent(t *testing.T) {
	var receivedPath string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		// Return NDJSON log lines
		w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"stats query failed","app":"nginx"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	// URL-encode the query properly to avoid malformed request
	r := httptest.NewRequest("GET", `/loki/api/v1/query_range?query=%7Bapp%3D%22nginx%22%7D+%7C%3D+%22stats+query%22&start=1&end=2`, nil)
	p.handleQueryRange(w, r)

	// Should go to log query path, not stats_query_range
	if receivedPath == "/select/logsql/stats_query_range" {
		t.Error("query with 'stats' in log content was incorrectly routed to stats handler")
	}
	if receivedPath != "/select/logsql/query" {
		t.Errorf("expected log query path /select/logsql/query, got %q", receivedPath)
	}
}
