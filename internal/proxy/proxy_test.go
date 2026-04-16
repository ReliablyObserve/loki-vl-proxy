package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
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

func TestContract_LabelValues_ForwardsSubstringFilter_OnV149Plus(t *testing.T) {
	var receivedQ, receivedFilter string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			writeVLFieldNames(w, []fieldHit{{"app", 1}})
		case "/select/logsql/stream_field_values":
			receivedQ = r.URL.Query().Get("q")
			receivedFilter = r.URL.Query().Get("filter")
			writeVLFieldValues(w, []fieldHit{{"argocd", 1}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	h := http.Header{}
	h.Set("Server", "VictoriaLogs/v1.49.0")
	p.observeBackendVersionFromHeaders(h)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/label/app/values?search=arg", nil)
	p.handleLabelValues(w, r)

	if receivedQ != "arg" {
		t.Fatalf("expected q=arg to be forwarded for substring filter, got %q", receivedQ)
	}
	if receivedFilter != "substring" {
		t.Fatalf("expected filter=substring for VictoriaLogs >= v1.49.0, got %q", receivedFilter)
	}
}

func TestContract_LabelValues_DoesNotForwardSubstringFilter_OnV148(t *testing.T) {
	var receivedQ, receivedFilter string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			writeVLFieldNames(w, []fieldHit{{"app", 1}})
		case "/select/logsql/stream_field_values":
			receivedQ = r.URL.Query().Get("q")
			receivedFilter = r.URL.Query().Get("filter")
			writeVLFieldValues(w, []fieldHit{{"argocd", 1}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	h := http.Header{}
	h.Set("Server", "VictoriaLogs/v1.48.0")
	p.observeBackendVersionFromHeaders(h)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/label/app/values?search=arg", nil)
	p.handleLabelValues(w, r)

	if receivedQ != "" {
		t.Fatalf("expected q to stay unset for VictoriaLogs < v1.49.0, got %q", receivedQ)
	}
	if receivedFilter != "" {
		t.Fatalf("expected filter to stay unset for VictoriaLogs < v1.49.0, got %q", receivedFilter)
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

func TestContract_Patterns_DoesNotAppendSortClause(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if strings.Contains(r.FormValue("query"), "sort by") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"sort unsupported"}`))
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"web","level":"info"}` + "\n"))
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:01Z","_msg":"GET /api/users 200 22ms","app":"web","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1&end=2", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data, _ := resp["data"].([]interface{})
	if len(data) == 0 {
		t.Fatalf("expected non-empty patterns response, got %v", resp)
	}
}

func TestContract_Patterns_StripsPipelineAndUsesLabelScope(t *testing.T) {
	var gotQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"web","level":"info"}` + "\n"))
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:01Z","_msg":"GET /api/users 200 22ms","app":"web","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D+%7C+json+%7C+filter+source_message_bytes%3A%3D89+%7C+extract+%60%28.%2A%29%60&start=1&end=2", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(gotQuery, "extract") || strings.Contains(gotQuery, "filter") || strings.Contains(gotQuery, "json") {
		t.Fatalf("expected /patterns backend query to be selector-scoped, got %q", gotQuery)
	}
	if gotQuery != `app:=web` {
		t.Fatalf("expected translated selector query app:=web, got %q", gotQuery)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data, _ := resp["data"].([]interface{})
	if len(data) == 0 {
		t.Fatalf("expected non-empty patterns response, got %v", resp)
	}
}

func TestContract_Patterns_FallsBackToQueryRangeWhenQueryUnavailable(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"query endpoint unavailable"}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1&end=2", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data, _ := resp["data"].([]interface{})
	if len(data) != 0 {
		t.Fatalf("expected empty patterns response when query backend is unavailable, got %v", resp)
	}
}

func TestContract_Patterns_FallsBackToQueryRangeWhenQueryHasNoExtractablePatterns(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			// Valid response body, but no extractable lines for pattern miner.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1&end=2", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data, _ := resp["data"].([]interface{})
	if len(data) != 0 {
		t.Fatalf("expected empty patterns response when query body has no extractable lines, got %v", resp)
	}
}

func TestContract_Patterns_PrefersQueryRangeForSelectedRange(t *testing.T) {
	var queryRangeCalls, queryCalls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query_range":
			queryRangeCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		case "/select/logsql/query":
			queryCalls++
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","level":"info"}` + "\n"))
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:01:00Z","_msg":"GET /api/users 200 22ms","level":"info"}` + "\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1712311200&end=1712311800&step=1m", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	if queryCalls == 0 {
		t.Fatalf("expected query endpoint to be used for patterns extraction")
	}
	if queryRangeCalls != 0 {
		t.Fatalf("expected query_range to be skipped for patterns extraction, got %d calls", queryRangeCalls)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data, _ := resp["data"].([]interface{})
	if len(data) == 0 {
		t.Fatalf("expected non-empty patterns response, got %v", resp)
	}
}

func TestContract_Patterns_UsesFromToWhenStartEndMissing(t *testing.T) {
	var receivedStart, receivedEnd, receivedStep string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		receivedStart = r.FormValue("start")
		receivedEnd = r.FormValue("end")
		receivedStep = r.FormValue("step")
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		"GET",
		"/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&from=1712311200&to=1712311800&step=1m",
		nil,
	)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	if receivedStart != "1712311200" || receivedEnd != "1712311800" {
		t.Fatalf("expected from/to to be forwarded as start/end, got start=%q end=%q", receivedStart, receivedEnd)
	}
	if receivedStep != "1m" {
		t.Fatalf("expected step to be forwarded to query, got %q", receivedStep)
	}
}

func TestContract_Patterns_AdaptiveSourceLimitForLongRanges(t *testing.T) {
	receivedLimits := make([]string, 0, 32)
	var limitsMu sync.Mutex
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		limitsMu.Lock()
		receivedLimits = append(receivedLimits, r.FormValue("limit"))
		limitsMu.Unlock()
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		"GET",
		"/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1712311200&end=1712916000&step=60",
		nil,
	)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	limitsMu.Lock()
	limitsSnapshot := append([]string(nil), receivedLimits...)
	limitsMu.Unlock()
	if len(limitsSnapshot) < 2 {
		t.Fatalf("expected windowed query fanout for long range, got %d backend calls", len(limitsSnapshot))
	}
	for i, limit := range limitsSnapshot {
		n, err := strconv.Atoi(limit)
		if err != nil {
			t.Fatalf("expected numeric per-window limit at call %d, got %q", i+1, limit)
		}
		if n <= 0 || n > 2000 {
			t.Fatalf("expected per-window limit in range 1..2000 at call %d, got %d", i+1, n)
		}
	}
}

func TestContract_Patterns_DerivesStepWhenMissing(t *testing.T) {
	var receivedStep string
	var stepMu sync.Mutex
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		stepMu.Lock()
		if strings.TrimSpace(receivedStep) == "" {
			receivedStep = r.FormValue("step")
		}
		stepMu.Unlock()
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		"GET",
		"/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1712311200&end=1712916000",
		nil,
	)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	stepMu.Lock()
	stepSnapshot := receivedStep
	stepMu.Unlock()
	if strings.TrimSpace(stepSnapshot) == "" {
		t.Fatalf("expected derived step to be forwarded when request step is missing")
	}
}

func TestContract_Patterns_WindowedSamplingCoversWholeRange(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		startRaw := strings.TrimSpace(r.FormValue("start"))
		startVal, err := strconv.ParseInt(startRaw, 10, 64)
		if err != nil {
			t.Fatalf("expected numeric start timestamp, got %q: %v", startRaw, err)
		}
		startSec := startVal
		if len(startRaw) > 10 {
			startSec = startVal / int64(time.Second)
		}
		ts := time.Unix(startSec, 0).UTC().Format(time.RFC3339)
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintf(w, `{"_time":"%s","_msg":"finished_unary_call code=OK method=Fetch","level":"info"}`+"\n", ts)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		"GET",
		"/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1712311200&end=1712484000&step=60s&line_limit=20",
		nil,
	)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}

	var resp patternsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Data) == 0 {
		t.Fatalf("expected non-empty patterns response")
	}
	samples := resp.Data[0].Samples
	if len(samples) < 2 {
		t.Fatalf("expected windowed sampling to produce samples across range, got %v", samples)
	}
	firstTS, okFirst := numberToInt64(samples[0][0])
	lastTS, okLast := numberToInt64(samples[len(samples)-1][0])
	if !okFirst || !okLast {
		t.Fatalf("expected numeric sample timestamps, got first=%v last=%v", samples[0], samples[len(samples)-1])
	}
	if firstTS >= lastTS {
		t.Fatalf("expected increasing sample timestamps across range, got first=%d last=%d", firstTS, lastTS)
	}
}

func TestBackendVersionDetection_FromResponseHeaders(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	if p.supportsDensePatternWindowing() {
		t.Fatalf("expected dense windowing disabled before backend version is detected")
	}
	if !p.supportsStreamMetadataEndpoints() {
		t.Fatalf("expected stream metadata endpoints enabled before version detection")
	}

	h := http.Header{}
	h.Set("Server", "VictoriaLogs/v1.50.0")
	p.observeBackendVersionFromHeaders(h)

	if got := p.backendVersionSemver; got != "v1.50.0" {
		t.Fatalf("expected semver v1.50.0 from headers, got %q", got)
	}
	if !p.supportsDensePatternWindowing() {
		t.Fatalf("expected dense pattern windowing to be enabled for VictoriaLogs >= v1.50.0")
	}
	if !p.supportsStreamMetadataEndpoints() {
		t.Fatalf("expected stream metadata endpoints to stay enabled for VictoriaLogs >= v1.50.0")
	}
	if got := p.backendCapabilityProfile; got != "vl-v1.50-plus" {
		t.Fatalf("expected capability profile vl-v1.50-plus, got %q", got)
	}
}

func TestBackendVersionDetection_OlderVersionKeepsConservativeProfile(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	h := http.Header{}
	h.Set("X-App-Version", "victoria-logs v1.49.0")
	p.observeBackendVersionFromHeaders(h)

	if got := p.backendVersionSemver; got != "v1.49.0" {
		t.Fatalf("expected semver v1.49.0 from headers, got %q", got)
	}
	if p.supportsDensePatternWindowing() {
		t.Fatalf("expected dense pattern windowing to stay disabled for VictoriaLogs < v1.50.0")
	}
	if !p.supportsStreamMetadataEndpoints() {
		t.Fatalf("expected stream metadata endpoints enabled for VictoriaLogs >= v1.30.0")
	}
	if !p.supportsMetadataSubstringFilter() {
		t.Fatalf("expected metadata substring filter enabled for VictoriaLogs >= v1.49.0")
	}
	if got := p.backendCapabilityProfile; got != "vl-v1.49-plus" {
		t.Fatalf("expected capability profile vl-v1.49-plus, got %q", got)
	}
}

func TestBackendVersionDetection_V148KeepsSubstringFilterDisabled(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	h := http.Header{}
	h.Set("Server", "VictoriaLogs/v1.48.0")
	p.observeBackendVersionFromHeaders(h)

	if got := p.backendVersionSemver; got != "v1.48.0" {
		t.Fatalf("expected semver v1.48.0 from headers, got %q", got)
	}
	if p.supportsMetadataSubstringFilter() {
		t.Fatalf("expected metadata substring filter disabled for VictoriaLogs < v1.49.0")
	}
	if got := p.backendCapabilityProfile; got != "vl-v1.30-plus" {
		t.Fatalf("expected capability profile vl-v1.30-plus, got %q", got)
	}
}

func TestBackendVersionDetection_LegacyVersionDisablesStreamMetadataFastPath(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	h := http.Header{}
	h.Set("Server", "VictoriaLogs/v1.29.0")
	p.observeBackendVersionFromHeaders(h)

	if got := p.backendVersionSemver; got != "v1.29.0" {
		t.Fatalf("expected semver v1.29.0 from headers, got %q", got)
	}
	if p.supportsStreamMetadataEndpoints() {
		t.Fatalf("expected stream metadata endpoints disabled for legacy VictoriaLogs versions")
	}
	if p.supportsDensePatternWindowing() {
		t.Fatalf("expected dense pattern windowing disabled for legacy VictoriaLogs versions")
	}
	if got := p.backendCapabilityProfile; got != "legacy-pre-v1.30" {
		t.Fatalf("expected capability profile legacy-pre-v1.30, got %q", got)
	}
}

func TestBackendVersionCompatibilityGate_BlocksTooOldVersion(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Server", "VictoriaLogs/v1.29.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}

	err = p.ValidateBackendVersionCompatibility(context.Background())
	if err == nil {
		t.Fatalf("expected compatibility gate to fail for backend v1.29.0")
	}
	if !strings.Contains(err.Error(), "backend-allow-unsupported-version") {
		t.Fatalf("expected bypass hint in error, got: %v", err)
	}
}

func TestBackendVersionCompatibilityGate_AllowsBypassForTooOldVersion(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "VictoriaLogs/v1.29.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                     backend.URL,
		Cache:                          cache.New(60*time.Second, 1000),
		BackendMinVersion:              "v1.30.0",
		BackendAllowUnsupportedVersion: true,
		BackendVersionCheckTimeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected bypassed compatibility gate, got: %v", err)
	}
}

func TestBackendVersionCompatibilityGate_PassesSupportedVersion(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "VictoriaLogs/v1.50.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected compatibility gate to pass, got: %v", err)
	}
}

func TestBackendVersionCompatibilityGate_FallbackMetricsDetectsVersion(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`vm_app_version{short_version="v1.50.0",version="v1.50.0-cluster"} 1` + "\n"))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected compatibility gate to pass via metrics fallback, got: %v", err)
	}
	if got := p.backendVersionSemver; got != "v1.50.0" {
		t.Fatalf("expected semver v1.50.0 from metrics fallback, got %q", got)
	}
	if !p.supportsDensePatternWindowing() {
		t.Fatalf("expected dense windowing enabled for v1.50.0")
	}
}

func TestBackendVersionCompatibilityGate_FallbackMetricsTooOldBlocks(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`vm_app_version{short_version="v1.29.0",version="v1.29.0"} 1` + "\n"))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	err = p.ValidateBackendVersionCompatibility(context.Background())
	if err == nil {
		t.Fatalf("expected compatibility gate to fail for fallback metrics v1.29.0")
	}
}

func TestBackendVersionCompatibilityGate_EndpointProbeInfersCapabilities(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("up 1\n"))
		case "/select/logsql/stream_field_names":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"hits":[]}`))
		case "/select/logsql/field_names":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"hits":[]}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected compatibility gate to continue with endpoint capability probe, got: %v", err)
	}
	if p.backendVersionSemver != "" {
		t.Fatalf("expected empty semver when only endpoint probe is available, got %q", p.backendVersionSemver)
	}
	if got := p.backendCapabilityProfile; got != "vl-probed-v1.49-plus" {
		t.Fatalf("expected capability profile vl-probed-v1.49-plus, got %q", got)
	}
	if !p.supportsStreamMetadataEndpoints() {
		t.Fatalf("expected stream metadata support inferred via endpoint probe")
	}
	if !p.supportsMetadataSubstringFilter() {
		t.Fatalf("expected metadata substring support inferred via endpoint probe")
	}
	if p.supportsDensePatternWindowing() {
		t.Fatalf("expected dense windowing to remain conservative without explicit semver")
	}
}

func TestBackendVersionCompatibilityGate_AllowsMissingVersionHeaders(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected compatibility gate to skip missing version headers, got: %v", err)
	}
}

func TestBackendVersionCompatibilityGate_AllowsHealthProbeFailure(t *testing.T) {
	p, err := New(Config{
		BackendURL:                 "http://127.0.0.1:65535",
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected compatibility gate to skip health probe failure, got: %v", err)
	}
}

func TestBackendVersionCompatibilityGate_AllowsNonSuccessHealthStatus(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "VictoriaLogs/v1.10.0")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("unavailable"))
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                 backend.URL,
		Cache:                      cache.New(60*time.Second, 1000),
		BackendMinVersion:          "v1.30.0",
		BackendVersionCheckTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatalf("expected compatibility gate to skip non-success health status, got: %v", err)
	}
}

func TestBackendVersionCompatibilityConfig_InvalidMinVersion(t *testing.T) {
	_, err := New(Config{
		BackendURL:        "http://example.com",
		Cache:             cache.New(60*time.Second, 1000),
		BackendMinVersion: "not-a-semver",
	})
	if err == nil {
		t.Fatalf("expected invalid backend minimum version error")
	}
}

func TestPatternsSnapshotCompaction_DeduplicatesEquivalentScopes(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.patternsSnapshotEntries = map[string]patternSnapshotEntry{}
	now := time.Now().UnixNano()

	payloadA := []byte(`{"status":"success","data":[{"pattern":"a","samples":[[1,2]]}]}`)
	payloadB := []byte(`{"status":"success","data":[{"pattern":"b","samples":[[1,3]]}]}`)

	keyOld := "patterns:org-a:query=%7Bapp%3D%22api%22%7D&start=1&end=10&step=60"
	keyNew := "patterns:org-a:query=%7Bapp%3D%22api%22%7D&start=5&end=15&step=60"
	keyOther := "patterns:org-a:query=%7Bapp%3D%22worker%22%7D&start=1&end=10&step=60"

	p.patternsSnapshotEntries[keyOld] = patternSnapshotEntry{Value: append([]byte(nil), payloadA...), UpdatedAtUnixNano: now - 10, PatternCount: 1}
	p.patternsSnapshotEntries[keyNew] = patternSnapshotEntry{Value: append([]byte(nil), payloadA...), UpdatedAtUnixNano: now, PatternCount: 1}
	p.patternsSnapshotEntries[keyOther] = patternSnapshotEntry{Value: append([]byte(nil), payloadB...), UpdatedAtUnixNano: now, PatternCount: 1}

	p.cache.SetWithTTL(keyOld, payloadA, time.Minute)
	p.cache.SetWithTTL(keyNew, payloadA, time.Minute)
	p.cache.SetWithTTL(keyOther, payloadB, time.Minute)

	droppedEntries, droppedPatterns := p.compactPatternsSnapshot(patternDedupSourceMemory, "test")
	if droppedEntries != 1 {
		t.Fatalf("expected one deduplicated entry, got %d", droppedEntries)
	}
	if droppedPatterns < 0 {
		t.Fatalf("expected non-negative dropped pattern count, got %d", droppedPatterns)
	}
	if _, ok := p.patternsSnapshotEntries[keyOld]; ok {
		t.Fatalf("expected older equivalent key to be dropped")
	}
	if _, ok := p.patternsSnapshotEntries[keyNew]; !ok {
		t.Fatalf("expected newer equivalent key to be kept")
	}
	if _, ok := p.patternsSnapshotEntries[keyOther]; !ok {
		t.Fatalf("expected different-scope key to stay")
	}
	if _, ok := p.cache.Get(keyOld); ok {
		t.Fatalf("expected dropped key to be invalidated from cache")
	}
}

func TestPatternsPersistenceLoop_PersistsAndStopsOnShutdown(t *testing.T) {
	tmpDir := t.TempDir()
	persistPath := filepath.Join(tmpDir, "patterns-snapshot.json")

	p, err := New(Config{
		BackendURL:              "http://127.0.0.1:65535",
		Cache:                   cache.New(60*time.Second, 1000),
		LogLevel:                "error",
		PatternsPersistPath:     persistPath,
		PatternsPersistInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	p.Init()

	payload := []byte(`{"status":"success","data":[{"pattern":"sample","samples":[[1,1]]}]}`)
	p.recordPatternSnapshotEntry("patterns:org-a:query=%7Bapp%3D%22api%22%7D&start=1&end=2", payload, time.Now())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, readErr := os.ReadFile(persistPath)
		if readErr == nil && len(data) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, readErr := os.ReadFile(persistPath)
	if readErr != nil {
		t.Fatalf("expected persisted snapshot file: %v", readErr)
	}
	if len(data) == 0 {
		t.Fatalf("expected persisted snapshot file to be non-empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.Shutdown(ctx)

	select {
	case <-p.patternsPersistDone:
	default:
		t.Fatalf("expected patterns persistence loop to be stopped after shutdown")
	}
}

func TestRecentTailCacheBypass_Decision(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.recentTailRefreshEnabled = true
	p.recentTailRefreshWindow = 2 * time.Minute
	p.recentTailRefreshMaxStaleness = 2 * time.Second

	nearNow := httptest.NewRequest("GET", "/loki/api/v1/query_range?end=now", nil)
	if !p.shouldBypassRecentTailCache("query_range", 5*time.Second, nearNow) {
		t.Fatalf("expected near-now stale cache to be bypassed")
	}

	oldRange := httptest.NewRequest("GET", "/loki/api/v1/query_range?end=now-10m", nil)
	if p.shouldBypassRecentTailCache("query_range", 5*time.Second, oldRange) {
		t.Fatalf("expected old-range cache hit to be retained")
	}

	fresh := httptest.NewRequest("GET", "/loki/api/v1/query_range?end=now", nil)
	if p.shouldBypassRecentTailCache("query_range", 9*time.Second, fresh) {
		t.Fatalf("expected fresh near-now cache hit to be retained")
	}

	p.recentTailRefreshEnabled = false
	if p.shouldBypassRecentTailCache("query_range", 5*time.Second, nearNow) {
		t.Fatalf("expected disabled tail-refresh to retain cache")
	}
}

func TestContract_Volume_BypassesNearNowStaleCache(t *testing.T) {
	var backendCalls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		backendCalls++
		_, _ = w.Write([]byte(`{"hits":[{"fields":{"service.name":"api"},"timestamps":["2026-01-01T00:00:00Z"],"values":[3]}]}`))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	p.recentTailRefreshEnabled = true
	p.recentTailRefreshWindow = time.Hour
	p.recentTailRefreshMaxStaleness = time.Nanosecond

	uri := "/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&end=now"
	w1 := httptest.NewRecorder()
	p.handleVolume(w1, httptest.NewRequest("GET", uri, nil))
	if backendCalls != 1 {
		t.Fatalf("expected first call to hit backend once, got %d", backendCalls)
	}

	time.Sleep(2 * time.Millisecond)
	w2 := httptest.NewRecorder()
	p.handleVolume(w2, httptest.NewRequest("GET", uri, nil))
	if backendCalls < 2 {
		t.Fatalf("expected stale near-now cache to be bypassed, backend calls=%d", backendCalls)
	}
}

func TestContract_Patterns_DisabledReturnsNotFound(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer vlBackend.Close()

	disabled := false
	p, err := New(Config{
		BackendURL:      vlBackend.URL,
		PatternsEnabled: &disabled,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7B%7D&start=1&end=2", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when patterns endpoint is disabled, got %d", w.Code)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp["errorType"] != "not_found" {
		t.Fatalf("expected not_found errorType, got %v", resp["errorType"])
	}
}

func TestContract_Patterns_EmptyResultDoesNotPoisonCache(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		switch {
		case callCount <= 1:
			// First probe: query is empty.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		default:
			// Subsequent probe has data and should not be blocked by sticky empty cache.
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"web","level":"info"}` + "\n"))
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:01Z","_msg":"GET /api/users 200 22ms","app":"web","level":"info"}` + "\n"))
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	reqURL := "/loki/api/v1/patterns?query=%7B%7D&start=1&end=2"

	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest("GET", reqURL, nil)
	p.handlePatterns(w1, r1)
	var first map[string]interface{}
	mustUnmarshal(t, w1.Body.Bytes(), &first)
	firstData, _ := first["data"].([]interface{})
	if len(firstData) != 0 {
		t.Fatalf("expected first patterns probe to be empty, got %v", first)
	}

	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", reqURL, nil)
	p.handlePatterns(w2, r2)
	var second map[string]interface{}
	mustUnmarshal(t, w2.Body.Bytes(), &second)
	secondData, _ := second["data"].([]interface{})
	if len(secondData) == 0 {
		t.Fatalf("expected second patterns probe to return data (empty response must not be sticky), got %v", second)
	}
	if callCount < 2 {
		t.Fatalf("expected backend to be queried again after empty probe; got callCount=%d", callCount)
	}
}

func TestContract_PatternHelpers_ParseAndLimitPayload(t *testing.T) {
	if got := parsePatternLimit(""); got != 50 {
		t.Fatalf("expected default limit 50, got %d", got)
	}
	if got := parsePatternLimit("5000"); got != maxPatternResponseLimit {
		t.Fatalf("expected maxPatternResponseLimit clamp, got %d", got)
	}
	if got := parsePatternLimit("7"); got != 7 {
		t.Fatalf("expected explicit parsed limit 7, got %d", got)
	}

	payload, err := json.Marshal(patternsResponse{
		Status: "success",
		Data: []patternResultEntry{
			{Pattern: "alpha <_>", Samples: [][]interface{}{{"1", "alpha 1"}}},
			{Pattern: "beta <_>", Samples: [][]interface{}{{"2", "beta 2"}}},
			{Pattern: "gamma <_>", Samples: [][]interface{}{{"3", "gamma 3"}}},
		},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	limited := limitPatternPayload(payload, 2)
	var resp patternsResponse
	mustUnmarshal(t, limited, &resp)
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 limited patterns, got %d", len(resp.Data))
	}
}

func TestContract_PatternsCacheKey_NormalizesRelativeRangeBoundaries(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:65535")

	key := p.patternsAutodetectCacheKey("org-a", `{app="web"}`, "now-1h", "now", "60s")
	if key == "" {
		t.Fatal("expected non-empty patterns cache key")
	}
	if strings.Contains(key, "now-1h") || strings.Contains(key, "now") {
		t.Fatalf("expected normalized numeric boundaries in patterns cache key, got %q", key)
	}
}

func TestContract_Patterns_FillsSamplesAcrossRequestedRange(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		// Only one line near range end. Handler should fill full range buckets with zeros.
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:04:58Z","_msg":"GET /api/users 200 15ms","app":"web","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		"GET",
		"/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=2026-04-04T10:00:00Z&end=2026-04-04T10:05:00Z&step=60s&limit=1",
		nil,
	)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}

	var resp patternsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Data) != 1 {
		t.Fatalf("expected one pattern, got %v", resp.Data)
	}
	samples := resp.Data[0].Samples
	if len(samples) != 6 {
		t.Fatalf("expected six 60s buckets across selected range, got %d samples: %v", len(samples), samples)
	}
	firstTS, okFirst := numberToInt64(samples[0][0])
	lastTS, okLast := numberToInt64(samples[len(samples)-1][0])
	if !okFirst || !okLast {
		t.Fatalf("expected numeric sample timestamps, got first=%v last=%v", samples[0], samples[len(samples)-1])
	}
	if firstTS != 1775296800 || lastTS != 1775297100 {
		t.Fatalf("expected filled start/end buckets 1775296800..1775297100, got %d..%d", firstTS, lastTS)
	}
}

func TestContract_Patterns_FillsSamplesAcrossRelativeNowRange(t *testing.T) {
	now := time.Now().UTC()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintf(
			w,
			`{"_time":"%s","_msg":"GET /api/users 200 15ms","app":"web","level":"info"}`+"\n",
			now.Format(time.RFC3339Nano),
		)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		"GET",
		"/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=now-24h&end=now&limit=1",
		nil,
	)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for patterns endpoint, got %d body=%s", w.Code, w.Body.String())
	}

	var resp patternsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Data) != 1 {
		t.Fatalf("expected one pattern, got %v", resp.Data)
	}
	samples := resp.Data[0].Samples
	if len(samples) < 100 {
		t.Fatalf("expected long-range relative fill to produce >=100 buckets, got %d samples: %v", len(samples), samples)
	}
}

func TestContract_Patterns_FillCoarsensHugePointRanges(t *testing.T) {
	entries := []patternResultEntry{
		{
			Pattern: "GET <_> <_> <_>",
			Samples: [][]interface{}{
				{int64(172799), 3},
				{int64(172800), 4},
			},
		},
	}

	filled := fillPatternSamplesAcrossRequestedRange(entries, "0", "172800", "1s")
	if len(filled) != 1 {
		t.Fatalf("expected one filled entry, got %d", len(filled))
	}
	if got := len(filled[0].Samples); got <= 100 || got > 11000 {
		t.Fatalf("expected adaptive coarsening to keep buckets in 101..11000, got %d", got)
	}
	firstTS, okFirst := numberToInt64(filled[0].Samples[0][0])
	lastTS, okLast := numberToInt64(filled[0].Samples[len(filled[0].Samples)-1][0])
	if !okFirst || !okLast {
		t.Fatalf("expected numeric filled timestamps, got first=%v last=%v", filled[0].Samples[0], filled[0].Samples[len(filled[0].Samples)-1])
	}
	if firstTS != 0 || lastTS != 172800 {
		t.Fatalf("expected filled range to remain anchored to request boundaries, got %d..%d", firstTS, lastTS)
	}
}

func TestContract_Patterns_CustomPatternsPrepended(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"web","level":"info"}` + "\n"))
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:01Z","_msg":"GET /api/users 200 22ms","app":"web","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:      vlBackend.URL,
		Cache:           c,
		LogLevel:        "error",
		PatternsCustom:  []string{"always-top", "always-top", "second-custom"},
		PatternsEnabled: nil,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1&end=2&step=1m", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp patternsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Data) < 2 {
		t.Fatalf("expected prepended custom patterns, got %+v", resp.Data)
	}
	if resp.Data[0].Pattern != "always-top" || resp.Data[1].Pattern != "second-custom" {
		t.Fatalf("expected custom patterns prepended, got %+v", resp.Data[:2])
	}
	if len(resp.Data[0].Samples) != 1 || len(resp.Data[0].Samples[0]) != 2 {
		t.Fatalf("expected synthetic sample for custom pattern, got %+v", resp.Data[0].Samples)
	}
}

func TestContract_Patterns_CachedPayloadStillPrependsCustomPatterns(t *testing.T) {
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:     "http://127.0.0.1:65535",
		Cache:          c,
		LogLevel:       "error",
		PatternsCustom: []string{"cached-custom"},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	cacheKey := p.patternsAutodetectCacheKey("", `{app="web"}`, "1", "2", "1m")
	payload, err := json.Marshal(patternsResponse{
		Status: "success",
		Data: []patternResultEntry{
			{Pattern: "auto-pattern", Samples: [][]interface{}{{int64(1), 3}}},
		},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	p.cache.SetWithTTL(cacheKey, payload, patternsCacheRetention)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7Bapp%3D%22web%22%7D&start=1&end=2&step=1m&limit=2", nil)
	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp patternsResponse
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if len(resp.Data) != 2 {
		t.Fatalf("expected custom+cached patterns with limit=2, got %+v", resp.Data)
	}
	if resp.Data[0].Pattern != "cached-custom" || resp.Data[1].Pattern != "auto-pattern" {
		t.Fatalf("expected custom pattern prepended over cached payload, got %+v", resp.Data)
	}
}

func TestContract_RefreshVolumeCacheAsync_PopulatesCache(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"hits":[{"fields":{"service.name":"api"},"timestamps":["2026-01-01T00:00:00Z"],"values":[3]}]}`))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	cacheKey := "volume:test-refresh"
	p.refreshVolumeCacheAsync("", cacheKey, `{app="api"}`, "", "", "")

	deadline := time.Now().Add(2 * time.Second)
	for {
		if body, _, ok := p.cache.GetWithTTL(cacheKey); ok {
			var resp map[string]interface{}
			mustUnmarshal(t, body, &resp)
			assertLokiSuccess(t, resp)
			data := assertDataIsObject(t, resp)
			if data["resultType"] != "vector" {
				t.Fatalf("expected vector resultType, got %v", data["resultType"])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("expected background volume refresh to populate cache")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestContract_RefreshVolumeRangeCacheAsync_PopulatesCache(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"hits":[{"fields":{"service.name":"api"},"timestamps":["2026-01-01T00:00:00Z"],"values":[5]}]}`))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	cacheKey := "volume_range:test-refresh"
	p.refreshVolumeRangeCacheAsync("", cacheKey, `{app="api"}`, "", "", "60", "")

	deadline := time.Now().Add(2 * time.Second)
	for {
		if body, _, ok := p.cache.GetWithTTL(cacheKey); ok {
			var resp map[string]interface{}
			mustUnmarshal(t, body, &resp)
			assertLokiSuccess(t, resp)
			data := assertDataIsObject(t, resp)
			if data["resultType"] != "matrix" {
				t.Fatalf("expected matrix resultType, got %v", data["resultType"])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("expected background volume_range refresh to populate cache")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestContract_DrilldownLimits_DefaultPatternFlagsAdvertised(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	p.handleDrilldownLimits(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from drilldown-limits, got %d", w.Code)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp["pattern_ingester_enabled"] != false {
		t.Fatalf("expected pattern_ingester_enabled=false by default, got %v", resp["pattern_ingester_enabled"])
	}
	limits, ok := resp["limits"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected limits object, got %T", resp["limits"])
	}
	if limits["pattern_persistence_enabled"] != false {
		t.Fatalf("expected limits.pattern_persistence_enabled=false by default, got %v", limits["pattern_persistence_enabled"])
	}
}

func TestContract_DrilldownLimits_PatternFlagsReflectRuntime(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer vlBackend.Close()

	persistPath := filepath.Join(t.TempDir(), "patterns.snapshot.json")
	p, err := New(Config{
		BackendURL:                    vlBackend.URL,
		PatternsAutodetectFromQueries: true,
		PatternsPersistPath:           persistPath,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	p.handleDrilldownLimits(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from drilldown-limits, got %d", w.Code)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp["pattern_ingester_enabled"] != true {
		t.Fatalf("expected pattern_ingester_enabled=true when autodetect is enabled, got %v", resp["pattern_ingester_enabled"])
	}
	limits, ok := resp["limits"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected limits object, got %T", resp["limits"])
	}
	if limits["pattern_persistence_enabled"] != true {
		t.Fatalf("expected limits.pattern_persistence_enabled=true when persistence path configured, got %v", limits["pattern_persistence_enabled"])
	}
}

func TestContract_DrilldownLimits_PatternsDisabledAdvertised(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer vlBackend.Close()

	disabled := false
	p, err := New(Config{
		BackendURL:      vlBackend.URL,
		PatternsEnabled: &disabled,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	p.handleDrilldownLimits(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from drilldown-limits, got %d", w.Code)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if resp["pattern_ingester_enabled"] != false {
		t.Fatalf("expected pattern_ingester_enabled=false, got %v", resp["pattern_ingester_enabled"])
	}
	limits, ok := resp["limits"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected limits object, got %T", resp["limits"])
	}
	if limits["pattern_persistence_enabled"] != false {
		t.Fatalf("expected limits.pattern_persistence_enabled=false, got %v", limits["pattern_persistence_enabled"])
	}
}

func TestContract_DrilldownLimits_AdvertisesGrafanaProfilesWhenDetected(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	r.Header.Set("User-Agent", "Grafana/11.6.6")
	r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	p.handleDrilldownLimits(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from drilldown-limits, got %d", w.Code)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if got := resp["grafana_client_surface"]; got != "grafana_drilldown" {
		t.Fatalf("expected grafana_client_surface=grafana_drilldown, got %v", got)
	}
	if got := resp["grafana_runtime_version"]; got != "11.6.6" {
		t.Fatalf("expected grafana_runtime_version=11.6.6, got %v", got)
	}
	if got := resp["grafana_runtime_family"]; got != "11.x" {
		t.Fatalf("expected grafana_runtime_family=11.x, got %v", got)
	}
	if got := resp["drilldown_profile"]; got != "drilldown-v1" {
		t.Fatalf("expected drilldown_profile=drilldown-v1, got %v", got)
	}
}

func TestContract_DrilldownLimits_ExposesRequiredLimitsContract(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	p.handleDrilldownLimits(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from drilldown-limits, got %d", w.Code)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)

	requiredTopLevel := []string{
		"limits",
		"pattern_ingester_enabled",
		"version",
		"maxDetectedFields",
		"maxDetectedValues",
		"maxLabelValues",
		"maxLines",
	}
	for _, key := range requiredTopLevel {
		if _, ok := resp[key]; !ok {
			t.Fatalf("drilldown-limits missing top-level key %q: %v", key, resp)
		}
	}
	if _, ok := resp["pattern_ingester_enabled"].(bool); !ok {
		t.Fatalf("expected boolean pattern_ingester_enabled, got %T", resp["pattern_ingester_enabled"])
	}

	limits, ok := resp["limits"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected limits object, got %T", resp["limits"])
	}

	requiredLimitKeys := []string{
		"discover_log_levels",
		"discover_service_name",
		"log_level_fields",
		"max_entries_limit_per_query",
		"max_line_size_truncate",
		"max_query_bytes_read",
		"max_query_length",
		"max_query_lookback",
		"max_query_range",
		"max_query_series",
		"metric_aggregation_enabled",
		"otlp_config",
		"pattern_persistence_enabled",
		"query_timeout",
		"retention_period",
		"retention_stream",
		"volume_enabled",
		"volume_max_series",
	}
	for _, key := range requiredLimitKeys {
		if _, ok := limits[key]; !ok {
			t.Fatalf("drilldown-limits missing limits.%s in contract: %v", key, limits)
		}
	}
	if _, ok := limits["discover_service_name"].([]interface{}); !ok {
		t.Fatalf("expected limits.discover_service_name to be an array, got %T", limits["discover_service_name"])
	}
	if _, ok := limits["log_level_fields"].([]interface{}); !ok {
		t.Fatalf("expected limits.log_level_fields to be an array, got %T", limits["log_level_fields"])
	}
	if _, ok := limits["retention_stream"].([]interface{}); !ok {
		t.Fatalf("expected limits.retention_stream to be an array, got %T", limits["retention_stream"])
	}
}

func TestContract_DrilldownLimits_ExposesBackendDetectionStateWhenAvailable(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.observeBackendVersionFromHeaders(http.Header{
		"Server": []string{"VictoriaLogs/v1.50.0"},
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	p.handleDrilldownLimits(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from drilldown-limits, got %d", w.Code)
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	if got := resp["backend_version_semver"]; got != "v1.50.0" {
		t.Fatalf("expected backend_version_semver=v1.50.0, got %v", got)
	}
	if got := resp["backend_capability_profile"]; got != "vl-v1.50-plus" {
		t.Fatalf("expected backend_capability_profile=vl-v1.50-plus, got %v", got)
	}
}

func TestContract_SupportsDensePatternWindowingForRequest_GrafanaRuntimeAware(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.backendVersionMu.Lock()
	p.backendSupportsDensePatternWindowing = true
	p.backendVersionSemver = "v1.50.0"
	p.backendVersionMu.Unlock()

	baseReq := httptest.NewRequest("GET", "/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D", nil)

	legacyCtx := context.WithValue(baseReq.Context(), requestGrafanaClientKey, grafanaClientProfile{
		surface:      "grafana_drilldown",
		runtimeMajor: 11,
	})
	if p.supportsDensePatternWindowingForRequest(baseReq.WithContext(legacyCtx)) {
		t.Fatal("expected dense windowing disabled for legacy drilldown runtime family")
	}

	modernCtx := context.WithValue(baseReq.Context(), requestGrafanaClientKey, grafanaClientProfile{
		surface:      "grafana_drilldown",
		runtimeMajor: 12,
	})
	if !p.supportsDensePatternWindowingForRequest(baseReq.WithContext(modernCtx)) {
		t.Fatal("expected dense windowing enabled for modern drilldown runtime family")
	}

	if !p.supportsDensePatternWindowingForRequest(baseReq) {
		t.Fatal("expected dense windowing enabled for non-grafana request context")
	}
}

func TestPatternWindowedSamplingConfig_DenseIgnoresStepInflationAndCapsFanout(t *testing.T) {
	start := strconv.FormatInt(0, 10)
	end := strconv.FormatInt(int64((24*time.Hour)/time.Second), 10)

	startNsA, endNsA, intervalA, perWindowA, okA := patternWindowedSamplingConfig(start, end, "1s", 10000, true)
	startNsB, endNsB, intervalB, perWindowB, okB := patternWindowedSamplingConfig(start, end, "30m", 10000, true)
	if !okA || !okB {
		t.Fatal("expected dense windowed config to be enabled for long ranges")
	}
	if startNsA != startNsB || endNsA != endNsB {
		t.Fatalf("expected identical dense range bounds, got A=%d..%d B=%d..%d", startNsA, endNsA, startNsB, endNsB)
	}
	if intervalA != intervalB {
		t.Fatalf("expected dense mode to ignore step-driven inflation, got intervalA=%s intervalB=%s", intervalA, intervalB)
	}
	if perWindowA != perWindowB {
		t.Fatalf("expected dense mode per-window limit to remain stable across steps, got %d vs %d", perWindowA, perWindowB)
	}

	span := time.Duration(endNsA - startNsA)
	windowCount := int(span/intervalA) + 1
	if windowCount > 64 {
		t.Fatalf("expected dense mode window cap <=64, got %d", windowCount)
	}
	if perWindowA < 200 {
		t.Fatalf("expected dense mode per-window lower bound >=200, got %d", perWindowA)
	}
}

func TestPatternWindowedSamplingConfig_DenseLongRangeBoostsPerWindowLimit(t *testing.T) {
	start := strconv.FormatInt(0, 10)
	end := strconv.FormatInt(int64((48*time.Hour)/time.Second), 10)

	_, _, _, perWindow, ok := patternWindowedSamplingConfig(start, end, "5m", 11540, true)
	if !ok {
		t.Fatal("expected dense windowed config to be enabled for long ranges")
	}
	if perWindow < 4000 {
		t.Fatalf("expected dense long-range per-window limit >=4000, got %d", perWindow)
	}
}

func TestShouldAcceptWindowedPatternResults(t *testing.T) {
	if shouldAcceptWindowedPatternResults(1, 4, true) {
		t.Fatal("expected dense mode to reject highly partial window coverage")
	}
	if !shouldAcceptWindowedPatternResults(2, 4, true) {
		t.Fatal("expected dense mode to accept >=50% window coverage")
	}
	if !shouldAcceptWindowedPatternResults(1, 4, false) {
		t.Fatal("expected non-dense mode to accept partial coverage when there is at least one successful window")
	}
	if shouldAcceptWindowedPatternResults(0, 4, false) {
		t.Fatal("expected zero successful windows to be rejected")
	}
}

func TestMergePatternResultEntries_DeterministicTieOrder(t *testing.T) {
	base := []patternResultEntry{
		{Level: "warn", Pattern: "zeta", Samples: [][]interface{}{{int64(1), 1}}},
		{Level: "info", Pattern: "beta", Samples: [][]interface{}{{int64(1), 1}}},
	}
	extra := []patternResultEntry{
		{Level: "info", Pattern: "alpha", Samples: [][]interface{}{{int64(2), 1}}},
	}

	merged := mergePatternResultEntries(base, extra)
	if len(merged) != 3 {
		t.Fatalf("expected 3 merged patterns, got %d", len(merged))
	}
	got := []string{
		merged[0].Level + "/" + merged[0].Pattern,
		merged[1].Level + "/" + merged[1].Pattern,
		merged[2].Level + "/" + merged[2].Pattern,
	}
	want := []string{"info/alpha", "info/beta", "warn/zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected deterministic order: got=%v want=%v", got, want)
	}
}

func TestContract_DrilldownLimits_UsesRuntimeTenantOverrides(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		TenantDefaultLimits: map[string]any{
			"max_query_series": 321.0,
			"query_timeout":    "9m",
		},
		TenantLimits: map[string]map[string]any{
			"team-a": {
				"max_query_series": 111.0,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	// Default tenant falls back to global overrides.
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	p.handleDrilldownLimits(w, r)
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	limits := resp["limits"].(map[string]interface{})
	if limits["query_timeout"] != "9m" {
		t.Fatalf("expected default query_timeout override, got %v", limits["query_timeout"])
	}
	if limits["max_query_series"] != 321.0 {
		t.Fatalf("expected default max_query_series override, got %v", limits["max_query_series"])
	}

	// Tenant-specific override wins over global override.
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil)
	r.Header.Set("X-Scope-OrgID", "team-a")
	p.handleDrilldownLimits(w, r)
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	limits = resp["limits"].(map[string]interface{})
	if limits["max_query_series"] != 111.0 {
		t.Fatalf("expected tenant max_query_series override, got %v", limits["max_query_series"])
	}
}

func TestContract_TenantLimitsConfig_UsesTenantSpecificValues(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		TenantDefaultLimits: map[string]any{
			"query_timeout": "7m",
		},
		TenantLimits: map[string]map[string]any{
			"team-a": {
				"query_timeout": "11m",
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/config/tenant/v1/limits", nil)
	r.Header.Set("X-Scope-OrgID", "team-a")
	p.handleTenantLimitsConfig(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from tenant limits endpoint, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("expected text/plain from tenant limits endpoint, got %q", w.Header().Get("Content-Type"))
	}
	var limits map[string]interface{}
	if err := yaml.Unmarshal(w.Body.Bytes(), &limits); err != nil {
		t.Fatalf("expected YAML tenant limits response, got %v", err)
	}
	if limits["query_timeout"] != "11m" {
		t.Fatalf("expected tenant override query_timeout=11m, got %v", limits["query_timeout"])
	}
}

func TestContract_TenantLimitsConfig_RejectsMultiTenantHeader(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/config/tenant/v1/limits", nil)
	r.Header.Set("X-Scope-OrgID", "team-a|team-b")
	p.handleTenantLimitsConfig(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for multi-tenant header, got %d", w.Code)
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

func TestTranslation_DottedLabelFilterTripletForwarded(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte{})
	}))
	defer vlBackend.Close()

	doGet(t, vlBackend.URL, `/loki/api/v1/query_range?query=%7Bservice_name%3D%22k8s-cluster-events%22%7D+%7C+k8s.cluster.name+%3D+%60my-cluster%60&start=1&end=2&limit=10`)

	if !strings.Contains(receivedQuery, `"k8s.cluster.name":=my-cluster`) {
		t.Fatalf("expected translated dotted field filter in backend query, got %q", receivedQuery)
	}
	if !strings.HasSuffix(receivedQuery, `| sort by (_time desc)`) {
		t.Fatalf("expected backend query to keep default sort suffix, got %q", receivedQuery)
	}
}

func TestTranslation_UnderscoreLabelFilterTripletPreservedInPassthroughMode(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte{})
	}))
	defer vlBackend.Close()

	doGet(t, vlBackend.URL, `/loki/api/v1/query_range?query=%7Bservice_name%3D%22k8s-cluster-events%22%7D+%7C+k8s_cluster_name+%3D+%60my-cluster%60&start=1&end=2&limit=10`)

	if !strings.Contains(receivedQuery, `k8s_cluster_name:=my-cluster`) {
		t.Fatalf("expected underscore key to be preserved in passthrough mode, got %q", receivedQuery)
	}
	if strings.Contains(receivedQuery, "k8s . `") {
		t.Fatalf("expected normalized field filter expression, got malformed dotted stage in %q", receivedQuery)
	}
}

func TestTranslation_MalformedSpacedDottedTripletNormalizedForDatasourceOps(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte{})
	}))
	defer vlBackend.Close()

	doGet(t, vlBackend.URL, `/loki/api/v1/query_range?query=%7Bservice_name%3D%22k8s-cluster-events%22%7D+%7C+custom+.+%60pipeline.processing.%60+%3D+%60vector-processing%60&start=1&end=2&limit=10`)

	if !strings.Contains(receivedQuery, `"custom.pipeline.processing":=vector-processing`) {
		t.Fatalf("expected malformed dotted triplet to normalize to valid dotted equality, got %q", receivedQuery)
	}
	if strings.Contains(receivedQuery, "custom . `") {
		t.Fatalf("expected malformed spaced dotted syntax to be removed, got %q", receivedQuery)
	}
}

func TestTranslation_DottedFieldComplexLiteralIsQuoted(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		receivedQuery = r.FormValue("query")
		w.Write([]byte{})
	}))
	defer vlBackend.Close()

	stack := `golang.a2z.com/EKSNodeMonitoringAgent/internal/monitor/kernel.(*KernelMonitor).handleEnvironment`
	logql := `{deployment_environment="dev"} | code.stacktrace = ` + "`" + stack + "`"
	doGet(t, vlBackend.URL, "/loki/api/v1/query_range?query="+url.QueryEscape(logql)+"&start=1&end=2&limit=10")

	if !strings.Contains(receivedQuery, `"code.stacktrace":="`+stack+`"`) {
		t.Fatalf("expected complex dotted field value to be quoted in backend query, got %q", receivedQuery)
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

func TestCache_DetectedFieldServiceNameHitOnRepeat(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		switch r.URL.Path {
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"{service.name=\"grafana\"}","hits":1}]}`))
		default:
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	path := `/loki/api/v1/detected_field/service_name/values?query=%7Bdeployment_environment%3D%22dev%22%7D&start=1&end=2`

	w1 := httptest.NewRecorder()
	p.handleDetectedFieldValues(w1, httptest.NewRequest(http.MethodGet, path, nil))
	if callCount != 1 {
		t.Fatalf("expected 1 backend call on cache miss, got %d", callCount)
	}

	w2 := httptest.NewRecorder()
	p.handleDetectedFieldValues(w2, httptest.NewRequest(http.MethodGet, path, nil))
	if callCount != 1 {
		t.Fatalf("expected cache hit before backend call, got %d", callCount)
	}

	var first, second map[string]interface{}
	mustUnmarshal(t, w1.Body.Bytes(), &first)
	mustUnmarshal(t, w2.Body.Bytes(), &second)
	firstValuesRaw, ok := first["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array in first response, got %T", first["values"])
	}
	secondValuesRaw, ok := second["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array in second response, got %T", second["values"])
	}
	firstValues := make([]string, 0, len(firstValuesRaw))
	secondValues := make([]string, 0, len(secondValuesRaw))
	for _, item := range firstValuesRaw {
		firstValues = append(firstValues, fmt.Sprint(item))
	}
	for _, item := range secondValuesRaw {
		secondValues = append(secondValues, fmt.Sprint(item))
	}
	if len(firstValues) != len(secondValues) || firstValues[0] != secondValues[0] {
		t.Fatalf("expected cached detected_field values to match original, got %v vs %v", firstValues, secondValues)
	}
}

func TestCacheTTLs_MetadataDiscoveryExtended(t *testing.T) {
	if got := CacheTTLs["labels"]; got < 2*time.Minute {
		t.Fatalf("labels TTL must stay >= 2m, got %s", got)
	}
	if got := CacheTTLs["label_values"]; got < 2*time.Minute {
		t.Fatalf("label_values TTL must stay >= 2m, got %s", got)
	}
	if got := CacheTTLs["label_inventory"]; got < 5*time.Minute {
		t.Fatalf("label_inventory TTL must stay >= 5m, got %s", got)
	}
	if got := CacheTTLs["detected_fields"]; got < 90*time.Second {
		t.Fatalf("detected_fields TTL must stay >= 90s, got %s", got)
	}
	if got := CacheTTLs["detected_field_values"]; got < 90*time.Second {
		t.Fatalf("detected_field_values TTL must stay >= 90s, got %s", got)
	}
	if got := CacheTTLs["detected_labels"]; got < 90*time.Second {
		t.Fatalf("detected_labels TTL must stay >= 90s, got %s", got)
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

func TestContract_Labels_ForwardsSubstringFilter_OnV149Plus(t *testing.T) {
	var receivedQ, receivedFilter string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQ = r.URL.Query().Get("q")
		receivedFilter = r.URL.Query().Get("filter")
		writeVLFieldNames(w, []fieldHit{{"app", 1}, {"level", 1}})
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	h := http.Header{}
	h.Set("Server", "VictoriaLogs/v1.49.0")
	p.observeBackendVersionFromHeaders(h)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/labels?query={app="nginx"}&search=app`, nil)
	p.handleLabels(w, r)

	if receivedQ != "app" {
		t.Fatalf("expected q=app to be forwarded for substring filter, got %q", receivedQ)
	}
	if receivedFilter != "substring" {
		t.Fatalf("expected filter=substring for VictoriaLogs >= v1.49.0, got %q", receivedFilter)
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
