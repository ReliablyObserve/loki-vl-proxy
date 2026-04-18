package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHandleDetectedFieldsAndValuesReuseCachedScan(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"service.name","hits":1}]}`)
		case "/select/logsql/query":
			backendCalls++
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"{\"method\":\"GET\"}","service.name":"api"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)

	r1 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_fields?query={service_name="api"}&start=1&end=2&limit=25`, nil)
	w1 := httptest.NewRecorder()
	p.handleDetectedFields(w1, r1)
	if w1.Code != http.StatusOK {
		t.Fatalf("detected_fields code=%d body=%s", w1.Code, w1.Body.String())
	}

	r2 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_field/method/values?query={service_name="api"}&start=1&end=2&limit=25`, nil)
	w2 := httptest.NewRecorder()
	p.handleDetectedFieldValues(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("detected_field_values code=%d body=%s", w2.Code, w2.Body.String())
	}

	if backendCalls != 1 {
		t.Fatalf("expected one backend scan reused through cache, got %d calls", backendCalls)
	}
}

func TestHandleDetectedLabelsReuseCachedScan(t *testing.T) {
	var streamCalls int
	var scanCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			streamCalls++
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"{cluster=\"eu\",service.name=\"api\"}","hits":1}]}`)
		case "/select/logsql/query":
			scanCalls++
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"ok","_stream":"{cluster=\"eu\",service.name=\"api\"}","level":"info"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)

	r1 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_labels?query={service_name="api"}&start=1&end=2&limit=25`, nil)
	w1 := httptest.NewRecorder()
	p.handleDetectedLabels(w1, r1)
	if w1.Code != http.StatusOK {
		t.Fatalf("detected_labels code=%d body=%s", w1.Code, w1.Body.String())
	}

	r2 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_labels?query={service_name="api"}&start=1&end=2&limit=25`, nil)
	w2 := httptest.NewRecorder()
	p.handleDetectedLabels(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("detected_labels cached code=%d body=%s", w2.Code, w2.Body.String())
	}

	if streamCalls != 1 || scanCalls != 1 {
		t.Fatalf("expected detected_labels cache reuse after one native call and one scan supplement, got streamCalls=%d scanCalls=%d", streamCalls, scanCalls)
	}
}

func TestHandleDetectedLabels_BackendErrorReturnsEmptySuccess(t *testing.T) {
	backend := httptest.NewServer(http.NotFoundHandler())
	backendURL := backend.URL
	backend.Close()

	p := newTestProxy(t, backendURL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_labels?query={service_name="api"}&limit=17`, nil)

	p.handleDetectedLabels(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Status         string        `json:"status"`
		Data           []interface{} `json:"data"`
		DetectedLabels []interface{} `json:"detectedLabels"`
		Limit          int           `json:"limit"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "success" || len(resp.Data) != 0 || len(resp.DetectedLabels) != 0 || resp.Limit != 17 {
		t.Fatalf("expected empty detected_labels success response, got %#v", resp)
	}
}

func TestHandleDetectedLabels_DoesNotCacheTransientErrorFallback(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var streamCalls atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			http.Error(w, `{"error":"transient backend failure"}`, http.StatusBadGateway)
			return
		}
		switch r.URL.Path {
		case "/select/logsql/streams":
			streamCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"{service.name=\"api\",level=\"info\"}","hits":1}]}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	reqPath := `/loki/api/v1/detected_labels?query={service_name="api"}&start=1&end=2&limit=25`

	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest(http.MethodGet, reqPath, nil)
	p.handleDetectedLabels(w1, r1)
	if w1.Code != http.StatusOK {
		t.Fatalf("first call expected 200, got %d body=%s", w1.Code, w1.Body.String())
	}

	fail.Store(false)

	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodGet, reqPath, nil)
	p.handleDetectedLabels(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("second call expected 200, got %d body=%s", w2.Code, w2.Body.String())
	}

	var resp struct {
		DetectedLabels []struct {
			Label string `json:"label"`
		} `json:"detectedLabels"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.DetectedLabels) == 0 {
		t.Fatalf("expected detected labels after backend recovery, got empty: %s (stream_calls=%d)", w2.Body.String(), streamCalls.Load())
	}
	foundService := false
	for _, item := range resp.DetectedLabels {
		if item.Label == "service_name" {
			foundService = true
			break
		}
	}
	if !foundService {
		t.Fatalf("expected service_name detected label after recovery, got %s", w2.Body.String())
	}
}

func TestHandleDetectedFields_DoesNotCacheTransientErrorFallback(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			http.Error(w, `{"error":"transient backend failure"}`, http.StatusBadGateway)
			return
		}
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"service.name","hits":1},{"value":"trace_id","hits":1}]}`)
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"ok","service.name":"api","trace_id":"abc123"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	reqPath := `/loki/api/v1/detected_fields?query={service_name="api"}&start=1&end=2&limit=25`

	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest(http.MethodGet, reqPath, nil)
	p.handleDetectedFields(w1, r1)
	if w1.Code != http.StatusOK {
		t.Fatalf("first call expected 200, got %d body=%s", w1.Code, w1.Body.String())
	}

	fail.Store(false)

	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodGet, reqPath, nil)
	p.handleDetectedFields(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("second call expected 200, got %d body=%s", w2.Code, w2.Body.String())
	}

	var resp struct {
		Fields []struct {
			Label string `json:"label"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Fields) == 0 {
		t.Fatalf("expected detected fields after backend recovery, got empty: %s", w2.Body.String())
	}
}

func TestLabelValuesServiceName_StripsFieldStagesForDrilldownQueries(t *testing.T) {
	var receivedQuery string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			http.NotFound(w, r)
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"service.name","hits":1}]}`)
		case "/select/logsql/field_values":
			receivedQuery = r.URL.Query().Get("query")
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(receivedQuery, "source_message_bytes") || strings.Contains(receivedQuery, "unpack_json") || strings.Contains(receivedQuery, "logfmt") {
				fmt.Fprintln(w, `{"values":[]}`)
				return
			}
			fmt.Fprintln(w, `{"values":[{"value":"grafana","hits":1}]}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		http.MethodGet,
		`/loki/api/v1/label/service_name/values?query=%7Bdeployment_environment%3D%22dev%22%7D+%7C+json+%7C+source_message_bytes%3D%2289%22&start=1&end=2`,
		nil,
	)
	p.handleLabelValues(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(receivedQuery, "source_message_bytes") || strings.Contains(receivedQuery, "unpack_json") || strings.Contains(receivedQuery, "logfmt") {
		t.Fatalf("service_name lookup should strip field-detection stages, got query %q", receivedQuery)
	}

	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) == 0 || resp.Data[0] != "grafana" {
		t.Fatalf("expected derived service_name value, got %#v (query=%q)", resp.Data, receivedQuery)
	}
}

func TestLabels_StripsFieldStagesForDrilldownQueries(t *testing.T) {
	var receivedQueries []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stream_field_names" && r.URL.Path != "/select/logsql/field_names" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		query := r.URL.Query().Get("query")
		receivedQueries = append(receivedQueries, query)
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(query, "source_message_bytes") || strings.Contains(query, "unpack_json") || strings.Contains(query, "logfmt") {
			fmt.Fprintln(w, `{"values":[]}`)
			return
		}
		fmt.Fprintln(w, `{"values":[{"value":"app","hits":1},{"value":"level","hits":1}]}`)
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		http.MethodGet,
		`/loki/api/v1/labels?query=%7Bdeployment_environment%3D%22dev%22%7D+%7C+json+%7C+source_message_bytes%3D%2289%22&start=1&end=2`,
		nil,
	)
	p.handleLabels(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if len(receivedQueries) == 0 {
		t.Fatal("expected metadata labels lookup to hit backend")
	}
	lastQuery := receivedQueries[len(receivedQueries)-1]
	if strings.Contains(lastQuery, "source_message_bytes") || strings.Contains(lastQuery, "unpack_json") || strings.Contains(lastQuery, "logfmt") {
		t.Fatalf("labels lookup should strip field-detection stages, got query %q (all=%v)", lastQuery, receivedQueries)
	}

	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) == 0 || resp.Data[0] != "app" {
		t.Fatalf("expected scoped labels after relaxed fallback, got %#v (queries=%v)", resp.Data, receivedQueries)
	}
}

func TestLabelValues_StripsFieldStagesForDrilldownQueries(t *testing.T) {
	var receivedValueQueries []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"app","hits":1}]}`)
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			query := r.URL.Query().Get("query")
			receivedValueQueries = append(receivedValueQueries, query)
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(query, "source_message_bytes") || strings.Contains(query, "unpack_json") || strings.Contains(query, "logfmt") {
				fmt.Fprintln(w, `{"values":[]}`)
				return
			}
			fmt.Fprintln(w, `{"values":[{"value":"grafana","hits":1}]}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		http.MethodGet,
		`/loki/api/v1/label/app/values?query=%7Bdeployment_environment%3D%22dev%22%7D+%7C+json+%7C+source_message_bytes%3D%2289%22&start=1&end=2`,
		nil,
	)
	p.handleLabelValues(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if len(receivedValueQueries) == 0 {
		t.Fatal("expected label values lookup to hit backend")
	}
	lastQuery := receivedValueQueries[len(receivedValueQueries)-1]
	if strings.Contains(lastQuery, "source_message_bytes") || strings.Contains(lastQuery, "unpack_json") || strings.Contains(lastQuery, "logfmt") {
		t.Fatalf("label values lookup should strip field-detection stages, got query %q (all=%v)", lastQuery, receivedValueQueries)
	}

	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 1 || resp.Data[0] != "grafana" {
		t.Fatalf("expected scoped label values after relaxed fallback, got %#v (queries=%v)", resp.Data, receivedValueQueries)
	}
}

func TestDetectedLabels_ScannedFallback_StripsFieldStagesForDrilldownQueries(t *testing.T) {
	var scanQueries []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			http.Error(w, "backend unavailable", http.StatusBadGateway)
		case "/select/logsql/query":
			query := r.FormValue("query")
			scanQueries = append(scanQueries, query)
			w.Header().Set("Content-Type", "application/x-ndjson")
			if strings.Contains(query, "source_message_bytes") || strings.Contains(query, "unpack_json") || strings.Contains(query, "logfmt") {
				http.Error(w, "unsupported parser stage", http.StatusBadRequest)
				return
			}
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"ok","_stream":"{app=\"grafana\",level=\"info\"}"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		http.MethodGet,
		`/loki/api/v1/detected_labels?query=%7Bdeployment_environment%3D%22dev%22%7D+%7C+json+%7C+source_message_bytes%3D%2289%22&start=1&end=2`,
		nil,
	)
	p.handleDetectedLabels(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if len(scanQueries) < 2 {
		t.Fatalf("expected detected_labels scan fallback to retry with relaxed query, got %v", scanQueries)
	}
	lastQuery := scanQueries[len(scanQueries)-1]
	if strings.Contains(lastQuery, "source_message_bytes") || strings.Contains(lastQuery, "unpack_json") || strings.Contains(lastQuery, "logfmt") {
		t.Fatalf("detected_labels scan fallback should strip field-detection stages, got query %q (all=%v)", lastQuery, scanQueries)
	}

	var resp struct {
		DetectedLabels []map[string]interface{} `json:"detectedLabels"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.DetectedLabels) == 0 {
		t.Fatalf("expected detected_labels after relaxed fallback, got empty response (queries=%v body=%s)", scanQueries, w.Body.String())
	}
}

func TestDetectedLabelValuesForServiceName_SurviveDetectedLabelsCache(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[{"value":"{service.name=\"api\",level=\"info\"}","hits":1}]}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	ctx := context.Background()
	query := `{service_name="api"}`
	start := "1"
	end := "2"
	limit := 25

	labels, _, err := p.detectLabels(ctx, query, start, end, limit)
	if err != nil {
		t.Fatalf("detectLabels returned error: %v", err)
	}
	if len(labels) == 0 {
		t.Fatalf("detectLabels returned no labels")
	}

	values := p.detectedLabelValuesForField(ctx, "service_name", query, start, end, limit)
	if len(values) != 1 || values[0] != "api" {
		t.Fatalf("expected cached service_name values to contain api, got %v", values)
	}
}

func TestHandleDetectedFieldValues_LevelFallsBackToDetectedLevel(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"values":[]}`)
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"level=error","detected_level":"error"}`)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/detected_field/level/values?query={app="api"}&limit=9`, nil)

	p.handleDetectedFieldValues(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Values []string `json:"values"`
		Limit  int      `json:"limit"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Values) != 1 || resp.Values[0] != "error" || resp.Limit != 9 {
		t.Fatalf("expected detected_level fallback, got %#v", resp)
	}
}

func TestHandlePatternsReuseCachedResponse(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		backendCalls++
		w.Header().Set("Content-Type", "application/x-ndjson")
		fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"GET /api/users 200 15ms"}`)
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)

	r1 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&step=1m&limit=10`, nil)
	w1 := httptest.NewRecorder()
	p.handlePatterns(w1, r1)
	if w1.Code != http.StatusOK {
		t.Fatalf("patterns code=%d body=%s", w1.Code, w1.Body.String())
	}

	r2 := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&step=1m&limit=10`, nil)
	w2 := httptest.NewRecorder()
	p.handlePatterns(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("patterns cached code=%d body=%s", w2.Code, w2.Body.String())
	}

	if backendCalls != 1 {
		t.Fatalf("expected patterns cache reuse, got %d backend calls", backendCalls)
	}
}

func TestHandlePatterns_UsesAutodetectWarmCacheAcrossLimitVariants(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		backendCalls++
		w.Header().Set("Content-Type", "application/x-ndjson")
		fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"GET /api/users 200 15ms","level":"info"}`)
		fmt.Fprintln(w, `{"_time":"2024-01-15T10:30:01Z","_msg":"GET /api/users 200 17ms","level":"info"}`)
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	p.patternsAutodetectFromQueries = true

	queryReq := httptest.NewRequest(http.MethodGet, `/loki/api/v1/query?query={app="api"}&start=1&end=2`, nil)
	queryReq.Header.Set("X-Scope-OrgID", "tenant-a")
	queryW := httptest.NewRecorder()
	p.proxyLogQuery(queryW, queryReq, `{app="api"}`)
	if queryW.Code != http.StatusOK {
		t.Fatalf("query code=%d body=%s", queryW.Code, queryW.Body.String())
	}

	patternReq := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&start=1&end=2&limit=1`, nil)
	patternReq.Header.Set("X-Scope-OrgID", "tenant-a")
	patternW := httptest.NewRecorder()
	p.handlePatterns(patternW, patternReq)
	if patternW.Code != http.StatusOK {
		t.Fatalf("patterns code=%d body=%s", patternW.Code, patternW.Body.String())
	}

	var payload struct {
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(patternW.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode patterns response: %v", err)
	}
	if len(payload.Data) != 1 {
		t.Fatalf("expected limit=1 payload from warm cache, got %d", len(payload.Data))
	}
	if backendCalls != 1 {
		t.Fatalf("expected patterns to reuse warm cache without extra backend call, got %d backend calls", backendCalls)
	}
}

func TestHandleMultiTenantFanoutRejectsExcessiveTenantCount(t *testing.T) {
	p := newTestProxy(t, "http://example.com")
	tenants := make([]string, 0, maxMultiTenantFanout+1)
	for i := 0; i < maxMultiTenantFanout+1; i++ {
		tenants = append(tenants, fmt.Sprintf("tenant-%02d", i))
	}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	r.Header.Set("X-Scope-OrgID", strings.Join(tenants, "|"))
	w := httptest.NewRecorder()

	if !p.handleMultiTenantFanout(w, r, "labels", p.handleLabels) {
		t.Fatal("expected multi-tenant fanout path to handle oversized tenant set")
	}
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized fanout, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestSyntheticTailSeenBoundsMemory(t *testing.T) {
	seen := newSyntheticTailSeen(4)
	for i := 0; i < 6; i++ {
		seen.Add(fmt.Sprintf("k-%d", i))
	}
	if seen.Contains("k-0") || seen.Contains("k-1") {
		t.Fatal("expected oldest entries to be evicted from synthetic tail dedup set")
	}
	for _, key := range []string{"k-2", "k-3", "k-4", "k-5"} {
		if !seen.Contains(key) {
			t.Fatalf("expected key %s to remain in bounded dedup set", key)
		}
	}
}

func TestHandlePatterns_InvalidQueryReturnsEmptySuccess(t *testing.T) {
	var backendCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		t.Fatalf("backend should not be called for invalid query")
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"&step=1m`, nil)

	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if backendCalls != 0 {
		t.Fatalf("expected no backend calls, got %d", backendCalls)
	}
	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "success" || len(resp.Data) != 0 {
		t.Fatalf("expected empty success response, got %#v", resp)
	}
}

func TestHandlePatterns_BackendFailureReturnsEmptySuccess(t *testing.T) {
	backend := httptest.NewServer(http.NotFoundHandler())
	backendURL := backend.URL
	backend.Close()

	p := newTestProxy(t, backendURL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, `/loki/api/v1/patterns?query={app="api"}&step=1m`, nil)

	p.handlePatterns(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "success" || len(resp.Data) != 0 {
		t.Fatalf("expected empty success response, got %#v", resp)
	}
}

func TestBufferedResponseWriterInitializesHeaderAndCapturesBody(t *testing.T) {
	bw := &bufferedResponseWriter{}
	bw.Header().Set("Content-Type", "application/json")
	if _, err := bw.Write([]byte(`{"status":"success"}`)); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if got := bw.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected content type to be preserved, got %q", got)
	}
	if got := string(bw.body); got != `{"status":"success"}` {
		t.Fatalf("unexpected buffered body %q", got)
	}
}

func TestParseDetectedLineLimit(t *testing.T) {
	cases := []struct {
		name string
		url  string
		want int
	}{
		{name: "default", url: "/loki/api/v1/detected_fields", want: 1000},
		{name: "line_limit", url: "/loki/api/v1/detected_fields?line_limit=25", want: 25},
		{name: "limit_overrides", url: "/loki/api/v1/detected_fields?line_limit=25&limit=11", want: 11},
		{name: "invalid_values", url: "/loki/api/v1/detected_fields?line_limit=bad&limit=-1", want: 1000},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, tc.url, nil)
			if got := parseDetectedLineLimit(r); got != tc.want {
				t.Fatalf("parseDetectedLineLimit() = %d, want %d", got, tc.want)
			}
		})
	}
}
