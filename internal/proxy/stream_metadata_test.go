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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newStreamMetadataProxy(t *testing.T, backendURL string, emitStructuredMetadata, streamResponse bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             backendURL,
		Cache:                  cache.New(30*time.Second, 100),
		LogLevel:               "error",
		EmitStructuredMetadata: emitStructuredMetadata,
		StreamResponse:         streamResponse,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStylePassthrough,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

func backendWithSingleNDJSONLine(t *testing.T, line string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(line + "\n"))
	}))
}

func decodeFirstTuple(t *testing.T, body []byte) []interface{} {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("expected one stream result, got %#v", resp.Data.Result)
	}
	if len(resp.Data.Result[0].Values) != 1 {
		t.Fatalf("expected one tuple value, got %#v", resp.Data.Result[0].Values)
	}
	return resp.Data.Result[0].Values[0]
}

// metadataObjectToMap decodes Loki categorized metadata object maps: {"name":"value", ...}.
func metadataObjectToMap(t *testing.T, raw interface{}) map[string]string {
	t.Helper()
	switch typed := raw.(type) {
	case map[string]string:
		out := make(map[string]string, len(typed))
		for name, value := range typed {
			if name == "" {
				t.Fatalf("expected non-empty metadata key in %#v", typed)
			}
			out[name] = value
		}
		return out
	case map[string]interface{}:
		items := typed
		out := make(map[string]string, len(items))
		for name, value := range items {
			if name == "" {
				t.Fatalf("expected non-empty metadata key in %#v", items)
			}
			switch typedValue := value.(type) {
			case string:
				out[name] = typedValue
			case bool:
				if typedValue {
					out[name] = "true"
				} else {
					out[name] = "false"
				}
			case float64:
				out[name] = strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", typedValue), "0"), ".")
			default:
				out[name] = fmt.Sprintf("%v", typedValue)
			}
		}
		return out
	default:
		t.Fatalf("expected metadata object map, got %T", raw)
	}
	return map[string]string{}
}

func responseHasEncodingFlag(t *testing.T, body []byte, want string) bool {
	t.Helper()
	var payload struct {
		Data struct {
			EncodingFlags []string `json:"encodingFlags"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode response for encodingFlags: %v", err)
	}
	for _, flag := range payload.Data.EncodingFlags {
		if flag == want {
			return true
		}
	}
	return false
}

func TestRequestWantsCategorizedLabels(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	if requestWantsCategorizedLabels(req) {
		t.Fatal("unexpected categorize-labels for empty header")
	}

	req.Header.Set("X-Loki-Response-Encoding-Flags", "foo, categorize-labels,bar")
	if !requestWantsCategorizedLabels(req) {
		t.Fatal("expected categorize-labels to be detected from flag list")
	}

	req.Header.Set("X-Loki-Response-Encoding-Flags", "foo,bar")
	if requestWantsCategorizedLabels(req) {
		t.Fatal("did not expect categorize-labels for unrelated flags")
	}
}

func TestShouldEmitStructuredMetadata(t *testing.T) {
	pDisabled := newStreamMetadataProxy(t, "http://unused", false, false)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	if pDisabled.shouldEmitStructuredMetadata(req) {
		t.Fatal("must not emit metadata when feature flag is disabled")
	}

	pEnabled := newStreamMetadataProxy(t, "http://unused", true, false)
	noFlags := httptest.NewRequest("GET", "/", nil)
	if pEnabled.shouldEmitStructuredMetadata(noFlags) {
		t.Fatal("must not emit metadata for default requests without flags")
	}
	if !pEnabled.shouldEmitStructuredMetadata(req) {
		t.Fatal("expected metadata emission when categorize-labels flag is present")
	}
}

func TestQueryRange_DefaultRequestStaysTwoTupleForStrictDecoders(t *testing.T) {
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"line","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","level":"info"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, true, false)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"}`)
	q.Set("start", "1")
	q.Set("end", "2")
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 2 {
		t.Fatalf("expected strict 2-tuple default query_range response, got %#v", tuple)
	}
}

func TestQueryRange_CategorizeLabelsReturnsLokiThreeTuple(t *testing.T) {
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"line","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","level":"info"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, true, false)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"}`)
	q.Set("start", "1")
	q.Set("end", "2")
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple categorize-labels response, got %#v", tuple)
	}
	if !responseHasEncodingFlag(t, rec.Body.Bytes(), "categorize-labels") {
		t.Fatalf("expected encodingFlags to include categorize-labels, body=%s", rec.Body.String())
	}
	meta, ok := tuple[2].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata object at tuple[2], got %T", tuple[2])
	}
	structuredRaw, ok := meta["structuredMetadata"]
	if !ok {
		t.Fatalf("expected Loki structuredMetadata payload, got %#v", meta)
	}
	structured := metadataObjectToMap(t, structuredRaw)
	if got := structured["service.name"]; got != "otel-app" {
		t.Fatalf("expected service.name pair in structured metadata, got %#v", structured)
	}
	if _, ok := meta["structured_metadata"]; ok {
		t.Fatalf("non-Loki structured_metadata alias must not be emitted, got %#v", meta)
	}
}

func TestQueryRange_CategorizeLabelsFeatureDisabledStillReturnsParserSafeTuple(t *testing.T) {
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"line","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","level":"info"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, false, false)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"}`)
	q.Set("start", "1")
	q.Set("end", "2")
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple categorize-labels response even when metadata feature is disabled, got %#v", tuple)
	}
	if !responseHasEncodingFlag(t, rec.Body.Bytes(), "categorize-labels") {
		t.Fatalf("expected encodingFlags to include categorize-labels, body=%s", rec.Body.String())
	}
	meta, ok := tuple[2].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata object at tuple[2], got %T", tuple[2])
	}
	if len(meta) != 0 {
		t.Fatalf("expected empty metadata object with feature disabled, got %#v", meta)
	}
}

func TestQuery_DefaultRequestStaysTwoTupleForStrictDecoders(t *testing.T) {
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"line","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","level":"info"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, true, false)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"}`)
	q.Set("start", "1")
	q.Set("end", "2")
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 2 {
		t.Fatalf("expected strict 2-tuple default query response, got %#v", tuple)
	}
}

func TestQuery_CategorizeLabelsReturnsThreeTuple(t *testing.T) {
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"line","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","level":"info"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, true, false)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"}`)
	q.Set("start", "1")
	q.Set("end", "2")
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple categorize-labels query response, got %#v", tuple)
	}
	if !responseHasEncodingFlag(t, rec.Body.Bytes(), "categorize-labels") {
		t.Fatalf("expected encodingFlags to include categorize-labels, body=%s", rec.Body.String())
	}
}

func TestStreamLogQuery_StreamingModePreservesThreeTupleMetadata(t *testing.T) {
	// Brace-heavy line + parser chain protects the regression that previously broke Drilldown/Explore.
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"time=\"2026-01-01T00:00:00Z\" level=error msg=\"Failed to call api\" details=\"{\\\"code\\\":500,\\\"path\\\":\\\"/api/v1/orders\\\"}\"","_stream":"{job=\"otel-proxy\",level=\"error\"}","http.status_code":"500","level":"error"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, true, true)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"} | json | logfmt | drop __error__, __error_details__`)
	q.Set("start", "1")
	q.Set("end", "2")
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 3 {
		t.Fatalf("expected stream-mode 3-tuple value, got %#v", tuple)
	}
	if !responseHasEncodingFlag(t, rec.Body.Bytes(), "categorize-labels") {
		t.Fatalf("expected encodingFlags to include categorize-labels, body=%s", rec.Body.String())
	}
	meta, ok := tuple[2].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata object at tuple[2], got %T", tuple[2])
	}
	parsedRaw, ok := meta["parsed"]
	if !ok {
		t.Fatalf("expected parsed payload for parser-chain query, got %#v", meta)
	}
	parsed := metadataObjectToMap(t, parsedRaw)
	if got := parsed["http.status_code"]; got != "500" {
		t.Fatalf("expected http.status_code in parsed payload, got %#v", parsed)
	}
}

func TestBuildStreamValue_CategorizeLabelsEmitsEmptyObjectWhenNoMetadata(t *testing.T) {
	tuple, ok := buildStreamValue("1", "line", nil, nil, true, true).([]interface{})
	if !ok {
		t.Fatalf("expected tuple slice, got %T", tuple)
	}
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple for categorize-labels path, got %#v", tuple)
	}
	if _, ok := tuple[2].(map[string]interface{}); !ok {
		t.Fatalf("expected empty metadata object in tuple[2], got %#v", tuple[2])
	}
}

func TestClassifyEntryFields_UsesLokiCanonicalMetadataKeysOnly(t *testing.T) {
	p := newStreamMetadataProxy(t, "http://unused", true, false)
	ec := make(map[string][]metadataFieldExposure, 4)
	sm := make(map[string]string, 4)
	pf := make(map[string]string, 4)
	_, structuredMetadata, parsed := p.classifyEntryFields(map[string]interface{}{
		"_time":        "2026-01-01T00:00:00Z",
		"_msg":         "line",
		"_stream":      `{job="otel-proxy",level="info"}`,
		"service.name": "otel-app",
	}, `{job="otel-proxy"}`, ec, sm, pf)

	tuple, ok := buildStreamValue("1", "line", structuredMetadata, parsed, true, true).([]interface{})
	if !ok || len(tuple) != 3 {
		t.Fatalf("expected metadata tuple from classified fields, got %#v", tuple)
	}
	meta, ok := tuple[2].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata object in tuple[2], got %#v", tuple[2])
	}
	structuredRaw, ok := meta["structuredMetadata"]
	if !ok {
		t.Fatalf("expected structuredMetadata key, got %#v", meta)
	}
	structured := metadataObjectToMap(t, structuredRaw)
	if got := structured["service.name"]; got != "otel-app" {
		t.Fatalf("expected service.name in structuredMetadata payload, got %#v", structured)
	}
	if _, ok := meta["structured_metadata"]; ok {
		t.Fatalf("did not expect structured_metadata alias, got %#v", meta)
	}
}

func TestQueryRange_DefaultParserChainStillStaysTwoTuple(t *testing.T) {
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"time=\"2026-01-01T00:00:00Z\" level=error msg=\"api call failed\"","_stream":"{job=\"otel-proxy\",level=\"error\"}","http.status_code":"500","level":"error"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, true, false)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"} |= "api" | json | logfmt | drop __error__, __error_details__`)
	q.Set("start", "1")
	q.Set("end", "2")
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	if strings.Contains(rec.Body.String(), `"structuredMetadata"`) {
		t.Fatalf("default parser-chain response must remain 2-tuple, got body=%s", rec.Body.String())
	}
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 2 {
		t.Fatalf("expected 2-tuple default parser-chain response, got %#v", tuple)
	}
}
