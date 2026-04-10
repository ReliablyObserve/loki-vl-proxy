package proxy

import (
	"encoding/json"
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

func TestRequestWantsCategorizedLabels(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	if requestWantsCategorizedLabels(req) {
		t.Fatal("unexpected categorize-labels for empty header")
	}

	req.Header.Set(lokiResponseEncodingFlagsHeader, "foo, categorize-labels,bar")
	if !requestWantsCategorizedLabels(req) {
		t.Fatal("expected categorize-labels to be detected from flag list")
	}

	req.Header.Set(lokiResponseEncodingFlagsHeader, "foo,bar")
	if requestWantsCategorizedLabels(req) {
		t.Fatal("did not expect categorize-labels for unrelated flags")
	}
}

func TestShouldEmitStructuredMetadata(t *testing.T) {
	pDisabled := newStreamMetadataProxy(t, "http://unused", false, false)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(lokiResponseEncodingFlagsHeader, "categorize-labels")
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
	req.Header.Set(lokiResponseEncodingFlagsHeader, "categorize-labels")
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple categorize-labels response, got %#v", tuple)
	}
	meta, ok := tuple[2].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata object at tuple[2], got %T", tuple[2])
	}
	if _, ok := meta["structuredMetadata"]; !ok {
		t.Fatalf("expected Loki structuredMetadata payload, got %#v", meta)
	}
	if _, ok := meta["structured_metadata"]; ok {
		t.Fatalf("non-Loki structured_metadata alias must not be emitted, got %#v", meta)
	}
}

func TestQuery_CategorizeLabelsReturnsThreeTuple(t *testing.T) {
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"line","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","level":"info"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, true, false)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"}`)
	req := httptest.NewRequest("GET", "/loki/api/v1/query?"+q.Encode(), nil)
	req.Header.Set(lokiResponseEncodingFlagsHeader, "categorize-labels")
	rec := httptest.NewRecorder()

	p.handleQuery(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple categorize-labels query response, got %#v", tuple)
	}
}

func TestQuery_DefaultRequestStaysTwoTupleForStrictDecoders(t *testing.T) {
	vlBackend := backendWithSingleNDJSONLine(t, `{"_time":"2026-01-01T00:00:00Z","_msg":"line","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","level":"info"}`)
	defer vlBackend.Close()

	p := newStreamMetadataProxy(t, vlBackend.URL, true, false)
	q := url.Values{}
	q.Set("query", `{job="otel-proxy"}`)
	req := httptest.NewRequest("GET", "/loki/api/v1/query?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	p.handleQuery(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 2 {
		t.Fatalf("expected strict 2-tuple default query response, got %#v", tuple)
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
	req.Header.Set(lokiResponseEncodingFlagsHeader, "categorize-labels")
	rec := httptest.NewRecorder()

	p.handleQueryRange(rec, req)
	tuple := decodeFirstTuple(t, rec.Body.Bytes())
	if len(tuple) != 3 {
		t.Fatalf("expected stream-mode 3-tuple value, got %#v", tuple)
	}
	meta, ok := tuple[2].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata object at tuple[2], got %T", tuple[2])
	}
	if _, ok := meta["parsed"]; !ok {
		t.Fatalf("expected parsed payload for parser-chain query, got %#v", meta)
	}
}

func TestBuildStreamValue_CategorizeLabelsEmitsEmptyObjectWhenNoMetadata(t *testing.T) {
	tuple, ok := buildStreamValue("1", "line", nil, true).([]interface{})
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
	_, metadata := p.classifyEntryFields(map[string]interface{}{
		"_time":        "2026-01-01T00:00:00Z",
		"_msg":         "line",
		"_stream":      `{job="otel-proxy",level="info"}`,
		"service.name": "otel-app",
	}, `{job="otel-proxy"}`)
	if _, ok := metadata["structuredMetadata"]; !ok {
		t.Fatalf("expected structuredMetadata key, got %#v", metadata)
	}
	if _, ok := metadata["structured_metadata"]; ok {
		t.Fatalf("did not expect structured_metadata alias, got %#v", metadata)
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
