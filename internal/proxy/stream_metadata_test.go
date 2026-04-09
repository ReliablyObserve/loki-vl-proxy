package proxy

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newStreamMetadataTestProxy(t *testing.T, emitStructuredMetadata bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             "http://unused",
		Cache:                  cache.New(30*time.Second, 100),
		LogLevel:               "error",
		EmitStructuredMetadata: emitStructuredMetadata,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStylePassthrough,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

func TestVLLogsToLokiStreams_DefaultValuesStayTwoTuple(t *testing.T) {
	p := newStreamMetadataTestProxy(t, false)
	body := []byte(`{"_time":"2026-01-01T00:00:00Z","_msg":"ok","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","deployment.environment.name":"dev","level":"info"}` + "\n")

	streams := p.vlLogsToLokiStreams(body, `{job="otel-proxy"}`, false)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	values, ok := streams[0]["values"].([]interface{})
	if !ok || len(values) != 1 {
		t.Fatalf("expected one stream value, got %#v", streams[0]["values"])
	}
	pair, ok := values[0].([]interface{})
	if !ok {
		t.Fatalf("expected stream value to be tuple, got %T", values[0])
	}
	if len(pair) != 2 {
		t.Fatalf("expected canonical [ts, line] tuple, got %v", pair)
	}
}

func TestVLLogsToLokiStreams_EmitStructuredMetadataAddsFlatThirdTupleValueByDefault(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	body := []byte(`{"_time":"2026-01-01T00:00:00Z","_msg":"ok","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","deployment.environment.name":"dev","level":"info"}` + "\n")

	streams := p.vlLogsToLokiStreams(body, `{job="otel-proxy"}`, false)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	values, ok := streams[0]["values"].([]interface{})
	if !ok || len(values) != 1 {
		t.Fatalf("expected one stream value, got %#v", streams[0]["values"])
	}
	pair, ok := values[0].([]interface{})
	if !ok {
		t.Fatalf("expected stream value to be tuple, got %T", values[0])
	}
	if len(pair) != 3 {
		t.Fatalf("expected [ts, line, metadata] tuple, got %v", pair)
	}
	meta, ok := pair[2].(map[string]string)
	if !ok {
		t.Fatalf("expected metadata object in tuple[2], got %T", pair[2])
	}
	if _, ok := meta["structuredMetadata"]; ok {
		t.Fatalf("default tuple metadata must be flat for broad parser compatibility, got %v", meta)
	}
	if got := meta["service.name"]; got != "otel-app" {
		t.Fatalf("expected flat structured metadata field service.name=otel-app, got %v", meta)
	}
	if got := meta["deployment.environment.name"]; got != "dev" {
		t.Fatalf("expected flat structured metadata field deployment.environment.name=dev, got %v", meta)
	}
}

func TestVLLogsToLokiStreams_EmitStructuredMetadataWithParserFlattensFieldsByDefault(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	body := []byte(`{"_time":"2026-01-01T00:00:00Z","_msg":"{\"message\":\"ok\",\"status\":200}","_stream":"{job=\"otel-proxy\",level=\"info\"}","http.status_code":"200","level":"info"}` + "\n")

	streams := p.vlLogsToLokiStreams(body, `{job="otel-proxy"} | json`, false)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	values, ok := streams[0]["values"].([]interface{})
	if !ok || len(values) != 1 {
		t.Fatalf("expected one stream value, got %#v", streams[0]["values"])
	}
	pair, ok := values[0].([]interface{})
	if !ok {
		t.Fatalf("expected stream value to be tuple, got %T", values[0])
	}
	if len(pair) != 3 {
		t.Fatalf("expected [ts, line, metadata] tuple, got %v", pair)
	}
	meta, ok := pair[2].(map[string]string)
	if !ok {
		t.Fatalf("expected metadata object in tuple[2], got %T", pair[2])
	}
	if _, ok := meta["parsed"]; ok {
		t.Fatalf("default tuple metadata must avoid nested parsed payload for broad parser compatibility, got %v", meta)
	}
	if got := meta["http.status_code"]; got != "200" {
		t.Fatalf("expected parser-derived field in flattened metadata, got %v", meta)
	}
}

func TestVLLogsToLokiStreams_CategorizedLabelsKeepsNestedStructuredMetadataAndParsed(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	body := []byte(`{"_time":"2026-01-01T00:00:00Z","_msg":"{\"message\":\"ok\",\"status\":200}","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","trace_id":"abc123","http.status_code":"200","level":"info"}` + "\n")

	streams := p.vlLogsToLokiStreams(body, `{job="otel-proxy"} | json`, true)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	values, ok := streams[0]["values"].([]interface{})
	if !ok || len(values) != 1 {
		t.Fatalf("expected one stream value, got %#v", streams[0]["values"])
	}
	pair, ok := values[0].([]interface{})
	if !ok || len(pair) != 3 {
		t.Fatalf("expected [ts, line, metadata] tuple, got %v", pair)
	}
	meta, ok := pair[2].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata object in tuple[2], got %T", pair[2])
	}
	if _, ok := meta["parsed"]; !ok {
		t.Fatalf("expected nested parsed payload when categorize-labels is requested, got %v", meta)
	}
}

func TestRequestWantsCategorizedLabels(t *testing.T) {
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	if requestWantsCategorizedLabels(req) {
		t.Fatal("expected false when encoding flag header is absent")
	}

	req.Header.Set("X-Loki-Response-Encoding-Flags", "foo,categorize-labels,bar")
	if !requestWantsCategorizedLabels(req) {
		t.Fatal("expected true when categorize-labels flag is present")
	}
}
