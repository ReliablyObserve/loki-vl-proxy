package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

	streams := p.vlLogsToLokiStreams(body, `{job="otel-proxy"}`, false, p.emitStructuredMetadata)
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

	streams := p.vlLogsToLokiStreams(body, `{job="otel-proxy"}`, false, p.emitStructuredMetadata)
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

	streams := p.vlLogsToLokiStreams(body, `{job="otel-proxy"} | json`, false, p.emitStructuredMetadata)
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

func TestVLLogsToLokiStreams_CategorizedLabelsStillEmitsFlatMetadataForParserSafety(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	body := []byte(`{"_time":"2026-01-01T00:00:00Z","_msg":"{\"message\":\"ok\",\"status\":200}","_stream":"{job=\"otel-proxy\",level=\"info\"}","service.name":"otel-app","trace_id":"abc123","http.status_code":"200","level":"info"}` + "\n")

	streams := p.vlLogsToLokiStreams(body, `{job="otel-proxy"} | json`, true, p.emitStructuredMetadata)
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
	meta, ok := pair[2].(map[string]string)
	if !ok {
		t.Fatalf("expected metadata object in tuple[2], got %T", pair[2])
	}
	if _, ok := meta["parsed"]; ok {
		t.Fatalf("metadata should remain flat map for parser safety, got %v", meta)
	}
	if _, ok := meta["structuredMetadata"]; ok {
		t.Fatalf("metadata should remain flat map for parser safety, got %v", meta)
	}
	if got := meta["http.status_code"]; got != "200" {
		t.Fatalf("expected flattened parser field, got %v", meta)
	}
}

func TestStreamLogQuery_StreamingModePreservesThreeTupleMetadata(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	rec := httptest.NewRecorder()
	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader(`{"_time":"2026-01-01T00:00:00Z","_msg":"{\"status\":200}","_stream":"{job=\"otel-proxy\",level=\"info\"}","http.status_code":"200","level":"info"}` + "\n")),
	}

	p.streamLogQuery(rec, resp, `{job="otel-proxy"} | json`, false, p.emitStructuredMetadata)

	var payload map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode streamLogQuery response: %v", err)
	}
	data, ok := payload["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected data object, got %T", payload["data"])
	}
	results, ok := data["result"].([]interface{})
	if !ok || len(results) != 1 {
		t.Fatalf("expected one stream result, got %v", data["result"])
	}
	stream0, ok := results[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected stream object, got %T", results[0])
	}
	values, ok := stream0["values"].([]interface{})
	if !ok || len(values) != 1 {
		t.Fatalf("expected one stream value, got %v", stream0["values"])
	}
	tuple, ok := values[0].([]interface{})
	if !ok || len(tuple) != 3 {
		t.Fatalf("expected 3-tuple stream value, got %v", values[0])
	}
	meta, ok := tuple[2].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata object in tuple[2], got %T", tuple[2])
	}
	if got, ok := meta["http.status_code"]; !ok || got != "200" {
		t.Fatalf("expected parser metadata in tuple[2], got %v", meta)
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

func TestShouldEmitStructuredMetadata_DisablesForGrafanaByDefault(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	req.Header.Set("X-Grafana-User", "admin")

	if p.shouldEmitStructuredMetadata(req) {
		t.Fatal("expected structured metadata disabled for Grafana callers by default")
	}
}

func TestShouldEmitStructuredMetadata_AllowsGrafanaOptInViaEncodingFlag(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	req.Header.Set("X-Grafana-User", "admin")
	req.Header.Set("X-Loki-Response-Encoding-Flags", "structured-metadata")

	if !p.shouldEmitStructuredMetadata(req) {
		t.Fatal("expected structured metadata when Grafana explicitly opts in via encoding flag")
	}
}

func TestShouldEmitStructuredMetadata_AllowsQueryParamOptIn(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?structured_metadata=true", nil)
	req.Header.Set("X-Grafana-User", "admin")

	if !p.shouldEmitStructuredMetadata(req) {
		t.Fatal("expected structured metadata when structured_metadata=true query param is set")
	}
}

func TestShouldEmitStructuredMetadata_AllowsQueryParamOptOut(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?structured_metadata=false", nil)
	req.Header.Set("X-Grafana-User", "admin")

	if p.shouldEmitStructuredMetadata(req) {
		t.Fatal("expected structured metadata disabled when structured_metadata=false query param is set")
	}
}

func TestShouldEmitStructuredMetadata_EnablesForNonGrafanaByDefault(t *testing.T) {
	p := newStreamMetadataTestProxy(t, true)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)

	if !p.shouldEmitStructuredMetadata(req) {
		t.Fatal("expected structured metadata enabled for non-Grafana callers by default")
	}
}

func TestQueryRange_GrafanaDefaultStaysTwoTupleForStrictDecoders(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-10T12:00:00Z","_msg":"iptables policy update: \"Drop if no profiles matched\" -j DROP","_stream":"{deployment_environment=\"dev\"}","chain.link.env":"dev","deployment_environment":"dev","service.name":"calico","level":"error"}` + "\n"))
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL:             vlBackend.URL,
		Cache:                  cache.New(30*time.Second, 100),
		LogLevel:               "error",
		EmitStructuredMetadata: true,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStylePassthrough,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={deployment_environment="dev"}&start=1&end=2&limit=10`, nil)
	req.Header.Set("X-Grafana-User", "admin")
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	// Strict decoder: Grafana compatibility path expects [ts, line] tuples.
	var strict struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &strict); err != nil {
		t.Fatalf("strict tuple decode failed (possible ReadArray regression): %v\nbody=%s", err, rec.Body.String())
	}
	if len(strict.Data.Result) != 1 || len(strict.Data.Result[0].Values) != 1 {
		t.Fatalf("unexpected strict decode shape: %+v", strict.Data.Result)
	}
	if strict.Data.Result[0].Values[0][1] == "" {
		t.Fatalf("expected non-empty log line in strict decode tuple, got %+v", strict.Data.Result[0].Values[0])
	}
}

func TestRequestLooksLikeGrafana(t *testing.T) {
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	if requestLooksLikeGrafana(req) {
		t.Fatal("expected non-Grafana request by default")
	}

	req.Header.Set("X-Grafana-User", "admin")
	if !requestLooksLikeGrafana(req) {
		t.Fatal("expected Grafana detection from X-Grafana-User")
	}

	req = httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	req.Header.Set("User-Agent", "Grafana/12.4.2")
	if !requestLooksLikeGrafana(req) {
		t.Fatal("expected Grafana detection from User-Agent")
	}
}

func TestTupleModeForRequest(t *testing.T) {
	grafanaReq := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	grafanaReq.Header.Set("X-Grafana-User", "admin")
	if got := tupleModeForRequest(grafanaReq, false); got != "grafana_default_2tuple" {
		t.Fatalf("unexpected mode for Grafana default 2-tuple: %q", got)
	}

	grafanaOptInReq := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	grafanaOptInReq.Header.Set("X-Grafana-User", "admin")
	grafanaOptInReq.Header.Set("X-Loki-Response-Encoding-Flags", "structured-metadata")
	if got := tupleModeForRequest(grafanaOptInReq, true); got != "grafana_optin_3tuple" {
		t.Fatalf("unexpected mode for Grafana opt-in 3-tuple: %q", got)
	}

	nonGrafanaReq := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	if got := tupleModeForRequest(nonGrafanaReq, true); got != "nongrafana_3tuple" {
		t.Fatalf("unexpected mode for non-Grafana 3-tuple: %q", got)
	}
}
