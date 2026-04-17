package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

func decodeLastJSONLogLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) == 0 {
		t.Fatal("expected at least one log line")
	}
	var payload map[string]any
	if err := json.Unmarshal(lines[len(lines)-1], &payload); err != nil {
		t.Fatalf("invalid request log json: %v", err)
	}
	return payload
}

func TestRequestLogger_UsesEnduserSemanticFields(t *testing.T) {
	var buf bytes.Buffer
	p := &Proxy{
		log:                      slog.New(slog.NewJSONHandler(&buf, nil)),
		metrics:                  metrics.NewMetrics(),
		metricsTrustProxyHeaders: true,
	}

	h := p.requestLogger("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22x%22%7D", nil)
	req.RemoteAddr = "10.0.0.10:1234"
	req.Header.Set("X-Grafana-User", "alice@example.com")
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.10")
	req.Header.Set("User-Agent", "Grafana/12")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("invalid request log json: %v", err)
	}

	if payload["enduser.id"] != "alice@example.com" {
		t.Fatalf("expected enduser.id, got %#v", payload["enduser.id"])
	}
	if payload["enduser.name"] != "alice@example.com" {
		t.Fatalf("expected enduser.name, got %#v", payload["enduser.name"])
	}
	if payload["enduser.source"] != "grafana_user" {
		t.Fatalf("expected enduser.source grafana_user, got %#v", payload["enduser.source"])
	}
	if payload["client.address"] != "203.0.113.9" {
		t.Fatalf("expected client.address from forwarded-for, got %#v", payload["client.address"])
	}
	if payload["network.peer.address"] != "10.0.0.10" {
		t.Fatalf("expected network.peer.address without port, got %#v", payload["network.peer.address"])
	}
	if payload["user_agent.original"] != "Grafana/12" {
		t.Fatalf("expected user_agent.original, got %#v", payload["user_agent.original"])
	}
	if _, ok := payload["user.id"]; ok {
		t.Fatalf("did not expect user.id field, got %#v", payload["user.id"])
	}
	if _, ok := payload["user.name"]; ok {
		t.Fatalf("did not expect user.name field, got %#v", payload["user.name"])
	}
	if _, ok := payload["event.duration_ms"]; ok {
		t.Fatalf("did not expect legacy event.duration_ms field, got %#v", payload["event.duration_ms"])
	}
}

func TestRequestLogger_MetricsUseTemplateRoutes(t *testing.T) {
	p := &Proxy{
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: metrics.NewMetrics(),
	}

	h := p.requestLogger("label_values", "/loki/api/v1/label/{name}/values", func(w http.ResponseWriter, _ *http.Request) {
		p.metrics.RecordRequestWithRoute("label_values", "/loki/api/v1/label/{name}/values", http.StatusOK, 5*time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	for _, path := range []string{
		"/loki/api/v1/label/app/values",
		"/loki/api/v1/label/pod/values",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}

	metricsRR := httptest.NewRecorder()
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	p.metrics.Handler(metricsRR, metricsReq)

	body := metricsRR.Body.String()
	expected := `loki_vl_proxy_requests_total{system="loki",direction="downstream",endpoint="label_values",route="/loki/api/v1/label/{name}/values",status="200"} 2`
	if !strings.Contains(body, expected) {
		t.Fatalf("expected aggregated template-route series %q, body=%s", expected, body)
	}
	for _, rawRoute := range []string{
		`route="/loki/api/v1/label/app/values"`,
		`route="/loki/api/v1/label/pod/values"`,
	} {
		if strings.Contains(body, rawRoute) {
			t.Fatalf("did not expect raw request path in metric labels: %s", rawRoute)
		}
	}
}

func TestRequestLogger_DetectsGrafanaDrilldownSurface(t *testing.T) {
	var buf bytes.Buffer
	p := &Proxy{
		log:                      slog.New(slog.NewJSONHandler(&buf, nil)),
		metrics:                  metrics.NewMetrics(),
		metricsTrustProxyHeaders: true,
	}

	h := p.requestLogger("detected_fields", "/loki/api/v1/detected_fields", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_fields?query=%7Bservice_name%3D%22api%22%7D", nil)
	req.Header.Set("User-Agent", "Grafana/12.3.3")
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("invalid request log json: %v", err)
	}
	if got := payload["grafana.client.surface"]; got != "grafana_drilldown" {
		t.Fatalf("expected drilldown surface, got %#v", got)
	}
	if got := payload["grafana.client.source_tag"]; got != "grafana-lokiexplore-app" {
		t.Fatalf("expected source tag, got %#v", got)
	}
	if got := payload["grafana.version"]; got != "12.3.3" {
		t.Fatalf("expected grafana version, got %#v", got)
	}
	if got := payload["grafana.runtime.family"]; got != "12.x+" {
		t.Fatalf("expected grafana runtime family, got %#v", got)
	}
	if got := payload["grafana.drilldown.profile"]; got != "drilldown-v2" {
		t.Fatalf("expected drilldown profile, got %#v", got)
	}
}

func TestRequestLogger_DetectsGrafanaDatasourceSurface(t *testing.T) {
	var buf bytes.Buffer
	p := &Proxy{
		log:     slog.New(slog.NewJSONHandler(&buf, nil)),
		metrics: metrics.NewMetrics(),
	}

	h := p.requestLogger("labels", "/loki/api/v1/labels", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	req.Header.Set("User-Agent", "Grafana/12.4.1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("invalid request log json: %v", err)
	}
	if got := payload["grafana.client.surface"]; got != "grafana_loki_datasource" {
		t.Fatalf("expected datasource surface, got %#v", got)
	}
	if got := payload["grafana.version"]; got != "12.4.1" {
		t.Fatalf("expected grafana version, got %#v", got)
	}
	if got := payload["grafana.runtime.family"]; got != "12.x+" {
		t.Fatalf("expected grafana runtime family, got %#v", got)
	}
	if got := payload["grafana.datasource.profile"]; got != "grafana-datasource-v12" {
		t.Fatalf("expected datasource profile, got %#v", got)
	}
}

func TestRequestLogger_InfoOmitsPerTypeBreakdowns(t *testing.T) {
	var buf bytes.Buffer
	p := &Proxy{
		log:     slog.New(slog.NewJSONHandler(&buf, nil)),
		metrics: metrics.NewMetrics(),
	}

	h := p.requestLogger("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, "/select/logsql/query", "vl.example", 8427, http.StatusOK, 12*time.Millisecond, nil)
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, "/select/logsql/query", "vl.example", 8427, http.StatusOK, 18*time.Millisecond, nil)
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, "/select/logsql/stats_query_range", "vl.example", 8427, http.StatusOK, 25*time.Millisecond, nil)
		p.observeInternalOperation(ctx, "translate_query", "cache_hit", 2*time.Millisecond)
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "translated", 3*time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22x%22%7D", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	payload := decodeLastJSONLogLine(t, &buf)

	if got := payload["upstream.calls"]; got != float64(3) {
		t.Fatalf("expected upstream.calls=3, got %#v", got)
	}
	if got := payload["upstream.call_types"]; got != float64(2) {
		t.Fatalf("expected upstream.call_types=2, got %#v", got)
	}
	if got := payload["proxy.operation_types"]; got != float64(2) {
		t.Fatalf("expected proxy.operation_types=2, got %#v", got)
	}
	if _, ok := payload["upstream.calls_by_type"]; ok {
		t.Fatalf("did not expect upstream.calls_by_type at info level, got %#v", payload["upstream.calls_by_type"])
	}
	if _, ok := payload["upstream.duration_ms_by_type"]; ok {
		t.Fatalf("did not expect upstream.duration_ms_by_type at info level, got %#v", payload["upstream.duration_ms_by_type"])
	}
	if _, ok := payload["proxy.operations_by_type"]; ok {
		t.Fatalf("did not expect proxy.operations_by_type at info level, got %#v", payload["proxy.operations_by_type"])
	}
	if _, ok := payload["proxy.operation_duration_ms_by_type"]; ok {
		t.Fatalf("did not expect proxy.operation_duration_ms_by_type at info level, got %#v", payload["proxy.operation_duration_ms_by_type"])
	}

	metricsRR := httptest.NewRecorder()
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	p.metrics.Handler(metricsRR, metricsReq)
	body := metricsRR.Body.String()

	for _, needle := range []string{
		`loki_vl_proxy_backend_duration_seconds_count{system="vl",direction="upstream",endpoint="select_logsql_query",route="/select/logsql/query"} 2`,
		`loki_vl_proxy_backend_duration_seconds_count{system="vl",direction="upstream",endpoint="select_logsql_stats_query_range",route="/select/logsql/stats_query_range"} 1`,
		`loki_vl_proxy_upstream_calls_per_request_count{system="loki",direction="downstream",endpoint="query_range",route="/loki/api/v1/query_range"} 1`,
		`loki_vl_proxy_internal_operation_total{operation="translate_query",outcome="cache_hit"} 1`,
		`loki_vl_proxy_internal_operation_total{operation="translate_stats_response_labels",outcome="translated"} 1`,
	} {
		if !strings.Contains(body, needle) {
			t.Fatalf("expected metrics output to contain %q", needle)
		}
	}
}

func TestRequestLogger_DebugEmitsPerTypeBreakdowns(t *testing.T) {
	var buf bytes.Buffer
	p := &Proxy{
		log: slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		metrics: metrics.NewMetrics(),
	}

	h := p.requestLogger("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, "/select/logsql/query", "vl.example", 8427, http.StatusOK, 12*time.Millisecond, nil)
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, "/select/logsql/query", "vl.example", 8427, http.StatusOK, 18*time.Millisecond, nil)
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, "/select/logsql/stats_query_range", "vl.example", 8427, http.StatusOK, 25*time.Millisecond, nil)
		p.observeInternalOperation(ctx, "translate_query", "cache_hit", 2*time.Millisecond)
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "translated", 3*time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=%7Bapp%3D%22x%22%7D", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	payload := decodeLastJSONLogLine(t, &buf)

	upstreamCallsByType, ok := payload["upstream.calls_by_type"].(map[string]any)
	if !ok {
		t.Fatalf("expected upstream.calls_by_type map, got %#v", payload["upstream.calls_by_type"])
	}
	if got := upstreamCallsByType["vl:select_logsql_query"]; got != float64(2) {
		t.Fatalf("expected two select_logsql_query calls, got %#v", got)
	}
	if got := upstreamCallsByType["vl:select_logsql_stats_query_range"]; got != float64(1) {
		t.Fatalf("expected one select_logsql_stats_query_range call, got %#v", got)
	}
	internalOpsByType, ok := payload["proxy.operations_by_type"].(map[string]any)
	if !ok {
		t.Fatalf("expected proxy.operations_by_type map, got %#v", payload["proxy.operations_by_type"])
	}
	if got := internalOpsByType["translate_query:cache_hit"]; got != float64(1) {
		t.Fatalf("expected translate_query:cache_hit once, got %#v", got)
	}
	if got := internalOpsByType["translate_stats_response_labels:translated"]; got != float64(1) {
		t.Fatalf("expected translate_stats_response_labels:translated once, got %#v", got)
	}
}

func TestWriteError_DowngradesClientErrorsToWarn(t *testing.T) {
	var buf bytes.Buffer
	p := &Proxy{log: slog.New(slog.NewJSONHandler(&buf, nil))}

	p.writeError(httptest.NewRecorder(), http.StatusForbidden, "peer cache endpoint is restricted to configured peers")
	payload := decodeLastJSONLogLine(t, &buf)
	if got := payload["level"]; got != "WARN" {
		t.Fatalf("expected WARN for 4xx writeError, got %#v", got)
	}

	p.writeError(httptest.NewRecorder(), http.StatusBadGateway, "backend unavailable")
	payload = decodeLastJSONLogLine(t, &buf)
	if got := payload["level"]; got != "ERROR" {
		t.Fatalf("expected ERROR for 5xx writeError, got %#v", got)
	}
}

func TestParseGrafanaSourceTag(t *testing.T) {
	got := parseGrafanaSourceTag([]string{`foo=bar, Source="grafana-lokiexplore-app"`})
	if got != "grafana-lokiexplore-app" {
		t.Fatalf("unexpected source tag: %q", got)
	}
	if empty := parseGrafanaSourceTag([]string{"foo=bar"}); empty != "" {
		t.Fatalf("expected empty source tag, got %q", empty)
	}
}

func TestParseGrafanaRuntimeMajor(t *testing.T) {
	if got := parseGrafanaRuntimeMajor("12.4.1"); got != 12 {
		t.Fatalf("expected 12, got %d", got)
	}
	if got := parseGrafanaRuntimeMajor("11.6.6-beta1"); got != 11 {
		t.Fatalf("expected 11, got %d", got)
	}
	if got := parseGrafanaRuntimeMajor("dev"); got != 0 {
		t.Fatalf("expected 0 for non-semver token, got %d", got)
	}
}
