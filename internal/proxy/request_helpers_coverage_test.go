package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestHelpersAndGrafanaProfileCoverage(t *testing.T) {
	rt := newRequestTelemetry()
	ctx := context.WithValue(context.Background(), requestTelemetryKey, rt)
	setCacheResult(ctx, "hit")
	if got := snapshotTelemetry(ctx).cacheResult; got != "hit" {
		t.Fatalf("expected cache result to be recorded, got %q", got)
	}
	setCacheResult(context.Background(), "miss")

	if host, port := splitHostPortValue("127.0.0.1:3100"); host != "127.0.0.1" || port != 3100 {
		t.Fatalf("unexpected host/port split: %q %d", host, port)
	}
	if host, port := splitHostPortValue("example.invalid"); host != "example.invalid" || port != 0 {
		t.Fatalf("unexpected fallback host/port split: %q %d", host, port)
	}

	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
	req.RemoteAddr = "10.0.0.10:9090"
	req.Header.Set("X-Forwarded-For", "198.51.100.10, 10.0.0.10")
	if got := forwardedClientAddress(req, true); got != "198.51.100.10" {
		t.Fatalf("unexpected forwarded client address %q", got)
	}
	if got := forwardedClientAddress(req, false); got != "10.0.0.10" {
		t.Fatalf("unexpected direct client address %q", got)
	}

	if got := upstreamBreakdownKey("", ""); got != "unknown" {
		t.Fatalf("unexpected empty upstream breakdown key %q", got)
	}
	if got := upstreamBreakdownKey("vl", "select_logsql_query"); got != "vl:select_logsql_query" {
		t.Fatalf("unexpected upstream breakdown key %q", got)
	}
	if got := internalOperationBreakdownKey("", ""); got != "unknown:unknown" {
		t.Fatalf("unexpected empty internal operation key %q", got)
	}
	if got := internalOperationBreakdownKey("translate_query", "cache_hit"); got != "translate_query:cache_hit" {
		t.Fatalf("unexpected internal operation key %q", got)
	}

	drilldownReq := httptest.NewRequest(http.MethodGet, "/patterns", nil)
	drilldownReq.Header.Set("User-Agent", "Grafana/12.4.0")
	profile := detectGrafanaClientProfile(drilldownReq, "patterns", "/patterns")
	if profile.surface != "grafana_drilldown" || profile.drilldownProfile != "drilldown-v2" || profile.runtimeFamily != "12.x+" {
		t.Fatalf("unexpected drilldown profile %#v", profile)
	}

	dsReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
	dsReq.Header["X-Query-Tags"] = []string{`source="grafana-loki"`}
	dsReq.Header.Set("User-Agent", "Grafana/11.6.1")
	profile = detectGrafanaClientProfile(dsReq, "query_range", "/loki/api/v1/query_range")
	if profile.surface != "grafana_loki_datasource" || profile.datasourceProfile != "grafana-datasource-v11" {
		t.Fatalf("unexpected datasource profile %#v", profile)
	}

	if got := parseGrafanaVersionFromUserAgent("Mozilla/5.0 Grafana/12.2.1-beta1 extra"); got != "12.2.1-beta1" {
		t.Fatalf("unexpected grafana version parse %q", got)
	}
	if got := parseGrafanaVersionFromUserAgent("curl/8.0.0"); got != "" {
		t.Fatalf("expected empty grafana version for non-grafana agent, got %q", got)
	}
}

func TestTranslateStatsResponseLabelsWithContext_Coverage(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)

	invalid := []byte(`{not-json`)
	if got := p.translateStatsResponseLabelsWithContext(context.Background(), invalid, `{app="api"}`); string(got) != string(invalid) {
		t.Fatalf("expected invalid body to pass through unchanged")
	}

	noResults := []byte(`{"status":"success","data":{}}`)
	if got := p.translateStatsResponseLabelsWithContext(context.Background(), noResults, `{app="api"}`); string(got) != string(noResults) {
		t.Fatalf("expected empty results body to pass through unchanged")
	}

	body := []byte(`{"result":[{"metric":{"__name__":"hits","service.name":"api","level":"warn"}}]}`)
	got := p.translateStatsResponseLabelsWithContext(context.Background(), body, `{app="api"} |= "x" | stats count() by (detected_level)`)

	var resp struct {
		Result []struct {
			Metric map[string]interface{} `json:"metric"`
		} `json:"result"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("decode translated stats response: %v", err)
	}
	metric := resp.Result[0].Metric
	if _, ok := metric["__name__"]; ok {
		t.Fatalf("expected __name__ to be removed, got %#v", metric)
	}
	if metric["level"] != "warn" {
		t.Fatalf("expected original level to be preserved, got %#v", metric)
	}
	if metric["service_name"] != "api" || metric["detected_level"] != "warn" {
		t.Fatalf("unexpected translated metric labels %#v", metric)
	}

	noServiceBody := []byte(`{"result":[{"metric":{"k8s_cluster_name":"ops-sand"}}]}`)
	noServiceGot := p.translateStatsResponseLabelsWithContext(context.Background(), noServiceBody, `sum by(k8s_cluster_name) (rate({deployment_environment="dev"}[1m]))`)

	var noServiceResp struct {
		Result []struct {
			Metric map[string]interface{} `json:"metric"`
		} `json:"result"`
	}
	if err := json.Unmarshal(noServiceGot, &noServiceResp); err != nil {
		t.Fatalf("decode no-service stats response: %v", err)
	}
	noServiceMetric := noServiceResp.Result[0].Metric
	if _, exists := noServiceMetric["service_name"]; exists {
		t.Fatalf("unexpected synthetic service_name for metric without service signal: %#v", noServiceMetric)
	}
	if noServiceMetric["k8s_cluster_name"] != "ops-sand" {
		t.Fatalf("expected original metric labels to remain intact, got %#v", noServiceMetric)
	}
}
