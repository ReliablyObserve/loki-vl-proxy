package proxy

import (
	"math"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestProxyHelpers_ReloadFieldMappings(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	if got := p.labelTranslator.ToLoki("service.name"); got != "service.name" {
		t.Fatalf("expected passthrough translator before reload, got %q", got)
	}

	p.ReloadFieldMappings([]FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}})

	if got := p.labelTranslator.ToLoki("service.name"); got != "service_name" {
		t.Fatalf("expected reloaded field mapping to apply, got %q", got)
	}
}

func TestProxyHelpers_RequestPolicyError(t *testing.T) {
	err := &requestPolicyError{status: 403, msg: "denied"}
	if got := err.Error(); got != "denied" {
		t.Fatalf("expected error message to round-trip, got %q", got)
	}
}

func TestProxyHelpers_HostOnlyAndKnownPeerHost(t *testing.T) {
	if got := hostOnly("10.0.0.1:3100"); got != "10.0.0.1" {
		t.Fatalf("expected hostOnly to strip port, got %q", got)
	}
	if got := hostOnly("10.0.0.2"); got != "10.0.0.2" {
		t.Fatalf("expected hostOnly to keep bare host, got %q", got)
	}

	pc := cache.NewPeerCache(cache.PeerConfig{
		SelfAddr:      "10.0.0.1:3100",
		DiscoveryType: "static",
		StaticPeers:   "10.0.0.2:3100,10.0.0.3:3100",
		Timeout:       50 * time.Millisecond,
	})
	defer pc.Close()

	p := &Proxy{peerCache: pc}
	if !p.isKnownPeerHost("10.0.0.2") {
		t.Fatal("expected configured peer host to be recognized")
	}
	if p.isKnownPeerHost("10.0.0.9") {
		t.Fatal("expected unknown host to be rejected")
	}
}

func TestProxyHelpers_CanonicalReadCacheKey_NormalizesEquivalentHelperRequests(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	reqA := httptest.NewRequest("GET", "/loki/api/v1/labels?from=10&to=20&q=svc&query=%7Bapp%3D%22demo%22%7D", nil)
	reqB := httptest.NewRequest("GET", "/loki/api/v1/labels?search=svc&end=20&start=10&query=%7Bapp%3D%22demo%22%7D", nil)

	keyA := p.canonicalReadCacheKey("labels", "tenant-a", reqA)
	keyB := p.canonicalReadCacheKey("labels", "tenant-a", reqB)
	if keyA != keyB {
		t.Fatalf("expected equivalent helper requests to share cache key, got %q != %q", keyA, keyB)
	}
}

func TestProxyHelpers_CanonicalReadCacheKey_NormalizesDetectedLimitsAndDefaults(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	reqA := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?line_limit=1000", nil)
	reqB := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?limit=1000&query=*", nil)

	keyA := p.canonicalReadCacheKey("detected_fields", "tenant-a", reqA)
	keyB := p.canonicalReadCacheKey("detected_fields", "tenant-a", reqB)
	if keyA != keyB {
		t.Fatalf("expected equivalent detected_fields requests to share cache key, got %q != %q", keyA, keyB)
	}
}

func TestProxyHelpers_AddStatsByStreamClause_PreservesStreamIdentity(t *testing.T) {
	got := addStatsByStreamClause(`app:=api-gateway | stats count()`)
	if got != `app:=api-gateway | stats by (_stream, level) count()` {
		t.Fatalf("unexpected stats identity clause: %q", got)
	}
}

func TestProxyHelpers_PreserveMetricStreamIdentity_UsesStreamForBareMetrics(t *testing.T) {
	got := preserveMetricStreamIdentity(`rate({app="api-gateway"} |= "GET"[5m])`, `app:=api-gateway ~"GET" | stats rate()`, nil)
	if got != `app:=api-gateway ~"GET" | stats by (_stream, level) rate()` {
		t.Fatalf("unexpected preserved metric query: %q", got)
	}
}

func TestProxyHelpers_ParseBareParserMetricCompatSpec(t *testing.T) {
	spec, ok := parseBareParserMetricCompatSpec(`count_over_time({app="api-gateway"} | json | status >= 500 [5m])`)
	if !ok {
		t.Fatal("expected bare parser metric query to be recognized")
	}
	if spec.funcName != "count_over_time" {
		t.Fatalf("unexpected funcName %q", spec.funcName)
	}
	if spec.baseQuery != `{app="api-gateway"} | json | status >= 500` {
		t.Fatalf("unexpected baseQuery %q", spec.baseQuery)
	}
	if spec.rangeWindow != 5*time.Minute {
		t.Fatalf("unexpected rangeWindow %v", spec.rangeWindow)
	}
}

func TestProxyHelpers_ParseBareParserMetricCompatSpec_RejectsAggregatedQuery(t *testing.T) {
	if _, ok := parseBareParserMetricCompatSpec(`sum by (app) (count_over_time({app="api-gateway"} | json | status >= 500 [5m]))`); ok {
		t.Fatal("expected aggregated parser metric query to bypass bare compat handling")
	}
}

func TestProxyHelpers_ParseBareParserMetricCompatSpec_AcceptsUnwrapRangeFunctions(t *testing.T) {
	spec, ok := parseBareParserMetricCompatSpec(`sum_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`)
	if !ok {
		t.Fatal("expected unwrap range function to use bare compat handling")
	}
	if spec.unwrapField != "duration_ms" {
		t.Fatalf("unexpected unwrapField %q", spec.unwrapField)
	}
}

func TestProxyHelpers_ParseBareParserMetricCompatSpec_AcceptsRateCounterWithTemplateWindow(t *testing.T) {
	spec, ok := parseBareParserMetricCompatSpec(`rate_counter({app="api-gateway"} | json | unwrap counter [$__interval])`)
	if !ok {
		t.Fatal("expected rate_counter unwrap query to use bare compat handling")
	}
	if spec.funcName != "rate_counter" {
		t.Fatalf("unexpected funcName %q", spec.funcName)
	}
	if spec.unwrapField != "counter" {
		t.Fatalf("unexpected unwrapField %q", spec.unwrapField)
	}
	if spec.rangeWindow != 0 {
		t.Fatalf("expected template window to defer duration resolution, got %v", spec.rangeWindow)
	}
	if spec.rangeWindowExpr != "$__interval" {
		t.Fatalf("unexpected rangeWindowExpr %q", spec.rangeWindowExpr)
	}

	resolved, ok := resolveBareParserMetricRangeWindow(spec, "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z", "30s")
	if !ok {
		t.Fatal("expected template window to resolve with request step")
	}
	if resolved.rangeWindow != 30*time.Second {
		t.Fatalf("unexpected resolved rangeWindow %v", resolved.rangeWindow)
	}
}

func TestProxyHelpers_ParseBareParserMetricCompatSpec_AcceptsBracedTemplateWindow(t *testing.T) {
	spec, ok := parseBareParserMetricCompatSpec(`rate_counter({app="api-gateway"} | json | unwrap counter [${__rate_interval}])`)
	if !ok {
		t.Fatal("expected braced template window to be recognized")
	}
	if spec.rangeWindowExpr != "${__rate_interval}" {
		t.Fatalf("unexpected rangeWindowExpr %q", spec.rangeWindowExpr)
	}

	resolved, ok := resolveBareParserMetricRangeWindow(spec, "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z", "10s")
	if !ok {
		t.Fatal("expected braced template window to resolve with request step")
	}
	if resolved.rangeWindow != time.Minute {
		t.Fatalf("expected $__rate_interval minimum 1m, got %v", resolved.rangeWindow)
	}
}

func TestProxyHelpers_ResolveGrafanaRangeTemplateTokens(t *testing.T) {
	query := `rate({app="api-gateway"}[$__auto]) + rate_counter({app="api-gateway"} | json | unwrap counter [${__rate_interval}]) + count_over_time({app="api-gateway"}[$__range_s]) + sum_over_time({app="api-gateway"} | json | unwrap duration [${__interval_ms}])`
	got := resolveGrafanaRangeTemplateTokens(query, "2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z", "15s")
	if strings.Contains(got, "$__") || strings.Contains(got, "${__") {
		t.Fatalf("expected all Grafana template tokens to resolve, got %q", got)
	}
	if !strings.Contains(got, "[15s]") {
		t.Fatalf("expected $__auto/$__interval_ms to resolve to step, got %q", got)
	}
	if !strings.Contains(got, "[1m]") {
		t.Fatalf("expected $__rate_interval to clamp to 1m minimum, got %q", got)
	}
	if !strings.Contains(got, "[5m]") {
		t.Fatalf("expected $__range_s to resolve to query range, got %q", got)
	}
}

func TestProxyHelpers_ExtractParserProbeQuery_UnquotesInput(t *testing.T) {
	got := extractParserProbeQuery("\"rate_counter({app=\\\"api-gateway\\\"} | json | unwrap counter [5m])\"")
	if strings.HasPrefix(got, `"`) {
		t.Fatalf("expected parser probe query to be unquoted, got %q", got)
	}
	unescaped := strings.ReplaceAll(got, `\"`, `"`)
	if !strings.Contains(unescaped, `{app="api-gateway"} | json | unwrap counter`) {
		t.Fatalf("unexpected parser probe extraction %q", got)
	}
}

func TestProxyHelpers_ParseAbsentOverTimeCompatSpec(t *testing.T) {
	spec, ok := parseAbsentOverTimeCompatSpec(`absent_over_time({app="missing"}[5m])`)
	if !ok {
		t.Fatal("expected absent_over_time to be recognized")
	}
	if spec.baseQuery != `{app="missing"}` {
		t.Fatalf("unexpected baseQuery %q", spec.baseQuery)
	}
	if spec.rangeWindow != 5*time.Minute {
		t.Fatalf("unexpected rangeWindow %v", spec.rangeWindow)
	}
}

func TestProxyHelpers_BuildBareParserMetricMatrix(t *testing.T) {
	body := buildBareParserMetricMatrix([]bareParserMetricSeries{
		{
			metric: map[string]string{"app": "api-gateway", "status": "500"},
			samples: []bareParserMetricSample{
				{tsNanos: 120 * int64(time.Second), value: 1},
				{tsNanos: 180 * int64(time.Second), value: 1},
			},
		},
	}, 180*int64(time.Second), 300*int64(time.Second), int64(time.Minute), bareParserMetricCompatSpec{
		funcName:    "count_over_time",
		rangeWindow: 5 * time.Minute,
	})

	data := body["data"].(map[string]interface{})
	results := data["result"].([]lokiMatrixResult)
	if len(results) != 1 {
		t.Fatalf("expected single result series, got %d", len(results))
	}
	got := results[0].Values
	want := [][]interface{}{
		{float64(180), "2"},
		{float64(240), "2"},
		{float64(300), "2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected matrix values: got=%v want=%v", got, want)
	}
}

func TestProxyHelpers_BareParserMetricWindowValue_RateCounterHandlesResets(t *testing.T) {
	window := []bareParserMetricSample{
		{tsNanos: 1, value: 100},
		{tsNanos: 2, value: 110},
		{tsNanos: 3, value: 5},
		{tsNanos: 4, value: 12},
	}
	spec := bareParserMetricCompatSpec{rangeWindow: time.Minute}

	got := bareParserMetricWindowValue("rate_counter", window, spec)
	want := 22.0 / 60.0 // +10, reset then +5, then +7
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("rate_counter reset handling mismatch: got=%v want=%v", got, want)
	}
}

func TestProxyHelpers_StatsResponseIsEmpty(t *testing.T) {
	if !statsResponseIsEmpty([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`)) {
		t.Fatal("expected empty vector result to count as absent")
	}
	if !statsResponseIsEmpty([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[123,"0"]}]}}`)) {
		t.Fatal("expected zero-valued vector result to count as absent")
	}
	if statsResponseIsEmpty([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[123,"2"]}]}}`)) {
		t.Fatal("expected positive-valued vector result not to count as absent")
	}
}
