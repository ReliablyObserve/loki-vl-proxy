package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func capabilityFixture() CapabilityStats {
	return CapabilityStats{
		Epoch:    3,
		Releases: []CapabilityRelease{{Version: "v1.54.0", Flavour: "oss", Source: "metrics"}, {Version: "v1.52.2-enterprise", Flavour: "enterprise", Source: "header"}},
		Gates: []CapabilityGate{
			{Gate: "vl.pipe_coalesce", Side: "victorialogs", Source: "both", On: true},
			{Gate: "vl.extra_filters_len", Side: "victorialogs", Source: "version", On: false},
		},
		Mismatches: []CapabilityCount{{Gate: "vl.pipe_coalesce", N: 1}},
		Probes:     []CapabilityProbe{{Gate: "vl.pipe_coalesce", Verdict: "on", N: 4}, {Gate: "vl.pipe_coalesce", Verdict: "inconclusive", N: 2}},
		Clients:    []CapabilityClient{{Surface: "drilldown", Grafana: "13.3.1", Datasource: "13.2.5", Drilldown: "unknown", N: 7}},
	}
}

// The capability families are exposed in Prometheus text and OTLP alike, with the names and labels the dashboard and
// the docs read, and nothing when no provider is set.
//
// conformance: vl-capability-layer
func TestCapabilityMetricsExporters(t *testing.T) {
	m := NewMetrics()
	rec := httptest.NewRecorder()
	m.Handler(rec, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(rec.Body.String(), "loki_vl_proxy_capability_gate") {
		t.Fatal("capability families without a provider")
	}
	m.SetCapabilityStatsProvider(capabilityFixture)
	rec = httptest.NewRecorder()
	m.Handler(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`loki_vl_proxy_backend_version_info{version="v1.54.0",flavour="oss",source="metrics"} 1`,
		`loki_vl_proxy_backend_version_info{version="v1.52.2-enterprise",flavour="enterprise",source="header"} 1`,
		`loki_vl_proxy_capability_gate{gate="vl.pipe_coalesce",side="victorialogs",source="both"} 1`,
		`loki_vl_proxy_capability_gate{gate="vl.extra_filters_len",side="victorialogs",source="version"} 0`,
		`loki_vl_proxy_capability_mismatch_total{gate="vl.pipe_coalesce"} 1`,
		`loki_vl_proxy_capability_epoch 3`,
		`loki_vl_proxy_backend_feature_probe_total{gate="vl.pipe_coalesce",verdict="inconclusive"} 2`,
		`loki_vl_proxy_client_profile_requests_total{surface="drilldown",grafana_version="13.3.1",datasource_version="13.2.5",drilldown_version="unknown"} 7`,
		"# TYPE loki_vl_proxy_capability_epoch gauge",
		"# TYPE loki_vl_proxy_capability_mismatch_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
	assertOnlyPrefixedOrLegacyMetricNames(t, prometheusMetricNames(body))

	lockProcEnvTest(t)
	payload := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).buildPayload()
	names := strings.Join(otlpMetricNamesFromPayload(t, payload), " ")
	for _, f := range capabilityFamilies {
		if !strings.Contains(names, f.name) {
			t.Errorf("OTLP lacks %s", f.name)
		}
	}
}
