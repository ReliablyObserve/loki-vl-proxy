package metrics

import (
	"strings"
	"testing"
	"time"
)

// The served and reuse families write one series per route and class, layer and tier that has data, with fixed
// label values; avoided work is written only for reuse with a recorded cost, and unknown-cost reuse adds none.
// The same counters go out over OTLP.
//
// conformance: observability-vl-savings
func TestSavingsMetricsExposition(t *testing.T) {
	m := NewMetrics()
	built := VLWork{Calls: 2, Duration: 1500 * time.Millisecond, Bytes: 300, Rows: 7}
	m.RecordServedRequest("query_range", "", ServedVL, 2*time.Second, 1000, built)
	m.RecordServedRequest("query_range", "", ServedCache, 3*time.Millisecond, 1000, VLWork{})
	m.RecordServedRequest("query_range", "", ServedClass(200), time.Millisecond, 10, VLWork{}) // folds into local
	m.RecordVLReuse("query_range", "", ReuseResponse, ReuseL1Memory, built, true)
	m.RecordVLReuse("query_range", "", ReuseWindow, ReuseL3Peer, VLWork{Calls: 9}, false)
	m.RecordVLReuse("query_range", "", ReuseLayer(99), ReuseL1Memory, built, true) // out of the set: dropped
	body := scrape(m)
	const r = `route="/loki/api/v1/query_range"`
	for _, want := range []string{
		`loki_vl_proxy_served_request_duration_seconds_count{` + r + `,served="vl"} 1`,
		`loki_vl_proxy_served_request_duration_seconds_count{` + r + `,served="cache"} 1`,
		`loki_vl_proxy_served_request_duration_seconds_count{` + r + `,served="local"} 1`,
		`loki_vl_proxy_served_response_bytes_total{` + r + `,served="cache"} 1000`,
		`loki_vl_proxy_served_vl_calls_total{` + r + `,served="vl"} 2`,
		`loki_vl_proxy_served_vl_seconds_total{` + r + `,served="vl"} 1.5`,
		`loki_vl_proxy_served_vl_bytes_total{` + r + `,served="vl"} 300`,
		`loki_vl_proxy_served_vl_rows_total{` + r + `,served="vl"} 7`,
		`loki_vl_proxy_vl_reuse_total{` + r + `,layer="response",tier="l1_memory",cost="recorded"} 1`,
		`loki_vl_proxy_vl_reuse_total{` + r + `,layer="window",tier="l3_peer",cost="unknown"} 1`,
		`loki_vl_proxy_vl_avoided_calls_total{` + r + `,layer="response",tier="l1_memory"} 2`,
		`loki_vl_proxy_vl_avoided_seconds_total{` + r + `,layer="response",tier="l1_memory"} 1.5`,
		`loki_vl_proxy_vl_avoided_bytes_total{` + r + `,layer="response",tier="l1_memory"} 300`,
		`loki_vl_proxy_vl_avoided_rows_total{` + r + `,layer="response",tier="l1_memory"} 7`,
	} {
		if !strings.Contains(body, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	for _, absent := range []string{`layer="window",tier="l3_peer"} `, `served="error"`, `served="partial"`} {
		if strings.Contains(body, absent) {
			t.Errorf("unexpected %s", absent)
		}
	}

	pusher := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m)
	names := map[string]bool{}
	for _, metric := range pusher.savingsMetrics(time.Now().UnixNano()) {
		names[metric["name"].(string)] = true
	}
	for _, want := range []string{"loki_vl_proxy_served_requests_total", "loki_vl_proxy_served_response_bytes_total", "loki_vl_proxy_served_vl_calls_total",
		"loki_vl_proxy_served_vl_seconds_total", "loki_vl_proxy_served_vl_bytes_total", "loki_vl_proxy_served_vl_rows_total", "loki_vl_proxy_vl_reuse_total",
		"loki_vl_proxy_vl_avoided_calls_total", "loki_vl_proxy_vl_avoided_seconds_total", "loki_vl_proxy_vl_avoided_bytes_total", "loki_vl_proxy_vl_avoided_rows_total"} {
		if !names[want] {
			t.Errorf("OTLP lacks %s", want)
		}
	}
	var nilMetrics *Metrics
	nilMetrics.RecordServedRequest("query", "", ServedVL, time.Second, 1, VLWork{})
	nilMetrics.RecordVLReuse("query", "", ReuseResponse, ReuseL1Memory, VLWork{}, true)
	if ReuseTierOf("l2_disk") != ReuseL2Disk || ReuseTierOf("l3_peer") != ReuseL3Peer || ReuseTierOf("") != ReuseL1Memory {
		t.Fatal("ReuseTierOf")
	}
}

// A fresh process writes the families' headers and no sample.
//
// conformance: observability-vl-savings
func TestSavingsMetricsEmpty(t *testing.T) {
	body := scrape(NewMetrics())
	if !strings.Contains(body, "# TYPE loki_vl_proxy_vl_reuse_total counter") || strings.Contains(body, "loki_vl_proxy_vl_reuse_total{") {
		t.Fatal("empty reuse family")
	}
	if got := NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, NewMetrics()).savingsMetrics(0); len(got) != 0 {
		t.Fatalf("OTLP metrics with no data: %v", got)
	}
}
