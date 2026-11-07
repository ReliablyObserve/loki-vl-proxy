package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func exposition(m *Metrics) string {
	rec := httptest.NewRecorder()
	m.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// A request's phase times are observed per route for the phases that ran (the proxy's own time always); its
// fan-out counts are observed for every kind, zero included; one cache lookup is observed by tier and result.
//
// conformance: observability-query-flow
func TestRequestFlowMetricsExposition(t *testing.T) {
	m := NewMetrics()
	m.RecordRequestFlow("labels", "/loki/api/v1/labels", RequestFlow{
		ParseCount: 1, TranslateCount: 2, CacheLookups: 3, CacheHits: 3, VLCalls: 0,
		Parse: 2 * time.Millisecond, Translate: 3 * time.Millisecond, Cache: 4 * time.Millisecond, Proxy: 40 * time.Millisecond,
	})
	m.RecordRequestFlow("labels", "/loki/api/v1/labels", RequestFlow{VLCalls: 60, VL: 3 * time.Second, Proxy: -time.Second})
	m.RecordCacheLookup("l1_memory", "hit", 20*time.Microsecond)
	m.RecordCacheLookup("l9_unheard_of", "weird", time.Millisecond) // folds into unknown / miss
	body := exposition(m)

	for _, want := range []string{
		"# TYPE loki_vl_proxy_request_phase_seconds histogram",
		"# TYPE loki_vl_proxy_request_fanout histogram",
		"# TYPE loki_vl_proxy_cache_lookup_seconds histogram",
		`loki_vl_proxy_request_phase_seconds_count{route="/loki/api/v1/labels",phase="parse"} 1`,
		`loki_vl_proxy_request_phase_seconds_count{route="/loki/api/v1/labels",phase="proxy"} 2`,
		`loki_vl_proxy_request_phase_seconds_count{route="/loki/api/v1/labels",phase="vl"} 1`,
		`loki_vl_proxy_request_phase_seconds_bucket{route="/loki/api/v1/labels",phase="parse",le="0.005"} 1`,
		`loki_vl_proxy_request_phase_seconds_bucket{route="/loki/api/v1/labels",phase="vl",le="2.5"} 0`,
		`loki_vl_proxy_request_phase_seconds_bucket{route="/loki/api/v1/labels",phase="vl",le="5"} 1`,
		`loki_vl_proxy_request_fanout_count{route="/loki/api/v1/labels",kind="vl_call"} 2`,
		`loki_vl_proxy_request_fanout_bucket{route="/loki/api/v1/labels",kind="vl_call",le="0"} 1`,
		`loki_vl_proxy_request_fanout_bucket{route="/loki/api/v1/labels",kind="vl_call",le="55"} 1`,
		`loki_vl_proxy_request_fanout_bucket{route="/loki/api/v1/labels",kind="vl_call",le="+Inf"} 2`,
		`loki_vl_proxy_request_fanout_sum{route="/loki/api/v1/labels",kind="vl_call"} 60`,
		`loki_vl_proxy_request_fanout_count{route="/loki/api/v1/labels",kind="cache_hit"} 2`,
		`loki_vl_proxy_cache_lookup_seconds_count{tier="l1_memory",result="hit"} 1`,
		`loki_vl_proxy_cache_lookup_seconds_bucket{tier="l1_memory",result="hit",le="5e-05"} 1`,
		`loki_vl_proxy_cache_lookup_seconds_count{tier="unknown",result="miss"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("exposition lacks %s", want)
		}
	}
	// A nil Metrics and an endpoint-only record are safe.
	var nilMetrics *Metrics
	nilMetrics.RecordRequestFlow("labels", "", RequestFlow{})
	nilMetrics.RecordCacheLookup("l1_memory", "hit", 0)
	m.RecordRequestFlow("query_range", "", RequestFlow{})
	body = exposition(m)
	if !strings.Contains(body, `loki_vl_proxy_request_fanout_count{route="/loki/api/v1/query_range",kind="parse"} 1`) {
		t.Fatal("an empty route takes the endpoint's default route")
	}
	// A phase that did not run is not observed: the empty request has only its own time.
	if !strings.Contains(body, `loki_vl_proxy_request_phase_seconds_count{route="/loki/api/v1/query_range",phase="proxy"} 1`) || strings.Contains(body, `route="/loki/api/v1/query_range",phase="vl"`) {
		t.Fatal("a phase that did not run was exposed")
	}
}

// conformance: observability-query-flow
func TestRequestFlowMetricsAreSafeInParallel(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.RecordRequestFlow("labels", "/loki/api/v1/labels", RequestFlow{ParseCount: 1, Parse: time.Millisecond})
				m.RecordCacheLookup("l2_disk", "stale", time.Millisecond)
			}
		}()
	}
	wg.Wait()
	body := exposition(m)
	for _, want := range []string{
		`loki_vl_proxy_request_fanout_count{route="/loki/api/v1/labels",kind="parse"} 800`,
		`loki_vl_proxy_cache_lookup_seconds_count{tier="l2_disk",result="stale"} 800`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("exposition lacks %s", want)
		}
	}
}
