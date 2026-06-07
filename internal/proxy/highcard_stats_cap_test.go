package proxy

import (
	"fmt"
	"strings"
	"testing"

	fj "github.com/valyala/fastjson"
)

// TestCapStatsResultsByTotalCount_KeepsBusiest is the regression guard for the
// high-cardinality "pod" / *_id Drilldown chart bug. VL's stats_query_range
// returns results in alphabetical label order; a plain results[:N] slice keeps
// the noise floor (pods with count==1). The fix ranks by total count so the
// busiest series survive. This test asserts top-N-by-count, NOT first-N.
func TestCapStatsResultsByTotalCount_KeepsBusiest(t *testing.T) {
	var p fj.Parser
	// 6 pods. Alphabetically first are the noise floor (count 1); the busiest
	// (counts 50, 40, 30) are alphabetically LAST — a first-N slice would drop them.
	body := `{"data":{"result":[
		{"metric":{"pod":"aaa-noise-1"},"values":[[100,"1"]]},
		{"metric":{"pod":"bbb-noise-2"},"values":[[100,"1"]]},
		{"metric":{"pod":"ccc-noise-3"},"values":[[100,"1"]]},
		{"metric":{"pod":"xxx-busy-30"},"values":[[100,"10"],[160,"20"]]},
		{"metric":{"pod":"yyy-busy-40"},"values":[[100,"15"],[160,"25"]]},
		{"metric":{"pod":"zzz-busy-50"},"values":[[100,"20"],[160,"30"]]}
	]}}`
	v, err := p.Parse(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	results := v.GetArray("data", "result")
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}

	capped := capStatsResultsByTotalCount(results, 3)
	if len(capped) != 3 {
		t.Fatalf("expected cap to 3, got %d", len(capped))
	}
	gotPods := map[string]bool{}
	for _, r := range capped {
		gotPods[string(r.Get("metric", "pod").GetStringBytes())] = true
	}
	// The three busiest must survive; the three noise-floor pods must be dropped.
	for _, want := range []string{"zzz-busy-50", "yyy-busy-40", "xxx-busy-30"} {
		if !gotPods[want] {
			t.Errorf("busiest pod %q was dropped (got %v) — cap is NOT ranking by count", want, keysOf(gotPods))
		}
	}
	for _, noise := range []string{"aaa-noise-1", "bbb-noise-2", "ccc-noise-3"} {
		if gotPods[noise] {
			t.Errorf("noise-floor pod %q survived (got %v) — cap kept first-N alphabetical, not top-N", noise, keysOf(gotPods))
		}
	}
}

func TestCapStatsResultsByTotalCount_NoCapWhenFitsOrDisabled(t *testing.T) {
	var p fj.Parser
	body := `{"data":{"result":[
		{"metric":{"pod":"a"},"values":[[100,"1"]]},
		{"metric":{"pod":"b"},"values":[[100,"5"]]}
	]}}`
	v, _ := p.Parse(body)
	results := v.GetArray("data", "result")

	if got := capStatsResultsByTotalCount(results, 5); len(got) != 2 {
		t.Errorf("fits-already: expected 2 unchanged, got %d", len(got))
	}
	if got := capStatsResultsByTotalCount(results, 0); len(got) != 2 {
		t.Errorf("maxSeries=0 (disabled): expected 2 unchanged, got %d", len(got))
	}
	if got := capStatsResultsByTotalCount(results, -1); len(got) != 2 {
		t.Errorf("maxSeries<0: expected 2 unchanged, got %d", len(got))
	}
}

func TestCapStatsResultsByTotalCount_Deterministic(t *testing.T) {
	// Ties on total must break deterministically on the metric JSON, so repeated
	// requests return a stable series set (no cache-key churn / flapping charts).
	var p fj.Parser
	body := `{"data":{"result":[
		{"metric":{"pod":"p3"},"values":[[100,"7"]]},
		{"metric":{"pod":"p1"},"values":[[100,"7"]]},
		{"metric":{"pod":"p2"},"values":[[100,"7"]]}
	]}}`
	v, _ := p.Parse(body)
	results := v.GetArray("data", "result")
	first := podSetFromResults(capStatsResultsByTotalCount(results, 2))
	for i := 0; i < 5; i++ {
		if got := podSetFromResults(capStatsResultsByTotalCount(results, 2)); got != first {
			t.Fatalf("non-deterministic tie-break: run %d got %q, first was %q", i, got, first)
		}
	}
	// With ascending tie-break on the metric JSON, p1 and p2 win over p3.
	if !strings.Contains(first, "p1") || !strings.Contains(first, "p2") {
		t.Errorf("expected p1+p2 (ascending tie-break), got %q", first)
	}
}

// TestCapSeriesByTotalCount_MapVariant covers the map-based helper used by the
// raw-scan path via buildManualRangeMetricMatrix.
func TestCapSeriesByTotalCount_MapVariant(t *testing.T) {
	series := map[string]manualSeriesSamples{
		"{pod=\"noise-a\"}": {Metric: map[string]string{"pod": "noise-a"}, Samples: []rangeMetricSample{{ts: 1, value: 1}}},
		"{pod=\"noise-b\"}": {Metric: map[string]string{"pod": "noise-b"}, Samples: []rangeMetricSample{{ts: 1, value: 1}}},
		"{pod=\"busy-c\"}":  {Metric: map[string]string{"pod": "busy-c"}, Samples: []rangeMetricSample{{ts: 1, value: 40}, {ts: 2, value: 60}}},
		"{pod=\"busy-d\"}":  {Metric: map[string]string{"pod": "busy-d"}, Samples: []rangeMetricSample{{ts: 1, value: 30}, {ts: 2, value: 50}}},
	}
	capped := capSeriesByTotalCount(series, 2)
	if len(capped) != 2 {
		t.Fatalf("expected 2, got %d", len(capped))
	}
	if _, ok := capped["{pod=\"busy-c\"}"]; !ok {
		t.Errorf("busiest series busy-c dropped: %v", mapKeys(capped))
	}
	if _, ok := capped["{pod=\"busy-d\"}"]; !ok {
		t.Errorf("second-busiest series busy-d dropped: %v", mapKeys(capped))
	}
	if _, ok := capped["{pod=\"noise-a\"}"]; ok {
		t.Errorf("noise series survived top-N cap: %v", mapKeys(capped))
	}
	// Disabled / fits-already passthrough.
	if got := capSeriesByTotalCount(series, 0); len(got) != 4 {
		t.Errorf("maxSeries=0 should passthrough, got %d", len(got))
	}
	if got := capSeriesByTotalCount(series, 10); len(got) != 4 {
		t.Errorf("fits-already should passthrough, got %d", len(got))
	}
}

// TestResolvedMaxStatsQuerySeries_Default verifies the 500 default and override.
func TestResolvedMaxStatsQuerySeries_Default(t *testing.T) {
	if got := (&Proxy{maxStatsQuerySeries: 0}).resolvedMaxStatsQuerySeries(); got != 500 {
		t.Errorf("default: expected 500, got %d", got)
	}
	if got := (&Proxy{maxStatsQuerySeries: 1200}).resolvedMaxStatsQuerySeries(); got != 1200 {
		t.Errorf("override: expected 1200, got %d", got)
	}
	if got := (*Proxy)(nil).resolvedMaxStatsQuerySeries(); got != 500 {
		t.Errorf("nil receiver: expected 500, got %d", got)
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func mapKeys(m map[string]manualSeriesSamples) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func podSetFromResults(results []*fj.Value) string {
	pods := make([]string, 0, len(results))
	for _, r := range results {
		pods = append(pods, string(r.Get("metric", "pod").GetStringBytes()))
	}
	return fmt.Sprintf("%v", pods)
}
