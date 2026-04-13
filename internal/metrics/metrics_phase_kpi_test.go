package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics_ExportsWindowPhaseKPIs(t *testing.T) {
	m := NewMetrics()
	m.RecordQueryRangeWindowPrefilterOutcome(8, 2) // 0.8 hit ratio
	m.RecordQueryRangeWindowRetry()
	m.RecordQueryRangeWindowRetry()
	m.RecordQueryRangeWindowDegradedBatch()
	m.RecordQueryRangeWindowPartialResponse()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler(w, r)
	body := w.Body.String()

	for _, snippet := range []string{
		"loki_vl_proxy_window_prefilter_hit_ratio 0.8",
		"loki_vl_proxy_window_retry_total 2",
		"loki_vl_proxy_window_degraded_batch_total 1",
		"loki_vl_proxy_window_partial_response_total 1",
	} {
		if !strings.Contains(body, snippet) {
			t.Fatalf("expected metrics output to contain %q\n%s", snippet, body)
		}
	}
}
