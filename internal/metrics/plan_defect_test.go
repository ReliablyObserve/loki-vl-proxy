package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRecordPlanDefect: the plan defect counter is exported with the same
// name, labels and value by the Prometheus scrape and the OTLP push, one series
// per endpoint and VictoriaLogs route, and a nil Metrics records nothing.
func TestRecordPlanDefect(t *testing.T) {
	lockProcEnvTest(t)
	var none *Metrics
	none.RecordPlanDefect("query_range", "/select/logsql/query") // must not panic

	m := NewMetrics()
	m.RecordPlanDefect("query_range", "/select/logsql/stats_query_range")
	m.RecordPlanDefect("query_range", "/select/logsql/stats_query_range")
	m.RecordPlanDefect("series", "unknown")

	w := httptest.NewRecorder()
	m.Handler(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	for _, want := range []string{
		"# TYPE loki_vl_proxy_plan_defects_total counter\n",
		`loki_vl_proxy_plan_defects_total{endpoint="query_range",plan_route="/select/logsql/stats_query_range"} 2` + "\n",
		`loki_vl_proxy_plan_defects_total{endpoint="series",plan_route="unknown"} 1` + "\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("scrape lacks %q", want)
		}
	}

	raw, err := json.Marshal(NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).buildPayload())
	if err != nil {
		t.Fatal(err)
	}
	payload := string(raw)
	if strings.Count(payload, `"name":"loki_vl_proxy_plan_defects_total"`) != 1 {
		t.Fatalf("OTLP payload has no single plan defect metric")
	}
	for _, want := range []string{`"stringValue":"/select/logsql/stats_query_range"`, `"stringValue":"unknown"`, `"key":"plan_route"`} {
		if !strings.Contains(payload, want) {
			t.Fatalf("OTLP payload lacks %s", want)
		}
	}
	if empty, _ := json.Marshal(NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, NewMetrics()).buildPayload()); strings.Contains(string(empty), "loki_vl_proxy_plan_defects_total") {
		t.Fatal("an empty counter is pushed")
	}
}
