package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// The query complexity counter has one series per limit, always exported (zero before the first refusal), the same
// by the Prometheus scrape and the OTLP push, and a value outside the fixed set counts nowhere, so the label cannot
// grow.
//
// conformance: query-complexity-limits
func TestRecordQueryComplexityRejection(t *testing.T) {
	lockProcEnvTest(t)
	var none *Metrics
	none.RecordQueryComplexityRejection("plan_time") // must not panic
	if none.QueryComplexityRejectionCount("plan_time") != 0 {
		t.Fatal("a nil Metrics counted")
	}

	m := NewMetrics()
	m.RecordQueryComplexityRejection("ast_nodes")
	m.RecordQueryComplexityRejection("ast_nodes")
	m.RecordQueryComplexityRejection("plan_time")
	m.RecordQueryComplexityRejection("not-a-limit")
	if got := []int64{m.QueryComplexityRejectionCount("ast_nodes"), m.QueryComplexityRejectionCount("nesting_depth"), m.QueryComplexityRejectionCount("plan_time"), m.QueryComplexityRejectionCount("not-a-limit")}; got[0] != 2 || got[1] != 0 || got[2] != 1 || got[3] != 0 {
		t.Fatalf("counts %v", got)
	}

	w := httptest.NewRecorder()
	m.Handler(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	for _, want := range []string{
		"# TYPE loki_vl_proxy_query_complexity_rejections_total counter\n",
		`loki_vl_proxy_query_complexity_rejections_total{limit="ast_nodes"} 2` + "\n",
		`loki_vl_proxy_query_complexity_rejections_total{limit="nesting_depth"} 0` + "\n",
		`loki_vl_proxy_query_complexity_rejections_total{limit="plan_time"} 1` + "\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("scrape lacks %q", want)
		}
	}
	if strings.Contains(body, "not-a-limit") {
		t.Fatal("an undeclared limit became a label value")
	}

	raw, err := json.Marshal(NewOTLPPusher(OTLPConfig{Endpoint: "http://unused"}, m).buildPayload())
	if err != nil {
		t.Fatal(err)
	}
	payload := string(raw)
	if strings.Count(payload, `"name":"loki_vl_proxy_query_complexity_rejections_total"`) != 1 {
		t.Fatal("OTLP payload has no single query complexity metric")
	}
	for _, name := range QueryComplexityLimits {
		if !strings.Contains(payload, `"stringValue":"`+name+`"`) {
			t.Fatalf("OTLP payload lacks the %s series", name)
		}
	}
}
