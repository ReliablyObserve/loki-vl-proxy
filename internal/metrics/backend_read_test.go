package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBackendReadBytesBothExporters(t *testing.T) {
	m := NewMetrics()
	for i, kind := range []string{"raw", "stats", "metadata", "private-query-value"} {
		m.RecordBackendReadBytes(kind, int64(i+1))
	}
	m.RecordBackendReadBytes("raw", -100)
	w := httptest.NewRecorder()
	m.Handler(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{`{kind="raw"} 1`, `{kind="stats"} 2`, `{kind="metadata"} 3`, `{kind="other"} 4`} {
		if !strings.Contains(w.Body.String(), "loki_vl_proxy_backend_read_bytes_total"+want) {
			t.Fatalf("missing byte counter %s", want)
		}
	}
	if strings.Contains(w.Body.String(), "private-query-value") {
		t.Fatal("unbounded input became a metric label")
	}
	p := &OTLPPusher{metrics: m}
	families := p.backendReadMetrics(123)
	if len(families) != 1 || families[0]["name"] != "loki_vl_proxy_backend_read_bytes_total" {
		t.Fatal("missing OTLP family", families)
	}
	sum := families[0]["sum"].(map[string]interface{})
	points := sum["dataPoints"].([]map[string]interface{})
	if len(points) != 4 {
		t.Fatalf("OTLP cardinality=%d", len(points))
	}
	for i, point := range points {
		if point["asInt"] != int64(i+1) {
			t.Fatal("OTLP value differs", point)
		}
		attrs := point["attributes"].([]map[string]interface{})
		if attrs[0]["key"] != "kind" || attrs[0]["value"].(map[string]interface{})["stringValue"] != backendReadKinds[i] {
			t.Fatal("OTLP labels differ", attrs)
		}
	}
}
