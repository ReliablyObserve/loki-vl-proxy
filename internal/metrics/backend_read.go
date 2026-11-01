package metrics

import (
	"fmt"
	"strings"
)

var backendReadKinds = [...]string{"raw", "stats", "metadata", "other"}

// RecordBackendReadBytes counts consumed decoded bytes at upstream body close.
// Unknown caller input folds into a single label, never creating a series.
func (m *Metrics) RecordBackendReadBytes(kind string, n int64) {
	if n <= 0 {
		return
	}
	i := 3
	switch kind {
	case "raw":
		i = 0
	case "stats":
		i = 1
	case "metadata":
		i = 2
	}
	m.backendReadBytes[i].Add(n)
}

func (m *Metrics) writeBackendReadMetrics(sb *strings.Builder) {
	sb.WriteString("# HELP loki_vl_proxy_backend_read_bytes_total Consumed decoded upstream body bytes recorded at close; not wire traffic or cache bytes.\n# TYPE loki_vl_proxy_backend_read_bytes_total counter\n")
	for i, kind := range backendReadKinds {
		fmt.Fprintf(sb, "loki_vl_proxy_backend_read_bytes_total{kind=%q} %d\n", kind, m.backendReadBytes[i].Load())
	}
}

func (p *OTLPPusher) backendReadMetrics(now int64) []map[string]interface{} {
	points := make([]map[string]interface{}, 0, len(backendReadKinds))
	for i, kind := range backendReadKinds {
		points = append(points, p.counterDP("loki_vl_proxy_backend_read_bytes_total", p.metrics.backendReadBytes[i].Load(), now, attr("kind", kind)))
	}
	return []map[string]interface{}{p.sumMetric("loki_vl_proxy_backend_read_bytes_total", "Consumed decoded upstream body bytes recorded at close; not wire traffic or cache bytes.", "By", points...)}
}
