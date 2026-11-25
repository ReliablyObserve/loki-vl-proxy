package proxy

import (
	"fmt"
	"strings"
	"testing"
)

// BenchmarkFieldMappingLabels measures the label paths that read a mapped
// field's names, in each -field-mapping-label-mode: the /labels listing, one
// stream's labels, the detected_labels row scan and one volume row.
func BenchmarkFieldMappingLabels(b *testing.B) {
	listed := []string{"k8s.namespace.name", "k8s.pod.name", "k8s.container.name", "custom.team", "host.name", "service.name"}
	for i := range 60 {
		listed = append(listed, fmt.Sprintf("custom.attribute_%d", i))
	}
	stream := map[string]string{"k8s.namespace.name": "prod", "k8s.pod.name": "api-1", "host.name": "node-a", "service.name": "api"}
	rows := strings.Repeat(`{"_msg":"x","_stream":"{k8s.namespace.name=\"prod\",k8s.pod.name=\"api-1\",host.name=\"node-a\",service.name=\"api\"}"}`+"\n", 200)
	for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeOriginal, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
		p := &Proxy{labelTranslator: NewLabelTranslator(LabelStyleUnderscores, mappingTestMappings()),
			declaredLabelNames: []string{"cluster", "host.name"}}
		p.labelTranslator.SetFieldMappingLabelMode(mode)
		b.Run("listing/"+string(mode), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = p.lokiLabelListing(listed)
			}
		})
		b.Run("stream/"+string(mode), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = p.labelTranslator.TranslateLabelsMap(stream)
			}
		})
		b.Run("detectedScan/"+string(mode), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := scanDetectedLabelSummariesStream(strings.NewReader(rows), p.labelTranslator); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("volumeRow/"+string(mode), func(b *testing.B) {
			b.ReportAllocs()
			req := volumeRequest{limit: 100}
			plan := volumePlan{keys: []string{"pod", "host_name"}, required: map[string]bool{"pod": true}}
			row := map[string]string{"k8s.pod.name": "api-1", "host.name": "node-a"}
			metrics := map[string]map[string]string{}
			for b.Loop() {
				_ = p.volumeNamesForRow(req, plan, row, metrics)
			}
		})
	}
}
