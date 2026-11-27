package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
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
	for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeUnset, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
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

// BenchmarkFieldMappingLabelRoutes measures the label routes through their
// handlers in each mode, with the VictoriaLogs calls of a request: cold (a new
// window with the response cache off) and repeat (the same request again).
func BenchmarkFieldMappingLabelRoutes(b *testing.B) {
	routes := []struct{ name, path, extra string }{
		{"labels", "/loki/api/v1/labels", ""},
		{"labelValues", "/loki/api/v1/label/pod/values", ""},
		{"detectedLabels", "/loki/api/v1/detected_labels", ""},
		{"volume", "/loki/api/v1/index/volume", "&targetLabels=pod"},
		{"volumeRange", "/loki/api/v1/index/volume_range", "&targetLabels=pod&step=600"},
	}
	for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeUnset, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
		for _, route := range routes {
			for _, state := range []string{"cold", "repeat"} {
				b.Run(route.name+"/"+state+"/"+string(mode), func(b *testing.B) {
					vl := mappingVLData()
					var c *cache.Cache
					if state == "cold" {
						c = cache.NewDisabled()
					}
					p := newMappingProxy(b, vl.server(b).URL, c, Config{LabelStyle: LabelStyleUnderscores,
						FieldMappingLabelMode: mode, FieldMappings: mappingTestMappings()})
					mux := http.NewServeMux()
					p.RegisterProxyRoutes(mux)
					request := func(window int) {
						// Windows ten days apart share no cached listing.
						start := int64(1_600_000_000) + int64(window)*864_000
						target := fmt.Sprintf("%s?query=%%7Bpod%%3D%%22api-1%%22%%7D&start=%d000000000&end=%d000000000%s",
							route.path, start, start+3600, route.extra)
						rec := httptest.NewRecorder()
						mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
						if rec.Code != http.StatusOK {
							b.Fatalf("%d %s", rec.Code, rec.Body.String())
						}
					}
					request(0)
					vl.snapshot()
					b.ReportAllocs()
					n := 0
					for b.Loop() {
						if state == "cold" {
							n++
						}
						request(n)
					}
					calls := 0
					for _, count := range vl.snapshot() {
						calls += count
					}
					b.ReportMetric(float64(calls)/float64(b.N), "vl-calls/op")
				})
			}
		}
	}
}

// BenchmarkFieldMappingAliasCollision measures what the alias collision rules
// cost per mode: the detected_fields scan of rows whose lines carry keys named
// like the aliases, and the lowering of a metric that groups by an alias after
// a parser (the translation cache off).
func BenchmarkFieldMappingAliasCollision(b *testing.B) {
	for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeUnset, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
		p := newMappingProxy(b, collisionVLFor(b), cache.NewDisabled(), Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
			FieldMappingLabelMode: mode, FieldMappings: collisionMappings()})
		mux := http.NewServeMux()
		p.RegisterProxyRoutes(mux)
		b.Run("detectedFields/"+string(mode), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_fields?query=%7Bservice_name%3D%22svc%22%7D&start=1767222000000000000&end=1767225601000000000", nil))
				if rec.Code != http.StatusOK {
					b.Fatal(rec.Code)
				}
			}
		})
		b.Run("lowerGrouping/"+string(mode), func(b *testing.B) {
			b.ReportAllocs()
			p.translationCache = nil
			for b.Loop() {
				if _, err := p.translateQuery(`sum by (pod, namespace_extracted) (count_over_time({service_name="svc"} | logfmt | pod_extracted!="" [5m]))`); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
