package proxy

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// BenchmarkSplitPostProcessing compares reading the markers of a translated query through the physical-plan codec
// (new) with the two helpers it replaced (old), over the translation of the capture sample.
func BenchmarkSplitPostProcessing(b *testing.B) {
	raw, err := os.ReadFile("../querycorpus/testdata/plan-capture-sample.json")
	if err != nil {
		b.Fatal(err)
	}
	var sample struct {
		Rows []struct {
			Query string `json:"q"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &sample); err != nil {
		b.Fatal(err)
	}
	var in []string
	for _, r := range sample.Rows {
		if s, err := translator.TranslateLogQL(r.Query); err == nil {
			in = append(in, s)
		}
	}
	b.Run("old", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s, _, _ := translator.ParseWithoutMarker(in[i%len(in)])
			_, _ = translator.ParseAllLabelReplaceMarkers(s)
		}
	})
	b.Run("new", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _, _ = splitPostProcessing(in[i%len(in)])
		}
	})
}
