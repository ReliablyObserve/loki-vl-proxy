package proxy

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// BenchmarkSplitPostProcessing compares reading the markers of a translated query through translator.SplitPost
// (new) with the two helpers it replaced (old), over the translation of the capture sample: all of it, and the part
// without a marker.
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
	var all, plain []string
	for _, r := range sample.Rows {
		if s, err := translator.TranslateLogQL(r.Query); err == nil {
			all = append(all, s)
			if !strings.Contains(s, translator.WithoutMarkerSuffix) && !strings.Contains(s, "__lvp_lr:") {
				plain = append(plain, s)
			}
		}
	}
	for _, set := range []struct {
		name string
		in   []string
	}{{"all", all}, {"no-marker", plain}} {
		in := set.in
		b.Run(set.name+"/old", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s, _, _ := translator.ParseWithoutMarker(in[i%len(in)])
				_, _ = translator.ParseAllLabelReplaceMarkers(s)
			}
		})
		b.Run(set.name+"/new", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _, _ = splitPostProcessing(in[i%len(in)])
			}
		})
	}
}
