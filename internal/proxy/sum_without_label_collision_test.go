package proxy

import (
	"strings"
	"testing"
)

// TestSumWithoutExtractedLabelCollision: a parsed key that is also a label of
// the stream becomes `<key>_extracted` on Loki (pkg/logql/log/labels.go,
// parser.go), so a series is the stream, its `pod` and the parsed `pod_extracted`.
// When the selector names the label (`pod=~".+"`) the translation sees the
// collision and groups the fold as it did before the extraction list was read
// (by the stream); when it does not, the translation cannot see it and the
// answer carries the parsed value under the stream label's name. Both differ
// from Loki, whose labels the test spells out
// (semantics/unwrap-sum-without-extracted-label-collision, open); the second
// pins the proxy's answer so that a fix asks for a parity case.
//
// conformance: semantics/unwrap-sum-without-extracted-label-collision, metric-series-identity
func TestSumWithoutExtractedLabelCollision(t *testing.T) {
	parsedPod := func(rows []emuRow) []emuRow {
		for i := range rows {
			rows[i].labels["pod"] = []string{"p1", "p2"}[i%2]
		}
		return rows
	}
	t.Run("the selector names the label", func(t *testing.T) {
		sent, answer := runExtractionListShapeRows(t, `sum without (ns) (max_over_time({app="uw", pod=~".+"} | logfmt amount, pod | unwrap amount [5m]))`, extractionListModes[0], parsedPod)
		if last := sent[len(sent)-1]; !strings.HasSuffix(last, "| stats by (_stream, __lvp_bad) sum(__lvp_inner)") || strings.Contains(last, "pod, __lvp_bad") {
			t.Fatalf("the fold groups by the parsed pod: %s", last)
		}
		const want = "3 series: {app=uw,pod=a,service_name=uw} 1.8e+09=881; {app=uw,pod=b,service_name=uw} 1.8e+09=470; {app=uw,pod=c,service_name=uw} 1.8e+09=340"
		if answer != want {
			t.Fatalf("answer %s, want the fold by the stream: %s", answer, want)
		}
		if strings.Contains(answer, "pod_extracted") {
			t.Fatal("the proxy answers Loki's pod_extracted label: the collision is handled, turn this into a parity case")
		}
	})
	t.Run("the selector does not name the label", func(t *testing.T) {
		_, answer := runExtractionListShapeRows(t, `sum without (ns) (max_over_time({app="uw"} | logfmt amount, pod | unwrap amount [5m]))`, extractionListModes[0], parsedPod)
		const want = "2 series: {app=uw,pod=p1,service_name=uw} 1.8e+09=172; {app=uw,pod=p2,service_name=uw} 1.8e+09=186"
		if answer != want {
			t.Fatalf("answer %s, want the proxy's known answer %s (Loki: {app,pod=<stream pod>,pod_extracted=<parsed pod>})", answer, want)
		}
	})
}
