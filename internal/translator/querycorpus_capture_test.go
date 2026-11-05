//go:build corpuscapture

package translator

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// The inner query of label_replace goes through translateLogQLFull again,
// from tryTranslateLabelReplace. Only the query the test passed in is a
// client input; the inner one is recorded as built by the translator.
func TestQueryCorpusMarksTranslatorSubQueriesInternal(t *testing.T) {
	const outer = `label_replace(rate({app="a"}[5m]), "dst", "$1", "app", "(.*)")`
	got := querycorpus.CaptureForTest(t, func() {
		if _, err := TranslateLogQLWithCapabilities(outer, func(s string) string { return s }, nil, logsql.Capabilities{}); err != nil {
			t.Fatal(err)
		}
	})
	direct := map[string]bool{}
	for _, e := range got {
		if e.Kind == querycorpus.LogQLTranslate {
			direct[e.Query] = direct[e.Query] || e.Direct
		}
	}
	if !direct[outer] {
		t.Fatalf("outer query not recorded as a client input: %+v", got)
	}
	if direct[`rate({app="a"}[5m])`] {
		t.Fatalf("inner query recorded as a client input: %+v", got)
	}
	if _, ok := direct[`rate({app="a"}[5m])`]; !ok {
		t.Fatalf("inner query not recorded: %+v", got)
	}
}
