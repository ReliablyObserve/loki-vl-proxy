package proxy

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// checkGate compares the spec's gate reading with translator.SplitUnwrapGate on its current base query.
func checkGate(t *testing.T, where string, spec statsCompatSpec) {
	t.Helper()
	base, _, ok := translator.SplitUnwrapGate(spec.BaseQuery)
	if spec.gated() != ok || spec.ungated() != base {
		t.Fatalf("%s: gated %v ungated %q; SplitUnwrapGate %v %q (base %q, gate %q)", where, spec.gated(), spec.ungated(), ok, base, spec.BaseQuery, spec.Gate)
	}
}

// The handlers read the unwrap gate from the decoded spec (statsCompatSpec.gated and ungated) where they split it
// off the base query's text: on every plan-corpus row's translation (two VictoriaLogs versions, plain and
// underscore labels), the reading equals translator.SplitUnwrapGate as decoded, after the window-phase rewrite
// that inserts a filter before the gate, and after withoutUnwrapGate; UngatedText equals the text reader.
//
// conformance: architecture-syntax-package-layering
func TestUnwrapGateReadFromTheDecodedSpec(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	underscores := func(s string) string { return strings.ReplaceAll(s, ".", "_") }
	start := time.Unix(1767225600, 0)
	ctx := context.Background()
	for _, v := range []string{"1.53.0", "1.49.0"} {
		for name, labelFn := range map[string]translator.LabelTranslateFunc{"identity": nil, "underscores": underscores} {
			t.Run(v+" "+name, func(t *testing.T) {
				gated := 0
				for _, q := range qs {
					text, err := translator.TranslateLogQLWithCapabilities(q, labelFn, nil, logsql.CapabilitiesFor(v))
					if err != nil {
						continue
					}
					clean, _ := translator.SplitPost(text)
					stats, isStats := translator.DecodeStats(clean)
					if !isStats {
						continue
					}
					spec, ok := statsSpecOf(stats)
					if !ok {
						continue
					}
					checkGate(t, q+" decoded", spec)
					if spec.gated() {
						gated++
					}
					phased := spec
					phased.BaseQuery = withWindowPhaseFilter(ctx, phased.BaseQuery, start, time.Minute, 30*time.Second)
					checkGate(t, q+" window phase", phased)
					checkGate(t, q+" without gate", withoutUnwrapGate(spec))
					checkGate(t, q+" window phase without gate", withoutUnwrapGate(phased))
				}
				t.Logf("%d gated stats queries", gated)
				if gated == 0 {
					t.Fatal("no gated stats query in the corpus")
				}
			})
		}
	}
}
