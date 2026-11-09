package proxy

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// gatedStatsTexts are the plan corpus's translated stats queries whose base ends with the unwrap gate.
func gatedStatsTexts(b *testing.B) []string {
	b.Helper()
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		b.Fatal(err)
	}
	var out []string
	for _, q := range qs {
		text, err := translator.TranslateLogQLWithLabels(q, nil)
		if err != nil {
			continue
		}
		clean, _ := translator.SplitPost(text)
		if stats, ok := translator.DecodeStats(clean); ok && stats.Gate != "" {
			out = append(out, clean)
		}
	}
	if len(out) == 0 {
		b.Fatal("no gated stats query")
	}
	return out
}

// legacyUngatedText is the text reader the native-tumbling check used before the gate was decoded.
func legacyUngatedText(query string) string {
	idx := strings.Index(query, " | stats ")
	if idx < 0 {
		return query
	}
	if base, _, ok := translator.SplitUnwrapGate(query[:idx]); ok {
		return base + query[idx:]
	}
	return query
}

// The unwrap-gate reads of one gated range request before: the native-tumbling check split the gate off the whole
// text, and the quantile-counts check, the raw-row spec and the unwrap window check each split it off the base.
func BenchmarkUnwrapGateReadsBefore(b *testing.B) {
	qs := gatedStatsTexts(b)
	specs := make([]statsCompatSpec, len(qs))
	for i, q := range qs {
		specs[i], _ = parseStatsCompatSpec(q)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(qs)
		_ = legacyUngatedText(qs[j])
		for k := 0; k < 3; k++ {
			_, _, _ = translator.SplitUnwrapGate(specs[j].BaseQuery)
		}
	}
}

// After: the decode splits the gate off the base and the text once (translator.DecodeStats), and the three checks
// read the decoded spec (statsCompatSpec.gated).
func BenchmarkUnwrapGateReadsAfter(b *testing.B) {
	qs := gatedStatsTexts(b)
	stats := make([]statsCompatSpec, len(qs))
	bases := make([]string, len(qs))
	for i, q := range qs {
		decoded, _ := translator.DecodeStats(q)
		stats[i], _ = statsSpecOf(decoded)
		bases[i] = decoded.Base
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(qs)
		_, _, _ = translator.SplitUnwrapGate(bases[j])
		_ = legacyUngatedText(qs[j])
		for k := 0; k < 3; k++ {
			_ = stats[j].gated()
		}
	}
}
