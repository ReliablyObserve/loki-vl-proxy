package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// statsPipesOf are stats and math pipes over the given names: every grouping and function shape the stats printer
// writes (no grouping, keys with buckets and offsets, conditions, aliases, the functions printed in place and the
// ones printed through String, no function at all).
func statsPipesOf(a, b, c string) []logsql.Pipe {
	return []logsql.Pipe{
		logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}}}},
		logsql.PipeStats{By: []logsql.GroupKey{{Field: a}}, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{Fields: []string{b, c}}, Alias: c}}},
		logsql.PipeStats{By: []logsql.GroupKey{{Field: a}, {Field: b, Bucket: c}, {Field: c, Bucket: a, Offset: b}}, Funcs: []logsql.StatsFuncAlias{
			{Func: logsql.Sum{Field: a}, Alias: b}, {Func: logsql.Min{Field: b}}, {Func: logsql.Max{Field: c}, If: logsql.FieldExact(a, b)},
			{Func: logsql.Avg{Field: a}}, {Func: logsql.Quantile{Phi: 0.9, Field: b}, Alias: a}, {Func: logsql.Rate{}},
		}},
		logsql.PipeStats{By: []logsql.GroupKey{{Field: a}}},
		logsql.PipeMath{Expr: a + "/" + b, Alias: c},
	}
}

// AppendPipe writes every pipe as its String does: the stats and math pipes print in place, onto the buffer, and the
// text is the same.
//
// conformance: architecture-syntax-package-layering
func TestAppendPipeMatchesString(t *testing.T) {
	for _, p := range append(statsPipesOf("app", "level", "__lvp_inner"), logsql.PipeLimit{N: 5}) {
		if got, want := string(logsql.AppendPipe([]byte("x "), p)), "x "+p.String(); got != want {
			t.Errorf("AppendPipe(%#v) = %q, want %q", p, got, want)
		}
		if st, ok := p.(logsql.PipeStats); ok {
			if got, want := string(logsql.AppendStats(nil, st)), p.String(); got != want {
				t.Errorf("AppendStats(%#v) = %q, want %q", p, got, want)
			}
		}
		if got, want := (logsql.Query{Pipes: []logsql.Pipe{p}}).String(), "* "+p.String(); got != want {
			t.Errorf("Query.String with %#v = %q, want %q", p, got, want)
		}
	}
}

// conformance: architecture-syntax-package-layering
func TestAppendStatsFuncsPreservesTypedFunctionText(t *testing.T) {
	for _, pipe := range statsPipesOf("app", "level", "__lvp_inner") {
		stats, ok := pipe.(logsql.PipeStats)
		if !ok {
			continue
		}
		got := string(logsql.AppendStatsFuncs([]byte("prefix:"), stats.Funcs))
		if len(stats.Funcs) == 0 {
			if got != "prefix:" {
				t.Fatalf("empty functions changed prefix: %q", got)
			}
			continue
		}
		if want := (logsql.PipeStats{Funcs: stats.Funcs}).String(); "| stats "+got[len("prefix:"):] != want {
			t.Fatalf("function-only typed text differs: got %q want %q", got, want)
		}
	}
}

// FuzzAppendPipeMatchesString holds AppendPipe to String on stats and math pipes over any names.
func FuzzAppendPipeMatchesString(f *testing.F) {
	f.Add("app", "level", "__lvp_inner")
	f.Add("", "a b", "\"")
	f.Fuzz(func(t *testing.T, a, b, c string) {
		for _, p := range statsPipesOf(a, b, c) {
			if got, want := string(logsql.AppendPipe(nil, p)), p.String(); got != want {
				t.Fatalf("AppendPipe(%#v) = %q, want %q", p, got, want)
			}
		}
	})
}
