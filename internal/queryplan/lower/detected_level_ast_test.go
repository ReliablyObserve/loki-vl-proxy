package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// pipesText is the text of pipes as the builders write them: each after a space, the first without it.
func pipesText(pipes []logsql.Pipe) string {
	if len(pipes) == 0 {
		return ""
	}
	return strings.TrimPrefix(logsql.Query{Pipes: pipes}.String(), "* ")
}

func filterText(f logsql.FilterExpr) string {
	if f == nil {
		return ""
	}
	return f.String()
}

// nodesText is the plan the text builders write for the same filter.
func nodesText(n *levelFilterNodes) translate.DetectedLevelFilterPlan {
	out := translate.DetectedLevelFilterPlan{Never: n.Never, Prefilter: filterText(n.Prefilter), Match: filterText(n.Match), Pipes: pipesText(n.Pipes)}
	if n.PrefilterPipe != nil {
		out.PrefilterPipe = "| filter " + n.PrefilterPipe.String()
	}
	return out
}

// levelBuilderValues are the values the typed builders are held to the text builders on: every spelling in three
// cases, unknown, words no level is, the empty value, quotes, backslashes and backquotes, and regexps (alternations,
// wildcards, case-insensitive ones, ones the words of cannot be listed, one that does not compile).
func levelBuilderValues() []string {
	vs := []string{"", "unknown", "UNKNOWN", "x", "notice", "a\"b", `a\b`, "a`b", "é", "error ", " error", "5", "critical", "fatal"}
	for _, s := range translate.DetectedLevelSpellings() {
		for _, w := range s.Spellings {
			vs = append(vs, w, strings.ToUpper(w), strings.ToUpper(w[:1])+w[1:])
		}
	}
	return append(vs, "error|warn", "(?i)err.*", ".*", ".+", "unknown|error", "debu.*", "(", "e.r", "^error$", "warn|",
		"[ew].*", "ERROR|warn", "(?i)unknown", "info|debug", "notice|error", "a`b|c", "x.*", "(a|b)c", "error|notice|x")
}

// The typed builders of a filter on detected_level print the text builders' text, byte for byte: the chain of each
// mode, the filter of a stored level, and for every operator, value and mode the prefilter, the match, the pipes
// and whether no row passes, alone and joined two by two.
//
// conformance: architecture-syntax-package-layering
func TestDetectedLevelNodesPrintTheBuildersText(t *testing.T) {
	fields := translate.DetectedLevelFieldNames
	if got, want := pipesText(levelChainPipes(translate.DetectedLevelChainGroup)), translate.BuildDetectedLevelChain(fields, translate.DetectedLevelChainGroup, nil); got != want {
		t.Fatalf("group chain:\n typed %s\n  text %s", got, want)
	}
	if got, want := pipesText(levelChainPipes(translate.DetectedLevelChainGrouping)), translate.BuildDetectedLevelChain(fields, translate.DetectedLevelChainGrouping, nil); got != want {
		t.Fatalf("grouping chain:\n typed %s\n  text %s", got, want)
	}
	if got, want := hasLevelFilter().String(), translate.BuildHasLevelFilter(fields); got != want {
		t.Fatalf("has-level filter:\n typed %s\n  text %s", got, want)
	}
	modes := []translate.DetectedLevelFilterMode{translate.DetectedLevelFilterLogs, translate.DetectedLevelFilterMetric, translate.DetectedLevelFilterStored}
	checked := 0
	for _, mode := range modes {
		var built []*levelFilterNodes
		var texts []translate.DetectedLevelFilterPlan
		for _, op := range []string{"=", "!=", "=~", "!~"} {
			for _, v := range levelBuilderValues() {
				want, werr := translate.BuildDetectedLevelFilter(op, v, fields, mode)
				got, gerr := buildLevelFilterNodes(op, v, mode)
				if (werr == nil) != (gerr == nil) {
					t.Fatalf("mode %d %s %q: typed error %v, text error %v", mode, op, v, gerr, werr)
				}
				if werr != nil {
					continue
				}
				checked++
				if g := nodesText(&got); g != want {
					t.Errorf("mode %d %s %q:\n typed %+v\n  text %+v", mode, op, v, g, want)
					continue
				}
				if m, err := translate.MatchDetectedLevel(op, v); err == nil {
					if s, _ := translate.BuildStoredLevelFilter(op, v, fields); filterText(storedFilter(m)) != s {
						t.Errorf("%s %q stored filter:\n typed %s\n  text %s", op, v, filterText(storedFilter(m)), s)
					}
				}
				built, texts = append(built, &got), append(texts, want)
			}
		}
		// Two filters in a row, every pair of a sample.
		for i := 0; i < len(built); i += 7 {
			for j := 0; j < len(built); j += 11 {
				got := pipesText(joinLevelFilters([]*levelFilterNodes{built[i], built[j]}, mode))
				want := translate.JoinDetectedLevelFilters([]translate.DetectedLevelFilterPlan{texts[i], texts[j]}, fields, mode)
				if got != want {
					t.Fatalf("mode %d join %+v, %+v:\n typed %s\n  text %s", mode, texts[i], texts[j], got, want)
				}
			}
		}
	}
	t.Logf("%d filters held to the text builders", checked)
}

// FuzzDetectedLevelNodes holds the typed builders to the text builders on any operator and value.
//
// conformance: architecture-syntax-package-layering
func FuzzDetectedLevelNodes(f *testing.F) {
	f.Add(uint8(0), "error")
	f.Add(uint8(2), "info|(?i)warn.*")
	f.Add(uint8(3), "a`b|notice")
	f.Fuzz(func(t *testing.T, op uint8, v string) {
		if len(v) > 64 {
			return
		}
		o := [...]string{"=", "!=", "=~", "!~"}[op%4]
		mode := []translate.DetectedLevelFilterMode{translate.DetectedLevelFilterLogs, translate.DetectedLevelFilterMetric, translate.DetectedLevelFilterStored}[int(op/4)%3]
		want, werr := translate.BuildDetectedLevelFilter(o, v, translate.DetectedLevelFieldNames, mode)
		got, gerr := buildLevelFilterNodes(o, v, mode)
		if (werr == nil) != (gerr == nil) {
			t.Fatalf("%s %q: typed error %v, text error %v", o, v, gerr, werr)
		}
		if werr == nil && nodesText(&got) != want {
			t.Fatalf("mode %d %s %q:\n typed %+v\n  text %+v", mode, o, v, nodesText(&got), want)
		}
	})
}
