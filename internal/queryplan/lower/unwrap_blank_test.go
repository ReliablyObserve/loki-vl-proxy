package lower

import (
	"errors"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// An unwrap written with a tab, a line feed, a carriage return or a run of blanks where the spaced spelling has one
// space (`| unwrap<CR>n`, `|\tunwrap n`, `|  unwrap  n`) is the query with `| unwrap n` to Loki's lexer
// (internal/logql TestBlankAfterKeywordMatchesLoki, recorded Loki answers): the lowering writes the spaced spelling's
// text for it, with no parser before the unwrap and after one, under a range function and under a vector aggregation.
// The string translator finds no `| unwrap ` there and fails the query; the plan serves it
// (semantics/translator-reads-only-space-between-tokens). A spelling Loki reads another way stays as it was.
//
// conformance: semantics/translator-reads-only-space-between-tokens
func TestUnwrapBlankSpellingsLowerAsSpaced(t *testing.T) {
	templates := []string{
		`max_over_time({A="0"} |¤unwrap¤A [1m])`, `max_over_time({A="0"}|¤unwrap¤A [1m])`,
		`sum_over_time({A="0"} | json |¤unwrap¤a [1m])`, `sum_over_time({A="0"} | logfmt |¤unwrap¤bytes(a) [1m])`,
		`quantile_over_time(0.5, {app="x"} |¤unwrap¤c [5m]) by (d)`, `sum(avg_over_time({A="0"} |¤unwrap¤duration(a)¤[1m]))`,
		`max by (a) (max_over_time({A="0"} |¤unwrap¤A [1m]))`,
		// The pipe written right before unwrap, with a blank after it or with one space (`|unwrap n`).
		`max_over_time({A="0"} |unwrap¤A [1m])`, `sum_over_time({A="0"} | json |unwrap¤a [1m])`,
	}
	rows := 0
	for _, tpl := range templates {
		spaced := strings.ReplaceAll(strings.ReplaceAll(tpl, "|unwrap", "| unwrap"), "¤", " ")
		want, err := PlanQuery(spaced, planOf(t, spaced), Profile{})
		if err != nil {
			t.Fatalf("%s: the spaced spelling is not lowered: %v", spaced, err)
		}
		for _, blank := range []string{"\t", "\n", "\r", "\r\n", " \t ", "  ", " "} {
			q := strings.ReplaceAll(tpl, "¤", blank)
			got, err := PlanQuery(q, planOf(t, q), Profile{})
			if err != nil {
				t.Errorf("%q: %v, want the spaced spelling's text", q, err)
				continue
			}
			if g, w := physical.Encode(got), physical.Encode(want); g != w {
				t.Errorf("%q:\n got %s\nwant %s", q, g, w)
			}
			rows++
		}
	}
	// A quoted `| unwrap ` before the stage keeps the translator's reading of the text: the lowering leaves it.
	q := "sum_over_time({a=\"b\"} |= \"| unwrap m\" |\tunwrap n [5m])"
	if _, err := PlanQuery(q, planOf(t, q), Profile{}); !errors.Is(err, ErrNotLowered) {
		t.Errorf("%q: %v, want not lowered", q, err)
	}
	t.Logf("%d blank spellings lowered as the spaced one", rows)
}

// spacedBlanks writes each run of the blanks Loki skips outside quoted strings as one space, a space after a pipe a
// name follows, and keeps the strings.
//
// conformance: semantics/translator-reads-only-space-between-tokens
func TestSpacedBlanks(t *testing.T) {
	for in, want := range map[string]string{
		"a | unwrap b":                "a | unwrap b",
		"a |\tunwrap\r\nb":            "a | unwrap b",
		"a  |  unwrap  b":             "a | unwrap b",
		"a |= \"x\t\ty\" |\tunwrap b": "a |= \"x\t\ty\" | unwrap b",
		"a |= `x\n\ny` |\nunwrap b":   "a |= `x\n\ny` | unwrap b",
		"a |= \"x\\\"\t\" |\tb":       "a |= \"x\\\"\t\" | b",
		"a |= \"open\t":               "a |= \"open\t",
		"a |unwrap b |= \"|x\"":       "a | unwrap b |= \"|x\"",
		"a |= \"x\" |json":            "a |= \"x\" | json",
		"":                            "",
	} {
		if got := spacedBlanks(in); got != want {
			t.Errorf("spacedBlanks(%q) = %q, want %q", in, got, want)
		}
	}
}

// BenchmarkUnwrapScreen measures the unwrap spelling screen and the lowering of an unwrap with no parser: the spaced
// spelling (one search of the text, as before) and a tab after unwrap (the search, then the text read again with its
// blanks as single spaces).
func BenchmarkUnwrapScreen(b *testing.B) {
	for _, q := range []struct{ name, q string }{
		{"spaced", `max_over_time({app="a"} | unwrap n [1m])`},
		{"tab", "max_over_time({app=\"a\"} | unwrap\tn [1m])"},
	} {
		expr, err := logql.Parse(q.q)
		if err != nil {
			b.Fatal(err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			b.Fatal(err)
		}
		queryplan.Resolve(p)
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := ScreenText(q.q, p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
