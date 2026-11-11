package logql

import "testing"

// The text a stage that starts with a name is kept as is read back to the same stage: a space between the name and
// the first token that follows it stays (`A 0` was printed `A0`, one name), and the bytes of a character the scanner
// cannot decode are not replaced by U+FFFD. The query is printed by the AST (keepSeriesGrouping rewrites a metric
// query that way) and must parse to the same stages again.
//
// conformance: semantics/keep-leaves-labels-loki-drops
func TestNameStageKeepsItsSource(t *testing.T) {
	for _, q := range []string{
		`{a="b"} | A 0!0`,
		`{a="b"} | A 0`,
		`{a="b"} | A "x"`,
		`{a="b"} | A b`,
		"{a=\"b\"} | A 0!0\xed\xa1!",
		"{a=\"b\"} | A \xff",
		"{a=\"b\"} | A\xff 0",
		`{a="b"} | A.b 0`,
		`{a="b"} | A0 0`,
	} {
		first, err := ParseLogQuery(q)
		if err != nil {
			continue // rejected here: nothing to print
		}
		printed := first.String()
		second, err := ParseLogQuery(printed)
		if err != nil {
			t.Errorf("%q printed as %q, which does not parse: %v", q, printed, err)
			continue
		}
		if second.String() != printed {
			t.Errorf("%q printed as %q, then %q", q, printed, second.String())
		}
		for i, st := range first.Pipeline {
			if lf, ok := st.(*LabelFilterStage); ok {
				if i >= len(second.Pipeline) || second.Pipeline[i].String() != lf.String() {
					t.Errorf("%q: stage %d changed on the way through print and parse", q, i)
				}
			}
		}
	}
}
