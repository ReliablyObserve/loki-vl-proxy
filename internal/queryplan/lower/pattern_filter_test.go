package lower

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// loweredMatchesTranslator reports whether the plan lowers q to the query the string translator writes, read by
// the LogsQL parser (grouping and the case of `or` are spelling). ok is false when neither lowers q.
func loweredMatchesTranslator(t testing.TB, q string) (lowered, legacy string, same, ok bool) {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		return "", "", false, false
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return "", "", false, false
	}
	queryplan.Resolve(p)
	legacy, terr := translator.TranslateLogQLWithLabels(q, nil)
	op, lerr := Plan(p, Profile{})
	if terr != nil || lerr != nil {
		if (terr == nil) != (lerr == nil) {
			t.Fatalf("%s: translator error %v, lowering error %v", q, terr, lerr)
		}
		return "", "", false, false
	}
	lowered = op.(physical.VLQuery).Text
	want, err := logsql.Parse(legacy)
	if err != nil {
		t.Fatalf("%s: the translator's %s does not parse: %v", q, legacy, err)
	}
	got, err := logsql.Parse(lowered)
	if err != nil {
		t.Fatalf("%s: the lowered %s does not parse: %v", q, lowered, err)
	}
	return lowered, legacy, reflect.DeepEqual(want, got), true
}

// A pattern line filter lowers to the query the string translator writes, for every shape its builder has: the
// exact regexp, the single-literal filters, the extract pipe with its filter and delete, or chains that mix them,
// several extracting filters (numbered apart), negations, line filters after a pipe (in one filter pipe), and
// literals that the extract pattern escapes.
//
// conformance: architecture-syntax-package-layering, semantics/pattern-line-filter
func TestPlanLowersPatternLineFilters(t *testing.T) {
	for _, q := range []string{
		`{app="a"} |> "<_>"`,
		`{app="a"} |> "x <_>"`,
		`{app="a"} |> ""`,
		`{app="a"} |> "lit"`,
		`{app="a"} |> "<_> x"`,
		`{app="a"} |> "<_> x <_>"`,
		`{app="a"} !> "<_> x <_>"`,
		`{app="a"} |> "<_> x <_> y"`,
		`{app="a"} !> "<_> x <_> y"`,
		`{app="a"} |> "<_> x <_> y" or "<_> z <_>"`,
		`{app="a"} |> "<_> z <_>" or "q"`,
		`{app="a"} |> "x <_>" or "y <_>"`,
		`{app="a"} |> "<_> x <_> y" or "<_>"`,
		`{app="a"} !> "<_> x <_> y" or "q" or "w <_>"`,
		`{app="a"} |> "<_> x <_> y" |= "k" |> "<_> m <_> n"`,
		`{app="a"} !> "<_> x <_> y" |= "k" != "j"`,
		`{app="a"} !> "<_> x <_> y" !> "<_> m <_> n" |~ "r.+"`,
		`{app="a"} |= "k" |> "<_> x <_> y" |> "<_> q <_>"`,
		`{app="a"} |> "<_> a<b & \"c\" <_> d"`,
		"{app=\"a\"} |> `<_> \\x <_> y`",
		`{app="a"} |> "<_> x <_> y" != ip("10.0.0.1")`,
	} {
		lowered, legacy, same, ok := loweredMatchesTranslator(t, q)
		if !ok {
			t.Fatalf("%s: not lowered", q)
		}
		if !same {
			t.Errorf("%s:\n lowered %s\n  string %s", q, lowered, legacy)
		}
	}
}

// FuzzPatternLineFilterLowering holds the lowering of pattern line filters to the string translator on any
// patterns, operators and chains: both write the same query, or both refuse it.
//
// conformance: architecture-syntax-package-layering
func FuzzPatternLineFilterLowering(f *testing.F) {
	f.Add("<_> x <_> y", "<_> z <_>", "k", uint8(0))
	f.Add("x <_>", "q", "", uint8(5))
	f.Add("<_> a<b & \"c\" <_>", "", "m", uint8(10))
	f.Fuzz(func(t *testing.T, p1, p2, text string, ops uint8) {
		op := func(bit uint8) string {
			if ops&bit != 0 {
				return "!>"
			}
			return "|>"
		}
		q := `{app="a"} ` + op(1) + " " + strconv.Quote(p1)
		if ops&2 != 0 {
			q += " or " + strconv.Quote(p2)
		}
		if ops&4 != 0 {
			q += ` |= ` + strconv.Quote(text)
		}
		if ops&8 != 0 {
			q += " " + op(16) + " " + strconv.Quote(p2)
		}
		if lowered, legacy, same, ok := loweredMatchesTranslator(t, q); ok && !same {
			t.Fatalf("%s:\n lowered %s\n  string %s", q, lowered, legacy)
		}
	})
}
