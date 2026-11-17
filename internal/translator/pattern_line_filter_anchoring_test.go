package translator

import (
	"strconv"
	"strings"
	"testing"
)

// This file pins the shape of the LogsQL the string translator writes for a pattern line filter (|> and !>): which
// patterns need the extract pipe and the order of its prefilters. Whether the filter keeps the lines Loki keeps is
// held on the plan's nodes against Loki's recorded verdicts (internal/translate, TestPatternLineFilterKeepsLokiLines),
// which also checks that the nodes write this translator's text.

// translatedPattern is `{app="x"} <op> <operands>` after translation: the
// LogsQL text and the part that follows the stream selector.
type translatedPattern struct {
	logql, logsql, query string
}

func translatePattern(t *testing.T, op string, patterns []string) translatedPattern {
	t.Helper()
	quoted := make([]string, len(patterns))
	for i, p := range patterns {
		quoted[i] = strconv.Quote(p)
	}
	logql := `{app="x"} ` + op + " " + strings.Join(quoted, " or ")
	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("%s: %v", logql, err)
	}
	q := strings.TrimSpace(strings.TrimPrefix(got, `app:="x"`))
	if strings.HasPrefix(q, "| ") {
		// No bare filter before the first pipe: every line enters it.
		q = `~"" ` + q
	}
	return translatedPattern{logql: logql, logsql: got, query: q}
}

// TestPatternLineFilterSingleLiteralNeedsNoPipe pins which patterns are
// answered by a filter alone and which still extract.
func TestPatternLineFilterSingleLiteralNeedsNoPipe(t *testing.T) {
	for pattern, pipe := range map[string]bool{
		"<_>": false, "foo<_>": false, "foo": false, "<_>foo": false, "<_>foo<_>": false, "<_> GET <_>": false,
		"foo<_>bar": true, "<_>a<_>b": true, "<_>a<_>b<_>": true, "a<_>b<_>": true, "<_> a <_> b": true,
	} {
		tp := translatePattern(t, "|>", []string{pattern})
		if got := strings.Contains(tp.logsql, "| extract"); got != pipe {
			t.Errorf("%q: extract=%v, want %v: %s", pattern, got, pipe, tp.logsql)
		}
	}
}

// TestPatternLineFilterLiteralPrefilterOrder pins the order of a |> pattern
// that needs extract: a plain filter per literal, then the anchored regexp,
// then the extract pipe. VictoriaLogs answers a plain regexp as a substring
// search that skips the blocks without the text, so it must run first; the
// plain filters are necessary conditions, so the answer does not change (the
// corpus tests above evaluate them too). `!>` takes none, and an alternative
// without a literal (`<_>`) takes them away from the whole chain.
func TestPatternLineFilterLiteralPrefilterOrder(t *testing.T) {
	for _, tc := range []struct{ op, pattern, want string }{
		{"|>", `<_> "GET <_> HTTP/1.1" <_>`, `_msg:~" \"GET " _msg:~" HTTP/1\\.1\" " ~"(?s)^.+ \"GET .+ HTTP/1\\.1\" .+" | extract`},
		{"|>", `<_>a<_>a<_>`, `_msg:~"a" ~"(?s)^.+a.+a.+" | extract`},
		{"!>", `<_> "GET <_> HTTP/1.1" <_>`, `app:="x" | extract`},
	} {
		got, err := TranslateLogQL(`{app="x"} ` + tc.op + " " + strconv.Quote(tc.pattern))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got, tc.want) {
			t.Errorf("%s %q: want %q in %s", tc.op, tc.pattern, tc.want, got)
		}
	}
	got, err := TranslateLogQL(`{app="x"} |> "<_> a <_> b <_>" or "<_>"`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, `_msg:~" a "`) {
		t.Errorf("an alternative without a literal takes the literal prefilter away from the chain: %s", got)
	}
}
