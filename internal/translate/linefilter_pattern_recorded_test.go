package translate

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The pattern line filter (`|>`, `!>`) is held to Loki's own verdicts, recorded with the Loki pipeline oracle
// (test/oracle/loki process: Loki parses the log query, builds its pipeline and runs it on one line). Each grid
// below is a list of (filter, line) cases; testdata/pattern-line-filter-loki.txt.gz holds one line per grid: its
// name and one character per case, 1 when Loki keeps the line, 0 when it drops it and E when Loki rejects the
// filter (named captures, captures in a row: TestPatternLineFilterValidationMatchesLoki holds those). A run with
// LVP_RUN_ORACLES=1 records the file again.

// patternCase is one pattern line filter (op is `|>` or `!>`, patterns are its `or` alternatives) and one line.
type patternCase struct {
	op       string
	patterns []string
	line     string
	// alphabet and maxLen, when set, say the case's line is one of every line of at most maxLen pieces of the alphabet.
	alphabet []string
	maxLen   int
}

func (c patternCase) query() string {
	quoted := make([]string, len(c.patterns))
	for i, p := range c.patterns {
		quoted[i] = strconv.Quote(p)
	}
	return `{app="x"} ` + c.op + " " + strings.Join(quoted, " or ")
}

// A grid of enumerated lines also holds each filter to seeing both outcomes: under `|>` at least one line kept and one
// dropped (a filter no line of the grid can satisfy would only ever be seen dropping lines).
type patternGrid struct {
	name      string
	cases     []patternCase
	perFilter bool
}

const patternLokiAnswers = "testdata/pattern-line-filter-loki.txt.gz"

// bothOps is every filter of filters under `|>` and `!>`, each against every line.
func bothOps(filters [][]string, lines []string, enumerated ...any) []patternCase {
	var alphabet []string
	maxLen := 0
	if len(enumerated) == 2 {
		alphabet, maxLen = enumerated[0].([]string), enumerated[1].(int)
	}
	var out []patternCase
	for _, f := range filters {
		for _, op := range []string{"|>", "!>"} {
			for _, l := range lines {
				out = append(out, patternCase{op, f, l, alphabet, maxLen})
			}
		}
	}
	return out
}

// fewestPieces is the fewest pieces of the alphabet that write text.
func fewestPieces(text string, alphabet []string) (int, bool) {
	least := map[int]int{0: 0}
	for i := 0; i < len(text); i++ {
		n, ok := least[i]
		if !ok {
			continue
		}
		for _, piece := range alphabet {
			if strings.HasPrefix(text[i:], piece) {
				if m, seen := least[i+len(piece)]; !seen || n+1 < m {
					least[i+len(piece)] = n + 1
				}
			}
		}
	}
	n, ok := least[len(text)]
	return n, ok
}

// satisfiable reports whether some line of an enumerated grid (every line of at most maxLen pieces of the alphabet)
// can pass an alternative of the filter at all: its literals are written with the alphabet, and the shortest line it
// accepts (its literals and one piece for each capture) is within maxLen pieces. A filter no line of the grid can
// pass is not asked to keep one.
func satisfiable(patterns []string, alphabet []string, maxLen int) bool {
	for _, p := range patterns {
		pieces, ok := 0, true
		for _, n := range logql.LinePatternNodes(p) {
			if n.Capture {
				pieces++
				continue
			}
			fewest, written := fewestPieces(n.Text, alphabet)
			if !written {
				ok = false
				break
			}
			pieces += fewest
		}
		if ok && pieces <= maxLen {
			return true
		}
	}
	return false
}

// patternGrids are the recorded grids: the shapes of one literal and captures over every short line of a small
// alphabet; edge pairs (a literal found twice, a leading or trailing capture, newlines, quotes, html-like text,
// backslashes, the empty pattern); a seeded random grid of patterns of up to five nodes (with `or` alternatives)
// over lines built from the same pieces; a corpus of 638 filters on 2,530 lines; and the one-literal shapes that need no extract pipe against every short
// line over their own alphabet, alone and or-ed with a pattern that extracts.
func patternGrids() []patternGrid {
	var grids []patternGrid

	var exhaustive []patternCase
	for _, c := range []struct {
		patterns []string
		alphabet []string
		length   int
	}{
		{[]string{"<_>"}, []string{"a", "b", "\n"}, 4},
		{[]string{"foo<_>"}, []string{"f", "o", "x"}, 6},
		{[]string{"<_>foo"}, []string{"f", "o", "x"}, 6},
		{[]string{"foo"}, []string{"f", "o", "x"}, 6},
		{[]string{"<_>foo<_>"}, []string{"f", "o", "x"}, 6},
		{[]string{"ab<_>cd"}, []string{"a", "b", "c", "d", "x"}, 6},
		{[]string{"<_>ab<_>cd"}, []string{"a", "b", "c", "d", "x"}, 6},
		{[]string{"ab<_>cd<_>"}, []string{"a", "b", "c", "d", "x"}, 6},
		{[]string{"<_>ab<_>cd<_>"}, []string{"a", "b", "c", "d", "x"}, 6},
		{[]string{"a<_>b<_>c"}, []string{"a", "b", "c", "x"}, 6},
		{[]string{"<_>:<_>"}, []string{":", "x", " "}, 5},
		{[]string{"a<_>a"}, []string{"a", "x"}, 6},
		{[]string{""}, []string{"a"}, 2},
		// Alternatives of an or chain.
		{[]string{"foo<_>", "<_>bar"}, []string{"f", "o", "b", "a", "r", "x"}, 5},
		{[]string{"ab<_>cd", "<_>ef", "gh"}, []string{"a", "b", "c", "d", "e", "f", "g", "h"}, 4},
		{[]string{"", "<_>x"}, []string{"x", "y"}, 4},
	} {
		exhaustive = append(exhaustive, bothOps([][]string{c.patterns}, lines(c.alphabet, c.length), c.alphabet, c.length)...)
	}
	grids = append(grids, patternGrid{"exhaustive", exhaustive, true})

	var edges []patternCase
	for _, c := range [][2]string{
		{"<_> bar", "x bar y"}, {"foo", "foo bar"}, {"<_>a<_>", "a"}, {"foo <_>", "foo bar"},
		{"<_>a<_>", "aab"}, {"<_> bar", "x bar bar"}, {"<_> bar", "x bar"}, {"<_> bar <_>", " bar x bar y"},
		{"<_>x", "axbx"}, {"aa", "aaa"},
		{"foo", "xfoo"}, {"foo", "foofoo"}, {"foo <_>", "xfoo bar"}, {"foo <_>", "foo "},
		{"<_>", ""}, {"<_>", "x"}, {"<_> <_>", " a"}, {"<_> <_>", "a  b"}, {"<_> <_>", "a "}, {"<_>bar", "bar"},
		{"<_> end", "first\nsecond end"}, {"start <_>", "start\nx"}, {"start<_>", "start\nx"},
		{`<_> bar`, `"q" x bar`}, {`key=<_> next`, `key="a b" next`}, {`key=<_> next`, `key='a' next`},
		{"a < b<_>", "a < b c"}, {"<_>&amp;<_>", "x&amp;y"}, {"<_>&amp;<_>", "x&y"}, {"<_>&lt;", "x&lt;"},
		{"<_>&lt;", "x<"}, {"<a b> <_>", "<a b> c"}, {"<_>\"<_>", "a\"b"}, {"<_>`<_>", "a`b"},
		{`<_>\<_>`, `a\b`}, {"", ""}, {"", "x"},
	} {
		edges = append(edges, bothOps([][]string{{c[0]}}, []string{c[1]})...)
	}
	grids = append(grids, patternGrid{"edges", edges, false})

	literals := []string{"a", "b", " ", "ab", "ba", "aa", " a", "a ", "\n", `"`, "<", "&", "&lt;", "<b", "x<y", "\\", "'", "`", "&amp;", " GET ", "=", `"x"`}
	alphabet := []string{"a", "b", " ", "\n", `"`, "'", "<", "&", "x", "lt;", ">", "`", "amp;", "=", "GET"}
	rng := rand.New(rand.NewSource(20261012))
	genPattern := func() string {
		var b strings.Builder
		capture := rng.Intn(2) == 0
		for n := 1 + rng.Intn(5); n > 0; n-- {
			if capture {
				b.WriteString("<_>")
			} else {
				b.WriteString(literals[rng.Intn(len(literals))])
			}
			capture = !capture
		}
		return b.String()
	}
	var random []patternCase
	for i := 0; i < 400; i++ {
		patterns := []string{genPattern()}
		if rng.Intn(4) == 0 {
			patterns = append(patterns, genPattern())
		}
		var lines []string
		for j := 0; j < 25; j++ {
			var b strings.Builder
			for n := rng.Intn(10); n > 0; n-- {
				if rng.Intn(3) == 0 {
					b.WriteString(literals[rng.Intn(len(literals))])
				} else {
					b.WriteString(alphabet[rng.Intn(len(alphabet))])
				}
			}
			lines = append(lines, b.String())
		}
		random = append(random, bothOps([][]string{patterns}, lines)...)
	}
	grids = append(grids, patternGrid{"random", random, false})

	// The corpus: the 19 patterns Drilldown and the measured differences use (each under both operators) and 600
	// generated ones (under `|>` and `!>` in turn), against 2,500 generated lines and 30 edge lines: 638 filters on
	// 2,530 lines.
	fixed := []string{"<_> bar", "foo", "<_>a<_>", "foo <_>", "<_>x", "aa", "<_> <_>", "<_>bar", "<_> end", "start <_>",
		"key=<_> next", "a < b<_>", "<_>&amp;<_>", "<_>&lt;", "<a b> <_>", "<_>\"<_>", "<_>`<_>", "", "<_>"}
	rng = rand.New(rand.NewSource(7))
	type filter struct {
		op       string
		patterns []string
	}
	var filters []filter
	for _, p := range fixed {
		filters = append(filters, filter{"|>", []string{p}}, filter{"!>", []string{p}})
	}
	for i := 0; i < 600; i++ {
		ps := []string{genPattern()}
		if rng.Intn(4) == 0 {
			ps = append(ps, genPattern())
		}
		filters = append(filters, filter{[]string{"|>", "!>"}[i%2], ps})
	}
	var corpusLines []string
	for i := 0; i < 2500; i++ {
		var b strings.Builder
		for n := rng.Intn(10); n > 0; n-- {
			if rng.Intn(3) == 0 {
				b.WriteString(literals[rng.Intn(len(literals))])
			} else {
				b.WriteString(alphabet[rng.Intn(len(alphabet))])
			}
		}
		corpusLines = append(corpusLines, b.String())
	}
	corpusLines = append(corpusLines, "x bar y", "foo bar", "a", "aab", "x bar bar", " bar x bar y", "axbx", "aaa", "xfoo", "foofoo",
		"xfoo bar", "foo ", "x", " a", "a  b", "a ", "bar", "first\nsecond end", "start\nx", `"q" x bar`, `key="a b" next`,
		`key='a' next`, "a < b c", "x&amp;y", "x&y", "x&lt;", "x<", "<a b> c", "a\"b", "a`b")
	var corpus []patternCase
	for _, f := range filters {
		for _, l := range corpusLines {
			corpus = append(corpus, patternCase{op: f.op, patterns: f.patterns, line: l})
		}
	}
	grids = append(grids, patternGrid{"corpus", corpus, false})

	type set struct {
		alphabet []string
		maxLen   int
		literals []string
	}
	// Patterns that need the extract pipe, to be or-ed with the pipe-free ones.
	withExtract := []string{"<_>a<_>b", "a<_>a", "<_>a<_>a<_>"}
	var single []patternCase
	for _, s := range []set{
		{[]string{"a", "b", " "}, 7, []string{"a", "b", "aa", "ab", "ba", "aba", "aab", "bab", "aaa", "a ", " a", "a a", " "}},
		{[]string{".", "a", "*", "$", "^", "\\", " "}, 4, []string{".", "a.", ".*", "$", "a$", "^a", "\\", ".a"}},
		{[]string{"<", "&", "lt;", "a", "\n", " "}, 4, []string{"<", "&", "&lt;", "<a", "\n", "a\n"}},
	} {
		var filters [][]string
		for _, lit := range s.literals {
			for _, shape := range []string{"L", "<_>L", "<_>L<_>", "L<_>", "<_>L <_>"} {
				p := strings.ReplaceAll(shape, "L", lit)
				filters = append(filters, []string{p}, []string{p, "<_>" + lit}, []string{p, withExtract[len(filters)%len(withExtract)]})
			}
		}
		single = append(single, bothOps(filters, lines(s.alphabet, s.maxLen), s.alphabet, s.maxLen)...)
	}
	grids = append(grids, patternGrid{"single-literal", single, true})
	return grids
}

// patternVerdicts returns Loki's recorded verdict characters of each grid, by name.
func patternVerdicts(t *testing.T, grids []patternGrid) map[string]string {
	t.Helper()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordPatternVerdicts(t, grids)
	}
	f, err := os.Open(patternLokiAnswers)
	if err != nil {
		t.Fatalf("%v: record Loki's answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		name, verdicts, _ := strings.Cut(sc.Text(), " ")
		out[name] = verdicts
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	for _, g := range grids {
		if len(out[g.name]) != len(g.cases) {
			t.Fatalf("%s: grid %s has %d cases and %d recorded answers: record them again with LVP_RUN_ORACLES=1", patternLokiAnswers, g.name, len(g.cases), len(out[g.name]))
		}
	}
	return out
}

// recordPatternVerdicts asks Loki's pipeline about every case of the grids. A filter's verdict does not depend on
// the line's position, so each distinct (query, line) is asked once.
func recordPatternVerdicts(t *testing.T, grids []patternGrid) {
	t.Helper()
	type ask struct{ query, line string }
	stream := map[string]string{"app": "x"}
	index := map[ask]int{}
	var asks []oraclerun.ProcessCase
	for _, g := range grids {
		for _, c := range g.cases {
			a := ask{c.query(), c.line}
			if _, ok := index[a]; !ok {
				index[a] = len(asks)
				asks = append(asks, oraclerun.ProcessCase{Query: a.query, Labels: stream, Line: a.line})
			}
		}
	}
	answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "test", "oracle", "loki"), asks)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(patternLokiAnswers), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(patternLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	zw, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
	for _, g := range grids {
		var b strings.Builder
		for _, c := range g.cases {
			switch a := answers[index[ask{c.query(), c.line}]]; {
			case a.Error != "":
				b.WriteByte('E')
			case a.Keep:
				b.WriteByte('1')
			default:
				b.WriteByte('0')
			}
		}
		fmt.Fprintf(zw, "%s %s\n", g.name, b.String())
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// The pattern line filter keeps exactly the lines Loki's pattern filter keeps, for every alternative shape: a capture
// alone, one literal with a capture before it, after it and on both sides, several literals, a trailing literal and
// the empty pattern, `or` alternatives, the negated filter, and text the extract pipe and a regexp read specially
// (quotes, `<`, `&`, backslashes, newlines). The plan's filters and pipes are evaluated the way VictoriaLogs does
// (vlExtract and the filter evaluator in this package's tests) and compared with Loki's recorded verdicts; a filter
// Loki rejects is not compared (the validation test holds it).
//
// conformance: semantics/pattern-line-filter, semantics/pattern-line-filter-anchoring
func TestPatternLineFilterKeepsLokiLines(t *testing.T) {
	grids := patternGrids()
	verdicts := patternVerdicts(t, grids)
	for _, g := range grids {
		t.Run(g.name, func(t *testing.T) { holdPatternGrid(t, g, verdicts[g.name]) })
	}
}

// holdPatternGrid evaluates the plan's filters on every case of the grid and compares them with Loki's verdicts.
func holdPatternGrid(t *testing.T, g patternGrid, verdicts string) {
	t.Helper()
	checked, kept, rejected, mismatches := 0, 0, 0, 0
	var last string
	var (
		filters []logsql.FilterExpr
		pipes   []logsql.Pipe
	)
	type seen struct {
		kept, dropped int
		possible      bool
	}
	perFilter := map[string]*seen{}
	for i, c := range g.cases {
		v := verdicts[i]
		if v == 'E' {
			rejected++
			continue
		}
		if g.perFilter && c.op == "|>" {
			key := strings.Join(c.patterns, "\x00")
			if perFilter[key] == nil {
				perFilter[key] = &seen{possible: satisfiable(c.patterns, c.alphabet, c.maxLen)}
			}
			if v == '1' {
				perFilter[key].kept++
			} else {
				perFilter[key].dropped++
			}
		}
		if v == '1' {
			kept++
		}
		if key := c.op + "\x00" + strings.Join(c.patterns, "\x00"); key != last {
			last = key
			seq := 0
			filters, pipes = PatternLineFilter(c.patterns, c.op == "!>", &seq)
			checkPatternText(t, c, filters, pipes)
		}
		checked++
		if got := patternRowKept(t, filters, pipes, c.line); got != (v == '1') {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("%s %q on %q: kept %v, Loki %v\n filters %v\n pipes %v", c.op, c.patterns, c.line, got, v == '1', filters, pipes)
			}
		}
	}
	if mismatches > 0 {
		t.Errorf("grid %s: %d mismatches in %d verdicts", g.name, mismatches, checked)
	}
	if kept == 0 {
		t.Errorf("grid %s is vacuous: Loki keeps no line", g.name)
	}
	for key, n := range perFilter {
		if (n.kept == 0 && n.possible) || n.dropped == 0 {
			t.Errorf("grid %s: filter %q under |> keeps %d and drops %d lines: the grid cannot tell it from keeping or dropping every line",
				g.name, strings.Split(key, "\x00"), n.kept, n.dropped)
		}
	}
	t.Logf("grid %s: %d cases (%d filters), %d evaluated, Loki keeps %d, %d rejected by Loki, %d mismatches",
		g.name, len(g.cases), len(perFilter), checked, kept, rejected, mismatches)
}

// checkPatternText holds the nodes of a pattern line filter to the text the string translator writes for it.
func checkPatternText(t *testing.T, c patternCase, filters []logsql.FilterExpr, pipes []logsql.Pipe) {
	t.Helper()
	parts := PatternLineFilterParts(c.patterns, c.op == "!>", new(int))
	var text []string
	for _, f := range filters {
		text = append(text, f.String())
	}
	for _, p := range pipes {
		text = append(text, p.String())
	}
	if len(parts) > 0 && strings.Join(parts, " ") != strings.Join(text, " ") {
		t.Errorf("%s %q: text of the nodes differs:\n nodes %s\n parts %s", c.op, c.patterns, strings.Join(text, " "), strings.Join(parts, " "))
	}
}
