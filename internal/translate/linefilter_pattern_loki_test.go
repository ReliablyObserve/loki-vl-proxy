package translate

import (
	"html"
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// lokiPatternTest is Loki's pattern matcher verdict for one pattern alternative on one line, as the registry records it
// against Loki's pattern package (semantics/pattern-line-filter-anchoring: the filters matched Loki's real matcher on
// 1.6M line checks): each literal is searched once, at its first occurrence
// after the previous one (a leading literal may start anywhere in the line), every capture must be non-empty, and a
// pattern that ends on a literal must end the line there. The empty pattern accepts only the empty line.
func lokiPatternTest(pattern, line string) bool {
	if pattern == "" {
		return line == ""
	}
	nodes := logql.LinePatternNodes(pattern)
	pos := 0
	for i, n := range nodes {
		switch {
		case n.Capture:
			end := len(line)
			if i+1 < len(nodes) {
				idx := strings.Index(line[pos:], nodes[i+1].Text)
				if idx < 0 {
					return false
				}
				end = pos + idx
			}
			if end == pos {
				return false
			}
			pos = end
		case i == 0:
			idx := strings.Index(line, n.Text)
			if idx < 0 {
				return false
			}
			pos = idx + len(n.Text)
		default:
			if !strings.HasPrefix(line[pos:], n.Text) {
				return false
			}
			pos += len(n.Text)
		}
	}
	return !nodes[len(nodes)-1].Capture && pos == len(line) || nodes[len(nodes)-1].Capture
}

// vlExtract is VictoriaLogs' extract pipe over a line: every literal of the pattern is searched at its first
// occurrence after the previous one, a field takes the text up to the next literal (the rest of the line for the last
// one), and a literal that is not found leaves every field empty.
func vlExtract(t *testing.T, pattern, line string) map[string]string {
	t.Helper()
	fields := map[string]string{}
	type part struct {
		text, field string
	}
	var parts []part
	for rest := pattern; rest != ""; {
		lt := strings.IndexByte(rest, '<')
		if lt < 0 {
			parts = append(parts, part{text: html.UnescapeString(rest)})
			break
		}
		if lt > 0 {
			parts = append(parts, part{text: html.UnescapeString(rest[:lt])})
		}
		gt := strings.IndexByte(rest, '>')
		if gt < 0 {
			t.Fatalf("unterminated field in %q", pattern)
		}
		name := strings.TrimPrefix(rest[lt+1:gt], "plain:")
		parts = append(parts, part{field: name})
		fields[name] = ""
		rest = rest[gt+1:]
	}
	got := map[string]string{}
	pos := 0
	for i, p := range parts {
		if p.field == "" {
			if i == 0 {
				idx := strings.Index(line, p.text)
				if idx < 0 {
					return fields
				}
				pos = idx + len(p.text)
				continue
			}
			if !strings.HasPrefix(line[pos:], p.text) {
				return fields
			}
			pos += len(p.text)
			continue
		}
		end := len(line)
		if i+1 < len(parts) {
			idx := strings.Index(line[pos:], parts[i+1].text)
			if idx < 0 {
				return fields
			}
			end = pos + idx
		}
		got[p.field] = line[pos:end]
		pos = end
	}
	for k, v := range got {
		fields[k] = v
	}
	return fields
}

var patternRegexps = map[string]*regexp.Regexp{}

func patternRegexp(pattern string) *regexp.Regexp {
	re := patternRegexps[pattern]
	if re == nil {
		re = regexp.MustCompile(pattern)
		patternRegexps[pattern] = re
	}
	return re
}

// evalLineFilter evaluates the filter nodes the pattern line filter writes over the fields of one row.
func evalLineFilter(t *testing.T, f logsql.FilterExpr, fields map[string]string) bool {
	t.Helper()
	switch n := f.(type) {
	case logsql.Regexp:
		return patternRegexp(n.Pattern).MatchString(fields["_msg"])
	case logsql.FieldFilter:
		v := fields[n.Field]
		switch n.Op {
		case logsql.FieldOpRegexp:
			return patternRegexp(n.Value).MatchString(v)
		case logsql.FieldOpAny:
			return v != ""
		case logsql.FieldOpEmpty:
			return v == ""
		}
	case logsql.NotExpr:
		return !evalLineFilter(t, n.Expr, fields)
	case logsql.Group:
		return evalLineFilter(t, n.Expr, fields)
	case logsql.AllOf:
		for _, e := range n {
			if !evalLineFilter(t, e, fields) {
				return false
			}
		}
		return true
	case logsql.AnyOf:
		for _, e := range n {
			if evalLineFilter(t, e, fields) {
				return true
			}
		}
		return false
	case logsql.AnyOfLower:
		for _, e := range n {
			if evalLineFilter(t, e, fields) {
				return true
			}
		}
		return false
	}
	t.Fatalf("unexpected filter node %T (%s)", f, f)
	return false
}

// patternRowKept runs the filters and pipes of a pattern line filter over a line and reports whether the row stays.
func patternRowKept(t *testing.T, filters []logsql.FilterExpr, pipes []logsql.Pipe, line string) bool {
	t.Helper()
	fields := map[string]string{"_msg": line}
	for _, f := range filters {
		if !evalLineFilter(t, f, fields) {
			return false
		}
	}
	for _, p := range pipes {
		switch n := p.(type) {
		case logsql.PipeExtract:
			for k, v := range vlExtract(t, n.Pattern, line) {
				fields[k] = v
			}
		case logsql.PipeFilter:
			if !evalLineFilter(t, n.Expr, fields) {
				return false
			}
		case logsql.PipeDelete:
		default:
			t.Fatalf("unexpected pipe %T", p)
		}
	}
	return true
}

// lines returns every string of at most n characters over the alphabet.
func lines(alphabet []string, n int) []string {
	out := []string{""}
	frontier := []string{""}
	for i := 0; i < n; i++ {
		var next []string
		for _, s := range frontier {
			for _, c := range alphabet {
				next = append(next, s+c)
			}
		}
		out = append(out, next...)
		frontier = next
	}
	return out
}

// The pattern line filter keeps exactly the lines Loki's pattern matcher accepts, for every alternative shape: a
// capture alone, one literal with a capture before it, after it and on both sides, several literals, a trailing
// literal and the empty pattern, with `or` alternatives and for the negated filter.
// conformance: semantics/pattern-line-filter, semantics/pattern-line-filter-anchoring
func TestPatternLineFilterKeepsLokiLines(t *testing.T) {
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
		{[]string{"a<b>"}, []string{"a", "b", "<", ">"}, 5},
		{[]string{""}, []string{"a"}, 2},
		// Alternatives of an or chain.
		{[]string{"foo<_>", "<_>bar"}, []string{"f", "o", "b", "a", "r", "x"}, 5},
		{[]string{"ab<_>cd", "<_>ef", "gh"}, []string{"a", "b", "c", "d", "e", "f", "g", "h"}, 4},
		{[]string{"", "<_>x"}, []string{"x", "y"}, 4},
	} {
		for _, negative := range []bool{false, true} {
			seq := 0
			filters, pipes := PatternLineFilter(c.patterns, negative, &seq)
			parts := PatternLineFilterParts(c.patterns, negative, new(int))
			// The nodes write the text of the string translator.
			var text []string
			for _, f := range filters {
				text = append(text, f.String())
			}
			for _, p := range pipes {
				text = append(text, p.String())
			}
			if len(parts) > 0 && strings.Join(parts, " ") != strings.Join(text, " ") {
				t.Errorf("%q negative=%v: text of the nodes differs:\n nodes %s\n parts %s", c.patterns, negative, strings.Join(text, " "), strings.Join(parts, " "))
			}
			for _, line := range lines(c.alphabet, c.length) {
				want := false
				for _, p := range c.patterns {
					want = want || lokiPatternTest(p, line)
				}
				if negative {
					want = !want
				}
				if got := patternRowKept(t, filters, pipes, line); got != want {
					t.Fatalf("%q negative=%v line %q: kept %v, Loki %v\n filters %v\n pipes %v", c.patterns, negative, line, got, want, filters, pipes)
				}
			}
		}
	}
}

// The extract pipe of the filter numbers its scratch fields per alternative that extracts, across the query.
// conformance: semantics/pattern-line-filter
func TestPatternLineFilterScratchNumbering(t *testing.T) {
	seq := 0
	_, pipes := PatternLineFilter([]string{`a<_>b<_>c`}, false, &seq)
	if seq != 1 || !strings.Contains(pipes[0].String(), "__lvp_pf0_1") {
		t.Errorf("first filter: seq %d, pipes %v", seq, pipes)
	}
	_, pipes = PatternLineFilter([]string{`x<_>y<_>z`}, false, &seq)
	if seq != 2 || !strings.Contains(pipes[0].String(), "__lvp_pf1_1") || strings.Contains(pipes[0].String(), "__lvp_pf0") {
		t.Errorf("second filter: seq %d, pipes %v", seq, pipes)
	}
	// An exact pattern needs no scratch field.
	if _, pipes := PatternLineFilter([]string{"<_>foo"}, false, &seq); seq != 2 || pipes != nil {
		t.Errorf("exact pattern: seq %d, pipes %v", seq, pipes)
	}
	// The literals of a quoted pattern are read as text: extract reads '<' as the start of a field.
	_, pipes = PatternLineFilter([]string{`a<_>&b<_>c`}, false, &seq)
	if !strings.Contains(pipes[0].String(), "&amp;b") {
		t.Errorf("literal text must be escaped for extract: %v", pipes)
	}
}
