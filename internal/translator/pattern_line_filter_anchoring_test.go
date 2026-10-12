package translator

import (
	"html"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// This file pins the pattern line filters (|> and !>) to Loki's verdict per
// line. The reference is a port of Loki v3.7.7 pkg/logql/log/pattern:
// the lexer (lexer.rl: <(alpha|_)(alnum|_)*> is a capture, every other rune a
// literal, adjacent literals merged by expr.y) and Matcher.Test
// (pattern.go:124-165): each literal is searched once, at its first occurrence
// after the previous one; a capture must be non-empty, and a pattern ending on
// a literal must consume the line to its end.
//
// The translated LogsQL is evaluated the way VictoriaLogs does, by a small
// evaluator of the filters and pipes the translation emits: regexp filters on
// _msg, the extract pipe (a port of VictoriaLogs lib/logstorage/pattern.go
// parsePatternSteps and apply, which search each text at its first occurrence
// like Loki), `field:*` (non-empty), `field:""` (empty or missing), NOT, or and
// implicit and. The regexps are compiled without DotNL, so the translation must
// carry its own (?s).

type lokiPatternNode struct {
	lit     string
	capture bool
}

// lokiPatternNodes is the Loki pattern lexer and parser (lexer.rl, expr.y).
func lokiPatternNodes(p string) []lokiPatternNode {
	var nodes []lokiPatternNode
	isAlpha := func(c byte) bool { return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
	for i := 0; i < len(p); {
		if p[i] == '<' && i+1 < len(p) && isAlpha(p[i+1]) {
			j := i + 2
			for j < len(p) && (isAlpha(p[j]) || (p[j] >= '0' && p[j] <= '9')) {
				j++
			}
			if j < len(p) && p[j] == '>' {
				nodes = append(nodes, lokiPatternNode{lit: p[i+1 : j], capture: true})
				i = j + 1
				continue
			}
		}
		if n := len(nodes); n > 0 && !nodes[n-1].capture {
			nodes[n-1].lit += p[i : i+1]
		} else {
			nodes = append(nodes, lokiPatternNode{lit: p[i : i+1]})
		}
		i++
	}
	return nodes
}

// lokiPatternTest is pattern.Matcher.Test (pkg/logql/log/pattern/pattern.go:124-165).
func lokiPatternTest(pattern, line string) bool {
	e := lokiPatternNodes(pattern)
	if len(line) == 0 || len(e) == 0 {
		return len(line) == 0 && len(e) == 0
	}
	off := 0
	for i, n := range e {
		if n.capture {
			continue
		}
		j := strings.Index(line[off:], n.lit)
		if j == -1 {
			return false
		}
		if i != 0 && j == 0 {
			return false
		}
		off += j + len(n.lit)
	}
	reqRem := e[len(e)-1].capture
	hasRem := off != len(line)
	return reqRem == hasRem
}

// vlExtract is VictoriaLogs' extract pipe on one value (lib/logstorage/pattern.go
// parsePatternSteps and apply, v1.43-v1.52), for `<plain:name>` and `<_>` fields.
func vlExtract(t *testing.T, ptn, s string) map[string]string {
	t.Helper()
	type step struct{ prefix, field, opt string }
	var steps []step
	n := strings.IndexByte(ptn, '<')
	if n < 0 {
		steps = append(steps, step{prefix: ptn})
	} else {
		prefix, rest := ptn[:n], ptn[n+1:]
		for {
			m := strings.IndexByte(rest, '>')
			if m < 0 {
				t.Fatalf("extract pattern %q: missing '>'", ptn)
			}
			field := rest[:m]
			rest = rest[m+1:]
			if field == "_" || field == "*" {
				field = ""
			}
			steps = append(steps, step{prefix: prefix, field: field})
			if rest == "" {
				break
			}
			m = strings.IndexByte(rest, '<')
			if m < 0 {
				steps = append(steps, step{prefix: rest})
				break
			}
			prefix, rest = rest[:m], rest[m+1:]
		}
	}
	for i := range steps {
		steps[i].prefix = html.UnescapeString(steps[i].prefix)
		if k := strings.IndexByte(steps[i].field, ':'); k >= 0 {
			steps[i].opt = strings.TrimSpace(steps[i].field[:k])
			steps[i].field = steps[i].field[k+1:]
		}
		steps[i].field = strings.TrimSpace(steps[i].field)
		if i > 0 && steps[i].prefix == "" {
			t.Fatalf("extract pattern %q: missing delimiter between fields", ptn)
		}
		if steps[i].field != "" && steps[i].opt != "plain" {
			t.Fatalf("extract pattern %q: field %q unquotes quoted values, Loki does not", ptn, steps[i].field)
		}
	}
	matches := make([]string, len(steps))
	out := func() map[string]string {
		m := map[string]string{}
		for i, st := range steps {
			if st.field != "" {
				m[st.field] = matches[i]
			}
		}
		return m
	}
	idx := func(s, prefix string) int {
		if prefix == "" {
			return 0
		}
		return strings.Index(s, prefix)
	}
	k := idx(s, steps[0].prefix)
	if k < 0 {
		return out()
	}
	s = s[k+len(steps[0].prefix):]
	for i := range steps {
		next := ""
		if i+1 < len(steps) {
			next = steps[i+1].prefix
		}
		if next == "" {
			matches[i] = s
			return out()
		}
		k := idx(s, next)
		if k < 0 {
			return out()
		}
		matches[i] = s[:k]
		s = s[k+len(next):]
	}
	return out()
}

// logsqlRow evaluates the translated query on one line.
type logsqlRow struct {
	t      *testing.T
	fields map[string]string
}

func splitTopLevel(t *testing.T, s, sep string) []string {
	t.Helper()
	var out []string
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\\' && inQuote:
			i++
		case s[i] == '"':
			inQuote = !inQuote
		case !inQuote && strings.HasPrefix(s[i:], sep):
			out = append(out, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	return append(out, s[start:])
}

// evalLogsQLLine reports whether VictoriaLogs keeps the line under query, and
// the fields the query leaves on the row.
func evalLogsQLLine(t *testing.T, query, line string) (bool, map[string]string) {
	t.Helper()
	row := &logsqlRow{t: t, fields: map[string]string{"_msg": line}}
	for i, stage := range splitTopLevel(t, query, " | ") {
		stage = strings.TrimSpace(stage)
		switch {
		case i == 0 || strings.HasPrefix(stage, "filter "):
			expr := strings.TrimPrefix(stage, "filter ")
			p := &filterParser{t: t, s: expr}
			ok := p.parseOr(row)
			if p.pos != len(p.s) {
				t.Fatalf("filter %q: unparsed tail %q", expr, p.s[p.pos:])
			}
			if !ok {
				return false, row.fields
			}
		case strings.HasPrefix(stage, "extract "):
			rest := strings.TrimPrefix(stage, "extract ")
			q, err := strconv.QuotedPrefix(rest)
			if err != nil {
				t.Fatalf("extract stage %q: %v", stage, err)
			}
			if tail := strings.TrimSpace(rest[len(q):]); tail != "from _msg" {
				t.Fatalf("extract stage %q: want `from _msg`, got %q", stage, tail)
			}
			ptn, _ := strconv.Unquote(q)
			for k, v := range vlExtract(t, ptn, row.fields["_msg"]) {
				row.fields[k] = v
			}
		case strings.HasPrefix(stage, "delete "):
			for _, f := range strings.Split(strings.TrimPrefix(stage, "delete "), ",") {
				f = strings.TrimSpace(f)
				for k := range row.fields {
					if k == f || (strings.HasSuffix(f, "*") && strings.HasPrefix(k, strings.TrimSuffix(f, "*"))) {
						delete(row.fields, k)
					}
				}
			}
		default:
			t.Fatalf("unexpected stage %q in %q", stage, query)
		}
	}
	return true, row.fields
}

var regexpCache sync.Map

// compiledRegexp compiles each distinct regexp of a translation once: the
// corpus test evaluates the same few filters on thousands of lines.
func compiledRegexp(re string) *regexp.Regexp {
	if c, ok := regexpCache.Load(re); ok {
		return c.(*regexp.Regexp)
	}
	c := regexp.MustCompile(re)
	regexpCache.Store(re, c)
	return c
}

type filterParser struct {
	t   *testing.T
	s   string
	pos int
}

func (p *filterParser) skip() {
	for p.pos < len(p.s) && p.s[p.pos] == ' ' {
		p.pos++
	}
}

func (p *filterParser) peekWord(w string) bool {
	p.skip()
	return strings.HasPrefix(p.s[p.pos:], w+" ") || strings.HasPrefix(p.s[p.pos:], w+"(")
}

func (p *filterParser) parseOr(row *logsqlRow) bool {
	v := p.parseAnd(row)
	for p.peekWord("or") {
		p.pos += 2
		w := p.parseAnd(row)
		v = v || w
	}
	return v
}

func (p *filterParser) parseAnd(row *logsqlRow) bool {
	v := p.parseUnary(row)
	for {
		p.skip()
		if p.pos >= len(p.s) || p.s[p.pos] == ')' || p.peekWord("or") {
			return v
		}
		if p.peekWord("and") {
			p.pos += 3
		}
		w := p.parseUnary(row)
		v = v && w
	}
}

func (p *filterParser) parseUnary(row *logsqlRow) bool {
	p.skip()
	if p.peekWord("NOT") {
		p.pos += 3
		return !p.parseUnary(row)
	}
	if p.s[p.pos] == '(' {
		p.pos++
		v := p.parseOr(row)
		p.skip()
		if p.pos >= len(p.s) || p.s[p.pos] != ')' {
			p.t.Fatalf("filter %q: missing ')'", p.s)
		}
		p.pos++
		return v
	}
	field := "_msg"
	if p.s[p.pos] != '~' {
		k := strings.IndexByte(p.s[p.pos:], ':')
		if k < 0 {
			p.t.Fatalf("filter %q: cannot parse at %q", p.s, p.s[p.pos:])
		}
		field = p.s[p.pos : p.pos+k]
		p.pos += k + 1
	}
	rest := p.s[p.pos:]
	switch {
	case strings.HasPrefix(rest, "*"):
		p.pos++
		return row.fields[field] != ""
	case strings.HasPrefix(rest, `""`):
		p.pos += 2
		return row.fields[field] == ""
	case strings.HasPrefix(rest, "~"):
		q, err := strconv.QuotedPrefix(rest[1:])
		if err != nil {
			p.t.Fatalf("filter %q: %v", p.s, err)
		}
		p.pos += 1 + len(q)
		re, _ := strconv.Unquote(q)
		return compiledRegexp(re).MatchString(row.fields[field])
	}
	p.t.Fatalf("filter %q: unsupported filter at %q", p.s, rest)
	return false
}

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

// keeps evaluates the translation on line the way VictoriaLogs does.
func (tp translatedPattern) keeps(t *testing.T, line string) bool {
	t.Helper()
	keep, fields := evalLogsQLLine(t, tp.query, line)
	for k := range fields {
		if keep && k != "_msg" {
			t.Fatalf("%s leaves the scratch field %q on the row: %s", tp.logql, k, tp.logsql)
		}
	}
	return keep
}

// translatedPatternVerdict translates `{app="x"} <op> <operands>` and evaluates
// it on line.
func translatedPatternVerdict(t *testing.T, op string, patterns []string, line string) (bool, string) {
	t.Helper()
	tp := translatePattern(t, op, patterns)
	return tp.keeps(t, line), tp.logsql
}

func lokiPatternVerdict(op string, patterns []string, line string) bool {
	matched := false
	for _, p := range patterns {
		matched = matched || lokiPatternTest(p, line)
	}
	if op == "!>" {
		return !matched
	}
	return matched
}

// conformance: semantics/pattern-line-filter-anchoring
func TestPatternLineFilterAnchoringEdges(t *testing.T) {
	cases := []struct {
		pattern, line string
		loki          bool
	}{
		// The measured differences.
		{"<_> bar", "x bar y", false},
		{"foo", "foo bar", false},
		{"<_>a<_>", "a", false},
		{"foo <_>", "foo bar", true},
		// A literal is searched once, at its first occurrence.
		{"<_>a<_>", "aab", false},
		{"<_> bar", "x bar bar", false},
		{"<_> bar", "x bar", true},
		{"<_> bar <_>", " bar x bar y", false},
		{"<_>x", "axbx", false},
		{"aa", "aaa", false},
		// A leading literal may start anywhere in the line.
		{"foo", "xfoo", true},
		{"foo", "foofoo", false},
		{"foo <_>", "xfoo bar", true},
		{"foo <_>", "foo ", false},
		// Captures must be non-empty, including a leading and a trailing one.
		{"<_>", "", false},
		{"<_>", "x", true},
		{"<_> <_>", " a", false},
		{"<_> <_>", "a  b", true},
		{"<_> <_>", "a ", false},
		{"<_>bar", "bar", false},
		// Multi-line lines: a capture spans newlines.
		{"<_> end", "first\nsecond end", true},
		{"start <_>", "start\nx", false},
		{"start<_>", "start\nx", true},
		// A quoted capture is plain text, never unquoted.
		{`<_> bar`, `"q" x bar`, true},
		{`key=<_> next`, `key="a b" next`, true},
		{`key=<_> next`, `key='a' next`, true},
		// HTML-like text is literal.
		{"a < b<_>", "a < b c", true},
		{"<_>&amp;<_>", "x&amp;y", true},
		{"<_>&amp;<_>", "x&y", false},
		{"<_>&lt;", "x&lt;", true},
		{"<_>&lt;", "x<", false},
		{"<a b> <_>", "<a b> c", true},
		{"<_>\"<_>", "a\"b", true},
		{"<_>`<_>", "a`b", true},
		{`<_>\<_>`, `a\b`, true},
		// Empty pattern.
		{"", "", true},
		{"", "x", false},
	}
	for _, c := range cases {
		if got := lokiPatternTest(c.pattern, c.line); got != c.loki {
			t.Fatalf("reference port: Test(%q, %q) = %v, Loki %v", c.pattern, c.line, got, c.loki)
		}
		for _, op := range []string{"|>", "!>"} {
			want := lokiPatternVerdict(op, []string{c.pattern}, c.line)
			got, query := translatedPatternVerdict(t, op, []string{c.pattern}, c.line)
			if got != want {
				t.Errorf("%s %q on %q: proxy keeps=%v, Loki keeps=%v\n  LogsQL: %s", op, c.pattern, c.line, got, want, query)
			}
		}
	}
}

// conformance: semantics/pattern-line-filter-anchoring
func TestPatternLineFilterAnchoringDifferential(t *testing.T) {
	literals := []string{"a", "b", " ", "ab", "ba", "aa", " a", "a ", "\n", `"`, "<", "&", "&lt;", "<b", "x<y", "\\"}
	alphabet := []string{"a", "b", " ", "\n", `"`, "'", "<", "&", "x", "lt;", ">"}
	rng := rand.New(rand.NewSource(20261012))
	genPattern := func() string {
		var b strings.Builder
		nodes := 1 + rng.Intn(5)
		capture := rng.Intn(2) == 0
		for i := 0; i < nodes; i++ {
			if capture {
				b.WriteString("<_>")
			} else {
				b.WriteString(literals[rng.Intn(len(literals))])
			}
			capture = !capture
		}
		return b.String()
	}
	genLine := func(parts []string) string {
		var b strings.Builder
		for n := rng.Intn(9); n > 0; n-- {
			if len(parts) > 0 && rng.Intn(3) == 0 {
				b.WriteString(parts[rng.Intn(len(parts))])
			} else {
				b.WriteString(alphabet[rng.Intn(len(alphabet))])
			}
		}
		return b.String()
	}
	iterations := 3000
	if testing.Short() {
		iterations = 800
	}
	mismatches := 0
	for i := 0; i < iterations; i++ {
		patterns := []string{genPattern()}
		if rng.Intn(4) == 0 {
			patterns = append(patterns, genPattern())
		}
		var lits []string
		for _, p := range patterns {
			for _, n := range lokiPatternNodes(p) {
				if !n.capture {
					lits = append(lits, n.lit)
				}
			}
		}
		for j := 0; j < 12; j++ {
			line := genLine(lits)
			for _, op := range []string{"|>", "!>"} {
				want := lokiPatternVerdict(op, patterns, line)
				got, query := translatedPatternVerdict(t, op, patterns, line)
				if got != want {
					mismatches++
					if mismatches <= 20 {
						t.Errorf("%s %q on %q: proxy keeps=%v, Loki keeps=%v\n  LogsQL: %s", op, patterns, line, got, want, query)
					}
				}
			}
		}
	}
	if mismatches > 0 {
		t.Errorf("%d mismatches against Loki's pattern.Matcher.Test", mismatches)
	}
}

// TestPatternLineFilterAnchoringCorpus is the committed form of the proof
// that ran Loki's own pattern package against real VictoriaLogs storage: the
// same seed (7), the same generator and the same fixed patterns and lines, so
// the 638 filters (19 fixed and 600 generated, each of |> and !> for the fixed
// ones, alternating for the generated ones) are checked on 2,530 lines, about
// 1.6M verdicts. The reference is the port of Loki's Matcher.Test above and the
// translation is evaluated with the port of the VictoriaLogs extract pipe, so
// the test needs no storage; the storage run is described in the pull request.
//
// conformance: semantics/pattern-line-filter-anchoring
func TestPatternLineFilterAnchoringCorpus(t *testing.T) {
	literals := []string{"a", "b", " ", "ab", "ba", "aa", " a", "a ", "\n", `"`, "<", "&", "&lt;", "<b", "x<y", "\\", "'", "`", "&amp;", " GET ", "=", `"x"`}
	alphabet := []string{"a", "b", " ", "\n", `"`, "'", "<", "&", "x", "lt;", ">", "`", "amp;", "=", "GET"}
	rng := rand.New(rand.NewSource(7))
	genPattern := func() string {
		var b strings.Builder
		nodes := 1 + rng.Intn(5)
		capture := rng.Intn(2) == 0
		for i := 0; i < nodes; i++ {
			if capture {
				b.WriteString("<_>")
			} else {
				b.WriteString(literals[rng.Intn(len(literals))])
			}
			capture = !capture
		}
		return b.String()
	}
	type filter struct {
		op       string
		patterns []string
	}
	var filters []filter
	fixed := []string{"<_> bar", "foo", "<_>a<_>", "foo <_>", "<_>x", "aa", "<_> <_>", "<_>bar", "<_> end", "start <_>", "key=<_> next", "a < b<_>", "<_>&amp;<_>", "<_>&lt;", "<a b> <_>", "<_>\"<_>", "<_>`<_>", "", "<_>"}
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
	var lines []string
	for i := 0; i < 2500; i++ {
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
	lines = append(lines, "x bar y", "foo bar", "a", "aab", "x bar bar", " bar x bar y", "axbx", "aaa", "xfoo", "foofoo", "xfoo bar", "foo ", "x", " a", "a  b", "a ", "bar", "first\nsecond end", "start\nx", `"q" x bar`, `key="a b" next`, `key='a' next`, "a < b c", "x&amp;y", "x&y", "x&lt;", "x<", "<a b> c", "a\"b", "a`b")
	if len(filters) != 638 || len(lines) != 2530 {
		t.Fatalf("corpus drifted from the committed proof: %d filters, %d lines (want 638, 2530)", len(filters), len(lines))
	}
	checks, mismatches, keptBy := 0, 0, 0
	for _, f := range filters {
		tp := translatePattern(t, f.op, f.patterns)
		for _, line := range lines {
			checks++
			want := lokiPatternVerdict(f.op, f.patterns, line)
			if want {
				keptBy++
			}
			if got := tp.keeps(t, line); got != want {
				mismatches++
				if mismatches <= 20 {
					t.Errorf("%s %q on %q: proxy keeps=%v, Loki keeps=%v\n  LogsQL: %s", f.op, f.patterns, line, got, want, tp.logsql)
				}
			}
		}
	}
	if keptBy == 0 || keptBy == checks {
		t.Errorf("the corpus is vacuous: Loki keeps %d of %d verdicts", keptBy, checks)
	}
	if mismatches > 0 {
		t.Errorf("%d mismatches in %d verdicts against Loki's pattern.Matcher.Test", mismatches, checks)
	}
	t.Logf("%d filters x %d lines = %d verdicts, Loki keeps %d, mismatches %d", len(filters), len(lines), checks, keptBy, mismatches)
}

// TestPatternLineFilterSingleLiteralShapes checks the pipe-free translation of
// the patterns with one literal (`L`, `<_>L`, `<_>L<_>`; `L<_>` and `<_>` are
// plain regexps) against the port of Matcher.Test on every line over a small
// alphabet, so each overlap of the literal with itself and with its
// neighbours occurs: `aa` in `aaa`, `aba` in `ababa`, a literal at the start
// or the end of the line, and two or more occurrences. Alternatives that mix
// these shapes with the ones that need extract are checked the same way.
//
// conformance: semantics/pattern-line-filter-anchoring
func TestPatternLineFilterSingleLiteralShapes(t *testing.T) {
	type set struct {
		alphabet []string
		maxLen   int
		literals []string
	}
	sets := []set{
		{[]string{"a", "b", " "}, 7, []string{"a", "b", "aa", "ab", "ba", "aba", "aab", "bab", "aaa", "a ", " a", "a a", " "}},
		{[]string{".", "a", "*", "$", "^", "\\"}, 4, []string{".", "a.", ".*", "$", "a$", "^a", "\\", ".a"}},
		{[]string{"<", "&", "lt;", "a", "\n"}, 4, []string{"<", "&", "&lt;", "<a", "\n", "a\n"}},
	}
	// Patterns that need the extract pipe, to be or-ed with the pipe-free ones.
	withExtract := []string{"<_>a<_>b", "a<_>a", "<_>a<_>a<_>"}
	checked, kept := 0, 0
	for _, s := range sets {
		var lines []string
		var gen func(prefix string, n int)
		gen = func(prefix string, n int) {
			lines = append(lines, prefix)
			if n == 0 {
				return
			}
			for _, c := range s.alphabet {
				gen(prefix+c, n-1)
			}
		}
		gen("", s.maxLen)
		var filters [][]string
		for _, lit := range s.literals {
			for _, shape := range []string{"L", "<_>L", "<_>L<_>", "L<_>", "<_>L <_>"} {
				p := strings.ReplaceAll(shape, "L", lit)
				filters = append(filters, []string{p})
				filters = append(filters, []string{p, "<_>" + lit}, []string{p, withExtract[len(filters)%len(withExtract)]})
			}
		}
		for _, f := range filters {
			for _, op := range []string{"|>", "!>"} {
				tp := translatePattern(t, op, f)
				for _, line := range lines {
					want := lokiPatternVerdict(op, f, line)
					if want {
						kept++
					}
					checked++
					if got := tp.keeps(t, line); got != want {
						t.Fatalf("%s %q on %q: proxy keeps=%v, Loki keeps=%v\n  LogsQL: %s", op, f, line, got, want, tp.logsql)
					}
				}
			}
		}
	}
	if kept == 0 || kept == checked {
		t.Fatalf("vacuous: Loki keeps %d of %d verdicts", kept, checked)
	}
	t.Logf("%d verdicts, Loki keeps %d, 0 mismatches", checked, kept)
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
