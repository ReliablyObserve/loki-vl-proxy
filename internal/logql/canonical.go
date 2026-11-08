package logql

import (
	"strings"
)

// CanonicalizeLogRanges rewrites the alternative forms Loki accepts for a log
// expression into the plain `{selector} pipeline [range]` form the translator
// handles: parentheses around a log query or a log range, and a pipeline or
// unwrap written after the range (`({a} | json)[5m]`, `rate({a}[5m] | json)`).
// Every other query is returned unchanged, without parsing it. It also writes
// the keywords in lower case (LowercaseKeywords).
func CanonicalizeLogRanges(query string) string {
	query = LowercaseKeywords(query)
	if !hasAlternativeLogRange(query) || InputSizeError(query) != "" {
		return query
	}
	p := &parser{sc: newScanner(query), canon: &canonState{}}
	p.advance()
	if _, err := p.parseExpr(); err != nil || p.cur.Typ != TokEOF || len(p.canon.rewrites) == 0 {
		return query
	}
	out := make([]byte, 0, len(query))
	last := 0
	for _, r := range p.canon.rewrites {
		out = append(out, query[last:r.start]...)
		out = append(out, r.text...)
		last = r.end
	}
	return string(append(out, query[last:]...))
}

// LowercaseKeywords writes in lower case the words Loki's lexer reads without
// regard to case (pkg/logql/syntax/lex.go lower-cases a word before it looks it
// up): SUM, TOPK(3, ...), RATE(...), BY, OFFSET, UNWRAP and the like. Names,
// strings and label names that are not keywords keep their spelling. The
// translator and the handlers match the lower-case spelling.
func LowercaseKeywords(query string) string {
	upper := false
	for i := 0; i < len(query) && !upper; i++ {
		upper = query[i] >= 'A' && query[i] <= 'Z'
	}
	if !upper {
		return query
	}
	var out []byte
	last := 0
	s := newScanner(query)
	for tok := s.next(); tok.Typ != TokEOF; tok = s.next() {
		if span := query[s.start:s.pos]; span != tok.Val && strings.EqualFold(span, tok.Val) {
			out = append(append(out, query[last:s.start]...), tok.Val...)
			last = s.pos
		}
	}
	if out == nil {
		return query
	}
	return string(append(out, query[last:]...))
}

// hasAlternativeLogRange is the cheap pre-check for CanonicalizeLogRanges. It
// looks, outside string literals, for a `(` that opens a log query (it is
// followed by `{` or `(` and does not follow an identifier or a number, which
// would make it a call) and for a range closed by `]` that a pipeline follows.
func hasAlternativeLogRange(q string) bool {
	var quote byte
	prev := byte(0) // last significant byte outside strings
	for i := 0; i < len(q); i++ {
		c := q[i]
		if quote != 0 {
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '`':
			quote = c
		case '(':
			if next := nextSignificant(q, i+1); !isWordByte(prev) && (next == '{' || next == '(') {
				return true
			}
		case ']':
			if afterRange(q, i+1) {
				return true
			}
		}
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			prev = c
		}
	}
	return false
}

func isWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// nextSignificant returns the first byte of q[i:] that is not white space.
func nextSignificant(q string, i int) byte {
	if i = skipSpace(q, i); i < len(q) {
		return q[i]
	}
	return 0
}

// afterRange reports whether a pipeline stage (`|`, `!=`, `!~`, `!>`) follows
// the range that ends just before q[i], past an optional offset.
func afterRange(q string, i int) bool {
	i = skipSpace(q, i)
	if strings.HasPrefix(q[i:], "offset") {
		i = skipSpace(q, i+len("offset"))
		for i < len(q) && (isWordByte(q[i]) || q[i] == '.') { // the duration
			i++
		}
		i = skipSpace(q, i)
	}
	return i < len(q) && (q[i] == '|' || q[i] == '!')
}

func skipSpace(q string, i int) int {
	for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
		i++
	}
	return i
}

// FirstArgument is the first argument of a call, given the text after its '(',
// and whether a comma follows it (a call with further arguments). Strings in
// double quotes or backticks and nested brackets are skipped.
func FirstArgument(s string) (string, bool) {
	n := FirstArgumentLen(s)
	return s[:n], n < len(s) && s[n] == ','
}

// FirstArgumentLen is the length of the first argument of a call, given the
// text after its '(': up to the first comma or unmatched ')' outside strings
// (double-quoted with escapes, or backtick-quoted) and nested brackets. It is
// len(s) when the call is not closed.
func FirstArgumentLen(s string) int {
	depth := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '(' || c == '[' || c == '{':
			depth++
		case c == ')' || c == ']' || c == '}':
			if depth == 0 {
				return i
			}
			depth--
		case c == ',' && depth == 0:
			return i
		}
	}
	return len(s)
}

// StripComments removes the `#` comments of a query (to the end of their line,
// outside strings), which Loki's lexer skips. The handlers and the string
// translator read the query text, so the request carries it without them. A
// query the scanner cannot read to its end is returned as it is: validation
// reports Loki's error for it.
func StripComments(query string) string {
	if !strings.Contains(query, "#") {
		return query
	}
	var spans [][2]int
	s := &scanner{src: query, comments: &spans}
	for {
		t := s.next()
		if t.Typ == TokEOF {
			break
		}
		if t.Typ == TokError {
			return query
		}
	}
	if len(spans) == 0 {
		return query
	}
	var b strings.Builder
	b.Grow(len(query))
	last := 0
	for _, sp := range spans {
		b.WriteString(query[last:sp[0]])
		last = sp[1]
	}
	b.WriteString(query[last:])
	return b.String()
}
