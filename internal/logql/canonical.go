package logql

import "strings"

// CanonicalizeLogRanges rewrites the alternative forms Loki accepts for a log
// expression into the plain `{selector} pipeline [range]` form the translator
// handles: parentheses around a log query or a log range, and a pipeline or
// unwrap written after the range (`({a} | json)[5m]`, `rate({a}[5m] | json)`).
// Every other query is returned unchanged, without parsing it.
func CanonicalizeLogRanges(query string) string {
	if !hasAlternativeLogRange(query) {
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

// firstArgumentLen is the length of the first argument of a call, given the
// text after its '(': up to the first comma or unmatched ')' outside strings.
func firstArgumentLen(s string) int {
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
