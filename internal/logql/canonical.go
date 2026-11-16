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
// up): SUM, TOPK(3, ...), RATE(...), BY, OFFSET, UNWRAP and the like, and the
// two letters outside ASCII that lower case into an ASCII one, the Kelvin sign
// U+212A (topK, read as topk) and U+0130 (avg_over_tİme, read as
// avg_over_time). Names, strings and label names that are not keywords keep
// their spelling. The translator and the handlers match the lower-case
// spelling.
func LowercaseKeywords(query string) string {
	if !foldsToKeyword(query) {
		return query
	}
	var out []byte
	last := 0
	s := newScanner(query)
	for tok := s.next(); tok.Typ != TokEOF; tok = s.next() {
		// The scanner's own rule (strings.ToLower, read without allocating by lowerPrefixLen): a letter that only
		// case-folds to an ASCII one (U+017F, ſ) is not the keyword's to Loki's lexer either. A span that is not valid
		// UTF-8 keeps its bytes (strings.ToLower would write U+FFFD for them), and so does a word right after a digit, a letter, an underscore or a dot: in
		// lower case it could join the token before (`0KEEP` would read as the bytes literal 0k), and a word inside a
		// range, which Loki's lexer reads as the range's text (its error quotes it as written).
		if span := query[s.start:s.pos]; span != tok.Val && !s.inRange && lowerPrefixLen(span, tok.Val) == len(span) &&
			(s.start == 0 || !isWordByte(query[s.start-1]) && query[s.start-1] != '.') {
			out = append(append(out, query[last:s.start]...), tok.Val...)
			last = s.pos
		}
	}
	if out == nil {
		return query
	}
	return string(append(out, query[last:]...))
}

// foldsToKeyword reports a query with a letter the scanner lower cases into an ASCII keyword letter: an ASCII
// capital, the Kelvin sign (U+212A, UTF-8 E2 84 AA) or U+0130 (C4 B0). A query with none of them has every keyword
// in lower case already. Every request asks it, so it reads eight bytes at a time: a word of ASCII bytes with no
// capital is skipped whole, and only a word with a capital or a byte outside ASCII is read byte by byte.
func foldsToKeyword(query string) bool {
	const ones, highs = 0x0101010101010101, 0x8080808080808080
	i := 0
	for ; i+8 <= len(query); i += 8 {
		w := uint64(query[i]) | uint64(query[i+1])<<8 | uint64(query[i+2])<<16 | uint64(query[i+3])<<24 |
			uint64(query[i+4])<<32 | uint64(query[i+5])<<40 | uint64(query[i+6])<<48 | uint64(query[i+7])<<56
		// A byte above 0x40 and below 0x5B ('A' to 'Z') in a word of ASCII bytes sets its high bit here.
		capital := (ones*(127+'Z'+1) - w&(ones*127)) & ^w & (w&(ones*127) + ones*(127-('A'-1))) & highs
		if w&highs != 0 || capital != 0 {
			break
		}
	}
	for ; i < len(query); i++ {
		switch c := query[i]; {
		case c >= 'A' && c <= 'Z':
			return true
		case c == 0xE2 && strings.HasPrefix(query[i:], "\u212A"), c == 0xC4 && strings.HasPrefix(query[i:], "\u0130"):
			return true
		}
	}
	return false
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
// reports Loki's error for it. So is a query with a comment between a
// function's name and its parenthesis, or a vector aggregation's name and its
// grouping: Loki's lexer reads the name as an identifier there, which removing
// the comment would hide.
func StripComments(query string) string {
	if !strings.Contains(query, "#") || strings.IndexByte(query, 0) >= 0 {
		// A NUL byte anywhere, a comment included, is Loki's lexer error: the query keeps its comments so that the
		// validation reports it where Loki does.
		return query
	}
	var spans [][2]int
	s := &scanner{src: query, extra: &scanExtra{comments: &spans}}
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
	for _, sp := range spans {
		if commentMakesName(query, sp[0]) {
			return query
		}
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

// onlyUnwrapStubs reports a log query whose unwrap stages all lack their label: Grafana's stubs, which
// StripIncompleteUnwrapStubs drops.
func onlyUnwrapStubs(expr Expr) bool {
	lq, ok := expr.(*LogQuery)
	if !ok {
		return false
	}
	for _, st := range lq.Pipeline {
		if u, ok := st.(*UnwrapStage); ok && u.Label != "" {
			return false
		}
	}
	return true
}

// StripParserFlags removes the logfmt flags of a query (`--strict`,
// `--keep-empty`). The parser keeps them on the typed stage; the request text
// the handlers and the string translator read carries the stage without them,
// which they evaluate as `| logfmt`. A query that does not parse is returned as
// it is: validation reports Loki's error for it.
func StripParserFlags(query string) string {
	if !strings.Contains(query, "--") {
		return query
	}
	var spans [][2]int
	p := &parser{sc: newScanner(query), flagSpans: &spans}
	p.advance()
	// A query with an unwrap outside a range aggregation is Loki's syntax error at the unwrap: it keeps its flags, so
	// that the error is at Loki's column. Grafana's stubs (`| unwrap [1s]`), which the handlers drop, do not count.
	expr, err := p.parseExpr()
	if err != nil || p.cur.Typ != TokEOF || len(spans) == 0 || p.plainUnwrapAt > 0 && !onlyUnwrapStubs(expr) {
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
