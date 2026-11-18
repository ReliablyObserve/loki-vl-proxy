package logql

import (
	"errors"
	"math/bits"
	"sort"
	"strings"
)

// CanonicalSpelling writes the metric calls and parentheses of a query in one spelling, the one its parsed tree
// prints the same whatever the blanks and parentheses around them were:
//   - no blank between the name of a metric call and its parenthesis: `rate ({a="x"}[5m])` is
//     `rate({a="x"}[5m])`, `vector (1)` is `vector(1)`, `sum (rate(...))` is `sum(rate(...))`;
//   - no blank right inside the parentheses of such a call: `vector( 1 )` is `vector(1)`,
//     `sum by (a) ( rate(...) )` is `sum by (a) (rate(...))`, `label_replace( x, ... )` is `label_replace(x, ...)`;
//   - no parentheses around a call or around parentheses: `(rate(...))` is `rate(...)`, `sum((rate(...)))` is
//     `sum(rate(...))`, `x + (sum(...))` is `x + sum(...)`; parentheses around a binary operation go only where no
//     operator reads them, around the whole query or as the whole argument of a call (`(a + b)` is `a + b`,
//     `sum((a + b))` is `sum(a + b)`, `(a + b) * c` keeps them).
//
// The metric calls are the range functions, the vector aggregations and rankings, label_replace (whose first
// argument is spelled as a query of its own), vector and approx_topk. Everything else keeps its bytes: the log
// queries, their pipelines and ranges, the by, without, on and ignoring clauses, the numbers, and the blanks
// between the operands and operators of a binary operation.
//
// Loki reads the respelled query as it reads the query (the Loki parse oracle holds it on the query corpus). A
// query the validation rejects (ValidateLogQL), that holds a `#` comment, or a call the parser keeps as text without
// reading its parts (approx_topk, a label_replace or vector call it cannot type) is returned as it is: its
// validation answers Loki's error, with Loki's positions. So is a query already
// in that spelling: a scan over its bytes outside strings (respellable) decides that without parsing it.
func CanonicalSpelling(query string) string {
	if !respellable(query) {
		return query
	}
	return respell(query, true)
}

// CanonicalSpellingCandidate is CanonicalSpelling's pre-check: false for a query CanonicalSpelling keeps as it is,
// read without parsing it and without allocating. A caller that validates the query itself (a memoised validation)
// asks it first, validates, then calls CanonicalSpellingOfValid.
func CanonicalSpellingCandidate(query string) bool { return respellable(query) }

// CanonicalSpellingOfValid is CanonicalSpelling for a query the caller's validation accepted (a superset of
// ValidateLogQL's checks): it does not validate the query again.
func CanonicalSpellingOfValid(query string) string { return respell(query, false) }

// respell is CanonicalSpelling past its pre-check; validate runs ValidateLogQL first.
func respell(query string, validate bool) string {
	if InputSizeError(query) != "" {
		return query
	}
	edits, ok := spellEdits(query)
	if !ok || len(edits) == 0 || validate && ValidateLogQL(query) != "" {
		return query
	}
	out := make([]byte, 0, len(query))
	last := 0
	for _, e := range edits {
		out = append(out, query[last:e.start]...)
		// A removal that would join two words (`and(rate(...))`, `bool((vector(1)))`) leaves one blank.
		if len(out) > 0 && e.end < len(query) && wordish(out[len(out)-1]) && wordish(query[e.end]) {
			out = append(out, ' ')
		}
		last = e.end
	}
	return string(append(out, query[last:]...))
}

// wordish reports a byte a word may hold: a letter, a digit, an underscore, or a byte outside ASCII.
func wordish(c byte) bool { return isWordByte(c) || c >= 0x80 }

// spellState collects what CanonicalSpelling rewrites.
type spellState struct {
	// edits are spans of the input to remove, in the order they were found.
	edits []rewrite
	// pending is a binary operation read inside parentheses, with the edits that remove them: kept only when no
	// operator reads the parentheses (spellArgumentParens).
	pending struct {
		expr  Expr
		edits [2]rewrite
	}
	// bad is set when a span to remove holds anything but blanks (a comment).
	bad bool
	// keepOpen is the offset of a parenthesis right after a vector matching clause, which keeps its spelling; -1 for
	// none.
	keepOpen int
}

// spellEdits parses q for CanonicalSpelling and returns its edits, sorted, and false when q is not respelled.
func spellEdits(q string) ([]rewrite, bool) {
	if hasComment(q) {
		return nil, false
	}
	p := &parser{sc: newScanner(q), spell: &spellState{keepOpen: -1}}
	p.advance()
	expr, err := p.parseExpr()
	if err != nil || p.cur.Typ != TokEOF || p.sc.lexErrCount() > 0 || p.treeErr != "" || p.plainUnwrapAt > 0 || p.spell.bad {
		return nil, false
	}
	p.spellArgumentParens(expr) // `(a + b)`
	edits := p.spell.edits
	sort.Slice(edits, func(i, j int) bool { return edits[i].start < edits[j].start })
	for i := 1; i < len(edits); i++ {
		if edits[i].start < edits[i-1].end {
			return nil, false
		}
	}
	return edits, true
}

// spellGap removes the blanks between offsets from and to.
func (p *parser) spellGap(from, to int) {
	if to <= from {
		return
	}
	for i := from; i < to; i++ {
		if c := p.sc.src[i]; c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			p.spell.bad = true
			return
		}
	}
	p.spell.edits = append(p.spell.edits, rewrite{start: from, end: to})
}

// spellCallOpen removes the blanks after the opening parenthesis of a call, read just before.
func (p *parser) spellCallOpen() { p.spellGap(p.prevEnd, p.pos()) }

// spellParens handles the parentheses around inner, which open at offset open and close at the current token,
// first being the offset of the first token inside them: removed around a call or around parentheses, kept around
// a number or a log query, and pending around a binary operation (spellArgumentParens).
func (p *parser) spellParens(open, first int, inner Expr) {
	switch inner.(type) {
	case *LogQuery, *StreamSelector, *LiteralExpr, *BadLiteralExpr:
		return
	}
	st := p.spell
	if open == st.keepOpen {
		return
	}
	edits := [2]rewrite{p.spellRemove(open, first), p.spellRemove(p.prevEnd, p.pos()+1)}
	if _, ok := inner.(*BinOpExpr); !ok {
		st.edits = append(st.edits, edits[:]...)
		return
	}
	if st.pending.expr == inner {
		// Parentheses around parentheses: the inner ones go.
		st.edits = append(st.edits, st.pending.edits[:]...)
	}
	st.pending.expr, st.pending.edits = inner, edits
}

// spellArgumentParens removes the pending parentheses around a binary operation that is the whole query or the
// whole argument of a call.
func (p *parser) spellArgumentParens(arg Expr) {
	if st := p.spell; st.pending.expr != nil && st.pending.expr == arg {
		st.edits = append(st.edits, st.pending.edits[:]...)
		st.pending.expr = nil
	}
}

// spellRemove is the edit that removes src[from:to] (respell keeps a blank where the removal joins two words).
func (p *parser) spellRemove(from, to int) rewrite {
	src := p.sc.src
	for i := from; i < to; i++ {
		if c := src[i]; c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '(' && c != ')' {
			p.spell.bad = true
		}
	}
	return rewrite{start: from, end: to}
}

// spellOpaqueCall handles a metric call the parser keeps as text, src[start:end], whose arguments start at arg. A call
// whose parts the parser does not read keeps its spelling, and so does the query: approx_topk, a vector call that is
// not one number (label_replace is read where it stands, spellLabelReplace).
func (p *parser) spellOpaqueCall(name string, start, _, end int) {
	if name != "vector" {
		p.spell.bad = true
		return
	}
	o := &OpaqueMetricExpr{Raw: strings.TrimSpace(p.sc.src[start:end]), Func: name}
	if typeOpaqueCall(o); !o.Typed {
		p.spell.bad = true
	}
}

// errSpellKept stops CanonicalSpelling's parse of a query whose spelling it keeps.
var errSpellKept = errors.New("logql: spelling kept")

// spellLabelReplace reads the arguments of a label_replace call whose name starts at offset start, the parser past
// its opening parenthesis: its first argument is respelled as the whole argument of a call, where it stands (read
// apart, the argument of each call nested in it would be read again at every level of the nesting), its four
// strings keep their spelling, and so does the query when they are not four strings and a closing parenthesis.
func (p *parser) spellLabelReplace(start int, treeErr *string) (Expr, error) {
	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	p.spellArgumentParens(inner) // `label_replace((a + b), ...)`
	for i := 0; i < 4; i++ {
		if p.cur.Typ != TokComma {
			return nil, errSpellKept
		}
		if p.advance(); p.cur.Typ != TokString && p.cur.Typ != TokRawString {
			return nil, errSpellKept
		}
		p.advance()
	}
	if p.cur.Typ != TokRParen {
		return nil, errSpellKept
	}
	p.spellGap(p.prevEnd, p.pos())
	end := p.sc.pos
	p.advance()
	return &OpaqueMetricExpr{Raw: strings.TrimSpace(p.sc.src[start:end]), Func: "label_replace", treeErr: treeErr}, nil
}

// hasComment reports a `#` outside the strings of q.
func hasComment(q string) bool {
	var quote byte
	for i := 0; i < len(q); i++ {
		switch c := q[i]; {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '#':
			return true
		}
	}
	return false
}

// lowerASCII reports a word of ASCII bytes with no capital.
func lowerASCII(w string) bool {
	for i := 0; i < len(w); i++ {
		if c := w[i]; c >= 0x80 || c >= 'A' && c <= 'Z' {
			return false
		}
	}
	return true
}

// metricCall reports a name CanonicalSpelling writes with its parenthesis: a range function, a vector aggregation
// or ranking, label_replace, vector or approx_topk (TestMetricCallNamesTheParsersCalls holds the list to the parser's).
func metricCall(name string) bool {
	switch name {
	case "sum", "avg", "min", "max", "count", "stddev", "stdvar", "bottomk", "topk", "sort", "sort_desc",
		"rate", "count_over_time", "bytes_rate", "bytes_over_time", "avg_over_time", "sum_over_time", "min_over_time",
		"max_over_time", "stdvar_over_time", "stddev_over_time", "quantile_over_time", "first_over_time",
		"last_over_time", "absent_over_time", "rate_counter",
		"label_replace", "vector", "approx_topk":
		return true
	}
	return false
}

// respellable is CanonicalSpelling's pre-check: whether q, outside its strings, holds a spelling CanonicalSpelling may
// rewrite:
//   - a `(` with a blank or a `(` right after it, or a `)` with a blank before it;
//   - a `(` after a blank that follows a metric call's name, and, or, unless or bool;
//   - a `(` right after the start of the query, `(`, `,` or an operator, or right after and, or, unless or bool;
//   - a `(` after a word with a capital or a byte outside ASCII, which the parser may read as a keyword.
//
// It is false for a query with a `#` comment (not respelled). A `(` right after a `)` opens a call after its grouping
// or an operand after a matching clause, whose parentheses CanonicalSpelling keeps. It may report a query
// CanonicalSpelling then leaves as it is (a label filter in parentheses after and), never the other way round
// (FuzzCanonicalSpelling). It reads eight bytes at a time, stops only at the parentheses, quotes and `#`, and allocates
// nothing.
func respellable(q string) bool {
	i := 0
	for i+8 <= len(q) {
		m := specialBytes(load8(q, i))
		if m == 0 {
			i += 8
			continue
		}
		// The first parenthesis, quote or `#` of the word; the scan goes on right after it.
		j := i + bits.TrailingZeros64(m)>>3
		switch q[j] {
		case '"', '`':
			i = quoteEnd(q, j) + 1
			continue
		case '#':
			return false
		default:
			if spellAt(q, j) {
				return !hasComment(q[j:])
			}
		}
		i = j + 1
	}
	for ; i < len(q); i++ {
		switch q[i] {
		case '"', '`':
			i = quoteEnd(q, i)
		case '#':
			return false
		case '(', ')':
			if spellAt(q, i) {
				return !hasComment(q[i:])
			}
		}
	}
	return false
}

// load8 is the eight bytes at q[i], the first in the low byte.
func load8(q string, i int) uint64 {
	return uint64(q[i]) | uint64(q[i+1])<<8 | uint64(q[i+2])<<16 | uint64(q[i+3])<<24 |
		uint64(q[i+4])<<32 | uint64(q[i+5])<<40 | uint64(q[i+6])<<48 | uint64(q[i+7])<<56
}

// specialBytes marks with its high bit each byte of w that is `"`, `#`, `(`, `)` or a backquote.
func specialBytes(w uint64) uint64 {
	const ones, pairs = 0x0101010101010101, 0xFEFEFEFEFEFEFEFE
	return zeroBytes((w^(ones*0x22))&pairs) | zeroBytes((w^(ones*0x28))&pairs) | zeroBytes(w^(ones*0x60))
}

// spellAt reports whether CanonicalSpelling may respell around the parenthesis at q[j].
func spellAt(q string, j int) bool {
	if q[j] == ')' {
		return j > 0 && isBlankByte(q[j-1])
	}
	if j == 0 || j+1 < len(q) && (isBlankByte(q[j+1]) || q[j+1] == '(') {
		return true
	}
	b := q[j-1]
	switch {
	case b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_':
		// A name right before its parenthesis: a call, unless it is and, or, unless or bool (the last two letters of
		// none of the call names but vector and duration).
		if j < 2 {
			return false
		}
		switch q[j-2 : j] {
		case "nd", "or", "ss", "ol":
			return keywordBefore(q, j)
		}
		return false
	case isBlankByte(b):
		k := j - 2
		for k >= 0 && isBlankByte(q[k]) {
			k--
		}
		if k < 0 {
			return true // the start of the query
		}
		if !wordish(q[k]) {
			return q[k] != ')' && strings.IndexByte("(,+-*/%^<>=", q[k]) >= 0
		}
		start := k
		for start > 0 && wordish(q[start-1]) {
			start--
		}
		word := q[start : k+1]
		return !lowerASCII(word) || metricCall(word) || word == "and" || word == "or" || word == "unless" || word == "bool"
	case b == ')':
		return false
	}
	// An operator, a comma or a `(` before it, or a word in another case.
	return true
}

// keywordBefore reports and, or, unless or bool right before q[j].
func keywordBefore(q string, j int) bool {
	start := j
	for start > 0 && wordish(q[start-1]) {
		start--
	}
	switch q[start:j] {
	case "and", "or", "unless", "bool":
		return true
	}
	return false
}

// quoteEnd is the index of the quote that closes the string opening at q[i], or the end of q.
func quoteEnd(q string, i int) int {
	if q[i] == '`' {
		if j := strings.IndexByte(q[i+1:], '`'); j >= 0 {
			return i + 1 + j
		}
		return len(q)
	}
	for k := i + 1; k < len(q); k++ {
		switch q[k] {
		case '"':
			return k
		case '\\':
			k++ // the escaped byte
		}
	}
	return len(q)
}

// zeroBytes marks with its high bit each zero byte of v, exactly.
func zeroBytes(v uint64) uint64 {
	const lows = 0x7F7F7F7F7F7F7F7F
	return ^((v&lows + lows) | v | lows)
}

func isBlankByte(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
