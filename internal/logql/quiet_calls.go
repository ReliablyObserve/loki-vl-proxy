package logql

import "sync/atomic"

// A call the parser keeps as text (label_replace, vector, approx_topk) is read twice: the parser reads over it
// (consumeBalancedParens), then a quiet parser of its own reads its parts (typeOpaqueCall). A call nested in it is
// read over again by that parser, and so on down: a query nested d calls deep would be scanned d times over, its
// cost the square of its size. So the first quiet parser that reads over a call records where every parenthesis in
// it closes, and the quiet parsers of the calls nested in it jump to there instead of scanning again. Each level is
// then scanned once by the recording parser and once by its own parser, for its own tokens only. They also note the
// log queries they read in one shared list (typeCall), so that no level walks the tree below it again.

// quietCalls is what the quiet parsers of nested calls kept as text share: the parentheses the first of them read
// over, in the order they open, and the log queries they noted (noteChecked).
type quietCalls struct {
	spans []parenSpan
	// open is the stack of the spans still open while a call is read over.
	open  []int32
	notes []*LogQuery
	// The first spans, open spans and notes, in the allocation of the struct.
	spanBuf [4]parenSpan
	openBuf [4]int32
	noteBuf [2]*LogQuery
}

func newQuietCalls() *quietCalls {
	c := &quietCalls{}
	c.spans, c.open, c.notes = c.spanBuf[:0], c.openBuf[:0], c.noteBuf[:0]
	return c
}

// parenSpan is the text inside one pair of parentheses, as a scanner reads it: from the first token after the
// opening parenthesis to right after the closing one. Its offsets are in the text of the parser that recorded it.
// The scanner state at both ends is kept: a parser whose scanner stands at the first token in the same state reads
// the same tokens to the same end, so it can jump there.
type parenSpan struct {
	// first is the offset of the first token after the opening parenthesis, firstEnd the scanner position right
	// after it.
	first, firstEnd int32
	// end is the scanner position right after the closing parenthesis, -1 while it is not read.
	end int32
	// braceDepth and inRange are the scanner state after the first token and after the closing parenthesis.
	firstBraceDepth, endBraceDepth int32
	firstInRange, endInRange       bool
}

// quietParser is a parser of src from offset pos whose scanner notes no lexer error, for a text read again: a call
// kept as text (typeOpaqueCall), the call a validation reads again (opaqueFirstArgument, labelReplaceArgumentsError,
// vectorArgumentError) or a call left open (unterminatedCallError). base is the offset of src in the query the parse
// started from. Every such parser is made here.
func quietParser(src string, pos, base int) *parser {
	return &parser{sc: scanner{src: src, pos: pos, quiet: true}, base: base}
}

// parenJumpsOff turns the jumps off (tests compare a parse with and without them).
var parenJumpsOff atomic.Bool

// mayJumpParens reports whether p reads over parentheses through the jumps: a quiet parser (it notes no lexer
// error), whose scanner keeps no state the jumps do not (comment spans, unknown names, the NUL bookkeeping), and
// that does not collect rewrites while it reads (CanonicalizeLogRanges, CanonicalSpelling, StripParserFlags).
func (p *parser) mayJumpParens() bool {
	return p.sc.quiet && !p.sc.hasNUL && p.sc.extra == nil && p.canon == nil && p.spell == nil && p.flagSpans == nil &&
		!parenJumpsOff.Load()
}

// recordParens is consumeBalancedParens for the parser that records the jumps: it reads over the call and notes
// the pairs of parentheses inside the call's own (the call's own are never jumped over: its parser reads them). Only
// a call kept as text nested in the call is read over again, by the parser of a call around it: the jumps start
// (and are allocated) at the first such call, and every pair from there on is noted. A pair that closes with none
// open since then was opened before: it is no jump.
func (p *parser) recordParens() (endPos int, ok bool) {
	depth := 1
	if p.calls != nil {
		p.calls.open = p.calls.open[:0]
	}
	callName := false // the current token names a call kept as text
	for depth > 0 && p.cur.Typ != TokEOF {
		switch p.cur.Typ {
		case TokLParen:
			depth++
			record := callName || p.calls != nil
			p.advance()
			if record {
				p.openParenSpan()
			}
			callName = false
			continue
		case TokRParen:
			depth--
			if depth == 0 {
				endPos = p.sc.pos // right after the closing ')'
				break
			}
			if c := p.calls; c != nil && len(c.open) > 0 {
				s := &c.spans[c.open[len(c.open)-1]]
				c.open = c.open[:len(c.open)-1]
				s.end, s.endBraceDepth, s.endInRange = int32(p.base+p.sc.pos), p.sc.braceDepth, p.sc.inRange
			}
		}
		callName = p.cur.Typ == TokIdent && opaqueMetricFunctions[p.cur.Val]
		p.advance()
	}
	if depth != 0 {
		return 0, false
	}
	return endPos, true
}

// openParenSpan notes the parentheses whose first token is the current one.
func (p *parser) openParenSpan() {
	if p.calls == nil {
		p.calls = newQuietCalls()
	}
	c := p.calls
	c.open = append(c.open, int32(len(c.spans)))
	c.spans = append(c.spans, parenSpan{
		first: int32(p.base + p.pos()), firstEnd: int32(p.base + p.sc.pos), end: -1,
		firstBraceDepth: p.sc.braceDepth, firstInRange: p.sc.inRange,
	})
}

// jumpParens is consumeBalancedParens through the jumps: when the recorded parentheses start at the current token,
// with the scanner in the same state, the parser goes on right after their closing parenthesis. It reports false,
// having read nothing, when it cannot jump.
func (p *parser) jumpParens() (endPos int, ok bool) {
	spans, at := p.calls.spans, int32(p.base+p.pos())
	lo, hi := 0, len(spans)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if spans[mid].first < at {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(spans) {
		return 0, false
	}
	s := &spans[lo]
	end := int(s.end) - p.base
	if s.first != at || s.end < 0 || end > len(p.sc.src) || int(s.firstEnd) != p.base+p.sc.pos ||
		s.firstBraceDepth != p.sc.braceDepth || s.firstInRange != p.sc.inRange {
		return 0, false
	}
	p.sc.pos, p.sc.braceDepth, p.sc.inRange = end, s.endBraceDepth, s.endInRange
	p.advance()
	return end, true
}
