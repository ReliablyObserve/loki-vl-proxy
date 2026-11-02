package translator

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// LabelFilterExpr is a parsed label filter stage (`| status>=500, (a="1" or
// b="2")`), the grammar of Loki's labelFilter rule in pkg/logql/syntax/syntax.y:
//
//	labelFilter: matcher | ipLabelFilter | unitFilter | numberFilter
//	           | '(' labelFilter ')'
//	           | labelFilter labelFilter | labelFilter AND labelFilter
//	           | labelFilter COMMA labelFilter | labelFilter OR labelFilter
//
// The tokens AND and OR carry a precedence (OR below AND, both left
// associative); COMMA and the juxtaposed `labelFilter labelFilter` carry none,
// which makes the parser always shift: the right operand of a comma or of a
// juxtaposition is everything up to the closing parenthesis or the end of the
// stage, so `a, b or c` is `a and (b or c)` and `a or b, c` is `a or (b and c)`.
type LabelFilterExpr struct {
	Op          LabelFilterOp
	Left, Right *LabelFilterExpr
	// Leaf is the text of a single filter (`status>=500`) when Op is
	// LabelFilterLeaf.
	Leaf string
}

// LabelFilterOp is the kind of a LabelFilterExpr node.
type LabelFilterOp int

// The kinds of LabelFilterExpr nodes.
const (
	LabelFilterLeaf LabelFilterOp = iota
	LabelFilterAnd
	LabelFilterOr
)

// LabelFilterSyntaxError is a rejected label filter, positioned the way Loki's
// goyacc parser reports it.
type LabelFilterSyntaxError struct {
	// Offset is the byte offset of the unexpected token in the parsed text;
	// len(text) when the text ended too early.
	Offset int
	// Token is Loki's name for the unexpected token (IDENTIFIER, STRING,
	// NUMBER, DURATION, BYTES, ")", "and", "$end", ...).
	Token string
	// Expecting is the expected-token list Loki prints ("IDENTIFIER or ("),
	// empty when it prints none.
	Expecting string
	// Message replaces the whole "syntax error: unexpected ..." text for an
	// error of Loki's lexer ("literal not terminated").
	Message string
}

func (e *LabelFilterSyntaxError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	msg := "syntax error: unexpected " + e.Token
	if e.Expecting != "" {
		msg += ", expecting " + e.Expecting
	}
	return msg
}

// IsLeaf reports whether the expression is a single filter.
func (e *LabelFilterExpr) IsLeaf() bool { return e.Op == LabelFilterLeaf }

// String renders the expression with every binary node parenthesized, so the
// rendering parses back to the same tree.
func (e *LabelFilterExpr) String() string { return e.render(func(leaf string) string { return leaf }) }

// render renders the expression, replacing each leaf with leafText(leaf).
// Chains of one operator render flat; a nested operator of the other kind is
// parenthesized.
func (e *LabelFilterExpr) render(leafText func(string) string) string {
	if e.IsLeaf() {
		return leafText(e.Leaf)
	}
	var operands []string
	e.collect(e.Op, &operands, leafText)
	sep := " and "
	if e.Op == LabelFilterOr {
		sep = " or "
	}
	return "(" + strings.Join(operands, sep) + ")"
}

func (e *LabelFilterExpr) collect(op LabelFilterOp, out *[]string, leafText func(string) string) {
	if e.Op != op {
		*out = append(*out, e.render(leafText))
		return
	}
	e.Left.collect(op, out, leafText)
	e.Right.collect(op, out, leafText)
}

// translate renders the expression as a LogsQL filter, translating each leaf
// like a stand-alone label filter stage. Every binary node is parenthesized, so
// the result does not depend on the backend's operator precedence.
func (e *LabelFilterExpr) translate(labelFn LabelTranslateFunc, caps logsql.Capabilities) (string, bool) {
	ok := true
	out := e.render(func(leaf string) string {
		translated, leafOK := translateSingleLabelFilter(leaf, labelFn, caps)
		ok = ok && leafOK
		return parenthesizeCompound(translated)
	})
	return out, ok
}

// parenthesizeCompound wraps a translated filter that holds a top-level space
// (an `a OR b` alternation) so an enclosing and/or cannot split it.
func parenthesizeCompound(s string) string {
	var quote byte
	depth := 0
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
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ' ' && depth == 0:
			return "(" + s + ")"
		}
	}
	return s
}

// mayBeCompound reports whether a label filter stage can hold more than one
// filter: it has a parenthesis, a comma, an and/or keyword or a second
// operator. It is a cheap scan that spares the common stage, a single filter,
// the parse; a false answer is exact, a true one is not.
func mayBeCompound(stage string) bool {
	const delimiters = " \t\r\n(),\"`=!<>"
	var quote byte
	operators, inOperator, inIP := 0, false, false
	for i := 0; i < len(stage); i++ {
		c := stage[i]
		if quote != 0 {
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '(':
			// The parentheses of ip("...") belong to the filter.
			if inIP || i < 2 || !strings.EqualFold(stage[i-2:i], "ip") || i > 2 && strings.IndexByte(delimiters, stage[i-3]) < 0 {
				return true
			}
			inIP = true
		case ')':
			if !inIP {
				return true
			}
			inIP = false
		case ',':
			return true
		case '"', '`':
			quote, inOperator = c, false
		case '=', '!', '<', '>', '~':
			if !inOperator {
				if operators++; operators > 1 {
					return true
				}
			}
			inOperator = true
		default:
			inOperator = false
			if (c == 'a' || c == 'A' || c == 'o' || c == 'O') && (i == 0 || strings.IndexByte(delimiters, stage[i-1]) >= 0) {
				end := i
				for end < len(stage) && strings.IndexByte(delimiters, stage[end]) < 0 {
					end++
				}
				if word := stage[i:end]; strings.EqualFold(word, "and") || strings.EqualFold(word, "or") {
					return true
				}
			}
		}
	}
	return false
}

// lfTokKind is the kind of a label filter token.
type lfTokKind int

const (
	lfEOF lfTokKind = iota
	lfLParen
	lfRParen
	lfComma
	lfAnd
	lfOr
	lfOp     // = == != =~ !~ > >= < <=
	lfString // "..." or `...`
	lfWord   // label name, number, duration, bytes, bare value
)

type lfToken struct {
	kind       lfTokKind
	text       string
	start, end int
}

type lfParser struct {
	src string
	pos int
	tok lfToken
	// lexErr is the first error of the lexer, reported ahead of the parse error
	// it causes, like Loki's.
	lexErr *LabelFilterSyntaxError
	// depth is the number of parentheses open at the token being read.
	depth int
}

// maxLabelFilterDepth bounds the nesting of parentheses and the length of a
// chain of commas or juxtaposed filters (each recurses), so that a query made
// of nothing else cannot grow the stack with its size.
const maxLabelFilterDepth = 1000

// ParseLabelFilterExpr parses the text of a label filter stage (what follows
// the `|`). A syntax error carries Loki's token name and expected-token list.
//
// The value of every filter has the kind Loki's grammar wants for its
// operator: a string for =, !=, =~ and !~ (also a number, a duration or a bytes
// size for = and !=, and ip("...")), a number, a duration or a bytes size for
// ==, <, <=, > and >=.
func ParseLabelFilterExpr(text string) (*LabelFilterExpr, *LabelFilterSyntaxError) {
	p := &lfParser{src: text}
	p.next()
	expr, err := p.parseOr(true)
	if err == nil && p.tok.kind != lfEOF {
		err = p.unexpected("")
	}
	if p.lexErr != nil {
		return nil, p.lexErr
	}
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *lfParser) unexpected(expecting string) *LabelFilterSyntaxError {
	return &LabelFilterSyntaxError{Offset: p.tok.start, Token: lfTokenName(p.tok), Expecting: expecting}
}

// next reads the next token. Whitespace separates tokens; a word runs to the
// next space, parenthesis, comma, quote or operator character.
func (p *lfParser) next() {
	for p.pos < len(p.src) {
		r, size := utf8.DecodeRuneInString(p.src[p.pos:])
		if !unicode.IsSpace(r) {
			break
		}
		p.pos += size
	}
	start := p.pos
	if start >= len(p.src) {
		p.tok = lfToken{kind: lfEOF, start: len(p.src), end: len(p.src)}
		return
	}
	kind := lfWord
	switch c := p.src[start]; c {
	case '(':
		kind, p.pos = lfLParen, start+1
	case ')':
		kind, p.pos = lfRParen, start+1
	case ',':
		kind, p.pos = lfComma, start+1
	case '"', '`':
		kind = lfString
		p.pos = start + 1
		for p.pos < len(p.src) && p.src[p.pos] != c {
			if c == '"' && p.src[p.pos] == '\\' {
				p.pos++
			}
			p.pos++
		}
		if p.pos >= len(p.src) {
			if p.lexErr == nil {
				p.lexErr = &LabelFilterSyntaxError{Offset: start, Message: "literal not terminated"}
			}
			p.tok = lfToken{kind: lfEOF, start: len(p.src), end: len(p.src)}
			return
		}
		p.pos++
	case '=', '!', '<', '>':
		kind, p.pos = lfOp, start+1
		if p.pos < len(p.src) && (p.src[p.pos] == '=' || p.src[p.pos] == '~' && (c == '=' || c == '!')) {
			p.pos++
		}
	default:
		for p.pos < len(p.src) && !strings.ContainsRune(" \t\r\n(),\"`=!<>", rune(p.src[p.pos])) {
			p.pos++
		}
	}
	p.tok = lfToken{kind: kind, text: p.src[start:p.pos], start: start, end: p.pos}
	if kind == lfWord {
		switch strings.ToLower(p.tok.text) {
		case "and":
			p.tok.kind = lfAnd
		case "or":
			p.tok.kind = lfOr
		}
	}
}

// parseOr: OR binds loosest and is left associative.
func (p *lfParser) parseOr(first bool) (*LabelFilterExpr, *LabelFilterSyntaxError) {
	left, err := p.parseAnd(first)
	for err == nil && p.tok.kind == lfOr {
		p.next()
		var right *LabelFilterExpr
		if right, err = p.parseAnd(false); err == nil {
			left = &LabelFilterExpr{Op: LabelFilterOr, Left: left, Right: right}
		}
	}
	return left, err
}

// parseAnd: AND binds tighter than OR and is left associative.
func (p *lfParser) parseAnd(first bool) (*LabelFilterExpr, *LabelFilterSyntaxError) {
	left, err := p.parseJoined(first)
	for err == nil && p.tok.kind == lfAnd {
		p.next()
		var right *LabelFilterExpr
		if right, err = p.parseJoined(false); err == nil {
			left = &LabelFilterExpr{Op: LabelFilterAnd, Left: left, Right: right}
		}
	}
	return left, err
}

// parseJoined reads an operand and, when a comma or a juxtaposed operand
// follows, joins it with everything that comes after: neither carries a
// precedence in Loki's grammar, so the parser shifts.
func (p *lfParser) parseJoined(first bool) (*LabelFilterExpr, *LabelFilterSyntaxError) {
	left, err := p.parseOperand(first)
	if err != nil {
		return nil, err
	}
	switch {
	case p.tok.kind == lfComma:
		p.next()
	case p.tok.kind == lfLParen || p.tok.kind == lfWord && LabelFilterWordName(p.tok.text) == "IDENTIFIER":
	default:
		return left, nil
	}
	if p.depth >= maxLabelFilterDepth {
		return nil, p.unexpected("")
	}
	p.depth++
	right, err := p.parseOr(false)
	p.depth--
	if err != nil {
		return nil, err
	}
	return &LabelFilterExpr{Op: LabelFilterAnd, Left: left, Right: right}, nil
}

// parseOperand reads a parenthesized expression or a single filter. first is
// true at the start of the stage, where Loki lists no expected tokens.
func (p *lfParser) parseOperand(first bool) (*LabelFilterExpr, *LabelFilterSyntaxError) {
	switch p.tok.kind {
	case lfLParen:
		if p.depth >= maxLabelFilterDepth {
			return nil, p.unexpected("")
		}
		p.depth++
		p.next()
		inner, err := p.parseOr(false)
		p.depth--
		if err != nil {
			return nil, err
		}
		if p.tok.kind != lfRParen {
			return nil, p.unexpected("")
		}
		p.next()
		return inner, nil
	case lfWord:
		if LabelFilterWordName(p.tok.text) == "IDENTIFIER" {
			return p.parseFilter()
		}
	}
	if first {
		return nil, p.unexpected("")
	}
	return nil, p.unexpected("IDENTIFIER or (")
}

// parseFilter reads `name op value`: a string for =, !=, =~ and !~, a number,
// a duration or a bytes size for the comparison operators and ==, and
// ip("...") for = and !=.
func (p *lfParser) parseFilter() (*LabelFilterExpr, *LabelFilterSyntaxError) {
	start := p.tok.start
	p.next()
	if p.tok.kind != lfOp {
		return nil, p.unexpected("")
	}
	op := p.tok.text
	switch op {
	case "=", "==", "!=", "=~", "!~", ">", ">=", "<", "<=":
	default:
		err := p.unexpected("") // a lone "!", which Loki's lexer reads as a name
		err.Token = "IDENTIFIER"
		return nil, err
	}
	p.next()
	kind := "" // what the value is: STRING, NUMBER, DURATION, BYTES, IP, IDENTIFIER
	switch p.tok.kind {
	case lfString:
		kind = "STRING"
	case lfWord:
		if kind = LabelFilterWordName(p.tok.text); strings.EqualFold(p.tok.text, "ip") && strings.HasPrefix(strings.TrimLeftFunc(p.src[p.tok.end:], unicode.IsSpace), "(") {
			kind = "IP"
		}
	}
	allowed := kind == "NUMBER" || kind == "DURATION" || kind == "BYTES"
	switch op {
	case "=", "!=":
		allowed = allowed || kind == "STRING" || kind == "IP"
	case "=~", "!~":
		allowed = kind == "STRING"
	}
	if !allowed {
		expecting := ""
		if op == "=~" || op == "!~" {
			expecting = "STRING"
		}
		err := p.unexpected(expecting)
		if kind == "IP" {
			err.Token = "ip"
		}
		return nil, err
	}
	p.next()
	if kind == "IP" {
		if p.tok.kind != lfLParen {
			return nil, p.unexpected("")
		}
		p.next()
		if p.tok.kind != lfString {
			return nil, p.unexpected("STRING")
		}
		p.next()
		if p.tok.kind != lfRParen {
			return nil, p.unexpected(")")
		}
		p.next()
	}
	return &LabelFilterExpr{Leaf: strings.TrimSpace(p.src[start:p.tokenEnd()])}, nil
}

// tokenEnd is the end of the previous token: the text read before the current
// lookahead token, without the whitespace that precedes it.
func (p *lfParser) tokenEnd() int {
	return len(strings.TrimRightFunc(p.src[:p.tok.start], unicode.IsSpace))
}

// lfTokenName is the name Loki's parser prints for a token.
func lfTokenName(t lfToken) string {
	switch t.kind {
	case lfEOF:
		return "$end"
	case lfString:
		return "STRING"
	case lfAnd:
		return "and"
	case lfOr:
		return "or"
	case lfWord:
		return LabelFilterWordName(t.text)
	}
	return t.text
}

// LabelFilterWordName classifies a word the way Loki's lexer does: a number, a duration
// or a bytes size, otherwise an identifier.
func LabelFilterWordName(w string) string {
	digits := strings.TrimLeft(w, "+-")
	if digits == "" || (digits[0] < '0' || digits[0] > '9') && digits[0] != '.' {
		return "IDENTIFIER"
	}
	if _, err := time.ParseDuration(digits); err == nil && strings.TrimLeft(digits, "0123456789.") != "" {
		return "DURATION"
	}
	switch suffix := strings.ToLower(strings.TrimLeft(digits, "0123456789.")); suffix {
	case "":
		return "NUMBER"
	case "b", "k", "kb", "kib", "m", "mb", "mib", "g", "gb", "gib", "t", "tb", "tib", "p", "pb", "pib", "e", "eb", "eib":
		return "BYTES"
	}
	return "NUMBER"
}
