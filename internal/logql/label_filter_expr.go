package logql

import (
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
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
	// Name, Cmp, Value and Kind are the parts of that filter (`status`, `>=`,
	// `500`, a number), set on a leaf only. Value is a string literal without its
	// quotes and unescaped as the LogQL scanner does, the pattern of an
	// ip("...") filter likewise, and the literal text of a number, a duration or
	// a bytes size. Kind is LabelFilterValueNone, and Value empty, when a string
	// literal is not valid (the stage is still accepted, as its Leaf text is).
	Name  string
	Cmp   string
	Value string
	Kind  LabelFilterValueKind
}

// LabelFilterValueKind is the kind of value a label filter compares a label to.
type LabelFilterValueKind int

// The kinds of label filter values, named like Loki's lexer tokens.
const (
	LabelFilterValueNone LabelFilterValueKind = iota
	LabelFilterValueString
	LabelFilterValueNumber
	LabelFilterValueDuration
	LabelFilterValueBytes
	LabelFilterValueIP
)

func (k LabelFilterValueKind) String() string {
	switch k {
	case LabelFilterValueString:
		return "STRING"
	case LabelFilterValueNumber:
		return "NUMBER"
	case LabelFilterValueDuration:
		return "DURATION"
	case LabelFilterValueBytes:
		return "BYTES"
	case LabelFilterValueIP:
		return "IP"
	}
	return ""
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
func (e *LabelFilterExpr) String() string { return e.Render(func(leaf string) string { return leaf }) }

// Render renders the expression, replacing each leaf with leafText(leaf).
// Chains of one operator render flat; a nested operator of the other kind is
// parenthesized.
func (e *LabelFilterExpr) Render(leafText func(string) string) string {
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
		*out = append(*out, e.Render(leafText))
		return
	}
	e.Left.collect(op, out, leafText)
	e.Right.collect(op, out, leafText)
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

// next reads the next token. Whitespace and comments (`#` to the end of the
// line, as the LogQL scanner skips them) separate tokens; a word runs to the
// next space, comment, parenthesis, comma, quote or operator character.
func (p *lfParser) next() {
	for p.pos < len(p.src) {
		if p.src[p.pos] == '#' {
			for p.pos < len(p.src) && p.src[p.pos] != '\n' {
				p.pos++
			}
			continue
		}
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
		for p.pos < len(p.src) && !strings.ContainsRune(" \t\r\n(),\"`=!<>#", rune(p.src[p.pos])) {
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
	start, name := p.tok.start, p.tok.text
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
	value := p.tok.text
	kind := "" // what the value is: STRING, NUMBER, DURATION, BYTES, IP, IDENTIFIER
	switch p.tok.kind {
	case lfString:
		kind = "STRING"
	case lfWord:
		if kind = LabelFilterWordName(p.tok.text); strings.EqualFold(p.tok.text, "ip") && strings.HasPrefix(strings.TrimLeftFunc(p.src[p.tok.end:], unicode.IsSpace), "(") {
			kind = "IP"
		}
	}
	if p.tok.kind == lfWord && (value == "0B" || value == "0b") {
		// Loki's lexer reads 0B as the prefix of a binary literal with no digits after it.
		return nil, &LabelFilterSyntaxError{Offset: p.tok.start, Message: "decimal literal has no digits"}
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
		value = p.tok.text
		p.next()
		if p.tok.kind != lfRParen {
			return nil, p.unexpected(")")
		}
		p.next()
	}
	leaf := &LabelFilterExpr{Leaf: strings.TrimSpace(p.src[start:p.tokenEnd()]), Name: name, Cmp: op, Value: value}
	switch kind {
	case "STRING", "IP":
		var ok bool
		if leaf.Value, ok = unquoteLabelFilterString(value); ok {
			leaf.Kind = LabelFilterValueString
			if kind == "IP" {
				leaf.Kind = LabelFilterValueIP
			}
		}
	case "NUMBER":
		leaf.Kind = LabelFilterValueNumber
	case "DURATION":
		leaf.Kind = LabelFilterValueDuration
	case "BYTES":
		leaf.Kind = LabelFilterValueBytes
	}
	return leaf, nil
}

// unquoteLabelFilterString is the value of a string literal the way the LogQL
// scanner reads it: the text of a back-quoted literal as it is, a double-quoted
// one unescaped, which fails for an invalid escape or invalid UTF-8.
func unquoteLabelFilterString(literal string) (string, bool) {
	if literal[0] == '`' {
		return literal[1 : len(literal)-1], true
	}
	value, err := strconv.Unquote(literal)
	return value, err == nil && utf8.ValidString(literal)
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
