package logql

import (
	"fmt"
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
	// ZeroLine is set when Loki prints line 0 for the error: its lexer reports it with a scanner position that
	// is not valid.
	ZeroLine bool
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
	lfSym    // a sign or another single character Loki's lexer returns as a token of its own
)

type lfToken struct {
	kind       lfTokKind
	text       string
	start, end int
	// name is Loki's name for a lfWord: IDENTIFIER, NUMBER, DURATION or BYTES.
	name string
}

type lfParser struct {
	src string
	pos int
	tok lfToken
	// prevEnd is the end of the token before tok: a filter's text ends there, not at a comment after it.
	prevEnd int
	// lexErr is the first error of the lexer, reported ahead of the parse error
	// it causes, like Loki's.
	lexErr *LabelFilterSyntaxError
	// depth is the number of parentheses open at the token being read.
	depth int
	// literalEnd is where the last number, duration or bytes literal ended.
	literalEnd int
	// afterComment is set when a comment stands right before the token.
	afterComment bool
	// prevStart is the start of the token before tok; before the first token it is the offset of the token
	// before the text (negative when the text is part of a longer query).
	prevStart int
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
	return parseLabelFilterText(text, 0)
}

// parseLabelFilterText is ParseLabelFilterExpr for a text that follows another token of a query: before is the
// offset of that token relative to the start of the text (zero or negative).
func parseLabelFilterText(text string, before int) (*LabelFilterExpr, *LabelFilterSyntaxError) {
	expr, _, err := scanLabelFilter(text, before, false)
	return expr, err
}

// scanLabelFilter reads a label filter expression from the start of text. With binary set, the expression ends
// where a binary operator of the query follows it (`| a>5 + 3`: the operator belongs to the query, whose left
// operand is the pipeline); end is where the expression ends, len(text) when it takes the whole text.
func scanLabelFilter(text string, before int, binary bool) (expr *LabelFilterExpr, end int, serr *LabelFilterSyntaxError) {
	p := &lfParser{src: text, literalEnd: -1, prevStart: before}
	p.next()
	expr, err := p.parseOr(true)
	end = len(text)
	if err == nil && p.tok.kind != lfEOF {
		if binary && p.tok.isBinaryOperator() {
			end = p.tok.start
		} else {
			err = p.unexpected("")
		}
	}
	if p.lexErr != nil {
		return nil, 0, p.lexErr
	}
	if err != nil {
		return nil, 0, err
	}
	return expr, end, nil
}

// isBinaryOperator reports a token that is a binary operator of a query: + - * / % ^ == > >= < <= unless.
func (t lfToken) isBinaryOperator() bool {
	switch t.kind {
	case lfSym:
		return strings.Contains("+-*/%^", t.text)
	case lfOp:
		return t.text == "==" || t.text == ">" || t.text == ">=" || t.text == "<" || t.text == "<="
	case lfWord:
		return t.name == "unless"
	}
	return false
}

func (p *lfParser) unexpected(expecting string) *LabelFilterSyntaxError {
	return &LabelFilterSyntaxError{Offset: p.tok.start, Token: lfTokenName(p.tok), Expecting: expecting}
}

// next reads the next token. Whitespace (a space, a tab, a carriage return or a
// newline, as Loki's lexer reads it) and comments (`#` to the end of the line)
// separate tokens; a word runs to the next space, comment, parenthesis, comma,
// quote or operator character, and a number, duration or bytes literal ends where
// Loki's lexer ends it (scanNumberLiteral).
func (p *lfParser) next() {
	p.prevEnd = p.tok.end
	spaced := false
	p.afterComment = false
	if p.pos < len(p.src) && (p.src[p.pos] <= ' ' || p.src[p.pos] == '#') {
		spaced = p.skipSpace()
	}
	start := p.pos
	if start >= len(p.src) {
		p.tok = lfToken{kind: lfEOF, start: len(p.src), end: len(p.src)}
		return
	}
	kind, name, lexErr, binaryAfterSpace, zeroLine := lfWord, "", "", false, false
	switch c := p.src[start]; c {
	case '(':
		kind, p.pos = lfLParen, start+1
	case ')':
		kind, p.pos = lfRParen, start+1
	case ',':
		kind, p.pos = lfComma, start+1
	case '"', '`':
		kind = lfString
		if !p.lexString(start, c) {
			p.tok = lfToken{kind: lfEOF, start: len(p.src), end: len(p.src)}
			return
		}
	case '=', '!', '<', '>':
		kind, p.pos = lfOp, start+1
		if p.pos < len(p.src) && (p.src[p.pos] == '=' || p.src[p.pos] == '~' && (c == '=' || c == '!')) {
			p.pos++
		}
	default:
		kind, name, lexErr, binaryAfterSpace, zeroLine = p.lexWord(start, spaced)
	}
	if p.src[start] == 0 && (spaced || p.afterComment || start == 0) && p.lexErr == nil {
		// Loki's scanner meets a NUL after white space while it skips the white space, with no valid position:
		// it reports the token before, on line 0.
		p.lexErr = &LabelFilterSyntaxError{Offset: p.prevStart, Message: "invalid character NUL", ZeroLine: true}
	} else if p.pos < len(p.src) && p.src[p.pos] == 0 {
		// Loki's scanner reads the character after a token before it hands the token over, and rejects NUL.
		lexErr, zeroLine = "invalid character NUL", false
	}
	if binaryAfterSpace {
		// Loki's scanner reads a 0b or 0B literal after white space as a decimal number without digits, whatever
		// follows the prefix.
		lexErr = "decimal literal has no digits"
	}
	if lexErr != "" && p.lexErr == nil {
		p.lexErr = &LabelFilterSyntaxError{Offset: start, Message: lexErr, ZeroLine: zeroLine}
	}
	p.prevStart = start
	p.tok = lfToken{kind: kind, text: p.src[start:p.pos], start: start, end: p.pos, name: name}
	if kind == lfWord {
		p.tok.kind = connectorKind(p.tok.text)
	}
}

// connectorKind is lfAnd or lfOr for the words and and or in any case, lfWord for any other.
func connectorKind(word string) lfTokKind {
	switch {
	case len(word) == 2 && strings.EqualFold(word, "or"):
		return lfOr
	case len(word) == 3 && strings.EqualFold(word, "and"):
		return lfAnd
	}
	return lfWord
}

// lexString reads the quoted literal that starts at start; it reports false, with the lexer error set, when the
// literal is not terminated.
func (p *lfParser) lexString(start int, quote byte) bool {
	p.pos = start + 1
	for p.pos < len(p.src) && p.src[p.pos] != quote && (quote != '"' || p.src[p.pos] != '\n') {
		if quote == '"' && p.src[p.pos] == '\\' {
			p.pos++
		}
		p.pos++
	}
	if p.pos >= len(p.src) || p.src[p.pos] != quote {
		if p.lexErr == nil {
			p.lexErr = &LabelFilterSyntaxError{Offset: start, Message: "literal not terminated"}
		}
		return false
	}
	p.pos++
	return true
}

// skipSpace skips the white space and comments before a token. It reports whether the token follows white space
// that Loki's scanner skips on its own: a comment takes the line break that ends it with it, and what follows is
// read as if nothing preceded.
func (p *lfParser) skipSpace() (spaced bool) {
	for p.pos < len(p.src) {
		if p.src[p.pos] == '#' {
			start := p.pos
			for p.pos < len(p.src) && p.src[p.pos] != '\n' {
				p.pos++
			}
			if p.pos < len(p.src) {
				p.pos++
			}
			// A comment is a token for Loki's lexer.
			p.prevStart, p.afterComment = start, true
			spaced = false
			continue
		}
		if strings.IndexByte(" \t\r\n", p.src[p.pos]) < 0 {
			break
		}
		p.pos++
		spaced = true
	}
	return spaced
}

// lexWord reads the token that starts at start with a character that is not a bracket, comma, quote or
// operator: a number, duration or bytes literal, a sign or another symbol, or a word. It returns the token kind
// and name, the lexer error of the token, and whether it is a 0b literal after white space.
func (p *lfParser) lexWord(start int, spaced bool) (kind lfTokKind, name, lexErr string, binaryAfterSpace, zeroLine bool) {
	c := p.src[start]
	kind, name = lfWord, "IDENTIFIER"
	if c|0x20 >= 'a' && c|0x20 <= 'z' || c == '_' || c >= utf8.RuneSelf {
		// A name: letters, digits, '_' and '.' (dotted names stay one word); a keyword has its own token name.
		if p.pos = start + lfNameEnd(p.src[start:], p.literalEnd != start); p.pos > start {
			return kind, p.keywordName(start), "", false, false
		}
		_, size := utf8.DecodeRuneInString(p.src[start:])
		p.pos = start + size
		return kind, name, "", false, false
	}
	negEnd := 0
	if c == '-' {
		negEnd = scanNegativeDuration(p.src, start)
	}
	switch {
	case isDecimalDigit(int(c)) || c == '.' && isDecimalDigit(at(p.src, start+1)):
		lit := scanNumberLiteral(p.src, start)
		p.pos, name, p.literalEnd, lexErr = lit.end, lit.name, lit.end, lit.err
		binaryAfterSpace = spaced && c == '0' && lowerByte(at(p.src, start+1)) == 'b'
	case negEnd > 0:
		p.pos = negEnd
		name, p.literalEnd = "DURATION", negEnd
	case strings.IndexByte("+-./*%^{}]", c) >= 0:
		kind, name, p.pos = lfSym, "", start+1
	case c == '[':
		name, lexErr = "RANGE", p.lexRange(start)
		zeroLine = true
	case c == '\'':
		lexErr = p.lexChar(start)
	default:
		// Any other character is a token of its own: Loki's lexer returns it as an identifier.
		p.pos = start + 1
	}
	return kind, name, lexErr, binaryAfterSpace, zeroLine
}

// lexRange reads the `[duration]` that starts at start, which Loki's lexer returns as one RANGE token; it
// returns the lexer error of the token.
func (p *lfParser) lexRange(start int) string {
	end := strings.IndexByte(p.src[start:], ']')
	if end < 0 {
		p.pos = len(p.src)
		return "missing closing ']' in duration"
	}
	p.pos = start + end + 1
	switch d := p.src[start+1 : start+end]; {
	case d == "":
		return "empty duration string"
	case !lokiDuration(d):
		if unit := durationUnit(d); unit != "" && unit != d {
			return fmt.Sprintf("unknown unit %q in duration %q", unit, d)
		}
		return fmt.Sprintf("not a valid duration string: %q", d)
	}
	return ""
}

// lexChar reads the character literal that starts at start with a quote ('x'), which Loki's lexer returns as a
// name; it returns the lexer error of the token.
func (p *lfParser) lexChar(start int) string {
	n := 0
	for i := start + 1; i < len(p.src); n++ {
		switch p.src[i] {
		case '\'':
			p.pos = i + 1
			if n != 1 {
				return "invalid char literal"
			}
			return ""
		case '\n':
			p.pos = i
			return "literal not terminated"
		case '\\':
			i += 2
		default:
			_, size := utf8.DecodeRuneInString(p.src[i:])
			i += size
		}
	}
	p.pos = len(p.src)
	return "literal not terminated"
}

// keywordName is the token name of the word that ends at p.pos: IDENTIFIER, or the keyword Loki's lexer reads.
func (p *lfParser) keywordName(start int) string {
	word := p.src[start:p.pos]
	if len(word) > 20 || len(word) < 2 {
		return "IDENTIFIER"
	}
	var lower [20]byte
	for i := 0; i < len(word); i++ {
		c := word[i]
		if c >= utf8.RuneSelf {
			return "IDENTIFIER"
		}
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		lower[i] = c
	}
	if name := keywordTokenName(p.src, p.pos, string(lower[:len(word)])); name != "" {
		return name
	}
	return "IDENTIFIER"
}

// lfNameEnd is the length of the name at the start of s: letters, digits, '_' and, when dots is set, '.'.
func lfNameEnd(s string, dots bool) int {
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c|0x20 >= 'a' && c|0x20 <= 'z' || c >= '0' && c <= '9' || c == '_' || dots && c == '.' {
				i++
				continue
			}
			return i
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 || !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return i
		}
		i += size
	}
	return len(s)
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
	case p.tok.kind == lfLParen || p.tok.kind == lfWord && p.tok.name == "IDENTIFIER":
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
		if p.tok.name == "IDENTIFIER" {
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
	sign, serr := p.readSign(op)
	if serr != nil {
		return nil, serr
	}
	value := sign + p.tok.text
	kind := "" // what the value is: STRING, NUMBER, DURATION, BYTES, IP, IDENTIFIER
	switch p.tok.kind {
	case lfString:
		kind = "STRING"
	case lfWord:
		if kind = p.tok.name; p.isIPCall() {
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
			return nil, p.unexpected("(")
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
	leaf := &LabelFilterExpr{Leaf: p.src[start:p.tokenEnd()], Name: name, Cmp: op, Value: value}
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

// readSign reads the sign in front of a number, which only a number takes (a duration or a bytes size carries
// none), and requires the number after it.
func (p *lfParser) readSign(op string) (string, *LabelFilterSyntaxError) {
	if p.tok.kind != lfSym || p.tok.text != "+" && p.tok.text != "-" || op == "=~" || op == "!~" {
		return "", nil
	}
	sign := p.tok.text
	p.next()
	if p.tok.kind != lfWord || p.tok.name != "NUMBER" {
		err := p.unexpected("NUMBER")
		if p.tok.kind == lfWord && p.isIPCall() {
			err.Token = "ip"
		}
		return "", err
	}
	return sign, nil
}

// isIPCall reports whether the word at the current token is `ip` followed by a parenthesis. Loki's lexer looks
// for the parenthesis over every character Go calls white space, so `ip` followed by one of those is the keyword
// too, and the character after it is the unexpected token.
func (p *lfParser) isIPCall() bool {
	return strings.EqualFold(p.tok.text, "ip") && strings.HasPrefix(strings.TrimLeftFunc(p.src[p.tok.end:], unicode.IsSpace), "(")
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

// tokenEnd is the end of the previous token, before any whitespace or comment
// that separates it from the current lookahead token.
func (p *lfParser) tokenEnd() int {
	return p.prevEnd
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
		return t.name
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
