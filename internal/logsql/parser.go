// internal/logsql/parser.go
package logsql

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// Parse parses a complete LogsQL query (filter + optional pipe stages).
// Returns an error if the input is empty or syntactically invalid.
func Parse(input string) (*Query, error) {
	querycorpus.Record(querycorpus.LogsQL, input)
	p := newParser(input)
	q, err := p.parseQuery()
	if err == nil && p.scanErr != "" {
		return nil, fmt.Errorf("logsql: %s", p.scanErr)
	}
	return q, err
}

// ParseFilter parses only a filter expression (no pipes).
// Returns an error if the input is empty or syntactically invalid.
func ParseFilter(input string) (FilterExpr, error) {
	p := newParser(input)
	f, err := p.parseFilterExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().Typ != TokEOF {
		return nil, fmt.Errorf("logsql: unexpected token after filter: %q", p.peek().Val)
	}
	if p.scanErr != "" {
		return nil, fmt.Errorf("logsql: %s", p.scanErr)
	}
	return f, nil
}

// ---------------------------------------------------------------------------
// parser internals
// ---------------------------------------------------------------------------

type parser struct {
	sc  *Scanner
	buf *Token // one-token lookahead buffer
	// bufRaw is the source text of the buffered token, quotes included, and bufSpace whether whitespace
	// came before it.
	bufRaw   string
	bufSpace bool
	// scanErr is the first token the scanner could not read (an unterminated string, say): a query holding one
	// is rejected even where a parse path would read the error token as a word.
	scanErr string
}

func newParser(input string) *parser {
	return &parser{sc: NewScanner(input)}
}

// peek returns the next token without consuming it.
func (p *parser) peek() Token {
	if p.buf == nil {
		tok := p.sc.Next()
		if tok.Typ == TokError && p.scanErr == "" {
			p.scanErr = tok.Val
		}
		p.buf = &tok
		p.bufRaw, p.bufSpace = p.sc.Raw(), p.sc.SpaceBefore()
	}
	return *p.buf
}

// advance consumes and returns the next token.
func (p *parser) advance() Token {
	tok := p.peek()
	p.buf = nil
	return tok
}

// expect consumes a token of the given type or returns an error.
func (p *parser) expect(typ TokType) (Token, error) {
	tok := p.advance()
	if tok.Typ != typ {
		return tok, fmt.Errorf("logsql: expected %s, got %q (%s)", typ, tok.Val, tok.Typ)
	}
	return tok, nil
}

// expectIdent consumes an identifier token (or returns an error). A quoted name ("a.b" or `a.b`) is an identifier
// too: its text is returned as written, quotes included.
func (p *parser) expectIdent() (string, error) {
	p.peek()
	raw := p.bufRaw
	tok := p.advance()
	switch tok.Typ {
	case TokIdent:
		return tok.Val, nil
	case TokString, TokRawString:
		return raw, nil
	}
	return "", fmt.Errorf("logsql: expected identifier, got %q (type %d)", tok.Val, tok.Typ)
}

// expectString consumes a string token and returns its unquoted value.
func (p *parser) expectString() (string, error) {
	tok := p.advance()
	if tok.Typ != TokString && tok.Typ != TokRawString {
		return "", fmt.Errorf("logsql: expected string literal, got %q (type %d)", tok.Val, tok.Typ)
	}
	return tok.Val, nil
}

// ---------------------------------------------------------------------------
// Query-level parsing
// ---------------------------------------------------------------------------

func (p *parser) parseQuery() (*Query, error) {
	tok := p.peek()

	// Empty input is an error.
	if tok.Typ == TokEOF {
		return nil, fmt.Errorf("logsql: empty query")
	}

	// A bare pipe at the start is an error.
	if tok.Typ == TokPipe {
		return nil, fmt.Errorf("logsql: query cannot start with pipe")
	}

	// Parse the filter portion (everything before the first |).
	filter, err := p.parseFilterExpr()
	if err != nil {
		return nil, err
	}

	// Check: if filter is a Wildcard and there is a pipe following, handle the
	// special case where "*" was the actual filter (nil stored to produce "*").
	// We keep Wildcard as a valid FilterExpr — Query.String() handles nil→"*".
	// But we want to store nil so that Query.String() is canonical.
	// Actually Query.String() prints Wildcard.String() == "*" directly as well,
	// BUT Query.Filter == nil also prints "*". We use nil only when filter is nil
	// in the builder. Here we set Wildcard explicitly from the parser; that also
	// round-trips correctly since Wildcard.String() == "*". Keep it as is.

	var pipes []Pipe
	for p.peek().Typ == TokPipe {
		p.advance() // consume |
		pipe, err := p.parsePipe()
		if err != nil {
			return nil, err
		}
		pipes = append(pipes, pipe)
	}

	if p.peek().Typ != TokEOF {
		return nil, fmt.Errorf("logsql: unexpected token %q after query", p.peek().Val)
	}

	return &Query{Filter: filter, Pipes: pipes}, nil
}

// ---------------------------------------------------------------------------
// Filter expression parsing  (recursive descent: OR < AND < NOT < primary)
// ---------------------------------------------------------------------------

func (p *parser) parseFilterExpr() (FilterExpr, error) {
	return p.parseOrExpr()
}

func (p *parser) parseOrExpr() (FilterExpr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}
	for p.peek().Typ == TokOr {
		p.advance() // consume OR
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = OrExpr{Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseAndExpr() (FilterExpr, error) {
	// Filters written one after another are an AND too; an AND keyword joins the runs of them.
	var runs []FilterExpr
	var run []FilterExpr
	flush := func() {
		if len(run) == 1 {
			runs = append(runs, run[0])
		} else if len(run) > 1 {
			runs = append(runs, AllOf(run))
		}
		run = nil
	}
	first, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}
	run = append(run, first)
	for {
		explicit := p.peek().Typ == TokAnd
		if explicit {
			p.advance() // consume AND
			flush()
		} else if !p.startsFilter() {
			break
		}
		next, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		run = append(run, next)
	}
	flush()
	left := runs[0]
	for _, r := range runs[1:] {
		left = AndExpr{Left: left, Right: r}
	}
	return left, nil
}

// startsFilter reports whether the next token can begin a filter written right after another; VictoriaLogs wants
// whitespace between the two.
func (p *parser) startsFilter() bool {
	if t := p.peek(); !p.bufSpace || t.Typ == TokEOF {
		return false
	}
	switch p.peek().Typ {
	case TokIdent, TokString, TokRawString, TokLParen, TokLBrace, TokStar, TokEq, TokTilde, TokNot, TokBang, TokReNotMatch:
		return true
	}
	return false
}

func (p *parser) parseNotExpr() (FilterExpr, error) {
	if t := p.peek().Typ; t == TokNot || t == TokBang {
		p.advance()                   // consume NOT or !
		expr, err := p.parseNotExpr() // recurse to support NOT NOT expr
		if err != nil {
			return nil, err
		}
		return NotExpr{Expr: expr}, nil
	}
	return p.parsePrimaryFilter()
}

// isFieldOpTok reports whether tok follows a field name in a field filter.
func isFieldOpTok(t TokType) bool {
	switch t {
	case TokColon, TokColonEq, TokColonTilde, TokColonGT, TokColonGTE, TokColonLT, TokColonLTE:
		return true
	}
	return false
}

// parsePrimaryFilter handles atoms: literals, field filters, stream filters,
// time filters, parenthesised expressions.
func (p *parser) parsePrimaryFilter() (FilterExpr, error) {
	tok := p.peek()

	switch tok.Typ {
	case TokLParen:
		// Parenthesised sub-expression — consume and parse inner.
		p.advance()
		inner, err := p.parseFilterExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return inner, nil

	case TokLBrace:
		// Stream filter: {app="nginx", env="prod"}
		return p.parseStreamFilter()

	case TokStar:
		// Either Wildcard (*) or Substring (*word*)
		p.advance() // consume *
		if next := p.peek(); (next.Typ == TokIdent || next.Typ == TokString) && !p.bufSpace {
			value := p.advance().Val
			// A substring is *word*: the closing * must follow the word with no space.
			if p.peek().Typ == TokStar && !p.bufSpace {
				p.advance() // consume trailing *
				return Substring{Value: value}, nil
			}
			return nil, fmt.Errorf("logsql: unrecognised pattern *%s (missing trailing *?)", value)
		}
		return Wildcard{}, nil

	case TokString, TokRawString:
		// A quoted field name ("a.b":="x") or a phrase: "hello world"
		raw := p.bufRaw
		p.advance()
		if isFieldOpTok(p.peek().Typ) && !p.bufSpace {
			return p.parseFieldOp(raw)
		}
		if p.peek().Typ == TokStar && !p.bufSpace {
			p.advance()
			return Prefix{Value: tok.Val}, nil
		}
		return Phrase{Value: tok.Val}, nil

	case TokEq:
		// Exact message filter: ="value" or exact prefix ="prefix"*
		p.advance() // consume =
		val, err := p.parseStringArg()
		if err != nil {
			return nil, err
		}
		if p.peek().Typ == TokStar && !p.bufSpace {
			p.advance()
			return ExactPrefix{Value: val}, nil
		}
		return Exact{Value: val}, nil

	case TokTilde, TokReNotMatch:
		// Regexp message filter: ~"pattern"
		p.advance() // consume ~
		val, err := p.parseStringArg()
		if err != nil {
			return nil, err
		}
		if tok.Typ == TokReNotMatch {
			return NotExpr{Expr: Regexp{Pattern: val}}, nil
		}
		return Regexp{Pattern: val}, nil

	case TokIdent:
		if strings.HasPrefix(tok.Val, "-") {
			// A leading - negates the filter after it: -level:*, -(a or b).
			p.advance()
			if rest := tok.Val[1:]; rest != "" {
				p.buf = &Token{Typ: TokIdent, Val: rest}
				p.bufRaw, p.bufSpace = rest, false
			}
			inner, err := p.parseNotExpr()
			if err != nil {
				return nil, err
			}
			return Exclude{Expr: inner}, nil
		}
		return p.parseIdentOrFieldFilter()

	default:
		return nil, fmt.Errorf("logsql: unexpected token %q (type %d) in filter", tok.Val, tok.Typ)
	}
}

// parseIdentOrFieldFilter handles bare words and field-filter expressions.
// Called when the current token is TokIdent.
//
//nolint:gocyclo
func (p *parser) parseIdentOrFieldFilter() (FilterExpr, error) {
	tok := p.advance() // consume the ident
	name := tok.Val

	// Special: _time:duration — time filter
	if name == "_time" && p.peek().Typ == TokColon {
		return p.parseTimeFilter()
	}

	// Special function-like message filters.
	if p.peek().Typ == TokLParen && !p.bufSpace {
		if f, ok, err := p.parseFilterFunc(name, ""); ok || err != nil {
			return f, err
		}
	}

	// Check for prefix: word followed directly by * (no colon)
	if p.peek().Typ == TokStar && !p.bufSpace {
		p.advance() // consume *
		return Prefix{Value: name}, nil
	}

	if isFieldOpTok(p.peek().Typ) {
		return p.parseFieldOp(tok.Val)
	}

	// Plain word (no operator follows)
	if err := checkBareWord(name, false); err != nil {
		return nil, err
	}
	return Word{Value: name}, nil
}

// filterFuncs are the function-style filters kept as a FilterFunc: the arguments are read as written.
var filterFuncs = map[string]bool{
	"exact": true, "re": true, "lt_field": true,
	"pattern_match": true, "pattern_match_full": true, "pattern_match_prefix": true, "pattern_match_suffix": true,
}

// parseFilterFunc parses the arguments of a function-style filter name(...) whose ( is the next token, for field
// (empty for the message). ok is false when name is not one.
//
//nolint:gocyclo
func (p *parser) parseFilterFunc(name, field string) (f FilterExpr, ok bool, err error) {
	switch name {
	case "i":
		// Case-insensitive: i("phrase") or i(prefix*)
		p.advance() // consume (
		var val string
		switch p.peek().Typ {
		case TokString, TokRawString, TokIdent:
			val = p.advance().Val
		case TokStar: // i(*): the empty prefix
		default:
			return nil, true, fmt.Errorf("logsql: unexpected token inside i(): %q", p.peek().Val)
		}
		prefix := p.peek().Typ == TokStar
		if prefix {
			p.advance()
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, true, err
		}
		if prefix {
			return AnyCasePrefix{Field: field, Value: val}, true, nil
		}
		return CaseInsensitive{Field: field, Value: val}, true, nil

	case "contains_all", "contains_any", "seq":
		p.advance() // consume (
		parts, err := p.parseStringList()
		if err != nil {
			return nil, true, err
		}
		switch name {
		case "contains_all":
			return ContainsAll{Field: field, Parts: parts}, true, nil
		case "contains_any":
			return ContainsAny{Field: field, Parts: parts}, true, nil
		}
		return Sequence{Field: field, Parts: parts}, true, nil

	case "len_range":
		p.advance() // consume (
		f, err := p.parseLenRange(field)
		return f, true, err

	case "pattern":
		p.advance() // consume (
		f, err := p.parsePattern(field)
		return f, true, err
	}
	if op, isOp := fieldFuncOps[name]; isOp {
		p.advance() // consume (
		val, err := p.consumeUntilRParen()
		if err != nil {
			return nil, true, err
		}
		if err := checkFuncArgs(name, val); err != nil {
			return nil, true, err
		}
		return FieldFilter{Field: field, Op: op, Value: val}, true, nil
	}
	if filterFuncs[name] {
		p.advance() // consume (
		args, err := p.consumeUntilRParen()
		if err != nil {
			return nil, true, err
		}
		if err := checkFuncArgs(name, args); err != nil {
			return nil, true, err
		}
		return FilterFunc{Field: field, Name: name, Args: args}, true, nil
	}
	return nil, false, nil
}

// mustQuote are the words VictoriaLogs rejects unquoted, as a word or at the start of a compound token (in-x).
var mustQuote = map[string]bool{
	"in": true, "contains_all": true, "contains_any": true, "contains_common_case": true, "equals_common_case": true,
}

// checkBareWord rejects the unquoted words LogsQL does not read as a word: one made of glue characters only (a lone
// + or /), and a word that has to be quoted. afterField is whether the word is the value of a field filter.
func checkBareWord(w string, afterField bool) error {
	if !strings.ContainsFunc(w, isWordRune) {
		return fmt.Errorf("logsql: compound token %q must be put into quotes", w)
	}
	end := strings.IndexFunc(w, func(r rune) bool { return !isWordRune(r) })
	if end < 0 {
		end = len(w)
	}
	if first := strings.ToLower(w[:end]); mustQuote[first] || (!afterField && first == "options") {
		return fmt.Errorf("logsql: %q must be put into quotes", w)
	}
	return nil
}

// funcArity is the (min, max) number of arguments of a function filter.
var funcArity = map[string][2]int{
	"eq_field": {1, 1}, "le_field": {1, 1}, "lt_field": {1, 1}, "value_type": {1, 1}, "re": {1, 1}, "exact": {1, 1},
	"pattern_match": {1, 1}, "pattern_match_full": {1, 1}, "pattern_match_prefix": {1, 1}, "pattern_match_suffix": {1, 1},
	"range": {2, 2}, "len_range": {2, 2}, "string_range": {2, 2}, "ipv4_range": {1, 2}, "ipv6_range": {1, 2},
	"in": {0, 1 << 30}, "contains_common_case": {1, 1 << 30}, "equals_common_case": {1, 1 << 30},
	"json_array_contains_any": {1, 1 << 30},
}

// checkFuncArgs rejects the arguments of a function filter VictoriaLogs rejects: each must be one word or quoted
// string, there must be the right number of them, and a range takes numbers, an IP range IP addresses. in(...) may
// hold a query instead, which is not checked here.
func checkFuncArgs(name, raw string) error {
	args, isQuery, err := funcArgTokens(name, raw)
	if err != nil || isQuery {
		return err
	}
	return checkArgValues(name, args)
}

// funcArgTokens reads the arguments of a function filter: one token each, apart by commas. isQuery is set when
// in(...) holds a query, which it checks.
func funcArgTokens(name, raw string) (args []Token, isQuery bool, err error) {
	// Read the tokens, gluing the words and colons written together (an IPv6 address is one compound token).
	sc := NewScanner(raw)
	var toks []Token
	for {
		tok := sc.Next()
		if tok.Typ == TokEOF {
			break
		}
		if tok.Typ == TokPipe && name == "in" {
			// a query
			if _, err := newParser(raw).parseQuery(); err != nil {
				return nil, false, fmt.Errorf("logsql: in: %w", err)
			}
			return nil, true, nil
		}
		if n := len(toks); n > 0 && !sc.SpaceBefore() && (tok.Typ == TokIdent || tok.Typ == TokColon) &&
			(toks[n-1].Typ == TokIdent || toks[n-1].Typ == TokColon) {
			toks[n-1] = Token{Typ: TokIdent, Val: toks[n-1].Val + tok.Val}
			continue
		}
		toks = append(toks, tok)
	}
	for i := 0; i < len(toks); i += 2 {
		switch toks[i].Typ {
		case TokIdent, TokString, TokRawString:
			args = append(args, toks[i])
		default:
			return nil, false, fmt.Errorf("logsql: %s: unexpected %q in the arguments", name, toks[i].Val)
		}
		if i+1 < len(toks) && toks[i+1].Typ != TokComma {
			return nil, false, fmt.Errorf("logsql: %s: expected , between the arguments, got %q", name, toks[i+1].Val)
		}
		if i+2 == len(toks) {
			return nil, false, fmt.Errorf("logsql: %s: missing argument after ,", name)
		}
	}
	return args, false, nil
}

// checkArgValues checks the number and kind of the arguments of a function filter.
func checkArgValues(name string, args []Token) error {
	if n := funcArity[name]; len(args) < n[0] || len(args) > n[1] {
		return fmt.Errorf("logsql: %s: wrong number of arguments (%d)", name, len(args))
	}
	switch name {
	case "len_range":
		for _, a := range args {
			if _, err := strconv.ParseUint(a.Val, 10, 64); err != nil && !strings.EqualFold(strings.TrimPrefix(a.Val, "+"), "inf") {
				return fmt.Errorf("logsql: len_range: %q is not an integer", a.Val)
			}
		}
	case "range":
		for _, a := range args {
			if _, err := strconv.ParseFloat(strings.Map(func(r rune) rune {
				if unicode.IsLetter(r) && r != 'e' && r != 'E' {
					return -1
				}
				return r
			}, strings.ToLower(strings.TrimPrefix(strings.ToLower(a.Val), "+"))), 64); err != nil && !strings.EqualFold(strings.TrimLeft(a.Val, "+-"), "inf") {
				return fmt.Errorf("logsql: %s: %q is not a number", name, a.Val)
			}
		}
	case "ipv4_range", "ipv6_range":
		for _, a := range args {
			if err := checkIP(a.Val, name == "ipv6_range", len(args) == 1); err != nil {
				return fmt.Errorf("logsql: %s: %w", name, err)
			}
		}
	}
	return nil
}

// checkIP checks an address (or, when cidr, a CIDR) of an ipv4_range or ipv6_range filter.
func checkIP(s string, v6, cidr bool) error {
	if cidr {
		if _, _, err := net.ParseCIDR(s); err == nil {
			return nil
		}
	}
	ip := net.ParseIP(s)
	if ip == nil || (ip.To4() != nil) == v6 {
		return fmt.Errorf("%q is not an IPv%d address", s, map[bool]int{false: 4, true: 6}[v6])
	}
	return nil
}

// parseStringList parses the "a", b, ... of contains_all( and the like up to the closing ).
// The opening ( has been consumed.
func (p *parser) parseStringList() ([]string, error) {
	var parts []string
	for {
		switch tok := p.advance(); tok.Typ {
		case TokString, TokRawString, TokIdent:
			parts = append(parts, tok.Val)
		default:
			return nil, fmt.Errorf("logsql: unexpected token %q in argument list", tok.Val)
		}
		switch tok := p.advance(); tok.Typ {
		case TokComma:
		case TokRParen:
			return parts, nil
		default:
			return nil, fmt.Errorf("logsql: expected , or ) in argument list, got %q", tok.Val)
		}
	}
}

// parsePattern parses the "a <*> b") of pattern; the opening ( has been consumed.
func (p *parser) parsePattern(field string) (FilterExpr, error) {
	pattern, err := p.expectString()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return PatternMatch{Field: field, Pattern: pattern}, nil
}

// parseLenRange parses the min, max) of len_range; the opening ( has been consumed.
func (p *parser) parseLenRange(field string) (FilterExpr, error) {
	raw, err := p.consumeUntilRParen()
	if err != nil {
		return nil, err
	}
	if err := checkFuncArgs("len_range", raw); err != nil {
		return nil, err
	}
	raw = strings.ReplaceAll(raw, " ", "")
	parts := strings.SplitN(raw, ",", 2)
	min, max := parts[0], ""
	if len(parts) > 1 {
		max = parts[1]
	}
	return LenRange{Field: field, Min: min, Max: max}, nil
}

// valueRaw consumes a value token (a word, a quoted string or a raw string) and returns its source text.
func (p *parser) valueRaw() (string, error) {
	switch tok := p.peek(); tok.Typ {
	case TokIdent, TokString, TokRawString:
		raw := p.bufRaw
		p.advance()
		return raw, nil
	default:
		return "", fmt.Errorf("logsql: expected a value, got %q (%s)", tok.Val, tok.Typ)
	}
}

// parseFieldOp parses the operator and value of a field filter. field is the field's name as written, quotes
// included; the operator token has not been consumed.
//
//nolint:gocyclo
func (p *parser) parseFieldOp(field string) (FilterExpr, error) {
	if (field == "_stream" || field == `"_stream"`) && p.peek().Typ != TokColon {
		return nil, fmt.Errorf("logsql: _stream takes a stream filter {...}")
	}
	switch tok := p.advance(); tok.Typ {
	case TokColonEq:
		val, err := p.parseStringArg()
		if err != nil {
			return nil, err
		}
		if p.peek().Typ == TokStar && !p.bufSpace {
			p.advance()
			return FieldFilter{Field: field, Op: FieldOpExactPrefix, Value: val}, nil
		}
		return FieldFilter{Field: field, Op: FieldOpExact, Value: val}, nil

	case TokColonTilde:
		val, err := p.parseStringArg()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: field, Op: FieldOpRegexp, Value: val}, nil

	case TokColonGT, TokColonGTE, TokColonLT, TokColonLTE:
		val, err := p.valueRaw()
		if err != nil {
			return nil, err
		}
		op := map[TokType]FieldOp{TokColonGT: FieldOpGT, TokColonGTE: FieldOpGTE, TokColonLT: FieldOpLT, TokColonLTE: FieldOpLTE}[tok.Typ]
		return FieldFilter{Field: field, Op: op, Value: val}, nil

	default: // TokColon: a bare colon starts a word, a phrase, a prefix, a function or a negation
		return p.parseBareCsFilterValue(field)
	}
}

// fieldFuncOps are the field filter functions with the FieldOp they build; the argument is read as written.
var fieldFuncOps = map[string]FieldOp{
	"range":                   FieldOpRange,
	"in":                      FieldOpIn,
	"ipv4_range":              FieldOpIPv4Range,
	"ipv6_range":              FieldOpIPv6Range,
	"eq_field":                FieldOpEqField,
	"le_field":                FieldOpLeField,
	"string_range":            FieldOpStringRange,
	"value_type":              FieldOpValueType,
	"json_array_contains_any": FieldOpJSONArrayContainsAny,
	"contains_common_case":    FieldOpContainsCommonCase,
	"equals_common_case":      FieldOpEqualsCommonCase,
}

// parseBareCsFilterValue parses the value part of a field filter after a bare `:`.
// field has already been consumed; the colon has been consumed.
//
//nolint:gocyclo
func (p *parser) parseBareCsFilterValue(field string) (FilterExpr, error) {
	tok := p.peek()
	if (field == "_stream" || field == `"_stream"`) && tok.Typ != TokLBrace {
		return nil, fmt.Errorf("logsql: _stream takes a stream filter {...}")
	}

	switch tok.Typ {
	case TokLParen:
		// field:(a or b) holds the filters inside for the field
		if p.bufSpace {
			break
		}
		p.advance()
		inner, err := p.parseFilterExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return FieldScope{Field: field, Expr: inner}, nil

	case TokLBrace:
		// _stream:{app="x"} is the stream filter {app="x"}
		if field != "_stream" && field != `"_stream"` {
			break
		}
		return p.parseStreamFilter()

	case TokStar:
		p.advance() // consume *
		// field:*word* — Substring field filter; the word and the closing * follow with no space
		if next := p.peek(); (next.Typ == TokIdent || next.Typ == TokString) && !p.bufSpace {
			word := p.advance().Val
			if p.peek().Typ == TokStar && !p.bufSpace {
				p.advance() // consume trailing *
				return FieldFilter{Field: field, Op: FieldOpSubstring, Value: word}, nil
			}
			return nil, fmt.Errorf("logsql: expected * after %s:*%s for substring filter", field, word)
		}
		// field:* — any value
		return FieldFilter{Field: field, Op: FieldOpAny}, nil

	case TokString, TokRawString:
		// field:"" — empty, field:"value*" a prefix, field:"value" a phrase
		p.advance()
		if p.peek().Typ == TokStar && !p.bufSpace {
			p.advance()
			return FieldFilter{Field: field, Op: FieldOpPrefix, Value: tok.Val}, nil
		}
		if tok.Val == "" {
			return FieldFilter{Field: field, Op: FieldOpEmpty}, nil
		}
		return FieldFilter{Field: field, Op: FieldOpPhrase, Value: tok.Val}, nil

	case TokBang:
		// field:!"value" has a value other than value; any other filter after !: is negated
		p.advance()
		if next := p.peek(); next.Typ == TokString && !p.bufSpace {
			p.advance()
			if p.peek().Typ != TokStar || p.bufSpace {
				return FieldFilter{Field: field, Op: FieldOpNotExact, Value: next.Val}, nil
			}
			p.advance() // the *: a prefix, negated whole
			return NotExpr{Expr: FieldFilter{Field: field, Op: FieldOpPrefix, Value: next.Val}}, nil
		}
		inner, err := p.parseBareCsFilterValue(field)
		if err != nil {
			return nil, err
		}
		return NotExpr{Expr: inner}, nil

	case TokReNotMatch:
		p.advance()
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return NotExpr{Expr: FieldFilter{Field: field, Op: FieldOpRegexp, Value: val}}, nil

	case TokIdent:
		// Could be: range(...), in(...), prefix (ident*), or bare ident value
		ident := p.advance().Val
		if rest, ok := strings.CutPrefix(ident, "-"); ok && rest != "" {
			// field:-word negates the filter after the -, like -field:word
			p.buf, p.bufRaw, p.bufSpace = &Token{Typ: TokIdent, Val: rest}, rest, false
			inner, err := p.parseBareCsFilterValue(field)
			if err != nil {
				return nil, err
			}
			return NotExpr{Expr: inner}, nil
		}
		if p.peek().Typ == TokLParen && !p.bufSpace {
			if f, ok, err := p.parseFilterFunc(ident, field); ok || err != nil {
				return f, err
			}
		}

		// Check if followed by * → Prefix field filter
		if p.peek().Typ == TokStar && !p.bufSpace {
			p.advance() // consume *
			return FieldFilter{Field: field, Op: FieldOpPrefix, Value: ident}, nil
		}

		// A bare word value is a phrase
		if err := checkBareWord(ident, true); err != nil {
			return nil, err
		}
		return FieldFilter{Field: field, Op: FieldOpPhrase, Value: ident}, nil
	}

	return nil, fmt.Errorf("logsql: unexpected token %q after field %q", tok.Val, field)
}

// parseTimeFilter parses _time:... after `_time` has been consumed.
func (p *parser) parseTimeFilter() (FilterExpr, error) {
	p.advance() // consume :

	// The range is read as written, up to the whitespace, ) or | that ends it: a duration (5m), a date or timestamp
	// (2026-11-07T00:10:00Z), or a bracketed range ([a, b)) that may hold spaces.
	p.buf = nil
	rest := strings.TrimLeft(p.sc.Remaining(), " \t")
	skipped := len(p.sc.Remaining()) - len(rest)
	end := 0
	if kind, _, ok := strings.Cut(rest, "["); ok && (kind == "day_range" || kind == "week_range") {
		p.sc.Skip(skipped + len(kind))
		return p.parseBracketRange(kind)
	}
	if rest != "" && (rest[0] == '[' || rest[0] == '(') {
		i := strings.IndexAny(rest, "])")
		if i < 0 {
			return nil, fmt.Errorf("logsql: unclosed %c in time range", rest[0])
		}
		end = i + 1
	} else {
		for end < len(rest) && !strings.ContainsRune(" \t\r\n)|", rune(rest[end])) {
			end++
		}
	}
	if end == 0 {
		return nil, fmt.Errorf("logsql: missing the time range after the colon of _time")
	}
	rng := rest[:end]
	if err := checkTimeRange(rng); err != nil {
		return nil, err
	}
	p.sc.Skip(skipped + end)

	switch rng {
	case "day_range", "week_range":
		return p.parseBracketRange(rng)
	}
	// offset follows the range: _time:5m offset 1h
	if p.isKeyword("offset") {
		p.advance()
		off, err := p.valueRaw()
		if err != nil || !durationRe.MatchString(strings.TrimLeft(off, "+-")) {
			return nil, fmt.Errorf("logsql: expected a duration after offset, got %q", off)
		}
		rng += " offset " + off
	}
	return TimeFilter{Range: rng}, nil
}

var (
	durationRe = regexp.MustCompile(`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h|d|w|y))+$`)
	dateRe     = regexp.MustCompile(`^[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}(:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?)?(Z|[+-][0-9]{2}:[0-9]{2}))?)?)?$`)
)

// checkTimeRange rejects what is not a _time: range: a duration or date, or a bracketed pair of times (each
// now, now-1h, a duration or a date), or a day_range or week_range.
func checkTimeRange(rng string) error {
	isTime := func(t string) bool {
		t = strings.TrimSpace(t)
		if rest, ok := strings.CutPrefix(t, "now"); ok {
			return rest == "" || ((rest[0] == '-' || rest[0] == '+') && durationRe.MatchString(rest[1:]))
		}
		return durationRe.MatchString(strings.TrimPrefix(t, "-")) || dateRe.MatchString(t) || isUnixTime(t)
	}
	if strings.HasPrefix(rng, "[") || strings.HasPrefix(rng, "(") {
		a, b, ok := strings.Cut(rng[1:len(rng)-1], ",")
		if !ok || !isTime(a) || !isTime(b) {
			return fmt.Errorf("logsql: bad time range %q", rng)
		}
		return nil
	}
	if rng == "day_range" || rng == "week_range" || isTime(rng) {
		return nil
	}
	return fmt.Errorf("logsql: bad time range %q", rng)
}

func isUnixTime(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

var (
	clockRe   = regexp.MustCompile(`^[0-9]{2}:[0-9]{2}(:[0-9]{2})?$`)
	weekdayRe = regexp.MustCompile(`^(?i:mon|tue|wed|thu|fri|sat|sun)[a-z]*$`)
)

// checkDayWeekRange checks the [a, b] of a day_range (clock times) or week_range (weekday names).
func checkDayWeekRange(kind, bracket string) error {
	a, b, ok := strings.Cut(bracket[1:len(bracket)-1], ",")
	if !ok {
		return fmt.Errorf("logsql: bad %s %s", kind, bracket)
	}
	re := clockRe
	if kind == "week_range" {
		re = weekdayRe
	}
	if !re.MatchString(strings.TrimSpace(a)) || !re.MatchString(strings.TrimSpace(b)) {
		return fmt.Errorf("logsql: bad %s %s", kind, bracket)
	}
	return nil
}

// parseBracketRange parses [start, end] [offset dur] after day_range or week_range.
func (p *parser) parseBracketRange(kind string) (FilterExpr, error) {
	p.buf = nil
	rest := strings.TrimLeft(p.sc.Remaining(), " \t")
	if !strings.HasPrefix(rest, "[") {
		return nil, fmt.Errorf("logsql: expected [ after %s", kind)
	}
	end := strings.IndexByte(rest, ']')
	if end < 0 {
		return nil, fmt.Errorf("logsql: unclosed [ after %s", kind)
	}
	bracket := rest[:end+1]
	if err := checkDayWeekRange(kind, bracket); err != nil {
		return nil, err
	}
	p.sc.Skip(len(p.sc.Remaining()) - len(rest) + end + 1) // advance past ]

	var offset string
	if p.isKeyword("offset") {
		p.advance()
		if t := p.peek(); t.Typ != TokIdent || !durationRe.MatchString(strings.TrimLeft(t.Val, "+-")) {
			return nil, fmt.Errorf("logsql: expected a duration after offset, got %q", t.Val)
		}
		offset = p.advance().Val
	}
	if kind == "day_range" {
		return DayRange{Bracket: bracket, Offset: offset}, nil
	}
	return WeekRange{Bracket: bracket, Offset: offset}, nil
}

// parseStreamFilter parses {label=value, ...} stream selectors.
// The opening { has NOT been consumed yet.
func (p *parser) parseStreamFilter() (FilterExpr, error) {
	p.advance() // consume {

	var matchers []LabelMatcher

	for {
		tok := p.peek()
		if tok.Typ == TokRBrace {
			p.advance()
			break
		}
		if tok.Typ == TokEOF {
			return nil, fmt.Errorf("logsql: unclosed stream filter {")
		}

		// Parse label name
		name, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: expected label name in stream filter: %w", err)
		}

		// Parse operator: =, !=, =~, !~
		opTok := p.advance()
		var op string
		switch opTok.Typ {
		case TokEq:
			op = "="
		case TokNeq:
			op = "!="
		case TokReMatch:
			op = "=~"
		case TokReNotMatch:
			op = "!~"
		default:
			return nil, fmt.Errorf("logsql: expected label operator in stream filter, got %q", opTok.Val)
		}

		// Parse value: must be a quoted string
		val, err := p.expectString()
		if err != nil {
			return nil, fmt.Errorf("logsql: expected label value string in stream filter: %w", err)
		}

		matchers = append(matchers, LabelMatcher{Name: name, Op: op, Value: val})

		// Optional comma
		if p.peek().Typ == TokComma {
			p.advance()
		}
	}

	return StreamFilter{Matchers: matchers}, nil
}

// consumeUntilRParen consumes tokens up to the ) that closes the opening ( already consumed and returns the text
// between them as written, trimmed.
func (p *parser) consumeUntilRParen() (string, error) {
	p.buf = nil
	start := p.sc.pos
	depth := 1
	for {
		switch tok := p.advance(); tok.Typ {
		case TokLParen:
			depth++
		case TokRParen:
			depth--
			if depth == 0 {
				return strings.TrimSpace(withoutComments(p.sc.src[start:p.sc.start])), nil
			}
		case TokEOF, TokError:
			return "", fmt.Errorf("logsql: unclosed parenthesis")
		}
	}
}

// withoutComments is raw source text without its `#` comments, which run to the end of the line outside quoted
// strings: what the scanner reads, so text cut from the source prints back to the same query.
func withoutComments(raw string) string {
	if !strings.Contains(raw, "#") {
		return raw
	}
	var b strings.Builder
	var quote byte
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case quote != 0:
			b.WriteByte(c)
			if c == '\\' && quote != '`' && i+1 < len(raw) {
				i++
				b.WriteByte(raw[i])
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'' || c == '`':
			quote = c
			b.WriteByte(c)
		case c == '#':
			for i < len(raw) && raw[i] != '\n' {
				i++
			}
			if i < len(raw) {
				b.WriteByte('\n')
			}
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Pipe parsing
// ---------------------------------------------------------------------------

//nolint:gocyclo
func (p *parser) parsePipe() (Pipe, error) {
	tok := p.peek()
	if tok.Typ != TokIdent {
		return nil, fmt.Errorf("logsql: expected pipe name after |, got %q (type %d)", tok.Val, tok.Typ)
	}
	name := strings.ToLower(p.advance().Val) // pipe names are read in any case

	switch name {
	case "unpack_json":
		return p.parsePipeUnpack(true)
	case "unpack_logfmt":
		return p.parsePipeUnpack(false)
	case "extract":
		return p.parsePipeExtract(false)
	case "extract_regexp":
		return p.parsePipeExtract(true)
	case "format":
		return p.parsePipeFormat()
	case "rename", "mv":
		return p.parsePipeRename()
	case "replace":
		return p.parsePipeReplace(false)
	case "replace_regexp":
		return p.parsePipeReplace(true)
	case "pack_json", "pack_logfmt":
		return p.parsePipePack(name)
	case "filter":
		expr, err := p.parseFilterExpr()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe filter: %w", err)
		}
		return PipeFilter{Expr: expr}, nil
	case "fields":
		labels, err := p.parseCommaSeparatedIdents()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe fields: %w", err)
		}
		return PipeFields{Labels: labels}, nil
	case "delete":
		labels, err := p.parseCommaSeparatedIdents()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe delete: %w", err)
		}
		return PipeDelete{Labels: labels}, nil
	case "limit":
		n, err := p.parseInt()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe limit: %w", err)
		}
		return PipeLimit{N: n}, nil
	case "sort":
		return p.parsePipeSort()
	case "stats":
		return p.parsePipeStats()
	case "math":
		return p.parsePipeMath()
	case "top":
		return p.parsePipeTopN()
	case "first":
		return p.parsePipeFirstLast(false)
	case "last":
		return p.parsePipeFirstLast(true)
	case "sample":
		n, err := p.parseInt()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe sample: %w", err)
		}
		if n < 1 {
			return nil, fmt.Errorf("logsql: pipe sample: the rate must be at least 1")
		}
		return PipeSample{N: n}, nil
	case "offset":
		n, err := p.parseInt()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe offset: %w", err)
		}
		return PipeOffset{N: n}, nil
	case "uniq":
		return p.parsePipeUniq()
	case "field_names":
		return PipeFieldNames{}, nil
	case "drop_empty_fields":
		return PipeDropEmptyFields{}, nil
	case "copy":
		return p.parsePipeCopy()
	case "coalesce":
		return p.parsePipeCoalesce()
	case "collapse_nums":
		return p.parsePipeCollapseNums()
	case "decolorize":
		return p.parsePipeDecolorize()
	case "facets":
		return p.parsePipeFacets()
	case "field_values":
		return p.parsePipeFieldValues()
	case "generate_sequence":
		return p.parsePipeGenerateSequence()
	case "hash":
		return p.parsePipeHash()
	case "join":
		return p.parsePipeJoin()
	case "json_array_concat":
		return p.parsePipeJSONArrayConcat()
	case "json_array_len":
		return p.parsePipeJSONArrayLen()
	case "len":
		return p.parsePipeLen()
	case "query_stats":
		return PipeQueryStats{}, nil
	case "running_stats":
		return p.parsePipeStatsLike("running_stats")
	case "set_stream_fields":
		labels, err := p.parseCommaSeparatedIdents()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe set_stream_fields: %w", err)
		}
		return PipeSetStreamFields{Fields: labels}, nil
	case "split":
		return p.parsePipeSplit()
	case "stream_context":
		return p.parsePipeStreamContext()
	case "time_add":
		dur, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe time_add: %w", err)
		}
		return PipeTimeAdd{Duration: dur}, nil
	case "total_stats":
		return p.parsePipeStatsLike("total_stats")
	case "union":
		inner, err := p.consumeInnerQuery()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe union: %w", err)
		}
		if _, err := newParser(inner).parseQuery(); err != nil {
			return nil, fmt.Errorf("logsql: pipe union: %w", err)
		}
		return PipeUnion{Inner: inner}, nil
	case "unpack_syslog":
		return PipeUnpackSyslog{}, nil
	case "unpack_words":
		return p.parsePipeUnpackWords()
	case "unroll":
		return p.parsePipeUnroll()
	case "update":
		return p.parsePipeUpdate()
	default:
		// A stats function written as a pipe is the stats pipe: | count() as rows
		if statsFuncNames[name] && strings.HasPrefix(p.sc.Remaining(), "(") {
			p.sc.pos -= len(name)
			return p.parsePipeStats()
		}
		return nil, fmt.Errorf("logsql: unknown pipe %q", name)
	}
}

// statsFuncNames are the stats functions parseStatsFunc reads.
var statsFuncNames = map[string]bool{
	"count": true, "sum": true, "min": true, "max": true, "avg": true, "median": true, "quantile": true, "stddev": true,
	"stdvar": true, "rate": true, "rate_sum": true, "count_uniq": true, "count_uniq_hash": true, "uniq_values": true,
	"values": true, "field_max": true, "field_min": true, "json_values": true, "json_values_sorted": true,
	"json_values_topk": true, "any": true, "count_empty": true, "sum_len": true, "histogram": true, "last": true,
	"first": true, "row_any": true, "row_min": true, "row_max": true,
}

// isKeyword reports whether the next token is the bare keyword kw, in any case.
func (p *parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.Typ == TokIdent && strings.EqualFold(t.Val, kw)
}

// atPipeEnd reports whether the next token ends the pipe: the next |, the closing ) of an enclosing group, or the end.
func (p *parser) atPipeEnd() bool {
	switch p.peek().Typ {
	case TokEOF, TokPipe, TokRParen:
		return true
	}
	return false
}

// expectAlias consumes a result name. An unquoted one may hold colons (a:b), as a compound token does.
func (p *parser) expectAlias() (string, error) {
	alias, err := p.expectIdent()
	if err != nil || strings.HasPrefix(alias, `"`) || strings.HasPrefix(alias, "`") {
		return alias, err
	}
	for p.peek().Typ == TokColon && !p.bufSpace {
		p.advance()
		if t := p.peek(); t.Typ != TokIdent || p.bufSpace {
			return "", fmt.Errorf("logsql: expected a name after : in %q", alias)
		}
		alias += ":" + p.advance().Val
	}
	return alias, nil
}

// isBucketSize reports whether s is a bucket size or offset: a number with an optional unit (60s, 1.5KiB, 5), or
// month or year.
func isBucketSize(s string) bool {
	if s == "month" || s == "year" {
		return true
	}
	return s != "" && s[0] >= '0' && s[0] <= '9'
}

// hasPlaceholder reports whether an extract pattern names a field to extract: a <name> other than <_> and <*>.
func hasPlaceholder(pattern string) bool {
	for {
		i := strings.IndexByte(pattern, '<')
		if i < 0 {
			return false
		}
		j := strings.IndexByte(pattern[i:], '>')
		if j < 0 {
			return false
		}
		name := pattern[i+1 : i+j]
		if k := strings.IndexByte(name, ':'); k >= 0 {
			name = name[k+1:]
		}
		if name != "" && name != "_" && name != "*" {
			return true
		}
		pattern = pattern[i+j+1:]
	}
}

// parseIf parses an optional if (cond) argument of a pipe.
func (p *parser) parseIf() (FilterExpr, error) {
	if !p.isKeyword("if") {
		return nil, nil
	}
	p.advance()
	if _, err := p.expect(TokLParen); err != nil {
		return nil, fmt.Errorf("logsql: if: %w", err)
	}
	if p.peek().Typ == TokRParen {
		p.advance()
		return DeferredExpr{}, nil // if () holds for every row
	}
	cond, err := p.parseFilterExpr()
	if err != nil {
		return nil, fmt.Errorf("logsql: if: %w", err)
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, fmt.Errorf("logsql: if: %w", err)
	}
	return cond, nil
}

// expectFieldName consumes a field name or field-name prefix (foo*) and returns it as written.
func (p *parser) expectFieldName() (string, error) {
	name, err := p.expectIdent()
	if err != nil {
		return "", err
	}
	if p.peek().Typ == TokStar && !p.bufSpace && !strings.HasPrefix(name, `"`) && !strings.HasPrefix(name, "`") {
		p.advance()
		name += "*"
	}
	return name, nil
}

// parseFieldList parses (a, b, "c.d", prefix*), the opening ( included.
func (p *parser) parseFieldList() ([]string, error) {
	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	var fields []string
	for p.peek().Typ != TokRParen {
		f, err := p.expectFieldName()
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
		if p.peek().Typ == TokComma {
			p.advance()
		} else if p.peek().Typ != TokRParen {
			return nil, fmt.Errorf("logsql: expected , or ) in field list, got %q", p.peek().Val)
		}
	}
	p.advance() // consume )
	return fields, nil
}

// parseStringArg consumes a string argument: a quoted or backquoted string, or a word.
func (p *parser) parseStringArg() (string, error) {
	switch tok := p.advance(); tok.Typ {
	case TokString, TokRawString, TokIdent:
		return tok.Val, nil
	default:
		return "", fmt.Errorf("logsql: expected a string, got %q (%s)", tok.Val, tok.Typ)
	}
}

// parseKeepSkip parses the optional keep_original_fields or skip_empty_results flag.
func (p *parser) parseKeepSkip() (keep, skip bool) {
	switch {
	case p.isKeyword("keep_original_fields"):
		p.advance()
		keep = true
	case p.isKeyword("skip_empty_results"):
		p.advance()
		skip = true
	}
	return keep, skip
}

// parsePipeUnpack parses unpack_json or unpack_logfmt after the name:
// [if (cond)] [[from] f] [fields (a, b)] [preserve_keys (k)] [result_prefix "p"] [keep_original_fields | skip_empty_results]
func (p *parser) parsePipeUnpack(isJSON bool) (Pipe, error) {
	name := "unpack_logfmt"
	if isJSON {
		name = "unpack_json"
	}
	fail := func(err error) (Pipe, error) { return nil, fmt.Errorf("logsql: pipe %s: %w", name, err) }
	cond, err := p.parseIf()
	if err != nil {
		return fail(err)
	}
	var from string
	if !p.atPipeEnd() && !p.isKeyword("fields") && !p.isKeyword("preserve_keys") && !p.isKeyword("result_prefix") &&
		!p.isKeyword("keep_original_fields") && !p.isKeyword("skip_empty_results") {
		if p.isKeyword("from") {
			p.advance()
		}
		if from, err = p.expectIdent(); err != nil {
			return fail(err)
		}
	}
	var fields, preserve []string
	if p.isKeyword("fields") {
		p.advance()
		if fields, err = p.parseFieldList(); err != nil {
			return fail(err)
		}
	}
	if isJSON && p.isKeyword("preserve_keys") {
		p.advance()
		if preserve, err = p.parseFieldList(); err != nil {
			return fail(err)
		}
	}
	var prefix string
	if p.isKeyword("result_prefix") {
		p.advance()
		if prefix, err = p.parseStringArg(); err != nil {
			return fail(err)
		}
	}
	keep, skip := p.parseKeepSkip()
	if isJSON {
		return PipeUnpackJSON{From: from, If: cond, Fields: fields, PreserveKeys: preserve, ResultPrefix: prefix, KeepOriginalFields: keep, SkipEmptyResults: skip}, nil
	}
	return PipeUnpackLogfmt{From: from, If: cond, Fields: fields, ResultPrefix: prefix, KeepOriginalFields: keep, SkipEmptyResults: skip}, nil
}

// parsePipeExtract parses extract or extract_regexp after the name:
// [if (cond)] "pattern" [from f] [keep_original_fields | skip_empty_results]
func (p *parser) parsePipeExtract(isRegexp bool) (Pipe, error) {
	name := "extract"
	if isRegexp {
		name = "extract_regexp"
	}
	fail := func(err error) (Pipe, error) { return nil, fmt.Errorf("logsql: pipe %s: %w", name, err) }
	cond, err := p.parseIf()
	if err != nil {
		return fail(err)
	}
	pattern, err := p.parseStringArg()
	if err != nil {
		return fail(err)
	}
	if !isRegexp && !hasPlaceholder(pattern) {
		return fail(fmt.Errorf("pattern %q needs a named field like <name>", pattern))
	}
	var from string
	if p.isKeyword("from") {
		p.advance()
		if from, err = p.expectIdent(); err != nil {
			return fail(err)
		}
	}
	keep, skip := p.parseKeepSkip()
	if isRegexp {
		return PipeExtractRegexp{Pattern: pattern, From: from, If: cond, KeepOriginalFields: keep, SkipEmptyResults: skip}, nil
	}
	return PipeExtract{Pattern: pattern, From: from, If: cond, KeepOriginalFields: keep, SkipEmptyResults: skip}, nil
}

// parsePipeFormat parses format after the name: [if (cond)] "template" [as f] [keep_original_fields | skip_empty_results]
func (p *parser) parsePipeFormat() (Pipe, error) {
	fail := func(err error) (Pipe, error) { return nil, fmt.Errorf("logsql: pipe format: %w", err) }
	cond, err := p.parseIf()
	if err != nil {
		return fail(err)
	}
	tmpl, err := p.parseStringArg()
	if err != nil {
		return fail(err)
	}
	var result string
	if p.isKeyword("as") {
		p.advance()
		if result, err = p.expectIdent(); err != nil {
			return fail(err)
		}
	}
	keep, skip := p.parseKeepSkip()
	return PipeFormat{Template: tmpl, ResultField: result, If: cond, KeepOriginalFields: keep, SkipEmptyResults: skip}, nil
}

// parseNamePairs parses src [as] dst, ... up to the end of the pipe, as rename and copy write them.
func (p *parser) parseNamePairs(pipe string) ([][2]string, error) {
	var pairs [][2]string
	for {
		src, err := p.expectFieldName()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe %s: %w", pipe, err)
		}
		if p.isKeyword("as") {
			p.advance()
		}
		dst, err := p.expectFieldName()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe %s: %w", pipe, err)
		}
		pairs = append(pairs, [2]string{src, dst})
		if p.peek().Typ != TokComma {
			return pairs, nil
		}
		p.advance()
	}
}

// parsePipeRename parses: rename src as dst [, src2 as dst2 ...] (mv is the same pipe)
func (p *parser) parsePipeRename() (Pipe, error) {
	pairs, err := p.parseNamePairs("rename")
	if err != nil {
		return nil, err
	}
	return PipeRename{Pairs: pairs}, nil
}

// parsePipeReplace parses replace or replace_regexp after the name: [if (cond)] ("old", "new") [at f] [limit N]
func (p *parser) parsePipeReplace(isRegexp bool) (Pipe, error) {
	name := "replace"
	if isRegexp {
		name = "replace_regexp"
	}
	fail := func(err error) (Pipe, error) { return nil, fmt.Errorf("logsql: pipe %s: %w", name, err) }
	cond, err := p.parseIf()
	if err != nil {
		return fail(err)
	}
	if _, err := p.expect(TokLParen); err != nil {
		return fail(err)
	}
	old, err := p.parseStringArg()
	if err != nil {
		return fail(err)
	}
	if _, err := p.expect(TokComma); err != nil {
		return fail(err)
	}
	repl, err := p.parseStringArg()
	if err != nil {
		return fail(err)
	}
	if _, err := p.expect(TokRParen); err != nil {
		return fail(err)
	}
	var field string
	if p.isKeyword("at") {
		p.advance()
		if field, err = p.expectIdent(); err != nil {
			return fail(err)
		}
	}
	var limit int
	if p.isKeyword("limit") {
		p.advance()
		if limit, err = p.parseInt(); err != nil {
			return fail(err)
		}
	}
	if isRegexp {
		return PipeReplaceRegexp{Field: field, Regex: old, Replacement: repl, If: cond, Limit: limit}, nil
	}
	return PipeReplace{Field: field, Old: old, New: repl, If: cond, Limit: limit}, nil
}

// parsePipePack parses pack_json or pack_logfmt after the name: [fields (a, b)] [as] [f]
func (p *parser) parsePipePack(name string) (Pipe, error) {
	fail := func(err error) (Pipe, error) { return nil, fmt.Errorf("logsql: pipe %s: %w", name, err) }
	var fields []string
	if p.isKeyword("fields") {
		p.advance()
		var err error
		if fields, err = p.parseFieldList(); err != nil {
			return fail(err)
		}
	}
	if p.isKeyword("as") {
		p.advance()
	}
	var result string
	if !p.atPipeEnd() {
		var err error
		if result, err = p.expectIdent(); err != nil {
			return fail(err)
		}
	}
	if name == "pack_json" {
		return PipePackJSON{Fields: fields, ResultField: result}, nil
	}
	return PipePackLogfmt{Fields: fields, ResultField: result}, nil
}

// parseCommaSeparatedIdents parses f1, f2, f3 until EOF or pipe.
func (p *parser) parseCommaSeparatedIdents() ([]string, error) {
	var fields []string
	for {
		ident, err := p.expectFieldName()
		if err != nil {
			return nil, err
		}
		fields = append(fields, ident)
		if p.peek().Typ != TokComma {
			return fields, nil
		}
		p.advance()
	}
}

// parseInt parses a decimal integer from the next TokIdent.
func (p *parser) parseInt() (int, error) {
	tok, err := p.expect(TokIdent)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(tok.Val)
	if err != nil {
		return 0, fmt.Errorf("logsql: expected integer, got %q: %w", tok.Val, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("logsql: expected a non-negative integer, got %d", n)
	}
	return n, nil
}

// parsePipeSort parses: sort by (field [desc], ...) [limit N]
func (p *parser) parsePipeSort() (Pipe, error) {
	// expect "by"
	byTok, err := p.expect(TokIdent)
	if err != nil || byTok.Val != "by" {
		return nil, fmt.Errorf("logsql: expected 'by' after sort")
	}
	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	var fields []SortField
	for p.peek().Typ != TokRParen && p.peek().Typ != TokEOF {
		ident, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		desc := false
		if p.peek().Typ == TokIdent && p.peek().Val == "desc" {
			p.advance()
			desc = true
		}
		fields = append(fields, SortField{Field: ident, Desc: desc})
		if p.peek().Typ == TokComma {
			p.advance()
		}
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	limit := 0
	if p.peek().Typ == TokIdent && p.peek().Val == "limit" {
		p.advance()
		limit, err = p.parseInt()
		if err != nil {
			return nil, err
		}
	}
	return PipeSort{By: fields, Limit: limit}, nil
}

// parsePipeStats parses: stats [by (f1, f2)] func(...) as alias [, ...]
func (p *parser) parsePipeStats() (Pipe, error) {
	by, funcs, err := p.parseStatsBody("stats")
	if err != nil {
		return nil, err
	}
	return PipeStats{By: by, Funcs: funcs}, nil
}

// parsePipeStatsLike parses running_stats or total_stats with the same syntax as stats.
func (p *parser) parsePipeStatsLike(keyword string) (Pipe, error) {
	by, funcs, err := p.parseStatsBody(keyword)
	if err != nil {
		return nil, err
	}
	if keyword == "running_stats" {
		return PipeRunningStats{By: by, Funcs: funcs}, nil
	}
	return PipeTotalStats{By: by, Funcs: funcs}, nil
}

// parseStatsBody parses the shared [by (...)] func() as alias [, ...] body.
// parseStatsBy parses the (a, b, _time:5m offset 1h) after "by"; a field may be followed by a bucket and its offset.
func (p *parser) parseStatsBy(keyword string) ([]GroupKey, error) {
	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	var byKeys []GroupKey
	for p.peek().Typ != TokRParen && p.peek().Typ != TokEOF {
		ident, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		key := GroupKey{Field: ident}
		if p.peek().Typ == TokColon && !p.bufSpace {
			p.advance()
			if key.Bucket, err = p.valueRaw(); err != nil || !isBucketSize(key.Bucket) {
				return nil, fmt.Errorf("logsql: %s: bad bucket %q of %s", keyword, key.Bucket, ident)
			}
			if p.isKeyword("offset") {
				p.advance()
				if key.Offset, err = p.valueRaw(); err != nil || !isBucketSize(strings.TrimLeft(key.Offset, "+-")) {
					return nil, fmt.Errorf("logsql: %s: bad bucket offset %q of %s", keyword, key.Offset, ident)
				}
			}
		}
		byKeys = append(byKeys, key)
		if p.peek().Typ == TokComma {
			p.advance()
		}
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return byKeys, nil
}

func (p *parser) parseStatsBody(keyword string) ([]GroupKey, []StatsFuncAlias, error) {
	var byKeys []GroupKey
	if p.peek().Typ == TokIdent && p.peek().Val == "by" {
		p.advance()
		var err error
		if byKeys, err = p.parseStatsBy(keyword); err != nil {
			return nil, nil, err
		}
	}

	var funcs []StatsFuncAlias
	for {
		tok := p.peek()
		if tok.Typ == TokEOF || tok.Typ == TokPipe || tok.Typ != TokIdent {
			break
		}
		fn, err := p.parseStatsFunc()
		if err != nil {
			return nil, nil, err
		}
		cond, err := p.parseIf()
		if err != nil {
			return nil, nil, fmt.Errorf("logsql: %s: %w", keyword, err)
		}
		// The alias is optional, and "as" before it too.
		var alias string
		if p.isKeyword("as") {
			p.advance()
			if alias, err = p.expectAlias(); err != nil {
				return nil, nil, err
			}
		} else if t := p.peek().Typ; t == TokIdent || t == TokString || t == TokRawString {
			if alias, err = p.expectAlias(); err != nil {
				return nil, nil, err
			}
		}
		funcs = append(funcs, StatsFuncAlias{Func: fn, Alias: alias, If: cond})
		if p.peek().Typ == TokComma {
			p.advance()
		} else {
			break
		}
	}
	if len(funcs) == 0 {
		return nil, nil, fmt.Errorf("logsql: %s requires at least one function", keyword)
	}
	for _, k := range byKeys {
		for _, f := range funcs {
			if f.Alias != "" && f.Alias == k.Field {
				return nil, nil, fmt.Errorf("logsql: %s: the result name %q is also a by field", keyword, f.Alias)
			}
		}
	}
	return byKeys, funcs, nil
}

// parseStatsFunc parses a single stats function like count(), sum(field), etc.
//
//nolint:gocyclo
func (p *parser) parseStatsFunc() (StatsFunc, error) {
	name := strings.ToLower(p.advance().Val) // consume function name ident; names are read in any case

	// row_any and row_max take variadic ident arguments — handle before generic path.
	if name == "row_any" {
		if p.peek().Typ != TokLParen {
			return nil, fmt.Errorf("logsql: expected ( after row_any")
		}
		p.advance() // consume (
		var fields []string
		for p.peek().Typ == TokIdent {
			fields = append(fields, p.advance().Val)
			if p.peek().Typ == TokComma {
				p.advance()
			}
		}
		if p.peek().Typ == TokRParen {
			p.advance()
		}
		return RowAny{Fields: fields}, nil
	}
	if name == "row_max" || name == "row_min" {
		if p.peek().Typ != TokLParen {
			return nil, fmt.Errorf("logsql: expected ( after %s", name)
		}
		p.advance() // consume (
		by := p.advance().Val
		if p.peek().Typ == TokComma {
			p.advance()
		}
		var fields []string
		for p.peek().Typ == TokIdent {
			fields = append(fields, p.advance().Val)
			if p.peek().Typ == TokComma {
				p.advance()
			}
		}
		if p.peek().Typ == TokRParen {
			p.advance()
		}
		if name == "row_min" {
			return RowMin{By: by, Fields: fields}, nil
		}
		return RowMax{By: by, Fields: fields}, nil
	}

	// quantile takes (phi, field) — phi is a float, field is an ident.
	if name == "quantile" {
		if _, err := p.expect(TokLParen); err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
		}
		phiTok := p.advance() // phi as ident/number token
		phi, err := strconv.ParseFloat(phiTok.Val, 64)
		if err != nil || phi < 0 || phi > 1 {
			return nil, fmt.Errorf("logsql: stats func %q: phi must be a number from 0 to 1, got %q", name, phiTok.Val)
		}
		if p.peek().Typ == TokComma {
			p.advance()
		}
		fieldTok := p.advance()
		field := fieldTok.Val
		if _, err := p.expect(TokRParen); err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
		}
		return Quantile{Phi: phi, Field: field}, nil
	}

	// json_values_topk takes (field, limit).
	if name == "json_values_topk" {
		if _, err := p.expect(TokLParen); err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
		}
		field, err := p.expectIdent() // a quoted name keeps its quotes, as in the other stats functions
		if err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
		}
		if p.peek().Typ == TokComma {
			p.advance()
		}
		limitTok := p.advance()
		limit, _ := strconv.Atoi(limitTok.Val)
		if _, err := p.expect(TokRParen); err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
		}
		return JSONValuesTopK{Field: field, Limit: limit}, nil
	}

	// uniq_values and values take (field, limit).
	if name == "uniq_values" || name == "values" {
		if _, err := p.expect(TokLParen); err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
		}
		field, err := p.expectIdent() // a quoted name keeps its quotes, as in the other stats functions
		if err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
		}
		if p.peek().Typ == TokComma {
			p.advance()
		}
		limitTok := p.advance()
		limit, err := strconv.Atoi(limitTok.Val)
		if err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: expected a limit, got %q", name, limitTok.Val)
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
		}
		if name == "uniq_values" {
			return UniqValues{Field: field, Limit: limit}, nil
		}
		return Values{Field: field, Limit: limit}, nil
	}

	// Generic single-field argument parsing.

	// Consume opening paren
	if _, err := p.expect(TokLParen); err != nil {
		return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
	}

	// Parse the field arguments: none, * (every field) or names.
	var args []string
	for p.peek().Typ != TokRParen {
		if p.peek().Typ == TokStar {
			p.advance()
			if name != "count" {
				return nil, fmt.Errorf("logsql: stats func %q: * is not modelled", name)
			}
		} else {
			a, err := p.expectFieldName()
			if err != nil {
				return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
			}
			args = append(args, a)
		}
		if p.peek().Typ == TokComma {
			p.advance()
		} else if p.peek().Typ != TokRParen {
			return nil, fmt.Errorf("logsql: stats func %q: expected , or ), got %q", name, p.peek().Val)
		}
	}
	p.advance() // consume )
	if name == "count" {
		return Count{Fields: args}, nil
	}
	if (name == "field_max" || name == "field_min") && len(args) < 2 {
		return nil, fmt.Errorf("logsql: stats func %q: needs the field to compare and the fields to return", name)
	}
	if (name == "count_uniq" || name == "count_uniq_hash" || name == "field_max" || name == "field_min") && len(args) > 1 {
		// the unique combinations of several fields: the list is kept as the field
		args = []string{strings.Join(args, ", ")}
	}
	if len(args) > 1 || (len(args) == 0 && name != "rate") {
		return nil, fmt.Errorf("logsql: stats func %q: needs exactly one field (%d given)", name, len(args))
	}
	var field string
	if len(args) == 1 {
		field = args[0]
	}

	switch name {
	case "sum":
		return Sum{Field: field}, nil
	case "min":
		return Min{Field: field}, nil
	case "max":
		return Max{Field: field}, nil
	case "avg":
		return Avg{Field: field}, nil
	case "median":
		return Median{Field: field}, nil
	case "rate":
		return Rate{}, nil
	case "rate_sum":
		return RateSum{Field: field}, nil
	case "count_uniq":
		return CountUniq{Field: field}, nil
	case "count_uniq_hash":
		return CountUniqHash{Field: field}, nil
	case "field_max":
		return FieldMax{Field: field}, nil
	case "field_min":
		return FieldMin{Field: field}, nil
	case "json_values":
		return JSONValues{Field: field}, nil
	case "json_values_sorted":
		return JSONValuesSorted{Field: field}, nil
	case "any":
		return Any{Field: field}, nil
	case "count_empty":
		return CountEmpty{Field: field}, nil
	case "sum_len":
		return SumLen{Field: field}, nil
	case "histogram":
		return Histogram{Field: field}, nil
	case "last":
		return Last{Field: field}, nil
	case "first":
		return First{Field: field}, nil
	case "stddev":
		return Stddev{Field: field}, nil
	case "stdvar":
		return Stdvar{Field: field}, nil
	default:
		return nil, fmt.Errorf("logsql: unknown stats function %q", name)
	}
}

// parseIdentListInParens parses ( ident, ident, ... ) and returns the ident list.
func (p *parser) parseIdentListInParens() ([]string, error) {
	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	var fields []string
	for p.peek().Typ != TokRParen && p.peek().Typ != TokEOF {
		f, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
		if p.peek().Typ == TokComma {
			p.advance()
		}
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return fields, nil
}

// parsePipeCoalesce parses: coalesce(f1, f2, ...) [default "val"] [as result]
func (p *parser) parsePipeCoalesce() (Pipe, error) {
	fields, err := p.parseFieldList()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe coalesce: %w", err)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("logsql: pipe coalesce: needs at least one field")
	}
	var def string
	if p.isKeyword("default") {
		p.advance()
		if def, err = p.parseStringArg(); err != nil {
			return nil, fmt.Errorf("logsql: pipe coalesce default: %w", err)
		}
	}
	var result string
	if p.isKeyword("as") {
		p.advance()
		if result, err = p.expectIdent(); err != nil {
			return nil, fmt.Errorf("logsql: pipe coalesce: %w", err)
		}
	}
	return PipeCoalesce{Fields: fields, Default: def, Result: result}, nil
}

// parsePipeCollapseNums parses: collapse_nums [at field]
func (p *parser) parsePipeCollapseNums() (Pipe, error) {
	if p.peek().Typ == TokIdent && p.peek().Val == "at" {
		p.advance()
		field, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe collapse_nums: %w", err)
		}
		return PipeCollapseNums{Field: field}, nil
	}
	return PipeCollapseNums{}, nil
}

// parsePipeDecolorize parses: decolorize [field]
func (p *parser) parsePipeDecolorize() (Pipe, error) {
	if p.peek().Typ == TokIdent {
		field := p.advance().Val
		return PipeDecolorize{Field: field}, nil
	}
	return PipeDecolorize{}, nil
}

// parsePipeFacets parses: facets [limit N]
func (p *parser) parsePipeFacets() (Pipe, error) {
	if p.peek().Typ == TokIdent && p.peek().Val == "limit" {
		p.advance()
		n, err := p.parseInt()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe facets: %w", err)
		}
		return PipeFacets{Limit: n}, nil
	}
	return PipeFacets{}, nil
}

// parsePipeFieldValues parses: field_values field [limit N]
func (p *parser) parsePipeFieldValues() (Pipe, error) {
	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe field_values: %w", err)
	}
	limit := 0
	if p.peek().Typ == TokIdent && p.peek().Val == "limit" {
		p.advance()
		limit, err = p.parseInt()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe field_values: %w", err)
		}
	}
	return PipeFieldValues{Field: field, Limit: limit}, nil
}

// parsePipeGenerateSequence parses: generate_sequence limit N
func (p *parser) parsePipeGenerateSequence() (Pipe, error) {
	n, err := p.parseInt()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe generate_sequence: %w", err)
	}
	return PipeGenerateSequence{Limit: n}, nil
}

// parsePipeHash parses: hash(f1, f2) as result
func (p *parser) parsePipeHash() (Pipe, error) {
	fields, err := p.parseIdentListInParens()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe hash: %w", err)
	}
	asTok := p.advance()
	if asTok.Typ != TokIdent || asTok.Val != "as" {
		return nil, fmt.Errorf("logsql: pipe hash: expected 'as', got %q", asTok.Val)
	}
	result, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe hash: %w", err)
	}
	return PipeHash{Fields: fields, Result: result}, nil
}

// parsePipeJoin parses: join by (f1, f2) (inner_query) [prefix pfx]
func (p *parser) parsePipeJoin() (Pipe, error) {
	byTok, err := p.expect(TokIdent)
	if err != nil || byTok.Val != "by" {
		return nil, fmt.Errorf("logsql: pipe join: expected 'by'")
	}
	by, err := p.parseIdentListInParens()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe join: %w", err)
	}
	inner, err := p.consumeInnerQuery()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe join: %w", err)
	}
	if _, err := newParser(inner).parseQuery(); err != nil {
		return nil, fmt.Errorf("logsql: pipe join: %w", err)
	}
	var prefix string
	if p.peek().Typ == TokIdent && p.peek().Val == "prefix" {
		p.advance()
		prefix, err = p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe join: %w", err)
		}
	}
	return PipeJoin{By: by, Inner: inner, Prefix: prefix}, nil
}

// parsePipeJSONArrayLen parses: json_array_len(field) as result
func (p *parser) parsePipeJSONArrayLen() (Pipe, error) {
	if _, err := p.expect(TokLParen); err != nil {
		return nil, fmt.Errorf("logsql: pipe json_array_len: %w", err)
	}
	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe json_array_len: %w", err)
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, fmt.Errorf("logsql: pipe json_array_len: %w", err)
	}
	asTok := p.advance()
	if asTok.Typ != TokIdent || asTok.Val != "as" {
		return nil, fmt.Errorf("logsql: pipe json_array_len: expected 'as', got %q", asTok.Val)
	}
	result, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe json_array_len: %w", err)
	}
	return PipeJSONArrayLen{Field: field, Result: result}, nil
}

// parsePipeJSONArrayConcat parses: json_array_concat [delimiter] [from <src>] [as <result>]
func (p *parser) parsePipeJSONArrayConcat() (Pipe, error) {
	pipe := PipeJSONArrayConcat{}
	if p.peek().Typ == TokString {
		delim, err := p.expectString()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe json_array_concat: %w", err)
		}
		pipe.Delimiter = delim
		pipe.HasDelimiter = true
	}
	if p.peek().Typ == TokIdent && p.peek().Val == "from" {
		p.advance()
		from, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe json_array_concat: %w", err)
		}
		pipe.From = from
	}
	if p.peek().Typ == TokIdent && p.peek().Val == "as" {
		p.advance()
		as, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe json_array_concat: %w", err)
		}
		pipe.As = as
	}
	return pipe, nil
}

// parsePipeLen parses: len(field) as result
func (p *parser) parsePipeLen() (Pipe, error) {
	if _, err := p.expect(TokLParen); err != nil {
		return nil, fmt.Errorf("logsql: pipe len: %w", err)
	}
	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe len: %w", err)
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, fmt.Errorf("logsql: pipe len: %w", err)
	}
	asTok := p.advance()
	if asTok.Typ != TokIdent || asTok.Val != "as" {
		return nil, fmt.Errorf("logsql: pipe len: expected 'as', got %q", asTok.Val)
	}
	result, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe len: %w", err)
	}
	return PipeLen{Field: field, Result: result}, nil
}

// parsePipeSplit parses: split "sep" [from field] [as result]
func (p *parser) parsePipeSplit() (Pipe, error) {
	sep, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe split: %w", err)
	}
	var from, as string
	if p.peek().Typ == TokIdent && p.peek().Val == "from" {
		p.advance()
		from, err = p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe split: %w", err)
		}
	}
	if p.peek().Typ == TokIdent && p.peek().Val == "as" {
		p.advance()
		as, err = p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe split: %w", err)
		}
	}
	return PipeSplit{Separator: sep, From: from, As: as}, nil
}

// parsePipeStreamContext parses: stream_context [before N] [after N]
func (p *parser) parsePipeStreamContext() (Pipe, error) {
	var before, after int
	var err error
	for p.peek().Typ == TokIdent {
		kw := p.peek().Val
		if kw != "before" && kw != "after" {
			break
		}
		p.advance()
		n, e := p.parseInt()
		if e != nil {
			return nil, fmt.Errorf("logsql: pipe stream_context: %w", e)
		}
		if kw == "before" {
			before = n
		} else {
			after = n
		}
		_ = err
	}
	return PipeStreamContext{Before: before, After: after}, nil
}

// parsePipeUnpackWords parses: unpack_words [from field] [as result]
func (p *parser) parsePipeUnpackWords() (Pipe, error) {
	var from, as string
	var err error
	if p.peek().Typ == TokIdent && p.peek().Val == "from" {
		p.advance()
		from, err = p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe unpack_words: %w", err)
		}
	}
	if p.peek().Typ == TokIdent && p.peek().Val == "as" {
		p.advance()
		as, err = p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe unpack_words: %w", err)
		}
	}
	return PipeUnpackWords{From: from, As: as}, nil
}

// parsePipeUnroll parses: unroll (f1, f2, ...)
func (p *parser) parsePipeUnroll() (Pipe, error) {
	fields, err := p.parseIdentListInParens()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe unroll: %w", err)
	}
	return PipeUnroll{Fields: fields}, nil
}

// parsePipeUpdate parses: update field = expr (raw expression to end of pipe)
func (p *parser) parsePipeUpdate() (Pipe, error) {
	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe update: %w", err)
	}
	if _, err := p.expect(TokEq); err != nil {
		return nil, fmt.Errorf("logsql: pipe update: expected '=': %w", err)
	}
	return PipeUpdate{Field: field, Expr: p.rawToPipe()}, nil
}

// consumeInnerQuery consumes a balanced (inner) query string after the opening (.
// Opening ( must not yet be consumed.
func (p *parser) consumeInnerQuery() (string, error) {
	if _, err := p.expect(TokLParen); err != nil {
		return "", err
	}
	p.buf = nil
	rest := p.sc.Remaining()

	// Find the matching ) at depth 0.
	depth, pos := 1, 0
	for pos < len(rest) {
		switch rest[pos] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				inner := strings.TrimSpace(rest[:pos])
				p.sc.Skip(pos + 1) // advance past the )
				return inner, nil
			}
		case '"':
			pos++
			for pos < len(rest) && rest[pos] != '"' {
				if rest[pos] == '\\' {
					pos++
				}
				pos++
			}
		}
		pos++
	}
	return "", fmt.Errorf("logsql: unclosed ( in inner query")
}

// parsePipeTopN parses: top N [by (f1, f2)]
func (p *parser) parsePipeTopN() (Pipe, error) {
	n, err := p.parseInt()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe top: %w", err)
	}
	by, err := p.parseOptionalByClause()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe top: %w", err)
	}
	return PipeTop{N: n, By: by}, nil
}

// parsePipeFirstLast parses: first|last [N] [by (f1, f2)]
// isLast=true produces PipeLast; false produces PipeFirst.
func (p *parser) parsePipeFirstLast(isLast bool) (Pipe, error) {
	n := 0
	if p.peek().Typ == TokIdent {
		if v, err := strconv.Atoi(p.peek().Val); err == nil {
			p.advance()
			n = v
		}
	}
	by, err := p.parseOptionalByClause()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe first/last: %w", err)
	}
	if isLast {
		return PipeLast{N: n, By: by}, nil
	}
	return PipeFirst{N: n, By: by}, nil
}

// parsePipeUniq parses: uniq [by (f1, f2)]
func (p *parser) parsePipeUniq() (Pipe, error) {
	by, err := p.parseOptionalByClause()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe uniq: %w", err)
	}
	return PipeUniq{By: by}, nil
}

// parsePipeCopy parses: copy src1 as dst1 [, src2 as dst2 ...]
func (p *parser) parsePipeCopy() (Pipe, error) {
	pairs, err := p.parseNamePairs("copy")
	if err != nil {
		return nil, err
	}
	return PipeCopy{Pairs: pairs}, nil
}

// parseOptionalByClause parses an optional "by (f1, f2)" clause and returns the field list.
func (p *parser) parseOptionalByClause() ([]string, error) {
	if p.peek().Typ != TokIdent || p.peek().Val != "by" {
		return nil, nil
	}
	p.advance() // consume "by"
	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	var fields []string
	for p.peek().Typ != TokRParen && p.peek().Typ != TokEOF {
		ident, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		fields = append(fields, ident)
		if p.peek().Typ == TokComma {
			p.advance()
		}
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return fields, nil
}

// rawToPipe consumes the rest of the pipe as written, up to the | that ends it (or the end), and returns it trimmed.
// A | inside a quoted string does not end it. No token may be buffered.
func (p *parser) rawToPipe() string {
	p.buf = nil
	rest := p.sc.Remaining()
	n := 0
	for n < len(rest) && rest[n] != '|' {
		if q := rest[n]; q == '"' || q == '\'' || q == '`' {
			if q == '`' {
				if i := strings.IndexByte(rest[n+1:], '`'); i >= 0 {
					n += i + 2
					continue
				}
			} else if pre, err := strconv.QuotedPrefix(rest[n:]); err == nil {
				n += len(pre)
				continue
			}
		}
		n++
	}
	p.sc.Skip(n)
	return strings.TrimSpace(rest[:n])
}

// lastSpacedAs is the byte offset of the last " as " in s, in any case, or -1. It compares bytes, so the offset
// is valid in s whatever its encoding (strings.ToLower changes the length of invalid UTF-8).
func lastSpacedAs(s string) int {
	for i := len(s) - 4; i >= 0; i-- {
		if s[i] == ' ' && s[i+1]|0x20 == 'a' && s[i+2]|0x20 == 's' && s[i+3] == ' ' {
			return i
		}
	}
	return -1
}

// parsePipeMath parses both forms:
//   - alias:=expr  (legacy form, our internal parser)
//   - expr as alias (VL canonical form, output of String())
func (p *parser) parsePipeMath() (Pipe, error) {
	// Capture the full remaining chunk for this pipe stage before advancing.
	chunk := p.rawToPipe()

	// Try alias:=expr form first.
	if idx := strings.Index(chunk, ":="); idx > 0 {
		alias := strings.TrimSpace(chunk[:idx])
		if !strings.ContainsAny(alias, " \t+-*/()|") {
			expr := strings.TrimSpace(chunk[idx+2:])
			if expr != "" {
				return PipeMath{Alias: alias, Expr: expr}, nil
			}
		}
	}

	// Try expr as alias form (scan for last " as " separator).
	if idx := lastSpacedAs(chunk); idx >= 0 {
		expr := strings.TrimSpace(chunk[:idx])
		alias := strings.TrimSpace(chunk[idx+4:])
		if expr != "" && alias != "" && !strings.ContainsAny(alias, " \t+-*/()") {
			return PipeMath{Alias: alias, Expr: expr}, nil
		}
	}

	return nil, fmt.Errorf("logsql: pipe math: expected 'alias:=expr' or 'expr as alias', got %q", chunk)
}
