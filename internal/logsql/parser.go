// internal/logsql/parser.go
package logsql

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse parses a complete LogsQL query (filter + optional pipe stages).
// Returns an error if the input is empty or syntactically invalid.
func Parse(input string) (*Query, error) {
	p := newParser(input)
	return p.parseQuery()
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
	return f, nil
}

// ---------------------------------------------------------------------------
// parser internals
// ---------------------------------------------------------------------------

type parser struct {
	sc  *Scanner
	buf *Token // one-token lookahead buffer
}

func newParser(input string) *parser {
	return &parser{sc: NewScanner(input)}
}

// peek returns the next token without consuming it.
func (p *parser) peek() Token {
	if p.buf == nil {
		tok := p.sc.Next()
		p.buf = &tok
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
		return tok, fmt.Errorf("logsql: expected token type %d, got %q (type %d)", typ, tok.Val, tok.Typ)
	}
	return tok, nil
}

// expectIdent consumes an identifier token (or returns an error).
func (p *parser) expectIdent() (string, error) {
	tok := p.advance()
	if tok.Typ != TokIdent {
		return "", fmt.Errorf("logsql: expected identifier, got %q (type %d)", tok.Val, tok.Typ)
	}
	return tok.Val, nil
}

// expectString consumes a string token (TokString) and returns its unquoted value.
func (p *parser) expectString() (string, error) {
	tok := p.advance()
	if tok.Typ != TokString {
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
	left, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}
	for p.peek().Typ == TokAnd {
		p.advance() // consume AND
		right, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		left = AndExpr{Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseNotExpr() (FilterExpr, error) {
	if p.peek().Typ == TokNot {
		p.advance() // consume NOT
		expr, err := p.parsePrimaryFilter()
		if err != nil {
			return nil, err
		}
		return NotExpr{Expr: expr}, nil
	}
	return p.parsePrimaryFilter()
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
		if p.peek().Typ == TokIdent {
			ident := p.advance().Val
			// Check for trailing *
			if p.peek().Typ == TokStar {
				p.advance() // consume trailing *
				return Substring{Value: ident}, nil
			}
			// Just *word — treat as Wildcard followed by word? No, that's two
			// tokens. In LogsQL *word* is Substring. *word (no trailing *) is
			// not a standard form; treat as Substring without trailing star
			// is wrong. According to the spec, *error* is Substring. There is
			// no *error (without trailing *) in the test cases, so we can
			// return an error or treat it as Wildcard + Word. For safety,
			// return Substring only when there's a trailing *.
			//
			// Actually in the AST, Prefix is "err*" and Substring is "*err*".
			// There is no "*err" form. So we'd have to put the ident back —
			// but we can't unread two tokens. Instead we return an error for
			// unrecognised form.
			return nil, fmt.Errorf("logsql: unrecognised pattern *%s (missing trailing *?)", ident)
		}
		return Wildcard{}, nil

	case TokString:
		// Phrase: "hello world"
		p.advance()
		return Phrase{Value: tok.Val}, nil

	case TokEq:
		// Exact message filter: ="value"
		p.advance() // consume =
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return Exact{Value: val}, nil

	case TokTilde:
		// Regexp message filter: ~"pattern"
		p.advance() // consume ~
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return Regexp{Pattern: val}, nil

	case TokIdent:
		return p.parseIdentOrFieldFilter()

	default:
		return nil, fmt.Errorf("logsql: unexpected token %q (type %d) in filter", tok.Val, tok.Typ)
	}
}

// parseIdentOrFieldFilter handles bare words and field-filter expressions.
// Called when the current token is TokIdent.
func (p *parser) parseIdentOrFieldFilter() (FilterExpr, error) {
	tok := p.advance() // consume the ident
	name := tok.Val

	// Special: _time:duration — time filter
	if name == "_time" {
		return p.parseTimeFilter()
	}

	// Check for prefix: word followed directly by * (no colon)
	if p.peek().Typ == TokStar {
		// Peek ahead: is there a colon after? The scanner already consumed
		// the ident; peek is TokStar. This is a Prefix filter.
		p.advance() // consume *
		return Prefix{Value: name}, nil
	}

	// Check for field filter operators
	switch p.peek().Typ {
	case TokColonEq:
		p.advance()
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: name, Op: FieldOpExact, Value: val}, nil

	case TokColonTilde:
		p.advance()
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: name, Op: FieldOpRegexp, Value: val}, nil

	case TokColonGT:
		p.advance()
		val, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: name, Op: FieldOpGT, Value: val}, nil

	case TokColonGTE:
		p.advance()
		val, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: name, Op: FieldOpGTE, Value: val}, nil

	case TokColonLT:
		p.advance()
		val, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: name, Op: FieldOpLT, Value: val}, nil

	case TokColonLTE:
		p.advance()
		val, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: name, Op: FieldOpLTE, Value: val}, nil

	case TokColon:
		// Bare colon — may be: :*, :"", :word*, :*word*, :range(...), :in(...)
		p.advance() // consume :
		return p.parseBareCsFilterValue(name)
	}

	// Plain word (no operator follows)
	return Word{Value: name}, nil
}

// parseBareCsFilterValue parses the value part of a field filter after a bare `:`.
// field has already been consumed; the colon has been consumed.
func (p *parser) parseBareCsFilterValue(field string) (FilterExpr, error) {
	tok := p.peek()

	switch tok.Typ {
	case TokStar:
		p.advance() // consume *
		// Check for field:*word* — Substring field filter
		if p.peek().Typ == TokIdent {
			word := p.advance().Val
			if p.peek().Typ == TokStar {
				p.advance() // consume trailing *
				return FieldFilter{Field: field, Op: FieldOpSubstring, Value: word}, nil
			}
			// *word without trailing * — not canonical, error
			return nil, fmt.Errorf("logsql: expected * after %s:*%s for substring filter", field, word)
		}
		// field:* — any value
		return FieldFilter{Field: field, Op: FieldOpAny}, nil

	case TokString:
		// field:"" — empty, or field:"value" — exact
		p.advance()
		if tok.Val == "" {
			return FieldFilter{Field: field, Op: FieldOpEmpty}, nil
		}
		// Non-empty string after bare colon: best-effort exact
		return FieldFilter{Field: field, Op: FieldOpExact, Value: tok.Val}, nil

	case TokIdent:
		// Could be: range(...), in(...), prefix (ident*), or bare ident value
		ident := p.advance().Val

		switch ident {
		case "range":
			// field:range(min,max)
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			val, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpRange, Value: val}, nil

		case "in":
			// field:in(a,b,c)
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			val, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpIn, Value: val}, nil
		}

		// Check if followed by * → Prefix field filter
		if p.peek().Typ == TokStar {
			p.advance() // consume *
			return FieldFilter{Field: field, Op: FieldOpPrefix, Value: ident}, nil
		}

		// Plain word value: best-effort exact
		return FieldFilter{Field: field, Op: FieldOpExact, Value: ident}, nil
	}

	return nil, fmt.Errorf("logsql: unexpected token %q after %s:", tok.Val, field)
}

// parseTimeFilter parses _time:5m after `_time` has been consumed.
func (p *parser) parseTimeFilter() (FilterExpr, error) {
	// Expect a colon (bare :)
	tok := p.peek()
	if tok.Typ != TokColon {
		return nil, fmt.Errorf("logsql: expected ':' after _time, got %q", tok.Val)
	}
	p.advance() // consume :

	// The duration/range is an ident token (e.g. "5m", "[2024-01-01,2024-01-02]")
	rangeTok := p.peek()
	if rangeTok.Typ != TokIdent {
		return nil, fmt.Errorf("logsql: expected time range after _time:, got %q", rangeTok.Val)
	}
	p.advance()
	return TimeFilter{Range: rangeTok.Val}, nil
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

// consumeUntilRParen consumes tokens until ) and returns the raw joined string.
// Used for range(min,max) and in(a,b,c). Opening ( has already been consumed.
func (p *parser) consumeUntilRParen() (string, error) {
	var parts []string
	for {
		tok := p.advance()
		switch tok.Typ {
		case TokRParen:
			return strings.Join(parts, ""), nil
		case TokEOF:
			return "", fmt.Errorf("logsql: unclosed parenthesis")
		case TokComma:
			parts = append(parts, ",")
		case TokIdent:
			parts = append(parts, tok.Val)
		case TokString:
			parts = append(parts, tok.Val)
		default:
			parts = append(parts, tok.Val)
		}
	}
}

// ---------------------------------------------------------------------------
// Pipe parsing
// ---------------------------------------------------------------------------

func (p *parser) parsePipe() (Pipe, error) {
	tok := p.peek()
	if tok.Typ != TokIdent {
		return nil, fmt.Errorf("logsql: expected pipe name after |, got %q (type %d)", tok.Val, tok.Typ)
	}
	name := p.advance().Val

	switch name {
	case "unpack_json":
		return PipeUnpackJSON{}, nil
	case "unpack_logfmt":
		return PipeUnpackLogfmt{}, nil
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
	default:
		return nil, fmt.Errorf("logsql: unknown pipe %q", name)
	}
}

// parseCommaSeparatedIdents parses f1, f2, f3 until EOF or pipe.
func (p *parser) parseCommaSeparatedIdents() ([]string, error) {
	var fields []string
	for {
		tok := p.peek()
		if tok.Typ == TokEOF || tok.Typ == TokPipe {
			break
		}
		ident, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		fields = append(fields, ident)
		if p.peek().Typ == TokComma {
			p.advance()
		} else {
			break
		}
	}
	return fields, nil
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
	var byKeys []GroupKey

	// Optional "by (f1, f2)"
	if p.peek().Typ == TokIdent && p.peek().Val == "by" {
		p.advance() // consume "by"
		if _, err := p.expect(TokLParen); err != nil {
			return nil, err
		}
		for p.peek().Typ != TokRParen && p.peek().Typ != TokEOF {
			ident, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			byKeys = append(byKeys, GroupKey{Field: ident})
			if p.peek().Typ == TokComma {
				p.advance()
			}
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
	}

	// One or more func() as alias
	var funcs []StatsFuncAlias
	for {
		tok := p.peek()
		if tok.Typ == TokEOF || tok.Typ == TokPipe {
			break
		}
		if tok.Typ != TokIdent {
			break
		}

		fn, err := p.parseStatsFunc()
		if err != nil {
			return nil, err
		}

		// expect "as"
		asTok := p.advance()
		if asTok.Typ != TokIdent || asTok.Val != "as" {
			return nil, fmt.Errorf("logsql: expected 'as' after stats func, got %q", asTok.Val)
		}

		alias, err := p.expectIdent()
		if err != nil {
			return nil, err
		}

		funcs = append(funcs, StatsFuncAlias{Func: fn, Alias: alias})

		if p.peek().Typ == TokComma {
			p.advance()
		} else {
			break
		}
	}

	if len(funcs) == 0 {
		return nil, fmt.Errorf("logsql: stats requires at least one function")
	}

	return PipeStats{By: byKeys, Funcs: funcs}, nil
}

// parseStatsFunc parses a single stats function like count(), sum(field), etc.
func (p *parser) parseStatsFunc() (StatsFunc, error) {
	name := p.advance().Val // consume function name ident

	// Consume opening paren
	if _, err := p.expect(TokLParen); err != nil {
		return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
	}

	// Parse optional field argument
	var field string
	if p.peek().Typ == TokIdent {
		field = p.advance().Val
	}

	// Consume closing paren
	if _, err := p.expect(TokRParen); err != nil {
		return nil, fmt.Errorf("logsql: stats func %q: %w", name, err)
	}

	switch name {
	case "count":
		return Count{}, nil
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

// parsePipeMath parses: math alias:=expr
func (p *parser) parsePipeMath() (Pipe, error) {
	// alias is an ident
	alias, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe math: expected alias: %w", err)
	}
	// expect :=
	opTok, err := p.expect(TokColonEq)
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe math: expected ':=': %w", err)
	}
	_ = opTok
	// The expression is everything until EOF or next pipe.
	// Collect remaining ident/operator tokens as raw string.
	var exprParts []string
	for {
		tok := p.peek()
		if tok.Typ == TokEOF || tok.Typ == TokPipe {
			break
		}
		exprParts = append(exprParts, p.advance().Val)
	}
	expr := strings.Join(exprParts, "")
	return PipeMath{Alias: alias, Expr: expr}, nil
}
