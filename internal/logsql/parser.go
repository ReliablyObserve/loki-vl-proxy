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
		return tok, fmt.Errorf("logsql: expected %s, got %q (%s)", typ, tok.Val, tok.Typ)
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
		expr, err := p.parseNotExpr() // recurse to support NOT NOT expr
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
		// Exact message filter: ="value" or exact prefix ="prefix"*
		p.advance() // consume =
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		if p.peek().Typ == TokStar {
			p.advance()
			return ExactPrefix{Value: val}, nil
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

	// Special function-like message filters.
	switch name {
	case "i":
		// Case-insensitive: i("phrase") or i(prefix*)
		if p.peek().Typ != TokLParen {
			return Word{Value: name}, nil
		}
		p.advance() // consume (
		switch p.peek().Typ {
		case TokString:
			val := p.advance().Val
			if _, err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			return CaseInsensitive{Value: val}, nil
		case TokIdent:
			word := p.advance().Val
			if p.peek().Typ == TokStar {
				p.advance()
				if _, err := p.expect(TokRParen); err != nil {
					return nil, err
				}
				return AnyCasePrefix{Value: word}, nil
			}
			if _, err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			return CaseInsensitive{Value: word}, nil
		default:
			return nil, fmt.Errorf("logsql: unexpected token inside i(): %q", p.peek().Val)
		}

	case "contains_all", "contains_any":
		if p.peek().Typ != TokLParen {
			return Word{Value: name}, nil
		}
		p.advance() // consume (
		var parts []string
		for p.peek().Typ != TokRParen && p.peek().Typ != TokEOF {
			switch p.peek().Typ {
			case TokString:
				parts = append(parts, p.advance().Val)
			case TokIdent:
				parts = append(parts, p.advance().Val)
			case TokComma:
				p.advance()
			default:
				p.advance() // skip unexpected
			}
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		if name == "contains_all" {
			return ContainsAll{Parts: parts}, nil
		}
		return ContainsAny{Parts: parts}, nil

	case "len_range":
		if p.peek().Typ != TokLParen {
			return Word{Value: name}, nil
		}
		p.advance() // consume (
		raw, err := p.consumeUntilRParen()
		if err != nil {
			return nil, err
		}
		raw = strings.ReplaceAll(raw, " ", "")
		parts := strings.SplitN(raw, ",", 2)
		min, max := parts[0], ""
		if len(parts) > 1 {
			max = parts[1]
		}
		return LenRange{Min: min, Max: max}, nil

	case "pattern":
		if p.peek().Typ != TokLParen {
			return Word{Value: name}, nil
		}
		p.advance() // consume (
		raw, err := p.consumeUntilRParen()
		if err != nil {
			return nil, err
		}
		return PatternMatch{Pattern: raw}, nil
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

		case "ipv4_range":
			// field:ipv4_range(first,last)
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			val, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpIPv4Range, Value: val}, nil

		case "ipv6_range":
			// field:ipv6_range(first,last)
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			val, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpIPv6Range, Value: val}, nil

		case "eq_field":
			// field:eq_field(field2)
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			other, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpEqField, Value: other}, nil

		case "le_field":
			// field:le_field(field2)
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			other, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpLeField, Value: other}, nil

		case "string_range":
			// field:string_range(lo,hi) — values may be quoted strings
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			val, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpStringRange, Value: val}, nil

		case "value_type":
			// field:value_type(uint64)
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			typeName, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpValueType, Value: typeName}, nil

		case "json_array_contains_any":
			// field:json_array_contains_any("v1","v2")
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			val, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpJSONArrayContainsAny, Value: val}, nil

		case "contains_common_case":
			// field:contains_common_case("p1","p2")
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			val, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpContainsCommonCase, Value: val}, nil

		case "equals_common_case":
			// field:equals_common_case("p1","p2")
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			val, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return FieldFilter{Field: field, Op: FieldOpEqualsCommonCase, Value: val}, nil

		case "len_range":
			// field:len_range(0,100)
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			raw, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			raw = strings.ReplaceAll(raw, " ", "")
			parts := strings.SplitN(raw, ",", 2)
			min, max := parts[0], ""
			if len(parts) > 1 {
				max = parts[1]
			}
			return LenRange{Field: field, Min: min, Max: max}, nil

		case "pattern":
			// field:pattern("a <*> b")
			if _, err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			raw, err := p.consumeUntilRParen()
			if err != nil {
				return nil, err
			}
			return PatternMatch{Field: field, Pattern: raw}, nil
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

// parseTimeFilter parses _time:... after `_time` has been consumed.
func (p *parser) parseTimeFilter() (FilterExpr, error) {
	tok := p.peek()
	if tok.Typ != TokColon {
		return nil, fmt.Errorf("logsql: expected ':' after _time, got %q", tok.Val)
	}
	p.advance() // consume :

	rangeTok := p.peek()
	if rangeTok.Typ != TokIdent {
		return nil, fmt.Errorf("logsql: expected time range after _time:, got %q", rangeTok.Val)
	}
	p.advance()

	switch rangeTok.Val {
	case "day_range", "week_range":
		return p.parseBracketRange(rangeTok.Val)
	default:
		return TimeFilter{Range: rangeTok.Val}, nil
	}
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
	p.sc.Skip(len(p.sc.Remaining()) - len(rest) + end + 1) // advance past ]

	var offset string
	if p.peek().Typ == TokIdent && p.peek().Val == "offset" {
		p.advance()
		if p.peek().Typ == TokIdent {
			offset = p.advance().Val
		}
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
func (p *parser) parseStatsBody(keyword string) ([]GroupKey, []StatsFuncAlias, error) {
	var byKeys []GroupKey
	if p.peek().Typ == TokIdent && p.peek().Val == "by" {
		p.advance()
		if _, err := p.expect(TokLParen); err != nil {
			return nil, nil, err
		}
		for p.peek().Typ != TokRParen && p.peek().Typ != TokEOF {
			ident, err := p.expectIdent()
			if err != nil {
				return nil, nil, err
			}
			byKeys = append(byKeys, GroupKey{Field: ident})
			if p.peek().Typ == TokComma {
				p.advance()
			}
		}
		if _, err := p.expect(TokRParen); err != nil {
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
		asTok := p.advance()
		if asTok.Typ != TokIdent || asTok.Val != "as" {
			return nil, nil, fmt.Errorf("logsql: %s: expected 'as' after stats func, got %q", keyword, asTok.Val)
		}
		alias, err := p.expectIdent()
		if err != nil {
			return nil, nil, err
		}
		funcs = append(funcs, StatsFuncAlias{Func: fn, Alias: alias})
		if p.peek().Typ == TokComma {
			p.advance()
		} else {
			break
		}
	}
	if len(funcs) == 0 {
		return nil, nil, fmt.Errorf("logsql: %s requires at least one function", keyword)
	}
	return byKeys, funcs, nil
}

// parseStatsFunc parses a single stats function like count(), sum(field), etc.
func (p *parser) parseStatsFunc() (StatsFunc, error) {
	name := p.advance().Val // consume function name ident

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
		phi, _ := strconv.ParseFloat(phiTok.Val, 64)
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
		fieldTok := p.advance()
		field := fieldTok.Val
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
		fieldTok := p.advance()
		field := fieldTok.Val
		if p.peek().Typ == TokComma {
			p.advance()
		}
		limitTok := p.advance()
		limit, _ := strconv.Atoi(limitTok.Val)
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

// parsePipeCoalesce parses: coalesce(f1, f2, ...) [default "val"] as result
func (p *parser) parsePipeCoalesce() (Pipe, error) {
	fields, err := p.parseIdentListInParens()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe coalesce: %w", err)
	}
	var def string
	if p.peek().Typ == TokIdent && p.peek().Val == "default" {
		p.advance()
		s, err := p.expectString()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe coalesce default: %w", err)
		}
		def = s
	}
	asTok := p.advance()
	if asTok.Typ != TokIdent || asTok.Val != "as" {
		return nil, fmt.Errorf("logsql: pipe coalesce: expected 'as', got %q", asTok.Val)
	}
	result, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe coalesce: %w", err)
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
	if p.peek().Typ == TokIdent && p.peek().Val == "limit" {
		p.advance()
	}
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
	// Capture raw expression using Remaining() same as parsePipeMath
	p.buf = nil
	raw := p.sc.Remaining()
	if idx := strings.IndexByte(raw, '|'); idx >= 0 {
		raw = raw[:idx]
		p.sc.AdvanceTo('|')
	} else {
		p.sc.AdvanceTo(0)
	}
	p.advance()
	return PipeUpdate{Field: field, Expr: strings.TrimSpace(raw)}, nil
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
	var pairs [][2]string
	for {
		tok := p.peek()
		if tok.Typ == TokEOF || tok.Typ == TokPipe {
			break
		}
		src, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe copy: %w", err)
		}
		asTok := p.advance()
		if asTok.Typ != TokIdent || asTok.Val != "as" {
			return nil, fmt.Errorf("logsql: pipe copy: expected 'as' after %q, got %q", src, asTok.Val)
		}
		dst, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("logsql: pipe copy: %w", err)
		}
		pairs = append(pairs, [2]string{src, dst})
		if p.peek().Typ == TokComma {
			p.advance()
		} else {
			break
		}
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

// parsePipeMath parses: math alias:=expr
func (p *parser) parsePipeMath() (Pipe, error) {
	// alias is an ident
	alias, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("logsql: pipe math: expected alias: %w", err)
	}
	// expect :=
	if _, err := p.expect(TokColonEq); err != nil {
		return nil, fmt.Errorf("logsql: pipe math: expected ':=': %w", err)
	}
	// Capture raw math expression (may contain +, -, *, /, (, ), etc.)
	// p.buf may hold a lookahead token from the expect() call above;
	// clear it so Remaining() reflects the true scanner position.
	p.buf = nil
	raw := p.sc.Remaining()
	if idx := strings.IndexByte(raw, '|'); idx >= 0 {
		raw = raw[:idx]
		p.sc.AdvanceTo('|')
	} else {
		p.sc.AdvanceTo(0) // advance to EOF
	}
	p.advance() // reload cur token (will be TokPipe or TokEOF)
	return PipeMath{Alias: alias, Expr: strings.TrimSpace(raw)}, nil
}
