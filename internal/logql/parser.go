package logql

import (
	"fmt"
	"strings"
)

// Parse parses a LogQL expression string and returns the typed AST.
// It accepts log queries, range aggregations, and vector aggregations.
func Parse(input string) (Expr, error) {
	p := &parser{sc: newScanner(input), input: input}
	p.advance()
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.cur.Typ != TokEOF {
		return nil, fmt.Errorf("logql: unexpected token %q at position %d", p.cur.Val, p.sc.pos)
	}
	return expr, nil
}

// ParseAndValidate parses a LogQL expression and returns any syntax error.
// It is a lightweight gate for request validation — callers that only need
// to know whether a query is well-formed should use this instead of Parse.
func ParseAndValidate(input string) error {
	_, err := Parse(input)
	return err
}

// ParseLogQuery parses a log query (stream selector + optional pipeline).
// Returns an error if the expression is a metric expression.
func ParseLogQuery(input string) (*LogQuery, error) {
	expr, err := Parse(input)
	if err != nil {
		return nil, err
	}
	lq, ok := expr.(*LogQuery)
	if !ok {
		return nil, fmt.Errorf("logql: expression is not a log query: %T", expr)
	}
	return lq, nil
}

// vectorOps maps function names to VectorOp constants.
var vectorOps = map[string]VectorOp{
	"sum":       VectorSum,
	"avg":       VectorAvg,
	"min":       VectorMin,
	"max":       VectorMax,
	"count":     VectorCount,
	"stddev":    VectorStddev,
	"stdvar":    VectorStdvar,
	"bottomk":   VectorBottomK,
	"topk":      VectorTopK,
	"sort":      VectorSort,
	"sort_desc": VectorSortDesc,
}

// rangeOps maps function names to RangeOp constants.
var rangeOps = map[string]RangeOp{
	"rate":               RangeRate,
	"count_over_time":    RangeCountOverTime,
	"bytes_rate":         RangeBytesRate,
	"bytes_over_time":    RangeBytesOverTime,
	"avg_over_time":      RangeAvgOverTime,
	"sum_over_time":      RangeSumOverTime,
	"min_over_time":      RangeMinOverTime,
	"max_over_time":      RangeMaxOverTime,
	"stdvar_over_time":   RangeStdvarOverTime,
	"stddev_over_time":   RangeStddevOverTime,
	"quantile_over_time": RangeQuantileOverTime,
	"first_over_time":    RangeFirstOverTime,
	"last_over_time":     RangeLastOverTime,
	"absent_over_time":   RangeAbsentOverTime,
}

type parser struct {
	sc    *scanner
	cur   Token
	input string
}

func (p *parser) advance() Token {
	prev := p.cur
	p.cur = p.sc.next()
	return prev
}

func (p *parser) expect(typ TokType) (Token, error) {
	if p.cur.Typ != typ {
		return Token{}, fmt.Errorf("logql: expected %v, got %v (%q)", typ, p.cur.Typ, p.cur.Val)
	}
	return p.advance(), nil
}

// parseExpr is the top-level entry point.
func (p *parser) parseExpr() (Expr, error) {
	if p.cur.Typ == TokEOF {
		return nil, fmt.Errorf("logql: empty expression")
	}

	// Vector aggregation: starts with a known aggregation name followed by
	// optional grouping or '('.
	if p.cur.Typ == TokIdent {
		if op, ok := vectorOps[p.cur.Val]; ok {
			return p.parseVectorAggregation(op)
		}
		if op, ok := rangeOps[p.cur.Val]; ok {
			return p.parseRangeAggregation(op)
		}
		return nil, fmt.Errorf("logql: unexpected identifier %q", p.cur.Val)
	}

	if p.cur.Typ == TokLBrace {
		return p.parseLogQuery()
	}

	return nil, fmt.Errorf("logql: unexpected token %v (%q)", p.cur.Typ, p.cur.Val)
}

// parseLogQuery parses {selector} [pipeline stages].
func (p *parser) parseLogQuery() (*LogQuery, error) {
	sel, err := p.parseStreamSelector()
	if err != nil {
		return nil, err
	}

	lq := &LogQuery{Selector: sel}

	for {
		stage, err := p.parsePipelineStage()
		if err != nil {
			return nil, err
		}
		if stage == nil {
			break
		}
		lq.Pipeline = append(lq.Pipeline, stage)
	}

	return lq, nil
}

// parseStreamSelector parses {label="value",...}.
func (p *parser) parseStreamSelector() (*StreamSelector, error) {
	if _, err := p.expect(TokLBrace); err != nil {
		return nil, err
	}

	sel := &StreamSelector{}

	if p.cur.Typ == TokRBrace {
		p.advance()
		return sel, nil
	}

	for {
		m, err := p.parseLabelMatcher()
		if err != nil {
			return nil, err
		}
		sel.Matchers = append(sel.Matchers, m)

		if p.cur.Typ == TokComma {
			p.advance()
			continue
		}
		break
	}

	if _, err := p.expect(TokRBrace); err != nil {
		return nil, err
	}

	return sel, nil
}

func (p *parser) parseLabelMatcher() (LabelMatcher, error) {
	name, err := p.expect(TokIdent)
	if err != nil {
		return LabelMatcher{}, fmt.Errorf("logql: expected label name: %w", err)
	}

	var op MatchOp
	switch p.cur.Typ {
	case TokEq:
		op = MatchEq
	case TokNeq:
		op = MatchNeq
	case TokReMatch:
		op = MatchRe
	case TokReNotMatch:
		op = MatchNotRe
	default:
		return LabelMatcher{}, fmt.Errorf("logql: expected label operator, got %v (%q)", p.cur.Typ, p.cur.Val)
	}
	p.advance()

	val, err := p.expect(TokString)
	if err != nil {
		return LabelMatcher{}, fmt.Errorf("logql: expected label value: %w", err)
	}

	return LabelMatcher{Name: name.Val, Op: op, Value: val.Val}, nil
}

// parsePipelineStage parses one pipeline stage. Returns nil, nil when no more
// pipeline stages are found (i.e. EOF or unexpected token).
func (p *parser) parsePipelineStage() (Stage, error) {
	switch p.cur.Typ {
	case TokPipeEq:
		p.advance()
		val, err := p.expect(TokString)
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterContains, Value: val.Val}, nil

	case TokBangEq:
		p.advance()
		val, err := p.expect(TokString)
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterExcludes, Value: val.Val}, nil

	case TokPipeTilde:
		p.advance()
		val, err := p.expect(TokString)
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterMatchRe, Value: val.Val}, nil

	case TokBangTilde:
		p.advance()
		val, err := p.expect(TokString)
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterExcludeRe, Value: val.Val}, nil

	case TokPipeGt:
		p.advance()
		val, err := p.expect(TokString)
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterContainsPat, Value: val.Val}, nil

	case TokBangGt:
		p.advance()
		val, err := p.expect(TokString)
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterExcludePat, Value: val.Val}, nil

	case TokPipe:
		p.advance()
		return p.parsePipeBody()
	}

	return nil, nil
}

// parsePipeBody parses what comes after a bare `|`.
func (p *parser) parsePipeBody() (Stage, error) {
	if p.cur.Typ != TokIdent {
		return nil, fmt.Errorf("logql: expected stage name after |, got %v", p.cur.Typ)
	}

	kw := p.cur.Val

	switch kw {
	case "json":
		p.advance()
		return &ParserStage{Type: ParserJSON}, nil

	case "logfmt":
		p.advance()
		return &ParserStage{Type: ParserLogfmt}, nil

	case "regexp":
		p.advance()
		param, err := p.expect(TokRawString)
		if err != nil {
			return nil, err
		}
		return &ParserStage{Type: ParserRegexp, Param: param.Val}, nil

	case "pattern":
		p.advance()
		param, err := p.expect(TokRawString)
		if err != nil {
			return nil, err
		}
		return &ParserStage{Type: ParserPattern, Param: param.Val}, nil

	case "unpack":
		p.advance()
		return &ParserStage{Type: ParserUnpack}, nil

	case "drop":
		p.advance()
		labels, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		return &DropStage{Labels: labels}, nil

	case "keep":
		p.advance()
		labels, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		return &KeepStage{Labels: labels}, nil

	case "decolorize":
		p.advance()
		return &DecolorizeStage{}, nil

	case "unwrap":
		p.advance()
		return p.parseUnwrap()

	case "line_format":
		p.advance()
		val, err := p.expect(TokString)
		if err != nil {
			return nil, err
		}
		return &LineFormatStage{Template: val.Val}, nil

	case "label_format":
		p.advance()
		raw := p.consumeRestOfStage()
		return &LabelFormatStage{Raw: raw}, nil
	}

	// Unknown keyword after `|`: consume the identifier, then check for a
	// comparison operator. If present, treat as a label filter expression
	// (e.g. `level="error"`). Otherwise it is an unknown stage — return error.
	p.advance() // consume the ident
	switch p.cur.Typ {
	case TokEq, TokNeq, TokBangEq, TokReMatch, TokReNotMatch:
		raw := kw + p.consumeRestOfStage()
		return &LabelFilterStage{Raw: raw}, nil
	}
	return nil, fmt.Errorf("logql: unknown pipeline stage %q", kw)
}

// parseUnwrap parses `unwrap label` or `unwrap bytes(label)`.
func (p *parser) parseUnwrap() (Stage, error) {
	// Peek: if current ident followed by '(' it's a converter form.
	if p.cur.Typ != TokIdent {
		return nil, fmt.Errorf("logql: expected label name after unwrap")
	}
	name := p.advance() // consume ident

	if p.cur.Typ == TokLParen {
		// converter(label) form
		converter := name.Val
		p.advance() // consume '('
		label, err := p.expect(TokIdent)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return &UnwrapStage{Label: label.Val, Converter: converter}, nil
	}

	return &UnwrapStage{Label: name.Val}, nil
}

// parseIdentList parses a comma-separated list of identifiers (for drop/keep).
func (p *parser) parseIdentList() ([]string, error) {
	var labels []string
	for p.cur.Typ == TokIdent {
		labels = append(labels, p.advance().Val)
		if p.cur.Typ == TokComma {
			p.advance()
		} else {
			break
		}
	}
	if len(labels) == 0 {
		return nil, fmt.Errorf("logql: expected at least one label name")
	}
	return labels, nil
}

// consumeRestOfStage reads tokens until the next pipeline operator or EOF,
// returning them as a raw string. Used for label filter and label_format stages
// whose expression grammar is complex and opaque to this parser.
func (p *parser) consumeRestOfStage() string {
	var parts []string
	for {
		switch p.cur.Typ {
		case TokEOF, TokPipe, TokPipeEq, TokBangEq, TokPipeTilde, TokBangTilde, TokPipeGt, TokBangGt:
			return strings.Join(parts, "")
		}
		parts = append(parts, tokenRaw(p.cur))
		p.advance()
	}
}

// tokenRaw reconstructs the approximate source text for a token.
func tokenRaw(tok Token) string {
	switch tok.Typ {
	case TokString:
		return `"` + tok.Val + `"`
	case TokRawString:
		return "`" + tok.Val + "`"
	case TokEq:
		return "="
	case TokNeq, TokBangEq:
		return "!="
	case TokReMatch:
		return "=~"
	case TokReNotMatch, TokBangTilde:
		return "!~"
	case TokComma:
		return ", "
	case TokLParen:
		return "("
	case TokRParen:
		return ")"
	case TokLBrace:
		return "{"
	case TokRBrace:
		return "}"
	}
	return tok.Val
}

// parseRangeAggregation parses `rate({...}[5m])`.
func (p *parser) parseRangeAggregation(op RangeOp) (*RangeAggregation, error) {
	p.advance() // consume function name

	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	lq, err := p.parseLogQuery()
	if err != nil {
		return nil, err
	}

	// Expect [duration]
	if _, err := p.expect(TokLBracket); err != nil {
		return nil, fmt.Errorf("logql: expected '[' for range, got %v (%q)", p.cur.Typ, p.cur.Val)
	}
	dur, err := p.expect(TokDuration)
	if err != nil {
		return nil, fmt.Errorf("logql: expected duration: %w", err)
	}
	if _, err := p.expect(TokRBracket); err != nil {
		return nil, err
	}

	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	return &RangeAggregation{Op: op, Query: lq, Range: dur.Val}, nil
}

// parseVectorAggregation parses `sum by (...) (inner)`.
func (p *parser) parseVectorAggregation(op VectorOp) (*VectorAggregation, error) {
	p.advance() // consume function name

	va := &VectorAggregation{Op: op}

	// Optional grouping before the inner expression
	if p.cur.Typ == TokBy || p.cur.Typ == TokWithout {
		g, err := p.parseGrouping()
		if err != nil {
			return nil, err
		}
		va.Grouping = g
	}

	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	va.Inner = inner

	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	return va, nil
}

// parseGrouping parses `by (label1, label2)` or `without (label1)`.
func (p *parser) parseGrouping() (*Grouping, error) {
	without := p.cur.Typ == TokWithout
	p.advance() // consume by/without

	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	var labels []string
	for p.cur.Typ == TokIdent {
		labels = append(labels, p.advance().Val)
		if p.cur.Typ == TokComma {
			p.advance()
		}
	}

	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	return &Grouping{Without: without, Labels: labels}, nil
}
