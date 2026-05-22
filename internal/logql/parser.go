package logql

import (
	"fmt"
	"strconv"
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

// ParseAndValidate parses a LogQL expression and runs semantic validation.
// Returns the first error found, or nil if the query is well-formed.
func ParseAndValidate(input string) error {
	expr, err := Parse(input)
	if err != nil {
		return err
	}
	if msg := validateSemantics(expr, input); msg != "" {
		return fmt.Errorf("%s", msg)
	}
	return nil
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
	"rate_counter":       RangeRateCounter,
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

// isBinOpTok returns true if the current token can start a binary operator
// between two metric expressions.
func isBinOpTok(t TokType) bool {
	switch t {
	case TokPlus, TokMinus, TokStar, TokSlash, TokPercent, TokCaret,
		TokLt, TokGt, TokLtEq, TokGtEq, TokEqEq, TokBangEq,
		TokAnd, TokOr, TokUnless:
		return true
	}
	return false
}

// parseExpr is the top-level entry point, including infix binary operations.
func (p *parser) parseExpr() (Expr, error) {
	lhs, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	return p.maybeInfix(lhs)
}

// parsePrimary parses a single expression without any infix operators.
func (p *parser) parsePrimary() (Expr, error) {
	if p.cur.Typ == TokEOF {
		return nil, fmt.Errorf("logql: empty expression")
	}

	// Number literal (e.g. scalar in binary ops)
	if p.cur.Typ == TokNumber {
		val, err := strconv.ParseFloat(p.cur.Val, 64)
		if err != nil {
			return nil, fmt.Errorf("logql: invalid number %q", p.cur.Val)
		}
		p.advance()
		return &LiteralExpr{Value: val}, nil
	}

	// Negative number literal
	if p.cur.Typ == TokMinus {
		p.advance()
		if p.cur.Typ != TokNumber {
			return nil, fmt.Errorf("logql: expected number after '-'")
		}
		val, err := strconv.ParseFloat(p.cur.Val, 64)
		if err != nil {
			return nil, fmt.Errorf("logql: invalid number %q", p.cur.Val)
		}
		p.advance()
		return &LiteralExpr{Value: -val}, nil
	}

	// Vector aggregation: starts with a known aggregation name followed by
	// optional grouping or '('.
	if p.cur.Typ == TokIdent {
		if op, ok := vectorOps[p.cur.Val]; ok {
			return p.parseVectorAggregation(op)
		}
		if op, ok := rangeOps[p.cur.Val]; ok {
			ra, err := p.parseRangeAggregation(op)
			if err != nil {
				return nil, err
			}
			// Optional trailing by/without grouping: `rate(...)[5m]) by (label)`
			if p.cur.Typ == TokBy || p.cur.Typ == TokWithout {
				g, gErr := p.parseGrouping()
				if gErr != nil {
					return nil, gErr
				}
				ra.Grouping = g
			}
			return ra, nil
		}
		return nil, fmt.Errorf("logql: unexpected identifier %q", p.cur.Val)
	}

	// Parenthesised expression
	if p.cur.Typ == TokLParen {
		p.advance()
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return inner, nil
	}

	if p.cur.Typ == TokLBrace {
		return p.parseLogQuery()
	}

	return nil, fmt.Errorf("logql: unexpected token %v (%q)", p.cur.Typ, p.cur.Val)
}

// maybeInfix checks for a binary operator after a primary and builds a BinOpExpr
// if found. Handles optional vector matching modifiers (on/ignoring, group_left/right).
func (p *parser) maybeInfix(lhs Expr) (Expr, error) {
	if !isBinOpTok(p.cur.Typ) {
		return lhs, nil
	}
	op := p.advance().Val
	if op == "!=" {
		op = "!="
	}

	// Optional vector matching: on(labels) / ignoring(labels)
	var vm *VectorMatching
	if p.cur.Typ == TokOn || p.cur.Typ == TokIgnoring {
		card := p.advance().Val
		labels, err := p.parseLabelList()
		if err != nil {
			return nil, err
		}
		vm = &VectorMatching{Card: card, MatchLabels: labels}
		// Optional group_left / group_right
		if p.cur.Typ == TokGroupLeft || p.cur.Typ == TokGroupRight {
			side := p.advance().Val
			include, err := p.parseLabelList()
			if err != nil {
				return nil, err
			}
			vm.GroupSide = side
			vm.Include = include
		}
	}

	rhs, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	lhs = &BinOpExpr{Left: lhs, Right: rhs, Op: op, VectorMatching: vm}
	return p.maybeInfix(lhs)
}

// parseLabelList parses a parenthesised comma-separated label name list: (label1, label2).
// Returns an empty slice if the next token is not '('.
func (p *parser) parseLabelList() ([]string, error) {
	if p.cur.Typ != TokLParen {
		return nil, nil
	}
	p.advance() // consume '('
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
	return labels, nil
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
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterContains, Value: val}, nil

	case TokBangEq:
		p.advance()
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterExcludes, Value: val}, nil

	case TokPipeTilde:
		p.advance()
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterMatchRe, Value: val}, nil

	case TokBangTilde:
		p.advance()
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterExcludeRe, Value: val}, nil

	case TokPipeGt:
		p.advance()
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterContainsPat, Value: val}, nil

	case TokBangGt:
		p.advance()
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &LineFilterStage{Op: LineFilterExcludePat, Value: val}, nil

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

	// Unknown keyword after `|`: consume the identifier and the rest of the
	// stage. If followed by a comparison/assignment operator or dotted-label
	// continuation, this is a label filter. Otherwise treat as an opaque stage
	// (unknown parser keywords, custom extensions) so the query passes through
	// to the backend rather than being rejected by the proxy.
	p.advance() // consume the ident
	raw := kw + p.consumeRestOfStage()
	return &LabelFilterStage{Raw: raw}, nil
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

// expectStringOrRaw accepts either a quoted string or a raw (backtick) string,
// returning the unescaped value. Used for line filter operands.
func (p *parser) expectStringOrRaw() (string, error) {
	switch p.cur.Typ {
	case TokString:
		return p.advance().Val, nil
	case TokRawString:
		return p.advance().Val, nil
	}
	return "", fmt.Errorf("logql: expected STRING or RAWSTRING, got %v (%q)", p.cur.Typ, p.cur.Val)
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

// consumeRestOfStage reads tokens until the next pipeline operator, range
// bracket, or EOF, returning them as a raw string. Used for label filter and
// label_format stages whose expression grammar is complex and opaque to this
// parser. Stops at [ and ) so that the outer range aggregation parser can
// consume the [duration] and closing ) tokens.
func (p *parser) consumeRestOfStage() string {
	var parts []string
	for {
		switch p.cur.Typ {
		case TokEOF, TokPipe, TokPipeEq, TokPipeTilde, TokPipeGt, TokBangGt,
			TokLBracket, TokRParen:
			return strings.Join(parts, "")
		case TokBangEq, TokBangTilde:
			// When parts is non-empty we've already consumed `label=value` —
			// the `!=`/`!~` here starts a NEW line filter stage; stop.
			// When parts is empty the `!=`/`!~` IS the label filter operator
			// (e.g. `status!=200`); continue consuming.
			if len(parts) > 0 {
				return strings.Join(parts, "")
			}
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

// parseRangeAggregation parses `rate({...}[5m])` or a subquery
// like `max_over_time(rate({...}[5m])[1h:5m])`.
//
// For quantile_over_time the optional phi parameter comes before the
// inner expression: quantile_over_time(0.95, {app="nginx"}[5m]).
func (p *parser) parseRangeAggregation(op RangeOp) (*RangeAggregation, error) {
	p.advance() // consume function name

	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	ra := &RangeAggregation{Op: op}

	// quantile_over_time accepts an optional phi before the inner expression.
	if op == RangeQuantileOverTime && p.cur.Typ == TokNumber {
		phi, err := strconv.ParseFloat(p.cur.Val, 64)
		if err != nil {
			return nil, fmt.Errorf("logql: invalid phi: %w", err)
		}
		p.advance()
		ra.Param = phi
		ra.HasParam = true
		if _, err := p.expect(TokComma); err != nil {
			return nil, err
		}
	}
	if op == RangeQuantileOverTime && p.cur.Typ == TokMinus {
		// Negative phi
		p.advance()
		if p.cur.Typ != TokNumber {
			return nil, fmt.Errorf("logql: expected number after '-' in quantile_over_time")
		}
		phi, err := strconv.ParseFloat(p.cur.Val, 64)
		if err != nil {
			return nil, fmt.Errorf("logql: invalid phi: %w", err)
		}
		p.advance()
		ra.Param = -phi
		ra.HasParam = true
		if _, err := p.expect(TokComma); err != nil {
			return nil, err
		}
	}

	// Inner expression: log query ({...}) or metric expr (subquery).
	var err error
	if p.cur.Typ == TokLBrace {
		lq, lqErr := p.parseLogQuery()
		if lqErr != nil {
			return nil, lqErr
		}
		ra.Inner = lq
	} else {
		ra.Inner, err = p.parsePrimary()
		if err != nil {
			return nil, err
		}
	}

	// Expect [duration] or [duration:step]
	if _, err := p.expect(TokLBracket); err != nil {
		return nil, fmt.Errorf("logql: expected '[' for range, got %v (%q)", p.cur.Typ, p.cur.Val)
	}
	dur, err := p.expect(TokDuration)
	if err != nil {
		return nil, fmt.Errorf("logql: expected duration: %w", err)
	}
	ra.Range = dur.Val

	// Optional :step for subqueries — the colon is not a scanner keyword so it
	// arrives as TokError with Val ":".
	if p.cur.Typ == TokError && p.cur.Val == ":" {
		p.advance() // consume ':'
		step, stepErr := p.expect(TokDuration)
		if stepErr == nil {
			ra.Step = step.Val
		}
	}

	if _, err := p.expect(TokRBracket); err != nil {
		return nil, err
	}

	// Optional offset modifier: [duration] offset 1h
	if p.cur.Typ == TokIdent && p.cur.Val == "offset" {
		p.advance() // consume "offset"
		off, offErr := p.expect(TokDuration)
		if offErr != nil {
			return nil, fmt.Errorf("logql: expected duration after offset: %w", offErr)
		}
		ra.Offset = off.Val
	}

	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	return ra, nil
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

	// topk/bottomk carry a numeric parameter before the inner expression.
	if op == VectorTopK || op == VectorBottomK {
		kTok, err := p.expect(TokNumber)
		if err != nil {
			return nil, fmt.Errorf("logql: %s requires a numeric k parameter", op)
		}
		k, _ := strconv.ParseFloat(kTok.Val, 64)
		va.Param = k
		va.HasParam = true
		if _, err := p.expect(TokComma); err != nil {
			return nil, fmt.Errorf("logql: expected ',' after k in %s", op)
		}
	}

	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	va.Inner = inner

	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	// Optional trailing grouping: sum(...) by (label) — equivalent to sum by (label) (...)
	if va.Grouping == nil && (p.cur.Typ == TokBy || p.cur.Typ == TokWithout) {
		g, err := p.parseGrouping()
		if err != nil {
			return nil, err
		}
		va.Grouping = g
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
