package logql

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseError is a query rejection whose message is already formatted the way
// Loki's parser reports it (e.g. "parse error at line 1, col 15: ..." or
// "parse error : ..."). ValidateLogQL surfaces it verbatim.
type ParseError struct {
	Msg string
}

func (e *ParseError) Error() string { return e.Msg }

// newParseError mirrors logqlmodel.NewParseError: line/col 0 renders as
// "parse error : msg".
func newParseError(line, col int, msg string) *ParseError {
	if line == 0 && col == 0 {
		return &ParseError{Msg: "parse error : " + msg}
	}
	return &ParseError{Msg: fmt.Sprintf("parse error at line %d, col %d: %s", line, col, msg)}
}

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
		// Unknown metric-level function (e.g. label_replace, label_join):
		// capture the full raw text and return it as-is so VictoriaLogs
		// receives the original expression unchanged.
		startPos := p.sc.pos - len(p.cur.Val)
		name := p.cur.Val
		p.advance()
		if p.cur.Typ == TokLParen {
			p.advance() // consume '('
			endPos, ok := p.consumeBalancedParens()
			if !ok || endPos < startPos {
				return nil, fmt.Errorf("logql: unterminated '(' after %q", name)
			}
			raw := strings.TrimSpace(p.input[startPos:endPos])
			return &OpaqueMetricExpr{Raw: raw}, nil
		}
		return nil, fmt.Errorf("logql: unexpected identifier %q", name)
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
	return p.parseBinaryRHS(lhs, 1)
}

// Loki syntax.y declares these levels from lowest to highest. Only exponentiation
// is right associative; parentheses are handled by parsePrimary.
func binaryPrecedence(token TokType) int {
	switch token {
	case TokOr:
		return 1
	case TokAnd, TokUnless:
		return 2
	case TokEqEq, TokBangEq, TokLt, TokLtEq, TokGt, TokGtEq:
		return 3
	case TokPlus, TokMinus:
		return 4
	case TokStar, TokSlash, TokPercent:
		return 5
	case TokCaret:
		return 6
	default:
		return 0
	}
}

func (p *parser) parseBinaryRHS(lhs Expr, minPrecedence int) (Expr, error) {
	for {
		precedence := binaryPrecedence(p.cur.Typ)
		if precedence < minPrecedence {
			return lhs, nil
		}
		op := p.advance().Val
		returnBool := false
		if p.cur.Typ == TokIdent && p.cur.Val == "bool" {
			returnBool = true
			p.advance()
		}

		var vm *VectorMatching
		if p.cur.Typ == TokOn || p.cur.Typ == TokIgnoring {
			card := p.advance().Val
			labels, err := p.parseLabelList()
			if err != nil {
				return nil, err
			}
			vm = &VectorMatching{Card: card, MatchLabels: labels}
			if p.cur.Typ == TokGroupLeft || p.cur.Typ == TokGroupRight {
				side := p.advance().Val
				include, err := p.parseLabelList()
				if err != nil {
					return nil, err
				}
				vm.GroupSide, vm.Include = side, include
			}
		}

		rhs, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		nextPrecedence := precedence + 1
		if op == "^" {
			nextPrecedence = precedence
		}
		rhs, err = p.parseBinaryRHS(rhs, nextPrecedence)
		if err != nil {
			return nil, err
		}
		lhs = &BinOpExpr{Left: lhs, Right: rhs, Op: op, ReturnBool: returnBool, VectorMatching: vm}
	}
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

	valStr, err := p.expectStringOrRaw()
	if err != nil {
		return LabelMatcher{}, fmt.Errorf("logql: expected label value after %q: %w", name.Val, err)
	}

	return LabelMatcher{Name: name.Val, Op: op, Value: valStr}, nil
}

// parsePipelineStage parses one pipeline stage. Returns nil, nil when no more
// pipeline stages are found (i.e. EOF or unexpected token).
func (p *parser) parsePipelineStage() (Stage, error) {
	if isLineFilterOperator(p.cur.Typ) {
		return p.parseLineFilterStage()
	}
	switch p.cur.Typ {
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
		start := p.sc.pos
		p.advance()
		param, fields, err := p.consumeExplicitFieldList(start)
		if err != nil {
			return nil, err
		}
		return &ParserStage{Type: ParserJSON, Param: param, Fields: fields}, nil

	case "logfmt":
		start := p.sc.pos
		p.advance()
		param, fields, err := p.consumeExplicitFieldList(start)
		if err != nil {
			return nil, err
		}
		return &ParserStage{Type: ParserLogfmt, Param: param, Fields: fields}, nil

	case "regexp":
		p.advance()
		param, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &ParserStage{Type: ParserRegexp, Param: param}, nil

	case "pattern":
		p.advance()
		param, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &ParserStage{Type: ParserPattern, Param: param}, nil

	case "unpack":
		p.advance()
		return &ParserStage{Type: ParserUnpack}, nil

	case "drop":
		p.advance()
		labels, matchers, err := p.parseDropKeepList()
		if err != nil {
			return nil, err
		}
		return &DropStage{Labels: labels, Matchers: matchers}, nil

	case "keep":
		p.advance()
		labels, matchers, err := p.parseDropKeepList()
		if err != nil {
			return nil, err
		}
		return &KeepStage{Labels: labels, Matchers: matchers}, nil

	case "decolorize":
		p.advance()
		return &DecolorizeStage{}, nil

	case "unwrap":
		p.advance()
		return p.parseUnwrap()

	case "line_format":
		p.advance()
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		return &LineFormatStage{Template: val}, nil

	case "label_format":
		p.advance()
		return p.parseLabelFormat()
	}

	// Unknown keyword after `|`: consume the identifier and the rest of the
	// stage. If followed by a comparison/assignment operator or dotted-label
	// continuation, this is a label filter. Otherwise treat as an opaque stage
	// (unknown parser keywords, custom extensions) so the query passes through
	// to the backend rather than being rejected by the proxy.
	p.advance() // consume the ident (this is the label name in a label filter)
	// A label filter must have an operator (=, !=, =~, !~, >=, <=, >, <) next.
	// Two consecutive identifiers (e.g. `| distinct method`) is invalid syntax —
	// Loki rejects this with "syntax error: unexpected IDENTIFIER".
	if p.cur.Typ == TokIdent {
		return nil, fmt.Errorf("logql: parse error: unexpected identifier %q after %q in pipeline stage", p.cur.Val, kw)
	}
	raw := kw + p.consumeRestOfStage()
	return &LabelFilterStage{Raw: raw}, nil
}

// parseUnwrap parses `unwrap label` or `unwrap bytes(label)`.
// Also handles Grafana's incomplete stub forms:
//   - | unwrap [range]             → UnwrapStage{Label: ""}
//   - | unwrap converter() [range] → UnwrapStage{Label: "", Converter: "converter"}
func (p *parser) parseUnwrap() (Stage, error) {
	// Incomplete stub: | unwrap [range] — no label name, bare bracket.
	// Grafana's metric builder emits this while the unwrap field picker is open.
	if p.cur.Typ == TokLBracket {
		p.consumeRange()
		return &UnwrapStage{Label: ""}, nil
	}

	if p.cur.Typ != TokIdent {
		return nil, fmt.Errorf("logql: expected label name after unwrap")
	}
	name := p.advance() // consume ident

	if p.cur.Typ == TokLParen {
		// converter(label) form
		converter := name.Val
		p.advance() // consume '('

		// Incomplete stub: | unwrap converter() [range] — empty parens.
		if p.cur.Typ == TokRParen {
			p.advance() // consume ')'
			p.consumeRange()
			return &UnwrapStage{Label: "", Converter: converter}, nil
		}

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

// consumeRange consumes an optional [duration] token if present.
func (p *parser) consumeRange() {
	if p.cur.Typ != TokLBracket {
		return
	}
	p.advance() // consume '['
	for p.cur.Typ != TokRBracket && p.cur.Typ != TokEOF {
		p.advance()
	}
	if p.cur.Typ == TokRBracket {
		p.advance() // consume ']'
	}
}

// expectStringOrRaw accepts either a quoted string, a raw (backtick) string,
// or an ip("...") function call (line filter extension). Returns the value.
func (p *parser) expectStringOrRaw() (string, error) {
	switch p.cur.Typ {
	case TokString:
		return p.advance().Val, nil
	case TokRawString:
		return p.advance().Val, nil
	case TokIdent:
		// ip("cidr") line filter extension: validate and treat as raw pass-through.
		name := p.advance().Val
		if p.cur.Typ == TokLParen {
			p.advance() // consume '('
			val, err := p.expectStringOrRaw()
			if err != nil {
				return "", err
			}
			if _, err := p.expect(TokRParen); err != nil {
				return "", err
			}
			if name == "ip" && !validIPPattern(val) {
				return "", fmt.Errorf("ip: invalid pattern: %q", val)
			}
			return name + "(" + val + ")", nil
		}
		return "", fmt.Errorf("logql: expected STRING or RAWSTRING, got IDENT (%q)", name)
	}
	return "", fmt.Errorf("logql: expected STRING or RAWSTRING, got %v (%q)", p.cur.Typ, p.cur.Val)
}

// parseDropKeepList parses the item list for drop/keep stages.
// Each item is either a bare label name or a conditional matcher: field="value", field!="val", field=~"re", field!~"re".
func (p *parser) parseDropKeepList() (labels []string, matchers []DropMatcher, err error) {
	first := true
	for p.cur.Typ == TokIdent {
		first = false
		name := p.advance().Val
		switch p.cur.Typ {
		case TokEq: // field="value"
			p.advance()
			val, e := p.expectStringOrRaw()
			if e != nil {
				err = e
				return
			}
			matchers = append(matchers, DropMatcher{Name: name, Op: "=", Value: val})
		case TokBangEq: // field!="value" (outside braces produces TokBangEq)
			p.advance()
			val, e := p.expectStringOrRaw()
			if e != nil {
				err = e
				return
			}
			matchers = append(matchers, DropMatcher{Name: name, Op: "!=", Value: val})
		case TokReMatch: // field=~"regex"
			p.advance()
			val, e := p.expectStringOrRaw()
			if e != nil {
				err = e
				return
			}
			matchers = append(matchers, DropMatcher{Name: name, Op: "=~", Value: val})
		case TokBangTilde: // field!~"regex" (outside braces produces TokBangTilde)
			p.advance()
			val, e := p.expectStringOrRaw()
			if e != nil {
				err = e
				return
			}
			matchers = append(matchers, DropMatcher{Name: name, Op: "!~", Value: val})
		default:
			labels = append(labels, name)
		}
		if p.cur.Typ == TokComma {
			p.advance()
		} else {
			break
		}
	}
	if first {
		err = fmt.Errorf("logql: expected at least one label name or matcher after drop/keep")
	}
	return
}

// consumeExplicitFieldList consumes an optional comma-separated field list
// after | json or | logfmt. Each item is a bare name or name="expression" form.
// Preserve exact extraction expressions, including raw strings and escapes,
// because the AST is also used to execute nested metric operands.
// Loki's grammar (labelExtractionExpression: IDENTIFIER EQ STRING | IDENTIFIER)
// requires a string after '=', so `| json foo=` is a syntax error.
func (p *parser) consumeExplicitFieldList(start int) (string, []ExtractionField, error) {
	end := start
	var fields []ExtractionField
	for p.cur.Typ == TokIdent {
		end = p.sc.pos
		name := p.advance().Val // field name
		field := ExtractionField{Name: name, Expression: name}
		if p.cur.Typ == TokEq {
			p.advance()
			if p.cur.Typ != TokString && p.cur.Typ != TokRawString {
				return "", nil, p.syntaxError("STRING")
			}
			end = p.sc.pos
			field.Expression = p.advance().Val
		}
		fields = append(fields, field)
		if p.cur.Typ != TokComma {
			break
		}
		end = p.sc.pos
		p.advance()
		if p.cur.Typ != TokIdent {
			return "", nil, p.syntaxError("IDENTIFIER")
		}
	}
	return strings.TrimSpace(p.input[start:end]), fields, nil
}

// parseLabelFormat parses `dst="template"` / `dst=src` entries separated by
// commas (Loki: labelFormat: IDENTIFIER EQ IDENTIFIER | IDENTIFIER EQ STRING).
// Raw keeps the historical token rendering consumed by the string translator.
func (p *parser) parseLabelFormat() (Stage, error) {
	var (
		raw     strings.Builder
		formats []LabelFormat
	)
	for {
		if p.cur.Typ != TokIdent {
			return nil, p.syntaxError("IDENTIFIER")
		}
		name := p.cur.Val
		appendStageToken(&raw, p.advance())
		if p.cur.Typ != TokEq {
			return nil, p.syntaxError("=")
		}
		appendStageToken(&raw, p.advance())
		format := LabelFormat{Name: name}
		switch p.cur.Typ {
		case TokString, TokRawString:
		case TokIdent:
			format.Rename = true
		default:
			return nil, p.syntaxError("IDENTIFIER or STRING")
		}
		format.Value = p.cur.Val
		appendStageToken(&raw, p.advance())
		formats = append(formats, format)
		if p.cur.Typ != TokComma {
			return &LabelFormatStage{Raw: raw.String(), Formats: formats}, nil
		}
		appendStageToken(&raw, p.advance())
	}
}

// syntaxError reports an unexpected current token the way Loki's yacc parser
// does ("syntax error: unexpected $end, expecting STRING").
func (p *parser) syntaxError(expecting string) error {
	msg := "syntax error: unexpected " + lokiTokenName(p.cur)
	if expecting != "" {
		msg += ", expecting " + expecting
	}
	if p.cur.Typ == TokEOF {
		return newParseError(1, len(p.input)+1, msg)
	}
	return newParseError(0, 0, msg)
}

// lokiTokenName renders a token with the name Loki's grammar uses for it.
func lokiTokenName(tok Token) string {
	switch tok.Typ {
	case TokEOF:
		return "$end"
	case TokNumber:
		return "NUMBER"
	case TokString, TokRawString:
		return "STRING"
	case TokDuration:
		return "RANGE"
	case TokIdent:
		if tok.Val == "label_replace" {
			return tok.Val
		}
		if _, ok := rangeOps[tok.Val]; ok {
			return strings.ToUpper(tok.Val)
		}
		if _, ok := vectorOps[tok.Val]; ok || tok.Val == "vector" {
			return strings.ToUpper(tok.Val)
		}
		return "IDENTIFIER"
	}
	return tok.Typ.String()
}

// consumeBalancedParens consumes tokens including nested parentheses until the
// matching close paren (which is also consumed). It returns the byte position in
// p.input immediately after the closing ')' and ok=true. If the parentheses
// never balance — EOF is reached with the depth still positive — it returns
// ok=false so the caller can emit a parse error instead of slicing p.input with
// a stale endPos of 0 (which panics for any startPos > 0). Used for opaque
// function calls.
func (p *parser) consumeBalancedParens() (endPos int, ok bool) {
	depth := 1
	for depth > 0 && p.cur.Typ != TokEOF {
		switch p.cur.Typ {
		case TokLParen:
			depth++
		case TokRParen:
			depth--
			if depth == 0 {
				endPos = p.sc.pos // right after the closing ')'
			}
		}
		p.advance()
	}
	if depth != 0 {
		return 0, false
	}
	return endPos, true
}

// consumeRestOfStage reads tokens until the next pipeline operator, range
// bracket, or EOF, returning them as a raw string. Used for label filter and
// label_format stages whose expression grammar is complex and opaque to this
// parser. Stops at [ and ) so that the outer range aggregation parser can
// consume the [duration] and closing ) tokens.
func (p *parser) consumeRestOfStage() string {
	var b strings.Builder
	for {
		switch p.cur.Typ {
		case TokEOF, TokPipe, TokPipeEq, TokPipeTilde, TokPipeGt, TokBangGt,
			TokLBracket, TokRParen:
			return b.String()
		case TokBangEq, TokBangTilde:
			// When b is non-empty we've already consumed `label=value` —
			// the `!=`/`!~` here starts a NEW line filter stage; stop.
			// When b is empty the `!=`/`!~` IS the label filter operator
			// (e.g. `status!=200`); continue consuming.
			if b.Len() > 0 {
				return b.String()
			}
		}
		appendStageToken(&b, p.advance())
	}
}

// appendStageToken appends a token's source rendering to b. It inserts a space
// when adjacent tokens would merge without one:
//
//	(a) alphanumeric + alphanumeric: `200`+`and` → `200and` (unparseable)
//	(b) string-end + alphanumeric:   `"error"`+`or` → `"error"or` (ambiguous)
func appendStageToken(b *strings.Builder, tok Token) {
	raw := tokenRaw(tok)
	if b.Len() > 0 && raw != "" && isAlphanumeric(raw[0]) {
		prev := b.String()[b.Len()-1]
		if isAlphanumeric(prev) || prev == '"' || prev == '`' {
			b.WriteByte(' ')
		}
	}
	b.WriteString(raw)
}

// isAlphanumeric reports whether c is a letter, digit, or underscore —
// characters that must be separated by whitespace from adjacent word tokens.
func isAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// tokenRaw reconstructs the approximate source text for a token.
func tokenRaw(tok Token) string {
	switch tok.Typ {
	case TokString:
		return strconv.Quote(tok.Val)
	case TokRawString:
		return quoteParserArgument(tok.Val)
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

// parseQuantileParam parses the optional phi parameter for quantile_over_time,
// which may be positive (0.95) or negative (-0.5), followed by a comma.
func (p *parser) parseQuantileParam(ra *RangeAggregation) error {
	switch p.cur.Typ {
	case TokNumber:
		phi, err := strconv.ParseFloat(p.cur.Val, 64)
		if err != nil {
			return fmt.Errorf("logql: invalid phi: %w", err)
		}
		p.advance()
		ra.Param = phi
		ra.HasParam = true
		_, err = p.expect(TokComma)
		return err
	case TokMinus:
		p.advance()
		if p.cur.Typ != TokNumber {
			return fmt.Errorf("logql: expected number after '-' in quantile_over_time")
		}
		phi, err := strconv.ParseFloat(p.cur.Val, 64)
		if err != nil {
			return fmt.Errorf("logql: invalid phi: %w", err)
		}
		p.advance()
		ra.Param = -phi
		ra.HasParam = true
		_, err = p.expect(TokComma)
		return err
	}
	return nil
}

// parseRangeAggregation parses `rate({...}[5m])`. LogQL (unlike PromQL) has
// no subquery grammar: the argument must be a log selector, optionally
// parenthesised, so `max_over_time(rate({...}[5m])[1h:5m])` is rejected with
// Loki's syntax error.
//
// For quantile_over_time the optional phi parameter comes before the
// inner expression: quantile_over_time(0.95, {app="nginx"}[5m]).
func (p *parser) parseRangeAggregation(op RangeOp) (*RangeAggregation, error) {
	p.advance() // consume function name

	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	ra := &RangeAggregation{Op: op}

	if op == RangeQuantileOverTime {
		if err := p.parseQuantileParam(ra); err != nil {
			return nil, err
		}
	}

	// Inner expression: a log query ({...}), optionally parenthesised.
	if err := p.checkRangeAggregationArgument(!ra.HasParam); err != nil {
		return nil, err
	}
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
		if _, ok := ra.Inner.(*LogQuery); !ok {
			return nil, newParseError(0, 0, "syntax error: unexpected expression in range aggregation, expecting { or (")
		}
	}

	// Expect [duration]. bracketCol is the 1-based column of '[' (the scanner
	// position just past it), which Loki reports for a malformed duration.
	bracketCol := p.sc.pos
	if _, err := p.expect(TokLBracket); err != nil {
		return nil, fmt.Errorf("logql: expected '[' for range, got %v (%q)", p.cur.Typ, p.cur.Val)
	}
	dur, err := p.expect(TokDuration)
	if err != nil {
		return nil, fmt.Errorf("logql: expected duration: %w", err)
	}
	ra.Range = dur.Val

	// A PromQL-style [range:step] is not LogQL. Loki's lexer rejects the
	// duration token itself; the colon arrives here as TokError ":".
	if p.cur.Typ == TokError && p.cur.Val == ":" {
		literal := ra.Range + ":"
		if p.advance(); p.cur.Typ == TokDuration {
			literal += p.cur.Val
		}
		return nil, newParseError(0, bracketCol, fmt.Sprintf("unknown unit %q in duration %q", durationUnit(ra.Range)+":", literal))
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

	// Optional @ modifier: [duration] @ <unix_timestamp | start() | end()>
	// Loki accepts this; we parse and discard it (VictoriaLogs handles its own timestamp logic).
	if p.cur.Typ == TokAt {
		p.advance() // consume "@"
		switch {
		case p.cur.Typ == TokNumber:
			p.advance() // consume unix timestamp
		case p.cur.Typ == TokIdent && (p.cur.Val == "start" || p.cur.Val == "end"):
			p.advance() // consume "start" or "end"
			if _, err := p.expect(TokLParen); err != nil {
				return nil, fmt.Errorf("logql: expected '(' after start/end in @ modifier")
			}
			if _, err := p.expect(TokRParen); err != nil {
				return nil, fmt.Errorf("logql: expected ')' after start/end in @ modifier")
			}
		default:
			return nil, fmt.Errorf("logql: expected unix timestamp or start()/end() after @, got %v", p.cur.Val)
		}
	}

	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	return ra, nil
}

// checkRangeAggregationArgument rejects a range aggregation whose argument is
// not a (parenthesised) log selector before it is parsed, reporting the first
// offending token like Loki's grammar does: `max_over_time(sum(...)[30m:5m])`
// → "parse error at line 1, col 15: syntax error: unexpected SUM, expecting
// NUMBER or { or (". The probe runs on a scanner copy and consumes nothing.
func (p *parser) checkRangeAggregationArgument(numberAllowed bool) error {
	sc := *p.sc
	probe := parser{sc: &sc, cur: p.cur, input: p.input}
	expecting := "{ or ("
	if numberAllowed {
		expecting = "NUMBER or { or ("
	}
	for probe.cur.Typ == TokLParen {
		probe.advance()
		expecting = "{ or ("
	}
	if probe.cur.Typ == TokLBrace {
		return nil
	}
	msg := "syntax error: unexpected " + lokiTokenName(probe.cur) + ", expecting " + expecting
	switch probe.cur.Typ {
	case TokEOF:
		return newParseError(1, len(p.input)+1, msg)
	case TokIdent, TokNumber:
		return newParseError(1, sc.pos-len(probe.cur.Val)+1, msg)
	}
	return newParseError(0, 0, msg)
}

// durationUnit returns the trailing unit letters of a duration literal
// ("30m" → "m"), used to mirror Loki's duration lexer error text.
func durationUnit(d string) string {
	i := len(d)
	for i > 0 && (d[i-1] < '0' || d[i-1] > '9') {
		i--
	}
	return d[i:]
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
		// Loki (syntax.mustNewVectorAggregationExpr) requires an integer k > 0.
		k, err := strconv.Atoi(kTok.Val)
		if err != nil {
			return nil, newParseError(0, 0, fmt.Sprintf("invalid parameter %s(%s,", op, kTok.Val))
		}
		if k <= 0 {
			return nil, newParseError(0, 0, fmt.Sprintf("invalid parameter (must be greater than 0) %s(%s", op, kTok.Val))
		}
		va.Param = float64(k)
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
