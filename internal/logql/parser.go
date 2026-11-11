package logql

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
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
	var b strings.Builder
	b.Grow(len("parse error at line , col : ") + 4 + len(msg))
	b.WriteString("parse error at line ")
	var digits [20]byte
	b.Write(strconv.AppendInt(digits[:0], int64(line), 10))
	b.WriteString(", col ")
	b.Write(strconv.AppendInt(digits[:0], int64(col), 10))
	b.WriteString(": ")
	b.WriteString(msg)
	return &ParseError{Msg: b.String()}
}

// errLexerFirst stops a parse once the scanner has noted an error of Loki's lexer, which Parse reports.
var errLexerFirst = errors.New("logql: lexer error")

// Parse parses a LogQL expression string and returns the typed AST.
// It accepts log queries, range aggregations, and vector aggregations.
func Parse(input string) (Expr, error) {
	expr, unknown, err := parseExpression(input)
	if err == nil && unknown != nil {
		return nil, unknown
	}
	return expr, err
}

// parseExpression is Parse that reports a query whose names hold a character Loki's lexer reads as an identifier
// of its own (a label called ٣ or €) apart: Loki accepts such a name, but the translation to LogsQL does not carry
// it, so the proxy turns the query away. The error comes after the semantic checks, which have Loki's own errors
// for the rest of the query.
func parseExpression(input string) (Expr, *ParseError, error) {
	querycorpus.Record(querycorpus.LogQL, input)
	// Loki rejects a query this long before lexing it; doing the same bounds how deep a query can nest, and so
	// the recursion of every parse, for whatever query length the handlers accept.
	if msg := InputSizeError(input); msg != "" {
		return nil, nil, &ParseError{Msg: msg}
	}
	p := &parser{sc: newScanner(input)}
	p.advance()
	expr, err := p.parseExpr()
	if err == nil && p.cur.Typ != TokEOF {
		err = p.syntaxErrorAt("")
	}
	if p.sc.lexErrCount() > 0 {
		// Loki's lexer reports its own error ahead of the parse error it may cause, and for a query that parses.
		e := p.sc.extra.err
		return nil, nil, p.errorMsgAt(int(e.off), e.msg, e.zeroLine)
	}
	if err != nil {
		if p.plainUnwrapAt > 0 {
			// An unwrap stage outside a range aggregation: Loki's parser stops at its token, before it reads on.
			err = p.errorAt(int(p.plainUnwrapAt)-1, "unwrap", "", false)
		}
		return nil, nil, err
	}
	var unknown *ParseError
	if at := p.unknownName(); at >= 0 {
		r, _ := utf8.DecodeRuneInString(input[at:])
		unknown, _ = p.errorMsgAt(at, "unsupported name character "+strconv.QuoteRune(r), false).(*ParseError)
	}
	return expr, unknown, nil
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

// opaqueMetricFunctions are the metric-level calls the parser keeps as raw text
// (OpaqueMetricExpr): the ones in Loki's grammar that vectorOps and rangeOps do not cover.
var opaqueMetricFunctions = map[string]bool{"label_replace": true, "vector": true, "approx_topk": true}

// typeOpaqueCall reads the parts of a label_replace or vector call from its text
// with the same parser: label_replace(<expr>, "dst", "replacement", "src",
// "regex") and vector(<number>), as Loki's grammar writes them (syntax.y
// labelReplaceExpr, vectorExpr). Any other call, or one that does not read so,
// keeps only its text; validation reports why.
func typeOpaqueCall(o *OpaqueMetricExpr) {
	p := &parser{sc: &scanner{src: o.Raw, quiet: true}}
	p.advance()
	if p.cur.Typ != TokIdent || p.cur.Val != o.Func {
		return
	}
	if p.advance(); p.cur.Typ != TokLParen {
		return
	}
	p.advance()
	switch o.Func {
	case "label_replace":
		inner, err := p.parseExpr()
		if err != nil {
			return
		}
		args := make([]string, 0, 4)
		for i := 0; i < 4; i++ {
			if p.cur.Typ != TokComma {
				return
			}
			p.advance()
			if p.cur.Typ != TokString && p.cur.Typ != TokRawString {
				return
			}
			args = append(args, p.cur.Val)
			p.advance()
		}
		if p.cur.Typ != TokRParen {
			return
		}
		o.Inner, o.Args, o.Typed = inner, args, true
	case "vector":
		if p.cur.Typ != TokNumber {
			return
		}
		v, err := strconv.ParseFloat(p.cur.Val, 64)
		if err != nil {
			return
		}
		if p.advance(); p.cur.Typ != TokRParen {
			return
		}
		o.Value, o.Typed = v, true
	}
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
	sc  *scanner
	cur Token
	// prevEnd is the byte offset where the token before cur ends.
	prevEnd int
	// pipePos is the offset of the last `|` read, where the token before a label filter's text stands.
	pipePos int
	// canon is set by CanonicalizeLogRanges only.
	canon *canonState
	// flagSpans collects the spans of the logfmt flags read, when not nil (StripParserFlags).
	flagSpans *[][2]int
	// rangeDepth is above 0 inside the argument of a range aggregation, where an unwrap stage may stand. plainUnwrapAt
	// is one more than the offset of the first unwrap stage outside one: Loki's parser stops at that token whatever
	// follows it.
	rangeDepth, plainUnwrapAt int32
}

// keep marks as read over the characters that start no token between offset start and the current token: the tokens
// of a stage, a range or a call kept as text are no names the parser read. It is deferred by the functions that
// read over such text.
func (p *parser) keep(start int) {
	if p.sc.extra == nil {
		return
	}
	end := p.pos()
	for i, at := range p.sc.extra.unknown {
		if int(at) >= start && int(at) < end {
			p.sc.extra.unknown[i] = -1
		}
	}
}

// unknownName is the offset of the first character that starts no token the parser read as a name (see
// Token.Unknown), or -1.
func (p *parser) unknownName() int {
	if p.sc.extra == nil {
		return -1
	}
	for _, at := range p.sc.extra.unknown {
		if at >= 0 {
			return int(at)
		}
	}
	return -1
}

// pos is the byte offset of the current token in the input.
func (p *parser) pos() int { return int(p.sc.start) }

// canonState collects what CanonicalizeLogRanges rewrites.
type canonState struct {
	// rewrites are the spans of alternative log range forms, in order.
	rewrites []rewrite
	// queryStart and queryEnd delimit the log query parseLogQuery returned last.
	queryStart, queryEnd int
}

// rewrite replaces input[start:end] with text.
type rewrite struct {
	start, end int
	text       string
}

// addRewrite records a rewrite, dropping the ones it contains.
func (p *parser) addRewrite(start, end int, text string) {
	c := p.canon
	for len(c.rewrites) > 0 && c.rewrites[len(c.rewrites)-1].start >= start {
		c.rewrites = c.rewrites[:len(c.rewrites)-1]
	}
	c.rewrites = append(c.rewrites, rewrite{start, end, text})
}

func (p *parser) advance() Token {
	prev := p.cur
	p.prevEnd = p.sc.pos
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
	if p.sc.lexErrCount() > 0 {
		// Loki's lexer error is reported ahead of whatever the rest of the query holds: stop here.
		return nil, errLexerFirst
	}
	if p.cur.Typ == TokEOF {
		return nil, fmt.Errorf("logql: empty expression")
	}

	// Number literal (e.g. scalar in binary ops), possibly negative.
	if p.cur.Typ == TokNumber || p.cur.Typ == TokMinus {
		return p.parseNumberLiteral()
	}

	// Vector aggregation: starts with a known aggregation name followed by
	// optional grouping or '('.
	if p.cur.Typ == TokIdent {
		if op, ok := vectorOps[p.cur.Val]; ok {
			if !p.keywordAhead(true) {
				return nil, p.syntaxErrorAt("") // Loki reads the name as an identifier it cannot place
			}
			return p.parseVectorAggregation(op)
		}
		if op, ok := rangeOps[p.cur.Val]; ok {
			if !p.keywordAhead(false) {
				return nil, p.syntaxErrorAt("")
			}
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
		// Loki's grammar has three more metric-level calls (syntax.y
		// labelReplaceExpr, vectorExpr, vectorOp APPROX_TOPK); any other name
		// is an IDENTIFIER it cannot place (group, label_join, abs, ...).
		name := p.cur.Val
		if !opaqueMetricFunctions[name] {
			return nil, p.syntaxErrorAt("")
		}
		if name != "approx_topk" && !p.keywordAhead(false) {
			return nil, p.syntaxErrorAt("")
		}
		// Capture the full raw text and return it as-is so VictoriaLogs
		// receives the original expression unchanged.
		startPos := p.sc.pos - len(p.cur.Val)
		p.advance()
		if p.cur.Typ == TokLParen {
			p.advance() // consume '('
			argStart := p.pos()
			endPos, ok := p.consumeBalancedParens()
			if !ok || endPos < startPos {
				return nil, fmt.Errorf("logql: unterminated '(' after %q", name)
			}
			if p.canon != nil {
				// label_replace(rate(({a} |= "x")[1m]), ...): the first argument is a
				// metric expression, written in the plain form like any other.
				argEnd := argStart + FirstArgumentLen(p.sc.src[argStart:endPos])
				if arg := p.sc.src[argStart:argEnd]; CanonicalizeLogRanges(arg) != arg {
					p.addRewrite(argStart, argEnd, CanonicalizeLogRanges(arg))
				}
			}
			raw := strings.TrimSpace(p.sc.src[startPos:endPos])
			o := &OpaqueMetricExpr{Raw: raw, Func: name}
			if p.canon == nil {
				typeOpaqueCall(o)
			}
			return o, nil
		}
		return nil, p.syntaxErrorAt("(")
	}

	// Parenthesised expression
	if p.cur.Typ == TokLParen {
		open := p.pos()
		p.advance()
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Typ != TokRParen {
			return nil, p.syntaxErrorAt("")
		}
		if _, ok := inner.(*LogQuery); ok && p.canon != nil {
			p.addRewrite(open, p.pos()+1, strings.TrimSpace(p.sc.src[p.canon.queryStart:p.canon.queryEnd]))
		}
		p.advance()
		return inner, nil
	}

	if p.cur.Typ == TokLBrace {
		return p.parseLogQuery()
	}

	return nil, p.syntaxErrorAt("")
}

// parseNumberLiteral parses a number, or a minus and a number.
func (p *parser) parseNumberLiteral() (Expr, error) {
	sign := 1.0
	if p.cur.Typ == TokMinus {
		p.advance()
		sign = -1
		if p.cur.Typ != TokNumber {
			return nil, p.syntaxErrorAt("NUMBER")
		}
	}
	val, err := strconv.ParseFloat(p.cur.Val, 64)
	if err != nil {
		p.advance()
		return &BadLiteralExpr{Err: "parse error : unable to parse literal as a float: " + err.Error()}, nil
	}
	p.advance()
	return &LiteralExpr{Value: sign * val}, nil
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

// atKeywordName reports a word Loki's lexer returns as a keyword token (of, bool, offset, unwrap, json, regexp, logfmt,
// unpack, pattern, label_format, line_format, decolorize, drop, keep, variants, in any case) where a label name is
// expected: Loki's grammar wants an IDENTIFIER there and answers a 400, so no label can be called that (the proxy
// has no other reading of it). or, and, by and the other words with a token of their own are not identifiers here.
func (p *parser) atKeywordName() bool {
	return p.cur.Typ == TokIdent && keywordLabelWord(p.cur.Val)
}

// keywordLabelWord reports one of the words atKeywordName lists, in lower case (the scanner lower-cases a keyword
// whatever its case). A switch on the length avoids a map lookup per label name; it is lokiAlwaysKeywords without
// the words that have a token of their own (by, without, on, ignoring, group_left, group_right, unless).
func keywordLabelWord(v string) bool {
	switch len(v) {
	case 2:
		return v == "of"
	case 4:
		return v == "bool" || v == "json" || v == "drop" || v == "keep"
	case 6:
		return v == "offset" || v == "unwrap" || v == "regexp" || v == "logfmt" || v == "unpack"
	case 7:
		return v == "pattern"
	case 8:
		return v == "variants"
	case 10:
		return v == "decolorize"
	case 11:
		return v == "line_format"
	case 12:
		return v == "label_format"
	}
	return false
}

// parseLabelList parses a parenthesised comma-separated label name list: (label1, label2).
// Returns an empty slice if the next token is not '('.
func (p *parser) parseLabelList() ([]string, error) {
	if p.cur.Typ != TokLParen {
		return nil, nil
	}
	p.advance() // consume '('
	var labels []string
	afterComma, afterUnknown := false, false
	for p.cur.Typ == TokIdent {
		if p.atKeywordName() {
			return nil, p.syntaxErrorAt(listNameExpecting(len(labels) == 0))
		}
		if (p.cur.Unknown || afterUnknown) && len(labels) > 0 && !afterComma {
			return nil, p.syntaxErrorAt(listCloseExpecting(labels, false)) // a character that starts no token is no comma
		}
		afterUnknown = p.cur.Unknown
		labels = append(labels, p.advance().Val)
		afterComma = p.cur.Typ == TokComma
		if afterComma {
			p.advance()
		}
	}
	if p.cur.Typ != TokRParen {
		return nil, p.syntaxErrorAt(listCloseExpecting(labels, afterComma))
	}
	p.advance()
	return labels, nil
}

// parseLogQuery parses {selector} [pipeline stages].
func (p *parser) parseLogQuery() (*LogQuery, error) {
	start := p.pos()
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
	if p.canon != nil {
		p.canon.queryStart, p.canon.queryEnd = start, p.pos()
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
		if p.atKeywordName() {
			expecting := "IDENTIFIER"
			if len(sel.Matchers) == 0 {
				expecting = "IDENTIFIER or }"
			}
			return nil, p.syntaxErrorAt(expecting)
		}
		expecting := "IDENTIFIER"
		if len(sel.Matchers) == 0 {
			expecting = "IDENTIFIER or }"
		}
		m, err := p.parseLabelMatcher(expecting)
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

	if p.cur.Typ != TokRBrace {
		return nil, p.syntaxErrorAt("} or ,")
	}
	p.advance()

	return sel, nil
}

// parseLabelMatcher parses one matcher of a stream selector; expecting is what Loki names as the alternatives to
// the label name.
func (p *parser) parseLabelMatcher(expecting string) (LabelMatcher, error) {
	if p.cur.Typ != TokIdent {
		return LabelMatcher{}, p.syntaxErrorAt(expecting)
	}
	name := p.advance()

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
		return LabelMatcher{}, p.syntaxErrorAt("= or =~ or !~ or !=")
	}
	p.advance()

	// Loki's grammar takes a STRING here and nothing else: the ip("...") form
	// belongs to line and label filters, not to a stream selector.
	if p.cur.Typ != TokString && p.cur.Typ != TokRawString {
		return LabelMatcher{}, p.selectorValueError()
	}

	val := p.advance()
	return LabelMatcher{Name: name.Val, Op: op, Value: val.Val, Escaped: val.Escaped || val.Typ == TokRawString}, nil
}

// selectorValueError reports a stream-selector matcher value that is not a
// string the way Loki does. Loki's lexer returns a function name (ip, rate,
// sum, label_replace, ...) as its keyword token only when a '(' follows it, and
// a duration as DURATION with no line.
func (p *parser) selectorValueError() error {
	name := lokiTokenName(p.cur)
	zeroLine := p.cur.Typ == TokDuration || p.cur.Typ == TokDurationLit || p.cur.Typ == TokBytes || p.cur.Typ == TokLBracket || isLineFilterOperator(p.cur.Typ)
	switch p.cur.Typ {
	case TokDuration:
		name = "DURATION"
	case TokIdent:
		name = p.currentTokenName()
	}
	pos := p.pos()
	if p.cur.Typ == TokEOF {
		pos = len(p.sc.src)
	}
	if p.cur.Typ == TokError || p.cur.Typ == TokAt {
		// Loki's lexer reports a bad string literal itself, at the start of the string, and reads any other
		// character that starts no token as an identifier.
		switch p.cur.Val {
		case "unterminated quoted string":
			return p.errorMsgAt(pos, "literal not terminated", false)
		case "invalid quoted string":
			return p.errorMsgAt(pos, "invalid char escape", false)
		}
		name = "IDENTIFIER"
	}
	return p.errorAt(pos, name, "STRING", zeroLine)
}

// isLexerFunctionName reports whether Loki's lexer turns the identifier into a
// keyword token only when a '(' follows it.
func isLexerFunctionName(name string) bool {
	if strings.EqualFold(name, "ip") || name == "label_replace" || name == "vector" {
		return true
	}
	if _, ok := rangeOps[name]; ok {
		return true
	}
	_, ok := vectorOps[name]
	return ok
}

// parsePipelineStage parses one pipeline stage. Returns nil, nil when no more
// pipeline stages are found (i.e. EOF or unexpected token).
func (p *parser) parsePipelineStage() (Stage, error) {
	if isLineFilterOperator(p.cur.Typ) {
		return p.parseLineFilterStage()
	}
	switch p.cur.Typ {
	case TokPipe:
		p.pipePos = p.pos()
		p.advance()
		return p.parsePipeBody()
	}

	return nil, nil
}

// parsePipeBody parses what comes after a bare `|`.
func (p *parser) parsePipeBody() (Stage, error) {
	if p.cur.Typ == TokLParen {
		// A parenthesized label filter expression: `| (a="1" or b="2"), c>5`.
		return p.parseLabelFilterStage(p.pos(), "", TokLParen)
	}
	if p.cur.Typ != TokIdent {
		if p.cur.Typ == TokDuration || p.cur.Typ == TokDurationLit || p.cur.Typ == TokBytes {
			// A literal, not a range: Loki names it by what it holds.
			name := lokiTokenName(p.cur)
			if p.cur.Typ == TokDuration {
				name = LabelFilterWordName(p.cur.Val)
			}
			return nil, p.errorAt(p.pos(), name, "", zeroLineToken(name))
		}
		return nil, p.syntaxErrorAt("")
	}

	kw := p.cur.Val
	stageStart := p.pos()

	switch kw {
	case "json", "logfmt":
		return p.parseJSONOrLogfmt(kw == "logfmt")

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
		if p.rangeDepth == 0 && p.plainUnwrapAt == 0 {
			p.plainUnwrapAt = int32(p.pos()) + 1
		}
		p.advance()
		return p.parseUnwrap()

	case "line_format":
		p.advance()
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		stage := &LineFormatStage{Template: val}
		if p.canon == nil {
			stage.Tmpl = typeLineTemplate(val)
		}
		return stage, nil

	case "label_format":
		p.advance()
		return p.parseLabelFormat()
	}

	return p.parseNameStage(stageStart, kw)
}

// parseNameStage parses the stage that opens with a name that is no parser or formatting keyword: a label filter, or
// an opaque stage that passes through to the backend.
func (p *parser) parseNameStage(stageStart int, kw string) (Stage, error) {
	// Unknown keyword after `|`: consume the identifier and the rest of the
	// stage. If followed by a comparison/assignment operator or dotted-label
	// continuation, this is a label filter. Otherwise treat as an opaque stage
	// (unknown parser keywords, custom extensions) so the query passes through
	// to the backend rather than being rejected by the proxy.
	unknownKw := p.cur.Unknown
	p.advance() // consume the ident (this is the label name in a label filter)
	bang := p.cur.Typ == TokError && p.cur.Val == "!"
	if unknownKw && !isLabelFilterOperator(p.cur.Typ) && !bang {
		// A character that starts no token is a name to Loki's lexer; the stage it opens needs an operator.
		return nil, p.syntaxErrorAt("")
	}
	// A label filter must have an operator (=, !=, =~, !~, >=, <=, >, <) next.
	// Two consecutive identifiers (e.g. `| distinct method`) is invalid syntax —
	// Loki rejects this with "syntax error: unexpected IDENTIFIER".
	if p.atOtherName() {
		return nil, p.syntaxErrorAt("")
	}
	if strings.EqualFold(kw, "ip") && p.cur.Typ == TokLParen {
		// ip("...") is a line filter operand (`|= ip("...")`) and a label filter
		// value, not a stage: Loki's lexer reads the function name as the token.
		return nil, p.errorAt(stageStart, "ip", "", false)
	}
	if isLabelFilterOperator(p.cur.Typ) || p.cur.Typ == TokError && p.cur.Val == "!" {
		return p.parseLabelFilterStage(stageStart, kw, TokIdent)
	}
	return &LabelFilterStage{Raw: kw + p.consumeRestOfStage(TokIdent)}, nil
}

// stageNameFollower reports the characters that may follow the name that opens a stage although they start no token:
// the `!` of `!=` and `!~` read apart, and the dot and colon of the proxy's dotted names and sub-query texts.
func stageNameFollower(v string) bool { return v == "!" || v == "." || v == ":" }

// atOtherName reports a name at the current token, or a character that starts no token, which Loki's lexer reads as an
// identifier of its own: a second name after the one that opens a stage.
func (p *parser) atOtherName() bool {
	return p.cur.Typ == TokIdent || p.cur.Typ == TokError && !stageNameFollower(p.cur.Val)
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

	if p.atKeywordName() {
		return nil, p.syntaxErrorAt("IDENTIFIER or BYTES_CONV or DURATION_CONV or DURATION_SECONDS_CONV")
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

		if p.atKeywordName() {
			return nil, p.syntaxErrorAt("IDENTIFIER")
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
	defer p.keep(p.pos())
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
		pos, keyword := p.pos(), p.functionKeyword() // for the error below, which the next token decides
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
		return "", p.errorAt(pos, identTokenName(name, keyword), "STRING", false)
	}
	return "", p.syntaxErrorAt("STRING")
}

// parseDropKeepList parses the item list for drop/keep stages.
// Each item is either a bare label name or a conditional matcher: field="value", field!="val", field=~"re", field!~"re".
func (p *parser) parseDropKeepList() (labels []string, matchers []DropMatcher, err error) {
	first := true
	for p.cur.Typ == TokIdent {
		if p.atKeywordName() {
			err = p.syntaxErrorAt("IDENTIFIER")
			return
		}
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
		if p.atKeywordName() {
			expecting := "IDENTIFIER"
			if len(fields) == 0 {
				expecting = "" // Loki's grammar allows a bare parser here, so it names no expected token
			}
			return "", nil, p.syntaxErrorAt(expecting)
		}
		end = p.sc.pos
		name := p.advance().Val // field name
		field := ExtractionField{Name: name, Expression: name}
		if p.cur.Typ == TokEq {
			p.advance()
			if p.cur.Typ != TokString && p.cur.Typ != TokRawString {
				return "", nil, p.syntaxErrorAt("STRING")
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
		if p.atKeywordName() {
			return "", nil, p.syntaxErrorAt("IDENTIFIER")
		}
		if p.cur.Typ != TokIdent {
			return "", nil, p.syntaxErrorAt("IDENTIFIER")
		}
	}
	return strings.TrimSpace(p.sc.src[start:end]), fields, nil
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
		if p.atKeywordName() {
			return nil, p.syntaxErrorAt("IDENTIFIER")
		}
		if p.cur.Typ != TokIdent {
			return nil, p.syntaxErrorAt("IDENTIFIER")
		}
		name := p.cur.Val
		p.appendStageToken(&raw)
		if p.cur.Typ != TokEq {
			return nil, p.syntaxErrorAt("=")
		}
		p.appendStageToken(&raw)
		format := LabelFormat{Name: name}
		switch p.cur.Typ {
		case TokString, TokRawString:
		case TokIdent:
			if p.atKeywordName() {
				return nil, p.syntaxErrorAt("IDENTIFIER or STRING")
			}
			format.Rename = true
		default:
			return nil, p.syntaxErrorAt("IDENTIFIER or STRING")
		}
		format.Value = p.cur.Val
		p.appendStageToken(&raw)
		formats = append(formats, format)
		if p.cur.Typ != TokComma {
			return &LabelFormatStage{Raw: raw.String(), Formats: formats}, nil
		}
		p.appendStageToken(&raw)
	}
}

// syntaxError reports an unexpected current token the way Loki's yacc parser
// does ("syntax error: unexpected $end, expecting STRING").
// parseJSONOrLogfmt parses a json or logfmt stage after its keyword: logfmt's
// flags, then the optional field list.
func (p *parser) parseJSONOrLogfmt(logfmt bool) (Stage, error) {
	start := p.sc.pos
	p.advance()
	stage := &ParserStage{Type: ParserJSON}
	if logfmt {
		stage.Type = ParserLogfmt
		var err error
		if start, err = p.logfmtFlags(stage, start); err != nil {
			return nil, err
		}
	} else if p.cur.Typ == TokMinus && (flagAt(p.sc.src[p.pos():], "--strict") || flagAt(p.sc.src[p.pos():], "--keep-empty")) {
		// The flags are logfmt's only: Loki's grammar has no json flag.
		return nil, p.errorAt(p.pos(), "FUNCTION_FLAG", "", true)
	}
	param, fields, err := p.consumeExplicitFieldList(start)
	if err != nil {
		return nil, err
	}
	stage.Param, stage.Fields = param, fields
	return stage, nil
}

// logfmtFlags reads the --strict and --keep-empty flags after `logfmt` into
// stage, and returns where the field list starts.
func (p *parser) logfmtFlags(stage *ParserStage, start int) (int, error) {
	for p.cur.Typ == TokMinus {
		at := p.pos()
		rest := p.sc.src[at:]
		flag := ""
		switch {
		case flagAt(rest, "--strict"):
			flag, stage.Strict = "--strict", true
		case flagAt(rest, "--keep-empty"):
			flag, stage.KeepEmpty = "--keep-empty", true
		case strings.HasPrefix(rest, "--"):
			// Loki reads `--foo` as two minus signs before a name.
			return 0, p.errorAt(at+2, "IDENTIFIER", "NUMBER", false)
		default:
			return start, nil
		}
		if p.flagSpans != nil {
			*p.flagSpans = append(*p.flagSpans, [2]int{at, at + len(flag)})
		}
		p.sc.pos = at + len(flag)
		start = p.sc.pos
		p.advance()
	}
	return start, nil
}

// flagAt reports s starting with flag, not followed by a name character.
func flagAt(s, flag string) bool {
	if !strings.HasPrefix(s, flag) {
		return false
	}
	if len(s) == len(flag) {
		return true
	}
	c := s[len(flag)]
	return c != '_' && c != '-' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z')
}

// offsetDuration reads the duration after `offset` as Loki's lexer does: a
// duration (compound and fractional ones included: 1h2m20s, 1.5h, 1d), or a
// minus written right before one Go's time.ParseDuration reads (-1s, -1h30m;
// not -1d, not "- 1s"), which shifts the window forward.
func (p *parser) offsetDuration() (string, time.Duration, error) {
	if p.cur.Typ == TokMinus {
		minus := p.pos()
		p.advance()
		if p.cur.Typ == TokDurationLit && p.pos() == minus+1 {
			if d, err := time.ParseDuration("-" + p.cur.Val); err == nil {
				text := "-" + p.cur.Val
				p.advance()
				return text, d, nil
			}
		}
		return "", 0, p.errorAt(minus, "-", "DURATION", false)
	}
	if p.cur.Typ != TokDurationLit {
		return "", 0, p.syntaxErrorAt("DURATION")
	}
	val := p.cur.Val
	if val[0] == '-' {
		// One token with its minus sign: a negative duration shifts the window forward.
		d, _ := time.ParseDuration(val)
		p.advance()
		if p.cur.Typ == TokDurationLit {
			return "", 0, p.errorAt(p.pos(), "DURATION", ")", true)
		}
		return val, d, nil
	}
	d, err := time.ParseDuration(val)
	if err != nil || d < 0 {
		var ok bool
		if d, ok = ParseUnitDuration(val); !ok {
			return "", 0, p.errorAt(p.pos(), "NUMBER", "DURATION", false)
		}
	}
	p.advance()
	// A second duration right after (`offset 1h 2m`, `offset 1h-2m`): Loki's lexer
	// reads it as one more DURATION token.
	if p.cur.Typ == TokDurationLit || (p.cur.Typ == TokMinus && signedDurationAt(p.sc.src, p.pos())) {
		return "", 0, p.errorAt(p.pos(), "DURATION", ")", true)
	}
	return val, d, nil
}

// signedDurationAt reports a minus at src[i] followed right away by text Go's
// time.ParseDuration reads with the sign: what Loki's lexer takes as one DURATION.
func signedDurationAt(src string, i int) bool {
	j := i + 1
	for j < len(src) && (src[j] == '.' || src[j] >= '0' && src[j] <= '9' || src[j] >= 'a' && src[j] <= 'z' || src[j] >= 'A' && src[j] <= 'Z' || src[j] >= 0x80) {
		j++
	}
	if j == i+1 {
		return false
	}
	_, err := time.ParseDuration(src[i:j])
	return err == nil
}

// syntaxErrorAt is syntaxError with the position Loki reports: the 1-based
// column of the offending token on line 1, or line 0 for the line-filter
// operators and a range, whose lexer tokens carry no line.
func (p *parser) syntaxErrorAt(expecting string) error {
	pos := p.pos()
	if p.cur.Typ == TokEOF {
		pos = len(p.sc.src)
	}
	if p.cur.Typ == TokError {
		// Loki's lexer reports a bad string literal itself, at the start of the string.
		switch p.cur.Val {
		case "unterminated quoted string":
			return p.errorMsgAt(pos, "literal not terminated", false)
		case "invalid quoted string":
			return p.errorMsgAt(pos, "invalid char escape", false)
		}
	}
	zeroLine := p.cur.Typ != TokEOF && (isLineFilterOperator(p.cur.Typ) || p.cur.Typ == TokLBracket || p.cur.Typ == TokDurationLit || p.cur.Typ == TokBytes)
	return p.errorAt(pos, p.currentTokenName(), expecting, zeroLine)
}

// currentTokenName is the name Loki's parser prints for the current token. Loki's lexer returns the name of a
// function (rate, sum, label_replace, ip, ...) as its keyword token only when a '(' follows it; any other
// occurrence is an identifier.
func (p *parser) currentTokenName() string {
	if p.cur.Typ == TokIdent {
		return identTokenName(p.cur.Val, isLexerFunctionName(p.cur.Val) && p.functionKeyword())
	}
	return lokiTokenName(p.cur)
}

// identTokenName is the name Loki's parser prints for the identifier name: the keyword of a function when keyword is
// set, IDENTIFIER for a name of one that is not followed by its parenthesis.
func identTokenName(name string, keyword bool) string {
	if isLexerFunctionName(name) {
		if keyword {
			return lokiTokenName(Token{Typ: TokIdent, Val: strings.ToLower(name)})
		}
		return "IDENTIFIER"
	}
	return lokiTokenName(Token{Typ: TokIdent, Val: name})
}

// functionKeyword reports whether Loki's lexer reads the function name at the current token as its keyword.
func (p *parser) functionKeyword() bool {
	_, vector := vectorOps[p.cur.Val]
	return p.keywordAhead(vector)
}

// keywordAhead is functionKeyword for a name known to be an aggregation (vector) or not: a '(' follows it, or for
// an aggregation `by (` or `without (`. The lexer looks ahead over every character Go calls white space, not only
// over the four it skips between tokens: the name is a keyword then, and the character that is not white space to
// the lexer is the unexpected token after it.
func (p *parser) keywordAhead(vector bool) bool {
	rest := p.sc.src[p.sc.pos:]
	if len(rest) > 0 && rest[0] == '(' {
		return true
	}
	for len(rest) > 0 && rest[0] == ' ' {
		rest = rest[1:]
	}
	rest = strings.TrimLeftFunc(rest, unicode.IsSpace)
	if strings.HasPrefix(rest, "(") {
		return true
	}
	if vector {
		for _, word := range [...]string{"by", "without"} {
			if startsWord(rest, word) {
				return strings.HasPrefix(strings.TrimLeftFunc(rest[len(word):], unicode.IsSpace), "(")
			}
		}
	}
	return false
}

// startsWord reports whether s starts with word, in any case, as a whole word.
func startsWord(s, word string) bool {
	if len(s) < len(word) || !strings.EqualFold(s[:len(word)], word) {
		return false
	}
	if len(s) == len(word) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(s[len(word):])
	return r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r)
}

// errorAt reports Loki's "syntax error: unexpected <token>" for the token that
// starts at byte offset pos of the input. zeroLine renders the line as 0, the
// way Loki does for tokens its lexer reads with a lookahead.
func (p *parser) errorAt(pos int, token, expecting string, zeroLine bool) error {
	msg := "syntax error: unexpected " + token
	if expecting != "" {
		msg += ", expecting " + expecting
	}
	return p.errorMsgAt(pos, msg, zeroLine)
}

// errorMsgAt reports msg at the line and column of byte offset pos.
func (p *parser) errorMsgAt(pos int, msg string, zeroLine bool) error {
	before := p.sc.src[:pos]
	line := 1 + strings.Count(before, "\n")
	col := utf8.RuneCountInString(before[strings.LastIndexByte(before, '\n')+1:]) + 1
	if zeroLine {
		line = 0
	}
	return newParseError(line, col, msg)
}

// errorKeywords are the identifiers Loki's lexer turns into keyword tokens that
// its parse errors print as written (lex.go tokens), not as IDENTIFIER.
var errorKeywords = map[string]bool{
	"json": true, "logfmt": true, "regexp": true, "unpack": true, "pattern": true,
	"line_format": true, "label_format": true, "unwrap": true, "offset": true,
	"bool": true, "ip": true, "decolorize": true, "drop": true, "keep": true,
	"of": true, "variants": true,
}

// listNameExpecting is what Loki's parse error names as expected where a name of a parenthesised label list is not
// an identifier: the first name or the closing parenthesis, or the next name after a comma.
// listCloseExpecting is what Loki expects where a list of names does not go on or end: a name or the closing
// parenthesis of an empty list, a name after a comma, a comma or the parenthesis after a name.
func listCloseExpecting(labels []string, afterComma bool) string {
	switch {
	case len(labels) == 0:
		return "IDENTIFIER or )"
	case afterComma:
		return "IDENTIFIER"
	}
	return ", or )"
}

func listNameExpecting(first bool) string {
	if first {
		return "IDENTIFIER or )"
	}
	return "IDENTIFIER"
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
	case TokDuration, TokLBracket:
		return "RANGE"
	case TokAt:
		return "IDENTIFIER" // Loki v3.7 has no @ modifier: its lexer reads it as a name
	case TokError:
		// A character that starts no token is an identifier of its own to Loki's lexer; a dot is a token of its own.
		if tok.Val == "." {
			return "."
		}
		return "IDENTIFIER"
	case TokDurationLit:
		return "DURATION"
	case TokBytes:
		return "BYTES"
	case TokIdent:
		if tok.Val == "label_replace" {
			return tok.Val
		}
		if errorKeywords[tok.Val] {
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
// p.sc.src immediately after the closing ')' and ok=true. If the parentheses
// never balance — EOF is reached with the depth still positive — it returns
// ok=false so the caller can emit a parse error instead of slicing p.sc.src with
// a stale endPos of 0 (which panics for any startPos > 0). Used for opaque
// function calls.
func (p *parser) consumeBalancedParens() (endPos int, ok bool) {
	defer p.keep(p.pos()) // the call's text is kept as it is: its tokens are no names
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
// parser. Stops at [ and an unmatched ) so that the outer range aggregation
// parser can consume the [duration] and closing ) tokens; a ) that closes a
// ( of the stage belongs to it. prev is the type of the token that precedes
// the first one read.
func (p *parser) consumeRestOfStage(prev TokType) string {
	defer p.keep(p.pos())
	var b strings.Builder
	depth := 0
	for {
		switch p.cur.Typ {
		case TokEOF, TokPipe, TokPipeEq, TokPipeTilde, TokPipeGt, TokBangGt, TokLBracket:
			return b.String()
		case TokLParen:
			depth++
		case TokRParen:
			if depth == 0 {
				return b.String()
			}
			depth--
		case TokBangEq, TokBangTilde:
			// After a complete filter (`label=value`) the `!=`/`!~` starts a NEW
			// line filter stage; stop. After a label name it IS the label filter
			// operator (e.g. `status!=200`, `a="1", status!=200`); continue.
			switch prev {
			case TokString, TokRawString, TokNumber, TokDuration, TokDurationLit, TokBytes, TokRParen:
				return b.String()
			}
		}
		prev = p.cur.Typ
		p.appendStageToken(&b)
	}
}

// isLabelFilterOperator reports whether t is a comparison operator of a label
// filter (`=`, `!=`, `=~`, `!~`, `==`, `<`, `<=`, `>`, `>=`).
func isLabelFilterOperator(t TokType) bool {
	switch t {
	case TokEq, TokNeq, TokReMatch, TokReNotMatch, TokBangEq, TokBangTilde, TokEqEq, TokLt, TokLtEq, TokGt, TokGtEq:
		return true
	}
	return false
}

// parseLabelFilterStage reads a label filter stage that starts at byte offset
// start of the input with the identifier kw (empty when the stage opens with a
// parenthesis); prev is the type of the token before the rest of it. It reads
// the stage up to the next pipeline operator, range bracket or unmatched ) and
// checks its syntax the way Loki's parser does.
func (p *parser) parseLabelFilterStage(start int, kw string, prev TokType) (Stage, error) {
	known := p.sc.lexErrCount()
	saved := *p.sc
	raw := kw + p.consumeRestOfStage(prev)
	// The filter lexer reads the text of the stage: the errors of the scanner inside it are not Loki's.
	if curStart := int(p.sc.start); known == 0 && p.sc.lexErrCount() > 0 && int(p.sc.extra.err.off) < curStart {
		// The error is inside the text the filter lexer reads; the token after the stage may have one of its own.
		p.sc.dropLexErr(0)
		p.sc.pos = curStart
		p.cur = p.sc.next()
	}
	text := p.sc.src[start:p.pos()]
	expr, end, serr := scanLabelFilter(text, p.pipePos-start, true)
	if serr == nil && end < len(text) {
		// A binary operator of the query follows the filter: the query goes on from there.
		*p.sc = saved
		p.sc.dropLexErr(known)
		p.sc.pos = start + end
		p.sc.inRange = false
		raw = text[:end] // the query is rejected for the operand anyway
		p.prevEnd = start + end
		p.cur = p.sc.next()
	}
	if serr != nil {
		if serr.Offset >= len(text) {
			// The stage ended too early: the token that ends it is the unexpected one.
			return nil, p.syntaxErrorAt(serr.Expecting)
		}
		// Loki stops at the offending token, before it reads the one after the stage.
		p.sc.dropLexErr(known)
		if serr.Message != "" {
			return nil, p.errorMsgAt(start+serr.Offset, serr.Message, serr.ZeroLine)
		}
		return nil, p.errorAt(start+serr.Offset, serr.Token, serr.Expecting, zeroLineToken(serr.Token))
	}
	return &LabelFilterStage{Raw: raw, Expr: expr}, nil
}

// zeroLineToken reports whether Loki prints line 0 for a parse error at this
// token: its lexer reads multi-character operators, durations and bytes with
// a lookahead that invalidates the scanner position.
func zeroLineToken(name string) bool {
	switch name {
	case "DURATION", "BYTES", "RANGE", "=~", "!~", "!=", "==", ">=", "<=", "|=", "|~", "|>", "!>":
		return true
	}
	return false
}

// appendStageToken appends the current token's source rendering to b and
// advances. It inserts a space when adjacent tokens would merge without one:
//
//	(a) alphanumeric, `.` or non-ASCII + alphanumeric: `200`+`and` → `200and`,
//	    `0µ`+`A` → `0µA` (unparseable)
//	(b) string-end + alphanumeric:   `"error"`+`or` → `"error"or` (ambiguous)
//
// Rule (a) does not apply to tokens that touch in the source: the scanner cuts
// one literal into several (`1h5m` is `1h` `5m`, `1e3` is `1e` `3`), and a
// space there would change it (`1h 5m`).
func (p *parser) appendStageToken(b *strings.Builder) {
	touches := p.prevEnd == p.pos()
	raw := tokenRaw(p.advance())
	if b.Len() > 0 && raw != "" && isAlphanumeric(raw[0]) {
		prev := b.String()[b.Len()-1]
		if ((isAlphanumeric(prev) || prev == '.' || prev >= 0x80) && !touches) || prev == '"' || prev == '`' {
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
			return newParseError(0, 0, "invalid parameter for operation quantile_over_time: "+err.Error())
		}
		p.advance()
		ra.Param = phi
		ra.HasParam = true
		if p.cur.Typ != TokComma {
			return p.syntaxErrorAt(",")
		}
		p.advance()
		return nil
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
	start := p.pos()
	p.advance() // consume function name

	if p.cur.Typ != TokLParen {
		return nil, p.syntaxErrorAt("(")
	}
	p.advance()

	ra := &RangeAggregation{Op: op}
	p.rangeDepth++
	defer func() { p.rangeDepth-- }()

	if op == RangeQuantileOverTime {
		if err := p.parseQuantileParam(ra); err != nil {
			return nil, err
		}
	}

	if err := p.checkRangeAggregationArgument(!ra.HasParam); err != nil {
		return nil, err
	}
	innerStart := p.pos()
	form, err := p.parseLogRange(ra)
	if err != nil {
		return nil, err
	}
	if p.cur.Typ != TokRParen {
		return nil, p.syntaxErrorAt(")")
	}
	if form.alt && p.canon != nil {
		// op(param, selector pipeline trailing-pipeline [range] offset): the
		// plain form, from the text of the query as written.
		query := form.log
		if form.trailing != "" {
			query += " " + form.trailing
		}
		p.addRewrite(start, p.pos()+1, p.sc.src[start:innerStart]+query+" "+form.rng+")")
	}
	p.advance()
	return ra, nil
}

// parseLogRange parses Loki's logRangeExpr (syntax.y): a log selector with its
// pipeline, the [range], an optional offset, and any parentheses around it.
// Parentheses come in two kinds. `(selector pipeline unwrap)[range]` closes
// before the range and then accepts only an unwrap, and only after a bare
// selector. `(selector[range] ...)` wraps the whole range expression, may nest,
// and may carry the pipeline after the range (`selector[range] | json`) when
// the selector had none. Anything else is rejected where Loki's parser stops.
// rangeForm is the source text of the parts of a log range, and whether it is
// written in one of the alternative forms.
type rangeForm struct {
	alt                bool
	log, trailing, rng string
}

func (p *parser) parseLogRange(ra *RangeAggregation) (rangeForm, error) {
	var form rangeForm
	open := 0
	for p.cur.Typ == TokLParen {
		p.advance()
		open++
	}
	lq, err := p.parseLogQuery()
	if err != nil {
		return form, err
	}
	ra.Inner = lq
	if p.canon != nil {
		form.log = strings.TrimSpace(p.sc.src[p.canon.queryStart:p.canon.queryEnd])
	}
	bare := len(lq.Pipeline) == 0
	form.alt = open > 0
	closed := open > 0 && p.cur.Typ == TokRParen
	if closed {
		p.advance()
		open--
		if p.cur.Typ != TokLBracket {
			return form, p.syntaxErrorAt("RANGE")
		}
	}
	rangeStart := p.pos()
	if err := p.parseRangeSuffix(ra); err != nil {
		return form, err
	}
	form.rng = strings.TrimSpace(p.sc.src[rangeStart:p.pos()])
	trailStart := p.pos()
	switch {
	case !p.atStageStart():
	case closed && bare && p.cur.Typ == TokPipe:
		p.advance()
		if p.cur.Typ != TokIdent || p.cur.Val != "unwrap" {
			return form, p.syntaxErrorAt("unwrap")
		}
		unwrap, err := p.parsePipeBody()
		if err != nil {
			return form, err
		}
		lq.Pipeline = append(lq.Pipeline, unwrap)
		fallthrough
	case !closed && bare:
		form.alt = true
		for p.atStageStart() {
			stage, err := p.parsePipelineStage()
			if err != nil {
				return form, err
			}
			lq.Pipeline = append(lq.Pipeline, stage)
		}
		form.trailing = strings.TrimSpace(p.sc.src[trailStart:p.pos()])
	default:
		return form, p.syntaxErrorAt(")")
	}
	for ; open > 0; open-- {
		if p.cur.Typ != TokRParen {
			return form, p.syntaxErrorAt(")")
		}
		p.advance()
	}
	return form, nil
}

// atStageStart reports whether the current token starts a pipeline stage.
func (p *parser) atStageStart() bool {
	return p.cur.Typ == TokPipe || isLineFilterOperator(p.cur.Typ)
}

// parseRangeSuffix parses `[duration]`, an optional offset and the @ modifier.
func (p *parser) parseRangeSuffix(ra *RangeAggregation) error {
	// Expect [duration]. bracketCol is the 1-based column of '[' (the scanner
	// position just past it), which Loki reports for a malformed duration.
	before := p.sc.src[:p.sc.pos]
	bracketCol := utf8.RuneCountInString(before[strings.LastIndexByte(before, '\n')+1:])
	textStart := p.sc.pos
	if p.cur.Typ != TokLBracket {
		expecting := ""
		if lq, ok := ra.Inner.(*LogQuery); ok && len(lq.Pipeline) > 0 {
			if _, ok := lq.Pipeline[len(lq.Pipeline)-1].(*UnwrapStage); ok {
				expecting = "RANGE or |"
			}
		}
		return p.syntaxErrorAt(expecting)
	}
	p.advance()
	// Loki's lexer reads the text up to the ']' as one token and parses it as a duration: a character in it that is
	// not part of one is an error of that token.
	dur, err := p.expect(TokDuration)
	if err != nil {
		// A Grafana variable ([$__auto]) keeps its older error: label_replace passes its argument through with it.
		if raw, closed := rangeToken(p.sc.src, textStart); closed && !strings.HasPrefix(raw, "$") {
			if msg := rangeLexError(raw); msg != "" {
				return newParseError(0, bracketCol, msg)
			}
		}
		return fmt.Errorf("logql: expected duration: %w", err)
	}
	colon := p.cur.Typ == TokError && p.cur.Val == ":"
	if p.cur.Typ != TokRBracket && !colon {
		// Something else than the ']' follows the duration (a colon is reported below).
		if raw, closed := rangeToken(p.sc.src, textStart); closed {
			if msg := rangeLexError(raw); msg != "" {
				return newParseError(0, bracketCol, msg)
			}
		}
	}
	ra.Range = dur.Val
	ra.RangeDuration = parseDuration(dur.Val)
	if !lokiDuration(dur.Val) {
		// Loki reads a range with Go's time.ParseDuration or Prometheus' y, w and d units, in lower case.
		return newParseError(0, bracketCol, fmt.Sprintf("unknown unit %q in duration %q", durationUnit(dur.Val), dur.Val))
	}

	// A PromQL-style [range:step] is not LogQL. Loki's lexer rejects the
	// duration token itself; the colon arrives here as TokError ":".
	if p.cur.Typ == TokError && p.cur.Val == ":" {
		literal := ra.Range + ":"
		if p.advance(); p.cur.Typ == TokDuration {
			literal += p.cur.Val
		}
		return newParseError(0, bracketCol, fmt.Sprintf("unknown unit %q in duration %q", durationUnit(ra.Range)+":", literal))
	}

	if _, err := p.expect(TokRBracket); err != nil {
		return err
	}

	// Optional offset modifier: [duration] offset 1h
	if p.cur.Typ == TokIdent && p.cur.Val == "offset" {
		p.advance() // consume "offset"
		text, d, err := p.offsetDuration()
		if err != nil {
			return err
		}
		ra.Offset = text
		ra.OffsetDuration = d
	}

	// Loki v3.7 has no @ modifier: its lexer reads `@` as a name where `)` must follow.
	if p.cur.Typ == TokAt {
		return p.errorAt(p.pos(), "IDENTIFIER", ")", false)
	}
	return nil
}

// checkRangeAggregationArgument rejects a range aggregation whose argument is
// not a (parenthesised) log selector before it is parsed, reporting the first
// offending token like Loki's grammar does: `max_over_time(sum(...)[30m:5m])`
// → "parse error at line 1, col 15: syntax error: unexpected SUM, expecting
// NUMBER or { or (". The probe runs on a scanner copy and consumes nothing.
func (p *parser) checkRangeAggregationArgument(numberAllowed bool) error {
	sc := *p.sc
	sc.quiet = true
	probe := parser{sc: &sc, cur: p.cur}
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
		return newParseError(1, len(p.sc.src)+1, msg)
	case TokIdent, TokNumber:
		return newParseError(1, sc.pos-len(probe.cur.Val)+1, msg)
	case TokDurationLit, TokBytes:
		return newParseError(0, sc.pos-len(probe.cur.Val)+1, msg)
	}
	return newParseError(0, 0, msg)
}

// durationUnit returns the trailing unit letters of a duration literal
// ("30m" → "m"), used to mirror Loki's duration lexer error text.
// lokiDuration reports a duration Loki's lexer reads: Go's time.ParseDuration
// form (ns, us, µs, ms, s, m, h, fractions, any order) or Prometheus' (y, w, d,
// h, m, s, ms in that order, integers).
func lokiDuration(d string) bool {
	if _, err := time.ParseDuration(d); err == nil {
		return true
	}
	return prometheusDurationRE.MatchString(d) && d != ""
}

var prometheusDurationRE = regexp.MustCompile(`^(?:\d+y)?(?:\d+w)?(?:\d+d)?(?:\d+h)?(?:\d+m)?(?:\d+s)?(?:\d+ms)?$`)

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

	if p.cur.Typ != TokLParen {
		if va.Grouping == nil {
			return nil, p.syntaxErrorAt("( or by or without")
		}
		return nil, p.syntaxErrorAt("(")
	}
	p.advance()

	// topk/bottomk carry a numeric parameter before the inner expression.
	if op == VectorTopK || op == VectorBottomK {
		if p.cur.Typ != TokNumber {
			if p.cur.Typ == TokLBrace || p.cur.Typ == TokIdent && isLexerFunctionName(p.cur.Val) {
				return nil, newParseError(0, 0, "parameter required for operation "+string(op))
			}
			return nil, p.syntaxErrorAt("")
		}
		kTok := p.advance()
		if p.cur.Typ == TokRParen {
			return nil, newParseError(0, 0, "parameter required for operation "+string(op))
		}
		if p.cur.Typ != TokComma {
			return nil, p.syntaxErrorAt("")
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
		p.advance()
	}

	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	va.Inner = inner

	// Loki's grammar takes only a metric expression inside a vector
	// aggregation (pkg/logql/syntax/syntax.y:176-185 vectorAggregationExpr).
	// A log expression there is read as the left operand of a binary
	// operation, so the closing parenthesis is the unexpected token:
	// `topk(5, {app="api"})` fails with "parse error at line 1, col 20:
	// syntax error: unexpected )".
	switch inner.(type) {
	case *LogQuery, *StreamSelector:
		if p.cur.Typ == TokRParen {
			return nil, p.syntaxErrorAt("")
		}
	}

	if p.cur.Typ != TokRParen {
		return nil, p.syntaxErrorAt("")
	}
	p.advance()

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

	if p.cur.Typ != TokLParen {
		return nil, p.syntaxErrorAt("(")
	}
	p.advance()

	var labels []string
	afterComma, afterUnknown := false, false
	for p.cur.Typ == TokIdent {
		if p.atKeywordName() {
			return nil, p.syntaxErrorAt(listNameExpecting(len(labels) == 0))
		}
		if (p.cur.Unknown || afterUnknown) && len(labels) > 0 && !afterComma {
			return nil, p.syntaxErrorAt(listCloseExpecting(labels, false)) // a character that starts no token is no comma
		}
		afterUnknown = p.cur.Unknown
		labels = append(labels, p.advance().Val)
		afterComma = p.cur.Typ == TokComma
		if afterComma {
			p.advance()
		}
	}

	if p.cur.Typ != TokRParen {
		return nil, p.syntaxErrorAt(listCloseExpecting(labels, afterComma))
	}
	p.advance()

	return &Grouping{Without: without, Labels: labels}, nil
}

// rangeToken is the text of the range that starts at byte offset start, right after its '[': what stands up to the
// next ']'. It reports false when there is none.
func rangeToken(src string, start int) (string, bool) {
	end := strings.IndexByte(src[start:], ']')
	if end < 0 {
		return "", false
	}
	return src[start : start+end], true
}

// rangeLexError is the error of Loki's lexer for the text d of a range, which it reads with Prometheus' duration
// parser: digits and a unit, over and over. It returns "" when d reads so, and is called for a text the parser did
// not read as a duration.
func rangeLexError(d string) string {
	if d == "" {
		return "empty duration string"
	}
	for rest := d; rest != ""; {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i == 0 {
			return fmt.Sprintf("not a valid duration string: %q", d)
		}
		rest = rest[i:]
		for i = 0; i < len(rest) && (rest[i] < '0' || rest[i] > '9'); i++ {
		}
		if i == 0 {
			return fmt.Sprintf("not a valid duration string: %q", d)
		}
		switch unit := rest[:i]; unit {
		case "y", "w", "d", "h", "m", "s", "ms":
		default:
			return fmt.Sprintf("unknown unit %q in duration %q", unit, d)
		}
		rest = rest[i:]
	}
	return ""
}
