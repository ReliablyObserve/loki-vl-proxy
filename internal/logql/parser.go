package logql

import (
	"errors"
	"fmt"
	"math"
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

// newParseError builds Loki's parse error: line/col 0 renders as
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
	expr, unknown, _, err := parseExpression(input)
	if err == nil && unknown != nil {
		return nil, unknown
	}
	return expr, err
}

// ParseUntypedTemplates is Parse that leaves every line_format template untyped (LineFormatStage.Tmpl nil): for a
// caller that reads a stage's template text and never its typed form, nor a plan key built from it. It saves the
// text/template parse of a template no query sent before (tens of allocations), which a reader of the text does not
// need. Every other node is Parse's.
func ParseUntypedTemplates(input string) (Expr, error) {
	expr, unknown, _, err := parseExpressionOpts(input, true, false)
	if err == nil && unknown != nil {
		return nil, unknown
	}
	return expr, err
}

// parseExpression is Parse that reports a query whose names hold a character Loki's lexer reads as an identifier
// of its own (a label called ٣ or €) apart: Loki accepts such a name, but the translation to LogsQL does not carry
// it, so the proxy turns the query away. The error comes after the semantic checks, which have Loki's own errors
// for the rest of the query.
//
// plainUnwrap is Loki's syntax error for an unwrap stage outside a range aggregation in a query that parses
// otherwise. Parse accepts such a log query (Grafana's metric builder sends `| unwrap [1s]` stubs, which the handlers
// drop before they validate), and the validation answers it ahead of every other check, as Loki's parser does.
func parseExpression(input string) (expr Expr, unknown, plainUnwrap *ParseError, err error) {
	return parseExpressionOpts(input, false, false)
}

// parseExpressionTree is parseExpression that keeps the errors of Loki's checks over the parsed tree the parser met
// (a vector operation's parameter) on their nodes, for the semantic pass to report in the order of its walk.
func parseExpressionTree(input string) (expr Expr, unknown, plainUnwrap *ParseError, err error) {
	return parseExpressionOpts(input, false, true)
}

// parseExpressionOpts is parseExpression; untypedTemplates leaves the line_format templates untyped, and keepTree
// keeps the tree check errors on their nodes (parseExpressionTree).
func parseExpressionOpts(input string, untypedTemplates, keepTree bool) (expr Expr, unknown, plainUnwrap *ParseError, err error) {
	querycorpus.Record(querycorpus.LogQL, input)
	// Loki rejects a query this long before lexing it; doing the same bounds how deep a query can nest, and so
	// the recursion of every parse, for whatever query length the handlers accept.
	if msg := InputSizeError(input); msg != "" {
		return nil, nil, nil, &ParseError{Msg: msg}
	}
	p := &parser{sc: newScanner(input), untypedTemplates: untypedTemplates}
	p.advance()
	expr, err = p.parseExpr()
	if err == nil && p.cur.Typ != TokEOF {
		err = p.syntaxErrorAt("")
	}
	if err != nil || p.sc.lexErrCount() > 0 {
		// An action Loki runs while it parses (a regex or parser stage it cannot build) stops its parse: it answers
		// ahead of a later syntax error and of every lexer error the parse went on after. Past an unwrap stage
		// outside a range aggregation Loki reads nothing, so only the actions before it count.
		msg := p.plainUnwrapErr
		if p.plainUnwrapAt == 0 {
			msg = p.actionErrorSoFar()
		}
		if msg != "" {
			return nil, nil, nil, &ParseError{Msg: msg}
		}
	}
	if p.sc.lexErrCount() > 0 {
		// Loki's lexer reports its own error ahead of the parse error it may cause, and for a query that parses.
		e := p.sc.extra.err
		return nil, nil, nil, p.errorMsgAt(int(e.off), e.msg, e.zeroLine)
	}
	if p.plainUnwrapAt > 0 {
		// An unwrap stage outside a range aggregation: Loki's parser stops at its token, before it reads on.
		plainUnwrap = &ParseError{Msg: p.plainUnwrapErr}
		if p.plainUnwrapErr == "" {
			plainUnwrap, _ = p.errorAt(int(p.plainUnwrapAt)-1, "unwrap", "", false).(*ParseError)
		}
		if err != nil {
			return nil, nil, nil, plainUnwrap
		}
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if p.treeErr != "" && !keepTree {
		return nil, nil, nil, &ParseError{Msg: p.treeErr}
	}
	if at := p.unknownName(); at >= 0 {
		r, _ := utf8.DecodeRuneInString(input[at:])
		unknown, _ = p.errorMsgAt(at, "unsupported name character "+strconv.QuoteRune(r), false).(*ParseError)
	}
	return expr, unknown, plainUnwrap, nil
}

// parseForValidation is Parse with Loki's syntax error for an unwrap stage outside a range aggregation, which Parse
// keeps for the handlers (see parseExpression): the parse the validators of every route start with.
func parseForValidation(input string) (Expr, error) {
	expr, unknown, plainUnwrap, err := parseExpression(input)
	switch {
	case err != nil:
		return nil, err
	case plainUnwrap != nil:
		return nil, plainUnwrap
	case unknown != nil:
		return nil, unknown
	}
	return expr, nil
}

// ParseAndValidate parses a LogQL expression and runs semantic validation.
// Returns the first error found, or nil if the query is well-formed. An unwrap stage outside a range aggregation is
// Loki's syntax error here, as in ValidateLogQL.
func ParseAndValidate(input string) error {
	expr, err := parseForValidation(input)
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
// "regex") and vector(<number>), as Loki's grammar writes them. Any other call, or one that does not read so,
// keeps only its text; validation reports why.
func typeOpaqueCall(o *OpaqueMetricExpr) {
	p := &parser{sc: scanner{src: o.Raw, quiet: true}}
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
	sc  scanner
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
	// opaqueText is set while the text of a stage kept as text is read: a dot in it keeps the space before it.
	opaqueText bool
	// afterUnwrap is set once a range aggregation's unwrap stage is read: Loki's grammar takes only label filters
	// after it. afterRange is set while the stages after the range are read (`{a="b"}[5m] | unwrap x`), where the
	// closing parenthesis follows them.
	afterUnwrap, afterRange bool
	// plainUnwrapErr is the error Loki reports ahead of the unwrap stage plainUnwrapAt marks: the error of a stage
	// read before it, or "" (see unwrapStageError).
	plainUnwrapErr string
	// curLogQuery is the log query read last, which may still be read. checkedLogQueries are the ones before it with a
	// stage Loki builds while it parses (mayFailWhileParsed), in the order of the text: the first in an array, which
	// keeps the parser off the heap, the others in moreCheckedLogQueries (stageErrorSoFar).
	curLogQuery           *LogQuery
	checkedLogQueries     [2]*LogQuery
	moreCheckedLogQueries []*LogQuery
	checkedLogQueryCount  int
	// untypedTemplates leaves line_format templates untyped (ParseUntypedTemplates).
	untypedTemplates bool
	// treeErr is the first error of a check Loki makes over the parsed tree that the parser met (a vector
	// operation's parameter, deferTreeError): Parse answers it once the query has parsed; the validation reads it
	// from its node in the order of its walk.
	treeErr string
}

// deferTreeError notes msg as the error of the check Loki makes over the parsed tree at node va: it answers after
// every syntax and lexer error and after the actions, at the node's place in the walk (validateSemantics).
func (p *parser) deferTreeError(va *VectorAggregation, msg string) {
	if va.treeErr == nil {
		va.treeErr = &msg
	}
	if p.treeErr == "" {
		p.treeErr = msg
	}
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

// blankName reports a name at the current token that is one character Go reads as white space and Loki's lexer
// does not skip (U+00A0, U+2003, a vertical tab, ...): an identifier of its own to Loki. In a list of label names
// (by, without, on, ignoring, group_left, group_right, drop and keep) it names a label no stream has, which changes
// nothing, so the lists read it and leave it out (see skipBlankName).
func (p *parser) blankName() bool {
	if !p.cur.Unknown {
		return false
	}
	r, size := utf8.DecodeRuneInString(p.cur.Val)
	return size == len(p.cur.Val) && unicode.IsSpace(r)
}

// skipBlankName reads over the name blankName reports: it is no name the parser keeps (see keep).
func (p *parser) skipBlankName() {
	start := p.pos()
	p.advance()
	p.keep(start)
}

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
		// Loki's grammar has three more metric-level calls (label_replace,
		// vector, approx_topk); any other name
		// is an IDENTIFIER it cannot place (group, label_join, abs, ...).
		name := p.cur.Val
		if !opaqueMetricFunctions[name] {
			return nil, p.syntaxErrorAt("")
		}
		if name != "approx_topk" && !p.keywordAhead(false) {
			return nil, p.syntaxErrorAt("")
		}
		return p.parseOpaqueCall(name)
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

// parseOpaqueCall reads a metric call kept as text (label_replace, vector, approx_topk) from its name: its raw text,
// and its typed parts where typeOpaqueCall can read them.
func (p *parser) parseOpaqueCall(name string) (Expr, error) {
	// Capture the full raw text and return it as-is so VictoriaLogs
	// receives the original expression unchanged.
	// The name's own span: a name written with the Kelvin sign (approx_topK) is longer than its lower case value.
	startPos := p.pos()
	p.advance()
	var treeErr *string
	if name == "approx_topk" && (p.cur.Typ == TokBy || p.cur.Typ == TokWithout) {
		// Loki's grammar reads a grouping before approx_topk's arguments and refuses it over the parsed tree.
		if _, err := p.parseGrouping(); err != nil {
			return nil, err
		}
		msg := "parse error : grouping not allowed for approx_topk aggregation"
		treeErr = &msg
		if p.treeErr == "" {
			p.treeErr = msg
		}
	}
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
		o := &OpaqueMetricExpr{Raw: raw, Func: name, treeErr: treeErr}
		if p.canon == nil {
			typeOpaqueCall(o)
			p.noteOpaqueCall(o.Inner)
		}
		return o, nil
	}
	return nil, p.syntaxErrorAt("(")
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

// Loki's grammar declares these levels from lowest to highest. Only exponentiation
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
			if p.cur.Typ != TokLParen {
				// Loki's grammar has no on or ignoring without its list of names.
				return nil, p.syntaxErrorAt("(")
			}
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

// parseLabelList parses a parenthesised comma-separated label name list: (label1, label2). The list may be empty;
// after a name a comma or the closing parenthesis follows, and after a comma a name, as in Loki's grammar: a
// trailing, doubled or leading comma and two names without one between are its syntax errors.
// Returns an empty slice if the next token is not '('.
func (p *parser) parseLabelList() ([]string, error) {
	if p.cur.Typ != TokLParen {
		return nil, nil
	}
	p.advance() // consume '('
	var labels []string
	afterComma, names := false, 0
	for p.cur.Typ == TokIdent {
		if p.atKeywordName() {
			return nil, p.syntaxErrorAt(listNameExpecting(names == 0))
		}
		if names > 0 && !afterComma {
			return nil, p.syntaxErrorAt(listCloseExpecting(names, false)) // a name needs a comma before it
		}
		names++
		if p.blankName() {
			p.skipBlankName()
		} else {
			labels = append(labels, p.advance().Val)
		}
		afterComma = p.cur.Typ == TokComma
		if afterComma {
			p.advance()
		}
	}
	if p.cur.Typ != TokRParen || afterComma {
		return nil, p.syntaxErrorAt(listCloseExpecting(names, afterComma))
	}
	p.advance()
	return labels, nil
}

// parseLogQuery parses {selector} [pipeline stages].
func (p *parser) parseLogQuery() (*LogQuery, error) {
	start := p.pos()
	p.afterUnwrap = false
	mem := new(logQueryMem)
	lq := &mem.lq
	lq.Selector = &mem.sel
	if prev := p.curLogQuery; prev != nil {
		p.noteChecked(prev)
	}
	// The log query is the one read from its selector on: a matcher Loki builds as it reads it fails ahead of a
	// later syntax error in the same selector (actionErrorSoFar).
	p.curLogQuery = lq
	if err := p.parseStreamSelector(&mem.sel, mem.matcher[:0]); err != nil {
		return nil, err
	}

	if n := stagesAhead(p.sc.src[p.pos():]); n > 0 {
		lq.Pipeline = make([]Stage, 0, n)
	}

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

// logQueryMem is a log query, its selector and room for the selector's first matcher: the one allocation the head
// of a log query costs.
type logQueryMem struct {
	lq      LogQuery
	sel     StreamSelector
	matcher [1]LabelMatcher
}

// stagesAhead is the number of pipeline stages the text of a log query after its selector seems to hold, at most
// maxStagesAhead: its pipes and its line filters that start with a !, outside quoted strings, up to the first [ (a
// range ends the log query). The pipeline is sized with it, so that a query costs one allocation for it, not one per
// doubling; a count that misses a stage only costs a doubling.
func stagesAhead(src string) int {
	n := 0
	for i := 0; i < len(src) && n < maxStagesAhead; i++ {
		switch c := src[i]; c {
		case '|':
			n++
		case '!':
			if i+1 < len(src) && (src[i+1] == '=' || src[i+1] == '~' || src[i+1] == '>') {
				n++
			}
		case '[':
			return n
		case '"', '`':
			for i++; i < len(src) && src[i] != c; i++ {
				if c == '"' && src[i] == '\\' {
					i++
				}
			}
		}
	}
	return n
}

// maxStagesAhead bounds the room stagesAhead gives a pipeline.
const maxStagesAhead = 16

// parseStreamSelector parses {label="value",...} into sel; room is where its matchers go while they fit.
func (p *parser) parseStreamSelector(sel *StreamSelector, room []LabelMatcher) error {
	if _, err := p.expect(TokLBrace); err != nil {
		return err
	}

	if p.cur.Typ == TokRBrace {
		p.advance()
		return nil
	}

	for {
		if p.atKeywordName() {
			expecting := "IDENTIFIER"
			if len(sel.Matchers) == 0 {
				expecting = "IDENTIFIER or }"
			}
			return p.syntaxErrorAt(expecting)
		}
		expecting := "IDENTIFIER"
		if len(sel.Matchers) == 0 {
			expecting = "IDENTIFIER or }"
		}
		m, err := p.parseLabelMatcher(expecting)
		if err != nil {
			return err
		}
		if sel.Matchers == nil {
			sel.Matchers = room
		}
		sel.Matchers = append(sel.Matchers, m)

		if p.cur.Typ == TokComma {
			p.advance()
			continue
		}
		break
	}

	if p.cur.Typ != TokRBrace {
		return p.syntaxErrorAt("} or ,")
	}
	p.advance()

	return nil
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
		if p.afterUnwrap {
			// Only a label filter, or the range or the parenthesis that closes the range expression, follows an unwrap.
			if p.afterRange {
				return nil, p.unwrapStageError(")")
			}
			return nil, p.unwrapStageError("RANGE or |")
		}
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
	if p.afterUnwrap {
		if err := p.labelFilterAfterUnwrap(); err != nil {
			return nil, err
		}
	}
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
			p.plainUnwrapErr = p.stageErrorSoFar()
		}
		p.advance()
		p.afterUnwrap = p.rangeDepth > 0
		return p.parseUnwrap()

	case "line_format":
		p.advance()
		val, err := p.expectStringOrRaw()
		if err != nil {
			return nil, err
		}
		stage := &LineFormatStage{Template: val}
		if p.canon == nil && !p.untypedTemplates {
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
	if dotted := p.cur.Typ == TokError && p.cur.Val == "."; !dotted && !p.atStageEnd() {
		// A name that is no filter and no stage of its own, followed by more of the stage (`| a, b`, `| a .5`,
		// `| a:b`): Loki's grammar has no place for the token after the name. Only a dot goes on, the proxy's dotted
		// name (`| a.b`, and the spaced `| custom . x = "v"` the translator joins into one name).
		return nil, p.syntaxErrorAt("")
	}
	p.opaqueText = true
	raw := p.consumeRestOfStage(kw, TokIdent)
	p.opaqueText = false
	return &LabelFilterStage{Raw: raw}, nil
}

// atStageEnd reports whether the current token ends a stage the way consumeRestOfStage stops: the end of the query,
// the next pipeline operator, a range bracket or a closing parenthesis.
func (p *parser) atStageEnd() bool {
	switch p.cur.Typ {
	case TokEOF, TokPipe, TokPipeEq, TokPipeTilde, TokPipeGt, TokBangGt, TokLBracket, TokRParen:
		return true
	}
	return false
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

	parenNext := strings.HasPrefix(strings.TrimLeftFunc(p.sc.src[p.prevEnd:], unicode.IsSpace), "(")
	if p.cur.Typ != TokLParen && isUnwrapConverter(name.Val) && parenNext {
		// Loki's lexer reads the converter's name as its keyword before a parenthesis it finds over any white space
		// of Go's, and the character in between is the unexpected token.
		return nil, p.syntaxErrorAt("(")
	}
	// A comment between a converter's name (in any case) and its parenthesis makes the name the label, to Loki's lexer
	// (`unwrap bytes # c` and `(c)` on the next line): the parenthesis is then the unexpected token after the stage.
	if p.cur.Typ == TokLParen && (parenNext || !isUnwrapConverter(strings.ToLower(name.Val))) {
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
		if p.cur.Typ != TokRParen {
			return nil, p.syntaxErrorAt(")")
		}
		p.advance()
		return &UnwrapStage{Label: label.Val, Converter: converter}, nil
	}

	return &UnwrapStage{Label: name.Val}, nil
}

// labelFilterAfterUnwrap checks the token after the `|` that follows an unwrap: Loki's grammar takes a label filter
// there, which starts with a name (not a keyword, not ip or a conversion before its parenthesis) or a parenthesis.
func (p *parser) labelFilterAfterUnwrap() error {
	switch {
	case p.cur.Typ == TokLParen:
		return nil
	case p.cur.Typ != TokIdent, keywordLabelWord(p.cur.Val), (p.cur.Val == "ip" || isUnwrapConverter(p.cur.Val)) && p.keywordAhead(false):
		return p.unwrapStageError("IDENTIFIER or (")
	}
	return nil
}

// unwrapStageError is Loki's answer to a stage its grammar does not take after an unwrap: the syntax error at the
// current token, unless a stage read before it fails as Loki builds it while it parses (stageErrorSoFar), which
// comes first.
func (p *parser) unwrapStageError(expecting string) error {
	if msg := p.stageErrorSoFar(); msg != "" {
		return &ParseError{Msg: msg}
	}
	return p.syntaxErrorAt(expecting)
}

// noteChecked keeps a log query read before the current one when it has a matcher or stage Loki builds while it
// parses (mayFailWhileParsed), in the order of the text: the first ones in an array, which keeps the parser off the
// heap, the others in moreCheckedLogQueries.
func (p *parser) noteChecked(lq *LogQuery) {
	if !mayFailWhileParsed(lq) {
		return
	}
	if p.checkedLogQueryCount < len(p.checkedLogQueries) {
		p.checkedLogQueries[p.checkedLogQueryCount] = lq
	} else {
		p.moreCheckedLogQueries = append(p.moreCheckedLogQueries, lq)
	}
	p.checkedLogQueryCount++
}

// noteOpaqueCall keeps the log queries of a call kept as text (label_replace's sample expression, read by
// typeOpaqueCall's own parser) after the ones read before it, so that their actions answer ahead of a later syntax
// error as the others do.
func (p *parser) noteOpaqueCall(inner Expr) {
	if inner == nil {
		return
	}
	if p.curLogQuery != nil {
		p.noteChecked(p.curLogQuery)
		p.curLogQuery = nil
	}
	walkLogQueries(inner, p.noteChecked)
}

// walkLogQueries calls f for every log query of expr, in the order of the text.
func walkLogQueries(expr Expr, f func(*LogQuery)) {
	switch x := expr.(type) {
	case *LogQuery:
		f(x)
	case *RangeAggregation:
		walkLogQueries(x.Inner, f)
	case *VectorAggregation:
		walkLogQueries(x.Inner, f)
	case *BinOpExpr:
		walkLogQueries(x.Left, f)
		walkLogQueries(x.Right, f)
	case *OpaqueMetricExpr:
		walkLogQueries(x.Inner, f)
	}
}

// stageErrorSoFar is the first error Loki raises while it parses (an action, actionErrorSoFar) among the log
// queries read so far, or "". Loki builds each matcher and stage as its grammar rule completes, ahead of a syntax
// error in a later token.
func (p *parser) stageErrorSoFar() string { return p.actionErrorSoFar() }

// actionErrorSoFar is the first error, in the order of the text, of the matchers and stages read so far that Loki
// raises while it parses: a selector regex that does not parse, a regexp or pattern parser it cannot build, a label
// filter, drop or keep regex that does not parse (logQueryActionError). It wins over a syntax error after it and
// over a lexer error the parse went on after; a syntax error or an invalid string before it ends the parse first.
func (p *parser) actionErrorSoFar() string {
	for _, lq := range p.checkedLogQueries[:min(p.checkedLogQueryCount, len(p.checkedLogQueries))] {
		if msg := logQueryActionError(lq); msg != "" {
			return msg
		}
	}
	for _, lq := range p.moreCheckedLogQueries {
		if msg := logQueryActionError(lq); msg != "" {
			return msg
		}
	}
	if p.curLogQuery == nil {
		return ""
	}
	return logQueryActionError(p.curLogQuery)
}

// logQueryActionError is the first action error of a log query, read in its order: its selector's regexes, then its
// stages (parseTimeStageError).
func logQueryActionError(lq *LogQuery) string {
	if msg := selectorRegexError(lq.Selector); msg != "" {
		return msg
	}
	return parseTimeStageError(lq.Pipeline)
}

// mayFailWhileParsed reports a log query with a matcher or stage Loki builds while it parses: a regex matcher in its
// selector, or a stage parseTimeStageError checks (a regexp or pattern parser, a label filter, a drop or keep with
// matchers).
func mayFailWhileParsed(lq *LogQuery) bool {
	if lq.Selector != nil {
		for _, m := range lq.Selector.Matchers {
			if m.Op == MatchRe || m.Op == MatchNotRe {
				return true
			}
		}
	}
	for _, s := range lq.Pipeline {
		switch st := s.(type) {
		case *ParserStage:
			if st.Type == ParserRegexp || st.Type == ParserPattern {
				return true
			}
		case *LabelFilterStage:
			return true
		case *DropStage:
			if len(st.Matchers) > 0 {
				return true
			}
		case *KeepStage:
			if len(st.Matchers) > 0 {
				return true
			}
		}
	}
	return false
}

// isUnwrapConverter reports the names of the unwrap conversion functions, which Loki's lexer reads as keywords
// before a parenthesis.
func isUnwrapConverter(name string) bool {
	return name == "bytes" || name == "duration" || name == "duration_seconds"
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
	for {
		// A list holds at least one item, and one follows every comma, as in Loki's grammar.
		if p.cur.Typ != TokIdent || p.atKeywordName() {
			err = p.syntaxErrorAt("IDENTIFIER")
			return
		}
		start, blank := p.pos(), p.blankName()
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
			if blank {
				p.keep(start) // a name of no label: see blankName
				break
			}
			labels = append(labels, name)
		}
		if p.cur.Typ != TokComma {
			return
		}
		p.advance()
	}
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
	if p.cur.Typ == TokMinus && (flagAt(p.sc.src[p.pos():], "--strict") || flagAt(p.sc.src[p.pos():], "--keep-empty")) {
		// A flag after the fields: Loki's lexer reads it as a token of its own that no rule accepts there.
		return nil, p.errorAt(p.pos(), "FUNCTION_FLAG", "", true)
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
		if isUnwrapConverter(p.cur.Val) && p.keywordAhead(false) {
			return lokiWords[p.cur.Val].name // bytes( is BYTES_CONV, duration( DURATION_CONV
		}
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
			if n := startsWord(rest, word); n >= 0 {
				return strings.HasPrefix(strings.TrimLeftFunc(rest[n:], unicode.IsSpace), "(")
			}
		}
	}
	return false
}

// startsWord is the length in bytes of word (ASCII lower case) at the start of s as a whole word, in any spelling
// the scanner lower cases to it (lowerPrefixLen), or -1.
func startsWord(s, word string) int {
	n := lowerPrefixLen(s, word)
	if n < 0 || n == len(s) {
		return n
	}
	if r, _ := utf8.DecodeRuneInString(s[n:]); r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) {
		return -1
	}
	return n
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
func listCloseExpecting(names int, afterComma bool) string {
	switch {
	case names == 0:
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
// ( of the stage belongs to it. kw is the text that opens the stage, which the result starts with, and prev the type
// of the token that precedes the first one read.
// stageTextBuffer bounds the source consumeRestOfStage sizes its buffer for.
const stageTextBuffer = 64

func (p *parser) consumeRestOfStage(kw string, prev TokType) string {
	defer p.keep(p.pos())
	var b strings.Builder
	// One buffer for the whole text: the name and the source still to read, which a stage's text rarely outgrows (a
	// run of blanks becomes one space), bounded so that a long query does not size each of its stages to its end.
	// Without it the buffer the name opens is grown again once the text passes 8 bytes (`_time=~""`).
	b.Grow(len(kw) + min(len(p.sc.src)-p.pos(), stageTextBuffer))
	b.WriteString(kw) // the first token is spaced from it like any other from the one before
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
	saved := p.sc
	raw := p.consumeRestOfStage(kw, prev)
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
		p.sc = saved
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
//	(c) in the text of a stage kept as text, alphanumeric, `.` or non-ASCII + a
//	    token that starts with `.`: `a`+`.5` → `a.5` and `a`+`.`+`b` → `a.b`
//	    read as one name
//
// Rules (a) and (c) do not apply to tokens that touch in the source: the scanner
// cuts one literal into several (`1h5m` is `1h` `5m`, `1e3` is `1e` `3`), and a
// space there would change it (`1h 5m`).
func (p *parser) appendStageToken(b *strings.Builder) {
	touches := p.prevEnd == p.pos()
	start := p.pos()
	tok := p.advance()
	raw := p.stageTokenRaw(tok, start)
	if tok.Typ == TokError && !utf8.ValidString(p.sc.src[start:p.prevEnd]) {
		raw = p.sc.src[start:p.prevEnd] // not the replacement character the scanner reads an invalid byte as
	}
	if b.Len() > 0 && raw != "" && isAlphanumeric(raw[0]) {
		prev := b.String()[b.Len()-1]
		if ((isAlphanumeric(prev) || prev == '.' || prev >= 0x80 || signedZero(prev, tok, raw)) && !touches) || prev == '"' || prev == '`' {
			b.WriteByte(' ')
		}
	} else if p.opaqueText && raw != "" && raw[0] == '.' && !touches && b.Len() > 0 {
		if prev := b.String()[b.Len()-1]; isAlphanumeric(prev) || prev == '.' || prev >= 0x80 {
			b.WriteByte(' ')
		}
	}
	b.WriteString(raw)
}

// signedZero reports a minus followed by the number 0, or by a number that starts with a 0 and a letter or `_`
// (`0x1F`, `0X0`, `0o7`, `0e1`, `0_1`): written together (`-0`), Loki's lexer reads them as a duration (Go's
// time.ParseDuration takes a 0 without a unit), and with a letter after it `-0X0` is no query Loki accepts (Loki's
// recorded answers, testdata/label_filter_number_literals_loki.jsonl.gz), so a minus and such a number that stand
// apart (`- 0X0`, the number) are written apart.
func signedZero(prev byte, tok Token, raw string) bool {
	if prev != '-' || tok.Typ != TokNumber || raw == "" || raw[0] != '0' {
		return false
	}
	return len(raw) == 1 || raw[1] == '_' || raw[1]|0x20 >= 'a' && raw[1]|0x20 <= 'z'
}

// isAlphanumeric reports whether c is a letter, digit, or underscore —
// characters that must be separated by whitespace from adjacent word tokens.
func isAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// tokenRaw reconstructs the approximate source text for a token.
// stageTokenRaw is tokenRaw of the token just read, which starts at start: a double-quoted string spelled the way
// strconv.Quote spells its value (not Token.Escaped) is its own text, with no copy.
func (p *parser) stageTokenRaw(tok Token, start int) string {
	if tok.Typ == TokString && !tok.Escaped {
		return p.sc.src[start:p.prevEnd]
	}
	return tokenRaw(tok)
}

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

// parseQuantileParam parses the optional phi parameter for quantile_over_time: a number followed by a comma.
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
	}
	// Loki reads the parameter as a NUMBER token: a sign before it is the start of no argument, the syntax error
	// checkRangeAggregationArgument reports at it.
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

// parseLogRange parses Loki's log range expression: a log selector with its
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
		p.afterRange = true
		for p.atStageStart() {
			stage, err := p.parsePipelineStage()
			if err != nil {
				return form, err
			}
			lq.Pipeline = append(lq.Pipeline, stage)
		}
		p.afterRange = false
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
	// not part of one, white space among them, is an error of that token. A Grafana variable ([$__auto]) keeps its
	// older error: label_replace passes its argument through with it.
	var length time.Duration
	raw, closed := rangeToken(p.sc.src, textStart)
	if closed && !strings.HasPrefix(raw, "$") {
		var msg string
		if length, msg = readRange(raw); msg != "" {
			return newParseError(0, bracketCol, msg)
		}
	}
	dur := p.cur
	if closed && raw == "0" && dur.Typ == TokNumber {
		p.advance() // a bare 0 is a duration of nothing to Loki, though the scanner reads it as a number
	} else {
		var err error
		if dur, err = p.expect(TokDuration); err != nil {
			return fmt.Errorf("logql: expected duration: %w", err)
		}
	}
	// The duration token is the whole range text here: the range read above, exactly as Loki reads it.
	ra.Range = dur.Val
	ra.RangeDuration = length
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
	probe := parser{sc: p.sc, cur: p.cur}
	probe.sc.quiet = true
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
		return p.errorMsgAt(len(p.sc.src), msg, false)
	case TokIdent, TokNumber, TokMinus, TokPlus, TokString, TokRawString, TokRParen:
		// Loki names the token's position (`quantile_over_time(-1, …)`, `quantile_over_time("x", …)`, `rate()`).
		return p.errorMsgAt(probe.pos(), msg, false)
	case TokDurationLit, TokBytes:
		// On line 0, at the literal's column on its own line, counted in characters.
		return p.errorMsgAt(probe.pos(), msg, true)
	}
	return newParseError(0, 0, msg)
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
			first, err := p.parseExpr()
			return nil, p.vectorParamError(op, first, err)
		}
		kTok := p.advance()
		if p.cur.Typ != TokComma {
			// Loki reads a number not followed by a comma as the start of the expression it aggregates
			// (`topk(1 + 2, …)` fails at the comma, `topk(1)` for its missing parameter).
			first, err := p.parseBinaryRHS(&LiteralExpr{}, 1)
			return nil, p.vectorParamError(op, first, err)
		}
		// Loki requires an integer k > 0, a check over the parsed tree: a later syntax or lexer error answers first.
		k, err := strconv.Atoi(kTok.Val)
		switch {
		case err != nil:
			p.deferTreeError(va, newParseError(0, 0, fmt.Sprintf("invalid parameter %s(%s,", op, kTok.Val)).Msg)
		case k <= 0:
			p.deferTreeError(va, newParseError(0, 0, fmt.Sprintf("invalid parameter (must be greater than 0) %s(%s", op, kTok.Val)).Msg)
		}
		va.Param = float64(k)
		va.HasParam = true
		p.advance()
	}

	var inner Expr
	if op != VectorTopK && op != VectorBottomK && p.cur.Typ == TokNumber {
		// Loki's grammar reads `op ( NUMBER , expr )` for every vector operation, and refuses the parameter of one
		// that takes none over the parsed tree; a number not followed by a comma starts the expression.
		numTok := p.cur
		lit, err := p.parseNumberLiteral()
		if err != nil {
			return nil, err
		}
		if p.cur.Typ == TokComma {
			p.deferTreeError(va, newParseError(0, 0, fmt.Sprintf("unsupported parameter for operation %s(%s,", op, numTok.Val)).Msg)
			p.advance()
		} else if inner, err = p.parseBinaryRHS(lit, 1); err != nil {
			return nil, err
		}
	}
	if inner == nil {
		var err error
		if inner, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	va.Inner = inner

	// Loki's grammar takes only a metric expression inside a vector
	// aggregation. A log expression there is read as the left operand of a binary
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

// vectorParamError is Loki's answer to a topk or bottomk whose first argument is not a number followed by a comma.
// Loki's grammar reads that argument as the expression the operation aggregates (first, or the error parsing it
// gave): a comma after it is the unexpected token (`topk(-1, …)` fails at the comma, as `topk(1 + 2, …)` and
// `topk(rate(…), 2)` do); a closing parenthesis after a metric expression leaves the operation without its parameter
// (`topk(-1)`, `topk(rate(…))`), and one after a log query is the unexpected token, as inside any vector aggregation.
func (p *parser) vectorParamError(op VectorOp, first Expr, err error) error {
	if err != nil {
		return err
	}
	if p.cur.Typ == TokRParen {
		switch first.(type) {
		case *LogQuery, *StreamSelector:
			return p.syntaxErrorAt("")
		}
		return newParseError(0, 0, "parameter required for operation "+string(op))
	}
	return p.syntaxErrorAt("")
}

// parseGrouping parses `by (label1, label2)` or `without (label1)`.
func (p *parser) parseGrouping() (*Grouping, error) {
	without := p.cur.Typ == TokWithout
	p.advance() // consume by/without

	if p.cur.Typ != TokLParen {
		return nil, p.syntaxErrorAt("(")
	}
	labels, err := p.parseLabelList()
	if err != nil {
		return nil, err
	}
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
// parser, or "" when d reads as one: a number and a unit, over and over, the units y, w, d, h, m, s and ms each at
// most once and in that order, and the sum at most the longest duration. Each part is checked in turn, in the order
// Loki answers (recorded with the Loki oracle, TestRangeDurationMatchesLoki): a part without digits or a unit, or
// with a number above 64 bits, is no duration string; then an unknown unit; then a unit out of order; then the sum.
func rangeLexError(d string) string {
	_, msg := readRange(d)
	return msg
}

// readRange is rangeLexError with the length of the range, exact in nanoseconds, when it reads (msg is ""). A bare 0
// is a duration of nothing, as in Prometheus' parser (Loki accepts `[0]`); any other number needs a unit (`[00]`).
func readRange(d string) (length time.Duration, msg string) {
	switch d {
	case "":
		return 0, "empty duration string"
	case "0":
		return 0, ""
	}
	var total uint64
	last := len(rangeUnits)
	for rest := d; rest != ""; {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		n, err := strconv.ParseUint(rest[:i], 10, 64)
		rest = rest[i:]
		for i = 0; i < len(rest) && (rest[i] < '0' || rest[i] > '9'); i++ {
		}
		if err != nil || i == 0 {
			return 0, fmt.Sprintf("not a valid duration string: %q", d)
		}
		unit := rangeUnitIndex(rest[:i])
		if unit < 0 {
			return 0, fmt.Sprintf("unknown unit %q in duration %q", rest[:i], d)
		}
		if unit >= last {
			return 0, fmt.Sprintf("not a valid duration string: %q", d)
		}
		last = unit
		size := rangeUnits[unit].size
		if n > math.MaxInt64/size || total+n*size > math.MaxInt64 {
			return 0, "duration out of range"
		}
		total += n * size
		rest = rest[i:]
	}
	return time.Duration(total), ""
}

// rangeUnits are the units of a range duration, from the smallest, with their length in nanoseconds.
var rangeUnits = [...]struct {
	name string
	size uint64
}{
	{"ms", uint64(time.Millisecond)}, {"s", uint64(time.Second)}, {"m", uint64(time.Minute)}, {"h", uint64(time.Hour)},
	{"d", 24 * uint64(time.Hour)}, {"w", 7 * 24 * uint64(time.Hour)}, {"y", 365 * 24 * uint64(time.Hour)},
}

// rangeUnitIndex is the index of unit in rangeUnits, or -1.
func rangeUnitIndex(unit string) int {
	for i, u := range rangeUnits {
		if u.name == unit {
			return i
		}
	}
	return -1
}
