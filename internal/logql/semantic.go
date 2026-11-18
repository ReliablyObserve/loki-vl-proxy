package logql

import (
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"unicode/utf8"
)

// errEmptyCompatibleMatchers is Loki's text for a selector with no equality matcher, rendered as a
// parse error.
const errEmptyCompatibleMatchers = `parse error : queries require at least one regexp or equality matcher that does not have an empty-compatible value. For instance, app=~".*" does not meet this requirement, but app=~".+" will`

// validateSemantics walks the AST and enforces constraints that cannot be
// caught by the parser alone. Returns a Loki-compatible error string, or ""
// if the expression is valid. Loki checks the whole parsed tree before it builds
// any pipeline, so the checks of the tree answer ahead of every build error.
func validateSemantics(expr Expr, raw string) string {
	if msg := semanticPass(expr, raw, false); msg != "" {
		return msg
	}
	return semanticPass(expr, raw, true)
}

// semanticPass walks the AST for the checks of one of Loki's phases: those over the parsed tree (the actions a
// query that parsed still carries, a range aggregation's grouping and unwrap, binary operands, a vector operation's
// parameter, label_replace, the empty-compatible selector), or with build the pipeline build of every log query.
func semanticPass(expr Expr, raw string, build bool) string {
	w := semanticWalk{raw: raw, build: build}
	return w.pass(expr)
}

// semanticWalk is one semanticPass over the tree of the query raw.
type semanticWalk struct {
	raw   string
	build bool
	// regexOK is the last label_replace regex that compiled (the empty one compiles): a nesting repeats its calls,
	// and Loki's verdict on a regex is the same at every level.
	regexOK string
}

// pass is semanticPass over expr.
func (w *semanticWalk) pass(expr Expr) string {
	raw, build := w.raw, w.build
	switch x := expr.(type) {
	case *LogQuery:
		return validateLogQuerySemantics(x, false, build)
	case *BadLiteralExpr:
		if build {
			return ""
		}
		return x.Err
	case *RangeAggregation:
		return w.rangeAgg(x)
	case *VectorAggregation:
		// Loki checks a node before its operand: the operation's parameter first.
		if x.treeErr != nil && !build {
			return *x.treeErr
		}
		return w.pass(x.Inner)
	case *BinOpExpr:
		return w.binOp(x)
	case *OpaqueMetricExpr:
		// label_replace(inner, ...) and similar wrappers: Loki validates the
		// wrapped sample expression like any other.
		if x.treeErr != nil && !build {
			return *x.treeErr
		}
		if build {
			if x.Func == "vector" {
				return "" // a number: no pipeline to build
			}
			inner := x.Inner
			if inner == nil && !x.Typed {
				inner, _ = opaqueFirstArgument(raw, x)
			}
			if inner != nil {
				return w.pass(inner)
			}
			return ""
		}
		inner := x.Inner
		if !x.Typed {
			// A typed call was read by the parser, with its own parse of its text (typeOpaqueCall): a vector call's
			// argument is a number, label_replace's first argument is x.Inner and its four strings are well formed
			// (TestTypedCallSkipsItsReread). Only a call it could not type is read again here, for Loki's error; a typed
			// one read again would read every call nested in it again, at every level of the nesting.
			var msg string
			if inner, msg = untypedCall(raw, x); msg != "" {
				return msg
			}
		}
		if inner != nil {
			if msg := w.pass(inner); msg != "" {
				return msg
			}
		}
		if x.Typed && x.Func == "label_replace" && len(x.Args) == 4 && x.Args[3] != w.regexOK {
			// Loki compiles the regex anchored, as it matches it.
			if _, err := regexp.Compile("^(?:" + x.Args[3] + ")$"); err != nil {
				return "parse error : invalid regex in label_replace: " + err.Error()
			}
			w.regexOK = x.Args[3]
		}
	}
	return ""
}

// binOp is the pass over a binary operation: the type of its operands, then the operands, the left one first.
func (w *semanticWalk) binOp(x *BinOpExpr) string {
	if w.build {
		if s := w.pass(x.Left); s != "" {
			return s
		}
		return w.pass(x.Right)
	}
	// Log stream queries cannot participate in binary metric operations: Loki names the type of the
	// operand, the left one first, before it looks at a literal.
	if msg := binaryLogOperandError(x.Op, "left", x.Left); msg != "" {
		return msg
	}
	if msg := binaryLogOperandError(x.Op, "right", x.Right); msg != "" {
		return msg
	}
	// A set operator joins two vectors: Loki refuses a literal on either side, the left one first.
	if x.Op == "and" || x.Op == "or" || x.Op == "unless" {
		if l, ok := x.Left.(*LiteralExpr); ok {
			return fmt.Sprintf("parse error : unexpected literal for left leg of logical/set binary operation (%s): %f", x.Op, l.Value)
		}
		if r, ok := x.Right.(*LiteralExpr); ok {
			return fmt.Sprintf("parse error : unexpected literal for right leg of logical/set binary operation (%s): %f", x.Op, r.Value)
		}
	}
	if s := w.pass(x.Left); s != "" {
		return s
	}
	return w.pass(x.Right)
}

// binaryLogOperandError is the parse error Loki raises for a log query used as an operand of a binary
// operation; it is empty for any other operand.
func binaryLogOperandError(op, side string, operand Expr) string {
	lq, ok := operand.(*LogQuery)
	if !ok {
		return ""
	}
	typ := "MatchersExpr"
	if len(lq.Pipeline) > 0 {
		typ = "PipelineExpr"
	}
	return "parse error : unexpected type for " + side + " leg of binary operation (" + op + "): *syntax." + typ
}

// untypedCall reads a call the parser keeps as text and could not type (approx_topk, or a call Loki rejects): its
// sample expression, if it has one, and Loki's error for it, if any: a vector call whose argument is not a number, a
// first argument that does not parse, label_replace strings that are not four well formed strings.
func untypedCall(raw string, x *OpaqueMetricExpr) (Expr, string) {
	var lokiErr *ParseError
	if x.Func == "vector" {
		if err := vectorArgumentError(raw, x); errors.As(err, &lokiErr) {
			return nil, lokiErr.Msg
		}
	}
	inner, err := opaqueFirstArgument(raw, x)
	if errors.As(err, &lokiErr) {
		return nil, lokiErr.Msg
	}
	if err := labelReplaceArgumentsError(raw, x); err != nil {
		return nil, err.Error()
	}
	return inner, ""
}

// callText is the text a call kept as text is read from, and its offset there: the query, where the call stands,
// so that an error carries the line and column Loki reports for the whole query, or the call's own text for a call
// not read from that query.
func callText(query string, x *OpaqueMetricExpr) (src string, pos int) {
	if pos := strings.Index(query, x.Raw); pos >= 0 {
		return query, pos
	}
	return x.Raw, 0
}

// vectorArgumentError reports the parse error Loki raises for a vector call whose argument is not a number it
// can read: `vector(x)`, `vector(5x)`, `vector(0x1)`. It is nil for a valid call.
func vectorArgumentError(query string, call *OpaqueMetricExpr) error {
	src, pos := callText(query, call)
	p := &parser{sc: scanner{src: src, pos: pos, quiet: true}}
	p.advance()
	if p.cur.Typ != TokIdent {
		return nil
	}
	if p.advance(); p.cur.Typ != TokLParen {
		return nil
	}
	p.advance()
	if p.cur.Typ != TokNumber {
		return p.syntaxErrorAt("NUMBER")
	}
	if _, err := strconv.ParseFloat(p.cur.Val, 64); err != nil {
		return newParseError(0, 0, "unable to parse vectorExpr as a float: "+err.Error())
	}
	if p.advance(); p.cur.Typ != TokRParen {
		return p.syntaxErrorAt(")")
	}
	return nil
}

// opaqueFirstArgument parses the leading expression argument of an opaque
// metric function call such as `label_replace(rate({app="a"}[5m]), ...)`, read
// from query where call stands so that an error carries the line and column
// Loki reports for the whole query. It returns a nil Expr when the call has no
// parseable expression argument, and the parse error when that argument is
// itself rejected.
func opaqueFirstArgument(query string, call *OpaqueMetricExpr) (Expr, error) {
	src, pos := callText(query, call)
	p := &parser{sc: scanner{src: src, pos: pos, quiet: true}}
	p.advance()
	if p.cur.Typ != TokIdent {
		return nil, nil
	}
	p.advance()
	if p.cur.Typ != TokLParen {
		return nil, nil
	}
	p.advance()
	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.cur.Typ != TokComma {
		return nil, nil
	}
	return inner, nil
}

// labelReplaceArgumentsError reports the parse error Loki raises for a
// label_replace call that does not hold exactly four string arguments after its
// expression, a string whose
// escapes are invalid (the LogQL lexer unquotes it) included, at the line and column of the query. It is nil for
// any other call.
func labelReplaceArgumentsError(query string, call *OpaqueMetricExpr) error {
	src, pos := callText(query, call)
	p := &parser{sc: scanner{src: src, pos: pos, quiet: true}}
	p.advance()
	if p.cur.Typ != TokIdent || p.cur.Val != "label_replace" {
		return nil
	}
	if p.advance(); p.cur.Typ != TokLParen {
		return nil
	}
	p.advance()
	if _, err := p.parseExpr(); err != nil {
		return nil // reported by opaqueFirstArgument
	}
	for i := 0; i < 4; i++ {
		if p.cur.Typ != TokComma {
			if i == 0 {
				return p.syntaxErrorAt("") // the expression could go on: Loki names nothing
			}
			return p.syntaxErrorAt(",")
		}
		p.advance()
		switch p.cur.Typ {
		case TokString, TokRawString:
		case TokError:
			if p.cur.Val == "invalid quoted string" {
				// Loki's lexer reports the start of the string.
				before := p.sc.src[:p.pos()]
				return newParseError(1, utf8.RuneCountInString(before)+1, "invalid char escape")
			}
			return p.syntaxErrorAt("STRING")
		default:
			return p.syntaxErrorAt("STRING")
		}
		p.advance()
	}
	if p.cur.Typ != TokRParen {
		return p.syntaxErrorAt(")")
	}
	return nil
}

// validateLogQuerySemantics validates a log query. lineIndependent is true
// inside range aggregations whose result does not depend on the log line
// (every op except bytes_rate/bytes_over_time): there Loki's removeLineformat
// optimisation drops a line_format stage before the
// pipeline is built, unless a parser or line filter follows it, so only a
// line_format that survives that rule can report an invalid template.
func validateLogQuerySemantics(lq *LogQuery, lineIndependent, build bool) string {
	if build {
		return pipelineBuildError(lq.Pipeline, lineIndependent)
	}
	parseMsg, emptyMsg := logQueryParseErrors(lq)
	if parseMsg != "" {
		return parseMsg
	}
	return emptyMsg
}

// logQueryParseErrors returns the error Loki raises while it parses a log query (a selector regex, then a stage's;
// see parseTimeStageError) and, apart, the selector's empty-compatible error, which Loki checks once the whole query
// is parsed.
func logQueryParseErrors(lq *LogQuery) (parseMsg, emptyMsg string) {
	msg := validateSelectorSemantics(lq.Selector)
	if msg != "" && msg != errEmptyCompatibleMatchers {
		return msg, ""
	}
	if stage := parseTimeStageError(lq.Pipeline); stage != "" {
		return stage, ""
	}
	return "", msg
}

// lineConsumedLater reports whether a later stage reads the log line — a
// parser or a line filter — which makes Loki keep an earlier line_format.
func lineConsumedLater(stages []Stage) bool {
	for _, s := range stages {
		switch s.(type) {
		case *ParserStage, *LineFilterStage:
			return true
		}
	}
	return false
}

// validateSelectorSemantics validates matcher regexes and Loki's
// syntax.validateMatchers rule: at least one matcher must not match the empty
// string, so {app=""}, {app=~""}, {app=~".*"}, {app!="x"} and {} are rejected
// while {app=~".+"} and {app="", pod="a"} are accepted.
func validateSelectorSemantics(sel *StreamSelector) string {
	selective := false
	for _, m := range sel.Matchers {
		matchesEmpty, err := matcherMatchesEmpty(m)
		if err != nil {
			return invalidRegexMessage(err)
		}
		if !matchesEmpty {
			selective = true
		}
	}
	if !selective {
		return errEmptyCompatibleMatchers
	}
	return ""
}

// invalidRegexMessage is Loki's parse error for a selector regex that does not parse: the regexp package's error
// without a position, as for a label filter regex.
func invalidRegexMessage(err error) string {
	return newParseError(0, 0, err.Error()).Msg
}

// firstActionError is the first error, in the order of the text, that Loki raises while it parses expr (a log
// query's selector regexes and its stages, logQueryActionError), or "". It reads a call kept as text through its typed
// sample expression.
func firstActionError(expr Expr) string {
	switch x := expr.(type) {
	case *LogQuery:
		return logQueryActionError(x)
	case *RangeAggregation:
		return firstActionError(x.Inner)
	case *VectorAggregation:
		return firstActionError(x.Inner)
	case *BinOpExpr:
		if msg := firstActionError(x.Left); msg != "" {
			return msg
		}
		return firstActionError(x.Right)
	case *OpaqueMetricExpr:
		if x.Inner != nil {
			return firstActionError(x.Inner)
		}
	}
	return ""
}

// selectorRegexError is the error of the first regex matcher of a selector that does not parse, or "".
func selectorRegexError(sel *StreamSelector) string {
	if sel == nil {
		return ""
	}
	for _, m := range sel.Matchers {
		if m.Op != MatchRe && m.Op != MatchNotRe {
			continue
		}
		if _, err := matcherMatchesEmpty(m); err != nil {
			return invalidRegexMessage(err)
		}
	}
	return ""
}

// matcherMatchesEmpty mirrors Prometheus labels.Matcher.Matches(""). Like
// labels.NewFastRegexMatcher it checks the raw expression's syntax first (so
// `a)|(b` is rejected even though its anchored form parses), then compiles
// the anchored ^(?s:value)$ form once (util.SplitFiltersAndMatchers).
func matcherMatchesEmpty(m LabelMatcher) (bool, error) {
	switch m.Op {
	case MatchEq:
		return m.Value == "", nil
	case MatchNeq:
		return m.Value != "", nil
	case MatchRe, MatchNotRe:
		if _, err := syntax.Parse(m.Value, syntax.Perl); err != nil {
			return false, err
		}
		re, err := regexp.Compile("^(?s:" + m.Value + ")$")
		if err != nil {
			return false, err
		}
		return re.MatchString("") == (m.Op == MatchRe), nil
	}
	return false, nil
}

// rangeAgg is the pass over a range aggregation.
func (w *semanticWalk) rangeAgg(ra *RangeAggregation) string {
	lq, isLogQuery := ra.Inner.(*LogQuery)
	if !isLogQuery {
		return w.pass(ra.Inner)
	}
	if w.build {
		// Build the inner pipeline as Loki does.
		lineIndependent := ra.Op != RangeBytesRate && ra.Op != RangeBytesOverTime
		return pipelineBuildError(lq.Pipeline, lineIndependent)
	}

	parseMsg, emptyMsg := logQueryParseErrors(lq)
	if parseMsg != "" {
		return parseMsg
	}
	// Loki's range aggregation rules, in its order: a by or without clause only on
	// the unwrap functions that keep labels, then unwrap where it is required or refused.
	if ra.Grouping != nil && !rangeGroupingAllowed[ra.Op] {
		return fmt.Sprintf("parse error : grouping not allowed for %s aggregation", ra.Op)
	}
	hasUnwrap := false
	for _, s := range lq.Pipeline {
		if _, ok := s.(*UnwrapStage); ok {
			hasUnwrap = true
			break
		}
	}
	switch {
	case !hasUnwrap && rangeUnwrapRequired[ra.Op]:
		return fmt.Sprintf("parse error : invalid aggregation %s without unwrap", ra.Op)
	case hasUnwrap && rangeUnwrapRefused[ra.Op]:
		return fmt.Sprintf("parse error : invalid aggregation %s with unwrap", ra.Op)
	}

	return emptyMsg
}

// The range aggregation rules of Loki v3.7 (recorded with the test oracle: TestRangeAggregationRulesLikeLoki).
var (
	rangeGroupingAllowed = map[RangeOp]bool{
		RangeAvgOverTime: true, RangeMinOverTime: true, RangeMaxOverTime: true, RangeStddevOverTime: true,
		RangeStdvarOverTime: true, RangeFirstOverTime: true, RangeLastOverTime: true, RangeQuantileOverTime: true,
	}
	rangeUnwrapRequired = map[RangeOp]bool{
		RangeRateCounter: true, RangeSumOverTime: true, RangeAvgOverTime: true, RangeMinOverTime: true,
		RangeMaxOverTime: true, RangeStddevOverTime: true, RangeStdvarOverTime: true, RangeFirstOverTime: true,
		RangeLastOverTime: true, RangeQuantileOverTime: true,
	}
	rangeUnwrapRefused = map[RangeOp]bool{RangeCountOverTime: true, RangeBytesOverTime: true, RangeBytesRate: true}
)
