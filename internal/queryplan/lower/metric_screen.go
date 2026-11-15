package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// ScreenMetric is the screen of a metric query ahead of its plan: one pass over the text, with no allocation, that
// names the construct at the top of the query (VectorAgg, RangeAgg, BinOp or LabelReplace, as Construct names the
// plan's root) or answers "" for a query the plan would leave to the string translator. A caller that serves some
// metric constructs from the plan calls it before it parses the query, so a query the plan does not serve costs no
// parse, build or resolve on top of its translation.
//
// It answers "" for a query with any of these (outside quoted strings, unless said otherwise):
//   - a ranking (topk, bottomk, sort, sort_desc), stddev or stdvar, vector(), the bool modifier, rate_counter or a call
//     it does not know: the proxy evaluates them, or the plan does not write them;
//   - a stage the lowering leaves to the translator in a metric query (unpack, line_format, decolorize), a regexp or
//     pattern parser with no blank before its expression, a keep with an unwrap (the keep identity has its own path),
//     a level filter after a parser, a set operator outside a log pipeline;
//   - an empty without clause, a grouping after a range function inside an aggregation, nested aggregations over an
//     unwrap, a range function that reads an unwrapped value with no unwrap;
//   - a binary operation inside an aggregation, more than one at the top, one with a without clause or a
//     label_replace operand (the binary route evaluates those operand by operand), one with no range function;
//   - a fold or a grouping by a label that may be a sanitized parser key, or an unwrap of one, with a label filter or a
//     drop after the parser (the lowering leaves those to the translator as costing more to write);
//   - a label filter with an equality on a number, duration or byte size after a json or logfmt parser
//     (WritesNumericEquality), or a comparison with a signed number;
//   - a word VictoriaLogs reserves as a field name (_time, _msg, _stream, _stream_id in any letter case) before a parser
//     (ReadsReservedField);
//   - a quoted string with detected_level or service_name in it, a byte from \x00 to \x02, or an escape that could
//     write one (the values the string translator reads as syntax);
//   - the spellings ScreenText leaves to the translator: a whole query in parentheses, a blank before an operand or a
//     range function's parenthesis; and a comment, or text it does not read.
//
// The screen only turns queries away: the typed gate on the plan and the lowering still decide every query it
// passes, so one it passes in error costs a plan and is translated as before. The proxy's corpus test holds it to the
// plan: the rows it passes that the plan then leaves to the translator, and the served rows it turns away.
func ScreenMetric(query string) string {
	query = strings.TrimSpace(query)
	var s metricScan
	if !s.scan(query) || s.ranges == 0 || s.needsUnwrap && !s.unwrap || s.keep && s.unwrap ||
		s.nestedVector && s.unwrap || s.lrBinary && s.quotedParen || s.parsed && s.afterParser && (s.without || s.candidate) {
		return ""
	}
	if s.binary {
		if s.binaries > 1 || s.without || s.labelReplaces > 0 {
			return ""
		}
		return "BinOp"
	}
	switch s.first {
	case "label_replace":
		// Under label_replace, the plan does not write the pipes that derive a service_name grouping.
		if s.labelReplaceCommas != 4 || s.groupsServiceName {
			return ""
		}
		return "LabelReplace"
	case "sum", "avg", "min", "max", "count":
		if paddedOperand(query) {
			return ""
		}
		return "VectorAgg"
	}
	if rangeFunctionWord(s.first) && !paddedCall(query) {
		return "RangeAgg"
	}
	return ""
}

// MetricSet is a set of the metric constructs ScreenMetric names.
type MetricSet uint8

// The metric constructs of a MetricSet.
const (
	MetricVectorAgg MetricSet = 1 << iota
	MetricRangeAgg
	MetricBinOp
	MetricLabelReplace
)

// MetricSetOf is the set of the metric constructs among the names (as Construct names them).
func MetricSetOf(constructs []string) MetricSet {
	var set MetricSet
	for _, c := range constructs {
		set |= metricBit(c)
	}
	return set
}

// metricBit is the member of a MetricSet a construct name is, or 0.
func metricBit(c string) MetricSet {
	switch c {
	case "VectorAgg":
		return MetricVectorAgg
	case "RangeAgg":
		return MetricRangeAgg
	case "BinOp":
		return MetricBinOp
	case "LabelReplace":
		return MetricLabelReplace
	}
	return 0
}

// ScreenMetricIn is ScreenMetric for a caller that serves only the constructs of set: "" for any other. The first
// word of the query bounds its construct (label_replace, a vector aggregation or a binary, a range aggregation or a
// binary, or a binary), so a query that can be none of set is turned away before the text is scanned.
func ScreenMetricIn(query string, set MetricSet) string {
	query = strings.TrimSpace(query)
	j := 0
	for j < len(query) && isNameByte(query[j]) {
		j++
	}
	can := MetricBinOp
	switch w := query[:j]; {
	case w == "label_replace":
		can = MetricLabelReplace
	case vectorWord(w):
		can |= MetricVectorAgg
	case rangeFunctionWord(w):
		can |= MetricRangeAgg
	}
	if set&can == 0 {
		return ""
	}
	if c := ScreenMetric(query); set&metricBit(c) != 0 {
		return c
	}
	return ""
}

// metricScan is the state of ScreenMetric's pass.
type metricScan struct {
	paren, brace, bracket int
	// first is the first word of the query (its top function).
	first string
	// binaries counts the binary operators at depth 0; binary is set by one.
	binaries int
	binary   bool
	// operand is set after a value, so a + or - that follows is an operator, not a sign.
	operand bool
	// pending is the opening parenthesis a name asks for next: a range function's, a clause's, label_replace's.
	pending byte
	// pendingVector is a vector aggregation's name read, until its operand's parenthesis.
	pendingVector bool
	// quantileArg is set by quantile_over_time until its first number: the parameter the translator copies as written.
	quantileArg bool
	// rangeAt, clauseAt and labelReplaceAt are the depths of the parentheses of the range function, the by or without
	// clause and the top label_replace call being read (0 outside one); afterRange is set when a range function's
	// parenthesis closes, until the next name.
	rangeAt, clauseAt, labelReplaceAt int
	afterRange                        bool
	// vectorAt holds the depths of the operand parentheses of the vector aggregations being read (vectors of them);
	// nestedVector is a vector aggregation inside another.
	vectorAt     [8]int
	nestedVector bool
	// lrBinary is a binary operation as the operand of label_replace; quotedParen a quoted parenthesis before it;
	// argString the string of the label_replace argument being read.
	lrBinary, quotedParen, argString bool
	// ranges, vectors and labelReplaces count those calls; labelReplaceCommas the commas between the arguments of the
	// top label_replace.
	ranges, vectors, labelReplaces, labelReplaceCommas int
	// parsed is a json or logfmt parser seen, afterParser a label filter or a drop after one.
	parsed, afterParser bool
	// needsUnwrap is a range function that reads an unwrapped value; unwrap the unwrap stage, unwrapLabel the label it
	// names next.
	needsUnwrap, unwrap, unwrapLabel bool
	// without, keep: those words; candidate: a grouping or unwrapped label that may be a sanitized parser key.
	without, keep, candidate bool
	// parsedOther is a regexp or pattern parser; groupsServiceName a clause naming service_name.
	parsedOther, groupsServiceName bool
	// log is set for a log query (ScreenLogStages): its whole text after the selector is a pipeline. stages are the
	// stage families read, and staged is a stage other than a line or label filter read.
	log     bool
	stages  LogStages
	allowed LogStages
	staged  bool
	// names is set in a stage that lists names (a parser's extraction list, a drop or keep), until the next pipe.
	names bool
	// In a log query, until the next pipe: flags is set in a json or logfmt stage (its --strict and --keep-empty
	// flags); keepStage in a keep, with keepName and keepMatcher for a name and a matcher read in it; and template in
	// a line_format or label_format, whose quoted strings are templates.
	flags, keepStage, keepName, keepMatcher, template bool
	// parserArg is set by a regexp or pattern parser in a log query, until its quoted expression is read.
	parserArg bool
}

// pipeline reports a position in a log pipeline: inside a range function, or anywhere in a log query.
func (s *metricScan) pipeline() bool { return s.rangeAt > 0 || s.log }

// Pending parentheses.
const (
	pendingRange byte = 1 + iota
	pendingClause
	pendingLabelReplace
)

// scan reads q and reports false at the first thing ScreenMetric turns away.
func (s *metricScan) scan(q string) bool {
	if q == "" || q[0] == '(' && wrappedWhole(q) {
		return false
	}
	for i := 0; i < len(q); i++ {
		if c := q[i]; !isBlank(c) && !isNameStart(c) {
			// A name right after a range function's parenthesis closes is its grouping; anything else ends that.
			s.afterRange = false
		}
		var ok bool
		if i, ok = s.step(q, i); !ok {
			return false
		}
	}
	return s.paren == 0 && s.brace == 0 && s.bracket == 0
}

// step reads the token at q[i] and returns the index of its last byte; false for one ScreenMetric turns away.
func (s *metricScan) step(q string, i int) (int, bool) {
	switch c := q[i]; {
	case c == '"' || c == '`':
		return s.quoted(q, i)
	case c == '#':
		return i, false
	case c == '(' || c == ')' || c == '{' || c == '}' || c == '[' || c == ']':
		return i, s.delimiter(c)
	case c == ',':
		return i, s.comma()
	case isBlank(c) || c == ':':
		return i, true
	case c == '|':
		// A pipe, or a line filter (|=, |~, |>).
		if i+1 < len(q) && (q[i+1] == '=' || q[i+1] == '~' || q[i+1] == '>') {
			i++
		}
		s.operand, s.names = false, false
		s.flags, s.keepStage, s.keepName, s.keepMatcher, s.template = false, false, false, false, false
		return i, true
	case c >= '0' && c <= '9' || c == '.':
		return s.number(q, i)
	case isNameStart(c):
		j := i
		for j < len(q) && isNameByte(q[j]) {
			j++
		}
		ok := !s.labelReplaceArg() && s.word(q, i, j)
		s.afterRange, s.operand = false, true
		return j - 1, ok
	}
	if end, ok := s.parserFlag(q, i); ok {
		return end, true
	}
	end, ok := s.operator(q, i)
	s.operand = false
	return end, ok
}

// parserFlag reads a json or logfmt parser's flag at q[i] (--strict, --keep-empty) whole, and returns the index of its
// last byte; false for anything else.
func (s *metricScan) parserFlag(q string, i int) (int, bool) {
	if !s.flags || q[i] != '-' || i+1 >= len(q) || q[i+1] != '-' {
		return 0, false
	}
	j := i + 2
	for j < len(q) && (isNameByte(q[j]) || q[j] == '-') {
		j++
	}
	return j - 1, true
}

// quoted reads the quoted string opening at q[i].
func (s *metricScan) quoted(q string, i int) (int, bool) {
	j := skipQuoted(q, i)
	// The derived label names count in a log pipeline's strings: a label filter value the translator unpacks.
	if j >= len(q) || misreadQuoted(q[i+1:j], q[i], s.pipeline() && s.brace == 0) || s.template && !plainTemplate(q[i+1:j]) {
		return j, false
	}
	if s.parserArg {
		s.parserArg = false
		if !plainParserArg(q[i+1:j], q[i]) {
			return j, false
		}
	}
	if s.labelReplaceCommas == 0 && strings.ContainsAny(q[i+1:j], "()") {
		s.quotedParen = true
	}
	if s.labelReplaceArg() {
		// Each argument of label_replace after the first is one string.
		if s.argString {
			return j, false
		}
		s.argString = true
	}
	s.operand = true
	return j, true
}

// delimiter reads a parenthesis, a brace or a bracket.
func (s *metricScan) delimiter(c byte) bool {
	switch c {
	case '(':
		return s.open()
	case ')':
		return (!s.labelReplaceArg() || s.argString) && s.close()
	case '{':
		s.brace++
		s.operand = false
	case '}':
		s.brace--
		s.operand = true
	case '[':
		s.bracket++
	case ']':
		s.bracket--
		s.operand = true
	}
	return s.brace >= 0 && s.bracket >= 0
}

// comma reads a comma: between the arguments of the top label_replace, each after the first one string.
func (s *metricScan) comma() bool {
	if s.labelReplaceAt > 0 && s.paren == s.labelReplaceAt {
		if s.labelReplaceCommas > 0 && !s.argString {
			return false
		}
		s.labelReplaceCommas++
		s.argString = false
	}
	return true
}

// number reads the number, duration or byte size at q[i].
func (s *metricScan) number(q string, i int) (int, bool) {
	if s.labelReplaceArg() || s.quantileArg && !shortestNumber(q, i) {
		return i, false
	}
	s.quantileArg = false
	j := i
	for j < len(q) && (isNameByte(q[j]) || q[j] == '.') {
		j++
	}
	s.operand = true
	return j - 1, true
}

// open reads an opening parenthesis.
func (s *metricScan) open() bool {
	s.paren++
	if s.pendingVector && s.pending != pendingClause {
		if s.vectors > 0 {
			s.nestedVector = true
		}
		if s.vectors == len(s.vectorAt) {
			return false
		}
		s.vectorAt[s.vectors] = s.paren
		s.vectors++
		s.pendingVector = false
	}
	switch s.pending {
	case pendingRange:
		s.rangeAt = s.paren
	case pendingClause:
		s.clauseAt = s.paren
	case pendingLabelReplace:
		if s.labelReplaceAt == 0 {
			s.labelReplaceAt = s.paren
		}
	}
	s.pending, s.operand = 0, false
	return true
}

// close reads a closing parenthesis.
func (s *metricScan) close() bool {
	s.afterRange = false
	if s.vectors > 0 && s.vectorAt[s.vectors-1] == s.paren {
		s.vectors--
	}
	switch s.paren {
	case s.rangeAt:
		s.rangeAt, s.afterRange = 0, true
	case s.clauseAt:
		s.clauseAt = 0
	}
	if s.paren--; s.paren < 0 {
		return false
	}
	s.operand = true
	return true
}

// labelReplaceArg reports a position in one of the string arguments of the top label_replace call.
func (s *metricScan) labelReplaceArg() bool {
	return s.labelReplaceAt > 0 && s.paren == s.labelReplaceAt && s.labelReplaceCommas > 0
}

// shortestNumber reports a number at q[i:] written as the shortest form of its value
// (strconv.FormatFloat(v, 'g', -1, 64)): no exponent, no leading zero but the one before the point, no point without
// a digit on each side, no trailing zero after the point, and at most 15 digits.
func shortestNumber(q string, i int) bool {
	j, digits, point := i, 0, -1
	for ; j < len(q); j++ {
		switch c := q[j]; {
		case c >= '0' && c <= '9':
			digits++
		case c == '.' && point < 0:
			point = j
		default:
			if isNameByte(c) || c == '.' {
				return false // an exponent, a suffix, a second point
			}
			goto done
		}
	}
done:
	n := q[i:j]
	switch {
	case digits == 0 || digits > 15 || n[0] == '.' || n[len(n)-1] == '.':
		return false
	case len(n) > 1 && n[0] == '0' && n[1] != '.':
		return false
	case point >= 0 && n[len(n)-1] == '0':
		return false
	}
	return true
}

// isNameStart reports the first byte of a name.
func isNameStart(c byte) bool { return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

// top reports a position at depth 0: outside every parenthesis, selector and range.
func (s *metricScan) top() bool { return s.paren == 0 && s.brace == 0 && s.bracket == 0 }

// word reads the word q[i:j] and reports false for one ScreenMetric turns away.
func (s *metricScan) word(q string, i, j int) bool {
	w := q[i:j]
	if done, ok := s.label(w); done {
		return ok
	}
	if done, ok := s.keyword(q, w, j); done {
		return ok
	}
	if s.keepStage {
		// A keep of names and matchers together loses the order they were written in (the lowering leaves it to
		// the translator).
		if comparisonFollows(q, dottedEnd(q, j)) {
			s.keepMatcher = true
		} else {
			s.keepName = true
		}
		if s.keepName && s.keepMatcher {
			return false
		}
	}
	if s.log && !s.names && !comparisonFollows(q, dottedEnd(q, j)) && !callFollows(q, j) {
		// In a log pipeline, a name (dotted ones included: k8s.pod.name) with no comparison after it: a label filter the
		// parser keeps untyped.
		return false
	}
	if s.first == "" && s.top() {
		s.first = w
	}
	if !s.log && s.rangeAt == 0 && vectorWord(w) {
		s.pendingVector = true
	}
	if !callFollows(q, j) {
		return true
	}
	return s.call(w)
}

// label reads a word that is a duration, a label name of a selector or a clause, or the label an unwrap names; done
// is false for another word.
func (s *metricScan) label(w string) (done, ok bool) {
	switch {
	case s.bracket > 0:
		return true, true // a duration or a step: [5m], [1h:1m]
	case reservedWord(w) && !s.parsed:
		return true, false
	case s.brace > 0:
		return true, true // a selector's label name
	case s.clauseAt > 0 && s.paren == s.clauseAt:
		s.candidate = s.candidate || translate.ParsedLabelCandidate(w)
		s.groupsServiceName = s.groupsServiceName || w == "service_name"
		return true, true
	case !s.unwrapLabel:
		return false, false
	}
	switch w {
	case "duration", "duration_seconds", "bytes":
		return true, true
	}
	s.unwrapLabel = false
	s.candidate = s.candidate || translate.ParsedLabelCandidate(w)
	// An unwrap of a reserved name in another letter case (UnwrapsReservedFieldOtherCase).
	return true, !reservedWord(w)
}

// keyword reads a word that names a stage, a clause or a construct the screen decides on; done is false for another
// word.
func (s *metricScan) keyword(q, w string, j int) (done, ok bool) {
	if s.log {
		if done, ok := s.logKeyword(q, w, j); done {
			return true, ok
		}
	}
	switch w {
	case "topk", "bottomk", "sort", "sort_desc", "stddev", "stdvar", "vector", "bool", "line_format", "decolorize", "unpack":
		return true, false
	case "regexp", "pattern":
		// A parser written with no blank before its quoted expression (`|pattern"<_> <m>"`) is the one with the blank to
		// Loki and the plan (semantics/translator-copies-unspaced-parser): the plan writes the spaced spelling's text.
		if j >= len(q) || !isBlank(q[j]) && !unspacedParser(q, j) {
			return true, false
		}
		s.parsedOther = true
	case "and", "or", "unless":
		// A set operator; inside a log pipeline, a label filter's.
		return true, s.pipeline()
	case "json", "logfmt":
		s.parsed = true
	case "drop":
		s.afterParser = s.afterParser || s.parsed
	case "keep":
		s.keep = true
	case "unwrap":
		s.unwrap, s.unwrapLabel = true, true
	case "detected_level":
		// A level filter after a parser.
		return s.parsed && s.rangeAt > 0, false
	case "by", "without":
		// A grouping after a range function inside an aggregation, a without after one, and an empty without clause.
		if s.afterRange && (s.paren > 0 || w == "without") || w == "without" && emptyClause(q, j) {
			return true, false
		}
		s.without = s.without || w == "without"
		s.pending = pendingClause
		return true, true
	}
	return false, false
}

// call reads the name of a call: only the ones the plan writes as the translator does.
func (s *metricScan) call(w string) bool {
	switch w {
	case "sum", "avg", "min", "max", "count":
		return true
	case "label_replace":
		if s.first != w {
			return false // the plan does not write a label_replace under another construct
		}
		s.labelReplaces++
		s.pending = pendingLabelReplace
		return true
	case "on", "ignoring", "group_left", "group_right", "duration", "duration_seconds", "bytes", "ip":
		return true
	}
	if !rangeFunctionWord(w) {
		return false
	}
	s.ranges++
	s.pending = pendingRange
	s.quantileArg = w == "quantile_over_time"
	switch w {
	case "rate", "bytes_rate", "count_over_time", "bytes_over_time", "absent_over_time":
	default:
		s.needsUnwrap = true
	}
	return true
}

// vectorWord reports the name of a vector aggregation the plan lowers.
func vectorWord(w string) bool {
	switch w {
	case "sum", "avg", "min", "max", "count":
		return true
	}
	return false
}

// operator reads the operator at q[i] and returns the index of its last byte; false for a byte the screen does not
// read, a sign, a comparison with a signed number, an equality on a number after a parser, and a binary operation
// inside an aggregation.
func (s *metricScan) operator(q string, i int) (int, bool) {
	end := i
	if i+1 < len(q) && strings.IndexByte("=!<>", q[i]) >= 0 && (q[i+1] == '=' || q[i+1] == '~' || q[i] == '!' && q[i+1] == '>') {
		end = i + 1
	}
	switch op := q[i : end+1]; op {
	case "=~", "!~", "!>":
		return end, true
	case "+", "-", "*", "/", "%", "^":
		// Arithmetic: a binary operator after a value; never in a log query.
		if s.log || op != "*" && op != "/" && op != "%" && op != "^" && !s.operand {
			return 0, false
		}
	case "=", "==", "!=", ">", "<", ">=", "<=":
		if s.brace > 0 {
			return end, true
		}
		if (s.paren > 0 || s.log) && !s.comparison(q, end+1, op) {
			return 0, false
		}
		if s.pipeline() {
			s.afterParser = s.afterParser || s.parsed
			s.afterParser = s.afterParser || s.parsedOther
		}
	default:
		return 0, false
	}
	switch {
	case s.log:
		// A label filter, or a line filter written != after the selector.
	case s.top():
		s.binaries++
		s.binary = true
	case s.rangeAt > 0 || s.brace > 0:
		// A label filter, an offset.
	case s.labelReplaceAt > 0 && s.paren == s.labelReplaceAt && s.labelReplaceCommas == 0:
		// The operand of a label_replace call is a binary operation.
		s.lrBinary = true
	default:
		return 0, false
	}
	return end, true
}

// comparison reports false for a label filter value ScreenMetric turns away: a signed number, or a number, duration
// or byte size compared for equality after a json or logfmt parser.
func (s *metricScan) comparison(q string, i int, op string) bool {
	for i < len(q) && isBlank(q[i]) {
		i++
	}
	if i >= len(q) {
		return false
	}
	c := q[i]
	if c == '-' || c == '+' {
		return false
	}
	numeric := c >= '0' && c <= '9' || c == '.'
	return !numeric || !s.parsed || op != "=" && op != "==" && op != "!="
}

// callFollows reports an opening parenthesis after the blanks at q[j:].
func callFollows(q string, j int) bool {
	for j < len(q) && isBlank(q[j]) {
		j++
	}
	return j < len(q) && q[j] == '('
}

// emptyClause reports a clause with nothing between its parentheses at q[j:]: `without ()`.
func emptyClause(q string, j int) bool {
	for j < len(q) && isBlank(q[j]) {
		j++
	}
	if j >= len(q) || q[j] != '(' {
		return false
	}
	for j++; j < len(q) && isBlank(q[j]); j++ {
	}
	return j < len(q) && q[j] == ')'
}

// misreadQuoted reports the content of a quoted string the string translator reads as syntax: detected_level or
// service_name in it (when names is set), a byte from \x00 to \x02, or (in a double-quoted string) an escape that decodes to one or that
// the parser does not read.
func misreadQuoted(v string, quote byte, names bool) bool {
	if names && (strings.Contains(v, translate.DetectedLevelLabel) || strings.Contains(v, "service_name")) {
		return true
	}
	for i := 0; i < len(v); i++ {
		switch c := v[i]; {
		case c <= 2:
			return true
		case c == '\\' && quote == '"':
			n, ok := escapeLength(v, i+1)
			if !ok {
				return true
			}
			i += n
		}
	}
	return false
}

// escapeLength reads the escape after a backslash at v[i:] and returns its length; false for one that is not an
// escape of a Go string or that decodes to a byte from \x00 to \x02.
func escapeLength(v string, i int) (int, bool) {
	if i >= len(v) {
		return 0, false
	}
	digits, base := 0, 16
	switch e := v[i]; {
	case strings.IndexByte(`abfnrtv\'"`, e) >= 0:
		return 1, true
	case e == 'x':
		digits = 2
	case e == 'u':
		digits = 4
	case e == 'U':
		digits = 8
	case e >= '0' && e <= '7':
		digits, base, i = 3, 8, i-1
	default:
		return 0, false
	}
	if i+1+digits > len(v) {
		return 0, false
	}
	value := 0
	for _, d := range []byte(v[i+1 : i+1+digits]) {
		switch {
		case d >= '0' && d <= '9' && int(d-'0') < base:
			value = value*base + int(d-'0')
		case base == 16 && (d|0x20) >= 'a' && (d|0x20) <= 'f':
			value = value*16 + int(d|0x20-'a') + 10
		default:
			return 0, false
		}
		if value > 2 {
			value = 3 // large enough: only a value up to 2 is turned away
		}
	}
	if base == 8 {
		return digits, value > 2
	}
	return 1 + digits, value > 2
}

// reservedWord reports a word VictoriaLogs reserves as a field name, in any letter case.
func reservedWord(w string) bool {
	if len(w) < 4 || w[0] != '_' {
		return false
	}
	return strings.EqualFold(w, "_time") || strings.EqualFold(w, "_msg") || strings.EqualFold(w, "_stream") ||
		strings.EqualFold(w, "_stream_id")
}

// rangeFunctionWord reports the name of a range aggregation the plan lowers.
func rangeFunctionWord(w string) bool {
	switch w {
	case "rate", "rate_counter", "bytes_rate", "count_over_time", "bytes_over_time", "sum_over_time", "avg_over_time", "max_over_time",
		"min_over_time", "first_over_time", "last_over_time", "stdvar_over_time", "stddev_over_time",
		"quantile_over_time", "absent_over_time":
		return true
	}
	return false
}
