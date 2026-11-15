package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// LogStages is a set of the stage families of a log query: the unit in which log queries with more than line and
// label filters move from the string translator to the plan.
type LogStages uint8

// The stage families of a LogStages.
const (
	// LogLevelFilter is a label filter on detected_level, the label the proxy derives from the stored fields and the
	// line.
	LogLevelFilter LogStages = 1 << iota
	// LogRegexpPattern is a regexp or pattern parser, and the label filters after it.
	LogRegexpPattern
	// LogUnspacedParser is a regexp or pattern parser written with no blank before its expression (`| pattern"<_> <m>"`):
	// the string translator copies it as written, which VictoriaLogs rejects (it has no regexp or pattern pipe), and the
	// plan writes the extract pipe it writes for the spaced spelling (semantics/translator-copies-unspaced-parser). A
	// caller that serves it serves the query's regexp and pattern parsers with it.
	LogUnspacedParser
)

// logStageNames are the names of the stage families, as LogStagesOf reads them.
var logStageNames = [...]struct {
	name  string
	stage LogStages
}{
	{"LevelFilter", LogLevelFilter},
	{"RegexpPattern", LogRegexpPattern},
	{"UnspacedParser", LogUnspacedParser},
}

// LogStagesOf is the set of the stage families among the names; a name that is not one adds nothing.
func LogStagesOf(names []string) LogStages {
	var set LogStages
	for _, n := range names {
		for _, s := range logStageNames {
			if s.name == n {
				set |= s.stage
			}
		}
	}
	return set
}

// ScreenLogStages is the screen of a log query with more than line and label filters ahead of its plan: one pass over
// the text, with no allocation, that answers the stage families the query holds, or 0 for a query the plan would leave
// to the string translator. A caller that serves some families from the plan calls it before it parses the query, so a
// query the plan does not serve costs no parse, build or resolve on top of its translation.
//
// It reads the pipeline as ScreenMetric reads one inside a range function, and answers 0 for a query with any of the
// classes it turns away there (outside quoted strings, unless said otherwise):
//   - a stage the lowering leaves to the translator or a family the screen does not name yet (unpack, line_format,
//     unwrap, and every parser and formatting stage until its family is named here), and a regexp or pattern parser
//     with no blank before its expression;
//   - a filter on service_name (the lowering leaves it to the translator) and a level filter after a parser;
//   - a label filter with an equality on a number, duration or byte size after a json or logfmt parser
//     (WritesNumericEquality), or a comparison with a signed number;
//   - a word VictoriaLogs reserves as a field name before a parser (ReadsReservedField);
//   - a quoted string with detected_level or service_name in it, a byte from \x00 to \x02, or an escape that could
//     write one;
//   - a comment, a grouping word, and text it does not read.
//
// Like ScreenMetric it only turns queries away: the typed gate on the plan and the lowering decide every query it
// passes. The proxy's corpus test holds it to the plan.
func ScreenLogStages(query string) LogStages { return ScreenLogStagesIn(query, ^LogStages(0)) }

// ScreenLogStagesIn is ScreenLogStages for a caller that serves only the families of allowed: it answers 0 at the first
// family outside it, so the rest of such a query is not read. A regexp or pattern parser is allowed with the blank too
// once an unspaced one came before it and allowed holds LogUnspacedParser (the caller serves them together).
func ScreenLogStagesIn(query string, allowed LogStages) LogStages {
	query = strings.TrimSpace(query)
	if query == "" || query[0] != '{' {
		return 0
	}
	s := metricScan{log: true, allowed: allowed}
	if !s.scan(query) {
		return 0
	}
	return s.stages
}

// PlanLogStages is the set of the stage families of a log query's plan, the typed answer ScreenLogStages reads from
// text: false for a plan that is not a log query or that has a stage no family holds. A plan with only line and label
// filters on other labels has no family (0, true).
func PlanLogStages(p *queryplan.Plan) (LogStages, bool) {
	sel, ok := p.Root.(*queryplan.LogSelect)
	if !ok {
		return 0, false
	}
	var set LogStages
	for _, st := range sel.Stages {
		switch v := st.(type) {
		case *queryplan.LineFilter:
		case *queryplan.Parse:
			if v.Type != logql.ParserRegexp && v.Type != logql.ParserPattern {
				return 0, false
			}
			set |= LogRegexpPattern
		case *queryplan.LabelFilter:
			if v.Expr == nil {
				return 0, false
			}
			if namesDerivedLabel(v.Expr, translate.DetectedLevelLabel) {
				set |= LogLevelFilter
			}
			if namesDerivedLabel(v.Expr, "service_name") {
				return 0, false
			}
		default:
			return 0, false
		}
	}
	return set, true
}

// namesDerivedLabel reports a leaf of e that filters on the label name.
func namesDerivedLabel(e *logql.LabelFilterExpr, name string) bool {
	if e == nil {
		return false
	}
	if !e.IsLeaf() {
		return namesDerivedLabel(e.Left, name) || namesDerivedLabel(e.Right, name)
	}
	return e.Name == name
}

// logKeyword reads a word of a log query that names a stage or a derived label; done is false for another word, which
// keyword then reads as in a metric query.
func (s *metricScan) logKeyword(q, w string, j int) (done, ok bool) {
	switch w {
	case translate.DetectedLevelLabel:
		// A level filter after a stage (the lowering writes one only on the stored fields and the line), and the name
		// with no comparison after it, which the parser keeps untyped.
		if s.staged || !comparisonFollows(q, j) || s.allowed&LogLevelFilter == 0 {
			return true, false
		}
		s.stages |= LogLevelFilter
		return true, true
	case "regexp", "pattern":
		// A regexp or pattern parser: a family of its own, and one written with no blank before its expression. The
		// word followed by anything but its quoted expression is a label (`| pattern="x"`), left to the translator.
		k := j
		for k < len(q) && isBlank(q[k]) {
			k++
		}
		if !unspacedParser(q, k) {
			return true, false
		}
		unspaced := s.allowed&LogUnspacedParser != 0 && (k == j || s.stages&LogUnspacedParser != 0)
		if s.allowed&LogRegexpPattern == 0 && !unspaced {
			return true, false
		}
		s.staged, s.parsedOther = true, true
		s.stages |= LogRegexpPattern
		if k == j {
			s.stages |= LogUnspacedParser
		}
		return true, true
	case "service_name", "unwrap", "by", "without", "json", "logfmt", "unpack", "drop", "keep",
		"line_format", "label_format", "decolorize":
		// A filter on service_name, a stage the lowering leaves to the translator, and a family not served yet.
		return true, false
	}
	return false, false
}

// comparisonFollows reports a label filter's comparison operator after the blanks at q[j:]: =, ==, !=, =~, !~, <, <=,
// > or >=. A ! before anything but = and ~ (the pattern filter !>) is not one.
func comparisonFollows(q string, j int) bool {
	for j < len(q) && isBlank(q[j]) {
		j++
	}
	switch {
	case j >= len(q):
		return false
	case q[j] == '!':
		return j+1 < len(q) && (q[j+1] == '=' || q[j+1] == '~')
	}
	return strings.IndexByte("=<>", q[j]) >= 0
}

// dottedEnd is the end of a dotted name whose first part ends at q[j]: past every `.part` that follows.
func dottedEnd(q string, j int) int {
	for j+1 < len(q) && q[j] == '.' && isNameStart(q[j+1]) {
		j++
		for j < len(q) && isNameByte(q[j]) {
			j++
		}
	}
	return j
}

// unspacedParser reports a regexp or pattern parser whose name ends at q[j] and whose expression, a quoted string,
// follows it with no blank (`|pattern"<_> <m>"`, "|regexp`(?P<a>x)`").
func unspacedParser(q string, j int) bool {
	return j < len(q) && (q[j] == '"' || q[j] == '`')
}
