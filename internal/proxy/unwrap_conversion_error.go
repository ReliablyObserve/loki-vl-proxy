package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/grafana/jsonparser"
)

// Unwrap conversion errors (LogQL docs, "Unwrapped range aggregations"): a line whose unwrapped label holds a value
// the conversion rejects makes a sample marked __error__="SampleExtractionErr", and a metric query fails with that
// pipeline error (HTTP 400, text) unless a label filter after the unwrap drops it. The answers come from Loki,
// recorded: TestUnwrapConversionErrorMatchesRecordedLoki (status, text, the series the error names, which lines
// make no sample).
//
// The proxy detects the error on the metric's own VictoriaLogs read: the unwrap gate of every stats query counts
// the values it would drop instead of dropping them (flag 1, value 0, a group of their own in every stats pipe),
// the flagged groups are taken out of the answer, and the buckets they held are remembered (hits). A clean read
// costs nothing more. After a hit, bounded lookups read the flagged rows of the hit's buckets, each line is read
// again as stored, and Loki's value of the label is derived from the line with Loki's parsers; only a value Loki
// rejects answers Loki's error. A nan value is a sample (strconv.ParseFloat reads it): the gate counts nan rows
// apart (flag 2) so the answer carries the NaN where every function of the query poisons its result.

// unwrapErrorProbe is one range aggregation over an unwrapped label whose conversion errors fail the query: the
// label, its conversion, the range, the stages before and after the unwrap, and the parser hints (the labels the
// parsers extract; nil extracts every label).
type unwrapErrorProbe struct {
	label          string
	conv           string
	window         time.Duration
	offset         time.Duration
	pipeline, post []logqlpkg.Stage
	hints          map[string]bool
}

// unwrapErrorProbes returns the range aggregations of expr whose conversion errors fail the query and the detection
// evaluates: a range aggregation, under vector aggregations and binary operations (not under label_replace, nor at
// an offset: registered as outside the detection).
func unwrapErrorProbes(expr logqlpkg.Expr) []unwrapErrorProbe {
	var out []unwrapErrorProbe
	var walk func(e logqlpkg.Expr, parent *logqlpkg.VectorAggregation)
	walk = func(e logqlpkg.Expr, parent *logqlpkg.VectorAggregation) {
		switch n := e.(type) {
		case *logqlpkg.RangeAggregation:
			if probe, ok := unwrapErrorProbeFor(n, parent); ok {
				out = append(out, probe)
			}
		case *logqlpkg.VectorAggregation:
			walk(n.Inner, n)
		case *logqlpkg.BinOpExpr:
			walk(n.Left, nil)
			walk(n.Right, nil)
		case *logqlpkg.OpaqueMetricExpr:
			if n.Typed && n.Inner != nil {
				walk(n.Inner, nil) // label_replace keeps its sample expression's errors
			}
		}
	}
	if expr != nil {
		walk(expr, nil)
	}
	return out
}

// unwrapErrorProbeFor is the probe of ra under the vector aggregation parent (nil: none), and whether Loki fails
// the query on its conversion errors in a way the detection evaluates.
func unwrapErrorProbeFor(ra *logqlpkg.RangeAggregation, parent *logqlpkg.VectorAggregation) (unwrapErrorProbe, bool) {
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok || ra.Offset != "" {
		// A range read at an offset is outside what the detection evaluates (registered).
		return unwrapErrorProbe{}, false
	}
	at := -1
	for i, st := range lq.Pipeline {
		if _, ok := st.(*logqlpkg.UnwrapStage); ok {
			at = i
		}
	}
	if at < 0 {
		return unwrapErrorProbe{}, false
	}
	uw := lq.Pipeline[at].(*logqlpkg.UnwrapStage)
	if strings.TrimSpace(uw.Label) == "" {
		return unwrapErrorProbe{}, false
	}
	post := lq.Pipeline[at+1:]
	if fails, known := unwrapErrorPostFilters(post); !fails || !known {
		return unwrapErrorProbe{}, false
	}
	pipeline := lq.Pipeline[:at]
	probe := unwrapErrorProbe{
		label:    uw.Label,
		conv:     translator.UnwrapConv(uw.Converter),
		window:   ra.RangeDuration,
		offset:   ra.OffsetDuration,
		pipeline: pipeline,
		post:     post,
	}
	probe.hints = unwrapParserHints(ra, parent, pipeline, post, uw.Label)
	return probe, true
}

// unwrapErrorPostFilters reads the label filters after the unwrap: fails is whether a line whose conversion
// failed can pass them (Loki then fails the query), known whether the detection evaluates them the way Loki does.
// A filter on __error__ or __error_details__ is decided here: at that point Loki holds __error__ as
// SampleExtractionErr and __error_details__ empty (recorded). VictoriaLogs' rows hold neither, so a filter that
// keeps Loki's failing line but drops a VictoriaLogs row hides the line from the counter: not evaluated. A string
// filter on another label is evaluated per line (unwrapLabelFilterKeeps). Anything else (a numeric comparison, a
// group, and, or) is not evaluated.
func unwrapErrorPostFilters(post []logqlpkg.Stage) (fails, known bool) {
	fails = true
	for _, st := range post {
		lf, ok := st.(*logqlpkg.LabelFilterStage)
		if !ok {
			return false, false
		}
		expr := lf.Expr
		if expr == nil || !expr.IsLeaf() || strings.HasPrefix(strings.TrimSpace(lf.Raw), "(") || expr.Kind != logqlpkg.LabelFilterValueString {
			return false, false
		}
		condition, err := translator.NewLabelFilterCondition(expr.Name, expr.Cmp, expr.Value)
		if err != nil {
			return false, false
		}
		var lokiValue string
		switch expr.Name {
		case errorLabel:
			lokiValue = "SampleExtractionErr"
		case errorDetailsLabel:
			lokiValue = ""
		default:
			continue
		}
		lokiKeeps, vlKeeps := condition.Matches(lokiValue), condition.Matches("")
		switch {
		case !lokiKeeps:
			fails = false
		case !vlKeeps:
			return false, false
		}
	}
	return fails, true
}

// unwrapComparison reads a label filter stage (a group of one filter included) that compares one label, named
// with letters, digits and underscores, with a number, a duration or a byte size: the label, the comparison and the
// literal as written.
func unwrapComparison(stage *logqlpkg.LabelFilterStage) (label, cmp, literal string, ok bool) {
	if stage == nil || stage.Expr == nil || !stage.Expr.IsLeaf() || !unwrapPlainLabelName(stage.Expr.Name) {
		return "", "", "", false
	}
	e := stage.Expr
	switch e.Kind {
	case logqlpkg.LabelFilterValueNumber, logqlpkg.LabelFilterValueDuration, logqlpkg.LabelFilterValueBytes:
	default:
		return "", "", "", false
	}
	switch e.Cmp {
	case "==", "!=", ">=", "<=", ">", "<", "=":
	default:
		return "", "", "", false
	}
	return e.Name, e.Cmp, e.Value, true
}

// unwrapPlainLabelName reports a label name of letters, digits and underscores that does not start with a digit:
// the names a numeric comparison is read for.
func unwrapPlainLabelName(name string) bool {
	for i, c := range name {
		if !unwrapNameRune(c) || i == 0 && c >= '0' && c <= '9' {
			return false
		}
	}
	return name != ""
}

// unwrapNameRune reports an ASCII letter, digit or underscore.
func unwrapNameRune(c rune) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// unwrapParserHints is the set of labels Loki's parsers extract for ra under parent: the labels the series of the
// aggregation keep and the unwrapped label, when the query names them (a range aggregation's own by, a sum by labels
// or none of sum_over_time or rate, absent_over_time, which keeps none), with the labels the query's label filters
// read; nil when Loki extracts every label.
// Recorded: TestUnwrapConversionErrorMatchesRecordedLoki (the series named in the error).
func unwrapParserHints(ra *logqlpkg.RangeAggregation, parent *logqlpkg.VectorAggregation, pipeline, post []logqlpkg.Stage, label string) map[string]bool {
	var labels []string
	switch {
	case ra.Op == logqlpkg.RangeAbsentOverTime:
	case ra.Grouping != nil:
		if ra.Grouping.Without {
			return nil
		}
		labels = ra.Grouping.Labels
	case parent != nil && parent.Op == logqlpkg.VectorSum && (ra.Op == logqlpkg.RangeSumOverTime || ra.Op == logqlpkg.RangeRate):
		if parent.Grouping != nil {
			if parent.Grouping.Without {
				return nil
			}
			labels = parent.Grouping.Labels
		}
	default:
		return nil
	}
	hints := make(map[string]bool, len(labels)+1)
	hints[label] = true
	if base, ok := strings.CutSuffix(label, extractedSuffix); ok && base != "" {
		// The parser extracts the key under its own name; the suffix comes from the collision.
		hints[base] = true
	}
	for _, name := range labels {
		hints[name] = true
	}
	// A label a filter reads is extracted for it.
	for _, st := range append(append([]logqlpkg.Stage{}, pipeline...), post...) {
		if lf, ok := st.(*logqlpkg.LabelFilterStage); ok && lf.Expr != nil {
			unwrapFilterLabels(lf.Expr, hints)
		}
	}
	return hints
}

// unwrapFilterLabels adds the labels a label filter expression reads to names.
func unwrapFilterLabels(e *logqlpkg.LabelFilterExpr, names map[string]bool) {
	if e == nil {
		return
	}
	if e.IsLeaf() {
		if e.Name != "" {
			names[e.Name] = true
		}
		return
	}
	unwrapFilterLabels(e.Left, names)
	unwrapFilterLabels(e.Right, names)
}

// unwrapNaNPattern is a nan value, which strconv.ParseFloat reads and the gate counts apart.
const unwrapNaNPattern = `^(?i:nan)$`

// unwrapAcceptablePattern is the syntax of the values the conversion conv accepts or overflows on (a number by
// syntax): a value outside it is a conversion error.
func unwrapAcceptablePattern(conv string) string {
	out := "(?:" + translator.UnwrapPattern(conv) + ")"
	if conv == "" {
		out += "|(?:" + unwrapNaNPattern + ")"
	}
	return out
}

// unwrapLookupQuery reads the rows of base whose unwrapped field the counting gate flags.
func unwrapLookupQuery(base, field, conv string) string {
	return base + unwrapCountedGate(field, conv) + ` | filter ` + unwrapBadField + `:="1"`
}

// unwrapBadField is the counting gate's flag: 0 a value that makes a sample, 1 a value the conversion rejects,
// 2 nan, 3 infinity (read as inf, then 0).
const unwrapBadField = "__lvp_bad"

// unwrapGateFieldRE finds the quoted field of a filter that may start an unwrap gate.
var unwrapGateFieldRE = regexp.MustCompile(` \| filter ("(?:[^"\\]|\\.)*"):~`)

// unwrapCountingQuery is unwrapCountingRewrite that counts the conversion errors of every gate accept takes.
func unwrapCountingQuery(query string, accept func(field, conv string) bool) (rewritten, base, field, conv string, ok bool) {
	rewritten, base, field, conv, _, ok = unwrapCountingRewrite(query, func(f, c string) (bool, bool) { return true, accept(f, c) })
	return rewritten, base, field, conv, ok
}

// unwrapCountingRewrite rewrites a stats query that holds an unwrap gate (translator.UnwrapGateFor) into the
// counting gate (unwrapCountedGateMode) and groups every stats pipe after it by the flag. accept decides, for the
// gate's field and conversion, whether the rewrite applies (ok) and whether it counts the conversion errors too
// (errors), not only the nan values. A query whose pipes after the gate could drop a flagged group, or that holds
// no stats pipe after it, is returned unchanged with ok false.
func unwrapCountingRewrite(query string, accept func(field, conv string) (errors, ok bool)) (rewritten, base, field, conv string, errors, ok bool) {
	at := -1
	for _, m := range unwrapGateFieldRE.FindAllStringSubmatchIndex(query, -1) {
		name, err := strconv.Unquote(query[m[2]:m[3]])
		if err != nil {
			continue
		}
		for _, c := range []string{"", "duration", "bytes"} {
			if strings.HasPrefix(query[m[0]:], translator.UnwrapGateFor(name, c)) {
				at, field, conv = m[0], name, c
				break
			}
		}
		if at >= 0 {
			break
		}
	}
	if at < 0 {
		return query, "", "", "", false, false
	}
	errors, ok = accept(field, conv)
	if !ok {
		return query, "", "", "", false, false
	}
	rest, grouped := unwrapGroupStatsByFlag(query[at+len(translator.UnwrapGateFor(field, conv)):])
	if !grouped {
		return query, "", "", "", false, false
	}
	base = query[:at]
	return base + unwrapCountedGateMode(field, conv, errors) + rest, base, field, conv, errors, true
}

// unwrapGroupStatsByFlag adds the flag to the grouping of every stats pipe of rest (the pipes after the gate).
// ok is false for a pipe that could drop or reorder a flagged group (anything but stats and the row-wise pipes,
// unwrapRowPipe), or when no stats pipe follows.
func unwrapGroupStatsByFlag(rest string) (string, bool) {
	pipes, ok := unwrapSplitPipes(rest)
	if !ok {
		return rest, false
	}
	var b strings.Builder
	b.Grow(len(rest) + 32)
	stats := false
	for _, pipe := range pipes {
		b.WriteString(" | ")
		switch {
		case strings.HasPrefix(pipe, "stats "):
			by, funcs, ok := splitStatsPipe(pipe)
			if !ok {
				return rest, false
			}
			stats = true
			b.WriteString("stats by (")
			for _, name := range by {
				b.WriteString(name)
				b.WriteString(", ")
			}
			b.WriteString(unwrapBadField + ") ")
			b.WriteString(funcs)
		case unwrapRowPipe(pipe):
			b.WriteString(pipe)
		default:
			return rest, false
		}
	}
	return b.String(), stats
}

// unwrapRowPipes are the pipes that compute fields of each row and keep every row and its fields: the flag reaches
// the stats after them.
var unwrapRowPipes = []string{"math ", "unpack_json ", "unpack_logfmt ", "delete ", "pack_json ", "hash(", "format ", "copy ", "replace_regexp ", "extract_regexp "}

// unwrapRowPipe reports a pipe the flag passes: a row-wise pipe, or a filter on the proxy's own fields (the window
// phase, the rate's presence), which flagged rows pass like any other.
func unwrapRowPipe(pipe string) bool {
	if strings.HasPrefix(pipe, "filter __lvp_") {
		return !strings.Contains(pipe, " or ") && !strings.Contains(pipe, unwrapBadField)
	}
	for _, head := range unwrapRowPipes {
		if strings.HasPrefix(pipe, head) {
			return !strings.Contains(pipe, unwrapBadField)
		}
	}
	return false
}

// unwrapSplitPipes splits text (which starts with " | ") into its pipes at the separators outside quotes and
// parentheses.
func unwrapSplitPipes(text string) ([]string, bool) {
	if !strings.HasPrefix(text, " | ") {
		return nil, false
	}
	var pipes []string
	depth, start := 0, 3
	for i := 3; i < len(text); i++ {
		switch c := text[i]; c {
		case '"', '`', '\'':
			end := unwrapClosingQuote(text, i)
			if end < 0 {
				return nil, false
			}
			i = end
		case '(':
			depth++
		case ')':
			depth--
		case ' ':
			if depth == 0 && strings.HasPrefix(text[i:], " | ") {
				pipes = append(pipes, text[start:i])
				start = i + 3
				i += 2
			}
		case '|':
			if depth == 0 {
				return nil, false
			}
		}
	}
	return append(pipes, text[start:]), depth == 0
}

// unwrapClosingQuote is the index of the quote that closes the one at text[open], or -1.
// topLevelStatsIndex is the index of the first " | stats " of query outside parentheses and quotes (not a
// subquery's), -1 when there is none.
func topLevelStatsIndex(query string) int {
	depth := 0
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '"', '`', '\'':
			end := unwrapClosingQuote(query, i)
			if end < 0 {
				return -1
			}
			i = end
		case '(':
			depth++
		case ')':
			depth--
		case ' ':
			if depth == 0 && strings.HasPrefix(query[i:], " | stats ") {
				return i
			}
		}
	}
	return -1
}

func unwrapClosingQuote(text string, open int) int {
	q := text[open]
	for i := open + 1; i < len(text); i++ {
		switch text[i] {
		case '\\':
			if q != '`' {
				i++
			}
		case q:
			return i
		}
	}
	return -1
}

// splitStatsPipe reads a stats pipe: the names it groups by and its function list.
func splitStatsPipe(pipe string) (by []string, funcs string, ok bool) {
	rest := strings.TrimPrefix(pipe, "stats ")
	if strings.HasPrefix(rest, "by (") {
		end := strings.IndexByte(rest, ')')
		if end < 0 {
			return nil, "", false
		}
		for _, name := range strings.Split(rest[len("by ("):end], ",") {
			if name = strings.TrimSpace(name); name != "" && name != unwrapBadField {
				by = append(by, name)
			}
		}
		rest = strings.TrimPrefix(rest[end+1:], " ")
	}
	if _, ok := statsFunctions(rest); !ok {
		return nil, "", false
	}
	return by, rest, true
}

// statsFunction is one function of a stats pipe: its name and the name of its result.
type statsFunction struct{ fn, result string }

// statsFunctions reads a stats pipe's function list (`sum(x) as c, count()`): each function and the name of its
// result (the alias, else the call as written).
func statsFunctions(list string) ([]statsFunction, bool) {
	var out []statsFunction
	for len(list) > 0 {
		open := strings.IndexByte(list, '(')
		if open <= 0 {
			return nil, false
		}
		fn := list[:open]
		for _, c := range fn {
			if !unwrapNameRune(c) {
				return nil, false
			}
		}
		depth, end := 0, -1
		for i := open; i < len(list) && end < 0; i++ {
			switch list[i] {
			case '(':
				depth++
			case ')':
				if depth--; depth == 0 {
					end = i
				}
			}
		}
		if end < 0 {
			return nil, false
		}
		f := statsFunction{fn: fn, result: list[:end+1]}
		list = list[end+1:]
		if alias, ok := strings.CutPrefix(list, " as "); ok {
			stop := strings.Index(alias, ", ")
			if stop < 0 {
				stop = len(alias)
			}
			f.result = alias[:stop]
			if f.result == "" || strings.ContainsAny(f.result, " |()\"") {
				return nil, false
			}
			list = alias[stop:]
		}
		out = append(out, f)
		if list == "" {
			break
		}
		next, ok := strings.CutPrefix(list, ", ")
		if !ok {
			return nil, false
		}
		list = next
	}
	return out, len(out) > 0
}

// unwrapCountedGate is the counting gate that also counts the conversion errors.
func unwrapCountedGate(field, conv string) string { return unwrapCountedGateMode(field, conv, true) }

// The flags of the counting gate that are not the clean value: nan and infinity.
const (
	unwrapNaNFlag      = "2"
	unwrapInfinityFlag = "3"
)

// unwrapCountedGateMode is the gate of field for conv that flags each row instead of dropping it: a value whose
// syntax converts is 0, infinity 3 (plain), nan 2 (plain), and with errors any other present value 1. The flagged
// rows pass with the value 0 (the math pipe's default), a value whose conversion overflows is flagged 1 (with
// errors) or dropped, and the value is converted as the plain gate converts it. Without errors a duration or a
// byte size keeps only the values that convert.
func unwrapCountedGateMode(field, conv string, errors bool) string {
	i := 0
	switch conv {
	case "duration":
		i = 2
	case "bytes":
		i = 4
	}
	if errors {
		i++
	}
	unwrapCountedGateOnce.Do(func() {
		for k, c := range []string{"", "", "duration", "duration", "bytes", "bytes"} {
			unwrapCountedGateTemplates[k] = buildUnwrapCountedGate(unwrapGateFieldMark, c, k%2 == 1)
		}
	})
	return strings.ReplaceAll(unwrapCountedGateTemplates[i], strconv.Quote(unwrapGateFieldMark), strconv.Quote(field))
}

// The counting gates are written once per conversion and mode around a marker field, which a request's quoted field
// replaces.
const unwrapGateFieldMark = "§field§"

var (
	unwrapCountedGateOnce      sync.Once
	unwrapCountedGateTemplates [6]string
)

// buildUnwrapCountedGate writes the counting gate of unwrapCountedGateMode.
func buildUnwrapCountedGate(field, conv string, errors bool) string {
	q := strconv.Quote(field)
	type class struct{ flag, cond string }
	var classes []class
	if conv == "" {
		classes = []class{
			{"0", q + ":~" + strconv.Quote(translator.UnwrapPatternWithoutInfinity(""))},
			{unwrapInfinityFlag, q + ":~" + strconv.Quote(translator.UnwrapInfinityPattern)},
			{unwrapNaNFlag, q + ":~" + strconv.Quote(unwrapNaNPattern)},
		}
	} else {
		classes = []class{{"0", q + ":~" + strconv.Quote(translator.UnwrapPattern(conv))}}
	}
	if errors {
		classes = append(classes, class{"1", q + ":*"})
	}
	var b strings.Builder
	var kept []string
	for i, c := range classes {
		b.WriteString(" | format if (")
		for _, prev := range classes[:i] {
			b.WriteString("-" + unwrapBadField + `:="` + prev.flag + `" `)
		}
		b.WriteString(c.cond + `) "` + c.flag + `" as ` + unwrapBadField)
		kept = append(kept, unwrapBadField+`:="`+c.flag+`"`)
	}
	b.WriteString(" | filter " + strings.Join(kept, " or "))
	inRange := translator.UnwrapInRange(conv)
	if conv == "" {
		b.WriteString(translator.UnwrapPlainConvert(field, unwrapBadField+`:="`+unwrapInfinityFlag+`"`))
		b.WriteString(` | format if (` + unwrapBadField + `:="` + unwrapInfinityFlag + `") "0" as ` + unwrapBadField)
	} else {
		_, convert := translator.UnwrapGateParts(field, conv)
		b.WriteString(convert)
	}
	switch {
	case errors:
		b.WriteString(` | format if (` + unwrapBadField + `:="0" -(` + inRange + `)) "1" as ` + unwrapBadField)
	case conv == "":
		b.WriteString(` | filter ` + unwrapBadField + `:="` + unwrapNaNFlag + `" or (` + inRange + `)`)
	default:
		b.WriteString(` | filter ` + inRange)
	}
	b.WriteString(` | math ` + translator.UnwrapValueAlias + ` default 0 as ` + translator.UnwrapValueAlias)
	return b.String()
}

// unwrapHit is a bucket of a stats answer that held a flagged row (or a raw row the conversion rejects): the query
// before the gate, the gate's field and conversion, and the bucket's time span [from, to).
type unwrapHit struct {
	base, field, conv string
	from, to          time.Time
}

// stripUnwrapCounter takes the groups of flagged rows out of a stats answer, and the flag off every other series;
// it returns the bucket times (seconds) of the groups it took out.
func stripUnwrapCounter(body []byte) ([]byte, []float64, error) {
	return stripUnwrapFlags(body, nil)
}

// unwrapNaN is how the nan groups (flag 2) of a query's answer apply: poison when every function of the query is
// NaN over a nan value (the value is NaN where a nan group sits), counts the results that count rows (a nan row is
// a sample: its count adds up). A query that does not poison drops the nan groups.
type unwrapNaN struct {
	poison bool
	counts map[string]bool
}

// unwrapPoisonFuncs are the stats functions whose result is NaN when a value is.
var unwrapPoisonFuncs = map[string]bool{"sum": true, "avg": true, "stddev": true, "stdvar": true}

// unwrapNaNFor reads how the nan groups of the stats query apply: the stats pipes from the first one of the query
// itself (not one of a subquery, lower.SeriesFold's), up to the value of a folded read (lower.SeriesFoldField),
// whose second stats pipe passes the series that keep their hash as they are.
func unwrapNaNFor(query string) *unwrapNaN {
	n := &unwrapNaN{counts: map[string]bool{"__sample_count": true}}
	pipes, ok := unwrapSplitPipes(query[max(topLevelStatsIndex(query), 0):])
	if !ok {
		return n
	}
	poison, last := true, false
	for _, pipe := range pipes {
		if strings.HasSuffix(pipe, " as "+lower.SeriesFoldField) {
			break
		}
		if !strings.HasPrefix(pipe, "stats ") {
			continue
		}
		_, list, ok := splitStatsPipe(pipe)
		if !ok {
			return n
		}
		funcs, _ := statsFunctions(list)
		last = false
		for _, f := range funcs {
			if f.fn == "count" {
				n.counts[f.result] = true
				continue
			}
			last = true
			poison = poison && unwrapPoisonFuncs[f.fn]
		}
	}
	n.poison = poison && last
	return n
}

// unwrapPoint is one point of a series: its time and its value, as written.
type unwrapPoint struct{ ts, value json.RawMessage }

// stripUnwrapFlags takes the counting gate's flag out of a stats answer: the groups of flag 1 are dropped and their
// bucket times returned (seconds, as the answer lists them), the nan groups (flag 2) apply to the answer as nan says
// (nil: dropped), and the clean series lose the flag. An answer without a flagged group keeps its bytes, the flag
// cut out.
func stripUnwrapFlags(body []byte, nan *unwrapNaN) ([]byte, []float64, error) {
	if !unwrapFlagged(body) {
		return dropKeptFlag(body, unwrapKeptFlag), nil, nil
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, nil, err
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(doc["data"], &data); err != nil {
		return nil, nil, err
	}
	var kind string
	_ = json.Unmarshal(data["resultType"], &kind)
	vector := kind == "vector"
	var results []map[string]json.RawMessage
	if err := json.Unmarshal(data["result"], &results); err != nil {
		return nil, nil, err
	}
	pointsKey := "values"
	if vector {
		pointsKey = "value"
	}
	kept := make([]map[string]json.RawMessage, 0, len(results))
	var nanGroups []map[string]json.RawMessage
	var times []float64
	for _, res := range results {
		var metric map[string]string
		if err := json.Unmarshal(res["metric"], &metric); err != nil {
			return nil, nil, err
		}
		flag := metric[unwrapBadField]
		delete(metric, unwrapBadField)
		raw, err := json.Marshal(metric)
		if err != nil {
			return nil, nil, err
		}
		res["metric"] = raw
		switch flag {
		case "1":
			points, err := unwrapPointsOf(res[pointsKey], vector)
			if err != nil {
				return nil, nil, err
			}
			for _, p := range points {
				ts, err := strconv.ParseFloat(string(unwrapUnquote(p.ts)), 64)
				if err != nil {
					return nil, nil, err
				}
				times = append(times, ts)
			}
		case unwrapNaNFlag:
			nanGroups = append(nanGroups, res)
		default:
			kept = append(kept, res)
		}
	}
	if nan != nil && nan.poison {
		for _, res := range nanGroups {
			var metric map[string]string
			_ = json.Unmarshal(res["metric"], &metric)
			points, err := unwrapPointsOf(res[pointsKey], vector)
			if err != nil {
				return nil, nil, err
			}
			if err := nan.apply(&kept, metric, points, vector); err != nil {
				return nil, nil, err
			}
		}
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return nil, nil, err
	}
	data["result"] = encoded
	if doc["data"], err = json.Marshal(data); err != nil {
		return nil, nil, err
	}
	out, err := json.Marshal(doc)
	return out, times, err
}

// unwrapKeptFlag is the flag of the clean rows, as an answer writes it.
var unwrapKeptFlag = []byte(`"` + unwrapBadField + `":"0"`)

// unwrapFlagged reports an answer holding a group of flagged rows (1) or of nan rows (2), in one scan.
func unwrapFlagged(body []byte) bool {
	prefix := []byte(`"` + unwrapBadField + `":"`)
	for i := 0; ; {
		at := bytes.Index(body[i:], prefix)
		if at < 0 {
			return false
		}
		i += at + len(prefix)
		if i < len(body) && (body[i] == '1' || body[i] == unwrapNaNFlag[0]) {
			return true
		}
	}
}

// unwrapPointsOf reads the points of a series (values) or of a vector sample (value).
func unwrapPointsOf(raw json.RawMessage, vector bool) ([]unwrapPoint, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if vector {
		var pair []json.RawMessage
		if err := json.Unmarshal(raw, &pair); err != nil || len(pair) < 2 {
			return nil, err
		}
		return []unwrapPoint{{pair[0], pair[1]}}, nil
	}
	var pairs [][]json.RawMessage
	if err := json.Unmarshal(raw, &pairs); err != nil {
		return nil, err
	}
	out := make([]unwrapPoint, 0, len(pairs))
	for _, pair := range pairs {
		if len(pair) >= 2 {
			out = append(out, unwrapPoint{pair[0], pair[1]})
		}
	}
	return out, nil
}

// dropKeptFlag cuts the clean rows' flag (kept) out of an answer, with the comma that joins it to a neighbour: every
// ",kept", then every "kept,", then every "kept". The answer is rewritten in place (it is the reader's own copy); a
// pass that finds nothing costs one scan.
func dropKeptFlag(body, kept []byte) []byte {
	var with [64]byte
	body = cutAll(body, append(append(with[:0], ','), kept...))
	if !bytes.Contains(body, kept) {
		return body
	}
	body = cutAll(body, append(append(with[:0], kept...), ','))
	return cutAll(body, kept)
}

// cutAll removes every occurrence of pat from b, in place.
func cutAll(b, pat []byte) []byte {
	at := bytes.Index(b, pat)
	if at < 0 {
		return b
	}
	w := at
	for r := at + len(pat); ; {
		next := bytes.Index(b[r:], pat)
		if next < 0 {
			w += copy(b[w:], b[r:])
			return b[:w]
		}
		w += copy(b[w:], b[r:r+next])
		r += next + len(pat)
	}
}

// unwrapSeriesKey is a series' identity: its labels, sorted, quoted.
func unwrapSeriesKey(metric map[string]string) string {
	names := make([]string, 0, len(metric))
	for name := range metric {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		b.WriteString(strconv.Quote(name) + "=" + strconv.Quote(metric[name]) + ",")
	}
	return b.String()
}

// apply adds one nan group (metric without the flag, its points) to the kept series: a count adds the group's
// count at each time, any other result is NaN there; a series the group alone holds is added.
func (n *unwrapNaN) apply(kept *[]map[string]json.RawMessage, metric map[string]string, points []unwrapPoint, vector bool) error {
	key := unwrapSeriesKey(metric)
	count := n.counts[metric["__name__"]]
	pointsKey := "values"
	if vector {
		pointsKey = "value"
	}
	value := func(old json.RawMessage, add json.RawMessage) json.RawMessage {
		if !count {
			return json.RawMessage(`"NaN"`)
		}
		return json.RawMessage(strconv.Quote(strconv.FormatFloat(unwrapCount(old)+unwrapCount(add), 'f', -1, 64)))
	}
	for _, series := range *kept {
		var m map[string]string
		if err := json.Unmarshal(series["metric"], &m); err != nil {
			return err
		}
		if unwrapSeriesKey(m) != key {
			continue
		}
		existing, err := unwrapPointsOf(series[pointsKey], vector)
		if err != nil {
			return err
		}
		for _, p := range points {
			at := -1
			for i, e := range existing {
				if bytes.Equal(unwrapUnquote(e.ts), unwrapUnquote(p.ts)) {
					at = i
					break
				}
			}
			if at >= 0 {
				existing[at].value = value(existing[at].value, p.value)
				continue
			}
			np := unwrapPoint{ts: p.ts, value: value(json.RawMessage(`"0"`), p.value)}
			existing = append(existing, np)
		}
		sort.SliceStable(existing, func(i, j int) bool {
			a, _ := strconv.ParseFloat(string(unwrapUnquote(existing[i].ts)), 64)
			b, _ := strconv.ParseFloat(string(unwrapUnquote(existing[j].ts)), 64)
			return a < b
		})
		encoded, err := encodeUnwrapPoints(existing, vector)
		if err != nil {
			return err
		}
		series[pointsKey] = encoded
		return nil
	}
	fresh := make([]unwrapPoint, len(points))
	for i, p := range points {
		fresh[i] = unwrapPoint{ts: p.ts, value: value(json.RawMessage(`"0"`), p.value)}
	}
	encoded, err := encodeUnwrapPoints(fresh, vector)
	if err != nil {
		return err
	}
	rawMetric, err := json.Marshal(metric)
	if err != nil {
		return err
	}
	*kept = append(*kept, map[string]json.RawMessage{"metric": rawMetric, pointsKey: encoded})
	return nil
}

// unwrapCount reads a count written as a JSON value: a finite number, else 0.
func unwrapCount(raw json.RawMessage) float64 {
	v, err := strconv.ParseFloat(string(unwrapUnquote(raw)), 64)
	if err != nil || !finite(v) {
		return 0
	}
	return v
}

// encodeUnwrapPoints writes points as a series' values, or a vector sample's value.
func encodeUnwrapPoints(points []unwrapPoint, vector bool) (json.RawMessage, error) {
	pairs := make([][]json.RawMessage, len(points))
	for i, p := range points {
		pairs[i] = []json.RawMessage{p.ts, p.value}
	}
	if vector {
		if len(pairs) == 0 {
			return json.RawMessage("[]"), nil
		}
		return json.Marshal(pairs[0])
	}
	return json.Marshal(pairs)
}

// unwrapUnquote is a JSON value as text: a string unquoted, null empty, anything else as written.
func unwrapUnquote(raw json.RawMessage) json.RawMessage {
	s := bytes.TrimSpace(raw)
	if bytes.Equal(s, []byte("null")) {
		return nil
	}
	if len(s) >= 2 && s[0] == '"' {
		var out string
		if err := json.Unmarshal(s, &out); err == nil {
			return json.RawMessage(out)
		}
	}
	return s
}

// parseVLStepParam reads a stats_query_range step: seconds as a number, or a Go duration; 0 when it is neither or
// not positive.
func parseVLStepParam(raw string) time.Duration {
	if v, err := strconv.ParseFloat(raw, 64); err == nil {
		if v > 0 {
			return time.Duration(v * float64(time.Second))
		}
		return 0
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	return 0
}

// unwrapCheckKey carries a request's unwrap conversion check in its context.
type unwrapCheckKey struct{}

// unwrapCheck is a request's unwrap conversion check: its probes, the bounds Loki evaluates, the hits its
// VictoriaLogs reads met, and the writer that holds the answer until the check decides.
type unwrapCheck struct {
	probes       []unwrapErrorProbe
	vlName       func(string) string
	instant      bool
	start, end   time.Time
	step         time.Duration
	mu           sync.Mutex
	hits         []unwrapHit
	rawHits      int
	writer       *unwrapLookupWriter
	heavy        *unwrapHit
	first        *unwrapFirstWindow
	cancel       context.CancelFunc
	firstMinSpan time.Duration
	firstOnce    sync.Once
	// confirmed counts the hits already looked up and lookups the lookups made: a request whose inner evaluations
	// record their status before its last read decides again on the hits that came later, within one budget.
	confirmed, lookups int
	looked             map[unwrapHit]bool
	// query is the request's query as it arrived, the key of the recent errors.
	query string

	// json is the request's JSON parser error detection, nil when the query
	// has nothing to check (json_parser_error_check.go).
	json *jsonErrorCheck
}

// unwrapFirstWindow is the lookup of the first evaluation window: done closes when it answered, err is Loki's
// error found there, nil when none.
type unwrapFirstWindow struct {
	done chan struct{}
	err  *orderedJSONPipelineError
}

// unwrapCheckFrom returns the request's check, nil when the request has none.
func unwrapCheckFrom(ctx context.Context) *unwrapCheck {
	if ctx == nil {
		return nil
	}
	c, _ := ctx.Value(unwrapCheckKey{}).(*unwrapCheck)
	return c
}

// accepts reports whether a probe of the check unwraps field (a VictoriaLogs name) with conv.
func (c *unwrapCheck) accepts(field, conv string) bool {
	return len(c.probesFor(field, conv)) > 0
}

// probesFor returns the probes that unwrap field (a VictoriaLogs name) with conv.
func (c *unwrapCheck) probesFor(field, conv string) []unwrapErrorProbe {
	if c == nil {
		return nil
	}
	var out []unwrapErrorProbe
	for _, probe := range c.probes {
		if probe.conv == conv && slices.Contains(c.fieldNames(probe), field) {
			out = append(out, probe)
		}
	}
	return out
}

// addHit remembers a hit.
func (c *unwrapCheck) addHit(h unwrapHit) {
	c.mu.Lock()
	c.hits = append(c.hits, h)
	c.mu.Unlock()
}

// countingStatsQuery rewrites a stats request of the check's request into its counting form; strip reads the
// answer (the flags out, the hits remembered). ok is false when the request is not rewritten.
func (c *unwrapCheck) countingStatsQuery(path string, params url.Values) (url.Values, func([]byte) ([]byte, error), bool) {
	if c == nil || (path != "/select/logsql/stats_query_range" && path != "/select/logsql/stats_query") {
		return params, nil, false
	}
	query := params.Get("query")
	rewritten, base, field, conv, errors, ok := unwrapCountingRewrite(query, func(field, conv string) (bool, bool) {
		errors := c.accepts(field, conv)
		return errors, errors || conv == ""
	})
	if !ok {
		return params, nil, false
	}
	out := make(url.Values, len(params))
	for k, v := range params {
		out[k] = v
	}
	out.Set("query", rewritten)
	nan := unwrapNaNFor(rewritten)
	span := unwrapVLSpan(params)
	heavy := errors && c.firstMinSpan > 0 && isHeavyBackendRequest(path, params, c.firstMinSpan)
	strip := func(body []byte) ([]byte, error) {
		stripped, times, err := stripUnwrapFlags(body, nan)
		if err != nil {
			return nil, err
		}
		if errors {
			for _, ts := range times {
				from := time.Unix(0, int64(ts*float64(time.Second)))
				h := unwrapHit{base: base, field: field, conv: conv, from: from, to: from.Add(span)}
				if path == "/select/logsql/stats_query" {
					h.from, h.to = unwrapParamTime(params.Get("start")), unwrapParamTime(params.Get("end"))
				}
				c.addHit(h)
				if heavy {
					c.mu.Lock()
					if c.heavy == nil {
						hh := h
						c.heavy = &hh
					}
					c.mu.Unlock()
				}
			}
		}
		return stripped, nil
	}
	return out, strip, true
}

// unwrapParamTime reads a VictoriaLogs time parameter (RFC 3339 or Unix time); zero when it is neither.
func unwrapParamTime(raw string) time.Time {
	if ns, ok := parseFlexibleUnixNanos(raw); ok {
		return time.Unix(0, ns)
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t
	}
	return time.Time{}
}

// rawRowRejected reports a raw row (of the query base, at ts) whose unwrapped value is present and the conversion
// rejects; value reads a field of the row.
func (c *unwrapCheck) rawRowRejected(base string, ts int64, value func(field string) string) {
	if c == nil {
		return
	}
	for _, probe := range c.probes {
		for _, field := range c.fieldNames(probe) {
			v := value(field)
			if v == "" {
				continue
			}
			if _, err := convertUnwrap(v, probe.conv); err == nil {
				break
			}
			// A raw row's hit is its own time: the lookup reads that instant (and a nanosecond on either side).
			c.mu.Lock()
			if c.rawHits < unwrapMaxRawHits {
				c.rawHits++
				at := time.Unix(0, ts)
				c.hits = append(c.hits, unwrapHit{base: base, field: field, conv: probe.conv, from: at, to: at.Add(1)})
			}
			c.mu.Unlock()
			return
		}
	}
}

// fieldNames are the VictoriaLogs names of the probe's label.
func (c *unwrapCheck) fieldNames(probe unwrapErrorProbe) []string {
	name := probe.label
	if c != nil && c.vlName != nil {
		if v := c.vlName(name); v != "" && v != name {
			return []string{v, name}
		}
	}
	return []string{name}
}

// Bounds of the check: the raw rows reported, and the lookups after the read.
const (
	unwrapMaxRawHits = 64
	unwrapMaxLookups = 4
)

// unwrapVLSpan is the span of a stats request's bucket: the step of stats_query_range, the whole range of
// stats_query.
func unwrapVLSpan(params url.Values) time.Duration {
	if step := parseVLStepParam(params.Get("step")); step > 0 {
		return step
	}
	from, to := unwrapParamTime(params.Get("start")), unwrapParamTime(params.Get("end"))
	if !from.IsZero() && !to.IsZero() && to.After(from) {
		return to.Sub(from)
	}
	return time.Nanosecond
}

// startFirstWindow starts the lookup of the first evaluation window for the hit's field and conversion, once per
// request; early runs it while the metric's read is still on its way, and cancels that read when it finds the
// error.
func (c *unwrapCheck) startFirstWindow(h unwrapHit, early bool) *unwrapFirstWindow {
	c.firstOnce.Do(func() {
		fw := &unwrapFirstWindow{done: make(chan struct{})}
		c.mu.Lock()
		c.first = fw
		c.mu.Unlock()
		writer := c.writer
		go func() {
			if writer != nil {
				for _, probe := range c.probesFor(h.field, h.conv) {
					from, _, _ := c.windows(probe)
					firstEnd := c.start.Add(-probe.offset)
					if err := c.lookupHit(writer.ctx, writer.p, probe, h, from, firstEnd); err != nil {
						fw.err = err
						break
					}
				}
			}
			close(fw.done)
			if fw.err != nil && early && c.cancel != nil {
				// Loki stops at the first rejected value: the metric's read is not needed any more.
				c.cancel()
			}
		}()
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.first
}

// recentKey names the request's query, tenant and probed field: a recent conversion error of the same key starts
// the next heavy read's first-window lookup early.
func (c *unwrapCheck) recentKey(ctx context.Context, p *Proxy, h unwrapHit) string {
	return p.contextScopeFingerprint(ctx) + "\x00" + c.query + "\x00" + h.field + "\x00" + h.conv
}

// How long, and how many, recent conversion errors are remembered.
const (
	unwrapRecentTTL = 10 * time.Minute
	unwrapRecentMax = 1024
)

// unwrapRecent is a bounded set of keys with the time each was marked. A Proxy holds its own (the queries that met a
// conversion error lately, per tenant); the zero value is ready to use.
type unwrapRecent struct {
	mu sync.Mutex
	at map[string]time.Time
}

// seen reports a key marked within unwrapRecentTTL.
func (r *unwrapRecent) seen(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.at[key]
	if ok && time.Since(t) > unwrapRecentTTL {
		delete(r.at, key)
		return false
	}
	return ok
}

// mark remembers a key, dropping expired keys (and, when still full, the oldest) to stay bounded.
func (r *unwrapRecent) mark(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.at == nil {
		r.at = map[string]time.Time{}
	}
	now := time.Now()
	if _, ok := r.at[key]; !ok && len(r.at) >= unwrapRecentMax {
		oldest, oldestAt := "", now
		for k, t := range r.at {
			if now.Sub(t) > unwrapRecentTTL {
				delete(r.at, k)
				continue
			}
			if t.Before(oldestAt) {
				oldest, oldestAt = k, t
			}
		}
		if len(r.at) >= unwrapRecentMax {
			delete(r.at, oldest)
		}
	}
	r.at[key] = now
}

// confirmUnwrap decides the check: Loki's error when a line of the read's hits fails Loki's conversion, nil
// otherwise (the JSON parser error is decided after it, jsonErrorCheck.confirm).
func (c *unwrapCheck) confirmUnwrap(ctx context.Context, p *Proxy) *orderedJSONPipelineError {
	if c == nil || len(c.probes) == 0 {
		return nil
	}
	c.mu.Lock()
	fw := c.first
	heavy := c.heavy
	hits := append([]unwrapHit(nil), c.hits[c.confirmed:]...)
	c.confirmed = len(c.hits)
	c.mu.Unlock()
	if fw == nil && heavy != nil {
		fw = c.startFirstWindow(*heavy, false)
	}
	if fw != nil {
		// The lookup reads with the request's context and ends with it.
		<-fw.done
		if fw.err != nil {
			return fw.err
		}
	}
	if len(hits) == 0 {
		return nil
	}
	return c.confirmHits(ctx, p, hits)
}

// confirmHits looks the hits up, earliest first, at most unwrapMaxLookups of them; the first line that fails
// Loki's conversion decides.
func (c *unwrapCheck) confirmHits(ctx context.Context, p *Proxy, hits []unwrapHit) *orderedJSONPipelineError {
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].from.Before(hits[j].from) })
	for _, h := range hits {
		if c.looked[h] {
			continue // the same rows read by an earlier operand, or twice by one
		}
		if c.looked == nil {
			c.looked = map[unwrapHit]bool{}
		}
		c.looked[h] = true
		looked := map[[2]time.Duration]bool{}
		for _, probe := range c.probesFor(h.field, h.conv) {
			if looked[[2]time.Duration{probe.window, probe.offset}] {
				continue // an identical aggregation (both operands of a binary operation) reads the same rows
			}
			looked[[2]time.Duration{probe.window, probe.offset}] = true
			if c.lookups >= unwrapMaxLookups {
				return nil
			}
			from, to, _ := c.windows(probe)
			span := h.to.Sub(h.from)
			lo, hi := h.from.Add(-span), h.to.Add(span)
			if lo.Before(from) {
				lo = from
			}
			if hi.After(to) {
				hi = to
			}
			if hi.Before(lo) {
				continue
			}
			c.lookups++
			if err := c.lookupHit(ctx, p, probe, h, lo, hi); err != nil {
				return err
			}
		}
	}
	return nil
}

// lookupHit reads the flagged rows of the hit's query between from and to (inclusive) and returns Loki's error
// for the first row Loki fails on.
func (c *unwrapCheck) lookupHit(ctx context.Context, p *Proxy, probe unwrapErrorProbe, h unwrapHit, from, to time.Time) *orderedJSONPipelineError {
	rows, err := p.unwrapLookupRows(ctx, unwrapLookupQuery(h.base, h.field, h.conv)+" | limit 3", from, to)
	if err != nil {
		return nil
	}
	_, _, last := c.windows(probe)
	var visible []map[string]string
	for _, row := range rows {
		ts, err := time.Parse(time.RFC3339Nano, row["_time"])
		if err != nil || !c.visible(ts.UnixNano(), probe, last) {
			continue
		}
		visible = append(visible, row)
	}
	if len(visible) == 0 {
		return nil
	}
	stored := make([]map[string]string, len(visible))
	var wg sync.WaitGroup
	for i, row := range visible {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stored[i], _ = p.unwrapStoredRow(ctx, probe, row)
		}()
	}
	wg.Wait()
	for _, row := range stored {
		if row == nil {
			continue
		}
		if err := p.unwrapRowError(ctx, probe, row, h.field); err != nil {
			return err
		}
	}
	return nil
}

// windows are the bounds of the lines Loki reads for probe, in the stored time (the offset applied): from is
// exclusive, to inclusive; last is the last evaluation time.
func (c *unwrapCheck) windows(probe unwrapErrorProbe) (from, to, last time.Time) {
	last = c.end
	if !c.instant && c.step > 0 && c.end.After(c.start) {
		last = c.start.Add(c.end.Sub(c.start) / c.step * c.step)
	}
	if c.instant {
		last = c.start
	}
	from = c.start.Add(-probe.offset - probe.window).Add(1)
	to = last.Add(-probe.offset)
	return from, to, last
}

// visible reports whether a line at ts (stored time) is inside a window (t-range, t] of an evaluation time t.
func (c *unwrapCheck) visible(ts int64, probe unwrapErrorProbe, last time.Time) bool {
	ts += int64(probe.offset)
	window := int64(probe.window)
	start := c.start.UnixNano()
	if c.instant || c.step <= 0 {
		return ts > start-window && ts <= start
	}
	if ts > last.UnixNano() || ts <= start-window {
		return false
	}
	step := int64(c.step)
	// The first evaluation time at or after ts.
	k := (ts - start + step - 1) / step
	if ts <= start {
		k = 0
	}
	t := start + k*step
	return t <= last.UnixNano() && ts > t-window
}

// unwrapLookupRows reads the rows of query between from and to (both inclusive: VictoriaLogs' end is exclusive),
// each as its fields' text.
func (p *Proxy) unwrapLookupRows(ctx context.Context, query string, from, to time.Time) ([]map[string]string, error) {
	params := url.Values{
		"query": {query},
		"start": {from.UTC().Format(time.RFC3339Nano)},
		"end":   {to.Add(1).UTC().Format(time.RFC3339Nano)},
	}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return nil, &upstreamStatusError{status: resp.StatusCode}
	}
	var rows []map[string]string
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		row := map[string]string{}
		if err := jsonparser.ObjectEach(line, func(key, value []byte, kind jsonparser.ValueType, _ int) error {
			row[string(key)] = orderedJSONScalarText(value, kind)
			return nil
		}); err != nil {
			continue
		}
		rows = append(rows, row)
	}
	return rows, sc.Err()
}

// orderedJSONScalarText is a JSON value's text: a string unescaped, anything else as written.
func orderedJSONScalarText(value []byte, kind jsonparser.ValueType) string {
	if kind == jsonparser.String {
		if s, err := jsonparser.ParseString(value); err == nil {
			return s
		}
	}
	return string(value)
}

// unwrapFormattedField carries the lookup's line (after the query's line_format stages) with the stored row.
const unwrapFormattedField = "__lvp_formatted"

// unwrapStoredRow reads a looked-up row again as it is stored (by its stream, time and line), without the fields
// the query's parsers wrote over it.
func (p *Proxy) unwrapStoredRow(ctx context.Context, probe unwrapErrorProbe, row map[string]string) (map[string]string, error) {
	id, msg := row["_stream_id"], row["_msg"]
	ts, err := time.Parse(time.RFC3339Nano, row["_time"])
	if id == "" || err != nil {
		return nil, err
	}
	rows, err := p.unwrapLookupRows(ctx, "_stream_id:"+strconv.Quote(id)+" _msg:="+strconv.Quote(msg)+" | limit "+strconv.Itoa(unwrapMaxRawHits), ts, ts)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	// The row of the same time and line; VictoriaLogs' filter already holds the line, the time picks among equal
	// lines of the stream.
	stored := rows[0]
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i]["_msg"] == msg && (rows[i]["_time"] == row["_time"] || stored["_msg"] != msg) {
			stored = rows[i]
		}
	}
	// The lookup's line is the one the query's line_format stages wrote.
	stored[unwrapFormattedField] = msg
	return stored, nil
}

// unwrapRowError is Loki's error for a stored row, nil when Loki does not fail on it. The row's labels are the
// ones the proxy answers for the stream (service_name and the level as every route derives them) and the row's
// stored fields as structured metadata.
func (p *Proxy) unwrapRowError(ctx context.Context, probe unwrapErrorProbe, row map[string]string, field string) *orderedJSONPipelineError {
	stored := cloneStringMap(row)
	delete(stored, unwrapFormattedField)
	dropIngestUnpackedFields(stored)
	desc := p.logQueryStreamDescriptor(row["_stream"], row["level"], map[string]map[string]string{}, map[string]cachedLogQueryStreamDescriptor{})
	base, err := p.orderedJSONBaseLabels(stored, desc, map[string][]metadataFieldExposure{})
	if err != nil {
		return nil
	}
	formatted := row[unwrapFormattedField]
	if formatted == "" {
		formatted = row["_msg"]
	}
	return unwrapLokiError(probe, base, row["_msg"], formatted)
}

// dropIngestUnpackedFields drops the fields VictoriaLogs unpacked from a JSON line at ingest (each top-level scalar
// of the line, stored under its key with its value): they are the line's, not structured metadata.
func dropIngestUnpackedFields(stored map[string]string) {
	line := stored["_msg"]
	if len(line) < 2 || line[0] != '{' {
		return
	}
	_ = jsonparser.ObjectEach([]byte(line), func(key, value []byte, kind jsonparser.ValueType, _ int) error {
		name := string(key)
		if strings.HasPrefix(name, "_") {
			return nil
		}
		if v, ok := stored[name]; ok && kind != jsonparser.Object && kind != jsonparser.Array && v == orderedJSONScalarText(value, kind) {
			delete(stored, name)
		}
		return nil
	})
}

// unwrapLokiError is Loki's pipeline error for the line (with the stored labels base) under probe, nil when Loki
// makes a sample of it, makes none, or the pipeline is not one the check reproduces.
func unwrapLokiError(probe unwrapErrorProbe, base map[string]string, line, formatted string) *orderedJSONPipelineError {
	labels, parsed, ok := unwrapLokiLabels(probe, base, line, formatted)
	if !ok || labels == nil || labels["__preserve_error__"] != "" {
		return nil
	}
	value := labels[probe.label]
	if value == "" {
		return nil
	}
	_, err := convertUnwrap(value, probe.conv)
	if err == nil {
		return nil
	}
	out := make(map[string]string, len(labels))
	for name, v := range labels {
		if parsed[name] && probe.hints != nil && !probe.hints[name] {
			continue
		}
		out[name] = v
	}
	out[errorLabel] = "SampleExtractionErr"
	out[errorDetailsLabel] = ""
	for _, st := range probe.post {
		lf, _ := st.(*logqlpkg.LabelFilterStage)
		if keep, known := unwrapLabelFilterKeeps(lf, out); !known || !keep {
			return nil
		}
	}
	out[errorDetailsLabel] = err.Error()
	return lokiPipelineError(out)
}

// unwrapLokiLabels runs the probe's stages before the unwrap over the line, as Loki does, from the stored labels
// base: the labels, the names the parsers extracted, and ok false when a stage is not one the check reproduces.
// formatted is the line after the query's line_format stages (VictoriaLogs' answer to the lookup), which the
// parsers after a line_format read. labels is nil when a filter drops the line.
func unwrapLokiLabels(probe unwrapErrorProbe, base map[string]string, line, formatted string) (map[string]string, map[string]bool, bool) {
	labels := cloneStringMap(base)
	parsed := map[string]bool{}
	preserve := probe.hints[errorLabel]
	text := line
	for _, st := range probe.pipeline {
		switch s := st.(type) {
		case *logqlpkg.ParserStage:
			if !unwrapParseLine(s, text, labels, parsed, preserve) {
				return nil, nil, false
			}
		case *logqlpkg.LineFormatStage:
			text = formatted
		case *logqlpkg.LabelFormatStage:
			if !unwrapLabelFormat(s, labels, probe.label) {
				return nil, nil, false
			}
		case *logqlpkg.LabelFilterStage:
			keep, known := unwrapLabelFilterKeeps(s, labels)
			if !known {
				return nil, nil, false
			}
			if !keep {
				return nil, parsed, true
			}
		case *logqlpkg.LineFilterStage:
			// VictoriaLogs ran the line filters on the line the lookup read.
		case *logqlpkg.DropStage, *logqlpkg.KeepStage:
			compiled, ok := compileOrderedJSONStage(st)
			if !ok {
				return nil, nil, false
			}
			applyOrderedJSONFields(compiled, labels)
		default:
			return nil, nil, false
		}
	}
	return labels, parsed, true
}

// unwrapLabelFormatRefRE is a template that is one label reference ({{.name}}).
var unwrapLabelFormatRefRE = regexp.MustCompile(`^\{\{-?\s*\.([A-Za-z_][A-Za-z0-9_]*)\s*-?\}\}$`)

// unwrapLabelFormat applies a label_format stage whose formats rename a label or copy one label's value
// ({{.name}}), as Loki does (recorded: TestUnwrapConversionErrorMatchesRecordedLoki): the formats read the labels
// as they were before the stage; a label another template writes is left out of the error's series. A stage that
// writes the unwrapped label is not reproduced (ok false).
func unwrapLabelFormat(stage *logqlpkg.LabelFormatStage, labels map[string]string, unwrapped string) bool {
	before := cloneStringMap(labels)
	for _, f := range stage.Formats {
		if f.Name == unwrapped {
			// The unwrapped label written by the stage: not reproduced.
			return false
		}
		source := f.Value
		if !f.Rename {
			m := unwrapLabelFormatRefRE.FindStringSubmatch(f.Value)
			if m == nil {
				delete(labels, f.Name)
				continue
			}
			source = m[1]
		}
		if v := before[source]; v != "" {
			labels[f.Name] = v
		} else {
			delete(labels, f.Name)
		}
		if f.Rename && source != f.Name {
			delete(labels, source)
		}
	}
	return true
}

// unwrapLabelFilterKeeps evaluates a label filter stage over labels as Loki does: a string or ip() comparison, or a
// comparison with a number, a duration or a byte size (a label missing is dropped, one that does not convert kept,
// with Loki's error). known is false for any other filter.
func unwrapLabelFilterKeeps(stage *logqlpkg.LabelFilterStage, labels map[string]string) (keep, known bool) {
	if stage == nil {
		return false, false
	}
	if compiled, valid := compileOrderedJSONStage(stage); valid && compiled.filter != nil {
		return compiled.filter.Matches(labels[compiled.filter.Field]), true
	}
	label, cmp, literal, ok := unwrapComparison(stage)
	if !ok {
		return false, false
	}
	conv, ok := unwrapComparisonConv(literal)
	if !ok {
		return false, false
	}
	bound, _ := convertUnwrap(literal, conv)
	raw, present := labels[label]
	if !present {
		return false, true
	}
	value, err := convertUnwrap(raw, conv)
	if err != nil {
		return true, true
	}
	switch cmp {
	case ">":
		return value > bound, true
	case ">=":
		return value >= bound, true
	case "<":
		return value < bound, true
	case "<=":
		return value <= bound, true
	case "!=":
		return value != bound, true
	}
	return value == bound, true
}

// unwrapComparisonConv is the conversion a comparison literal is read with: a number, a duration, a byte size.
func unwrapComparisonConv(literal string) (string, bool) {
	if _, err := strconv.ParseFloat(literal, 64); err == nil {
		return "", true
	}
	if _, err := time.ParseDuration(literal); err == nil {
		return "duration", true
	}
	if _, err := parseHumanBytes(literal); err == nil {
		return "bytes", true
	}
	return "", false
}

// unwrapParseLine runs one parser stage over the line into labels, as Loki's parser does (recorded:
// TestUnwrapParsersMatchRecordedLoki): a name the stage extracts that a stored label already holds gets the
// _extracted suffix, a name an earlier stage extracted is kept, and the stage's own first non-empty value of a
// name wins (json, logfmt) or its last (unpack). ok is false for a stage the check does not reproduce.
func unwrapParseLine(stage *logqlpkg.ParserStage, line string, labels map[string]string, parsed map[string]bool, preserveError bool) bool {
	add := func(name, value string, last bool) {
		if name == "" || name == errorLabel || name == errorDetailsLabel {
			return
		}
		if _, stored := labels[name]; stored && !parsed[name] {
			name += extractedSuffix
		}
		if parsed[name] && !last {
			return
		}
		labels[name], parsed[name] = value, true
	}
	switch stage.Type {
	case logqlpkg.ParserJSON:
		if stage.Param != "" || len(stage.Fields) > 0 {
			return false
		}
		own := map[string]bool{}
		err := unwrapWalkJSON([]byte(line), "", 0, func(name, value string) {
			if own[name] {
				return
			}
			own[name] = true
			add(name, value, false)
		})
		if err != nil {
			labels[errorLabel] = "JSONParserErr"
			labels[errorDetailsLabel] = err.Error()
			if preserveError {
				labels["__preserve_error__"] = "true"
			}
		}
		return true
	case logqlpkg.ParserLogfmt:
		if stage.Param != "" || len(stage.Fields) > 0 {
			return false
		}
		own := map[string]bool{}
		lokiLogfmtPairs(line, func(key, value string) {
			name := orderedJSONLabelName(key, true)
			if value == "" || own[name] {
				return
			}
			own[name] = true
			add(name, lokiLabelValue(value), false)
		})
		return true
	case logqlpkg.ParserUnpack:
		if !strings.Contains(line, `"_entry"`) {
			return true
		}
		_ = jsonparser.ObjectEach([]byte(line), func(key, value []byte, kind jsonparser.ValueType, _ int) error {
			if kind != jsonparser.String || string(key) == "_entry" {
				return nil
			}
			add(orderedJSONLabelName(string(key), true), orderedJSONScalar(value, kind), true)
			return nil
		})
		return true
	case logqlpkg.ParserRegexp:
		re, err := regexp.Compile(unwrapRegexpParam(stage.Param))
		if err != nil {
			return false
		}
		m := re.FindStringSubmatch(line)
		for i, name := range re.SubexpNames() {
			if name == "" || i >= len(m) || m == nil {
				continue
			}
			add(name, m[i], false)
		}
		return true
	}
	return false
}

// unwrapRegexpParam is a regexp parser's expression: its string literal unquoted.
func unwrapRegexpParam(param string) string {
	param = strings.TrimSpace(param)
	if strings.HasPrefix(param, "`") && strings.HasSuffix(param, "`") && len(param) >= 2 {
		return param[1 : len(param)-1]
	}
	if s, err := strconv.Unquote(param); err == nil {
		return s
	}
	return param
}

// unwrapWalkJSON calls yield with every scalar of a JSON object line (strings unescaped, numbers and booleans as
// written), nested objects flattened with "_" between the sanitized parts; arrays and null yield nothing.
func unwrapWalkJSON(data []byte, prefix string, depth int, yield func(name, value string)) error {
	if depth > 64 {
		return errOrderedJSONComplexity
	}
	return jsonparser.ObjectEach(data, func(key, value []byte, kind jsonparser.ValueType, _ int) error {
		name := orderedJSONLabelName(string(key), prefix == "")
		if prefix != "" {
			name = prefix + "_" + name
		}
		switch kind {
		case jsonparser.Object:
			return unwrapWalkJSON(value, name, depth+1, yield)
		case jsonparser.String, jsonparser.Number, jsonparser.Boolean:
			yield(name, orderedJSONScalar(value, kind))
		}
		return nil
	})
}

// unwrapLookupWriter holds a checked request's answer until the check decides: the handler's answer, or Loki's
// error in its place.
type unwrapLookupWriter struct {
	p        *Proxy
	ctx      context.Context
	parent   context.Context
	check    *unwrapCheck
	out      http.ResponseWriter
	header   http.Header
	status   int
	query    string
	passed   bool
	err      *orderedJSONPipelineError
	recorded bool
	mu       sync.Mutex
	// backendErr is a JSON parser error lookup VictoriaLogs did not answer:
	// the request answers it as a failed backend (jsonErrorCheck.failure);
	// backendStatus is the status the error layer gave it.
	backendErr    error
	backendStatus int
}

// startUnwrapConversionCheck gives a metric request with an unwrap its check: the request to serve (carrying the
// check) and, when a conversion error can fail the query, the writer the handler writes to. A heavy range read of
// a query that met a conversion error lately looks its first window up at once.
func (p *Proxy) startUnwrapConversionCheck(w http.ResponseWriter, r *http.Request, logql string, isRange bool) (*http.Request, *unwrapLookupWriter) {
	if !strings.Contains(logql, "unwrap") || unwrapCheckFrom(r.Context()) != nil {
		// An inner request (an operand, a fold's evaluation) reports to the check of the request it serves.
		return r, nil
	}
	expr, err := parseLogQLExpr(r.Context(), logql)
	if err != nil || !unwrapExprHasUnwrap(expr) {
		return r, nil
	}
	check := &unwrapCheck{probes: unwrapErrorProbes(expr), instant: !isRange, query: logql}
	if p.labelTranslator != nil {
		check.vlName = p.labelTranslator.ToVL
	}
	if isRange {
		if ns, ok := parseLokiTimeToUnixNano(r.FormValue("start")); ok {
			check.start = time.Unix(0, ns)
		}
		if ns, ok := parseLokiTimeToUnixNano(r.FormValue("end")); ok {
			check.end = time.Unix(0, ns)
		}
		check.step, _ = parsePositiveStepDuration(r.FormValue("step"))
		if _, binary := expr.(*logqlpkg.BinOpExpr); binary && check.step > 0 {
			// Binary operands are evaluated on the step-aligned axis (alignBinaryRangeRequest), as the JSON parser
			// error check reads them (startJSONParserErrorCheck).
			check.start = check.start.Add(-time.Duration(check.start.UnixNano() % int64(check.step)))
			check.end = check.end.Add(-time.Duration(check.end.UnixNano() % int64(check.step)))
		}
	} else {
		at := time.Now()
		if ns, ok := parseLokiTimeToUnixNano(r.FormValue("time")); ok {
			at = time.Unix(0, ns)
		}
		check.start, check.end = at, at
	}
	if len(check.probes) == 0 {
		return r.WithContext(context.WithValue(r.Context(), unwrapCheckKey{}, check)), nil
	}
	check.firstMinSpan = p.backendHeavyQueryMinRange
	ctx, cancel := context.WithCancel(r.Context())
	check.cancel = cancel
	ctx = context.WithValue(ctx, unwrapCheckKey{}, check)
	writer := &unwrapLookupWriter{p: p, ctx: ctx, parent: r.Context(), check: check, out: w, header: make(http.Header), query: logql}
	check.writer = writer
	if isRange && check.firstMinSpan > 0 {
		probe := check.probes[0]
		h := unwrapHit{field: check.fieldNames(probe)[0], conv: probe.conv}
		if check.end.Sub(check.start)+probe.window >= check.firstMinSpan && p.unwrapRecentErrors.seen(check.recentKey(ctx, p, h)) {
			if base, ok := p.unwrapFirstWindowBase(ctx, logql, h.field, h.conv); ok {
				h.base = base
				check.startFirstWindow(h, true)
			}
		}
	}
	return r.WithContext(ctx), writer
}

// unwrapExprHasUnwrap reports a range aggregation over an unwrapped label in expr.
func unwrapExprHasUnwrap(expr logqlpkg.Expr) bool {
	found := false
	var walk func(logqlpkg.Expr)
	walk = func(e logqlpkg.Expr) {
		switch n := e.(type) {
		case *logqlpkg.RangeAggregation:
			if lq, ok := n.Inner.(*logqlpkg.LogQuery); ok {
				for _, st := range lq.Pipeline {
					if _, ok := st.(*logqlpkg.UnwrapStage); ok {
						found = true
					}
				}
			}
		case *logqlpkg.VectorAggregation:
			walk(n.Inner)
		case *logqlpkg.BinOpExpr:
			walk(n.Left)
			walk(n.Right)
		case *logqlpkg.OpaqueMetricExpr:
			if n.Inner != nil {
				walk(n.Inner)
			}
		}
	}
	walk(expr)
	return found
}

// unwrapFirstWindowBase is the translated query before the unwrap gate of field and conv, for an early
// first-window lookup.
func (p *Proxy) unwrapFirstWindowBase(ctx context.Context, logql, field, conv string) (string, bool) {
	text, err := p.translateQueryWithContext(ctx, logql)
	if err != nil {
		return "", false
	}
	clean, _ := translator.SplitPost(text)
	_, base, _, _, _, ok := unwrapCountingRewrite(clean, func(f, c string) (bool, bool) { return true, f == field && c == conv })
	return base, ok
}

// setQuery records the query the handler serves (after a rewrite of its text).
func (l *unwrapLookupWriter) setQuery(logql string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.query = logql
	l.mu.Unlock()
}

// decide runs the check on the hits not looked up yet, as long as nothing of the answer went out: before the first
// byte, when the route records its status, and when the route asks whether the answer failed. Loki's error is written
// at once and ends the decision.
func (l *unwrapLookupWriter) decide() {
	if l.err != nil || l.backendErr != nil || l.passed {
		return
	}
	l.err = l.check.confirmUnwrap(l.ctx, l.p)
	unwrapFailed := l.err != nil
	if l.err == nil && l.check.json != nil {
		l.err = l.check.json.confirm(l.ctx, l.p, l.check)
	}
	if l.err == nil {
		if l.backendErr = l.check.json.backendFailure(); l.backendErr != nil {
			// A JSON parser error lookup VictoriaLogs did not answer: Loki's failed-backend answer, through the
			// error layer, with the client's context.
			l.backendStatus = l.p.writeErr(l.out, l.parent, l.backendErr)
			if l.check.cancel != nil {
				l.check.cancel()
			}
		}
		return
	}
	if l.err != nil {
		// Only a conversion error starts a later request's early first-window lookup (a JSON parser error is not
		// what that lookup finds).
		if key := l.recentHitKey(); unwrapFailed && key != "" {
			l.p.unwrapRecentErrors.mark(key)
		}
		// Written with the client's context; the request's own reads (a later operand's) stop: the answer is sent.
		l.p.writeErr(l.out, l.parent, l.err)
		if l.check.cancel != nil {
			l.check.cancel()
		}
	}
}

// pass sends the handler's header and status once, when its answer goes out.
func (l *unwrapLookupWriter) pass() {
	if l.passed {
		return
	}
	l.passed = true
	dst := l.out.Header()
	for k, v := range l.header {
		dst[k] = v
	}
	if l.status != 0 {
		l.out.WriteHeader(l.status)
	}
}

// recentHitKey is the recent-error key of the check's first probe.
func (l *unwrapLookupWriter) recentHitKey() string {
	if len(l.check.probes) == 0 {
		return ""
	}
	probe := l.check.probes[0]
	return l.check.recentKey(l.ctx, l.p, unwrapHit{field: l.check.fieldNames(probe)[0], conv: probe.conv})
}

// Header is the handler's header, copied out when its answer goes out.
func (l *unwrapLookupWriter) Header() http.Header { return l.header }

// WriteHeader decides the check, then writes the handler's status unless Loki's error went out instead.
func (l *unwrapLookupWriter) WriteHeader(code int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.passed || l.err != nil || l.backendErr != nil {
		return
	}
	l.status = code
	l.decide()
	if l.err == nil && l.backendErr == nil {
		l.pass()
	}
}

// Write decides the check first; the handler's bytes go out only when no error did.
func (l *unwrapLookupWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.decide()
	if l.err != nil || l.backendErr != nil {
		return len(b), nil
	}
	if !l.passed {
		// The handler's header and status go out with its first bytes; an implicit 200 takes the defaults every
		// answer writer applies.
		l.passed = true
		dst := l.out.Header()
		for k, v := range l.header {
			dst[k] = v
		}
		if l.status == 0 {
			l.status = http.StatusOK
			if dst.Get("Content-Type") == "" {
				dst.Set("Content-Type", "application/json")
			}
			dst.Set("X-Content-Type-Options", "nosniff")
		}
		l.out.WriteHeader(l.status)
	}
	return l.out.Write(b)
}

// Flush flushes the handler's answer.
func (l *unwrapLookupWriter) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.passed {
		if f, ok := l.out.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// unwrapAnswerFailed reports whether the answer a route is about to keep is replaced by Loki's error: the route's own
// check, or, for an inner request (an operand), the check of the request it serves.
func unwrapAnswerFailed(ctx context.Context, l *unwrapLookupWriter) bool {
	if l == nil {
		if c := unwrapCheckFrom(ctx); c != nil {
			l = c.writer
		}
	}
	return l.failed()
}

// failed reports an answer replaced by Loki's error; it decides on the hits so far first, so a route that keeps
// its answer (the response cache) asks after its reads.
func (l *unwrapLookupWriter) failed() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.decide()
	return l.err != nil || l.backendErr != nil
}

// unwrapAnswerUncertain reports whether the answer a route is about to keep holds JSON parser error marks that were
// not all confirmed, for the route's own check or, for an inner request (an operand), the check of the request it
// serves (as unwrapAnswerFailed).
func unwrapAnswerUncertain(ctx context.Context, l *unwrapLookupWriter) bool {
	if l == nil {
		if c := unwrapCheckFrom(ctx); c != nil {
			l = c.writer
		}
	}
	return l.uncertain()
}

// uncertain reports an answer whose JSON parser error marks were not all
// confirmed: it is sent, and not kept in the response cache.
func (l *unwrapLookupWriter) uncertain() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.decide()
	return l.check.json.uncertain()
}

// unwrapRecordedStatus is the status a route records for its request: Loki's 400 when the check answers the
// error (it decides now when the answer has not gone out yet), the handler's otherwise.
func unwrapRecordedStatus(ctx context.Context, code int) int {
	c := unwrapCheckFrom(ctx)
	if c == nil || c.writer == nil {
		return code
	}
	l := c.writer
	l.mu.Lock()
	defer l.mu.Unlock()
	l.decide()
	l.recorded = true
	if l.err != nil {
		return http.StatusBadRequest
	}
	if l.backendErr != nil {
		return l.backendStatus // a lookup VictoriaLogs did not answer
	}
	return code
}

// finish decides a check the handler never wrote for, and records the request when Loki's error went out and the
// handler did not record it.
func (l *unwrapLookupWriter) finish(endpoint, logql string, requestStart time.Time) {
	l.mu.Lock()
	l.decide()
	failed, recorded := l.err != nil || l.backendErr != nil, l.recorded
	status := http.StatusBadRequest
	if l.err == nil && l.backendErr != nil {
		status = l.backendStatus
	}
	l.mu.Unlock()
	if l.check.cancel != nil {
		l.check.cancel()
	}
	if failed && !recorded {
		elapsed := time.Since(requestStart)
		l.p.metrics.RecordRequest(endpoint, status, elapsed)
		l.p.queryTracker.Record(endpoint, logql, elapsed, true)
	}
}

// unwrapStripResponse replaces a successful VictoriaLogs answer's body with what strip makes of it.
func (p *Proxy) unwrapStripResponse(resp *http.Response, strip func([]byte) ([]byte, error)) error {
	if resp == nil || resp.StatusCode < 200 || resp.StatusCode >= 300 || resp.Body == nil {
		return nil
	}
	limit := p.decodedBodyLimit()
	// Read in one allocation when the length is known (a folded answer, or one VictoriaLogs sends with its length);
	// a body of unknown length (a chunked answer) grows from a small buffer, not readBodyLimited's 128 KiB.
	var body []byte
	var err error
	if n := resp.ContentLength; n >= 0 && n <= limit {
		body, err = readBodyKnownSize(resp.Body, n, limit)
	} else if body, err = io.ReadAll(io.LimitReader(resp.Body, limit+1)); err == nil && int64(len(body)) > limit {
		err = errBodyTooLarge
	}
	_ = resp.Body.Close()
	if errors.Is(err, errBodyTooLarge) {
		return newBackendBodyTooLarge(limit)
	}
	if err != nil {
		return err
	}
	out, err := strip(body)
	if err != nil {
		return err
	}
	resp.Body = io.NopCloser(bytes.NewReader(out))
	resp.ContentLength = int64(len(out))
	resp.Header.Set("Content-Length", strconv.Itoa(len(out)))
	return nil
}
