package lower

import (
	"slices"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// The range aggregations over an unwrapped label: sum, avg, min, max, stddev, first, last, quantile and rate_counter
// over a log query, and rate over a label (the sum of its values per second). The string translator writes the
// metric row's log query (metricLogQuery; the unwrap stage is no pipe), then the unwrap gate
// (translate.UnwrapGateFor: the rows whose value converts as Loki converts it, the value in translate.UnwrapValueAlias),
// then the stats pipes, so VictoriaLogs aggregates per step bucket and the proxy never buffers rows:
//
//   - a function alone aggregates the rows of its group: no grouping, or the labels of its by clause (`by ()`
//     groups nothing, written out);
//   - first and last read one value per series, since VictoriaLogs has no such function: the row of the earliest
//     (row_min) or the latest (row_max) time is kept per series, its value unpacked, and the largest per group is
//     the answer; a function without a clause groups by the stream and its level;
//   - under a vector aggregation the series identity is the stream (the stream and its level for a rate): sum
//     aggregates the rows, count, min, max and avg a value per series (the inner stats keeps the identity next to
//     the labels of the clause), and a without clause groups nothing and leaves the fold to the proxy, over a
//     stats that keeps a row per stream;
//   - quantile_over_time aggregates the rows of its group under any outer aggregation, which only chooses the
//     grouping, and rate_counter's value is computed by the proxy (the marker function).
//
// Left to the string translator under a construct name: stdvar_over_time (the proxy squares the deviation), a
// grouping by without, a range of no length under rate, a label that is not named as the translator reads it from
// the text, and every query whose text the translator reads as syntax (a quoted `| unwrap`, `| json` or `| stats`).

// rateCounterMarker is the stats function the proxy answers itself for rate_counter.
const rateCounterMarker = "__rate_counter__"

// UnwrapsReservedFieldOtherCase reports a plan whose lowering unwraps a label named _time, _stream or _stream_id in
// another letter case (logsql.ReservedFieldNameOtherCase): the lowering writes its gate in the outbound guard's
// form, where the string translator writes the filter VictoriaLogs cannot read back from its own print
// (semantics/reserved-field-name-case-outbound), so the shadows hold the lowering to the translator's text guarded.
func UnwrapsReservedFieldOtherCase(p *queryplan.Plan) bool {
	var walk func(n queryplan.Node) bool
	walk = func(n queryplan.Node) bool {
		switch n := n.(type) {
		case *queryplan.RangeAgg:
			u := unwrapStage(n.In)
			return u != nil && logsql.ReservedFieldNameOtherCase(u.Label)
		case *queryplan.VectorAgg:
			return walk(n.In)
		case *queryplan.Rank:
			return walk(n.In)
		case *queryplan.LabelReplace:
			return walk(n.In)
		case *queryplan.BinOp:
			return walk(n.L) || walk(n.R)
		}
		return false
	}
	return walk(p.Root)
}

// unwrapStage returns the unwrap stage of the log query, or nil.
func unwrapStage(sel *queryplan.LogSelect) *queryplan.Unwrap {
	for _, st := range sel.Stages {
		if u, ok := st.(*queryplan.Unwrap); ok {
			return u
		}
	}
	return nil
}

// stripUnwrap returns the log query without its unwrap stage, which the string translator drops, and the stage.
// owner is the construct the aggregation delegates as. A query with several unwrap stages and a stage without a
// label (a stub the translator rejects) are left to it; the stages that stand after the unwrap stay where they
// stand, as the translator keeps them.
func stripUnwrap(sel *queryplan.LogSelect, owner string) (*queryplan.LogSelect, *queryplan.Unwrap, error) {
	var rest queryplan.LogSelect
	u, err := stripUnwrapInto(sel, &rest, owner)
	if err != nil {
		return nil, nil, err
	}
	return &rest, u, nil
}

// stripUnwrapInto is stripUnwrap writing the query into rest, whose Stages may start with room (a caller that keeps
// the query to itself gives it a stack array).
func stripUnwrapInto(sel *queryplan.LogSelect, rest *queryplan.LogSelect, owner string) (*queryplan.Unwrap, error) {
	var u *queryplan.Unwrap
	idx := -1
	for i, st := range sel.Stages {
		if x, ok := st.(*queryplan.Unwrap); ok {
			if u != nil {
				return nil, &NotLoweredError{Construct: owner + " unwrap stages"}
			}
			u, idx = x, i
		}
	}
	if u == nil {
		return nil, &NotLoweredError{Construct: owner + " unwrap"}
	}
	if u.Label == "" {
		return nil, &NotLoweredError{Construct: owner + " unwrap label"}
	}
	stages := rest.Stages[:0]
	*rest = *sel
	rest.Stages = append(append(stages, sel.Stages[:idx]...), sel.Stages[idx+1:]...)
	return u, nil
}

// unwrapLog lowers the log query of an unwrap aggregation: the query without its unwrap stage, as the metric row
// writes it (metricLogQuery).
func unwrapLog(sel *queryplan.LogSelect, prof Profile, owner string) (metricLog, *queryplan.Unwrap, error) {
	var stages [8]queryplan.Stage
	rest := queryplan.LogSelect{Stages: stages[:0]}
	u, err := stripUnwrapInto(sel, &rest, owner)
	if err != nil {
		return metricLog{}, nil, err
	}
	log, err := metricLogQuery(&rest, prof, owner)
	if err != nil {
		return metricLog{}, nil, err
	}
	// The pipes that derive detected_level for a filter on it hold unpack pipes, which the translator reads as the
	// parser of the unwrapped label: left to it.
	if log.levelChain {
		return metricLog{}, nil, &NotLoweredError{Construct: owner + " " + levelUnwrapConstruct}
	}
	// The translator reads the unwrap and the parsers out of the text of the query, quoted strings included.
	if textReadAsClause(log) || !log.parser && textHas(log, "| unpack_") || readsUnwrapOrParser(&rest, log, true, !log.parser) {
		return metricLog{}, nil, quotedClauseError(owner)
	}
	return log, u, nil
}

// withoutUnwrap is a range aggregation that reads no unwrapped value (absent_over_time) written with an unwrap
// stage: the translator drops the stage, so the aggregation is the same over the query without it.
func withoutUnwrap(ra *queryplan.RangeAgg, owner string) (*queryplan.RangeAgg, error) {
	rest, _, err := stripUnwrap(ra.In, owner)
	if err != nil {
		return nil, err
	}
	cp := *ra
	cp.In = rest
	return &cp, nil
}

// readsUnwrapOrParser reports whether the log query holds text the string translator reads as a parser when
// parser is set (its regexp, hasParserPipeline, looks at quoted strings too) or, when unwrap is set, as an unwrap
// stage (its search for `| unwrap `).
// The printed query may not hold the client's value (a value that is not UTF-8), so the selector and the line
// filters are read as well.
func readsUnwrapOrParser(sel *queryplan.LogSelect, m metricLog, unwrap, parser bool) bool {
	if textHasUnwrapOrParser(m.head, unwrap, parser) || textHasUnwrapOrParser(m.restore, unwrap, parser) || textHasUnwrapOrParser(m.pipes, unwrap, parser) {
		return true
	}
	return anyClientString(sel, func(s string) bool { return textHasUnwrapOrParser(s, unwrap, parser) })
}

// textHasUnwrapOrParser reports whether s holds `| unwrap ` (when unwrap is set) or a pipe followed (after blanks) by json, logfmt,
// pattern, regexp or unpack at a word boundary.
func textHasUnwrapOrParser(s string, unwrap, parser bool) bool {
	if !strings.Contains(s, "|") {
		return false
	}
	if unwrap && strings.Contains(s, "| unwrap ") {
		return true
	}
	if !parser {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] != '|' {
			continue
		}
		j := i + 1
		for j < len(s) && isRegexpSpace(s[j]) {
			j++
		}
		for _, w := range [...]string{"json", "logfmt", "pattern", "regexp", "unpack"} {
			if strings.HasPrefix(s[j:], w) && (len(s) == j+len(w) || !isWordByte(s[j+len(w)])) {
				return true
			}
		}
	}
	return false
}

// isRegexpSpace is \s of Go's regexp: space, tab, newline, form feed and carriage return.
func isRegexpSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r'
}

func isWordByte(c byte) bool {
	// The word characters of the \b of Go's regexp, which the translator's scan uses: ASCII only. A byte of a
	// multi-byte character (or an invalid one) after the word is a boundary, so `| jsoné` is a parser to it.
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// unwrapConv is the conversion the gate of an unwrap stage selects: "" for a plain number, "duration" (duration()
// and duration_seconds()) or "bytes". ok is false for a converter the string translator does not know.
func unwrapConv(u *queryplan.Unwrap) (conv string, ok bool) {
	switch u.Converter {
	case "":
		return "", true
	case "bytes":
		return "bytes", true
	case "duration", "duration_seconds":
		return "duration", true
	}
	return "", false
}

// unwrapQuery collects the pipes of an unwrap aggregation and what its groupings need.
type unwrapQuery struct {
	pipes []string
	// chain and service say a stats pipe groups by detected_level or service_name, which the query derives.
	chain, service bool
	// squares says the pipes square the deviation themselves (a stdvar under a vector aggregation without a clause).
	squares bool
	// parser says the log query has a parser: the series are then the stream (and each line, for an unwrapped
	// value) instead of the stream and its level.
	parser bool
	// unpacked says the log query holds a json or logfmt unpack, which makes the translator resolve the labels its
	// stats read: a parser of regexp and pattern only does not.
	unpacked bool
	// fold is the series identity of a `without` fold over a parser that names the labels it extracts
	// (parsedSeriesLabels); nil for the others.
	fold []string
	// firstBy and firstEmpty are the grouping of the first stats pipe, which reads the unwrapped value (set marks
	// them known); the bare logfmt unpack ahead of it is narrowed to the labels it reads.
	firstBy    []string
	firstEmpty bool
	firstSet   bool
	// shape is what the pipes written so far say of their stats (statsShape).
	shape statsShape
}

// statsShape is what the route rule reads of the stats pipes of a lowered query besides its text: the typed
// facts buildVectorAgg's stats give it (builtVector.shape).
type statsShape struct {
	// grouped says a stats pipe groups by a key, and ungrouped that the last one groups by nothing (VictoriaLogs
	// then answers one row even over no input).
	grouped, ungrouped bool
}

// statsView is a lowered vector aggregation as the route rule reads it besides its text: the log query it
// filters with, whether its stats group by detected_level (which the query then derives) and the shape of its stats.
type statsView struct {
	log   metricLog
	chain bool
	shape statsShape
}

// note records a stats pipe that groups by keys keys.
func (s *statsShape) note(keys int) {
	s.grouped = s.grouped || keys > 0
	s.ungrouped = keys == 0
}

// noteFirst records the grouping of the stats pipe that follows the gate.
func (q *unwrapQuery) noteFirst(by []string, empty bool) {
	if !q.firstSet {
		q.firstSet, q.firstBy, q.firstEmpty = true, by, empty
	}
}

// stats appends a stats pipe over by (none: no grouping; empty: an explicit `by ()`) as the string translator writes
// it: a grouping by the stream and its level also groups by detected_level.
func (q *unwrapQuery) stats(by []string, empty bool, fn logsql.StatsFunc, alias string) {
	q.noteFirst(by, empty)
	var keyBuf [4]logsql.GroupKey
	keys := keyBuf[:len(by):len(keyBuf)]
	if len(by) >= len(keyBuf) {
		keys = make([]logsql.GroupKey, len(by), len(by)+1)
	}
	for i, f := range by {
		keys[i] = logsql.GroupKey{Field: f}
		q.chain = q.chain || f == translate.DetectedLevelLabel
		q.service = q.service || f == "service_name"
	}
	if hasField(by, "_stream") && hasField(by, "level") && !hasField(by, translate.DetectedLevelLabel) {
		keys = append(keys, logsql.GroupKey{Field: translate.DetectedLevelLabel})
		q.chain = true
	}
	q.shape.note(len(keys))
	text := logsql.PipeStats{By: keys, Funcs: []logsql.StatsFuncAlias{{Func: fn, Alias: alias}}}.String()
	if empty {
		text = "| stats by () " + strings.TrimPrefix(text, "| stats ")
	}
	q.pipes = append(q.pipes, text)
}

// unwrapOp is what an unwrap aggregation computes per row group.
type unwrapOp struct {
	// fn is the stats function over the converted value.
	fn logsql.StatsFunc
	// row marks first and last, which keep the row of the earliest or the latest time (row_min, row_max).
	row bool
	// quantile marks quantile_over_time, which aggregates the rows of its group under any outer aggregation.
	quantile bool
	// variance marks stdvar_over_time: VictoriaLogs has stddev only, so the proxy squares the deviation.
	variance bool
}

// unwrapFunc is the stats function of a range function over an unwrapped label. ok is false for the ones left to
// the string translator.
func unwrapFunc(ra *queryplan.RangeAgg) (op unwrapOp, ok bool) {
	v := translate.UnwrapValueAlias
	switch ra.Op {
	case logql.RangeSumOverTime:
		op.fn = logsql.Sum{Field: v}
	case logql.RangeAvgOverTime:
		op.fn = logsql.Avg{Field: v}
	case logql.RangeMinOverTime:
		op.fn = logsql.Min{Field: v}
	case logql.RangeMaxOverTime:
		op.fn = logsql.Max{Field: v}
	case logql.RangeStddevOverTime:
		op.fn = logsql.Stddev{Field: v}
	case logql.RangeStdvarOverTime:
		op.fn, op.variance = logsql.Stddev{Field: v}, true
	case logql.RangeFirstOverTime:
		op.fn, op.row = logsql.RowMin{By: "_time", Fields: []string{v}}, true
	case logql.RangeLastOverTime:
		op.fn, op.row = logsql.RowMax{By: "_time", Fields: []string{v}}, true
	case logql.RangeRateCounter:
		op.fn = logsql.DeferredExpr{Raw: rateCounterMarker + "(" + v + ")"}
	case logql.RangeQuantileOverTime:
		op.fn, op.quantile = logsql.Quantile{Phi: ra.Phi, Field: v}, true
	default:
		return unwrapOp{}, false
	}
	return op, true
}

// firstLastUnpack are the pipes that read the unwrapped value out of the row first and last keep per series.
var firstLastUnpack = logsql.PipeUnpackJSON{From: "__lvp_inner", Fields: []string{translate.UnwrapValueAlias}}.String() + " " +
	logsql.PipeMath{Expr: translate.UnwrapValueAlias, Alias: "__lvp_inner"}.String()

// unwrapRange lowers a range aggregation over an unwrapped label.
func unwrapRange(ra *queryplan.RangeAgg, prof Profile) (physical.Op, error) {
	rate := ra.Op == logql.RangeRate
	op, ok := unwrapFunc(ra)
	switch {
	case rate:
		op, ok = unwrapOp{fn: logsql.Sum{Field: translate.UnwrapValueAlias}}, true
	case !ok:
		return nil, &NotLoweredError{Construct: "RangeAgg unwrap " + string(ra.Op)}
	}
	seconds := 0.0
	if rate {
		// The translator reads the range's seconds from its text and writes none for a range of no length.
		if seconds = ra.Range.Seconds(); seconds <= 0 {
			return nil, &NotLoweredError{Construct: "RangeAgg range"}
		}
	}
	by, empty, err := rangeByLabels(ra, prof)
	if err != nil {
		return nil, err
	}
	log, u, err := unwrapLog(ra.In, prof, "RangeAgg")
	if err != nil {
		return nil, err
	}
	conv, ok := unwrapConv(u)
	if !ok {
		return nil, &NotLoweredError{Construct: "RangeAgg unwrap conversion"}
	}
	// The unwrap stage stands where it was written: the translator takes a keep after it to decide the series of a rate.
	var ident []string
	if rate {
		if ident, err = seriesIdent(ra.In, log.parser, false, "RangeAgg", prof); err != nil {
			return nil, err
		}
	}
	// The pipes start in room a query rarely outgrows.
	var pipeBuf [4]string
	q := unwrapQuery{parser: log.parser, unpacked: log.unpacked(), pipes: pipeBuf[:0]}
	switch {
	case rate:
		q.rate(op.fn, seconds, ident)
	case op.row:
		// A function alone groups by the series, unless the clause says otherwise.
		if len(by) == 0 && !empty {
			by = q.rowIdentity()
		}
		q.stats(by, empty, op.fn, "__lvp_inner")
		q.pipes = append(q.pipes, firstLastUnpack)
		q.stats(by, empty, logsql.Max{Field: "__lvp_inner"}, "")
	default:
		// Over a parser the series is each line of the stream, unless the clause says otherwise.
		if len(by) == 0 && !empty && q.parser {
			by = streamLine
		}
		q.stats(by, empty, op.fn, "")
	}
	return q.op(op, log, u.Label, conv, prof, "RangeAgg")
}

// squared is the right operand of the power the proxy takes of a deviation to answer stdvar_over_time.
var squared = physical.ScalarOp{V: 2, Text: "2"}

// op is the lowered query: the text, and for stdvar_over_time the deviation it computes raised to the power of 2
// by the proxy (a binary expression).
func (q *unwrapQuery) op(op unwrapOp, log metricLog, field, conv string, prof Profile, owner string) (physical.Op, error) {
	// A parser that is a bare logfmt unpack ends the query: it reads only the unwrapped label and the labels of
	// the first stats pipe. The translator narrows it before it resolves the labels, which then no longer finds
	// a plain unpack pipe.
	var names []string
	narrowed := false
	if q.parser && log.bareLogfmt >= 0 && q.firstSet {
		names, narrowed = narrowedFields(field, q.firstBy, q.firstEmpty, log.resolvable)
	}
	pre := log
	if narrowed {
		pre.bareMask &^= 1
	}
	keys, err := pre.keyPipes(q.firstBy, op.fn, field, owner, false)
	if err != nil {
		return nil, err
	}
	if narrowed {
		log.pipes = narrowedUnpack(log.pipes[:log.bareLogfmt], names)
	}
	text := physical.VLQuery{Text: q.text(log, field, conv, keys.text, prof)}
	if op.variance && !q.squares {
		return physical.Binary{Op: "^", L: text, R: squared}, nil
	}
	return text, nil
}

// narrowedUnpack is the log query's pipes up to the bare logfmt unpack, then the unpack reading only names:
// `| unpack_logfmt fields (a, b) skip_empty_results`, written in one allocation.
func narrowedUnpack(head string, names []string) string {
	const fields, skip = " fields (", ") skip_empty_results"
	n := len(head) + 1 + len(bareUnpackLogfmt) + len(fields) + len(skip)
	for _, name := range names {
		n += len(name) + 2
	}
	var b strings.Builder
	b.Grow(n)
	b.WriteString(head)
	b.WriteByte(' ')
	b.WriteString(bareUnpackLogfmt)
	b.WriteString(fields)
	for i, name := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(name)
	}
	b.WriteString(skip)
	return b.String()
}

// rate appends the stats pipes of a rate over an unwrapped label: the values summed per series (the stream, its
// stored level and the detected_level Loki attaches; the stream alone over a parser), divided by the range's
// seconds, and summed again.
func (q *unwrapQuery) rate(fn logsql.StatsFunc, seconds float64, ident []string) {
	if q.parser || ident != nil {
		keys, by := parserIdentity, parserBy
		if ident != nil {
			keys, by = groupKeys(ident), ident
		}
		pipes := translate.RateStatsPipesBy(fn, seconds, keys)
		q.noteFirst(by, false)
		first := pipes[0].(logsql.PipeStats)
		q.chain = q.chain || groupsBy(first, translate.DetectedLevelLabel)
		q.service = q.service || groupsBy(first, "service_name")
		q.pipes = append(q.pipes, pipes[0].String(), pipes[1].String())
		if q.unpacked {
			q.pipes = appendRateFilter(q.pipes)
		}
		q.pipes = append(q.pipes, pipes[2].String())
		return
	}
	pipes := translate.RateStatsPipesOf(fn, seconds)
	q.chain = true
	for _, p := range pipes {
		q.pipes = append(q.pipes, p.String())
	}
}

// streamLine is the series of an unwrapped value over a parser: each line of the stream, which the proxy folds.
var streamLine = []string{"_stream", "_msg"}

// rowIdentity is the grouping of first and last alone, without a clause.
func (q *unwrapQuery) rowIdentity() []string {
	if q.parser {
		return streamLine
	}
	return []string{"_stream", "level"}
}

// outerIdentity is the grouping of the stats that read one value per series under an aggregation without a clause.
func (q *unwrapQuery) outerIdentity() []string {
	if q.fold != nil {
		return q.fold
	}
	if q.parser {
		return streamLine
	}
	return []string{"_stream"}
}

// rateFilterField is the label the final stats of a rate reads, which the translator looks for among the fields
// the query defines.
const rateFilterField = "__lvp_rate"

// appendRateFilter appends, before the final stats of a rate over a parser and an unwrapped label, the filter the
// translator writes: its search of the query for a copy pipe finds the gate's, and it reads the rate as the
// label of a stats function that a parser may have defined.
func appendRateFilter(pipes []string) []string {
	return append(pipes, logsql.PipeFilter{Expr: logsql.FieldFilter{Field: rateFilterField, Op: logsql.FieldOpAny}}.String())
}

// text is the query: the log query with the derivations its groupings need, the unwrap gate and the stats pipes.
func (q *unwrapQuery) text(log metricLog, field, conv, keys string, prof Profile) string {
	chain, service := "", ""
	if q.chain {
		chain = translate.RateDetectedLevelChain()
	}
	if q.service {
		service = translate.ServiceNameGroupingPipes(prof.Caps)
	}
	// The gate is translate.UnwrapGateFor(field, conv) written in pieces.
	match, convert := translate.UnwrapGateParts(field, conv)
	inRange := translate.UnwrapInRange(conv)
	n := len(match) + len(convert) + len(translate.UnwrapFilterPipe) + len(inRange) + len(keys)
	for _, p := range q.pipes {
		n += len(p) + 1
	}
	var b strings.Builder
	log.write(&b, chain, service, n+40)
	// The pipes that resolve the labels of the first stats pipe, and drop the lines that have no value when they
	// resolved the unwrapped label or a copy defines it (a stored stream value, a json expression).
	b.WriteString(keys)
	b.WriteString(match)
	b.WriteString(convert)
	b.WriteString(translate.UnwrapFilterPipe)
	b.WriteString(inRange)
	for _, p := range q.pipes {
		b.WriteByte(' ')
		b.WriteString(p)
	}
	if logsql.ReservedFieldNameOtherCase(field) {
		// The gate filters the unwrapped label by name, quoted, and VictoriaLogs cannot read back from its own
		// print a filter on _time, _stream or _stream_id in another case (VictoriaMetrics/VictoriaLogs#1942): the
		// query is written in the form the proxy's outbound guard writes, a copy into a scratch field around each
		// such filter (logsql.GuardReservedFieldCase), so nothing is left for the guard to rewrite.
		return logsql.GuardReservedFieldCase(b.String())
	}
	return b.String()
}

// rangeByLabels are the labels of a range aggregation's by clause as VictoriaLogs names them; empty says the
// clause is `by ()`. A without clause, and a clause whose labels the label function turns into nothing, are left
// to the string translator.
func rangeByLabels(ra *queryplan.RangeAgg, prof Profile) (by []string, empty bool, err error) {
	g := ra.By
	if g == nil {
		return nil, false, nil
	}
	if g.Without {
		return nil, false, &NotLoweredError{Construct: "RangeAgg grouping"}
	}
	if len(g.Labels) == 0 {
		return nil, true, nil
	}
	by = translate.NormalizeByLabels(g.Labels, prof.Label)
	if len(by) == 0 || slices.Contains(g.Labels, "bool") {
		return nil, false, &NotLoweredError{Construct: "RangeAgg grouping label"}
	}
	return by, false, nil
}

// unwrapVector lowers sum, count, min, max and avg, with or without a by or without clause, over a range aggregation
// of an unwrapped label (see the top of this file for the shapes).
func unwrapVector(va *queryplan.VectorAgg, ra *queryplan.RangeAgg, prof Profile) (physical.Op, error) {
	out, _, err := unwrapVectorStats(va, ra, prof)
	return out, err
}

// unwrapVectorStats is unwrapVector with the stats it writes, typed the way buildVectorAgg's are (statsView): the
// route rule reads them next to the text.
func unwrapVectorStats(va *queryplan.VectorAgg, ra *queryplan.RangeAgg, prof Profile) (physical.Op, statsView, error) {
	switch va.Op {
	case logql.VectorSum, logql.VectorCount, logql.VectorMin, logql.VectorMax, logql.VectorAvg:
	default:
		return nil, statsView{}, &NotLoweredError{Construct: "VectorAgg " + string(va.Op)}
	}
	if ra.By != nil {
		return nil, statsView{}, &NotLoweredError{Construct: "VectorAgg grouping"}
	}
	rate := ra.Op == logql.RangeRate
	op, ok := unwrapFunc(ra)
	switch {
	case rate:
		op, ok = unwrapOp{fn: logsql.Sum{Field: translate.UnwrapValueAlias}}, true
	case !ok:
		return nil, statsView{}, &NotLoweredError{Construct: "VectorAgg unwrap " + string(ra.Op)}
	}
	seconds := 0.0
	if rate {
		if seconds = ra.Range.Seconds(); seconds <= 0 {
			return nil, statsView{}, &NotLoweredError{Construct: "VectorAgg range"}
		}
	}
	labels, raw, without, err := vectorClause(va, prof, nil)
	if err != nil {
		return nil, statsView{}, err
	}
	log, u, err := unwrapLog(ra.In, prof, "VectorAgg")
	if err != nil {
		return nil, statsView{}, err
	}
	conv, ok := unwrapConv(u)
	if !ok {
		return nil, statsView{}, &NotLoweredError{Construct: "VectorAgg unwrap conversion"}
	}
	// The translator reads a clause and a stats pipe out of the text of the query, quoted strings included.
	if (without || len(labels) > 0) && textReadAsClause(log) {
		return nil, statsView{}, &NotLoweredError{Construct: "VectorAgg quoted clause"}
	}
	// Stripping a without clause, the translator also folds every run of blanks in the query into one.
	if without && (textHas(log, "  ") || selectionHas(ra.In, "  ")) {
		return nil, statsView{}, &NotLoweredError{Construct: "VectorAgg quoted clause"}
	}
	// A fold over a parser, with a label filter or a drop after it, costs more allocations to write than the
	// translator spends on the same text (the stage's own lowering does).
	if without && log.parser && log.afterParser {
		return nil, statsView{}, &NotLoweredError{Construct: "VectorAgg" + costConstruct}
	}
	// The pipes start in room a query rarely outgrows.
	var pipeBuf [4]string
	q := unwrapQuery{parser: log.parser, unpacked: log.unpacked(), pipes: pipeBuf[:0]}
	if rate {
		// The unwrap stage stands where it was written: the translator takes a keep after it to decide the series.
		ident, err := seriesIdent(ra.In, log.parser, without, "VectorAgg", prof)
		if err != nil {
			return nil, statsView{}, err
		}
		q.vectorRate(va.Op, op.fn, seconds, labels, without, log, ident)
	} else {
		// Under a `without` fold a series is told apart by the labels the parsers name (quantile_over_time groups the
		// stream and each line whatever the clause says).
		if without && log.parser && !op.quantile {
			if names, ok := parsedSeriesLabels(ra.In, u.Label); ok {
				q.fold = translate.JoinByLabels([]string{"_stream"}, names)
			}
		}
		q.vector(va.Op, op, labels, without)
	}
	out, err := q.op(op, log, u.Label, conv, prof, "VectorAgg")
	if err != nil {
		return nil, statsView{}, err
	}
	if without {
		out = physical.Fold{Levels: []physical.FoldLevel{{Op: string(va.Op), Labels: raw, Without: true}}, In: out}
	}
	return out, statsView{log: log, chain: q.chain, shape: q.shape}, nil
}

// vectorClause reads the by or without clause of a vector aggregation: the labels as VictoriaLogs names them (for a
// by clause), the labels as written (for a without clause) and whether it is a without. buf, when not nil, holds
// the labels, so a caller with a small array allocates nothing for them.
func vectorClause(va *queryplan.VectorAgg, prof Profile, buf []string) (labels, raw []string, without bool, err error) {
	g := va.By
	if g == nil {
		return nil, nil, false, nil
	}
	if len(g.Labels) > 0 {
		if buf == nil {
			buf = make([]string, 0, len(g.Labels))
		}
		labels = translate.AppendNormalizedLabels(buf[:0], g.Labels, prof.Label)
		if len(labels) == 0 {
			return nil, nil, false, &NotLoweredError{Construct: "VectorAgg grouping label"}
		}
	}
	// A label named bool, with a blank on each side in the text, is a modifier to the translator's regexp.
	if !g.Without && slices.Contains(g.Labels, "bool") {
		return nil, nil, false, &NotLoweredError{Construct: "VectorAgg quoted clause"}
	}
	if g.Without {
		if len(g.Labels) == 0 {
			return nil, nil, false, &NotLoweredError{Construct: "VectorAgg without empty"}
		}
		return nil, g.Labels, true, nil
	}
	return labels, nil, false, nil
}

// vectorRate appends the stats of a vector aggregation over the rate of an unwrapped label: the shapes of a rate
// over rows (vectorStats), over the sum of the values.
func (q *unwrapQuery) vectorRate(op logql.VectorOp, fn logsql.StatsFunc, seconds float64, labels []string, without bool, log metricLog, ident []string) {
	if ident == nil {
		ident = seriesIdentity(q.parser)
	}
	pipes := rateVectorPipes(vectorStats(op, fn, true, seconds, labels, without, ident), op, labels, log, true)
	for i, p := range pipes {
		if st, ok := p.(logsql.PipeStats); ok {
			q.chain = q.chain || groupsBy(st, translate.DetectedLevelLabel)
			q.service = q.service || groupsBy(st, "service_name")
			q.shape.note(len(st.By))
			if i == 0 {
				by := make([]string, len(st.By))
				for k, key := range st.By {
					by[k] = key.Field
				}
				q.noteFirst(by, false)
			}
		}
		q.pipes = append(q.pipes, p.String())
	}
}

// rateVectorPipes puts the filter of the final stats of a rate (appendRateFilter) into the pipes of a sum by labels
// over a rate of a log query with a parser: the translator builds that stats through the path that resolves
// the labels it reads. copied says the query holds a copy pipe by then: the unwrap gate has one, a log query or
// the pipes that resolved the labels may.
func rateVectorPipes(pipes []logsql.Pipe, op logql.VectorOp, labels []string, log metricLog, copied bool) []logsql.Pipe {
	if !log.parser || !log.unpacked() || op != logql.VectorSum || len(labels) == 0 || !copied || len(pipes) != 3 {
		return pipes
	}
	return []logsql.Pipe{pipes[0], pipes[1], logsql.PipeFilter{Expr: logsql.FieldFilter{Field: rateFilterField, Op: logsql.FieldOpAny}}, pipes[2]}
}

// vector appends the stats of a vector aggregation over a function of an unwrapped label (not a rate).
func (q *unwrapQuery) vector(vop logql.VectorOp, op unwrapOp, labels []string, without bool) {
	if op.quantile {
		// The outer aggregation only picks the grouping: the rows of each group are aggregated.
		q.quantileVector(vop, op, labels)
		return
	}
	if op.variance {
		q.varianceVector(vop, op, labels, without)
		return
	}
	// The aggregation of the values of the series: count counts them, sum adds them, and so on. A sum over
	// the rows needs no inner stats, except for first and last, which read a value per series.
	perSeries := vop != logql.VectorSum || op.row
	identity := q.outerIdentity()
	if !op.row {
		identity = seriesIdentity(q.parser)
	}
	switch {
	case perSeries && len(labels) > 0:
		q.seriesStats(op, translate.JoinByLabels(labels, identity))
		q.stats(labels, false, outerFunc(vop), "")
	case len(labels) > 0:
		q.stats(labels, false, op.fn, "")
	default:
		q.seriesStats(op, q.outerIdentity())
		if without {
			q.stats(foldKeep(q.outerIdentity(), vop), false, outerFunc(vop), "")
		} else {
			q.stats(nil, false, outerFunc(vop), "")
		}
	}
}

// varianceVector appends the stats of a vector aggregation over stdvar_over_time. With no clause the deviation is
// taken per stream, squared, and the aggregation reads those values; with a clause the deviation is taken per
// group and the proxy squares it.
func (q *unwrapQuery) varianceVector(vop logql.VectorOp, op unwrapOp, labels []string, without bool) {
	if len(labels) > 0 {
		q.stats(labels, false, op.fn, "")
		return
	}
	q.stats(q.outerIdentity(), false, op.fn, "__lvp_inner")
	q.pipes = append(q.pipes, logsql.PipeMath{Alias: "__lvp_inner_var", Expr: "__lvp_inner*__lvp_inner"}.String())
	q.squares = true
	var by []string
	if without {
		by = foldKeep(q.outerIdentity(), vop)
	}
	q.stats(by, false, outerFuncOf(vop, "__lvp_inner_var"), "")
}

// foldKeep is the grouping of the outer stats of a `without` fold over the series rows of the identity ident: the
// translator keeps the labels of the series in it (keepWithoutSeries), except the line, which stands for the labels
// a parser extracts and which the proxy does not expose. sum, max and min fold the rows of a stream to the same
// value either way; count and avg read one value per series, which only a grouping without the line keeps.
func foldKeep(ident []string, op logql.VectorOp) []string {
	keep := ident
	perLine := false
	if i := slices.Index(ident, "_msg"); i >= 0 {
		keep, perLine = slices.Delete(slices.Clone(ident), i, i+1), true
	}
	switch {
	case len(keep) == 0:
		return nil
	case op == logql.VectorSum || op == logql.VectorMax || op == logql.VectorMin:
		return keep
	case !perLine:
		return keep
	}
	return nil
}

// seriesStats appends the stats that read one value per series, grouped by by, aliased for the outer stats.
func (q *unwrapQuery) seriesStats(op unwrapOp, by []string) {
	q.stats(by, false, op.fn, "__lvp_inner")
	if op.row {
		q.pipes = append(q.pipes, firstLastUnpack)
	}
}

// quantileVector appends the stats of a vector aggregation over quantile_over_time: with no clause the quantile is
// taken per stream and the aggregation reads those values; with a clause the quantile is taken per group.
func (q *unwrapQuery) quantileVector(vop logql.VectorOp, op unwrapOp, labels []string) {
	if len(labels) > 0 {
		q.stats(labels, false, op.fn, "")
		return
	}
	q.stats(q.outerIdentity(), false, op.fn, "__lvp_inner")
	q.stats(nil, false, outerFunc(vop), "")
}

// outerFunc is the stats function of a vector aggregation over the per-series values (__lvp_inner).
func outerFunc(vop logql.VectorOp) logsql.StatsFunc { return outerFuncOf(vop, "__lvp_inner") }

// outerFuncOf is the stats function of a vector aggregation over the field.
func outerFuncOf(vop logql.VectorOp, field string) logsql.StatsFunc {
	switch vop {
	case logql.VectorCount:
		return logsql.Count{}
	case logql.VectorMin:
		return logsql.Min{Field: field}
	case logql.VectorMax:
		return logsql.Max{Field: field}
	case logql.VectorAvg:
		return logsql.Avg{Field: field}
	}
	return logsql.Sum{Field: field}
}

// screenUnwrap turns away an unwrap aggregation whose spelling the string translator reads unlike the plan: it
// finds the unwrapped label and its conversion by searching the query text for `| unwrap ` (one blank on each
// side) and cutting the text after it at the next `|` or `[`. A query that spells the stage another way (`|unwrap
// n`), or holds the search text in an earlier quoted string, gives it another label, another conversion or none (a
// query the translator rejects): the lowering leaves it to the translator.
func screenUnwrap(query string, p *queryplan.Plan) *NotLoweredError {
	var ra *queryplan.RangeAgg
	owner := "RangeAgg"
	switch n := p.Root.(type) {
	case *queryplan.RangeAgg:
		ra = n
	case *queryplan.VectorAgg:
		if r, ok := n.In.(*queryplan.RangeAgg); ok {
			ra, owner = r, "VectorAgg"
		}
	}
	if ra == nil {
		return nil
	}
	u := unwrapStage(ra.In)
	if u == nil {
		return nil
	}
	field, conv, ok := unwrapFromText(query)
	if want, known := unwrapConv(u); !ok || !known || field != u.Label || conv != want {
		return &NotLoweredError{Construct: owner + " unwrap spelling"}
	}
	// The string translator copies the quantile's parameter as written, and the typed function prints the shortest
	// form of its value: a parameter written another way (`0.50`, `5e-1`) is left to the translator.
	if ra.Op == logql.RangeQuantileOverTime && quantileFromText(query) != strconv.FormatFloat(ra.Phi, 'g', -1, 64) {
		return &NotLoweredError{Construct: owner + " unwrap quantile spelling"}
	}
	return nil
}

// quantileFromText is the parameter the string translator reads out of the text of a quantile_over_time: what stands
// between the opening parenthesis of the call and the first comma, without the blanks around it. It is empty for a
// query that does not spell the call as the translator looks for it.
func quantileFromText(query string) string {
	const call = "quantile_over_time("
	idx := strings.Index(query, call)
	if idx < 0 {
		return ""
	}
	rest := query[idx+len(call):]
	comma := strings.IndexByte(rest, ',')
	if comma < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:comma])
}

// unwrapFromText is the label and conversion the string translator reads from the text of a query.
func unwrapFromText(query string) (field, conv string, ok bool) {
	const marker = "| unwrap "
	idx := strings.Index(query, marker)
	if idx < 0 {
		return "", "", false
	}
	rest := strings.TrimSpace(query[idx+len(marker):])
	for _, c := range translate.UnwrapConversions {
		if strings.HasPrefix(rest, c.Call) {
			conv = c.Conv
			break
		}
	}
	if end := strings.IndexAny(rest, "|["); end >= 0 {
		rest = rest[:end]
	}
	rest = strings.TrimSpace(rest)
	for _, c := range translate.UnwrapConversions {
		if strings.HasPrefix(rest, c.Call) && strings.HasSuffix(rest, ")") {
			rest = rest[len(c.Call) : len(rest)-1]
			break
		}
	}
	return rest, conv, true
}

// plainIdent reports whether s is a label name the translator reads as a plain identifier.
func plainIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if name := c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' && i > 0; !name {
			return false
		}
	}
	return true
}

// narrowedFields are the fields a bare logfmt unpack ahead of an unwrap reads: the unwrapped label and the labels
// of the first stats pipe, when each is a plain name that does not start with an underscore, each in every spelling
// of its key when the log query leaves it to be resolved (resolvable). ok is false when the unpack stays bare: a
// grouping that is none, or that holds the stream or the line.
func narrowedFields(field string, by []string, empty bool, resolvable func(string) bool) (names []string, ok bool) {
	if !empty && len(by) == 0 {
		return nil, false
	}
	names = make([]string, 0, 1+len(by))
	for i := -1; i < len(by); i++ {
		l := field
		if i >= 0 {
			l = by[i]
		}
		if !plainIdent(l) || l[0] == '_' {
			return nil, false
		}
		keys := [...]string{l}
		spellings := keys[:]
		if resolvable(l) {
			spellings = translate.ParsedKeyVariants(l)
		}
		for _, key := range spellings {
			if key = quoteField(key); !slices.Contains(names, key) {
				names = append(names, key)
			}
		}
	}
	return names, true
}
