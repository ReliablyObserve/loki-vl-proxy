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
	var u *queryplan.Unwrap
	idx := -1
	for i, st := range sel.Stages {
		if x, ok := st.(*queryplan.Unwrap); ok {
			if u != nil {
				return nil, nil, &NotLoweredError{Construct: owner + " unwrap stages"}
			}
			u, idx = x, i
		}
	}
	if u == nil {
		return nil, nil, &NotLoweredError{Construct: owner + " unwrap"}
	}
	if u.Label == "" {
		return nil, nil, &NotLoweredError{Construct: owner + " unwrap label"}
	}
	rest := *sel
	rest.Stages = make([]queryplan.Stage, 0, len(sel.Stages)-1)
	rest.Stages = append(append(rest.Stages, sel.Stages[:idx]...), sel.Stages[idx+1:]...)
	return &rest, u, nil
}

// unwrapLog lowers the log query of an unwrap aggregation: the query without its unwrap stage, as the metric row
// writes it (metricLogQuery).
func unwrapLog(sel *queryplan.LogSelect, prof Profile, owner string) (metricLog, *queryplan.Unwrap, error) {
	rest, u, err := stripUnwrap(sel, owner)
	if err != nil {
		return metricLog{}, nil, err
	}
	log, err := metricLogQuery(rest, prof, owner)
	if err != nil {
		return metricLog{}, nil, err
	}
	// The translator reads the unwrap and the parsers out of the text of the query, quoted strings included.
	if textReadAsClause(log) || textHas(log, "| unpack_") || readsUnwrapOrParser(rest, log, true) {
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

// readsUnwrapOrParser reports whether the log query holds text the string translator reads as a parser (its regexp,
// hasParserPipeline, looks at quoted strings too) or, when unwrap is set, as an unwrap stage (its search for
// `| unwrap `).
// The printed query may not hold the client's value (a value that is not UTF-8), so the selector and the line
// filters are read as well.
func readsUnwrapOrParser(sel *queryplan.LogSelect, m metricLog, unwrap bool) bool {
	if textHasUnwrapOrParser(m.head, unwrap) || textHasUnwrapOrParser(m.restore, unwrap) || textHasUnwrapOrParser(m.pipes, unwrap) {
		return true
	}
	for _, mt := range sel.Matchers {
		if textHasUnwrapOrParser(mt.Value, unwrap) {
			return true
		}
	}
	for _, st := range sel.Stages {
		switch st := st.(type) {
		case *queryplan.LineFilter:
			if textHasUnwrapOrParser(st.Value, unwrap) {
				return true
			}
			for _, alt := range st.Or {
				if textHasUnwrapOrParser(alt.Value, unwrap) {
					return true
				}
			}
		}
	}
	return false
}

// textHasUnwrapOrParser reports whether s holds `| unwrap ` (when unwrap is set) or a pipe followed (after blanks) by json, logfmt,
// pattern, regexp or unpack at a word boundary.
func textHasUnwrapOrParser(s string, unwrap bool) bool {
	if !strings.Contains(s, "|") {
		return false
	}
	if unwrap && strings.Contains(s, "| unwrap ") {
		return true
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
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
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
}

// stats appends a stats pipe over by (none: no grouping; empty: an explicit `by ()`) as the string translator writes
// it: a grouping by the stream and its level also groups by detected_level.
func (q *unwrapQuery) stats(by []string, empty bool, fn logsql.StatsFunc, alias string) {
	keys := make([]logsql.GroupKey, len(by), len(by)+1)
	for i, f := range by {
		keys[i] = logsql.GroupKey{Field: f}
		q.chain = q.chain || f == translate.DetectedLevelLabel
		q.service = q.service || f == "service_name"
	}
	if hasField(by, "_stream") && hasField(by, "level") && !hasField(by, translate.DetectedLevelLabel) {
		keys = append(keys, logsql.GroupKey{Field: translate.DetectedLevelLabel})
		q.chain = true
	}
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
	var q unwrapQuery
	switch {
	case rate:
		q.rate(op.fn, seconds)
	case op.row:
		// A function alone groups by the stream and its level, unless the clause says otherwise.
		if len(by) == 0 && !empty {
			by = []string{"_stream", "level"}
		}
		q.stats(by, empty, op.fn, "__lvp_inner")
		q.pipes = append(q.pipes, firstLastUnpack)
		q.stats(by, empty, logsql.Max{Field: "__lvp_inner"}, "")
	default:
		q.stats(by, empty, op.fn, "")
	}
	return q.op(op, log, u.Label, conv, prof), nil
}

// squared is the right operand of the power the proxy takes of a deviation to answer stdvar_over_time.
var squared = physical.ScalarOp{V: 2, Text: "2"}

// op is the lowered query: the text, and for stdvar_over_time the deviation it computes raised to the power of 2
// by the proxy (a binary expression).
func (q *unwrapQuery) op(op unwrapOp, log metricLog, field, conv string, prof Profile) physical.Op {
	text := physical.VLQuery{Text: q.text(log, field, conv, prof)}
	if op.variance && !q.squares {
		return physical.Binary{Op: "^", L: text, R: squared}
	}
	return text
}

// rate appends the stats pipes of a rate over an unwrapped label: the values summed per series (the stream, its
// stored level and the detected_level Loki attaches), divided by the range's seconds, and summed again.
func (q *unwrapQuery) rate(fn logsql.StatsFunc, seconds float64) {
	pipes := translate.RateStatsPipesOf(fn, seconds)
	q.chain = true
	for _, p := range pipes {
		q.pipes = append(q.pipes, p.String())
	}
}

// text is the query: the log query with the derivations its groupings need, the unwrap gate and the stats pipes.
func (q *unwrapQuery) text(log metricLog, field, conv string, prof Profile) string {
	chain, service := "", ""
	if q.chain {
		chain = translate.RateDetectedLevelChain()
	}
	if q.service {
		service = translate.ServiceNameGroupingPipes(prof.Caps)
	}
	gate := translate.UnwrapGateFor(field, conv)
	n := 0
	for _, p := range q.pipes {
		n += len(p) + 1
	}
	var b strings.Builder
	log.write(&b, chain, service, len(gate)+n)
	b.WriteString(gate)
	for _, p := range q.pipes {
		b.WriteByte(' ')
		b.WriteString(p)
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
	switch va.Op {
	case logql.VectorSum, logql.VectorCount, logql.VectorMin, logql.VectorMax, logql.VectorAvg:
	default:
		return nil, &NotLoweredError{Construct: "VectorAgg " + string(va.Op)}
	}
	if ra.By != nil {
		return nil, &NotLoweredError{Construct: "VectorAgg grouping"}
	}
	rate := ra.Op == logql.RangeRate
	op, ok := unwrapFunc(ra)
	switch {
	case rate:
		op, ok = unwrapOp{fn: logsql.Sum{Field: translate.UnwrapValueAlias}}, true
	case !ok:
		return nil, &NotLoweredError{Construct: "VectorAgg unwrap " + string(ra.Op)}
	}
	seconds := 0.0
	if rate {
		if seconds = ra.Range.Seconds(); seconds <= 0 {
			return nil, &NotLoweredError{Construct: "VectorAgg range"}
		}
	}
	labels, raw, without, err := vectorClause(va, prof)
	if err != nil {
		return nil, err
	}
	log, u, err := unwrapLog(ra.In, prof, "VectorAgg")
	if err != nil {
		return nil, err
	}
	conv, ok := unwrapConv(u)
	if !ok {
		return nil, &NotLoweredError{Construct: "VectorAgg unwrap conversion"}
	}
	// The translator reads a clause and a stats pipe out of the text of the query, quoted strings included.
	if (without || len(labels) > 0) && textReadAsClause(log) {
		return nil, &NotLoweredError{Construct: "VectorAgg quoted clause"}
	}
	// Stripping a without clause, the translator also folds every run of blanks in the query into one.
	if without && (textHas(log, "  ") || selectionHas(ra.In, "  ")) {
		return nil, &NotLoweredError{Construct: "VectorAgg quoted clause"}
	}
	var q unwrapQuery
	if rate {
		q.vectorRate(va.Op, op.fn, seconds, labels, without)
	} else {
		q.vector(va.Op, op, labels, without)
	}
	out := q.op(op, log, u.Label, conv, prof)
	if without {
		out = physical.Fold{Levels: []physical.FoldLevel{{Op: string(va.Op), Labels: raw, Without: true}}, In: out}
	}
	return out, nil
}

// vectorClause reads the by or without clause of a vector aggregation: the labels as VictoriaLogs names them (for a
// by clause), the labels as written (for a without clause) and whether it is a without.
func vectorClause(va *queryplan.VectorAgg, prof Profile) (labels, raw []string, without bool, err error) {
	g := va.By
	if g == nil {
		return nil, nil, false, nil
	}
	if len(g.Labels) > 0 {
		labels = translate.NormalizeByLabels(g.Labels, prof.Label)
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
func (q *unwrapQuery) vectorRate(op logql.VectorOp, fn logsql.StatsFunc, seconds float64, labels []string, without bool) {
	for _, p := range vectorStats(op, fn, true, seconds, labels, without) {
		if st, ok := p.(logsql.PipeStats); ok {
			q.chain = q.chain || groupsBy(st, translate.DetectedLevelLabel)
			q.service = q.service || groupsBy(st, "service_name")
		}
		q.pipes = append(q.pipes, p.String())
	}
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
	identity := []string{"_stream"}
	if !op.row {
		identity = []string{"_stream", "level"}
	}
	switch {
	case perSeries && len(labels) > 0:
		q.seriesStats(op, translate.JoinByLabels(labels, identity))
		q.stats(labels, false, outerFunc(vop), "")
	case len(labels) > 0:
		q.stats(labels, false, op.fn, "")
	default:
		q.seriesStats(op, []string{"_stream"})
		if without {
			q.stats([]string{"_stream"}, false, outerFunc(vop), "")
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
	q.stats([]string{"_stream"}, false, op.fn, "__lvp_inner")
	q.pipes = append(q.pipes, logsql.PipeMath{Alias: "__lvp_inner_var", Expr: "__lvp_inner*__lvp_inner"}.String())
	q.squares = true
	var by []string
	if without {
		by = []string{"_stream"}
	}
	q.stats(by, false, outerFuncOf(vop, "__lvp_inner_var"), "")
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
	q.stats([]string{"_stream"}, false, op.fn, "__lvp_inner")
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
