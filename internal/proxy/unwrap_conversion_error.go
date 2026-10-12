package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/jsonparser"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Unwrap conversion errors (Loki v3.7.7).
//
// pkg/logql/log/metrics_extraction.go:202-231 (streamLabelSampleExtractor.Process)
// skips a line whose unwrapped label is absent or empty, and marks a value its
// conversion rejects (convertFloat, convertDuration, convertBytes; lines
// 316-338) with __error__="SampleExtractionErr" and the conversion's own
// message as __error_details__. The label filters after the unwrap run next,
// with the error set: a string filter on __error__ compares
// "SampleExtractionErr" (label_filter.go:418-424 labelValue), one on
// __error_details__ compares "" (the details are not a label the builder
// returns). A sample that keeps __error__ fails the whole query when a range
// vector holds it (pkg/logql/evaluator.go:726-733, RangeVectorEvaluator.Next):
// HTTP 400, text/plain (pkg/util/server/error.go:46-51), with
// logqlmodel.PipelineError.Error() over the sample's series, which is every
// label of the line (labels.go:664-668 GroupedLabels returns the whole set when
// an error is set). __preserve_error__ only exempts parser errors
// (parser.go:778-787), so neither `drop __error__` nor a filter before the
// unwrap, nor `by (__error__)`, intercepts the conversion error.
//
// VictoriaLogs' stats only see the rows the translator's unwrap gate keeps
// (translator.UnwrapGateFor). The detection rides on the metric's own
// VictoriaLogs queries instead of scanning the rows again: a stats query that
// carries the gate is sent with the rows whose unwrapped value is present and
// outside every form the conversion may accept moved into a group of their own
// (unwrapCountingQuery); that group is taken out of the answer before any
// caller reads it, and the buckets it held are remembered. The metrics that
// read raw rows (quantile, first, last, rate_counter, bare parsers) report the
// rows they cannot convert. Only after such a hit, one `| limit` lookup
// restricted to the hit's bucket reads the row, re-derives the unwrapped value
// with Loki's own parser from the stored row (unwrapLokiValue: VictoriaLogs
// unpacks arrays, booleans and keys named like stored fields differently), and
// answers Loki's error only when Loki's value fails Loki's conversion.

// unwrapErrorProbe describes one unwrap range aggregation whose conversion
// errors reach the result.
type unwrapErrorProbe struct {
	label  string // the unwrapped LogQL label
	conv   string // "", "duration" or "bytes"
	window time.Duration
	// pipeline is the log pipeline before the unwrap and post the label filters
	// after it, to re-derive Loki's value and answer.
	pipeline, post []logqlpkg.Stage
	// hints are the labels Loki's parser extracts when the sample extractor
	// has parser hints (a `by` or singleton grouping); nil when it extracts all.
	hints map[string]bool
}

// unwrapErrorProbes returns the unwrap range aggregations in expr whose
// conversion errors reach the result, in evaluation order.
func unwrapErrorProbes(expr logqlpkg.Expr) []unwrapErrorProbe {
	var out []unwrapErrorProbe
	var walk func(e logqlpkg.Expr, parent *logqlpkg.VectorAggregation)
	walk = func(e logqlpkg.Expr, parent *logqlpkg.VectorAggregation) {
		switch n := e.(type) {
		case *logqlpkg.VectorAggregation:
			walk(n.Inner, n)
		case *logqlpkg.BinOpExpr:
			walk(n.Left, nil)
			walk(n.Right, nil)
		case *logqlpkg.RangeAggregation:
			if probe, ok := unwrapErrorProbeFor(n, parent); ok {
				out = append(out, probe)
			}
		}
	}
	walk(expr, nil)
	return out
}

// unwrapErrorProbeFor describes one range aggregation. ok is false when it has
// no unwrap, when its post filters intercept the error, and when a post filter
// is one the detection cannot evaluate (the query then answers as before; see
// semantics/unwrap-conversion-error-postfilter-forms).
func unwrapErrorProbeFor(ra *logqlpkg.RangeAggregation, parent *logqlpkg.VectorAggregation) (unwrapErrorProbe, bool) {
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok || ra.Offset != "" {
		return unwrapErrorProbe{}, false
	}
	window, ok := parsePositiveStepDuration(ra.Range)
	if !ok {
		return unwrapErrorProbe{}, false
	}
	at := -1
	var unwrap *logqlpkg.UnwrapStage
	for i, stage := range lq.Pipeline {
		if u, isUnwrap := stage.(*logqlpkg.UnwrapStage); isUnwrap {
			at, unwrap = i, u
			break
		}
	}
	if unwrap == nil || unwrap.Label == "" {
		return unwrapErrorProbe{}, false
	}
	conv := ""
	switch unwrap.Converter {
	case "":
	case "duration", "duration_seconds":
		// Loki converts both with convertDuration (syntax/extractor.go:63-71).
		conv = "duration"
	case "bytes":
		conv = "bytes"
	default:
		return unwrapErrorProbe{}, false
	}
	fails, known := unwrapErrorPostFilters(lq.Pipeline[at+1:])
	if !known || !fails {
		return unwrapErrorProbe{}, false
	}
	return unwrapErrorProbe{
		label:    unwrap.Label,
		conv:     conv,
		window:   window,
		pipeline: lq.Pipeline[:at],
		post:     lq.Pipeline[at+1:],
		hints:    unwrapParserHints(ra, parent, lq.Pipeline, unwrap.Label),
	}, true
}

// unwrapErrorPostFilters evaluates the label filters after an unwrap for a
// sample marked SampleExtractionErr. fails is false when a filter drops every
// such sample; known is false for a filter the detection cannot evaluate. The
// metric's VictoriaLogs query applies the post filters itself (the translator
// moves them before the gate), so a filter is known only when VictoriaLogs
// gives a failing row the answer Loki gives it: a string filter on another
// label, a filter on __error__ that also keeps the empty value (VictoriaLogs
// rows have no __error__), any filter on __error_details__ (Loki compares the
// empty value there too).
func unwrapErrorPostFilters(post []logqlpkg.Stage) (fails, known bool) {
	for _, stage := range post {
		lf, ok := stage.(*logqlpkg.LabelFilterStage)
		if !ok {
			return false, false
		}
		compiled, valid := compileOrderedJSONStage(lf)
		if !valid || compiled.filter == nil {
			return false, false // a comparison, an and/or expression, ip()
		}
		switch compiled.filter.Field {
		case "__error__":
			if !compiled.filter.Matches("SampleExtractionErr") {
				return false, true
			}
			if !compiled.filter.Matches("") {
				return false, false // VictoriaLogs drops every row; Loki keeps the failing ones
			}
		case "__error_details__":
			if !compiled.filter.Matches("") {
				return false, true
			}
		}
	}
	return true, true
}

// unwrapComparisonRE reads a comparison label filter (`latency > 250ms`).
var unwrapComparisonRE = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(==|!=|>=|<=|>|<|=)\s*([^\s"'` + "`" + `(),]+)\s*$`)

// unwrapParserHints returns the labels Loki's parser extracts for the
// aggregation's sample extractor, or nil when it extracts every label
// (pkg/logql/log/parser_hints.go:145-189 NewParserHint; a `sum` pushes its
// grouping down for sum_over_time and rate, syntax/ast.go:1612-1642;
// absent_over_time never keeps labels, syntax/extractor.go:44-48).
func unwrapParserHints(ra *logqlpkg.RangeAggregation, parent *logqlpkg.VectorAggregation, pipeline []logqlpkg.Stage, label string) map[string]bool {
	grouping := ra.Grouping
	if grouping == nil && parent != nil && parent.Op == logqlpkg.VectorSum &&
		(ra.Op == logqlpkg.RangeSumOverTime || ra.Op == logqlpkg.RangeRate) {
		grouping = parent.Grouping
		if grouping == nil {
			grouping = &logqlpkg.Grouping{}
		}
	}
	if ra.Op == logqlpkg.RangeAbsentOverTime {
		grouping = &logqlpkg.Grouping{}
	}
	if grouping == nil || grouping.Without {
		return nil
	}
	// appendLabelHints (parser_hints.go:207-214): a name, and for a name ending
	// in _extracted also the name without the suffix; never the other way.
	hints := map[string]bool{}
	add := func(name string) {
		hints[name] = true
		if base, ok := strings.CutSuffix(name, "_extracted"); ok {
			hints[base] = true
		}
	}
	add(label)
	for _, name := range grouping.Labels {
		add(name)
	}
	for _, stage := range pipeline {
		lf, ok := stage.(*logqlpkg.LabelFilterStage)
		if !ok {
			continue
		}
		if compiled, valid := compileOrderedJSONStage(lf); valid && compiled.filter != nil {
			add(compiled.filter.Field)
		} else if m := unwrapComparisonRE.FindStringSubmatch(lf.Raw); m != nil {
			add(m[1])
		}
	}
	return hints
}

// unwrapConvertibleForms holds, per conversion, the values the gate drops that
// the conversion may still accept (registered in
// semantics/unwrap-gate-parsefloat-divergences): nan. The detection skips them
// too, so a row it counts always fails the conversion. A value that is
// syntactically a number but overflows (1e309, 0x1p2000, 2562048h, 19EB) is not
// one of them: the gate lets it through to the range filter after the math pipe
// (translator.UnwrapInRange), which the counting gate turns into a flag.
var unwrapConvertibleForms = map[string]string{
	"": unwrapNaNPattern,
}

// unwrapAcceptablePattern is the expression of the values the conversion conv
// may accept by syntax: the gate's own pattern and the forms it drops.
func unwrapAcceptablePattern(conv string) string {
	pattern := "(?:" + translator.UnwrapPattern(conv) + ")"
	if extra := unwrapConvertibleForms[conv]; extra != "" {
		pattern += "|(?:" + extra + ")"
	}
	return pattern
}

// unwrapLookupQuery is the query that reads the rows of the metric's base query
// whose unwrapped value the conversion conv rejects: the counting gate flags a
// value outside the accepted syntax and one that overflows, so both are found.
func unwrapLookupQuery(base, field, conv string) string {
	return base + unwrapCountedGate(field, conv) + " | filter " + unwrapBadField + `:="1"`
}

const unwrapBadField = "__lvp_bad"

var unwrapGateFieldRE = regexp.MustCompile(`^ \| filter ("(?:[^"\\]|\\.)*"):~`)

// unwrapCountingQuery rewrites a stats query that carries an unwrap gate so the
// rows the conversion rejects are counted in the same pass: they are flagged
// (format if ... as __lvp_bad), let through the gate, given the value 0 and
// grouped apart in every stats pipe, so the other groups aggregate exactly the
// rows they did. ok is false for a query without the gate, with a pipe after
// the stats that could mix or drop the flagged groups, or whose (field, conv)
// accept rejects. base is the query before the gate (the metric's rows).
func unwrapCountingQuery(query string, accept func(field, conv string) bool) (rewritten, base, field, conv string, ok bool) {
	rewritten, base, field, conv, _, ok = unwrapCountingRewrite(query, func(f, c string) (bool, bool) { a := accept(f, c); return a, a })
	return
}

// unwrapCountingRewrite is unwrapCountingQuery where accept says, per gate, whether
// the rows the conversion rejects are counted (errors) and whether the gate is
// rewritten at all (ok): a plain gate is also rewritten to count the nan values
// the gate drops (unwrapNaNCounted), whatever the query does with errors.
func unwrapCountingRewrite(query string, accept func(field, conv string) (errors, ok bool)) (rewritten, base, field, conv string, errors, ok bool) {
	idx := -1
	for from := 0; ; {
		at := strings.Index(query[from:], ` | filter "`)
		if at < 0 {
			break
		}
		at += from
		from = at + 1
		m := unwrapGateFieldRE.FindStringSubmatch(query[at:])
		if m == nil {
			continue
		}
		f, err := strconv.Unquote(m[1])
		if err != nil {
			continue
		}
		for _, c := range []string{"", "duration", "bytes"} {
			if strings.HasPrefix(query[at:], translator.UnwrapGateFor(f, c)) {
				if idx >= 0 {
					return query, "", "", "", false, false // two gates: not a shape this rewrite knows
				}
				idx, field, conv = at, f, c
				break
			}
		}
	}
	if idx < 0 {
		return query, "", "", "", false, false
	}
	if errors, ok = accept(field, conv); !ok {
		return query, "", "", "", false, false
	}
	gate := translator.UnwrapGateFor(field, conv)
	after := query[idx+len(gate):]
	statsAt := strings.Index(after, " | stats ")
	if statsAt < 0 {
		return query, "", "", "", false, false
	}
	pipes := strings.Split(after[statsAt+len(" | "):], " | ")
	for i, pipe := range pipes {
		switch {
		case strings.HasPrefix(pipe, "stats by ("):
			end := strings.Index(pipe, ")")
			if end < 0 {
				return query, "", "", "", false, false
			}
			if inner := strings.TrimSpace(pipe[len("stats by ("):end]); inner == "" {
				pipes[i] = "stats by (" + unwrapBadField + pipe[end:]
			} else {
				pipes[i] = pipe[:end] + ", " + unwrapBadField + pipe[end:]
			}
		case strings.HasPrefix(pipe, "stats "):
			pipes[i] = "stats by (" + unwrapBadField + ") " + pipe[len("stats "):]
		case strings.HasPrefix(pipe, "math "), strings.HasPrefix(pipe, "filter "):
		default:
			return query, "", "", "", false, false // sort, limit, fields, ...: could mix or drop the flagged groups
		}
	}
	rewritten = query[:idx] + unwrapCountedGateMode(field, conv, errors) + after[:statsAt] + " | " + strings.Join(pipes, " | ")
	return rewritten, query[:idx], field, conv, errors, true
}

// unwrapCountedGate is the unwrap gate (translator.UnwrapGateFor) that also lets
// the rows the conversion rejects through: __lvp_bad is "0" for a row the gate
// keeps (one test of the gate's expression, as before) and "1" for a present
// value outside the forms the conversion may accept (tested only on the rows
// the gate refused) or whose converted value is out of range (an overflow:
// translator.UnwrapInRange), whose value is then 0. A plain gate also lets the
// nan values through, flagged "2" (unwrapNaNCounted).
func unwrapCountedGate(field, conv string) string { return unwrapCountedGateMode(field, conv, true) }

// unwrapNaNFlag marks a nan value in unwrapBadField; its value is 0.
const unwrapNaNFlag = "2"

// unwrapNaNPattern is the nan strconv.ParseFloat reads (case-insensitive, unsigned).
const unwrapNaNPattern = `^(?i:nan)$`

// unwrapCountedGateMode is unwrapCountedGate with the rejected values counted
// (errors) or, without, dropped as the plain gate does (a query whose
// `__error__=""` filter drops the error): a plain gate keeps counting the nan
// values, which the gate drops and VictoriaLogs' stats cannot carry
// (unwrapNaNCounted).
func unwrapCountedGateMode(field, conv string, errors bool) string {
	q := strconv.Quote(field)
	_, convert := translator.UnwrapGateParts(field, conv)
	bad := unwrapBadField
	nan := conv == ""
	gate := " | format if (" + q + ":~" + strconv.Quote(translator.UnwrapPattern(conv)) + `) "0" as ` + bad
	keep := bad + `:="0"`
	if nan {
		gate += " | format if (-" + bad + `:="0" ` + q + ":~" + strconv.Quote(unwrapNaNPattern) + `) "` + unwrapNaNFlag + `" as ` + bad
		keep += " or " + bad + `:="` + unwrapNaNFlag + `"`
	}
	if errors {
		rejected := q + ":*"
		if nan {
			rejected = "-" + bad + `:="` + unwrapNaNFlag + `" ` + rejected
		}
		gate += " | format if (-" + bad + `:="0" ` + rejected + `) "1" as ` + bad
		keep += " or " + bad + `:="1"`
	}
	gate += " | filter " + keep + convert
	inRange := translator.UnwrapInRange(conv)
	if errors {
		gate += " | format if (" + bad + `:="0" -(` + inRange + `)) "1" as ` + bad
	} else if nan {
		gate += " | filter " + bad + `:="` + unwrapNaNFlag + `" or (` + inRange + ")"
	} else {
		gate += " | filter " + inRange
	}
	// A flagged row's value is NaN (the math pipe reads no number in it): 0 instead,
	// so the stats keep a point for its group. The math pipe keeps the float column;
	// a format pipe would rewrite it as text, which the stats read only up to 27
	// characters (a value of 1e30 or 1e-30 would not count).
	return gate + " | math " + translator.UnwrapValueAlias + " default 0 as " + translator.UnwrapValueAlias
}

// unwrapHit is a span where a metric's VictoriaLogs query met a row whose
// unwrapped value the conversion rejects.
type unwrapHit struct {
	base, field, conv string    // the metric's rows before the gate, the VictoriaLogs field
	from, to          time.Time // zero: every evaluated window
}

// stripUnwrapCounter removes the flagged groups from a stats answer (matrix or
// vector) and returns the times of the buckets the rejected values (flag 1) held
// (seconds). An answer without a flagged group only loses the empty label,
// without decoding it. The nan groups (flag 2) are dropped, as the gate drops
// nan values.
func stripUnwrapCounter(body []byte) ([]byte, []float64, error) {
	return stripUnwrapFlags(body, nil)
}

// unwrapNaN says how the nan groups (flag 2) of a stats answer reach the result.
// Loki's sample is NaN (strconv.ParseFloat reads nan): sum, avg, stddev and
// stdvar over a window that holds one are NaN (pkg/logql/range_vector.go
// sumOverTime, avgOverTime, stddevOverTime), and so are the vector aggregations
// of those series; VictoriaLogs' sum restarts at a NaN, so its stats can only
// carry the rows apart (translator gate drops them, the counting gate groups
// them under flag 2). poison is set when every function of the query poisons its
// result; the value of each series that has a nan group at a time is then NaN
// (a series that only has nan samples included), and the nan group's row count
// is added to a count series (the presence of the sample). Otherwise the nan
// rows are dropped, which max and min over time do too (maxOverTime ignores a
// NaN unless every sample is).
type unwrapNaN struct {
	poison bool
	counts map[string]bool // aliases of the count functions, which add up
}

// unwrapNaNFor reads the stats pipes of a rewritten query.
func unwrapNaNFor(query string) *unwrapNaN {
	n := &unwrapNaN{poison: true, counts: map[string]bool{"__sample_count": true}}
	seen := false
	for _, pipe := range strings.Split(query, " | ") {
		if !strings.HasPrefix(pipe, "stats ") {
			continue
		}
		if end := strings.Index(pipe, ")"); strings.HasPrefix(pipe, "stats by (") && end > 0 {
			pipe = pipe[end+1:]
		} else {
			pipe = strings.TrimPrefix(pipe, "stats ")
		}
		for _, m := range unwrapStatsFuncRE.FindAllStringSubmatch(pipe, -1) {
			fn, expr, alias := m[1], m[0], m[2]
			if alias == "" {
				alias = strings.TrimSpace(expr)
			}
			if fn == "count" {
				// Only the sample count of a sum adds up: a count of series would count a
				// stream with clean and nan rows twice.
				n.counts[alias] = true
				n.poison = n.poison && alias == "__sample_count"
				continue
			}
			seen = true
			switch fn {
			case "sum", "avg", "stddev":
			default:
				n.poison = false
			}
		}
	}
	n.poison = n.poison && seen
	return n
}

var unwrapStatsFuncRE = regexp.MustCompile(`(\w+)\([^()]*\)(?: as (\w+))?`)

type unwrapPoint struct {
	ts, value json.RawMessage
}

// stripUnwrapFlags is stripUnwrapCounter with the nan groups applied to the
// answer (nan nil: dropped).
func stripUnwrapFlags(body []byte, nan *unwrapNaN) ([]byte, []float64, error) {
	flagged := func(flag string) []byte { return []byte(`"` + unwrapBadField + `":"` + flag + `"`) }
	if !bytes.Contains(body, flagged("1")) && !bytes.Contains(body, flagged(unwrapNaNFlag)) {
		kept := string(flagged("0"))
		out := bytes.ReplaceAll(body, []byte(","+kept), nil)
		out = bytes.ReplaceAll(out, []byte(kept+","), nil)
		return bytes.ReplaceAll(out, []byte(kept), nil), nil, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, nil, err
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(top["data"], &data); err != nil {
		return nil, nil, err
	}
	var result []map[string]json.RawMessage
	if raw, ok := data["result"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, nil, err
		}
	}
	type nanGroup struct {
		metric map[string]string
		points []unwrapPoint
	}
	var times []float64
	var nans []nanGroup
	kept := result[:0]
	for _, series := range result {
		var metric map[string]string
		if err := json.Unmarshal(series["metric"], &metric); err != nil {
			return nil, nil, err
		}
		flag, isFlagged := metric[unwrapBadField]
		if isFlagged && (flag == "1" || flag == unwrapNaNFlag) {
			var points []unwrapPoint
			if raw, ok := series["values"]; ok {
				var rows [][]json.RawMessage
				_ = json.Unmarshal(raw, &rows)
				for _, row := range rows {
					if len(row) == 2 {
						points = append(points, unwrapPoint{row[0], row[1]})
					}
				}
			}
			if raw, ok := series["value"]; ok {
				var row []json.RawMessage
				if json.Unmarshal(raw, &row) == nil && len(row) == 2 {
					points = append(points, unwrapPoint{row[0], row[1]})
				}
			}
			if flag == unwrapNaNFlag {
				delete(metric, unwrapBadField)
				nans = append(nans, nanGroup{metric, points})
				continue
			}
			for _, point := range points {
				var ts float64
				if json.Unmarshal(point.ts, &ts) == nil {
					times = append(times, ts)
				}
			}
			continue
		}
		if isFlagged {
			delete(metric, unwrapBadField)
			encoded, err := json.Marshal(metric)
			if err != nil {
				return nil, nil, err
			}
			series["metric"] = encoded
		}
		kept = append(kept, series)
	}
	if nan != nil && nan.poison {
		vector := string(data["resultType"]) == `"vector"`
		for _, group := range nans {
			if err := nan.apply(&kept, group.metric, group.points, vector); err != nil {
				return nil, nil, err
			}
		}
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return nil, nil, err
	}
	data["result"] = encoded
	if top["data"], err = json.Marshal(data); err != nil {
		return nil, nil, err
	}
	out, err := json.Marshal(top)
	return out, times, err
}

// unwrapSeriesKey identifies a series of a stats answer by its labels.
func unwrapSeriesKey(metric map[string]string) string {
	keys := make([]string, 0, len(metric))
	for k := range metric {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(strconv.Quote(k) + "=" + strconv.Quote(metric[k]) + ",")
	}
	return b.String()
}

// apply sets the points of a nan group onto the series of the answer with the
// same labels (a count series adds the group's count): the value is NaN where
// Loki's is, the series is added when only nan samples made it.
func (n *unwrapNaN) apply(kept *[]map[string]json.RawMessage, metric map[string]string, points []unwrapPoint, vector bool) error {
	key := unwrapSeriesKey(metric)
	index := -1
	for i, series := range *kept {
		var m map[string]string
		if err := json.Unmarshal(series["metric"], &m); err != nil {
			return err
		}
		if unwrapSeriesKey(m) == key {
			index = i
			break
		}
	}
	isCount := n.counts[metric["__name__"]]
	var target map[string]json.RawMessage
	var current []unwrapPoint
	instant := vector
	if index >= 0 {
		target = (*kept)[index]
		if raw, ok := target["values"]; ok {
			var rows [][]json.RawMessage
			if err := json.Unmarshal(raw, &rows); err != nil {
				return err
			}
			for _, row := range rows {
				if len(row) == 2 {
					current = append(current, unwrapPoint{row[0], row[1]})
				}
			}
		} else if raw, ok := target["value"]; ok {
			var row []json.RawMessage
			if err := json.Unmarshal(raw, &row); err != nil {
				return err
			}
			if len(row) == 2 {
				current = []unwrapPoint{{row[0], row[1]}}
			}
			instant = true
		} else {
			instant = false
		}
	} else {
		encoded, err := json.Marshal(metric)
		if err != nil {
			return err
		}
		target = map[string]json.RawMessage{"metric": encoded}
	}
	for _, point := range points {
		at := -1
		for i, c := range current {
			if string(c.ts) == string(point.ts) {
				at = i
				break
			}
		}
		value := json.RawMessage(`"NaN"`)
		if isCount {
			var add, have float64
			_ = json.Unmarshal(unwrapUnquote(point.value), &add)
			if at >= 0 {
				_ = json.Unmarshal(unwrapUnquote(current[at].value), &have)
			}
			value = json.RawMessage(`"` + strconv.FormatFloat(have+add, 'f', -1, 64) + `"`)
		}
		if at >= 0 {
			current[at].value = value
		} else {
			current = append(current, unwrapPoint{point.ts, value})
		}
	}
	sort.SliceStable(current, func(i, j int) bool {
		var a, b float64
		_ = json.Unmarshal(current[i].ts, &a)
		_ = json.Unmarshal(current[j].ts, &b)
		return a < b
	})
	rows := make([][]json.RawMessage, len(current))
	for i, c := range current {
		rows[i] = []json.RawMessage{c.ts, c.value}
	}
	var err error
	if instant && len(rows) > 0 {
		target["value"], err = json.Marshal(rows[0])
	} else {
		target["values"], err = json.Marshal(rows)
	}
	if err != nil {
		return err
	}
	if index < 0 {
		*kept = append(*kept, target)
	}
	return nil
}

// unwrapUnquote turns a JSON string number into the number.
func unwrapUnquote(raw json.RawMessage) json.RawMessage {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return json.RawMessage(s)
	}
	return raw
}

// parseVLStepParam reads a VictoriaLogs step argument ("60s", "1m", "60").
func parseVLStepParam(raw string) time.Duration {
	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second))
	}
	return 0
}

type unwrapCheckKey struct{}

// unwrapCheck is the conversion-error detection of one Loki request: which
// unwrap aggregations fail on a rejected value, Loki's evaluation, and the
// spans the metric's own VictoriaLogs queries met such a value in.
type unwrapCheck struct {
	probes     []unwrapErrorProbe
	vlName     func(string) string
	instant    bool
	start, end time.Time
	step       time.Duration

	mu      sync.Mutex
	hits    []unwrapHit
	rawHits int

	writer *unwrapLookupWriter
}

func unwrapCheckFrom(ctx context.Context) *unwrapCheck {
	c, _ := ctx.Value(unwrapCheckKey{}).(*unwrapCheck)
	return c
}

// accepts reports whether some checked aggregation unwraps field (a VictoriaLogs
// field) with conv.
func (c *unwrapCheck) accepts(field, conv string) bool {
	return len(c.probesFor(field, conv)) > 0
}

func (c *unwrapCheck) probesFor(field, conv string) []unwrapErrorProbe {
	var out []unwrapErrorProbe
	for _, probe := range c.probes {
		if probe.conv == conv && (probe.label == field || (c.vlName != nil && c.vlName(probe.label) == field)) {
			out = append(out, probe)
		}
	}
	return out
}

func (c *unwrapCheck) addHit(h unwrapHit) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.hits) < 4096 {
		c.hits = append(c.hits, h)
	}
}

// countingStatsQuery is the hook of the VictoriaLogs client: the query to
// send for path and a function that strips the counter off a 200 answer.
func (c *unwrapCheck) countingStatsQuery(path string, params url.Values) (url.Values, func([]byte) ([]byte, error), bool) {
	if c == nil || (path != "/select/logsql/stats_query_range" && path != "/select/logsql/stats_query") {
		return params, nil, false
	}
	rewritten, base, field, conv, _, ok := unwrapCountingRewrite(params.Get("query"), func(field, conv string) (bool, bool) {
		errors := c.accepts(field, conv)
		return errors, errors || conv == ""
	})
	if !ok {
		return params, nil, false
	}
	var nan *unwrapNaN
	if conv == "" {
		nan = unwrapNaNFor(rewritten)
	}
	sent := url.Values{}
	for k, v := range params {
		sent[k] = append([]string(nil), v...)
	}
	sent.Set("query", rewritten)
	step := parseVLStepParam(params.Get("step"))
	strip := func(body []byte) ([]byte, error) {
		out, times, err := stripUnwrapFlags(body, nan)
		if err != nil {
			return nil, err
		}
		for _, ts := range times {
			h := unwrapHit{base: base, field: field, conv: conv}
			if path == "/select/logsql/stats_query_range" && step > 0 {
				// A bucket labelled T holds [T, T+step) or, with the offset
				// argument, (T, T+step]: read a step on either side.
				t := time.Unix(0, int64(ts*float64(time.Second)))
				h.from, h.to = t.Add(-step), t.Add(2*step)
			}
			c.addHit(h)
		}
		return out, nil
	}
	return sent, strip, true
}

// rawRowRejected records a raw row a metric evaluator read and could not
// convert: for each checked aggregation whose field (read by value) holds a
// present value its conversion rejects, a hit at the row's time.
func (c *unwrapCheck) rawRowRejected(base string, ts int64, value func(field string) string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	full := c.rawHits >= unwrapMaxRawHits
	c.mu.Unlock()
	if full {
		return
	}
	t := time.Unix(0, ts)
	for _, probe := range c.probes {
		for _, field := range c.fieldNames(probe) {
			v := value(field)
			if v == "" {
				continue
			}
			if _, err := convertUnwrap(v, probe.conv); err != nil {
				c.mu.Lock()
				c.rawHits++
				c.mu.Unlock()
				c.addHit(unwrapHit{base: base, field: field, conv: probe.conv, from: t, to: t.Add(time.Nanosecond)})
				return
			}
		}
	}
}

// fieldNames are the VictoriaLogs fields that hold probe's unwrapped label.
func (c *unwrapCheck) fieldNames(probe unwrapErrorProbe) []string {
	names := []string{probe.label}
	if c.vlName != nil {
		if vl := c.vlName(probe.label); vl != probe.label {
			names = append(names, vl)
		}
	}
	return names
}

// unwrapMaxRawHits bounds the rows a raw evaluator records for confirmation.
const unwrapMaxRawHits = 64

// unwrapMaxLookups bounds the work spent confirming hits: at most this many
// lookups of three rows, each row's stored line re-read side by side. A query
// VictoriaLogs flags but Loki answers (an array VictoriaLogs renders as text, a
// duplicate logfmt key) pays this bound on every request.
const unwrapMaxLookups = 4

// confirm returns Loki's error for the first hit whose row Loki fails on, or
// nil. Each lookup reads at most three rows of one hit's span.
func (c *unwrapCheck) confirm(ctx context.Context, p *Proxy) *orderedJSONPipelineError {
	c.mu.Lock()
	hits := append([]unwrapHit(nil), c.hits...)
	c.mu.Unlock()
	if len(hits) == 0 {
		return nil
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].from.Before(hits[j].from) })
	seen := map[string]bool{}
	lookups := 0
	for _, h := range hits {
		for _, probe := range c.probesFor(h.field, h.conv) {
			from, to, last := c.windows(probe)
			if !h.from.IsZero() {
				if h.from.After(from) {
					from = h.from
				}
				if h.to.Before(to) {
					to = h.to
				}
			}
			if !to.After(from) {
				continue
			}
			query := unwrapLookupQuery(h.base, h.field, h.conv)
			if !c.instant && c.step > probe.window {
				query += windowPhaseFilter(c.start, c.step, probe.window)
			}
			query += " | limit 3"
			key := query + from.String() + to.String()
			if seen[key] {
				continue
			}
			seen[key] = true
			if lookups++; lookups > unwrapMaxLookups {
				return nil
			}
			rows, err := p.unwrapLookupRows(ctx, query, from, to)
			if err != nil {
				p.log.Warn("unwrap conversion lookup failed", "error", err)
				continue
			}
			// The stored rows are re-read side by side; the first row in the
			// lookup's order that Loki fails on answers.
			found := make([]*orderedJSONPipelineError, len(rows))
			var wg sync.WaitGroup
			for i, row := range rows {
				ts, ok := parseFlexibleUnixNanos(row["_time"])
				if !ok || !c.visible(ts, probe, last) {
					continue
				}
				wg.Add(1)
				go func(i int, row map[string]string) {
					defer wg.Done()
					found[i] = p.unwrapRowError(ctx, probe, row, h.field)
				}(i, row)
			}
			wg.Wait()
			for _, pipelineErr := range found {
				if pipelineErr != nil {
					return pipelineErr
				}
			}
		}
	}
	return nil
}

// windows returns the range Loki evaluates for probe, as VictoriaLogs bounds
// [from, to), and the last evaluation time.
func (c *unwrapCheck) windows(probe unwrapErrorProbe) (from, to, last time.Time) {
	last = c.start
	if !c.instant && c.step > 0 && !c.end.Before(c.start) {
		last = c.start.Add(c.end.Sub(c.start) / c.step * c.step)
	}
	// Loki's window is left-open, right-closed; VictoriaLogs' end is exclusive.
	return c.start.Add(-probe.window).Add(time.Nanosecond), last.Add(time.Nanosecond), last
}

func (c *unwrapCheck) visible(ts int64, probe unwrapErrorProbe, last time.Time) bool {
	if c.instant || c.step <= 0 {
		return ts > c.start.Add(-probe.window).UnixNano() && ts <= c.start.UnixNano()
	}
	return orderedJSONSampleVisible(ts, c.start.UnixNano(), last.UnixNano(), int64(c.step), int64(probe.window))
}

// unwrapLookupRows runs a lookup over [from, to).
func (p *Proxy) unwrapLookupRows(ctx context.Context, query string, from, to time.Time) ([]map[string]string, error) {
	params := url.Values{"query": {query}, "start": {from.UTC().Format(time.RFC3339Nano)}, "end": {to.UTC().Format(time.RFC3339Nano)}}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("backend returned", resp.StatusCode, body)
	}
	var rows []map[string]string
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	for scanner.Scan() && len(rows) < 64 {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var row map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, fmt.Errorf("invalid backend unwrap row: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, scanner.Err()
}

// unwrapStoredRow reads the row as stored (before any parser) by its stream id,
// time and, when no line_format rewrote it, its exact line; nil when the rows
// found are not one line (identical copies count as one).
func (p *Proxy) unwrapStoredRow(ctx context.Context, probe unwrapErrorProbe, row map[string]string) (map[string]string, error) {
	id, at := row["_stream_id"], row["_time"]
	ts, ok := parseFlexibleUnixNanos(at)
	if id == "" || !ok {
		return nil, nil
	}
	query := "_stream_id:" + strconv.Quote(id)
	formatted := false
	for _, stage := range probe.pipeline {
		if _, ok := stage.(*logqlpkg.LineFormatStage); ok {
			formatted = true
		}
	}
	if !formatted {
		query += " _msg:=" + strconv.Quote(row["_msg"])
	}
	t := time.Unix(0, ts)
	rows, err := p.unwrapLookupRows(ctx, query+" | limit 64", t, t.Add(time.Nanosecond))
	if err != nil {
		return nil, err
	}
	var match []map[string]string
	for _, r := range rows {
		if rts, ok := parseFlexibleUnixNanos(r["_time"]); ok && rts == ts && (formatted || r["_msg"] == row["_msg"]) {
			match = append(match, r)
		}
	}
	if len(match) == 0 {
		return nil, nil
	}
	for _, other := range match[1:] {
		if !maps.Equal(other, match[0]) {
			return nil, nil
		}
	}
	return match[0], nil
}

// unwrapRowError is Loki's pipeline error for the row the lookup read, or nil
// when Loki makes no failing sample of it: Loki's value of the label is derived
// from the stored row with Loki's parsers, and only a value Loki's conversion
// rejects fails the query.
func (p *Proxy) unwrapRowError(ctx context.Context, probe unwrapErrorProbe, row map[string]string, field string) *orderedJSONPipelineError {
	stored, err := p.unwrapStoredRow(ctx, probe, row)
	if err != nil {
		p.log.Warn("unwrap conversion lookup failed", "error", err)
		return nil
	}
	if stored == nil {
		return nil
	}
	dropIngestUnpackedFields(stored)
	desc := p.logQueryStreamDescriptor(stored["_stream"], stored["level"], map[string]map[string]string{}, map[string]cachedLogQueryStreamDescriptor{})
	base, err := p.orderedJSONBaseLabels(stored, desc, map[string][]metadataFieldExposure{})
	if err != nil {
		return nil
	}
	return unwrapLokiError(probe, base, stored["_msg"], row["_msg"])
}

// dropIngestUnpackedFields removes from a stored row the fields VictoriaLogs
// unpacked from a JSON line at ingest (its Loki push API parses JSON lines and
// keeps the line in _msg): a field named like a key path of the line (joined
// with dots) holding the same value is the line's own key, not structured
// metadata Loki stores beside the line.
func dropIngestUnpackedFields(stored map[string]string) {
	msg := stored["_msg"]
	if !strings.HasPrefix(strings.TrimSpace(msg), "{") {
		return
	}
	var walk func(data []byte, prefix string, depth int)
	walk = func(data []byte, prefix string, depth int) {
		if depth > 16 {
			return
		}
		_ = jsonparser.ObjectEach(data, func(key, value []byte, kind jsonparser.ValueType, _ int) error {
			name := string(key)
			if prefix != "" {
				name = prefix + "." + name
			}
			switch kind {
			case jsonparser.Object:
				walk(value, name, depth+1)
			case jsonparser.String, jsonparser.Number, jsonparser.Boolean:
				if v, ok := stored[name]; ok && v == orderedJSONScalar(value, kind) {
					delete(stored, name)
				}
			}
			return nil
		})
	}
	walk([]byte(msg), "", 0)
}

// unwrapLokiError is Loki's pipeline error for a stored line whose labels
// before the pipeline are base, or nil when Loki makes no failing sample of it.
func unwrapLokiError(probe unwrapErrorProbe, base map[string]string, line, formatted string) *orderedJSONPipelineError {
	labels, parsed, ok := unwrapLokiLabels(probe, base, line, formatted)
	if !ok || labels == nil {
		return nil
	}
	value := labels[probe.label]
	if value == "" || labels["__preserve_error__"] == "true" {
		return nil
	}
	_, convErr := convertUnwrap(value, probe.conv)
	if convErr == nil {
		return nil
	}
	for _, stage := range probe.post {
		lf, isFilter := stage.(*logqlpkg.LabelFilterStage)
		if !isFilter {
			return nil
		}
		if compiled, valid := compileOrderedJSONStage(lf); valid && compiled.filter != nil &&
			(compiled.filter.Field == "__error__" || compiled.filter.Field == "__error_details__") {
			continue // evaluated with the error set (unwrapErrorPostFilters)
		}
		if keep, known := unwrapLabelFilterKeeps(lf, labels); !known || !keep {
			return nil
		}
	}
	if probe.hints != nil {
		for name := range parsed {
			if !probe.hints[name] {
				delete(labels, name)
			}
		}
	}
	for name, v := range labels {
		if v == "" {
			delete(labels, name) // a label set holds no empty value
		}
	}
	labels["__error__"] = "SampleExtractionErr"
	labels["__error_details__"] = convErr.Error()
	return lokiPipelineError(labels)
}

// unwrapLokiLabels runs the pipeline before the unwrap on a stored line the way
// Loki does, from the labels Loki starts with (stream labels, structured
// metadata). formatted is the line after the pipeline's line_format, as
// VictoriaLogs returned it. It returns the labels (nil when a label filter
// drops the line), the names the parsers extracted, and ok false when a stage
// is one this derivation does not reproduce exactly (the query then answers as
// the metric does).
func unwrapLokiLabels(probe unwrapErrorProbe, base map[string]string, line, formatted string) (map[string]string, map[string]bool, bool) {
	labels := cloneStringMap(base)
	parsed := map[string]bool{}
	formattedOnce := false
	for _, stage := range probe.pipeline {
		switch s := stage.(type) {
		case *logqlpkg.LineFilterStage:
			// VictoriaLogs applied it to the line the lookup read.
		case *logqlpkg.LabelFilterStage:
			// VictoriaLogs compared its own unpacked values; Loki's may differ.
			keep, known := unwrapLabelFilterKeeps(s, labels)
			if !known {
				return nil, nil, false
			}
			if !keep {
				return nil, parsed, true
			}
		case *logqlpkg.LineFormatStage:
			if formattedOnce {
				return nil, nil, false
			}
			formattedOnce, line = true, formatted
		case *logqlpkg.ParserStage:
			if !unwrapParseLine(s, line, labels, parsed, probe.hints["__error__"]) {
				return nil, nil, false
			}
		case *logqlpkg.DropStage, *logqlpkg.KeepStage:
			compiled, ok := compileOrderedJSONStage(stage)
			if !ok {
				return nil, nil, false
			}
			applyOrderedJSONFields(compiled, labels)
		case *logqlpkg.LabelFormatStage:
			for _, f := range s.Formats {
				if f.Name == probe.label || (f.Rename && f.Value == probe.label) {
					return nil, nil, false
				}
			}
		default:
			return nil, nil, false
		}
	}
	return labels, parsed, true
}

// unwrapLabelFilterKeeps evaluates a label filter on Loki's labels of a line
// (pkg/logql/log/label_filter.go): a string filter compares the value (absent
// is empty); a comparison drops a line without the label, keeps one whose value
// it cannot parse (Loki marks it with an error that the unwrap's own error
// replaces) and compares otherwise. known is false for an expression this does
// not evaluate (and/or, ip()).
func unwrapLabelFilterKeeps(stage *logqlpkg.LabelFilterStage, labels map[string]string) (keep, known bool) {
	if compiled, valid := compileOrderedJSONStage(stage); valid && compiled.filter != nil {
		return compiled.filter.Matches(labels[compiled.filter.Field]), true
	}
	m := unwrapComparisonRE.FindStringSubmatch(stage.Raw)
	if m == nil {
		return false, false
	}
	conv, ok := unwrapComparisonConv(m[3])
	if !ok {
		return false, false
	}
	bound, _ := convertUnwrap(m[3], conv)
	raw, present := labels[m[1]]
	if !present {
		return false, true
	}
	value, err := convertUnwrap(raw, conv)
	if err != nil {
		return true, true
	}
	switch m[2] {
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
	default:
		return value == bound, true
	}
}

// unwrapComparisonConv returns the conversion a comparison filter's literal
// selects in Loki's grammar: a number, a duration or a byte size.
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

// unwrapParseLine extracts a parser stage's labels from line into labels, as
// Loki v3.7.7 does (pkg/logql/log/parser.go): a key named like a label it
// already has gets the _extracted suffix; `| json` keeps strings, numbers and
// booleans and flattens objects, skipping arrays and null; `| unpack` keeps
// the strings of a packed entry only; `| regexp` keeps its named captures. ok is false for a parser
// with arguments or one this does not reproduce.
func unwrapParseLine(stage *logqlpkg.ParserStage, line string, labels map[string]string, parsed map[string]bool, preserveError bool) bool {
	set := func(name, value string) {
		if _, exists := labels[name]; exists && !parsed[name] {
			name += "_extracted"
		}
		if parsed[name] {
			return
		}
		labels[name], parsed[name] = value, true
	}
	switch stage.Type {
	case logqlpkg.ParserJSON:
		if stage.Param != "" || len(stage.Fields) != 0 {
			return false
		}
		plan := &orderedJSONMetricPlan{preserveError: preserveError}
		extracted := map[string]bool{}
		before := cloneStringMap(labels)
		if err := plan.parseJSON(line, before, labels, extracted); err != nil {
			return false
		}
		for name := range extracted {
			parsed[name] = true
		}
	case logqlpkg.ParserLogfmt:
		if stage.Param != "" || len(stage.Fields) != 0 {
			return false
		}
		// Loki's decoder (lokiLogfmtPairs): the first non-empty value of a key wins.
		lokiLogfmtPairs(line, func(key, value string) {
			name := orderedJSONLabelName(key, true)
			if value = lokiLabelValue(value); name != "" && value != "" {
				set(name, value)
			}
		})
	case logqlpkg.ParserUnpack:
		// Loki adds the string keys only to a packed entry, one holding _entry,
		// and the last value of a key wins (parser.go:788-840: the keys are
		// buffered and set when _entry is found). A key named like a label the
		// line had before the parser gets the _extracted suffix; one an earlier
		// parser extracted is skipped.
		type pair struct{ name, value string }
		var pairs []pair
		packed := false
		_ = jsonparser.ObjectEach([]byte(line), func(key, value []byte, kind jsonparser.ValueType, _ int) error {
			if kind != jsonparser.String {
				return nil
			}
			if string(key) == "_entry" {
				packed = true
				return nil
			}
			name := string(key)
			if _, exists := labels[name]; exists && !parsed[name] {
				name += "_extracted"
			}
			if parsed[name] {
				return nil
			}
			pairs = append(pairs, pair{orderedJSONLabelName(name, true), orderedJSONScalar(value, kind)})
			return nil
		})
		if packed {
			for _, kv := range pairs {
				labels[kv.name], parsed[kv.name] = kv.value, true
			}
		}
	case logqlpkg.ParserRegexp:
		re, err := regexp.Compile(stage.Param)
		if err != nil {
			return false
		}
		match := re.FindStringSubmatch(line)
		for i, name := range re.SubexpNames() {
			if name != "" && match != nil && i < len(match) {
				set(name, match[i])
			}
		}
	default:
		return false
	}
	return true
}

// unwrapLookupWriter holds the metric's answer until the detection decided:
// Loki's error replaces it when a hit is confirmed. The decision is taken when
// the metric writes, after its VictoriaLogs queries ran, so the status the
// route records is the status sent (unwrapRecordedStatus).
type unwrapLookupWriter struct {
	p      *Proxy
	ctx    context.Context
	check  *unwrapCheck
	out    http.ResponseWriter
	header http.Header
	status int
	body   bytes.Buffer

	decided  bool
	err      *orderedJSONPipelineError
	recorded bool
}

// startUnwrapConversionCheck attaches the detection of logql's unwrap range
// aggregations to the request and returns the request and the writer the
// metric answers into; the writer is nil when the query has nothing to check
// or an outer request already checks it (a binary operand).
func (p *Proxy) startUnwrapConversionCheck(w http.ResponseWriter, r *http.Request, logql string, isRange bool) (*http.Request, *unwrapLookupWriter) {
	if !strings.Contains(logql, "unwrap") || unwrapCheckFrom(r.Context()) != nil {
		return r, nil
	}
	expr, err := logqlpkg.Parse(logql)
	if err != nil {
		return r, nil
	}
	probes := unwrapErrorProbes(expr)
	start, end, step, err := orderedJSONMetricTimes(r, isRange)
	if err != nil {
		return r, nil
	}
	if len(probes) == 0 {
		// No conversion error reaches the result, but the nan values the gate drops
		// still count (unwrapNaN): the stats queries are rewritten, nothing else.
		c := &unwrapCheck{instant: !isRange, start: start, end: end, step: step}
		return r.WithContext(context.WithValue(r.Context(), unwrapCheckKey{}, c)), nil
	}
	if _, binary := expr.(*logqlpkg.BinOpExpr); binary && isRange && step > 0 {
		// Binary operands are evaluated on the step-aligned axis (alignBinaryRangeRequest).
		start, end = start.Add(-time.Duration(start.UnixNano()%int64(step))), end.Add(-time.Duration(end.UnixNano()%int64(step)))
	}
	c := &unwrapCheck{probes: probes, instant: !isRange, start: start, end: end, step: step}
	if p.labelTranslator != nil {
		c.vlName = p.labelTranslator.ToVL
	}
	ctx := context.WithValue(r.Context(), unwrapCheckKey{}, c)
	l := &unwrapLookupWriter{p: p, ctx: ctx, check: c, out: w, header: http.Header{}}
	c.writer = l
	return r.WithContext(ctx), l
}

// setQuery re-reads the aggregations after the query was rewritten before
// translation (preferWorkingParser), so the derivation follows the pipeline the
// metric runs.
func (l *unwrapLookupWriter) setQuery(logql string) {
	if l == nil {
		return
	}
	if expr, err := logqlpkg.Parse(logql); err == nil {
		if probes := unwrapErrorProbes(expr); len(probes) > 0 {
			l.check.probes = probes
		}
	}
}

func (l *unwrapLookupWriter) decide() {
	if l.decided {
		return
	}
	l.decided = true
	l.err = l.check.confirm(l.ctx, l.p)
}

func (l *unwrapLookupWriter) Header() http.Header { return l.header }

func (l *unwrapLookupWriter) WriteHeader(code int) {
	l.decide()
	if l.status == 0 {
		l.status = code
	}
}

func (l *unwrapLookupWriter) Write(b []byte) (int, error) {
	l.decide()
	if l.status == 0 {
		l.status = http.StatusOK
	}
	if l.err != nil {
		return len(b), nil
	}
	return l.body.Write(b)
}

// Flush is a no-op: the answer is held until the detection decided.
func (l *unwrapLookupWriter) Flush() {}

// failed reports whether the answer is Loki's error. A nil writer never fails.
func (l *unwrapLookupWriter) failed() bool {
	if l == nil {
		return false
	}
	l.decide()
	return l.err != nil
}

// unwrapRecordedStatus is the status a route records for a request: Loki's 400
// when the detection replaced the metric's answer. The route's record is then
// the only one.
func unwrapRecordedStatus(ctx context.Context, code int) int {
	c := unwrapCheckFrom(ctx)
	if c == nil || c.writer == nil {
		return code
	}
	c.writer.recorded = true
	if c.writer.failed() {
		return http.StatusBadRequest
	}
	return code
}

// finish writes Loki's pipeline error or the metric's own answer.
func (l *unwrapLookupWriter) finish(endpoint, logql string, requestStart time.Time) {
	if l.failed() {
		l.p.writeLokiTextError(l.out, http.StatusBadRequest, l.err.Error())
		if !l.recorded {
			l.p.metrics.RecordRequest(endpoint, http.StatusBadRequest, time.Since(requestStart))
			l.p.queryTracker.Record(endpoint, logql, time.Since(requestStart), true)
		}
		return
	}
	for name, values := range l.header {
		l.out.Header()[name] = values
	}
	if l.status != 0 {
		l.out.WriteHeader(l.status)
	}
	_, _ = l.out.Write(l.body.Bytes())
}

// writeLokiTextError answers an error the way Loki writes it
// (pkg/util/server/error.go WriteError): the message as text/plain.
func (p *Proxy) writeLokiTextError(w http.ResponseWriter, code int, msg string) {
	if p.log != nil {
		p.log.Warn("request error", "code", code, "error", msg)
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(msg))
}

// unwrapStripResponse applies strip to a 200 answer's body, bounded like the
// other buffered backend reads.
func (p *Proxy) unwrapStripResponse(resp *http.Response, strip func([]byte) ([]byte, error)) error {
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	limit := int64(p.limits().BufferedBackendBodyBytes)
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	_ = resp.Body.Close()
	if err != nil {
		return err
	}
	if int64(len(body)) > limit {
		return fmt.Errorf("manual metric response exceeds %d bytes; narrow the query or increase -backend-max-buffered-response-bytes", limit)
	}
	out, err := strip(body)
	if err != nil {
		return fmt.Errorf("invalid backend stats answer: %w", err)
	}
	resp.Body = io.NopCloser(bytes.NewReader(out))
	resp.ContentLength = int64(len(out))
	resp.Header.Del("Content-Length")
	return nil
}
