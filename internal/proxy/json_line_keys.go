package proxy

import (
	"context"
	"net/http"
	"net/url"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// A Loki push of a JSON line to VictoriaLogs' Loki push route without
// disable_message_parsing=1 is stored as one field per key of the line, with
// VictoriaLogs' missing-message placeholder (its -defaultMsgValue) in _msg
// (profiles/json-line-stored-without-line). By default the proxy rebuilds the
// line from the row's fields and reports those fields as structured metadata,
// since VictoriaLogs cannot tell the line's keys from structured metadata pushed
// beside the line.
//
// -json-line-fields-as-line-keys reads every stored field of such a row as a key
// of its JSON line instead, as Loki holds the pushed line:
//   - a log response classifies the row against its rebuilt line, so its fields
//     are no labels until a stage exposes them (| json, an extraction list),
//     exactly as for a line VictoriaLogs kept (lineFieldExposure);
//   - a query whose every log selector reads a label that no stage can give such
//     a line (an unwrap, or a label filter that an empty value fails, after
//     | logfmt or without a parser) leaves those rows out in VictoriaLogs
//     (placeholderExclusion);
//   - the parse-risk probes of | json pipelines never count such a row as a line
//     that is not JSON.
// It runs in the Loki-compatible profile only, and only for the placeholder:
// a row with an empty _msg keeps today's reading.

// jsonLineKeysActive reports whether stored fields of placeholder rows are read
// as keys of the row's JSON line.
func (p *Proxy) jsonLineKeysActive() bool {
	return p != nil && p.jsonLineFieldsAsLineKeys && p.lokiCompatibleProfile()
}

// isVLPlaceholderMsg reports whether msg is VictoriaLogs' missing-message
// placeholder: its built-in -defaultMsgValue or the configured one. Unlike
// isVLMissingMsg, an empty _msg is not the placeholder.
func isVLPlaceholderMsg[T string | []byte](msg T, defaultMsgValue string) bool {
	return len(msg) > 0 && (string(msg) == vlDefaultMsgValue || (defaultMsgValue != "" && string(msg) == defaultMsgValue))
}

// placeholderPipelineLineFields are the fields a placeholder row's rebuilt line
// leaves out with the flag on: the fields regexp and pattern captures and
// label_format targets write (pipelineLineFields without the json and logfmt
// extraction lists). VictoriaLogs' unpack pipes read an extraction list from
// the placeholder text, which holds none of its names, so a stored field of
// such a name is the line's own key and stays in the line Loki returns.
func placeholderPipelineLineFields(pipeline []logqlpkg.Stage) map[string]bool {
	out := pipelineRegexpCaptureFields(pipeline)
	add := func(name string) {
		if name == "" || name == "_" {
			return
		}
		if out == nil {
			out = make(map[string]bool)
		}
		out[name] = true
	}
	for _, stage := range pipeline {
		switch s := stage.(type) {
		case *logqlpkg.ParserStage:
			if s.Type == logqlpkg.ParserPattern {
				for _, match := range patternCaptureRE.FindAllStringSubmatch(s.Param, -1) {
					add(match[1])
				}
			}
		case *logqlpkg.LabelFormatStage:
			for _, f := range s.Formats {
				add(f.Name)
			}
		}
	}
	return out
}

// rowLineFields picks the fields a row's rebuilt line leaves out: the
// placeholder set for a placeholder row with the flag on, else the query's.
type rowLineFields struct {
	base, placeholder map[string]bool
	defaultMsg        string
	active            bool
	// blankIsPlaceholder: the pipeline has a | logfmt stage, after which an
	// empty _msg is the placeholder (readsPlaceholderAsLine).
	blankIsPlaceholder bool
}

// rowLineFieldsOf is the rebuilt-line field choice of a log query.
func (p *Proxy) rowLineFieldsOf(query string, base map[string]bool) rowLineFields {
	out := rowLineFields{base: base, defaultMsg: p.defaultMsgValue()}
	if !p.jsonLineKeysActive() {
		return out
	}
	lq, err := parseLogQLQuery(noFlowCtx, query)
	if err != nil {
		return out
	}
	out.placeholder, out.active = placeholderPipelineLineFields(lq.Pipeline), true
	out.blankIsPlaceholder = pipelineHasParserStageOf(lq.Pipeline, false, true)
	return out
}

// forRowMsg is the field set for a row whose _msg is msg.
func forRowMsg[T string | []byte](f rowLineFields, msg T) map[string]bool {
	if f.active && (isVLPlaceholderMsg(msg, f.defaultMsg) || (len(msg) == 0 && f.blankIsPlaceholder)) {
		return f.placeholder
	}
	return f.base
}

// readsPlaceholderAsLine reports whether a row whose stored _msg is msg is
// classified against its rebuilt line (lineFieldExposure.placeholderLine).
//
// VictoriaLogs' unpack_logfmt reads the built-in placeholder's "_msg" word as a
// key without a value and blanks _msg: VictoriaLogs never stores an empty _msg
// (a row without one gets the placeholder), so after a | logfmt stage an empty
// _msg is the placeholder too.
func readsPlaceholderAsLine[T string | []byte](e *lineFieldExposure, msg T) bool {
	return e != nil && e.placeholderLine && (isVLPlaceholderMsg(msg, e.defaultMsg) || (len(msg) == 0 && e.logfmt))
}

// placeholderExclusionKey carries a request's placeholder exclusion: the
// LogsQL filter every VictoriaLogs read of the request is restricted with
// (extra_filters), or none.
type placeholderExclusionKey struct{}

// withPlaceholderExclusion returns r with the placeholder exclusion of its
// query, when the flag is on and the query's every log selector reads a label
// no stage can give a JSON line stored without its line (placeholderExclusion).
func (p *Proxy) withPlaceholderExclusion(r *http.Request, query string) *http.Request {
	if !p.jsonLineKeysActive() {
		return r
	}
	plan := resolvedPlan(query)
	if plan == nil {
		return r
	}
	f, ok := p.placeholderExclusion(plan)
	if !ok {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), placeholderExclusionKey{}, f))
}

// placeholderExclusionFrom is the request's placeholder exclusion filter, or "".
func placeholderExclusionFrom(ctx context.Context) string {
	f, _ := ctx.Value(placeholderExclusionKey{}).(string)
	return f
}

// withExclusionParams adds the request's placeholder exclusion to the
// parameters of a VictoriaLogs read of rows: VictoriaLogs ANDs extra_filters
// with the query (and every subquery), so the rows Loki keeps nothing from
// never reach a stats pipe, a probe or a row page.
func withExclusionParams(ctx context.Context, path string, params url.Values) url.Values {
	f := placeholderExclusionFrom(ctx)
	if f == "" || !readsQueryRows(path) {
		return params
	}
	out := cloneURLValues(params)
	out.Add("extra_filters", f)
	return out
}

// readsQueryRows reports whether a VictoriaLogs path reads the rows of the
// request's query: the metadata scans (field and stream names and values) are
// shared inventories the exclusion does not change.
func readsQueryRows(path string) bool {
	switch path {
	case "/select/logsql/query", "/select/logsql/stats_query", "/select/logsql/stats_query_range", "/select/logsql/hits":
		return true
	}
	return false
}

// placeholderExclusion is the exclusion filter of a plan: rows whose _msg is
// the placeholder are left out unless their stream carries a label the
// selectors' guard names. ok is false unless every log selector of the plan
// drops every such line with the same guard (placeholderDropGuard).
func (p *Proxy) placeholderExclusion(plan *queryplan.Plan) (string, bool) {
	var guard string
	seen, ok := false, true
	var walk func(queryplan.Node)
	walk = func(n queryplan.Node) {
		switch v := n.(type) {
		case *queryplan.LogSelect:
			g, drops := placeholderDropGuard(v.Stages)
			if !drops || (seen && g != guard) {
				ok = false
			}
			guard, seen = g, true
		case *queryplan.RangeAgg:
			walk(v.In)
		case *queryplan.VectorAgg:
			walk(v.In)
		case *queryplan.Rank:
			walk(v.In)
		case *queryplan.LabelReplace:
			walk(v.In)
		case *queryplan.BinOp:
			walk(v.L)
			walk(v.R)
		case *queryplan.Scalar, *queryplan.VectorLit:
		default:
			ok = false
		}
	}
	walk(plan.Root)
	if !ok || !seen {
		return "", false
	}
	notPlaceholder := logsql.Not(p.placeholderMsg())
	vlGuard := p.labelTranslator.ToVL(guard)
	f := logsql.Or(notPlaceholder, logsql.StreamFilter{Matchers: []logsql.LabelMatcher{{Name: vlGuard, Op: "!=", Value: ""}}})
	return string(logsql.AppendFilter(nil, f)), true
}

// placeholderDropGuard reports whether a pipeline keeps nothing from a flat
// JSON line without structured metadata, and the label that would keep it as a
// stream label (the line's own keys are no labels until a json parser reads
// them): a stage before any json parser that reads a label such a line lacks.
//   - an unwrap: Loki takes no sample of a line without the label;
//   - a label filter whose plain matchers (ANDed) include one an empty value
//     fails, or a numeric, duration or bytes comparison (lineFieldFilter).
//
// Line filters, line_format and decolorize change no label; | logfmt reads no
// key of a flat JSON line and gives an extraction list's names empty values.
// Any other stage (json, regexp, pattern, unpack, label_format, drop, keep), a
// filter of another form, or a label Loki gives such a line without a parser
// (detected_level from its ingest, service_name, which the proxy may derive,
// level, and the error labels) leaves the pipeline undecided.
func placeholderDropGuard(stages []queryplan.Stage) (string, bool) {
	for _, st := range stages {
		switch s := st.(type) {
		case *logqlpkg.LineFilterStage, *logqlpkg.DecolorizeStage, *logqlpkg.LineFormatStage:
		case *logqlpkg.ParserStage:
			if s.Type != logqlpkg.ParserLogfmt {
				return "", false
			}
		case *logqlpkg.UnwrapStage:
			return s.Label, placeholderGuardable(s.Label)
		case *logqlpkg.LabelFilterStage:
			conds := labelFilterConditions(s.Expr, lineFieldExposure{})
			if len(conds) == 0 {
				return "", false
			}
			for _, c := range conds {
				if !placeholderGuardable(c.name) {
					return "", false
				}
			}
			for _, c := range conds {
				if !c.matchesEmpty {
					return c.name, true
				}
			}
		default:
			return "", false
		}
	}
	return "", false
}

// placeholderGuardable reports whether a label is absent from a flat JSON line
// without structured metadata unless it is a stream label.
func placeholderGuardable(name string) bool {
	switch name {
	case "", detectedLevelLabel, "level", "service_name", "__error__", "__error_details__":
		return false
	}
	return isLokiLabelName(name)
}

// placeholderMsg matches the rows whose _msg is the placeholder: VictoriaLogs'
// built-in one and the configured one.
func (p *Proxy) placeholderMsg() logsql.FilterExpr {
	f := logsql.FieldExact("_msg", vlDefaultMsgValue)
	if custom := p.defaultMsgValue(); custom != "" && custom != vlDefaultMsgValue {
		f = logsql.Or(f, logsql.FieldExact("_msg", custom))
	}
	return f
}

// placeholderRiskFilter is the filter the parse-risk probes of | json
// pipelines are restricted with when the flag is on: a placeholder row holds a
// JSON line (its fields), which those probes would otherwise read as a line
// that is not JSON. "" when the flag is off.
func (p *Proxy) placeholderRiskFilter() string {
	if !p.jsonLineKeysActive() {
		return ""
	}
	return string(logsql.AppendFilter(nil, logsql.Not(p.placeholderMsg())))
}

// placeholderJSONStartFilter is the brace filter of a | json stats split
// (jsonObjectStartFilter), which with the flag on also keeps the placeholder
// rows: their line is a JSON object.
func (p *Proxy) placeholderJSONStartFilter() string {
	if !p.jsonLineKeysActive() {
		return jsonObjectStartFilter
	}
	return "(" + jsonObjectStartFilter + " or " + string(logsql.AppendFilter(nil, p.placeholderMsg())) + ")"
}

// placeholderValidPipe marks a placeholder row as a line VictoriaLogs parsed as
// one JSON object (jsonValidityPipes) when the flag is on: its keys are the
// stored fields the split's unpack keeps.
func (p *Proxy) placeholderValidPipe() string {
	if !p.jsonLineKeysActive() {
		return ""
	}
	return " | format if (" + string(logsql.AppendFilter(nil, p.placeholderMsg())) + ") \"1\" as " + jsonValidField
}

// placeholderRowLine is the line the | json row evaluator parses for a row:
// its _msg, or with the flag on, for a placeholder row, the JSON line rebuilt
// from the fields VictoriaLogs returned (the projection holds every field the
// query reads), leaving out the stream labels.
func (p *Proxy) placeholderRowLine(row map[string]string, streamLabels map[string]string) string {
	msg := row["_msg"]
	if !p.jsonLineKeysActive() || !isVLPlaceholderMsg(msg, p.defaultMsgValue()) {
		return msg
	}
	entry := make(map[string]interface{}, len(row))
	for k, v := range row {
		entry[k] = v
	}
	if line := storedLogLineFromEntry(msg, entry, streamLabels, nil, p.defaultMsgValue()); line != "" {
		return line
	}
	// No key the query reads: still a JSON object.
	return "{}"
}

// placeholderLogfmtOnly are the names of a pipeline's logfmt extraction lists
// that no json extraction list, label_format target, regexp or pattern capture
// also writes.
func placeholderLogfmtOnly(pipeline []logqlpkg.Stage) map[string]bool {
	var out map[string]bool
	for _, stage := range pipeline {
		if s, ok := stage.(*logqlpkg.ParserStage); ok && s.Type == logqlpkg.ParserLogfmt {
			for _, f := range s.Fields {
				if out == nil {
					out = map[string]bool{}
				}
				out[f.Name] = true
			}
		}
	}
	if out == nil {
		return nil
	}
	others := placeholderPipelineLineFields(pipeline)
	for name := range others {
		delete(out, name)
	}
	for _, stage := range pipeline {
		if s, ok := stage.(*logqlpkg.ParserStage); ok && s.Type == logqlpkg.ParserJSON {
			for _, f := range s.Fields {
				delete(out, f.Name)
			}
		}
	}
	return out
}
