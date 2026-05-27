package logql

import (
	"errors"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// TranslateOptions controls how LogQL is translated to LogsQL.
type TranslateOptions struct {
	// LabelFn translates a Loki label name to a VictoriaLogs field name.
	// If nil, label names are passed through unchanged.
	LabelFn translator.LabelTranslateFunc

	// StreamFields is the set of VL _stream_fields labels for which the
	// translator can emit the faster stream-selector syntax.
	// Reserved: the string translator uses this for stream-selector optimisation;
	// the AST path will use it in a follow-on PR.
	StreamFields map[string]bool

	// Caps gates VL version-specific features in the AST path.
	// Reserved: will be used to select capability-gated logsql constructs
	// (e.g. ipv4_range). The zero value disables all gated features.
	Caps logsql.Capabilities
}

// errFallthrough signals that translateExpr cannot handle this node and the
// caller should route to the string-based translator instead.
var errFallthrough = errors.New("fallthrough to string translator")

// Translate converts a parsed LogQL expression to a LogsQL query string.
//
// For log queries (LogQuery), it uses a typed AST-to-AST mapping that reads
// LogQL AST nodes and constructs the corresponding logsql.Query.
// For metric queries, VectorAggregation, RangeAggregation, and OpaqueMetricExpr,
// it falls through to the existing string-based translator.
func Translate(expr Expr, opts TranslateOptions) (string, error) {
	q, err := translateExpr(expr, opts)
	if errors.Is(err, errFallthrough) {
		return translator.TranslateLogQLWithStreamFields(expr.String(), opts.LabelFn, opts.StreamFields)
	}
	if err != nil {
		return "", err
	}
	return q.String(), nil
}

// translateExpr dispatches to the appropriate node translator.
func translateExpr(expr Expr, opts TranslateOptions) (*logsql.Query, error) {
	switch e := expr.(type) {
	case *LogQuery:
		return translateLogQuery(e, opts)
	default:
		return nil, errFallthrough
	}
}

// translateLogQuery maps a LogQL log query (stream selector + pipeline) to a logsql.Query.
//
// The filter section is built by combining stream-selector matchers and any leading
// line-filter stages. Parser stages (json, logfmt, regexp, pattern) map to logsql
// pipe types. Label-filter stages and other opaque stages fall through to the
// string translator for the whole query.
func translateLogQuery(lq *LogQuery, opts TranslateOptions) (*logsql.Query, error) {
	// Collect all filter parts (stream selector + line filters) as LogsQL strings.
	var filterParts []string

	selectorParts, err := translateStreamSelector(lq.Selector, opts)
	if err != nil {
		return nil, err
	}
	filterParts = append(filterParts, selectorParts...)

	// Walk the pipeline. Line filters contribute to the filter section.
	// Parser stages become logsql pipe nodes. Anything else triggers fallthrough.
	var pipes []logsql.Pipe
	for _, stage := range lq.Pipeline {
		switch s := stage.(type) {
		case *LineFilterStage:
			part, err := translateLineFilter(s)
			if err != nil {
				return nil, err
			}
			filterParts = append(filterParts, part)

		case *ParserStage:
			pipe, err := translateParser(s)
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, pipe)

		case *LabelFilterStage:
			// Label filters can be complex (arithmetic, nested logic); fall through
			// so the full query is handled by the string translator.
			return nil, errFallthrough

		case *DropStage:
			pipe, err := translateDrop(s, opts)
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, pipe)

		case *KeepStage:
			pipe, err := translateKeep(s, opts)
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, pipe)

		case *LineFormatStage:
			pipe, err := translateLineFormat(s)
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, pipe)

		case *LabelFormatStage:
			// label_format can rename/template labels; fall through to string translator.
			return nil, errFallthrough

		case *DecolorizeStage:
			pipes = append(pipes, logsql.PipeDecolorize{})

		case *UnwrapStage:
			// unwrap is a metric-level operator; queries containing it are metric
			// expressions and should not reach translateLogQuery. Fall through.
			return nil, errFallthrough

		default:
			// Unknown stage type; fall through for safety.
			return nil, errFallthrough
		}
	}

	// Combine filter parts (space-separated = implicit AND in LogsQL).
	var filter logsql.FilterExpr
	switch len(filterParts) {
	case 0:
		filter = nil // produces "*"
	case 1:
		filter = logsql.DeferredExpr{Raw: filterParts[0]}
	default:
		filter = logsql.DeferredExpr{Raw: strings.Join(filterParts, " ")}
	}

	return logsql.NewQuery(filter, pipes...), nil
}

// translateStreamSelector converts LogQL stream-selector matchers to LogsQL filter strings.
// Each matcher produces one string element; the caller joins them with spaces.
func translateStreamSelector(sel *StreamSelector, opts TranslateOptions) ([]string, error) {
	if sel == nil || len(sel.Matchers) == 0 {
		return nil, nil
	}

	parts := make([]string, 0, len(sel.Matchers))
	for _, m := range sel.Matchers {
		part, err := translateMatcher(m, opts)
		if err != nil {
			return nil, err
		}
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts, nil
}

// translateMatcher converts a single LabelMatcher to its LogsQL filter string.
//
// Field naming rules (matching translator package behaviour):
//   - If opts.LabelFn is set, the label is renamed before use.
//   - Dotted field names are wrapped in double quotes (VL requirement).
//
// Negation uses the `-` prefix (not `NOT`) to match the translator package output.
func translateMatcher(m LabelMatcher, opts TranslateOptions) (string, error) {
	label := m.Name
	if opts.LabelFn != nil {
		label = opts.LabelFn(label)
	}
	// VL requires quoting for dotted field names.
	quotedLabel := label
	if strings.Contains(label, ".") {
		quotedLabel = `"` + label + `"`
	}

	value := m.Value

	switch m.Op {
	case MatchEq:
		ff := logsql.FieldFilter{Field: quotedLabel, Op: logsql.FieldOpExact, Value: value}
		return ff.String(), nil
	case MatchNeq:
		ff := logsql.FieldFilter{Field: quotedLabel, Op: logsql.FieldOpExact, Value: value}
		return "-" + ff.String(), nil
	case MatchRe:
		ff := logsql.FieldFilter{Field: quotedLabel, Op: logsql.FieldOpRegexp, Value: value}
		return ff.String(), nil
	case MatchNotRe:
		ff := logsql.FieldFilter{Field: quotedLabel, Op: logsql.FieldOpRegexp, Value: value}
		return "-" + ff.String(), nil
	default:
		return "", errFallthrough
	}
}

// translateLineFilter converts a LogQL line filter stage to its LogsQL filter string.
//
// Loki |= is a substring match; VL's ~"text" is the closest equivalent.
// Pattern filters (|> and !>) fall through to the string translator.
func translateLineFilter(s *LineFilterStage) (string, error) {
	quoted := logsql.QuotePattern(s.Value)
	switch s.Op {
	case LineFilterContains:
		// |= "text" → ~"text" (substring/regexp match in VL)
		return "~" + quoted, nil
	case LineFilterExcludes:
		// != "text" → NOT ~"text"
		return "NOT ~" + quoted, nil
	case LineFilterMatchRe:
		// |~ "re" → ~"re"
		return "~" + quoted, nil
	case LineFilterExcludeRe:
		// !~ "re" → NOT ~"re"
		return "NOT ~" + quoted, nil
	case LineFilterContainsPat:
		// |> "pattern" — no direct VL equivalent without capabilities context
		return "", errFallthrough
	case LineFilterExcludePat:
		return "", errFallthrough
	default:
		return "", errFallthrough
	}
}

// translateParser converts a LogQL parser stage to the matching logsql Pipe.
func translateParser(s *ParserStage) (logsql.Pipe, error) {
	switch s.Type {
	case ParserJSON:
		return logsql.PipeUnpackJSON{}, nil
	case ParserLogfmt:
		return logsql.PipeUnpackLogfmt{}, nil
	case ParserRegexp:
		return logsql.PipeExtractRegexp{Pattern: s.Param, From: "_msg"}, nil
	case ParserPattern:
		return logsql.PipeExtract{Pattern: s.Param, From: "_msg"}, nil
	case ParserUnpack:
		return logsql.PipeUnpackJSON{}, nil
	default:
		return nil, errFallthrough
	}
}

// translateDrop converts a LogQL | drop stage to a logsql PipeDelete.
// Conditional drops (| drop level="debug") cannot be expressed natively in
// LogsQL; return errFallthrough so the string translator handles the query.
func translateDrop(s *DropStage, opts TranslateOptions) (logsql.Pipe, error) {
	if len(s.Matchers) > 0 {
		return nil, errFallthrough
	}
	labels := make([]string, 0, len(s.Labels))
	for _, l := range s.Labels {
		if opts.LabelFn != nil {
			l = opts.LabelFn(l)
		}
		labels = append(labels, l)
	}
	return logsql.PipeDelete{Labels: labels}, nil
}

// translateKeep converts a LogQL | keep stage to a logsql PipeFields.
// Conditional keeps (| keep level="debug") cannot be expressed natively in
// LogsQL; return errFallthrough so the string translator handles the query.
func translateKeep(s *KeepStage, opts TranslateOptions) (logsql.Pipe, error) {
	if len(s.Matchers) > 0 {
		return nil, errFallthrough
	}
	labels := make([]string, 0, len(s.Labels))
	for _, l := range s.Labels {
		if opts.LabelFn != nil {
			l = opts.LabelFn(l)
		}
		labels = append(labels, l)
	}
	return logsql.PipeFields{Labels: labels}, nil
}

// translateLineFormat converts a LogQL | line_format stage to a logsql PipeFormat.
// Complex Go template directives ({{if}}, {{range}}, {{with}}, pipes, or the
// special __line__/__timestamp__ variables) are not expressible in LogsQL
// | format; fall through to the string translator for those cases.
func translateLineFormat(s *LineFormatStage) (logsql.Pipe, error) {
	tmpl := s.Template
	if strings.Contains(tmpl, "{{if") || strings.Contains(tmpl, "{{range") ||
		strings.Contains(tmpl, "{{with") || strings.Contains(tmpl, "__line__") ||
		strings.Contains(tmpl, "__timestamp__") || strings.Contains(tmpl, "| ") {
		return nil, errFallthrough
	}
	return logsql.PipeFormat{Template: tmpl, ResultField: "_msg"}, nil
}
