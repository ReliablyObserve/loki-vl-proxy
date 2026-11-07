package translator

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// ASTTranslateOptions controls how LogQL is translated to LogsQL.
type ASTTranslateOptions struct {
	// LabelFn translates a Loki label name to a VictoriaLogs field name.
	// If nil, label names are passed through unchanged.
	LabelFn LabelTranslateFunc

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

// ASTSelectorFilter is the VictoriaLogs filter for a bare stream selector, built as nodes
// (SelectorFilter): no query text is written or read on the way.
func ASTSelectorFilter(sel *logql.StreamSelector, opts ASTTranslateOptions) (logsql.FilterExpr, error) {
	matchers := make([]SelectorMatcher, len(sel.Matchers))
	for i, m := range sel.Matchers {
		matchers[i] = SelectorMatcher{Name: m.Name, Op: m.Op.String(), Value: m.Value}
	}
	return SelectorFilter(matchers, opts.LabelFn, opts.StreamFields)
}

// errFallthrough signals that translateExpr cannot handle this node and the
// caller should route to the string-based translator instead.
var errFallthrough = errors.New("fallthrough to string translator")

// TranslateAST converts a parsed LogQL expression to a LogsQL query string.
//
// For log queries (logql.LogQuery), it uses a typed AST-to-AST mapping that reads
// LogQL AST nodes and constructs the corresponding logsql.Query.
// For metric queries, VectorAggregation, RangeAggregation, and OpaqueMetricExpr,
// it falls through to the existing string-based translator.
func TranslateAST(expr logql.Expr, opts ASTTranslateOptions) (string, error) {
	q, err := translateExpr(expr, opts)
	if errors.Is(err, errFallthrough) {
		return TranslateLogQLWithStreamFields(expr.String(), opts.LabelFn, opts.StreamFields)
	}
	if err != nil {
		return "", err
	}
	return q.String(), nil
}

// translateExpr dispatches to the appropriate node translator.
func translateExpr(expr logql.Expr, opts ASTTranslateOptions) (*logsql.Query, error) {
	switch e := expr.(type) {
	case *logql.LogQuery:
		return astTranslateLogQuery(e, opts)
	default:
		return nil, errFallthrough
	}
}

// astTranslateLogQuery maps a LogQL log query (stream selector + pipeline) to a logsql.Query.
//
// The filter section is built by combining stream-selector matchers and any leading
// line-filter stages. Parser stages (json, logfmt, regexp, pattern) map to logsql
// pipe types. Label-filter stages and other opaque stages fall through to the
// string translator for the whole query.
func astTranslateLogQuery(lq *logql.LogQuery, opts ASTTranslateOptions) (*logsql.Query, error) {
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
		case *logql.LineFilterStage:
			part, err := translateLineFilter(s)
			if err != nil {
				return nil, err
			}
			filterParts = append(filterParts, part)

		case *logql.ParserStage:
			pipe, err := translateParser(s)
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, pipe)

		case *logql.LabelFilterStage:
			// Label filters can be complex (arithmetic, nested logic); fall through
			// so the full query is handled by the string translator.
			return nil, errFallthrough

		case *logql.DropStage:
			pipe, err := translateDrop(s, opts)
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, pipe)

		case *logql.KeepStage:
			pipe, err := translateKeep(s, opts)
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, pipe)

		case *logql.LineFormatStage:
			pipe, err := translateLineFormat(s)
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, pipe)

		case *logql.LabelFormatStage:
			// label_format can rename/template labels; fall through to string translator.
			return nil, errFallthrough

		case *logql.DecolorizeStage:
			pipes = append(pipes, logsql.PipeDecolorize{})

		case *logql.UnwrapStage:
			// unwrap is a metric-level operator; queries containing it are metric
			// expressions and should not reach astTranslateLogQuery. Fall through.
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
func translateStreamSelector(sel *logql.StreamSelector, opts ASTTranslateOptions) ([]string, error) {
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

// translateMatcher converts a single logql.LabelMatcher to its LogsQL filter string.
//
// Field naming rules (matching translator package behaviour):
//   - If opts.LabelFn is set, the label is renamed before use.
//   - Dotted field names are wrapped in double quotes (VL requirement).
//
// Negation uses the `-` prefix (not `NOT`) to match the translator package output.
func translateMatcher(m logql.LabelMatcher, opts ASTTranslateOptions) (string, error) {
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
	case logql.MatchEq:
		ff := logsql.FieldFilter{Field: quotedLabel, Op: logsql.FieldOpExact, Value: value}
		return ff.String(), nil
	case logql.MatchNeq:
		ff := logsql.FieldFilter{Field: quotedLabel, Op: logsql.FieldOpExact, Value: value}
		return "-" + ff.String(), nil
	case logql.MatchRe:
		// A LogQL label matcher is anchored to the whole value; VictoriaLogs'
		// field:~"re" is unanchored. Line filters (|~) are NOT anchored — see
		// translateLineFilter.
		ff := logsql.FieldFilter{Field: quotedLabel, Op: logsql.FieldOpRegexp, Value: logsql.AnchorLabelMatcherRegex(value)}
		return ff.String(), nil
	case logql.MatchNotRe:
		ff := logsql.FieldFilter{Field: quotedLabel, Op: logsql.FieldOpRegexp, Value: logsql.AnchorLabelMatcherRegex(value)}
		return "-" + ff.String(), nil
	default:
		return "", errFallthrough
	}
}

// translateLineFilter converts a LogQL line filter stage to its LogsQL filter string.
//
// Loki |= is a substring match; VL's ~"text" is the closest equivalent.
// Pattern filters (|> and !>) fall through to the string translator.
func translateLineFilter(s *logql.LineFilterStage) (string, error) {
	if s.IP || len(s.Or) > 0 {
		return "", errFallthrough
	}
	quoted := logsql.QuotePattern(s.Value)
	switch s.Op {
	case logql.LineFilterContains:
		return "~" + strconv.Quote(regexp.QuoteMeta(s.Value)), nil
	case logql.LineFilterExcludes:
		return "NOT ~" + strconv.Quote(regexp.QuoteMeta(s.Value)), nil
	case logql.LineFilterMatchRe:
		// |~ "re" → ~"re"
		return "~" + quoted, nil
	case logql.LineFilterExcludeRe:
		// !~ "re" → NOT ~"re"
		return "NOT ~" + quoted, nil
	case logql.LineFilterContainsPat:
		// |> "pattern" — no direct VL equivalent without capabilities context
		return "", errFallthrough
	case logql.LineFilterExcludePat:
		return "", errFallthrough
	default:
		return "", errFallthrough
	}
}

// translateParser converts a LogQL parser stage to the matching logsql Pipe.
func translateParser(s *logql.ParserStage) (logsql.Pipe, error) {
	switch s.Type {
	case logql.ParserJSON:
		return logsql.PipeUnpackJSON{}, nil
	case logql.ParserLogfmt:
		return logsql.PipeUnpackLogfmt{}, nil
	case logql.ParserRegexp:
		return logsql.PipeExtractRegexp{Pattern: s.Param, From: "_msg"}, nil
	case logql.ParserPattern:
		return logsql.PipeExtract{Pattern: s.Param, From: "_msg"}, nil
	case logql.ParserUnpack:
		return logsql.PipeUnpackJSON{}, nil
	default:
		return nil, errFallthrough
	}
}

// translateDrop converts a LogQL | drop stage to a logsql PipeDelete.
// Conditional drops (| drop level="debug") cannot be expressed natively in
// LogsQL; return errFallthrough so the string translator handles the query.
func translateDrop(s *logql.DropStage, opts ASTTranslateOptions) (logsql.Pipe, error) {
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
func translateKeep(s *logql.KeepStage, opts ASTTranslateOptions) (logsql.Pipe, error) {
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
func translateLineFormat(s *logql.LineFormatStage) (logsql.Pipe, error) {
	tmpl := s.Template
	if strings.Contains(tmpl, "{{if") || strings.Contains(tmpl, "{{range") ||
		strings.Contains(tmpl, "{{with") || strings.Contains(tmpl, "__line__") ||
		strings.Contains(tmpl, "__timestamp__") || strings.Contains(tmpl, "| ") {
		return nil, errFallthrough
	}
	return logsql.PipeFormat{Template: tmpl, ResultField: "_msg"}, nil
}
