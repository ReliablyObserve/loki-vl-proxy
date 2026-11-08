package translator

import (
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
