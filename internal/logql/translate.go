package logql

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// TranslateOptions controls how LogQL is translated to LogsQL.
type TranslateOptions struct {
	// LabelFn translates a Loki label name to a VictoriaLogs field name.
	// If nil, label names are passed through unchanged.
	LabelFn translator.LabelTranslateFunc

	// StreamFields is the set of VL _stream_fields labels for which the
	// translator can emit the faster stream-selector syntax.
	StreamFields map[string]bool
}

// Translate converts a parsed LogQL expression to a LogsQL query string.
// It normalises the expression to canonical LogQL via Expr.String() and then
// delegates to the existing translator package, preserving all its rules.
func Translate(expr Expr, opts TranslateOptions) (string, error) {
	canonical := expr.String()
	return translator.TranslateLogQLWithStreamFields(canonical, opts.LabelFn, opts.StreamFields)
}
