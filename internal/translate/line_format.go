package translate

import (
	"regexp"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The builders of line_format (and the template of label_format) in a log query. The string translator reads the
// template as text: it turns every `{{.name}}` into the VictoriaLogs placeholder `<name>` and leaves everything
// else as it is. That is Loki's meaning only for a template that is text and `{{.name}}` fields, which the typed
// template says (logql.LineTemplate.Simple); the builders answer false for the others, and for text that
// VictoriaLogs would read as a placeholder, and the lowering leaves such a query to the translator.

// StoredLineField is the field a log query copies the stored line into before its first line_format rewrites it,
// so the response carries both the formatted and the stored line.
const StoredLineField = "_lvp_line"

var goTemplateRE = regexp.MustCompile(`\{\{\s*\.([\w.]+)\s*\}\}`)

// simpleFieldRE is a `{{.name}}` action as Loki reads it as one field of the labels: a single name, no dot chain.
var simpleFieldRE = regexp.MustCompile(`\{\{\s*\.\w+\s*\}\}`)

// GoTemplatePlaceholders is a Go template text with every `{{.name}}` turned into the VictoriaLogs placeholder
// `<name>`, as the string translator does.
func GoTemplatePlaceholders(tmpl string) string {
	return goTemplateRE.ReplaceAllString(tmpl, "<$1>")
}

// The reasons the builders answer false, named for the lowering's delegation constructs.
const (
	// TemplateNotText: the template is more than text and plain `{{.name}}` fields (a function, a pipe, a
	// condition, a dotted field), which the translator copies into the query as it is.
	TemplateNotText = "function"
	// TemplatePlaceholderText: the text holds a `<`, which VictoriaLogs reads as a placeholder.
	TemplatePlaceholderText = "angle bracket"
	// TemplateCarriageReturn: a carriage return, which the translator's unquoting drops from a raw string.
	TemplateCarriageReturn = "carriage return"
	// TemplateTrimMarker: an action the translator's reading leaves in the text (`{{- .a -}}`).
	TemplateTrimMarker = "trim marker"
)

// FormatTemplate is the `format` template of a Go template text. reason is "" when the string translator's
// reading of the text is Loki's meaning, else one of the Template constants: the lowering leaves such a query to
// the translator. The text is Loki's `{{.name}}` fields and plain text exactly when every `{{` opens such an
// action (the typed template, logql.LineTemplate.Simple, says the same; reading the text costs no template parse).
func FormatTemplate(text string) (format, reason string) {
	if strings.Contains(text, "<") {
		return "", TemplatePlaceholderText
	}
	if strings.Contains(text, "\r") {
		return "", TemplateCarriageReturn
	}
	rest := simpleFieldRE.ReplaceAllString(text, "")
	if !strings.Contains(rest, "{{") && !strings.Contains(rest, "}}") {
		return GoTemplatePlaceholders(text), ""
	}
	// What is left is more than text and plain fields. A dotted field, which the translator turns into one
	// placeholder, and an action it leaves in the text are told apart for the named construct.
	if converted := GoTemplatePlaceholders(rest); !strings.Contains(converted, "{{") && !strings.Contains(converted, "}}") {
		return "", TemplateNotText
	}
	if strings.Contains(text, "{{-") || strings.Contains(text, "-}}") {
		return "", TemplateTrimMarker
	}
	return "", TemplateNotText
}

// LineFormat is the pipes of a `| line_format` stage: `| format "<template>"`, behind `| copy _msg as _lvp_line`
// for the first line_format of a query that keeps the stored line. reason is FormatTemplate's.
func LineFormat(text string, keepLine bool) (pipes []logsql.Pipe, reason string) {
	converted, reason := FormatTemplate(text)
	if reason != "" {
		return nil, reason
	}
	if keepLine {
		pipes = append(pipes, logsql.PipeCopy{Pairs: [][2]string{{"_msg", StoredLineField}}})
	}
	return append(pipes, logsql.PipeFormat{Template: converted}), ""
}

// TemplateKeywordName: a name the typed stage may hold in another case than it was written (the LogQL scanner
// folds a word Loki reads as a keyword to lower case), where the translator keeps the text.
const TemplateKeywordName = "keyword name"

// TemplateRenameOntoItself: a label_format `a=a`, which the string translator writes as a rename that deletes the
// label.
const TemplateRenameOntoItself = "rename onto itself"

// LabelFormats are the pipes of a `| label_format` stage, one or two per assignment, in the order written. A
// template value is `| format "<template>" as dst`; a rename `dst=src` sets dst to src's value only when src
// exists, keeping dst otherwise (a plain rename would clear dst for a missing src; skip_empty_results keeps dst
// where the condition fails on VictoriaLogs before v1.35) and then removes src: `| format if (src:*) "<src>" as
// dst skip_empty_results | delete src`. reason is "" when the string translator's reading is Loki's meaning, else
// FormatTemplate's or TemplateRenameOntoItself.
func LabelFormats(formats []logql.LabelFormat) (pipes []logsql.Pipe, reason string) {
	for _, f := range formats {
		if logql.IsLokiKeyword(f.Name) || f.Rename && logql.IsLokiKeyword(f.Value) {
			return nil, TemplateKeywordName
		}
		if f.Rename {
			if f.Name == f.Value {
				return nil, TemplateRenameOntoItself
			}
			pipes = append(pipes,
				logsql.PipeFormat{
					Template: "<" + f.Value + ">", ResultField: f.Name, SkipEmptyResults: true,
					If: logsql.FieldFilter{Field: f.Value, Op: logsql.FieldOpAny},
				},
				logsql.PipeDelete{Labels: []string{f.Value}})
			continue
		}
		converted, reason := FormatTemplate(f.Value)
		if reason != "" {
			return nil, reason
		}
		pipes = append(pipes, logsql.PipeFormat{Template: converted, ResultField: f.Name})
	}
	return pipes, ""
}
