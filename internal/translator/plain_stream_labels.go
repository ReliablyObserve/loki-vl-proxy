package translator

import (
	"regexp"
	"strconv"
	"strings"
)

// Loki's parsers never overwrite a label the entry already has from its stream
// or its structured metadata: the parsed key is renamed name_extracted and the
// plain name keeps the stream value (pkg/logql/log/parser.go duplicateSuffix,
// LabelsBuilder.getWithCategory), so a filter, grouping, unwrap or keep after
// the parser reads the stream value. VictoriaLogs' unpack_json, unpack_logfmt,
// extract and extract_regexp overwrite the stored field of that name.
//
// For the names the proxy knows as stream labels of the tenant (and the query
// reads after a parser), the stored value is copied aside before the first
// parser stage, and:
//   - a metric query restores it into the field right after every parser stage
//     (metric results carry no entry labels), so every later stage and the
//     grouping read Loki's value;
//   - a log query leaves the field alone (the response exposes it as the parsed
//     label) and reads a scratch field that holds the stored value, or the
//     first parsed one when the entry has none.
//
// A name that is never a stream label keeps its plain translation, and so do
// the Drilldown fast paths that match it.

// streamLabelsProbe is the label name the proxy's translation function answers
// with the stream label names to read as stream values (see WithStreamLabels).
const streamLabelsProbe = "\x03"

// svScratch prefixes the scratch field of each such name.
const svScratch = "__lxsv_"

// WithStreamLabels wraps a label translation function so the translator learns
// the names (Loki label names) that are stream labels of the tenant.
func WithStreamLabels(labelFn LabelTranslateFunc, names []string) LabelTranslateFunc {
	if len(names) == 0 {
		return labelFn
	}
	joined := streamLabelsProbe + "=" + strings.Join(names, ",")
	return func(label string) string {
		if label == streamLabelsProbe {
			return joined
		}
		if labelFn == nil {
			return label
		}
		return labelFn(label)
	}
}

// plainStreamLabels returns the names WithStreamLabels carries.
func plainStreamLabels(labelFn LabelTranslateFunc) []string {
	if labelFn == nil {
		return nil
	}
	if r, ok := strings.CutPrefix(labelFn(streamLabelsProbe), streamLabelsProbe+"="); ok {
		return strings.Split(r, ",")
	}
	return nil
}

// isPlainParserStage reports whether the stage can overwrite a stored field of
// a name it extracts: a key parser, regexp or pattern.
func isPlainParserStage(stage string) bool {
	return isKeyParserStage(stage) || strings.HasPrefix(stage, "regexp ") || strings.HasPrefix(stage, "pattern ")
}

// svCopyPipes copies the stored value of each name aside.
func svCopyPipes(names []string) string {
	var b strings.Builder
	for _, n := range names {
		b.WriteString(" | copy " + n + " as " + svScratch + n)
	}
	return strings.TrimPrefix(b.String(), " ")
}

// svFillPipes follows a parser stage. Metric: the stored value is put back
// into the name. Log: the scratch field takes the parsed value (any original
// key the name sanitizes from) while the entry has no stored one.
func svFillPipes(names []string, metric bool) string {
	var b strings.Builder
	for _, n := range names {
		sv := svScratch + n
		if metric {
			b.WriteString(" | format if (" + sv + `:*) "<` + sv + `>" as ` + n)
			continue
		}
		for _, key := range parsedKeyVariants(n) {
			b.WriteString(" | format if (NOT " + sv + ":* " + strconv.Quote(key) + `:*) "<` + key + `>" as ` + sv)
		}
	}
	return strings.TrimPrefix(b.String(), " ")
}

// stringLiteralRE matches a quoted or backtick string of a pipeline.
var stringLiteralRE = regexp.MustCompile("\"(?:[^\"\\\\]|\\\\.)*\"|`[^`]*`")

// filteredNames returns the names a stage of the pipeline reads in a log
// query: a label filter compares them (the name follows a stage, an and / or
// operand or a list item, and a comparison operator follows it), a label_format
// takes them as a source (`x=name`), or a label_format / line_format template
// reads them (`{{.name}}`). A log query needs the stored value aside for those
// only.
func filteredNames(pipeline string, names []string) []string {
	stripped := stringLiteralRE.ReplaceAllString(pipeline, `""`)
	var out []string
	for _, n := range names {
		q := regexp.QuoteMeta(n)
		if regexp.MustCompile(`(?:^|[\s|(,])`+q+`\s*(?:=|!=|!~|>|<)`).MatchString(stripped) ||
			regexp.MustCompile(`\blabel_format\b[^|]*=\s*`+q+`\s*(?:,|\||$)`).MatchString(stripped) ||
			regexp.MustCompile(`\{\{[^}]*\.`+q+`\b`).MatchString(pipeline) {
			out = append(out, n)
		}
	}
	return out
}

// svRewriteRefs makes the translation of a label_format or line_format stage
// read the scratch field of each name instead of the stored field.
func svRewriteRefs(translated string, names []string) string {
	for _, n := range names {
		translated = strings.ReplaceAll(translated, "<"+n+">", "<"+svScratch+n+">")
		translated = strings.ReplaceAll(translated, "("+n+":*)", "("+svScratch+n+":*)")
	}
	return translated
}

// svNamesNotFormatted returns the names no earlier label_format set.
func svNamesNotFormatted(names []string, formatted map[string]bool) []string {
	var out []string
	for _, n := range names {
		if !formatted[n] {
			out = append(out, n)
		}
	}
	return out
}
