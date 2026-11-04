package translator

import (
	"regexp"
	"slices"
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

// VictoriaLogs' Loki push API unpacks the keys of a JSON line into fields at
// ingest, after the stream labels (app/vlinsert/loki/loki_json.go
// parseJSONRequest, addMsgField), and a block keeps the last value written for
// a name (lib/logstorage/block.go mustInitFromRows). A line whose JSON has a
// key named like one of its stream labels therefore stores the line's value
// under the label's name, while _stream keeps the label. In Loki the stream
// label wins and the key is the line's (version_extracted after a parser).
//
// A metric query reads its names (grouping, filters, unwrap) from the field,
// so before its first pipe the translation writes each name's value from
// _stream back into the field (withStreamTagRestore). An entry whose stream
// has no such label keeps the stored field: a structured metadata label, or a
// field VictoriaLogs did not take into the stream. VictoriaLogs runs the
// extract pipes once per run of equal _stream values, that is once per block.

// stScratch names the scratch field holding a name's quoted stream value.
const stScratch = "__lxst_"

// quotedBody matches the inside of a Go-quoted string: runs of plain bytes and
// backslash escapes. The regexp spells the quote \x22 and has no alternation,
// so the pipe holds no '"' or '|' that a quote-aware pipe scanner could take
// for a string or a pipe boundary.
const quotedBody = `[^\x22\\]*(?:\\.[^\x22\\]*)*`

// streamTagRestorePipes writes the value each name has in the entry's _stream
// into the field of that name. _stream is `{name="value",...}`, names sorted,
// values Go-quoted (lib/logstorage/rows.go marshalToStreamTag): the anchored
// regexp walks whole tags, so a name inside another tag's value or a longer
// name ending in it never matches; extract unquotes the captured value and
// keeps the field when the stream has no such tag (skip_empty_results).
func streamTagRestorePipes(names []string) string {
	var b strings.Builder
	i := 0
	for _, n := range names {
		if slices.Contains(DetectedLevelFieldNames, n) {
			// The detected_level derivation reads these names as stored
			// (severity-detected-level-derivation); they keep the stored field.
			continue
		}
		scratch := stScratch + strconv.Itoa(i)
		re := `^\{(?:[^=\x22]*=\x22` + quotedBody + `\x22,)*?` + regexp.QuoteMeta(n) + `=(?P<` + scratch + `>\x22` + quotedBody + `\x22)`
		b.WriteString(" | extract_regexp `" + re + "` from _stream")
		b.WriteString(" | extract \"<" + n + ">\" from " + scratch + " skip_empty_results")
		b.WriteString(" | delete " + scratch)
		i++
	}
	return strings.TrimPrefix(b.String(), " ")
}

// streamTagRestorePrefix starts every restore that streamTagRestorePipes writes.
const streamTagRestorePrefix = "| extract_regexp `^\\{(?:[^=\\x22]*=\\x22"

// WithoutStreamTagRestore removes the stream value restore of a translation
// (streamTagRestorePipes): it reads no line, so a check for parser stages of
// the query must not see its extract pipes.
func WithoutStreamTagRestore(query string) string {
	if !strings.Contains(query, stScratch) {
		return query
	}
	for {
		start := strings.Index(query, streamTagRestorePrefix)
		if start < 0 {
			return query
		}
		del := strings.Index(query[start:], "| delete "+stScratch)
		if del < 0 {
			return query
		}
		end := start + del + len("| delete "+stScratch)
		for end < len(query) && query[end] >= '0' && query[end] <= '9' {
			end++
		}
		if end < len(query) && query[end] == ' ' {
			end++
		}
		query = query[:start] + query[end:]
	}
}

// withStreamTagRestore inserts streamTagRestorePipes before the first pipe of
// a metric query's parts, after the filters VictoriaLogs evaluates first.
func withStreamTagRestore(parts []string, names []string) []string {
	restore := streamTagRestorePipes(names)
	if restore == "" {
		return parts
	}
	at := len(parts)
	for i, part := range parts {
		if strings.HasPrefix(strings.TrimSpace(part), "|") {
			at = i
			break
		}
	}
	return append(parts[:at:at], append([]string{restore}, parts[at:]...)...)
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
