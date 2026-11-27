package translate

import (
	"strconv"
	"unicode"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The collision rename of a parsed label. Loki gives a parsed label that shares its name with a stream label the
// suffix _extracted and keeps the stream label; VictoriaLogs' unpack_json and unpack_logfmt overwrite the stored
// field. A label filter on name_extracted therefore reads the parsed value of the key name where the entry has the
// stream label name, and the key name_extracted where it has not. The string translator unpacks the keys of
// those labels a second time into scratch fields behind each parser, picks the first parser's value into one
// field before the filter, filters on it and deletes the scratch fields at the end. The builders below are that
// text for json and logfmt parsers that extract every key.

// ExtractedScratch prefixes the scratch fields the collision rename uses.
const ExtractedScratch = "__lxp"

// ExtractedSuffix is the suffix Loki gives a parsed label that collides with a stream label.
const ExtractedSuffix = "_extracted"

// ExtractedBase is the label a name_extracted label may be the collision rename of: the part before the suffix,
// when it is letters, digits and underscores.
func ExtractedBase(label string) (string, bool) {
	n := len(label) - len(ExtractedSuffix)
	if n <= 0 || label[n:] != ExtractedSuffix {
		return "", false
	}
	base := label[:n]
	for _, r := range base {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return "", false
		}
	}
	return base, true
}

// LogsQLFieldName is a field name as LogsQL needs it written: quoted unless it is letters, digits, underscores
// and dots.
func LogsQLFieldName(name string) string {
	for _, c := range name {
		if c != '_' && c != '.' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return logsql.QuoteValue(name)
		}
	}
	return name
}

// extractedCollides is the condition under which the label base is a stream label of the entry, so a parsed key
// base is renamed: every stream has service_name in Loki; any other name is in _stream (a dotted spelling too), or
// one of aliasFields is (the stored fields of an alias, lower.Profile AliasField).
func extractedCollides(base string, aliasFields []string) logsql.FilterExpr {
	value := logsql.FieldFilter{Field: ExtractedScratch + "_" + base, Op: logsql.FieldOpAny}
	if base == "service_name" {
		return value
	}
	return logsql.AllOf{
		logsql.FieldFilter{Field: ExtractedScratch + "_stream", Op: logsql.FieldOpRegexp, Value: streamLabelPattern(base, aliasFields)},
		value,
	}
}

// StreamLabelPattern is the regexp a _stream copy matches when it holds the stream label base: the label under its
// own name in either spelling, or one of aliasFields.
func StreamLabelPattern(base string, aliasFields []string) string {
	return streamLabelPattern(base, aliasFields)
}

func streamLabelPattern(base string, aliasFields []string) string {
	name := dotOrUnderscore(base)
	if len(aliasFields) == 0 {
		return `[{,]` + name + `="`
	}
	b := make([]byte, 0, len(name)+32)
	b = append(b, "[{,](?:"...)
	b = append(b, name...)
	for _, f := range aliasFields {
		b = append(b, '|')
		for i := 0; i < len(f); i++ {
			switch c := f[i]; c {
			case '\\', '.', '+', '*', '?', '(', ')', '|', '[', ']', '{', '}', '^', '$':
				b = append(b, '\\', c)
			default:
				b = append(b, c)
			}
		}
	}
	b = append(b, ")=\""...)
	return string(b)
}

// ExtractedFilter is the filter on the label base_extracted: parsed, a filter on the scratch field that holds
// the parsed value of the key base, applies where the entry has the stream label base; literal, the filter on
// the keys the label can be stored under, where it has not. ok is false when parsed holds a top-level space (an
// alternation), which the string translator joins to the condition by plain text, so that it changes meaning.
func ExtractedFilter(base string, parsed, literal logsql.FilterExpr) (logsql.FilterExpr, bool) {
	return ExtractedFilterAliased(base, nil, parsed, literal)
}

// ExtractedFilterAliased is ExtractedFilter where the stream label base may also be stored as one of aliasFields
// (StreamAliasFields).
func ExtractedFilterAliased(base string, aliasFields []string, parsed, literal logsql.FilterExpr) (logsql.FilterExpr, bool) {
	if HasTopLevelSpace(parsed.String()) {
		return nil, false
	}
	collides := extractedCollides(base, aliasFields)
	return logsql.Group{Expr: logsql.AnyOf{
		logsql.Group{Expr: logsql.AllOf{collides, parsed}},
		logsql.Group{Expr: logsql.AllOf{logsql.NotExpr{Expr: logsql.Group{Expr: collides}}, logsql.Group{Expr: literal}}},
	}}, true
}

// ExtractedScratchField is the field of the scratch unpack of parser i (0-based, in query order) for the key.
func ExtractedScratchField(i int, key string) string {
	return LogsQLFieldName(ExtractedScratch + strconv.Itoa(i) + "_" + key)
}

// ExtractedUnpack is the pipe behind parser i that unpacks the keys of the bases into scratch fields, for a
// parser that extracts every key.
func ExtractedUnpack(logfmt bool, i int, bases []string) logsql.Pipe {
	fields := make([]string, len(bases))
	for k, b := range bases {
		fields[k] = LogsQLFieldName(b)
	}
	prefix := ExtractedScratch + strconv.Itoa(i) + "_"
	if logfmt {
		return logsql.PipeUnpackLogfmt{From: "_msg", Fields: fields, ResultPrefix: prefix}
	}
	return logsql.PipeUnpackJSON{From: "_msg", Fields: fields, ResultPrefix: prefix}
}

// ExtractedCoalesce are the pipes ahead of a filter that give each base its Loki value in __lxp_base: the value
// of the first parser, over the n parsers before the filter that extracted it. streamCopied holds whether
// _stream was copied aside already; the first base other than service_name copies it.
func ExtractedCoalesce(bases []string, n int, streamCopied *bool) []logsql.Pipe {
	var pipes []logsql.Pipe
	for _, base := range bases {
		if base != "service_name" && !*streamCopied {
			*streamCopied = true
			pipes = append(pipes, logsql.PipeCopy{Pairs: [][2]string{{"_stream", ExtractedScratch + "_stream"}}})
		}
	}
	for _, base := range bases {
		dst := ExtractedScratch + "_" + base
		for j := n - 1; j >= 0; j-- {
			field := ExtractedScratchField(j, base)
			if j == n-1 {
				pipes = append(pipes, logsql.PipeCopy{Pairs: [][2]string{{field, dst}}})
			} else {
				pipes = append(pipes, logsql.PipeFormat{
					Template:    "<" + unquoted(field) + ">",
					ResultField: dst,
					If:          logsql.FieldFilter{Field: field, Op: logsql.FieldOpAny},
				})
			}
		}
	}
	return pipes
}

// ExtractedDelete is the `| delete __lxp*` pipe that ends a query that used the scratch fields.
func ExtractedDelete() logsql.Pipe {
	return logsql.PipeDelete{Labels: []string{ExtractedScratch + "*"}}
}

// dotOrUnderscore is base with each underscore written as the class of a dot or an underscore, the spellings a
// stream label name can have in a stream.
func dotOrUnderscore(base string) string {
	b := make([]byte, 0, len(base)+8)
	for i := 0; i < len(base); i++ {
		if base[i] == '_' {
			b = append(b, "[._]"...)
		} else {
			b = append(b, base[i])
		}
	}
	return string(b)
}

// unquoted is a field name without the double quotes around it.
func unquoted(field string) string {
	if n := len(field); n >= 2 && field[0] == '"' && field[n-1] == '"' {
		return field[1 : n-1]
	}
	return field
}
