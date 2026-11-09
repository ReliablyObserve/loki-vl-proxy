package translate

import (
	"slices"
	"sort"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The builders of the json and logfmt parsers and of drop and keep in a log query: the pipes the string
// translator writes for them, built from the typed stages. The string translator reads the stage's text, so a
// builder answers false for the rare spelling whose text it reads differently from the typed stage, and the
// lowering leaves that query to the translator.
//
// The translator writes the same pipe for every json and logfmt stage, whatever its flags and field list:
// | unpack_json and | unpack_logfmt unpack every key. Only the labels a json expression defines
// (`| json a="b.c"`) are copied from the key they name (JSONAliasCopies).

// extractedSuffix is the suffix Loki gives a parsed label that collides with a stream label.
const extractedSuffix = "_extracted"

// levelSourceFields are the stored fields detected_level is derived from, other than detected_level itself, in
// the priority order the derivation reads them. A log query's drop keeps them in the row until the response is
// built.
var levelSourceFields = []string{
	"level", "LEVEL", "Level", "log.level",
	"severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl",
	"severity_text", "Severity_Text", "SEVERITY_TEXT",
	"severity_number",
}

// LevelSourceFields are the stored fields detected_level is derived from, other than detected_level itself.
func LevelSourceFields() []string { return slices.Clone(levelSourceFields) }

// JSONUnpack is the pipe of a json stage, whatever its field list: the string translator unpacks every key.
func JSONUnpack() logsql.Pipe { return logsql.PipeUnpackJSON{} }

// LogfmtUnpack is the pipe of a logfmt stage, whatever its flags and field list.
func LogfmtUnpack() logsql.Pipe { return logsql.PipeUnpackLogfmt{} }

// JSONAliasCopies are the `| copy "x.y" as a` pipes that give the labels a json expression defines
// (`| json a="x.y"`) their value as fields, in the order of the aliases. fields and param are the stage's
// extraction list and its text. ok is false when the string translator, which reads the text, would read it
// differently: an expression written with backslashes or as a raw string, one holding a comma, one that starts
// or ends with a space or is a bracket path, a name written twice, or a stage text with a line break.
func JSONAliasCopies(fields []logql.ExtractionField, param string) (pipes []logsql.Pipe, ok bool) {
	for i := 0; i < len(param); i++ {
		switch param[i] {
		case '\\', '`', '\n', '\r', '\t':
			return nil, false
		}
	}
	aliases := map[string]string{}
	seen := map[string]bool{}
	for _, f := range fields {
		if seen[f.Name] {
			return nil, false
		}
		seen[f.Name] = true
		if f.Expression == f.Name {
			continue // a bare name, or an expression that is the name: no copy
		}
		expr := f.Expression
		if expr == "" {
			continue
		}
		first, _ := utf8.DecodeRuneInString(expr)
		last, _ := utf8.DecodeLastRuneInString(expr)
		if unicode.IsSpace(first) || unicode.IsSpace(last) || first == '[' || hasByte(expr, ',') || hasByte(expr, '"') {
			return nil, false
		}
		aliases[f.Name] = expr
	}
	names := make([]string, 0, len(aliases))
	for alias := range aliases {
		names = append(names, alias)
	}
	sort.Strings(names)
	for _, alias := range names {
		name := alias
		if hasByte(alias, '.') || hasByte(alias, '-') {
			name = strconv.Quote(alias)
		}
		pipes = append(pipes, logsql.PipeCopy{Pairs: [][2]string{{strconv.Quote(aliases[alias]), name}}})
	}
	return pipes, true
}

// DropDelete is the `| delete` pipe of a log query's drop stage over its bare labels: the labels detected_level
// is derived from stay in the row, and a drop of matchers writes no pipe (the proxy applies it to the response).
// ok is false when there is no pipe to write.
func DropDelete(labels []string) (logsql.Pipe, bool) {
	var kept []string
	for _, l := range labels {
		if !slices.Contains(levelSourceFields, l) {
			kept = append(kept, l)
		}
	}
	if len(kept) == 0 {
		return nil, false
	}
	return logsql.PipeDelete{Labels: kept}, true
}

// KeepFields is the `| fields` pipe of a keep stage over the names it lists, in the order written: the row's
// time, line and stream, the names, the names without the _extracted suffix Loki gives a collision, level when
// detected_level is kept without it, and the stored names label translates them to.
func KeepFields(names []string, label func(string) string) logsql.Pipe {
	fields := slices.Clone(names)
	// A parsed label Loki renamed name_extracted is the stored field name after unpack_*: keep that too.
	n := len(fields)
	for i := 0; i < n; i++ {
		field := fields[i]
		if len(field) > len(extractedSuffix) && field[len(field)-len(extractedSuffix):] == extractedSuffix {
			if b := field[:len(field)-len(extractedSuffix)]; bareIdentifier(b) && !slices.Contains(fields, b) {
				fields = append(fields, b)
			}
		}
	}
	if slices.Contains(fields, DetectedLevelLabel) && !slices.Contains(fields, "level") {
		fields = append(fields, "level")
	}
	if label != nil {
		for _, field := range slices.Clone(fields) {
			if field == DetectedLevelLabel || field == "service_name" {
				continue // derived labels have their own read-path projection
			}
			if mapped := label(field); mapped != "" && mapped != field && !slices.Contains(fields, mapped) {
				fields = append(fields, mapped)
			}
		}
	}
	return logsql.PipeFields{Labels: append([]string{"_time", "_msg", "_stream"}, fields...)}
}

// bareIdentifier reports whether s is letters, digits and underscores.
func bareIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

func hasByte(s string, c byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return true
		}
	}
	return false
}

// streamLabelsProbe is the label name the proxy's translation function answers with the stream label names to
// read as stream values: WithStreamLabels of the string translator wraps the function that way.
const streamLabelsProbe = "\x03"

// StreamLabelList is the comma-joined Loki stream label names the label function carries, as it answers the
// probe, and false when it carries none.
func StreamLabelList(label func(string) string) (string, bool) {
	if label == nil {
		return "", false
	}
	r := label(streamLabelsProbe)
	if len(r) < 2 || r[0] != streamLabelsProbe[0] || r[1] != '=' {
		return "", false
	}
	return r[2:], true
}

// StreamLabelNames are the Loki stream label names the label function carries, nil when it carries none.
func StreamLabelNames(label func(string) string) []string {
	list, ok := StreamLabelList(label)
	if !ok {
		return nil
	}
	var names []string
	start := 0
	for i := 0; i <= len(list); i++ {
		if i == len(list) || list[i] == ',' {
			names = append(names, list[start:i])
			start = i + 1
		}
	}
	return names
}
