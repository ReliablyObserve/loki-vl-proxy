package translate

import (
	"regexp"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The builders of the regexp and pattern parsers and of decolorize in a log query: the pipes the string
// translator writes for them, and the label names it reads from their text. The string translator keeps the
// text of the stage's argument (a double-quoted one as written, a backtick one quoted); the typed stage holds
// the argument unquoted, so the builders quote it the way the translator quotes a backtick argument, which is
// also its text for a double-quoted argument written with the escapes strconv.Quote writes.

// ExtractRegexp is the pipe of a `| regexp` stage: `| extract_regexp "<regexp>"`, the regexp Go-quoted.
func ExtractRegexp(pattern string) logsql.Pipe {
	return logsql.PipeExtractRegexp{Pattern: pattern, Quoted: true}
}

// ExtractPattern is the pipe of a `| pattern` stage: `| extract "<pattern>"`. ok is false for a wildcard-only
// pattern (NoopPattern), which writes no pipe.
func ExtractPattern(pattern string) (logsql.Pipe, bool) {
	if NoopPattern(pattern) {
		return nil, false
	}
	return logsql.PipeExtract{Pattern: pattern}, true
}

// Decolorize is the pipe of a `| decolorize` stage.
func Decolorize() logsql.Pipe { return logsql.PipeDecolorize{} }

// NoopPattern reports whether a pattern is a wildcard-only expression. VictoriaLogs' extract needs named
// placeholders and rejects these, which Drilldown flows emit, so the stage is a no-op.
func NoopPattern(pattern string) bool {
	switch strings.TrimSpace(pattern) {
	case "(.*)", ".*", "^.*$", "(?s).*", "(?s:.*)":
		return true
	}
	return false
}

// RegexpCaptureNames are the names of the named groups of a regexp, the labels a `| regexp` stage defines. The
// string translator reads them with regexp.Compile, so a regexp Go does not compile defines none.
func RegexpCaptureNames(pattern string) []string {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}
	var names []string
	for _, n := range re.SubexpNames() {
		if n != "" {
			names = append(names, n)
		}
	}
	return names
}

var patternCaptureRE = regexp.MustCompile(`<([A-Za-z][A-Za-z0-9_]*)>`)

// PatternCaptureNames are the names the placeholders of a `| pattern` text define, in the order written and
// with repeats: `<name>` with a name that starts with a letter (the string translator's reading; `<_>` and a
// name that starts with an underscore define none).
func PatternCaptureNames(text string) []string {
	var out []string
	for _, m := range patternCaptureRE.FindAllStringSubmatch(text, -1) {
		out = append(out, m[1])
	}
	return out
}
