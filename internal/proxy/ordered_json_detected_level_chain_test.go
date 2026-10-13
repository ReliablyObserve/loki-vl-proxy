package proxy

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// emulateDetectedLevelChain evaluates translator.BuildDetectedLevelChain over
// one row's fields the way VictoriaLogs runs it, so the unit tests can follow
// the chain's own answer (and where it differs from Loki's) without a
// VictoriaLogs binary: the first non-empty stored level field decides (a
// severity text VictoriaLogs derived from the OTLP number is none) and a known
// spelling is normalized; otherwise, for a row no stored field decides, a line
// starting with a brace is unpacked as JSON for the level keys in the chain's
// fixed order and any other line as logfmt, and the first value found counts
// when it is a known spelling; else the line is scanned for a keyword, and a
// stored word the scan does not replace stays as it is; last the OTLP number.
// The chain's real answers are pinned against Loki by the e2e test
// TestRangeMetricCompatibilityJSONDetectedLevelProbes.
func emulateDetectedLevelChain(values map[string]string) string {
	names := translator.DetectedLevelFieldNames
	number := values["severity_number"]
	stored, has := "", number != ""
	for _, name := range names[:len(names)-1] {
		v := values[name]
		if v == "" {
			continue
		}
		has = true
		if stored == "" && !synthesizedSeverity(name, v, number) {
			stored = v
		}
	}
	if has && unspecifiedOnly(values) {
		has = false // translator.BuildHasLevelFilter excludes these rows
	}
	value := stored
	if !has {
		fields := []map[string]string{vlUnpackJSONFields(values["_msg"])}
		if !bracedLine.MatchString(values["_msg"]) {
			fields = append(fields, slidingLogfmtFields(values["_msg"]))
		}
		for _, fields := range fields {
			for _, name := range names[1 : len(names)-1] {
				if v := fields[name]; v != "" && value == "" {
					value = v
				}
			}
		}
	}
	if level := canonicalLevel([]byte(value)); level != "" {
		return level
	}
	if !has {
		if level := levelFromKeywords([]byte(values["_msg"])); level != levelUnknown {
			return level
		}
	}
	if stored != "" {
		return stored // a stored word Loki does not know stays as it is
	}
	if number != "" {
		return levelFromSeverityNumber([]byte(number))
	}
	if m := severityDerivedName.FindStringSubmatch(values["severity"]); m != nil {
		return strings.ToLower(m[1])
	}
	return levelUnknown
}

var (
	bracedLine          = regexp.MustCompile(translator.BracedLineRegexp)
	severityDerivedName = regexp.MustCompile(`^(Trace|Debug|Info|Warn|Error|Fatal)[234]?$`)
)

// synthesizedSeverity reports whether a stored level field holds a text
// VictoriaLogs derived from the OTLP severity number: severity_text naming the
// family of its number (Info, Info2..4 for 9..12) or Unspecified for a number
// outside 1..24, and, from before v1.50, a severity field naming a level
// without any number stored.
func synthesizedSeverity(name, value, number string) bool {
	switch name {
	case "severity":
		return number == "" && (value == "Unspecified" || severityDerivedName.MatchString(value))
	case "severity_text":
		n, err := strconv.Atoi(number)
		if err != nil {
			return false
		}
		if n < 1 || n > 24 {
			return value == "Unspecified"
		}
		return strings.HasPrefix(value, vlSeverityTexts[(n-1)/4*4+1]) && severityDerivedName.MatchString(value)
	}
	return false
}

// unspecifiedOnly reports whether the only level text of a row is the
// "Unspecified" VictoriaLogs writes for an OTLP record without severity: with
// severity_number 0 in severity_text, or alone in severity.
func unspecifiedOnly(values map[string]string) bool {
	names := translator.DetectedLevelFieldNames
	others := func(skip ...string) bool {
		for _, name := range names[:len(names)-1] {
			if values[name] != "" && !containsString(skip, name) {
				return false
			}
		}
		return true
	}
	return values["severity_text"] == "Unspecified" && values["severity_number"] == "0" && others("severity_text") ||
		values["severity"] == "Unspecified" && values["severity_number"] == "" && others("severity")
}
