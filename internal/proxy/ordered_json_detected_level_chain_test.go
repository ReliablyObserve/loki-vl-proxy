package proxy

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// emulateDetectedLevelChainFor evaluates translator.BuildDetectedLevelChain over
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
// A chain pruned to the fields and keys of present (nil: the full chain) reads
// no other: a field or key it does not list is not read, as if the row did not
// hold it.
func emulateDetectedLevelChainFor(values map[string]string, present *translator.DetectedLevelPresence) string {
	names := translator.DetectedLevelFieldNames
	keys := translator.DetectedLevelBodyKeys()
	if present != nil {
		kept := map[string]string{"_msg": values["_msg"]}
		for _, name := range present.Stored {
			kept[name] = values[name]
		}
		values, keys = kept, present.BodyKeys
	}
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
			for _, name := range keys {
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
	// the pipes that read a level from the severity names VictoriaLogs derived
	// from the OTLP number (before v1.50) are part of the chain only when the
	// window holds a severity field or a number
	if m := severityDerivedName.FindStringSubmatch(values["severity"]); m != nil && (present == nil || !containsString(present.Stored, noSeverityNamePipes)) {
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

// noSeverityNamePipes marks, in the presence read back from a chain, that it
// lacks the pipes mapping a derived severity name to a level.
const noSeverityNamePipes = "\x00no-severity-name-pipes"

var (
	chainStoredPickRE = regexp.MustCompile(`"<([^>]+)>" as __dl_s keep_original_fields`)
	chainBodyKeysRE   = regexp.MustCompile(`unpack_json[^|]*? fields \(([^)]*)\) result_prefix "__j_"`)
)

// chainPresence reads the stored fields and body keys a detected_level chain
// in query was built from.
func chainPresence(query string) *translator.DetectedLevelPresence {
	present := &translator.DetectedLevelPresence{}
	for _, m := range chainStoredPickRE.FindAllStringSubmatch(query, -1) {
		present.Stored = append(present.Stored, m[1])
	}
	if strings.Contains(query, `severity_number:*) "info" as __dl keep_original_fields`) {
		present.Stored = append(present.Stored, "severity_number")
	}
	if !strings.Contains(query, "-severity_number:* severity:~") {
		present.Stored = append(present.Stored, noSeverityNamePipes)
	}
	if m := chainBodyKeysRE.FindStringSubmatch(query); m != nil {
		present.BodyKeys = strings.Split(m[1], ", ")
	}
	return present
}

// chainStart reports whether pipe is the first of a detected_level chain,
// whatever its pruning leaves first.
func chainStart(pipe string) bool {
	if strings.HasPrefix(pipe, "copy ") {
		return false // the stored-level copies of the cheap plan
	}
	return strings.Contains(pipe, "__dl") || strings.Contains(pipe, `result_prefix "__j_"`)
}

// withoutLevelChain cuts the detected_level chain out of query.
func withoutLevelChain(query string) string {
	const chainEnd = "| delete __dl, __dl_*, __j_*, __l_*"
	from := len(query)
	for _, marker := range []string{"__dl", `result_prefix "__j_"`} {
		if i := strings.Index(query, marker); i >= 0 && i < from {
			from = i
		}
	}
	from = strings.LastIndex(query[:from], " | ")
	return query[:from] + query[strings.Index(query, chainEnd)+len(chainEnd):]
}

// isLevelPresenceQuery reports whether query is the presence probe.
func isLevelPresenceQuery(query string) bool {
	return strings.Contains(query, ` | stats count() if (`) && strings.Contains(query, ` as p0, `)
}

// presenceBase is the part of a presence probe the stats query shares.
func presenceBase(query string) string {
	return query[:strings.Index(query, " | format if (")]
}

// levelPresenceResponse evaluates the presence probe over the values of the
// rows its base selects, the way VictoriaLogs counts them: a stored field is
// present when some row holds it non-empty, a body key when its words are
// tokens of the line of a row no definite level field decides, and an escape
// when such a line holds a backslash and a u.
func levelPresenceResponse(rows []map[string]string) string {
	counts := make([]int, levelPresenceMaskLen())
	for _, row := range rows {
		decided := false
		for i, name := range levelPresenceStored {
			if row[name] == "" {
				continue
			}
			counts[i]++
			decided = decided || !containsString(levelPresenceUndecided, name)
		}
		if decided {
			continue
		}
		tokens := map[string]bool{}
		for _, w := range strings.FieldsFunc(row["_msg"], notLevelWordRune) {
			tokens[w] = true
		}
		for i, key := range levelPresenceBody {
			all := true
			for _, w := range strings.FieldsFunc(key, notLevelWordRune) {
				all = all && tokens[w]
			}
			if all {
				counts[len(levelPresenceStored)+i]++
			}
		}
		if strings.Contains(row["_msg"], `\u`) {
			counts[len(counts)-1]++
		}
	}
	out := map[string]string{}
	for i, n := range counts {
		out["p"+strconv.Itoa(i)] = strconv.Itoa(n)
	}
	line, _ := json.Marshal(out)
	return string(line) + "\n"
}

// chainStatsCalls drops the stored-level attempts from the stats queries a
// plan reading detected_level sent: the buckets grouped by the stored fields,
// which end in errLevelNotStored when a row has no stored level and are asked
// again with the chain.
func chainStatsCalls(calls []string) []string {
	var out []string
	for _, call := range calls {
		if !strings.Contains(call, "copy detected_level as __dl_d, level as __dl_l") {
			out = append(out, call)
		}
	}
	return out
}
