package proxy

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// emulateDetectedLevelChain evaluates translator.BuildDetectedLevelChain over
// one row's fields the way VictoriaLogs runs it, so the unit tests can follow
// the chain's own answer (and where it differs from Loki's) without a
// VictoriaLogs binary: the first non-empty stored level field decides and a
// known spelling is normalized; otherwise a line starting with a brace is
// unpacked as JSON for the level keys in the chain's fixed order, then as
// logfmt, and the first value found counts when it is a known spelling; else
// the line is scanned for a keyword, and a stored word the scan does not
// replace stays as it is. The chain's real answers are pinned against Loki by
// the e2e test TestRangeMetricCompatibilityJSONDetectedLevelProbes.
func emulateDetectedLevelChain(values map[string]string) string {
	names := translator.DetectedLevelFieldNames
	stored := ""
	for _, name := range names[:len(names)-1] {
		if v := values[name]; v != "" {
			stored = v
			break
		}
	}
	value := stored
	if stored == "" {
		json, logfmt := vlUnpackJSONFields(values["_msg"]), slidingLogfmtFields(values["_msg"])
		for _, fields := range []map[string]string{json, logfmt} {
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
	if stored == "" {
		if level := levelFromKeywords([]byte(values["_msg"])); level != levelUnknown {
			return level
		}
	}
	if stored != "" {
		return stored // a stored word Loki does not know stays as it is
	}
	if v := values["severity_number"]; v != "" {
		return levelFromSeverityNumber([]byte(v))
	}
	return levelUnknown
}
