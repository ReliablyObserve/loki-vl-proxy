package translate

import (
	"regexp"
	"strings"
	"testing"
)

// The level of a row a stored level field decides is computed in Go from the
// grouped stored value; it must be what the chain's spelling pipes give: the
// anchored, case-insensitive regexps of detectedLevelSpellings, and any other
// word as stored.
// conformance: severity-detected-level-derivation
func TestNormalizeStoredLevelMatchesTheChain(t *testing.T) {
	corpus := []string{"info", "INFO", "Info", "inf", "Information", "INFORMATION", "warn", "WRN", "Warning", "WARNING", "error", "ERR", "Err",
		"critical", "CRITICAL", "fatal", "Fatal", "trace", "TRC", "debug", "DBG", "unknown", "notice", "NOTICE", "verbose", "30", "true", "",
		" info", "info ", "info\n", "infoo", "in fo", "warning2", "ünfo", "ınfo", "ſtrace"}
	for _, value := range corpus {
		want := value
		for _, s := range detectedLevelSpellings {
			if regexp.MustCompile("(?i)^(" + strings.Join(s.spellings, "|") + ")$").MatchString(value) {
				want = s.level
				break
			}
		}
		if got := NormalizeStoredLevel(value); got != want {
			t.Errorf("NormalizeStoredLevel(%q) = %q, the chain's pipes give %q", value, got, want)
		}
	}
}
