package translate

// The tables and the matcher the detected_level builders read, for the typed builders of the query plan
// (internal/queryplan/lower): the data the text builders above are written from, and nothing that builds text.

// LevelSpelling is a canonical level and the stored spellings that normalise to it (case-insensitive).
type LevelSpelling struct {
	Level     string
	Spellings []string
}

// levelSpellingTable is detectedLevelSpellings as LevelSpelling values.
var levelSpellingTable = func() []LevelSpelling {
	out := make([]LevelSpelling, len(detectedLevelSpellings))
	for i, s := range detectedLevelSpellings {
		out[i] = LevelSpelling{Level: s.level, Spellings: s.spellings}
	}
	return out
}()

// DetectedLevelSpellings lists every canonical level with its spellings, in the builders' order. Callers must not
// change it.
func DetectedLevelSpellings() []LevelSpelling { return levelSpellingTable }

// DetectedLevelCanonical lists every value the body chain can produce, in the builders' order. Callers must not
// change it.
func DetectedLevelCanonical() []string { return detectedLevelCanonical }

// SeverityNumberRegexp is the regexp of the severity_number values that map to level.
func SeverityNumberRegexp(level string) (string, bool) {
	re, ok := detectedLevelSeverityNumberRegexp[level]
	return re, ok
}

// LevelKeywords are the words of Loki's keyword scan, in its order. Callers must not change it.
func LevelKeywords() []string { return levelKeywords }

// LevelKeywordRegexp is the regexp the chain runs over the lines that need the keyword scan.
func LevelKeywordRegexp() string { return levelKeywordRegexp }

// SeverityFamily is a family of OTel severity numbers VictoriaLogs names after one stem (Info for 9, Info2 for 10).
type SeverityFamily struct {
	Stem   string
	Lo, Hi int
}

// severityFamilyTable is vlSeverityFamilies as SeverityFamily values.
var severityFamilyTable = func() []SeverityFamily {
	out := make([]SeverityFamily, len(vlSeverityFamilies))
	for i, f := range vlSeverityFamilies {
		out[i] = SeverityFamily{Stem: f.stem, Lo: f.lo, Hi: f.hi}
	}
	return out
}()

// SeverityFamilies lists the families, in the builders' order. Callers must not change it.
func SeverityFamilies() []SeverityFamily { return severityFamilyTable }

// DetectedLevelMatch is what a filter `detected_level <op> "<value>"` accepts, as the builders read it.
type DetectedLevelMatch struct {
	Op, Value string
	// Negated is != and !~; Regexp is =~ and !~.
	Negated, Regexp bool
	// Targets are the canonical values the positive matcher accepts.
	Targets []string
}

// MatchDetectedLevel reads a filter on detected_level as BuildDetectedLevelFilter does; it fails on an operator
// other than =, !=, =~ and !~ and on a regexp that does not compile.
func MatchDetectedLevel(op, value string) (DetectedLevelMatch, error) {
	m, err := newDetectedLevelMatcher(op, value)
	if err != nil {
		return DetectedLevelMatch{}, err
	}
	return DetectedLevelMatch{Op: op, Value: value, Negated: m.negated(), Regexp: m.re != nil, Targets: m.canonicalTargets()}, nil
}

// DetectedLevelRawValues are the stored words the positive matcher m accepts that Loki keeps as they are, and
// whether a regexp's words cannot be listed (the prefilter reads them).
func DetectedLevelRawValues(m DetectedLevelMatch) (raws []string, anyRaw bool) {
	return rawLevelValues(m.Value, m.Regexp)
}
