package translator

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// LogsQL builders for Loki's detected_level over VictoriaLogs rows.
//
// The value follows the same rule order as the proxy's Go derivation: a stored
// detected_level, then the first stored field of Loki's log_level_fields list,
// then severity_number, then the line body. Stored fields are evaluated with
// index-friendly filters; the body is evaluated by a pipe chain that writes
// __dl (and scratch fields __dl_*, __j_*, __l_*), and only rows without a
// stored level need it.
//
// Known differences of the body chain from the Go derivation, inherent to the
// pipes: unpack_json needs '{' as the first byte and reads keys in field-list
// order rather than document order, nested objects are not searched by key
// (the keyword scan still sees their words), unpack_logfmt keys are
// case-sensitive and do not stop at syntax errors, and the keyword regexp
// folds ASCII only. unpack_logfmt skips the lines Loki reads as JSON, which
// start with a brace (the guard probes check the ones that do not also end
// with one). The stored-field filters do not tell stream fields from
// other fields: a detected_level stream field is an index label for Loki and
// does not decide the value, so callers leave it out of existingFields.

// BracedLineRegexp matches the start of a line Loki's ingest reads as JSON
// (field_detection.go isJSON: the first non-space rune is a brace and the last
// one a closing brace), where it never reads logfmt keys.
const BracedLineRegexp = `^[\s\v\x{85}\p{Z}]*\{`

// DetectedLevelFieldNames lists the stored fields that decide detected_level,
// in priority order: detected_level, Loki's log_level_fields, severity_number.
var DetectedLevelFieldNames = []string{
	"detected_level",
	"level", "LEVEL", "Level", "log.level",
	"severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl",
	"severity_text", "Severity_Text", "SEVERITY_TEXT",
	"severity_number",
}

// detectedLevelSpellings maps each canonical value to the stored spellings
// that normalise to it (case-insensitive).
var detectedLevelSpellings = []struct {
	level     string
	spellings []string
}{
	{"trace", []string{"trace", "trc"}},
	{"debug", []string{"debug", "dbg"}},
	{"info", []string{"info", "inf", "information"}},
	{"warn", []string{"warn", "wrn", "warning"}},
	{"error", []string{"error", "err"}},
	{"critical", []string{"critical"}},
	{"fatal", []string{"fatal"}},
}

// detectedLevelSeverityNumberRegexp maps severity_number values (plain
// decimals, as VictoriaLogs stores them) to canonical values. Non-numeric
// values are info.
var detectedLevelSeverityNumberRegexp = map[string]string{
	"trace":   `^(-[1-9][0-9]*|[1-4])$`,
	"debug":   `^[5-8]$`,
	"info":    `^(9|1[0-2])$`,
	"warn":    `^1[3-6]$`,
	"error":   `^(1[7-9]|20)$`,
	"fatal":   `^2[1-4]$`,
	"unknown": `^(-?0|2[5-9]|[3-9][0-9]|[1-9][0-9]{2,})$`,
}

// detectedLevelCanonical lists every value the body chain can produce.
var detectedLevelCanonical = []string{"trace", "debug", "info", "warn", "error", "critical", "fatal", "unknown"}

// vlSeverityFamilies are the severity texts VictoriaLogs derives from OTel
// severity numbers 1..24 when an OTLP record has no text: the stem for the
// first number of a family of four, the stem with 2, 3 and 4 for the rest
// (Info for 9, Info2 for 10), and Unspecified for 0 and every other number.
var vlSeverityFamilies = []struct {
	stem   string
	lo, hi int
}{{"Trace", 1, 4}, {"Debug", 5, 8}, {"Info", 9, 12}, {"Warn", 13, 16}, {"Error", 17, 20}, {"Fatal", 21, 24}}

// DetectedLevelStoredFields returns the level-deciding fields present in
// existingFields, in priority order.
func DetectedLevelStoredFields(existingFields []string) []string {
	present := make(map[string]struct{}, len(existingFields))
	for _, f := range existingFields {
		present[f] = struct{}{}
	}
	out := make([]string, 0, 2)
	for _, name := range DetectedLevelFieldNames {
		if _, ok := present[name]; ok {
			out = append(out, name)
		}
	}
	return out
}

// DetectedLevelChainMode selects how BuildDetectedLevelChain ends.
type DetectedLevelChainMode int

const (
	// DetectedLevelChainGroup keeps the result in __dl for stats/hits grouping.
	DetectedLevelChainGroup DetectedLevelChainMode = iota
	// DetectedLevelChainLogs writes the result to detected_level for rows
	// without a stored level and deletes every scratch field.
	DetectedLevelChainLogs
	// DetectedLevelChainGrouping writes Loki's value of every row into
	// detected_level, so a stats pipe can group by the Loki label itself.
	DetectedLevelChainGrouping
)

// BuildDetectedLevelChain returns the body chain (starting with "| "). When
// existingFields hold stored level fields, the unpack and regexp pipes skip
// the rows that have one (BuildHasLevelFilter), which a scratch field
// __dl_h marks once instead of repeating the filter in every pipe.
func BuildDetectedLevelChain(existingFields []string, mode DetectedLevelChainMode) string {
	stored := DetectedLevelStoredFields(existingFields)
	cond := ""
	if len(stored) > 0 {
		cond = "-__dl_h:*"
	}
	withCond := func(extra string) string {
		switch {
		case cond == "" && extra == "":
			return ""
		case cond == "":
			return " if (" + extra + ")"
		case extra == "":
			return " if (" + cond + ")"
		default:
			return " if (" + cond + " " + extra + ")"
		}
	}
	names := detectedLevelLogLevelFieldNames()
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = logsqlFieldName(n)
	}
	fieldList := strings.Join(quoted, ", ")

	var b strings.Builder
	// Stored fields first, in Loki's priority order: the first non-empty one
	// decides, and keep_original_fields keeps that first write.
	first := true
	writeStored := func(field string) {
		quotedField := logsqlFieldName(field)
		cond := quotedField + ":*"
		// A severity text VictoriaLogs derived from the OTLP number is no level
		// field of Loki's: the number (or a later field) decides.
		switch field {
		case "severity_text":
			cond += " !" + synthesizedSeverityText() + ` !(severity_text:="Unspecified" severity_number:="0")`
		case "severity":
			cond += " !" + synthesizedSeverityName()
		}
		sep := " | "
		if first {
			sep, first = "| ", false
		}
		fmt.Fprintf(&b, `%sformat if (%s) "<%s>" as __dl_s keep_original_fields`, sep, cond, field)
	}
	for _, field := range DetectedLevelFieldNames {
		if field == "severity_number" {
			continue
		}
		writeStored(field)
	}
	if len(stored) > 0 {
		// A row has a stored level (BuildHasLevelFilter) when __dl_s holds a
		// level text, or the number is one other than the 0 that goes with a
		// synthesised Unspecified, or the severity field names a finer level.
		b.WriteString(` | format if (__dl_s:*) "1" as __dl_h`)
		if hasField(stored, "severity_number") {
			b.WriteString(` | format if (-__dl_h:* severity_number:* !(severity_number:="0" severity_text:="Unspecified")) "1" as __dl_h`)
		}
		if hasField(stored, "severity") {
			b.WriteString(` | format if (-__dl_h:* ` + synthesizedSeverityName() + ` -severity:="Unspecified") "1" as __dl_h`)
		}
	}
	fmt.Fprintf(&b, ` | format if (__dl_s:*) "<__dl_s>" as __dl_v keep_original_fields`)
	fmt.Fprintf(&b, ` | unpack_json%s from _msg fields (%s) result_prefix "__j_"`, withCond(""), fieldList)
	fmt.Fprintf(&b, ` | unpack_logfmt%s from _msg fields (%s) result_prefix "__l_"`, withCond("-_msg:~"+quoteLevelRegexp(BracedLineRegexp)), fieldList)
	for _, prefix := range []string{"__j_", "__l_"} {
		for _, n := range names {
			f := logsqlFieldName(prefix + n)
			fmt.Fprintf(&b, ` | format if (%s:*) "<%s>" as __dl_v keep_original_fields`, f, prefix+n)
		}
	}
	for _, s := range detectedLevelSpellings {
		fmt.Fprintf(&b, " | format if (__dl_v:~%s) %q as __dl keep_original_fields", quoteLevelRegexp("(?i)^("+strings.Join(s.spellings, "|")+")$"), s.level)
	}
	b.WriteString(" | extract_regexp")
	b.WriteString(withCond(`__dl:""`))
	b.WriteString(" ")
	b.WriteString(quoteLevelRegexp(`(?i)(?:^|[ \t\n\[({"=])(?P<__dl_e>trace|debug|fatal|critical|error|err|warning|warn|info)(?:$|[ \t\n\[\](){}:,!"=])`))
	b.WriteString(" from _msg")
	b.WriteString(` | format if (__dl:"" __dl_e:~"(?i)^(error|err)$") "error" as __dl keep_original_fields`)
	b.WriteString(` | format if (__dl:"" __dl_e:~"(?i)^(warning|warn)$") "warn" as __dl keep_original_fields`)
	b.WriteString(` | format if (__dl:"" __dl_e:*) "<lc:__dl_e>" as __dl keep_original_fields`)
	// A stored word Loki does not know stays as it is (a level field holding
	// "notice" is "notice"), unlike a word read from the line.
	b.WriteString(` | format if (__dl:"" __dl_s:*) "<__dl_s>" as __dl keep_original_fields`)
	// severity_number decides only when no level field did, as in Loki.
	for _, level := range detectedLevelCanonical {
		pattern, ok := detectedLevelSeverityNumberRegexp[level]
		if !ok {
			continue
		}
		match := "severity_number:~" + quoteLevelRegexp(pattern)
		if names := severityNameClause([]string{level}); names != "" {
			match = "(" + match + " OR " + names + ")"
		}
		fmt.Fprintf(&b, ` | format if (__dl:"" -__dl_s:* %s) %q as __dl keep_original_fields`, match, level)
	}
	b.WriteString(` | format if (__dl:"" -__dl_s:* severity_number:*) "info" as __dl keep_original_fields`)
	b.WriteString(` | format "unknown" as __dl keep_original_fields`)
	switch mode {
	case DetectedLevelChainGrouping:
		b.WriteString(` | format "<__dl>" as detected_level`)
		b.WriteString(" | delete __dl, __dl_*, __j_*, __l_*")
	case DetectedLevelChainLogs:
		if cond == "" {
			b.WriteString(" | rename __dl as detected_level")
		} else {
			fmt.Fprintf(&b, ` | format if (%s) "<__dl>" as detected_level`, cond)
		}
		b.WriteString(" | delete __dl, __dl_*, __j_*, __l_*")
	default:
		b.WriteString(" | delete __dl_*, __j_*, __l_*")
	}
	return b.String()
}

func detectedLevelLogLevelFieldNames() []string {
	return DetectedLevelFieldNames[1 : len(DetectedLevelFieldNames)-1]
}

// logsqlFieldName quotes a field name when it holds characters outside the
// bare-name set.
func logsqlFieldName(name string) string {
	for _, c := range name {
		if c != '_' && c != '.' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return logsql.QuoteValue(name)
		}
	}
	return name
}

// BuildHasLevelFilter returns a filter matching rows whose detected_level is
// decided by a stored field, or "" when existingFields hold none. OTLP rows
// whose severity_text VictoriaLogs synthesised for severity_number 0 carry no
// level for Loki and are excluded.
func BuildHasLevelFilter(existingFields []string) string {
	stored := DetectedLevelStoredFields(existingFields)
	if len(stored) == 0 {
		return ""
	}
	parts := make([]string, len(stored))
	for i, f := range stored {
		parts[i] = logsqlFieldName(f) + ":*"
	}
	has := strings.Join(parts, " OR ")
	if len(parts) > 1 {
		has = "(" + has + ")"
	}
	for _, noLevel := range noLevelRows(stored) {
		has += " !(" + noLevel + ")"
	}
	return has
}

// noLevelRows returns the filters of the rows whose only level text is the
// "Unspecified" VictoriaLogs synthesises for an OTLP record without severity:
// severity_text with severity_number 0 (v1.50 and newer), or severity without
// any number (older). Loki stores no level for them, and the other stored
// level fields, when set, still decide.
func noLevelRows(stored []string) []string {
	var out []string
	if hasField(stored, "severity_text") && hasField(stored, "severity_number") {
		out = append(out, strings.Join(append(emptyExcept(stored, "severity_text", "severity_number"), `severity_text:="Unspecified"`, `severity_number:="0"`), " "))
	}
	if hasField(stored, "severity") {
		out = append(out, strings.Join(append(emptyExcept(stored, "severity", "severity_number"), `severity:="Unspecified"`, "-severity_number:*"), " "))
	}
	return out
}

// emptyExcept returns `f:""` filters for the stored fields other than skip.
func emptyExcept(stored []string, skip ...string) []string {
	out := make([]string, 0, len(stored))
	for _, f := range stored {
		if !hasField(skip, f) {
			out = append(out, logsqlFieldName(f)+`:""`)
		}
	}
	return out
}

func hasField(fields []string, name string) bool {
	for _, f := range fields {
		if f == name {
			return true
		}
	}
	return false
}

// emptyPrefix returns `f:""` filters for the stored fields ranked before name.
func emptyPrefix(stored []string, name string) []string {
	out := make([]string, 0, len(stored))
	for _, f := range stored {
		if f == name {
			break
		}
		out = append(out, logsqlFieldName(f)+`:""`)
	}
	return out
}

// detectedLevelMatcher evaluates a LogQL label matcher on a detected_level value.
type detectedLevelMatcher struct {
	op    string
	value string
	re    *regexp.Regexp
}

func newDetectedLevelMatcher(op, value string) (detectedLevelMatcher, error) {
	m := detectedLevelMatcher{op: op, value: value}
	switch op {
	case "=", "!=":
	case "=~", "!~":
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return m, fmt.Errorf("invalid detected_level regexp %q: %w", value, err)
		}
		m.re = re
	default:
		return m, fmt.Errorf("unsupported detected_level matcher %q", op)
	}
	return m, nil
}

func (m detectedLevelMatcher) negated() bool { return m.op == "!=" || m.op == "!~" }

// positiveMatch reports whether the non-negated form matches v.
func (m detectedLevelMatcher) positiveMatch(v string) bool {
	if m.re != nil {
		return m.re.MatchString(v)
	}
	return v == m.value
}

// canonicalTargets returns the canonical values the positive matcher accepts.
func (m detectedLevelMatcher) canonicalTargets() []string {
	out := make([]string, 0, len(detectedLevelCanonical))
	for _, v := range detectedLevelCanonical {
		if m.positiveMatch(v) {
			out = append(out, v)
		}
	}
	return out
}

func allDetectedLevelSpellings() []string {
	out := make([]string, 0, 16)
	for _, s := range detectedLevelSpellings {
		out = append(out, s.spellings...)
	}
	return out
}

func isKnownLevelSpelling(v string) bool {
	for _, s := range allDetectedLevelSpellings() {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

// storedTextClause matches a text level field whose normalised value the
// positive matcher accepts: a known spelling of a target, or a raw value that
// is not a known spelling (Loki keeps those unchanged).
func (m detectedLevelMatcher) storedTextClause(field string, targets []string) string {
	f := logsqlFieldName(field)
	var alts []string
	spellings := make([]string, 0, 8)
	for _, t := range targets {
		for _, s := range detectedLevelSpellings {
			if s.level == t {
				spellings = append(spellings, s.spellings...)
			}
		}
	}
	if len(spellings) > 0 {
		alts = append(alts, f+":~"+quoteLevelRegexp("(?i)^("+strings.Join(spellings, "|")+")$"))
	}
	known := quoteLevelRegexp("(?i)^(" + strings.Join(allDetectedLevelSpellings(), "|") + ")$")
	switch {
	case m.re != nil:
		alts = append(alts, "("+f+":~"+quoteLevelRegexp("^(?:"+m.value+")$")+" !"+f+":~"+known+")")
	case m.value != "" && !isKnownLevelSpelling(m.value):
		alts = append(alts, f+":="+logsql.QuoteValue(m.value))
	}
	switch len(alts) {
	case 0:
		return ""
	case 1:
		return alts[0]
	default:
		return "(" + strings.Join(alts, " OR ") + ")"
	}
}

// joinOr joins the non-empty filters with OR.
func joinOr(parts ...string) string {
	var alts []string
	for _, p := range parts {
		if p != "" {
			alts = append(alts, p)
		}
	}
	switch len(alts) {
	case 0:
		return ""
	case 1:
		return alts[0]
	default:
		return "(" + strings.Join(alts, " OR ") + ")"
	}
}

func severityNumberClause(targets []string) string {
	var alts []string
	for _, t := range targets {
		if re, ok := detectedLevelSeverityNumberRegexp[t]; ok {
			alts = append(alts, "severity_number:~"+quoteLevelRegexp(re))
		}
		if t == "info" {
			alts = append(alts, "(severity_number:* !severity_number:~`^-?[0-9]+$`)")
		}
	}
	switch len(alts) {
	case 0:
		return ""
	case 1:
		return alts[0]
	default:
		return "(" + strings.Join(alts, " OR ") + ")"
	}
}

// synthesizedSeverityText matches OTLP rows whose severity_text VictoriaLogs
// derived from a non-zero severity_number: a name of the number's family (the
// pairing inside a family is not checked: a record sending Info2 with number
// 12 is read as synthesised, and still gets info), or Unspecified for a number
// outside 0..24.
func synthesizedSeverityText() string {
	alts := make([]string, 0, len(vlSeverityFamilies)+1)
	for _, f := range vlSeverityFamilies {
		nums := make([]string, 0, 4)
		for n := f.lo; n <= f.hi; n++ {
			nums = append(nums, strconv.Itoa(n))
		}
		alts = append(alts, "(severity_text:~"+quoteLevelRegexp("^"+f.stem+"[234]?$")+" severity_number:~"+quoteLevelRegexp("^("+strings.Join(nums, "|")+")$")+")")
	}
	alts = append(alts, `(severity_text:="Unspecified" severity_number:~`+quoteLevelRegexp(`^(-[1-9][0-9]*|2[5-9]|[3-9][0-9]|[1-9][0-9]{2,})$`)+")")
	return "(" + strings.Join(alts, " OR ") + ")"
}

// severityStems returns the stems of vlSeverityFamilies for the canonical
// levels given (all of them when none is).
func severityStems(levels ...string) []string {
	var stems []string
	for _, f := range vlSeverityFamilies {
		if len(levels) == 0 || slices.Contains(levels, strings.ToLower(f.stem)) {
			stems = append(stems, f.stem)
		}
	}
	return stems
}

// synthesizedSeverityName matches a severity field holding a name
// VictoriaLogs before v1.50 derived from the OTLP number (it stores no
// severity_number then): "Unspecified", which says nothing, and the finer
// names, which Loki reads from the number as the coarse level.
func synthesizedSeverityName() string {
	return "(severity:~" + quoteLevelRegexp("^(Unspecified|("+strings.Join(severityStems(), "|")+")[234]?)$") + " -severity_number:*)"
}

// severityNameClause matches a severity field holding a derived name of one
// of the targets (see synthesizedSeverityName), or "" when none has one.
func severityNameClause(targets []string) string {
	stems := severityStems(targets...)
	if len(stems) == 0 || len(targets) == 0 {
		return ""
	}
	return "(-severity_number:* severity:~" + quoteLevelRegexp("^("+strings.Join(stems, "|")+")[234]?$") + ")"
}

// BuildStoredLevelFilter returns a filter matching rows whose stored fields
// decide a detected_level accepted by the matcher, "" when no stored row can
// match. Negated matchers return the positive filter: callers combine it with
// the body part and negate the whole expression.
func BuildStoredLevelFilter(op, value string, existingFields []string) (string, error) {
	m, err := newDetectedLevelMatcher(op, value)
	if err != nil {
		return "", err
	}
	return m.storedFilter(existingFields), nil
}

func (m detectedLevelMatcher) storedFilter(existingFields []string) string {
	stored := DetectedLevelStoredFields(existingFields)
	targets := m.canonicalTargets()
	otel := hasField(stored, "severity_text") && hasField(stored, "severity_number")
	var clauses []string
	for i, f := range stored {
		prefix := make([]string, 0, i)
		for _, g := range stored[:i] {
			prefix = append(prefix, logsqlFieldName(g)+`:""`)
		}
		var match string
		if f == "severity_number" {
			match = severityNumberClause(targets)
			if match != "" && otel {
				// A synthesised non-zero severity_text defers to the number.
				texts := emptyPrefix(stored, "severity_text")
				texts = append(texts, synthesizedSeverityText())
				prefix = []string{"(" + strings.Join(prefix, " ") + " OR (" + strings.Join(texts, " ") + "))"}
			}
		} else {
			match = m.storedTextClause(f, targets)
			if match != "" && f == "severity_text" && otel {
				match += ` !` + synthesizedSeverityText() + ` !(severity_text:="Unspecified" severity_number:="0")`
			}
			if f == "severity" {
				if match != "" {
					match = "(" + match + " !" + synthesizedSeverityName() + ")"
				}
				if names := severityNameClause(targets); names != "" {
					match = joinOr(match, names)
				}
			}
		}
		if match == "" {
			continue
		}
		clauses = append(clauses, strings.TrimSpace(strings.Join(append(prefix, match), " ")))
	}
	switch len(clauses) {
	case 0:
		return ""
	case 1:
		return clauses[0]
	default:
		return "(" + strings.Join(clauses, " OR ") + ")"
	}
}

// DetectedLevelFilterPlan is the LogsQL form of `| detected_level <op> "<value>"`.
type DetectedLevelFilterPlan struct {
	// Never is set when the matcher accepts no row (Loki: `detected_level=""`).
	Never bool
	// Prefilter is an index-friendly filter to AND with the base query, or "".
	Prefilter string
	// Pipes is the body chain followed by the final filter, starting with
	// "| ". It must run before any user parser stage. Matching rows keep the
	// body value in __dl.
	Pipes string
}

// BuildDetectedLevelFilter plans `| detected_level <op> "<value>"` as
// stored OR (no stored level AND body value), negated for != and !~.
func BuildDetectedLevelFilter(op, value string, existingFields []string) (DetectedLevelFilterPlan, error) {
	m, err := newDetectedLevelMatcher(op, value)
	if err != nil {
		return DetectedLevelFilterPlan{}, err
	}
	// Every row carries a non-empty value.
	if m.re == nil && value == "" {
		if m.negated() {
			return DetectedLevelFilterPlan{}, nil
		}
		return DetectedLevelFilterPlan{Never: true}, nil
	}
	stored := m.storedFilter(existingFields)
	has := BuildHasLevelFilter(existingFields)
	targets := m.canonicalTargets()

	body := ""
	if len(targets) > 0 {
		quoted := make([]string, len(targets))
		for i, t := range targets {
			quoted[i] = logsql.QuoteValue(t)
		}
		body = "__dl:in(" + strings.Join(quoted, ", ") + ")"
		if has != "" {
			body = "!(" + has + ") " + body
		}
	}
	var positive string
	switch {
	case stored != "" && body != "":
		positive = "(" + stored + " OR (" + body + "))"
	case stored != "":
		positive = stored
	case body != "":
		positive = "(" + body + ")"
	default:
		if !m.negated() {
			return DetectedLevelFilterPlan{Never: true}, nil
		}
		return DetectedLevelFilterPlan{}, nil
	}

	plan := DetectedLevelFilterPlan{}
	final := positive
	if m.negated() {
		final = "!" + positive
	} else {
		plan.Prefilter = detectedLevelPrefilter(targets, has, stored != "")
	}
	plan.Pipes = BuildDetectedLevelChain(existingFields, DetectedLevelChainGroup) + " | filter " + final
	return plan, nil
}

// detectedLevelPrefilter returns a word filter that every matching row passes:
// rows with a stored level, or bodies holding a spelling of a target. Targets
// that include unknown cannot be prefiltered.
func detectedLevelPrefilter(targets []string, has string, storedCanMatch bool) string {
	var words []string
	for _, t := range targets {
		if t == "unknown" {
			return ""
		}
		for _, s := range detectedLevelSpellings {
			if s.level == t {
				words = append(words, s.spellings...)
			}
		}
	}
	if len(words) == 0 {
		return ""
	}
	alts := make([]string, 0, len(words)+1)
	if has != "" && storedCanMatch {
		alts = append(alts, "("+has+")")
	}
	for _, w := range words {
		alts = append(alts, "i("+w+")")
	}
	return "(" + strings.Join(alts, " OR ") + ")"
}

// quoteLevelRegexp quotes a regexp for LogsQL, preferring backticks so regexp
// escapes pass through untouched.
func quoteLevelRegexp(re string) string {
	if !strings.Contains(re, "`") {
		return "`" + re + "`"
	}
	return logsql.QuoteValue(re)
}
