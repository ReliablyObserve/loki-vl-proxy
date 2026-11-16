package proxy

import (
	"bytes"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	detectedLevelLabel                   = "detected_level"
	detectedLevelMutationResponseVersion = "@detected-level-mutation-v2:"

	detectedLevelExtractedLabel = "detected_level_extracted"

	levelTrace    = "trace"
	levelDebug    = "debug"
	levelInfo     = "info"
	levelWarn     = "warn"
	levelError    = "error"
	levelCritical = "critical"
	levelFatal    = "fatal"
	levelUnknown  = "unknown"
)

var detectedLevelMutationStageRE = regexp.MustCompile(`\|\s*(?:drop|keep)\s+[^|]*\bdetected_level\b`)
var detectedLevelSourceDropStageRE = regexp.MustCompile(`(?i)\|\s*drop\s+[^|]*\b(?:level|log\.level|severity|severitytext|severity_text|severity_number|lvl)\b`)
var detectedLevelPresenceStageRE = regexp.MustCompile(`\|\s*detected_level\s*(?:!=|==|=)\s*""`)

// detectedLevelMutationResponseKeyVersion fences old keep/drop answers in the
// exact and compatibility caches, including copies fetched from cache peers.
func detectedLevelMutationResponseKeyVersion(query string) string {
	if (strings.Contains(query, detectedLevelLabel) && (detectedLevelMutationStageRE.MatchString(query) || detectedLevelPresenceStageRE.MatchString(query))) ||
		mayHoldDropStage(query) && detectedLevelSourceDropStageRE.MatchString(query) {
		return detectedLevelMutationResponseVersion
	}
	return ""
}

// mayHoldDropStage reports a `|` followed by blanks (regexp \s) and drop in any
// case: what detectedLevelSourceDropStageRE needs before it can match, found
// without running the case-folding regexp on every query.
func mayHoldDropStage(query string) bool {
	for s := query; ; {
		i := strings.IndexByte(s, '|')
		if i < 0 {
			return false
		}
		s = s[i+1:]
		j := 0
		for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\f' || s[j] == '\r') {
			j++
		}
		if len(s)-j >= 4 && s[j]|0x20 == 'd' && s[j+1]|0x20 == 'r' && s[j+2]|0x20 == 'o' && s[j+3]|0x20 == 'p' {
			return true
		}
	}
}

// The detected_level Loki gives an entry at ingest, derived on the read path from what VictoriaLogs stores. The
// rules below are written from Loki's documented settings (discover_log_levels, log_level_fields,
// log_level_from_json_max_depth) and from Loki's answers to the inputs of testdata/detected_level_golden.json,
// recorded from a real Loki (TestCompat_DetectedLevelGoldenMatchesLoki, evidence in
// testdata/detected_level_golden_loki.json). In order:
//
//  1. a stored detected_level field (Loki's structured metadata), normalised;
//  2. the stream's first level label in levelFieldNames order, normalised (a word Loki does not know stays as
//     it is; a log.level stream field, which no Loki stream label can be, counts);
//  3. the row's first level field in levelFieldNames order, normalised; log.level is never one: Loki stores
//     structured metadata named log.level as log_level, which it does not read;
//  4. severity_number: 1-4 trace, 5-8 debug, 9-12 info, 13-16 warn, 17-20 error, 21-24 fatal, below 1 trace, 0
//     and above 24 unknown, a value that is not a decimal integer info;
//  5. the line: a line with braces first and last is read as JSON, its first level key holding a string (decoded
//     names, document order, two objects deep, arrays skipped, up to the first syntax error); any other line as
//     logfmt, its level key earliest in levelFieldNames (names compared case-insensitively, up to the first syntax
//     error); the value counts when it is a known spelling, otherwise the first level word of the line (the
//     keyword scan) decides;
//  6. unknown.
//
// VictoriaLogs stores an OTLP record's severity with a severity text it derives from the number when the record
// has none (v1.50 and later: severity_number with severity_text; v1.47 to v1.49: one severity field). Loki stores
// only what the record holds, so a derived text is read as absent.

// levelFieldNames are the level field names Loki reads, in the order it reads them: the default of Loki's
// log_level_fields setting as its configuration reference documents it.
var levelFieldNames = [...]string{
	"level", "LEVEL", "Level", "log.level",
	"severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl",
	"severity_text", "Severity_Text", "SEVERITY_TEXT",
}

const (
	levelSlotDetected       = 0
	levelSlotFirstName      = 1
	levelSlotLogDotLevel    = 1 + 3
	levelSlotSeverity       = 1 + 4
	levelSlotSeverityText   = 1 + 11
	levelSlotSeverityNumber = 1 + len(levelFieldNames)
	levelSlotCount          = levelSlotSeverityNumber + 1
)

// levelFieldSlot is the slot of a field name: detected_level, a name of levelFieldNames (exact) or
// severity_number; -1 for any other name.
func levelFieldSlot(name []byte) int {
	switch len(name) {
	case 3:
		switch string(name) {
		case "lvl":
			return levelSlotFirstName + 8
		case "LVL":
			return levelSlotFirstName + 9
		case "Lvl":
			return levelSlotFirstName + 10
		}
	case 5:
		switch string(name) {
		case "level":
			return levelSlotFirstName
		case "LEVEL":
			return levelSlotFirstName + 1
		case "Level":
			return levelSlotFirstName + 2
		}
	case 8:
		switch string(name) {
		case "severity":
			return levelSlotSeverity
		case "SEVERITY":
			return levelSlotSeverity + 1
		case "Severity":
			return levelSlotSeverity + 2
		}
	case 9:
		if string(name) == "log.level" {
			return levelSlotLogDotLevel
		}
	case 12:
		if string(name) == "SeverityText" {
			return levelSlotSeverity + 3
		}
	case 13:
		switch string(name) {
		case "severity_text":
			return levelSlotSeverityText
		case "Severity_Text":
			return levelSlotSeverityText + 1
		case "SEVERITY_TEXT":
			return levelSlotSeverityText + 2
		}
	case 14:
		if string(name) == detectedLevelLabel {
			return levelSlotDetected
		}
	case 15:
		if string(name) == "severity_number" {
			return levelSlotSeverityNumber
		}
	}
	return -1
}

// levelFields holds the level-like fields of a row or a stream: one value per slot (the first non-empty one
// observed) and the first nested level key of an unpacked JSON line (parent.name, name in levelFieldNames).
type levelFields struct {
	vals [levelSlotCount][]byte

	nested     []byte
	nestedName []byte
}

// mayBeLevelField reports whether observe keeps anything for a field of this name.
func mayBeLevelField(name []byte) bool {
	return levelFieldSlot(name) >= 0 || nestedLevelName(name)
}

// nestedLevelName reports a field name VictoriaLogs writes for a level key one object down in a JSON line:
// parent.name with one dot and name in levelFieldNames.
func nestedLevelName(name []byte) bool {
	// every level name ends in l, y or t (either case): most names are turned away before any scan
	if len(name) < 5 {
		return false
	}
	switch name[len(name)-1] {
	case 'l', 'L', 'y', 'Y', 't', 'T':
	default:
		return false
	}
	i := bytes.IndexByte(name, '.')
	if i <= 0 || i == len(name)-1 || bytes.IndexByte(name[i+1:], '.') >= 0 {
		return false
	}
	slot := levelFieldSlot(name[i+1:])
	return slot >= levelSlotFirstName && slot < levelSlotSeverityNumber
}

// observe records one field. Empty values are never stored (Loki drops empty structured metadata, VictoriaLogs
// empty fields).
func (f *levelFields) observe(name, value []byte) {
	if len(value) == 0 {
		return
	}
	if slot := levelFieldSlot(name); slot >= 0 {
		if f.vals[slot] == nil {
			f.vals[slot] = value
		}
		return
	}
	if f.nested == nil && nestedLevelName(name) {
		f.nested, f.nestedName = value, name
	}
}

// firstName is the first level name with a value, in levelFieldNames order; -1 when none. A stream field named
// log.level (which no Loki stream label can be) counts, as the level-less stream rule reads it.
func (f *levelFields) firstName() (int, []byte) {
	if f == nil {
		return -1, nil
	}
	for slot := levelSlotFirstName; slot < levelSlotSeverityNumber; slot++ {
		if f.vals[slot] != nil {
			return slot, f.vals[slot]
		}
	}
	return -1, nil
}

func (f *levelFields) value(slot int) []byte {
	if f == nil || slot < 0 || slot >= levelSlotCount {
		return nil
	}
	return f.vals[slot]
}

// detectedLevel is a derived level: one of Loki's level names, or a stored word Loki keeps as it is.
type detectedLevel struct {
	canonical string
	raw       []byte
}

func (d detectedLevel) String() string {
	if d.canonical != "" || d.raw == nil {
		if d.canonical == "" {
			return levelUnknown
		}
		return d.canonical
	}
	return string(d.raw)
}

func (d detectedLevel) appendTo(dst []byte) []byte {
	if d.canonical != "" || d.raw == nil {
		if d.canonical == "" {
			return append(dst, levelUnknown...)
		}
		return append(dst, d.canonical...)
	}
	return append(dst, d.raw...)
}

// normalizedDetectedLevel is a stored level value as Loki writes it into detected_level: a known spelling as
// Loki's level name, any other value as it is.
func normalizedDetectedLevel(v []byte) detectedLevel {
	if c := canonicalLevel(v); c != "" {
		return detectedLevel{canonical: c}
	}
	return detectedLevel{raw: v}
}

type detectedLevelInput struct {
	stream *levelFields
	fields *levelFields
	body   []byte

	// jsonUnpacked: the row's fields are the keys of a JSON line VictoriaLogs unpacked and body is the line
	// rebuilt from them.
	jsonUnpacked bool
	bodyScan     bool
}

// detectedLevelName is the label the derived level is written to: Loki keeps a detected_level stream label as
// the stream's and writes the derived value as detected_level_extracted.
func detectedLevelName(streamLabels map[string]string) string {
	if streamLabels[detectedLevelLabel] != "" {
		return detectedLevelExtractedLabel
	}
	return detectedLevelLabel
}

func deriveDetectedLevel(in detectedLevelInput) detectedLevel {
	return deriveRowLevel(in.stream, in.fields, in.body, in.jsonUnpacked, in.bodyScan)
}

// deriveRowLevel applies the rules at the top of this file. stream and fields may be nil; body is read only with
// bodyScan.
func deriveRowLevel(stream, fields *levelFields, body []byte, jsonUnpacked, bodyScan bool) detectedLevel {
	if v := fields.value(levelSlotDetected); v != nil {
		return normalizedDetectedLevel(v)
	}
	if _, v := stream.firstName(); v != nil {
		return normalizedDetectedLevel(v)
	}
	if fields != nil {
		if jsonUnpacked {
			return unpackedRowLevel(fields, body, bodyScan)
		}
		if d, ok := storedFieldLevel(fields); ok {
			return d
		}
	}
	if !bodyScan {
		return detectedLevel{canonical: levelUnknown}
	}
	return detectedLevel{canonical: levelFromBody(body)}
}

// storedFieldLevel is rules 3 and 4 over a row's stored fields; ok is false when they leave it to the line.
func storedFieldLevel(f *levelFields) (detectedLevel, bool) {
	if v := f.vals[levelSlotFirstName]; v != nil {
		return normalizedDetectedLevel(v), true // level: first, never a derived OTLP text
	}
	number, text, severity := f.vals[levelSlotSeverityNumber], f.vals[levelSlotSeverityText], f.vals[levelSlotSeverity]
	derivedText := number != nil && text != nil && string(text) == vlSeverityText(number)
	// Before v1.50 VictoriaLogs stores an OTLP record's severity as one severity field, the record's text or the
	// name it derives from the number, and no number: a derived name stands for the number, read after the names.
	derivedSeverity := -1
	if number == nil && text == nil && severity != nil {
		derivedSeverity = vlSeverityIndex(severity)
	}
	for slot := levelSlotFirstName; slot < levelSlotSeverityNumber; slot++ {
		v := f.vals[slot]
		if v == nil || slot == levelSlotLogDotLevel || (slot == levelSlotSeverityText && derivedText) || (slot == levelSlotSeverity && derivedSeverity >= 0) {
			continue
		}
		return normalizedDetectedLevel(v), true
	}
	switch {
	case derivedSeverity > 0:
		return detectedLevel{canonical: levelFromSeverityInt(int64(derivedSeverity))}, true
	case derivedSeverity == 0:
		return detectedLevel{}, false // Unspecified: the record had no severity
	case number == nil:
		return detectedLevel{}, false
	case derivedText && bytes.Equal(number, []byte("0")):
		return detectedLevel{}, false // an OTLP record without a severity
	}
	return detectedLevel{canonical: levelFromSeverityNumber(number)}, true
}

// unpackedRowLevel reads the fields of an unpacked JSON line as the line's keys: the first level key at the top
// of the object in levelFieldNames order (VictoriaLogs returns fields sorted, so the line's key order is gone),
// else the nested one; a value that is not a known spelling leaves it to the keyword scan of the rebuilt line.
func unpackedRowLevel(f *levelFields, body []byte, bodyScan bool) detectedLevel {
	v := f.nested
	for slot := levelSlotFirstName; slot < levelSlotSeverityNumber; slot++ {
		if f.vals[slot] != nil {
			v = f.vals[slot]
			break
		}
	}
	if c := canonicalLevel(v); c != "" {
		return detectedLevel{canonical: c}
	}
	if !bodyScan {
		return detectedLevel{canonical: levelUnknown}
	}
	return detectedLevel{canonical: levelFromKeywords(body)}
}

// vlSeverityTexts are the severity texts VictoriaLogs derives from an OTLP severity number (OpenTelemetry's
// severity names), by number; every other number is Unspecified.
var vlSeverityTexts = [25]string{
	"Unspecified",
	"Trace", "Trace2", "Trace3", "Trace4",
	"Debug", "Debug2", "Debug3", "Debug4",
	"Info", "Info2", "Info3", "Info4",
	"Warn", "Warn2", "Warn3", "Warn4",
	"Error", "Error2", "Error3", "Error4",
	"Fatal", "Fatal2", "Fatal3", "Fatal4",
}

// vlSeverityIndex is the number whose derived name text is (0 for Unspecified), -1 for any other text.
func vlSeverityIndex(text []byte) int {
	for i, name := range vlSeverityTexts {
		if string(text) == name {
			return i
		}
	}
	return -1
}

// vlSeverityText is the text VictoriaLogs derives from a stored severity number; "" for a value that is no
// integer, which VictoriaLogs never stores as an OTLP number.
func vlSeverityText(number []byte) string {
	n, ok := parseLevelInt(number)
	switch {
	case !ok:
		return ""
	case n < 1 || n > 24:
		return vlSeverityTexts[0]
	}
	return vlSeverityTexts[n]
}

// canonicalLevel is Loki's level name for a known spelling of a level (compared case-insensitively), "" for any
// other value.
func canonicalLevel(v []byte) string {
	switch len(v) {
	case 3:
		switch {
		case asciiEqualFold(v, "trc"):
			return levelTrace
		case asciiEqualFold(v, "dbg"):
			return levelDebug
		case asciiEqualFold(v, "inf"):
			return levelInfo
		case asciiEqualFold(v, "wrn"):
			return levelWarn
		case asciiEqualFold(v, "err"):
			return levelError
		}
	case 4:
		switch {
		case asciiEqualFold(v, "info"):
			return levelInfo
		case asciiEqualFold(v, "warn"):
			return levelWarn
		}
	case 5:
		switch {
		case asciiEqualFold(v, "error"):
			return levelError
		case asciiEqualFold(v, "debug"):
			return levelDebug
		case asciiEqualFold(v, "trace"):
			return levelTrace
		case asciiEqualFold(v, "fatal"):
			return levelFatal
		}
	case 7:
		if asciiEqualFold(v, "warning") {
			return levelWarn
		}
	case 8:
		if asciiEqualFold(v, "critical") {
			return levelCritical
		}
	case 11:
		if asciiEqualFold(v, "information") {
			return levelInfo
		}
	}
	return ""
}

// levelFromSeverityNumber is the level of a stored severity_number; a value that is not a decimal integer is info.
func levelFromSeverityNumber(v []byte) string {
	n, ok := parseLevelInt(v)
	if !ok {
		return levelInfo
	}
	return levelFromSeverityInt(n)
}

func levelFromSeverityInt(n int64) string {
	switch {
	case n == 0 || n > 24:
		return levelUnknown
	case n <= 4:
		return levelTrace
	case n <= 8:
		return levelDebug
	case n <= 12:
		return levelInfo
	case n <= 16:
		return levelWarn
	case n <= 20:
		return levelError
	}
	return levelFatal
}

// parseLevelInt reads a decimal int64 with an optional sign, as strconv.ParseInt(v, 10, 64) does.
func parseLevelInt(v []byte) (int64, bool) {
	i, neg := 0, false
	if len(v) > 0 && (v[0] == '+' || v[0] == '-') {
		neg, i = v[0] == '-', 1
	}
	if i == len(v) {
		return 0, false
	}
	var n uint64
	for ; i < len(v); i++ {
		c := v[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		if n > (1<<63)/10 {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
		if n > 1<<63 {
			return 0, false
		}
	}
	if !neg && n == 1<<63 {
		return 0, false
	}
	if neg {
		return -int64(n), true // -1<<63 wraps to itself
	}
	return int64(n), true
}

// levelFromBody is rule 5 over a line: Loki's level name or unknown. A line whose first and last bytes after ASCII
// blanks are braces is read as JSON (and never as logfmt), any other line as logfmt; a value that is not a known
// spelling, or no value, leaves the level to the keyword scan.
func levelFromBody(body []byte) string {
	if t := trimLevelBlanks(body); len(t) >= 2 && t[0] == '{' && t[len(t)-1] == '}' {
		if v, found := levelFromJSONBody(t); found {
			if c := canonicalLevel(v); c != "" {
				return c
			}
		}
	} else if c := levelFromLogfmtBody(body); c != "" {
		return c
	}
	return levelFromKeywords(body)
}

// trimLevelBlanks trims the blanks Loki's JSON reading skips around a line: space, tab, CR and LF.
func trimLevelBlanks(line []byte) []byte {
	isBlank := func(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }
	for len(line) > 0 && isBlank(line[0]) {
		line = line[1:]
	}
	for len(line) > 0 && isBlank(line[len(line)-1]) {
		line = line[:len(line)-1]
	}
	return line
}

// levelJSONMaxDepth is Loki's default log_level_from_json_max_depth: the keys of the top object and of the
// objects one level down.
const levelJSONMaxDepth = 2

// levelFromJSONBody walks a JSON line (from its opening brace) and returns the value of its first level key in
// document order (names of levelFieldNames after decoding the key, levelJSONMaxDepth objects deep, arrays not
// entered) that holds a string, raw as the line writes it and possibly empty. The walk is lenient (numbers are
// read as a run of number bytes, a comma may precede a closing brace) and stops at the first syntax error,
// keeping what it found before it.
func levelFromJSONBody(body []byte) (v []byte, found bool) {
	w := jsonLevelWalk{data: body}
	w.skipBlanks()
	if w.pos >= len(w.data) || w.data[w.pos] != '{' {
		return nil, false
	}
	w.value(1)
	return w.found, w.hasFound
}

type jsonLevelWalk struct {
	data     []byte
	pos      int
	found    []byte
	hasFound bool
	nest     int
}

// levelJSONMaxNesting bounds the objects and arrays the walk enters: a walk nested deeper stops there.
const levelJSONMaxNesting = 10000

func (w *jsonLevelWalk) skipBlanks() {
	for w.pos < len(w.data) {
		switch w.data[w.pos] {
		case ' ', '\t', '\n', '\r':
			w.pos++
		default:
			return
		}
	}
}

// str reads the string at pos (an opening quote) and returns its raw contents; ok is false for a string that does
// not end or holds an escape JSON does not have.
func (w *jsonLevelWalk) str() ([]byte, bool) {
	start := w.pos + 1
	for i := start; i < len(w.data); i++ {
		switch w.data[i] {
		case '"':
			w.pos = i + 1
			return w.data[start:i], true
		case '\\':
			i++
			if i >= len(w.data) {
				return nil, false
			}
			switch w.data[i] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				if i+4 >= len(w.data) || !isHex4(w.data[i+1:i+5]) {
					return nil, false
				}
				i += 4
			default:
				return nil, false
			}
		}
	}
	return nil, false
}

// value reads the value at pos; the keys of an object there are at depth. It reports whether the walk may go on.
func (w *jsonLevelWalk) value(depth int) bool {
	if w.pos >= len(w.data) {
		return false
	}
	switch c := w.data[w.pos]; {
	case c == '{':
		return w.object(depth)
	case c == '[':
		return w.array()
	case c == '"':
		_, ok := w.str()
		return ok
	case c == 't':
		return w.literal("true")
	case c == 'f':
		return w.literal("false")
	case c == 'n':
		return w.literal("null")
	case c == '-' || ('0' <= c && c <= '9'):
		for w.pos < len(w.data) && isJSONNumberByte(w.data[w.pos]) {
			w.pos++
		}
		return true
	}
	return false
}

func isJSONNumberByte(c byte) bool {
	return ('0' <= c && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E'
}

func (w *jsonLevelWalk) literal(word string) bool {
	if !bytes.HasPrefix(w.data[w.pos:], []byte(word)) {
		return false
	}
	w.pos += len(word)
	return true
}

// object reads the object at pos (an opening brace) whose keys are at depth.
func (w *jsonLevelWalk) object(depth int) bool {
	if w.nest++; w.nest > levelJSONMaxNesting {
		return false
	}
	defer func() { w.nest-- }()
	w.pos++
	for {
		w.skipBlanks()
		if w.pos >= len(w.data) {
			return false
		}
		if w.data[w.pos] == '}' {
			w.pos++
			return true
		}
		if w.data[w.pos] != '"' {
			return false
		}
		key, ok := w.str()
		if !ok {
			return false
		}
		w.skipBlanks()
		if w.pos >= len(w.data) || w.data[w.pos] != ':' {
			return false
		}
		w.pos++
		w.skipBlanks()
		if w.pos >= len(w.data) {
			return false
		}
		switch c := w.data[w.pos]; {
		case c == '"':
			v, ok := w.str()
			if !ok {
				return false
			}
			if !w.hasFound && depth <= levelJSONMaxDepth && isJSONLevelKey(key) {
				w.found, w.hasFound = v, true
			}
		case c == '{' && depth < levelJSONMaxDepth:
			if !w.object(depth + 1) {
				return false
			}
		default:
			// deeper objects and arrays are read for their syntax only
			if !w.value(levelJSONMaxDepth + 1) {
				return false
			}
		}
		w.skipBlanks()
		if w.pos >= len(w.data) {
			return false
		}
		switch w.data[w.pos] {
		case ',':
			w.pos++
		case '}':
			w.pos++
			return true
		default:
			return false
		}
	}
}

// array reads the array at pos; nothing in it is a level key.
func (w *jsonLevelWalk) array() bool {
	if w.nest++; w.nest > levelJSONMaxNesting {
		return false
	}
	defer func() { w.nest-- }()
	w.pos++
	for {
		w.skipBlanks()
		if w.pos >= len(w.data) {
			return false
		}
		if w.data[w.pos] == ']' {
			w.pos++
			return true
		}
		if !w.value(levelJSONMaxDepth + 1) {
			return false
		}
		w.skipBlanks()
		if w.pos >= len(w.data) {
			return false
		}
		switch w.data[w.pos] {
		case ',':
			w.pos++
		case ']':
			w.pos++
			return true
		default:
			return false
		}
	}
}

func isJSONLevelKey(key []byte) bool {
	if bytes.IndexByte(key, '\\') >= 0 {
		var buf [32]byte
		decoded, ok := unquoteLogfmtLevel(key, buf[:0])
		if !ok {
			return false
		}
		key = decoded
	}
	slot := levelFieldSlot(key)
	return slot >= levelSlotFirstName && slot < levelSlotSeverityNumber
}

// levelFieldGroup is the priority of a logfmt key, its name compared case-insensitively with levelFieldNames
// (level, log.level, severity, severitytext, lvl, severity_text); -1 for any other key.
func levelFieldGroup(key []byte) int {
	switch len(key) {
	case 3:
		if asciiEqualFold(key, "lvl") {
			return 4
		}
	case 5:
		if asciiEqualFold(key, "level") {
			return 0
		}
	case 8:
		if asciiEqualFold(key, "severity") {
			return 2
		}
	case 9:
		if asciiEqualFold(key, "log.level") {
			return 1
		}
	case 12:
		if asciiEqualFold(key, "severitytext") {
			return 3
		}
	case 13:
		if asciiEqualFold(key, "severity_text") {
			return 5
		}
	}
	return -1
}

// asciiEqualFold compares a with the lower-case ASCII word b, ignoring ASCII case.
func asciiEqualFold(a []byte, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		c := a[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != b[i] {
			return false
		}
	}
	return true
}

const levelFieldGroups = 6

// levelFromLogfmtBody reads a logfmt line and returns Loki's level name for the value of its level key earliest in
// levelFieldNames (names compared case-insensitively, the first occurrence of each; a bare or empty value counts and
// is no level), "" when that value is not a known spelling or the line has no level key. The read stops at the
// first syntax error (a quote or an equals sign inside a key or an unquoted value, an unterminated quoted value, an
// escape JSON strings do not have), keeping the keys before it.
func levelFromLogfmtBody(line []byte) string {
	if bytes.IndexByte(line, '=') < 0 {
		return ""
	}
	var found [levelFieldGroups][]byte
	var quoted [levelFieldGroups]bool
	best := levelFieldGroups
scan:
	for i := 0; i < len(line) && best > 0; {
		if line[i] <= ' ' {
			i++
			continue
		}
		start := i
		for i < len(line) && line[i] > ' ' && line[i] != '=' {
			if line[i] == '"' {
				break scan
			}
			i++
		}
		if i == start {
			break // a key that is empty
		}
		key := line[start:i]
		var value []byte
		isQuoted := false
		if i < len(line) && line[i] == '=' {
			i++
			switch {
			case i < len(line) && line[i] == '"':
				end, ok := logfmtClosingQuote(line, i)
				if !ok || !validLogfmtEscapes(line[i+1:end]) {
					break scan
				}
				value, isQuoted = line[i+1:end], true
				i = end + 1
				if i < len(line) && line[i] > ' ' {
					break scan
				}
			default:
				vs := i
				for i < len(line) && line[i] > ' ' {
					if line[i] == '"' || line[i] == '=' {
						break scan
					}
					i++
				}
				value = line[vs:i]
			}
		}
		g := levelFieldGroup(key)
		if g < 0 || g >= best || found[g] != nil {
			continue
		}
		if value == nil {
			value = line[i:i] // a bare key: present, empty
		}
		found[g], quoted[g], best = value, isQuoted, g
	}
	if best == levelFieldGroups {
		return ""
	}
	v := found[best]
	if quoted[best] {
		var buf [16]byte
		if len(v) > 6*len("information") {
			return "" // longer than any known spelling written as \u escapes
		}
		v, _ = unquoteLogfmtLevel(v, buf[:0])
	}
	return canonicalLevel(v)
}

// logfmtClosingQuote is the index of the quote that ends the quoted value starting at open.
func logfmtClosingQuote(line []byte, open int) (int, bool) {
	for i := open + 1; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++
		case '"':
			return i, true
		}
	}
	return 0, false
}

// validLogfmtEscapes reports a quoted value whose escapes unquoteLogfmtLevel accepts.
func validLogfmtEscapes(quoted []byte) bool {
	for i := bytes.IndexByte(quoted, '\\'); i >= 0 && i < len(quoted); i++ {
		if quoted[i] != '\\' {
			continue
		}
		i++
		if i >= len(quoted) {
			return false
		}
		switch quoted[i] {
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
		case 'u':
			if i+4 >= len(quoted) || !isHex4(quoted[i+1:i+5]) {
				return false
			}
			i += 4
		default:
			return false
		}
	}
	return true
}

// unquoteLogfmtLevel unescapes a quoted logfmt value (\" \\ \/ \b \f \n \r \t \uXXXX); ok is false for any other
// escape. A value without escapes is returned as it is; one with escapes is written into buf.
func unquoteLogfmtLevel(quoted, buf []byte) ([]byte, bool) {
	if bytes.IndexByte(quoted, '\\') < 0 {
		return quoted, true
	}
	out := buf
	for i := 0; i < len(quoted); i++ {
		c := quoted[i]
		if c != '\\' {
			out = append(out, c)
			continue
		}
		i++
		if i >= len(quoted) {
			return nil, false
		}
		switch quoted[i] {
		case '"', '\\', '/':
			out = append(out, quoted[i])
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'u':
			if i+4 >= len(quoted) || !isHex4(quoted[i+1:i+5]) {
				return nil, false
			}
			r := rune(0)
			for _, h := range quoted[i+1 : i+5] {
				r = r<<4 | rune(hexValue(h))
			}
			out = utf8.AppendRune(out, r)
			i += 4
		default:
			return nil, false
		}
	}
	return out, true
}

func isHex4(s []byte) bool {
	for _, c := range s {
		if hexValue(c) < 0 {
			return false
		}
	}
	return len(s) == 4
}

func hexValue(c byte) int {
	switch {
	case '0' <= c && c <= '9':
		return int(c - '0')
	case 'a' <= c && c <= 'f':
		return int(c-'a') + 10
	case 'A' <= c && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// levelKeywords are the level words of the keyword scan with Loki's level name for each; a word that starts
// another (warning, error) comes first.
var levelKeywords = [...]struct{ word, level string }{
	{"trace", levelTrace}, {"debug", levelDebug}, {"info", levelInfo},
	{"warning", levelWarn}, {"warn", levelWarn},
	{"error", levelError}, {"err", levelError},
	{"critical", levelCritical}, {"fatal", levelFatal},
}

// levelWordStart marks the bytes a level word can start with: their letters in either case, and the first byte
// of U+0130, which Loki's lower-casing turns into i.
var levelWordStart = func() (t [256]bool) {
	for _, c := range []byte("tdiwecfTDIWECF") {
		t[c] = true
	}
	t[0xC4] = true
	return t
}()

// levelLeftBoundary and levelRightBoundary are the bytes a level word may follow and be followed by.
var levelLeftBoundary, levelRightBoundary = func() (l, r [256]bool) {
	for _, c := range []byte(" \t\n[({\"=") {
		l[c] = true
	}
	for _, c := range []byte(" \t\n[]{}()\":=,!") {
		r[c] = true
	}
	return l, r
}()

// levelFromKeywords is the keyword scan: the level of the first level word of the line (any letter case,
// standing between the boundary bytes), unknown when it has none.
func levelFromKeywords(line []byte) string {
	for i := 0; i < len(line); i++ {
		if !levelWordStart[line[i]] || (i > 0 && !levelLeftBoundary[line[i-1]]) {
			continue
		}
		for _, k := range levelKeywords {
			if end := matchLevelWord(line, i, k.word); end >= 0 && (end == len(line) || levelRightBoundary[line[end]]) {
				return k.level
			}
		}
	}
	return levelUnknown
}

// matchLevelWord is the end of word (lower-case ASCII) at line[i:], letters compared lower-cased (U+0130 is i),
// -1 when it is not there.
func matchLevelWord(line []byte, i int, word string) int {
	for j := 0; j < len(word); j++ {
		if i >= len(line) {
			return -1
		}
		c := line[i]
		if c == 0xC4 && word[j] == 'i' && i+1 < len(line) && line[i+1] == 0xB0 {
			i += 2
			continue
		}
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != word[j] {
			return -1
		}
		i++
	}
	return i
}
