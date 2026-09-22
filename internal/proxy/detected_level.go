package proxy

import (
	"bytes"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/grafana/jsonparser"
)

// Read-path detected_level.
//
// Loki attaches detected_level to every entry at ingest. The proxy has no
// ingest path, so it derives the same value on the read path from what
// VictoriaLogs stores for a row: stream fields, non-stream fields and _msg.
// Loki's result for the stored row defines the value; the rule order is:
//
//  1. a non-empty non-stream detected_level field, normalised. A
//     detected_level stream field is an index label for Loki: it does not
//     take part, and the derived value is exposed as detected_level_extracted;
//  2. the first non-empty stream field named in levelFieldNames, normalised;
//  3. the first non-empty non-stream field named in levelFieldNames,
//     normalised. A severity_text that VictoriaLogs synthesised from
//     severity_number (OTLP rows without a text) is skipped;
//  4. a non-empty severity_number field, mapped by OTel ranges;
//  5. the line body: JSON (objects at depth 0 and 1, first string value
//     under a level key), then logfmt (case-insensitive keys, lowest list
//     index wins, stops at the first syntax error), then a bounded keyword
//     scan of the whole line, else "unknown".
//
// Normalisation maps the known spellings to trace, debug, info, warn, error,
// critical and fatal, and keeps any other value unchanged. Rows whose _msg is
// VictoriaLogs' missing-message value (built-in or customised) had their JSON body unpacked into
// fields at ingestion: their fields are read as JSON body keys (an unknown
// word falls through to the keyword scan) and the keyword scan runs on the
// line rebuilt from those fields (storedLogLineFromFJ).

const (
	detectedLevelLabel = "detected_level"
	// detectedLevelExtractedLabel names the derived value on entries whose
	// stream already has a detected_level label, as Loki renames structured
	// metadata that collides with a stream label.
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

// levelFieldNames is Loki's default log_level_fields list, in priority order.
var levelFieldNames = [...]string{
	"level", "LEVEL", "Level", "log.level",
	"severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl",
	"severity_text", "Severity_Text", "SEVERITY_TEXT",
}

var levelFieldNameBytes = func() [len(levelFieldNames)][]byte {
	var out [len(levelFieldNames)][]byte
	for i, name := range levelFieldNames {
		out[i] = []byte(name)
	}
	return out
}()

// Indexes into levelFields.vals. Slot 0 is detected_level, slots 1..14 follow
// levelFieldNames, the last slot is severity_number.
const (
	levelSlotDetected       = 0
	levelSlotFirstName      = 1
	levelSlotSeverityText   = 1 + 11 // severity_text
	levelSlotSeverityNumber = 1 + len(levelFieldNames)
	levelSlotCount          = levelSlotSeverityNumber + 1
)

// levelFieldSlot returns the levelFields slot for a field name, or -1.
func levelFieldSlot(name []byte) int {
	switch len(name) {
	case 3, 5, 8, 9, 12, 13, 14, 15:
	default:
		return -1
	}
	switch string(name) {
	case "detected_level":
		return levelSlotDetected
	case "level":
		return 1
	case "LEVEL":
		return 2
	case "Level":
		return 3
	case "log.level":
		return 4
	case "severity":
		return 5
	case "SEVERITY":
		return 6
	case "Severity":
		return 7
	case "SeverityText":
		return 8
	case "lvl":
		return 9
	case "LVL":
		return 10
	case "Lvl":
		return 11
	case "severity_text":
		return 12
	case "Severity_Text":
		return 13
	case "SEVERITY_TEXT":
		return 14
	case "severity_number":
		return levelSlotSeverityNumber
	}
	return -1
}

// levelFields holds the non-empty level-like values of one row or stream.
// Values alias the caller's buffers and are valid while those buffers are.
type levelFields struct {
	vals [levelSlotCount][]byte
	// nested is the "<parent>.<name>" field with name in levelFieldNames and
	// the smallest field name, used only for rows unpacked from a JSON body.
	nested     []byte
	nestedName []byte
}

// mayBeLevelField reports whether observe can record a field with this name.
func mayBeLevelField(name []byte) bool {
	return levelFieldSlot(name) >= 0 || bytes.IndexByte(name, '.') > 0
}

// observe records name=value when name is level-like and value is non-empty.
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
	dot := bytes.IndexByte(name, '.')
	if dot <= 0 || bytes.IndexByte(name[dot+1:], '.') >= 0 {
		return
	}
	if slot := levelFieldSlot(name[dot+1:]); slot < levelSlotFirstName || slot >= levelSlotSeverityNumber {
		return
	}
	if f.nested == nil || bytes.Compare(name, f.nestedName) < 0 {
		f.nested, f.nestedName = value, name
	}
}

// firstName returns the first non-empty value among levelFieldNames.
func (f *levelFields) firstName() (int, []byte) {
	if f == nil {
		return -1, nil
	}
	for slot := levelSlotFirstName; slot < levelSlotSeverityNumber; slot++ {
		if v := f.vals[slot]; len(v) > 0 {
			return slot, v
		}
	}
	return -1, nil
}

// value returns the value in slot; a nil receiver holds nothing.
func (f *levelFields) value(slot int) []byte {
	if f == nil {
		return nil
	}
	return f.vals[slot]
}

// nestedValue returns the nested level value; a nil receiver holds nothing.
func (f *levelFields) nestedValue() []byte {
	if f == nil {
		return nil
	}
	return f.nested
}

// detectedLevel is a derived value: a canonical constant, or the raw stored
// value when it is not a known spelling (Loki keeps those unchanged).
type detectedLevel struct {
	canonical string
	raw       []byte
}

func (d detectedLevel) String() string {
	if d.canonical != "" {
		return d.canonical
	}
	return string(d.raw)
}

func (d detectedLevel) appendTo(dst []byte) []byte {
	if d.canonical != "" {
		return append(dst, d.canonical...)
	}
	return append(dst, d.raw...)
}

func normalizedDetectedLevel(v []byte) detectedLevel {
	if c := canonicalLevel(v); c != "" {
		return detectedLevel{canonical: c}
	}
	return detectedLevel{raw: v}
}

// detectedLevelInput is one row as VictoriaLogs returned it.
type detectedLevelInput struct {
	stream *levelFields // level-like stream fields (constant per stream)
	fields *levelFields // level-like non-stream fields of the row
	body   []byte       // _msg, or the rebuilt line for unpacked rows
	// jsonUnpacked marks rows stored without a message: their fields came
	// from a JSON body, so they follow the JSON body rule.
	jsonUnpacked bool
	bodyScan     bool // -detected-level-body-scan
}

// detectedLevelName returns the label name the derived value is exposed under
// for a stream: detected_level, or detected_level_extracted when the stream
// itself carries a detected_level label.
func detectedLevelName(streamLabels map[string]string) string {
	if _, ok := streamLabels[detectedLevelLabel]; ok {
		return detectedLevelExtractedLabel
	}
	return detectedLevelLabel
}

// deriveDetectedLevel returns Loki's detected_level for one stored row.
func deriveDetectedLevel(in detectedLevelInput) detectedLevel {
	return deriveRowLevel(in.stream, in.fields, in.body, in.jsonUnpacked, in.bodyScan)
}

// deriveRowLevel is deriveDetectedLevel with separate arguments: the body
// escapes into the JSON parser, and passing it apart from the levelFields
// pointers lets callers keep their per-row levelFields on the stack.
func deriveRowLevel(stream, fields *levelFields, body []byte, jsonUnpacked, bodyScan bool) detectedLevel {
	if v := fields.value(levelSlotDetected); len(v) > 0 {
		return normalizedDetectedLevel(v)
	}
	if _, v := stream.firstName(); v != nil {
		return normalizedDetectedLevel(v)
	}
	if jsonUnpacked {
		// Fields unpacked from a JSON body: a level key with a known word wins,
		// anything else falls through to the keyword scan of the line.
		_, v := fields.firstName()
		if v == nil {
			v = fields.nestedValue()
		}
		if c := canonicalLevel(v); c != "" {
			return detectedLevel{canonical: c}
		}
		if !bodyScan {
			return detectedLevel{canonical: levelUnknown}
		}
		return detectedLevel{canonical: levelFromKeywords(body)}
	}
	skipSeverityNumber := false
	if slot, v := fields.firstName(); v != nil {
		synthesized := false
		if slot == levelSlotSeverityText {
			var zero bool
			synthesized, zero = isVLSynthesizedSeverityText(v, fields.value(levelSlotSeverityNumber))
			// VictoriaLogs writes severity_number=0 and "Unspecified" for OTLP
			// records without a severity; Loki stores neither.
			skipSeverityNumber = synthesized && zero
		}
		if !synthesized {
			return normalizedDetectedLevel(v)
		}
		// Loki never stored the synthesised text: later names still count.
		for next := levelSlotSeverityText + 1; next < levelSlotSeverityNumber; next++ {
			if v := fields.value(next); len(v) > 0 {
				return normalizedDetectedLevel(v)
			}
		}
	}
	if v := fields.value(levelSlotSeverityNumber); len(v) > 0 && !skipSeverityNumber {
		return detectedLevel{canonical: levelFromSeverityNumber(v)}
	}
	if !bodyScan {
		return detectedLevel{canonical: levelUnknown}
	}
	return detectedLevel{canonical: levelFromBody(body)}
}

// vlSeverityTexts is VictoriaLogs' severity_text for each OTel severity
// number, used when an OTLP record carries no text.
var vlSeverityTexts = [...]string{
	"Unspecified",
	"Trace", "Trace2", "Trace3", "Trace4",
	"Debug", "Debug2", "Debug3", "Debug4",
	"Info", "Info2", "Info3", "Info4",
	"Warn", "Warn2", "Warn3", "Warn4",
	"Error", "Error2", "Error3", "Error4",
	"Fatal", "Fatal2", "Fatal3", "Fatal4",
}

// isVLSynthesizedSeverityText reports whether text is the value VictoriaLogs
// derives from number, and whether that number is zero.
func isVLSynthesizedSeverityText(text, number []byte) (synthesized, zero bool) {
	if len(number) == 0 {
		return false, false
	}
	n, ok := parseLevelInt(number)
	if !ok {
		return false, false
	}
	want := vlSeverityTexts[0]
	if n >= 0 && n < int64(len(vlSeverityTexts)) {
		want = vlSeverityTexts[n]
	}
	return string(text) == want, n == 0
}

// canonicalLevel returns the canonical level for a known spelling (case
// insensitive), or "".
func canonicalLevel(v []byte) string {
	switch len(v) {
	case 3:
		switch {
		case bytes.EqualFold(v, []byte("trc")):
			return levelTrace
		case bytes.EqualFold(v, []byte("dbg")):
			return levelDebug
		case bytes.EqualFold(v, []byte("inf")):
			return levelInfo
		case bytes.EqualFold(v, []byte("wrn")):
			return levelWarn
		case bytes.EqualFold(v, []byte("err")):
			return levelError
		}
	case 4:
		switch {
		case bytes.EqualFold(v, []byte("info")):
			return levelInfo
		case bytes.EqualFold(v, []byte("warn")):
			return levelWarn
		}
	case 5:
		switch {
		case bytes.EqualFold(v, []byte("trace")):
			return levelTrace
		case bytes.EqualFold(v, []byte("debug")):
			return levelDebug
		case bytes.EqualFold(v, []byte("error")):
			return levelError
		case bytes.EqualFold(v, []byte("fatal")):
			return levelFatal
		}
	case 7:
		if bytes.EqualFold(v, []byte("warning")) {
			return levelWarn
		}
	case 8:
		if bytes.EqualFold(v, []byte("critical")) {
			return levelCritical
		}
	case 11:
		if bytes.EqualFold(v, []byte("information")) {
			return levelInfo
		}
	}
	return ""
}

// levelFromSeverityNumber maps an OTel severity number the way Loki does:
// an unparsable value is info, 0 and values above 24 are unknown.
func levelFromSeverityNumber(v []byte) string {
	n, ok := parseLevelInt(v)
	switch {
	case !ok:
		return levelInfo
	case n == 0:
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
	case n <= 24:
		return levelFatal
	}
	return levelUnknown
}

// parseLevelInt parses a decimal integer with strconv.Atoi's syntax and
// 64-bit range (optional sign, digits only) without allocating.
func parseLevelInt(v []byte) (int64, bool) {
	neg := false
	if len(v) > 0 && (v[0] == '+' || v[0] == '-') {
		neg = v[0] == '-'
		v = v[1:]
	}
	if len(v) == 0 {
		return 0, false
	}
	const cutoff = uint64(1) << 63
	var n uint64
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, false
		}
		if n > (cutoff-uint64(c-'0'))/10 {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
	}
	switch {
	case neg && n == cutoff:
		return -1 << 63, true
	case n >= cutoff:
		return 0, false
	case neg:
		return -int64(n), true
	}
	return int64(n), true
}

// levelFromBody derives the level from a line body (rule 5).
func levelFromBody(body []byte) string {
	json := isLokiJSONLine(body)
	if !json && bytes.IndexByte(body, '=') < 0 {
		return levelFromKeywords(body)
	}
	keyword, mayHoldKey, exact := scanLevelLine(body)
	if mayHoldKey {
		var c string
		if json {
			v, _ := levelFromJSONBody(body)
			c = canonicalLevel(v)
		} else {
			c = levelFromLogfmtBody(body)
		}
		if c != "" {
			return c
		}
	}
	if !exact {
		return levelFromKeywords(body)
	}
	return keyword
}

// scanLevelLine reads a line once for the earliest bounded keyword and for
// whether it can hold a level key: an ASCII case-insensitive "lvl", "level"
// or "severity", or any byte (non-ASCII or a backslash escape) that could
// spell one in another form. Lines without one skip key parsing, whose result
// could only be "no key". exact is false when the keyword must be recomputed
// with Unicode lowering (see levelFromKeywords).
func scanLevelLine(line []byte) (keyword string, mayHoldKey, exact bool) {
	keyword, exact = levelUnknown, true
	found := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch lower := c | 0x20; {
		case c >= utf8.RuneSelf:
			mayHoldKey = true
			if c == 0xC4 && i+1 < len(line) && line[i+1] == 0xB0 {
				exact = false
			}
		case c == '\\':
			mayHoldKey = true
		case lower == 'l' && c >= 'A':
			rest := line[i:]
			if (len(rest) >= 3 && asciiEqualFold(rest[:3], []byte("lvl"))) || (len(rest) >= 5 && asciiEqualFold(rest[:5], []byte("level"))) {
				mayHoldKey = true
			}
		case lower == 's' && c >= 'A':
			if rest := line[i:]; len(rest) >= 8 && asciiEqualFold(rest[:8], []byte("severity")) {
				mayHoldKey = true
			}
		case !found:
			if level := keywordAt(line, i); level != "" {
				keyword, found = level, true
			}
		}
		if found && mayHoldKey {
			return keyword, mayHoldKey, exact
		}
	}
	return keyword, mayHoldKey, exact
}

// isLokiJSONLine reports whether the first non-space rune is '{' and the last
// non-space byte is '}', with Loki's unicode.IsSpace trimming.
func isLokiJSONLine(line []byte) bool {
	first := rune(0)
	for i := 0; i < len(line); {
		r, size := utf8.DecodeRune(line[i:])
		if !unicode.IsSpace(r) {
			first = r
			break
		}
		i += size
	}
	if first != '{' {
		return false
	}
	for i := len(line) - 1; i >= 0; i-- {
		if r := rune(line[i]); !unicode.IsSpace(r) {
			return r == '}'
		}
	}
	return false
}

var errLevelKeyFound = errors.New("level key found")

type jsonLevelWalk struct {
	value []byte
	found bool
}

// levelFromJSONBody returns the string value of the first key in
// levelFieldNames, walking objects at depth 0 and 1 in document order. Keys
// before a syntax error still count. The value is the raw JSON string content.
func levelFromJSONBody(body []byte) ([]byte, bool) {
	var w jsonLevelWalk
	_ = w.walk(body, 0)
	return w.value, w.found
}

func (w *jsonLevelWalk) walk(data []byte, depth int) error {
	if depth >= 2 {
		return nil
	}
	return jsonparser.ObjectEach(data, func(key, value []byte, dataType jsonparser.ValueType, _ int) error {
		switch dataType {
		case jsonparser.String:
			if slot := levelFieldSlot(key); slot >= levelSlotFirstName && slot < levelSlotSeverityNumber {
				w.value, w.found = value, true
				return errLevelKeyFound
			}
		case jsonparser.Object:
			return w.walk(value, depth+1)
		}
		return nil
	})
}

// levelFieldGroup returns the lowest levelFieldNames index whose name equals
// key case-insensitively (Unicode folding, like strings.EqualFold), or -1.
func levelFieldGroup(key []byte) int {
	for _, c := range key {
		if c >= utf8.RuneSelf {
			// Unicode folding can match a key of another byte length (for
			// example U+017F folds to 's'); compare the slow way.
			for i, name := range levelFieldNameBytes {
				if bytes.EqualFold(key, name) {
					return i
				}
			}
			return -1
		}
	}
	// ASCII keys: the names fold into six groups, identified by length.
	var group int
	switch len(key) {
	case 3:
		group = 8 // lvl
	case 5:
		group = 0 // level
	case 8:
		group = 4 // severity
	case 9:
		group = 3 // log.level
	case 12:
		group = 7 // SeverityText
	case 13:
		group = 11 // severity_text
	default:
		return -1
	}
	if asciiEqualFold(key, levelFieldNameBytes[group]) {
		return group
	}
	return -1
}

// asciiEqualFold compares ASCII strings of equal length case-insensitively.
func asciiEqualFold(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x == y {
			continue
		}
		if x|0x20 != y|0x20 || x|0x20 < 'a' || x|0x20 > 'z' {
			return false
		}
	}
	return true
}

// levelFromLogfmtBody scans logfmt pairs with the semantics of Loki's logfmt
// decoder, takes the value of the key with the lowest levelFieldNames index
// (case-insensitive) and returns its canonical level, or "" when that value
// is not a known word. Decoding stops at the first syntax error.
func levelFromLogfmtBody(line []byte) string {
	pos := len(levelFieldNames)
	res := ""
	d := logfmtLevelDecoder{line: line}
	for !d.eol() && d.scan() {
		if len(d.key) == 0 || len(d.key) > len("SEVERITY_TEXT")*3 {
			continue
		}
		group := levelFieldGroup(d.key)
		if group < 0 || group >= pos {
			continue
		}
		pos = group
		if d.hasEsc {
			var buf [16]byte
			res = canonicalLevel(unquoteLogfmtLevel(d.value, buf[:0]))
		} else {
			res = canonicalLevel(d.value)
		}
		if group == 0 {
			return res
		}
	}
	return res
}

// logfmtLevelDecoder is Loki's logfmt key/value decoder (pkg/logql/log/logfmt)
// reduced to what level detection reads. For quoted values with escapes value
// holds the quoted text and hasEsc is set; the caller unquotes it.
type logfmtLevelDecoder struct {
	line   []byte
	pos    int
	key    []byte
	value  []byte
	hasEsc bool
}

func (d *logfmtLevelDecoder) eol() bool { return d.pos >= len(d.line) }

//nolint:gocyclo // mirrors Loki's logfmt decoder state machine one to one.
func (d *logfmtLevelDecoder) scan() bool {
	d.key, d.value, d.hasEsc = nil, nil, false
	line := d.line

	// garbage
	i := d.pos
	for i < len(line) && line[i] <= ' ' {
		i++
	}
	if i >= len(line) {
		d.pos = len(line)
		return false
	}
	d.pos = i

	// key
	start, multibyte := d.pos, false
	for i = d.pos; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '=':
			d.pos = i
			if d.pos > start {
				d.key = line[start:d.pos]
				if multibyte && bytes.ContainsRune(d.key, utf8.RuneError) {
					return d.skipValue()
				}
			}
			if d.key == nil {
				return d.skipValue()
			}
			return d.equal()
		case c == '"':
			d.pos = i
			return d.skipValue()
		case c <= ' ':
			d.pos = i
			if d.pos > start {
				d.key = line[start:d.pos]
				if multibyte && bytes.ContainsRune(d.key, utf8.RuneError) {
					return false
				}
			}
			return true
		case c >= utf8.RuneSelf:
			multibyte = true
		}
	}
	d.pos = len(line)
	if d.pos > start {
		d.key = line[start:d.pos]
		if multibyte && bytes.ContainsRune(d.key, utf8.RuneError) {
			return false
		}
	}
	return true
}

func (d *logfmtLevelDecoder) equal() bool {
	line := d.line
	d.pos++
	if d.pos >= len(line) {
		return true
	}
	switch c := line[d.pos]; {
	case c <= ' ':
		return true
	case c == '"':
		return d.quotedValue()
	}
	start := d.pos
	for i := d.pos; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '=' || c == '"':
			d.pos = i
			return d.skipValue()
		case c <= ' ':
			d.pos = i
			if d.pos > start {
				d.value = line[start:d.pos]
			}
			return true
		}
	}
	d.pos = len(line)
	if d.pos > start {
		d.value = line[start:d.pos]
	}
	return true
}

func (d *logfmtLevelDecoder) skipValue() bool {
	line := d.line
	for i := d.pos; i < len(line); i++ {
		if line[i] <= ' ' {
			d.pos = i
			return false
		}
	}
	d.pos = len(line)
	return false
}

func (d *logfmtLevelDecoder) quotedValue() bool {
	line := d.line
	hasEsc, esc := false, false
	start := d.pos
	for i := d.pos + 1; i < len(line); i++ {
		c := line[i]
		switch {
		case esc:
			esc = false
		case c == '\\':
			hasEsc, esc = true, true
		case c == '"':
			d.pos = i + 1
			if hasEsc {
				quoted := line[start:d.pos]
				if !validLogfmtQuoted(quoted) {
					return false
				}
				d.value, d.hasEsc = quoted, true
				return true
			}
			start++
			if end := d.pos - 1; end > start {
				d.value = line[start:end]
			}
			return true
		}
	}
	d.pos = len(line)
	return false
}

// validLogfmtQuoted reports whether Loki's unquoteBytes accepts the quoted
// value s (including its quotes).
func validLogfmtQuoted(s []byte) bool {
	s = s[1 : len(s)-1]
	for r := 0; r < len(s); r++ {
		if s[r] != '\\' {
			continue
		}
		r++
		if r >= len(s) {
			return false
		}
		switch s[r] {
		case '"', '\\', '/', '\'', 'b', 'f', 'n', 'r', 't':
		case 'u':
			if r+5 > len(s) || !isHex4(s[r+1:r+5]) {
				return false
			}
			r += 4
		default:
			return false
		}
	}
	return true
}

func isHex4(s []byte) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// nonLevelWord stands for a decoded value that cannot be a level word.
var nonLevelWord = []byte{0}

// unquoteLogfmtLevel decodes a validated quoted logfmt value into buf. Level
// words are ASCII and short, so a value that decodes to a non-ASCII rune or
// does not fit buf comes back as nonLevelWord.
func unquoteLogfmtLevel(quoted, buf []byte) []byte {
	s := quoted[1 : len(quoted)-1]
	for r := 0; r < len(s); r++ {
		c := s[r]
		if c == '\\' {
			r++
			switch s[r] {
			case 'b':
				c = '\b'
			case 'f':
				c = '\f'
			case 'n':
				c = '\n'
			case 'r':
				c = '\r'
			case 't':
				c = '\t'
			case 'u':
				var rr rune
				for _, h := range s[r+1 : r+5] {
					rr <<= 4
					switch {
					case h >= '0' && h <= '9':
						rr |= rune(h - '0')
					case h >= 'a' && h <= 'f':
						rr |= rune(h-'a') + 10
					default:
						rr |= rune(h-'A') + 10
					}
				}
				r += 4
				if rr >= utf8.RuneSelf {
					return nonLevelWord
				}
				c = byte(rr)
			default:
				c = s[r]
			}
		} else if c >= utf8.RuneSelf {
			return nonLevelWord
		}
		if len(buf) == cap(buf) {
			return nonLevelWord
		}
		buf = append(buf, c)
	}
	return buf
}

// levelFromKeywords returns the level of the earliest bounded keyword in the
// line (case-insensitive), or "unknown". Boundaries follow Loki: a keyword
// must start the line or follow space, tab, LF, '[', '(', '{', '"' or '=',
// and must end the line or precede space, tab, LF, brackets, braces, parens,
// ':', ',', '!', '"' or '='.
func levelFromKeywords(line []byte) string {
	if bytes.Contains(line, []byte("İ")) {
		// strings.ToLower maps U+0130 to a one-byte 'i', which can complete
		// "info"; use the same lowering so positions and matches agree.
		return levelFromKeywordsLower([]byte(strings.ToLower(string(line))))
	}
	return levelFromKeywordsLower(line)
}

func levelFromKeywordsLower(line []byte) string {
	for i := 0; i < len(line); i++ {
		if level := keywordAt(line, i); level != "" {
			return level
		}
	}
	return levelUnknown
}

// keywordAt returns the level of a bounded keyword starting at line[i], or "".
func keywordAt(line []byte, i int) string {
	b := line[i]
	if b < 'A' || (b > 'Z' && b < 'a') || b > 'z' {
		return ""
	}
	c := b | 0x20
	switch c {
	case 't', 'd', 'f', 'c', 'e', 'w', 'i':
	default:
		return ""
	}
	if i > 0 && !isLevelLeftBoundary(line[i-1]) {
		return ""
	}
	rest := line[i:]
	switch c {
	case 't':
		if boundedKeyword(rest, "trace") {
			return levelTrace
		}
	case 'd':
		if boundedKeyword(rest, "debug") {
			return levelDebug
		}
	case 'f':
		if boundedKeyword(rest, "fatal") {
			return levelFatal
		}
	case 'c':
		if boundedKeyword(rest, "critical") {
			return levelCritical
		}
	case 'e':
		if boundedKeyword(rest, "error") || boundedKeyword(rest, "err") {
			return levelError
		}
	case 'w':
		if boundedKeyword(rest, "warning") || boundedKeyword(rest, "warn") {
			return levelWarn
		}
	case 'i':
		if boundedKeyword(rest, "info") {
			return levelInfo
		}
	}
	return ""
}

// boundedKeyword reports whether s starts with word (ASCII case-insensitive)
// followed by the end of s or a right boundary.
func boundedKeyword(s []byte, word string) bool {
	if len(s) < len(word) {
		return false
	}
	for j := 0; j < len(word); j++ {
		c := s[j]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != word[j] {
			return false
		}
	}
	return len(s) == len(word) || isLevelRightBoundary(s[len(word)])
}

func isLevelLeftBoundary(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '[' || c == '(' || c == '{' || c == '"' || c == '='
}

func isLevelRightBoundary(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '[', ']', '(', ')', '{', '}', ':', ',', '!', '"', '=':
		return true
	}
	return false
}
