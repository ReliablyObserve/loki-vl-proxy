package proxy

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// A plain reference for the detected_level rules (detected_level.go), written for reading, not speed: string
// lower-casing, a regular expression for the keyword scan, encoding/json for the JSON line, a split for logfmt.
// TestDetectedLevelReferenceMatchesRecordedLoki holds it to Loki's recorded answers (every row of
// testdata/detected_level_golden.json); FuzzDetectedLevelMatchesReference holds the proxy's byte-level
// derivation to it on any input.

var referenceLevelNames = []string{
	"level", "LEVEL", "Level", "log.level",
	"severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl",
	"severity_text", "Severity_Text", "SEVERITY_TEXT",
}

var referenceVLSeverityTexts = func() map[string]int {
	m := map[string]int{"Unspecified": 0}
	for i, base := range []string{"Trace", "Debug", "Info", "Warn", "Error", "Fatal"} {
		for k := 0; k < 4; k++ {
			name := base
			if k > 0 {
				name += strconv.Itoa(k + 1)
			}
			m[name] = i*4 + k + 1
		}
	}
	return m
}()

func referenceVLSeverityText(number string) string {
	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return ""
	}
	for name, i := range referenceVLSeverityTexts {
		if err == nil && n >= 1 && n <= 24 && int64(i) == n {
			return name
		}
	}
	return "Unspecified"
}

func referenceCanonical(v string) string {
	switch strings.ToLower(v) {
	case "trace", "trc":
		return "trace"
	case "debug", "dbg":
		return "debug"
	case "info", "inf", "information":
		return "info"
	case "warn", "warning", "wrn":
		return "warn"
	case "error", "err":
		return "error"
	case "critical":
		return "critical"
	case "fatal":
		return "fatal"
	}
	return ""
}

// referenceCanonicalASCII is referenceCanonical for values compared with ASCII case folding only: a stored value
// such as İNFO is no known spelling (strings.ToLower would make it info).
func referenceCanonicalASCII(v string) string {
	for _, r := range v {
		if r >= utf8.RuneSelf {
			return ""
		}
	}
	return referenceCanonical(v)
}

func referenceNormalized(v string) string {
	if c := referenceCanonicalASCII(v); c != "" {
		return c
	}
	return v
}

func referenceSeverityNumber(v string) string {
	n, err := strconv.ParseInt(v, 10, 64)
	switch {
	case err != nil:
		return "info"
	case n == 0 || n > 24:
		return "unknown"
	case n <= 4:
		return "trace"
	case n <= 8:
		return "debug"
	case n <= 12:
		return "info"
	case n <= 16:
		return "warn"
	case n <= 20:
		return "error"
	}
	return "fatal"
}

// referenceKeywordRE: a level word at the start of the text, followed by a boundary byte or the end.
var referenceKeywordRE = regexp.MustCompile(`^(trace|debug|info|warning|warn|error|err|critical|fatal)(?:$|[ \t\n\[\]{}()":=,!])`)

// referenceKeywords: the first level word of the line lower-cased (Loki's lower-casing turns İ into i), standing
// after a boundary byte or at the start.
func referenceKeywords(line string) string {
	lower := strings.Map(func(r rune) rune {
		if r == 'İ' {
			return 'i'
		}
		if 'A' <= r && r <= 'Z' {
			return r + 'a' - 'A'
		}
		return r
	}, line)
	for i := 0; i < len(lower); i++ {
		if i > 0 && strings.IndexByte(" \t\n[({\"=", lower[i-1]) < 0 {
			continue
		}
		if m := referenceKeywordRE.FindStringSubmatch(lower[i:]); m != nil {
			switch m[1] {
			case "warning":
				return "warn"
			case "err":
				return "error"
			}
			return m[1]
		}
	}
	return "unknown"
}

// referenceJSONShaped: a line Loki reads as JSON, braces first and last once spaces, tabs, CRs and LFs are trimmed.
func referenceJSONShaped(line string) (string, bool) {
	t := strings.Trim(line, " \t\r\n")
	return t, len(t) >= 2 && t[0] == '{' && t[len(t)-1] == '}'
}

// referenceJSON walks a JSON-shaped line: the raw value of the first level key holding a string (decoded key, top
// object or one object down, arrays skipped), found before the first syntax error.
func referenceJSON(t string) (string, bool) {
	r := &referenceJSONReader{s: t}
	r.value(1)
	return r.v, r.found
}

type referenceJSONReader struct {
	s     string
	i     int
	v     string
	found bool
	nest  int
}

func (r *referenceJSONReader) blanks() {
	for r.i < len(r.s) && strings.IndexByte(" \t\r\n", r.s[r.i]) >= 0 {
		r.i++
	}
}

var referenceJSONStringRE = regexp.MustCompile(`^"((?:[^"\\]|\\["\\/bfnrt]|\\u[0-9a-fA-F]{4})*)"`)
var referenceJSONNumberRE = regexp.MustCompile(`^-?[0-9.eE+-]*|^[0-9][0-9.eE+-]*`)

func (r *referenceJSONReader) str() (string, bool) {
	m := referenceJSONStringRE.FindStringSubmatch(r.s[r.i:])
	if m == nil {
		return "", false
	}
	r.i += len(m[0])
	return m[1], true
}

func (r *referenceJSONReader) value(depth int) bool {
	if r.i >= len(r.s) {
		return false
	}
	switch c := r.s[r.i]; {
	case c == '{':
		return r.object(depth)
	case c == '[':
		return r.array()
	case c == '"':
		_, ok := r.str()
		return ok
	case c == '-' || ('0' <= c && c <= '9'):
		r.i += len(referenceJSONNumberRE.FindString(r.s[r.i:]))
		return true
	}
	for _, lit := range []string{"true", "false", "null"} {
		if strings.HasPrefix(r.s[r.i:], lit) {
			r.i += len(lit)
			return true
		}
	}
	return false
}

func (r *referenceJSONReader) object(depth int) bool {
	if r.nest++; r.nest > 10000 {
		return false
	}
	defer func() { r.nest-- }()
	r.i++
	for {
		r.blanks()
		if r.i >= len(r.s) {
			return false
		}
		if r.s[r.i] == '}' {
			r.i++
			return true
		}
		if r.s[r.i] != '"' {
			return false
		}
		key, ok := r.str()
		if !ok {
			return false
		}
		r.blanks()
		if r.i >= len(r.s) || r.s[r.i] != ':' {
			return false
		}
		r.i++
		r.blanks()
		if r.i >= len(r.s) {
			return false
		}
		switch {
		case r.s[r.i] == '"':
			v, ok := r.str()
			if !ok {
				return false
			}
			var decoded string
			if !r.found && depth <= 2 && json.Unmarshal([]byte(`"`+key+`"`), &decoded) == nil && referenceIsLevelName(decoded) {
				r.v, r.found = v, true
			}
		case r.s[r.i] == '{' && depth < 2:
			if !r.object(depth + 1) {
				return false
			}
		default:
			if !r.value(3) {
				return false
			}
		}
		r.blanks()
		if r.i >= len(r.s) {
			return false
		}
		switch r.s[r.i] {
		case ',':
			r.i++
		case '}':
			r.i++
			return true
		default:
			return false
		}
	}
}

func (r *referenceJSONReader) array() bool {
	if r.nest++; r.nest > 10000 {
		return false
	}
	defer func() { r.nest-- }()
	r.i++
	for {
		r.blanks()
		if r.i >= len(r.s) {
			return false
		}
		if r.s[r.i] == ']' {
			r.i++
			return true
		}
		if !r.value(3) {
			return false
		}
		r.blanks()
		if r.i >= len(r.s) {
			return false
		}
		switch r.s[r.i] {
		case ',':
			r.i++
		case ']':
			r.i++
			return true
		default:
			return false
		}
	}
}

func referenceIsLevelName(name string) bool {
	for _, n := range referenceLevelNames {
		if n == name {
			return true
		}
	}
	return false
}

// referenceLevelLogfmt is the value of the logfmt level key earliest in referenceLevelNames (names case-insensitive,
// the first occurrence of each, a bare or empty value counted), quoted values decoded; found is false without one.
// The read stops at the first syntax error: a quote or equals sign inside a key or an unquoted value, an
// unterminated quoted value or one with a bad escape, a quoted value followed by more than blanks.
func referenceLevelLogfmt(line string) (string, bool) {
	if !strings.Contains(line, "=") {
		return "", false
	}
	groups := []string{"level", "log.level", "severity", "severitytext", "lvl", "severity_text"}
	found := map[int]string{}
	pairRE := regexp.MustCompile(`^([^\x00-\x20="]+)(?:=(?:"((?:[^"\\]|\\["\\/bfnrt]|\\u[0-9a-fA-F]{4})*)"|([^\x00-\x20="]*)))?(?:[\x00-\x20]|$)`)
	for rest := strings.TrimLeft(line, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f "); rest != ""; {
		m := pairRE.FindStringSubmatchIndex(rest)
		if m == nil {
			break
		}
		key := rest[m[2]:m[3]]
		value := ""
		switch {
		case m[4] >= 0:
			decoded, err := referenceLevelUnquote(rest[m[4]:m[5]])
			if err != nil {
				return "", false
			}
			value = decoded
		case m[6] >= 0:
			value = rest[m[6]:m[7]]
		}
		for g, name := range groups {
			if strings.EqualFold(key, name) && len(key) == len(name) {
				if _, ok := found[g]; !ok {
					found[g] = value
				}
			}
		}
		rest = strings.TrimLeft(rest[m[1]:], "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f ")
	}
	for g := range groups {
		if v, ok := found[g]; ok {
			return v, true
		}
	}
	return "", false
}

// referenceLevelUnquote decodes the escapes JSON strings have, as logfmt values carry them.
func referenceLevelUnquote(s string) (string, error) {
	var out string
	err := json.Unmarshal([]byte(`"`+s+`"`), &out)
	return out, err
}

func referenceDetectedLevel(stream, fields map[string]string, line string, unpacked, bodyScan bool) string {
	if v := fields["detected_level"]; v != "" {
		return referenceNormalized(v)
	}
	for _, name := range referenceLevelNames {
		if stream[name] != "" {
			return referenceNormalized(stream[name])
		}
	}
	if unpacked {
		v := ""
		for _, name := range referenceLevelNames {
			if fields[name] != "" {
				v = fields[name]
				break
			}
		}
		if v == "" {
			keys := make([]string, 0, len(fields))
			for k := range fields {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if parent, name, ok := strings.Cut(k, "."); ok && parent != "" && !strings.Contains(name, ".") && referenceIsLevelName(name) && fields[k] != "" {
					v = fields[k]
					break
				}
			}
		}
		if c := referenceCanonicalASCII(v); c != "" {
			return c
		}
		if !bodyScan {
			return "unknown"
		}
		return referenceKeywords(line)
	}
	number, text, severity := fields["severity_number"], fields["severity_text"], fields["severity"]
	derivedText := number != "" && text != "" && text == referenceVLSeverityText(number)
	derivedSeverity := -1
	if number == "" && text == "" && severity != "" {
		if n, ok := referenceVLSeverityTexts[severity]; ok {
			derivedSeverity = n
		}
	}
	for _, name := range referenceLevelNames {
		v := fields[name]
		if v == "" || name == "log.level" || (name == "severity_text" && derivedText) || (name == "severity" && derivedSeverity >= 0) {
			continue
		}
		return referenceNormalized(v)
	}
	switch {
	case derivedSeverity > 0:
		return referenceSeverityNumber(strconv.Itoa(derivedSeverity))
	case derivedSeverity == 0 || number == "" || (derivedText && number == "0"):
	default:
		return referenceSeverityNumber(number)
	}
	if !bodyScan {
		return "unknown"
	}
	if t, shaped := referenceJSONShaped(line); shaped {
		if v, found := referenceJSON(t); found {
			if c := referenceCanonicalASCII(v); c != "" {
				return c
			}
		}
	} else if v, found := referenceLevelLogfmt(line); found {
		if c := referenceCanonicalASCII(v); c != "" {
			return c
		}
	}
	return referenceKeywords(line)
}

// conformance: severity-detected-level-derivation, severity/recorded-level-table
func TestDetectedLevelReferenceMatchesRecordedLoki(t *testing.T) {
	for _, tc := range loadDetectedLevelGolden(t) {
		body := tc.Msg
		if tc.JSONUnpacked {
			body = goldenReconstructedLine(tc.Msg, tc.Fields)
		}
		if got := referenceDetectedLevel(tc.Stream, tc.Fields, body, tc.JSONUnpacked, true); got != tc.Want {
			t.Errorf("%s: reference %q, Loki %q (msg %q, stream %v, fields %v)", tc.Name, got, tc.Want, truncateForLog(tc.Msg), tc.Stream, tc.Fields)
		}
	}
}

// FuzzDetectedLevelMatchesReference: the proxy's derivation equals the reference on any line, stream label and
// stored field, with and without the line scan, for stored and unpacked rows.
//
// conformance: severity-detected-level-derivation, severity/recorded-level-table
func FuzzDetectedLevelMatchesReference(f *testing.F) {
	for _, tc := range loadDetectedLevelGolden(f) {
		if tc.JSONUnpacked || len(tc.Stream) > 1 || len(tc.Fields) > 2 {
			continue
		}
		var sk, sv, fk, fv, gk, gv string
		for k, v := range tc.Stream {
			sk, sv = k, v
		}
		keys := goldenSortedKeys(tc.Fields)
		if len(keys) > 0 {
			fk, fv = keys[0], tc.Fields[keys[0]]
		}
		if len(keys) > 1 {
			gk, gv = keys[1], tc.Fields[keys[1]]
		}
		f.Add(tc.Msg, sk, sv, fk, fv, gk, gv, false)
	}
	f.Add(`{"a":{"level":"warn"}}`, "", "", "a.lvl", "dbg", "level", "chatty", true)
	f.Fuzz(func(t *testing.T, line, sk, sv, fk, fv, gk, gv string, unpacked bool) {
		if len(line) > 4096 {
			return
		}
		stream, fields := map[string]string{}, map[string]string{}
		if sv != "" {
			stream[sk] = sv
		}
		if fv != "" {
			fields[fk] = fv
		}
		if gv != "" && gk != fk {
			fields[gk] = gv
		}
		for _, scan := range []bool{false, true} {
			want := referenceDetectedLevel(stream, fields, line, unpacked, scan)
			got := deriveDetectedLevel(detectedLevelInput{stream: goldenLevelFields(stream), fields: goldenLevelFields(fields), body: []byte(line), jsonUnpacked: unpacked, bodyScan: scan}).String()
			if got != want {
				t.Fatalf("line %q stream %v fields %v unpacked %v scan %v: proxy %q, reference %q", line, stream, fields, unpacked, scan, got, want)
			}
		}
	})
}

// TestDetectedLevelLineEdgesMatchReference: lines whose answer turns on how the line is read (JSON well-formedness
// and number grammar, nesting, decoded keys and raw values, logfmt escapes and priorities, boundary bytes), with
// values the keyword scan cannot find, so the reading itself decides.
//
// conformance: severity-detected-level-derivation, severity/recorded-level-table
func TestDetectedLevelLineEdgesMatchReference(t *testing.T) {
	deep := strings.Repeat(`{"a":`, 3) + `{"level":"inf"}` + strings.Repeat("}", 3)
	lines := []string{
		`{"n":1.5e+3,"level":"inf"}`, `{"n":-0,"level":"inf"}`, `{"n":01,"level":"inf"}`, `{"n":1.,"level":"inf"}`,
		`{"n":-,"level":"inf"}`, `{"n":1e,"level":"inf"}`, `{"n":tru,"level":"inf"}`, `{"n":nul,"level":"inf"}`,
		`{"n":[1,{"level":"dbg"},"x"],"level":"inf"}`, `{"n":[],"m":{},"level":"inf"}`, `{"n":[1,],"level":"inf"}`,
		`{"a":{"b":{"lvl":"dbg"}},"level":"inf"}`, `{"a":{"lvl":"dbg"}}`, deep,
		`{"level":"inf"}`, `{"le\x":"inf"}`, `{"level":"inf"}`, `{"level":"inf\t"}`, "{\"level\":\"inf\x01\"}",
		`{"level":"inf"} trailing`, `{"level":"inf"`, `{"level":"inf",}`, `{"level" "inf"}`, `{level:"inf"}`, `  {"level":"inf"}`,
		"\f{\"level\":\"inf\"}", `{}`, `{ }`, `[{"level":"inf"}]`,
		`level="inf"`, `level="in\x66"`, `level="inf`, `level=inf"x`, `LeVeL=inf`, `severity_TEXT=dbg lvl=inf`,
		`lvl=inf severity=dbg`, `level= lvl=inf`, `a=b level`, `=inf`, `level=inf=x`,
		"x\v error", "x\f error", "x\r error", "error\v", "(error)", "[ERROR]", "{err}", "İnfo", "x-ınfo info",
	}
	for _, line := range lines {
		want := referenceDetectedLevel(nil, nil, line, false, true)
		got := deriveDetectedLevel(detectedLevelInput{body: []byte(line), bodyScan: true}).String()
		if got != want {
			t.Errorf("%q: proxy %q, reference %q", line, got, want)
		}
	}
	// the walk stops at a nesting past its bound, without deep recursion
	nested := strings.Repeat("[", 20000) + strings.Repeat("]", 20000)
	if _, found := levelFromJSONBody([]byte(`{"a":` + nested + `,"level":"inf"}`)); found {
		t.Fatal("a level key after a line nested 20000 deep is read")
	}
}

// TestDeriveDetectedLevelAllocatesNothing: the derivation allocates nothing per row for stored fields, JSON,
// logfmt without escapes and the keyword scan.
func TestDeriveDetectedLevelAllocatesNothing(t *testing.T) {
	inputs := []detectedLevelInput{
		{fields: goldenLevelFields(map[string]string{"level": "warn"}), bodyScan: true},
		{fields: goldenLevelFields(map[string]string{"severity_number": "13", "severity_text": "Warn"}), bodyScan: true},
		{body: []byte(`{"ts":"2026-01-01T00:00:00Z","n":12.5,"tags":["a"],"ctx":{"lvl":"dbg"},"msg":"x"}`), bodyScan: true},
		{body: []byte(`ts=2026 level=Information msg="request served" n=3`), bodyScan: true},
		{body: []byte("request handled by worker pool, then an error"), bodyScan: true},
	}
	for _, in := range inputs {
		if n := testing.AllocsPerRun(100, func() { detectedLevelBenchSink = deriveDetectedLevel(in) }); n != 0 {
			t.Errorf("%q: %v allocations per row", in.body, n)
		}
	}
}
