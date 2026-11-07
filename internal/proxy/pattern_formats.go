package proxy

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	fj "github.com/valyala/fastjson"
)

// patternFormat is how a line is tokenized for the patterns miner, by the format Loki's pattern ingester gives
// it (pkg/pattern/drain DetectLogFormat): a JSON object, a logfmt line, or any other line.
type patternFormat uint8

const (
	// patternFormatPlain lines split at spaces and punctuation (patternLineTokenizer).
	patternFormatPlain patternFormat = iota
	// patternFormatJSON lines are mined from their message field only, written between variables.
	patternFormatJSON
	// patternFormatLogfmt lines are their keys and values, each value one token, written back as logfmt.
	patternFormatLogfmt
)

// patternJSONMessageFields are the fields a JSON line's pattern is mined from, the first that holds a string:
// the defaults of Loki's pattern_ingester_tokenizable_json_fields_default.
var patternJSONMessageFields = []string{"log", "message", "msg", "msg_", "_msg", "content"}

var patternJSONParsers fj.ParserPool

// patternLogfmtVariableKey reports whether a logfmt key's value is always a variable of the pattern: a timestamp
// or a trace id.
func patternLogfmtVariableKey(key string) bool {
	return key == "t" || strings.EqualFold(key, "ts") || strings.EqualFold(key, "time") ||
		strings.EqualFold(key, "timestamp") || strings.EqualFold(key, "traceID")
}

// patternLineTokens tokenizes a line for the miner by its format. ok is false for a line the miner has no pattern
// for: a JSON object without a message field (its fields are its values), or a line with fewer than
// patternMinTokens or more than patternMaxTokens tokens.
func (m *patternMiner) patternLineTokens(line string) (tokens []string, spacesAfter []int, format patternFormat, ok bool) {
	switch {
	case isPatternJSONLine(line):
		text, found := patternJSONMessage(line)
		if !found {
			return nil, nil, patternFormatJSON, false
		}
		tokens, spacesAfter, ok = m.tokenizer.Tokenize(text)
		return tokens, spacesAfter, patternFormatJSON, ok
	case isPatternLogfmtLine(line):
		tokens = make([]string, 0, 16)
		lokiLogfmtPairs(line, func(key, value string) {
			if patternLogfmtVariableKey(key) {
				value = patternVarPlaceholder
			}
			tokens = append(tokens, key, value)
		})
		if len(tokens) < patternMinTokens || len(tokens) > patternMaxTokens {
			return nil, nil, patternFormatLogfmt, false
		}
		return tokens, nil, patternFormatLogfmt, true
	}
	tokens, spacesAfter, ok = m.tokenizer.Tokenize(line)
	return tokens, spacesAfter, patternFormatPlain, ok
}

// isPatternJSONLine reports whether Loki reads a line as JSON for its patterns: it starts with { and ends with }.
func isPatternJSONLine(line string) bool {
	return len(line) >= 2 && line[0] == '{' && line[len(line)-1] == '}'
}

// patternJSONMessage is a JSON line's first top-level message field that holds a string, as the line writes it:
// the text between the string's quotes with its escapes still escaped, which is the text Loki mines.
func patternJSONMessage(line string) (string, bool) {
	parser := patternJSONParsers.Get()
	defer patternJSONParsers.Put(parser)
	v, err := parser.Parse(line)
	if err != nil || v.Type() != fj.TypeObject {
		return "", false
	}
	for _, field := range patternJSONMessageFields {
		if x := v.Get(field); x != nil && x.Type() == fj.TypeString {
			if raw, ok := patternJSONRawString(line, field); ok {
				return raw, true
			}
			return string(x.GetStringBytes()), true
		}
	}
	return "", false
}

// patternJSONRawString is the text between the quotes of the string a valid JSON object's top-level key holds, as
// the object writes it. ok is false when the object does not write the key as plain text (an escaped key), where
// the caller reads the decoded string.
func patternJSONRawString(object, key string) (string, bool) {
	s := patternJSONScanner{text: object, at: strings.IndexByte(object, '{') + 1}
	for {
		name, ok := s.string()
		if !ok || !s.expect(':') {
			return "", false
		}
		if s.peek() == '"' {
			value, ok := s.string()
			if !ok {
				return "", false
			}
			if name == key {
				return value, true
			}
		} else if !s.skipValue() {
			return "", false
		}
		if !s.expect(',') {
			return "", false
		}
	}
}

// patternJSONScanner reads the top level of a valid JSON object in place.
type patternJSONScanner struct {
	text string
	at   int
}

func (s *patternJSONScanner) skipSpace() {
	for s.at < len(s.text) && (s.text[s.at] == ' ' || s.text[s.at] == '\t' || s.text[s.at] == '\n' || s.text[s.at] == '\r') {
		s.at++
	}
}

// peek is the next byte after any space, 0 at the end.
func (s *patternJSONScanner) peek() byte {
	s.skipSpace()
	if s.at >= len(s.text) {
		return 0
	}
	return s.text[s.at]
}

// expect moves past c, the next byte after any space.
func (s *patternJSONScanner) expect(c byte) bool {
	if s.peek() != c {
		return false
	}
	s.at++
	return true
}

// string reads a string and returns the text between its quotes as written.
func (s *patternJSONScanner) string() (string, bool) {
	if s.peek() != '"' {
		return "", false
	}
	for i := s.at + 1; i < len(s.text); i++ {
		switch s.text[i] {
		case '\\':
			i++
		case '"':
			value := s.text[s.at+1 : i]
			s.at = i + 1
			return value, true
		}
	}
	return "", false
}

// skipValue moves past a value that is not a string: to the byte after it, which is its comma or the object's end.
func (s *patternJSONScanner) skipValue() bool {
	depth := 0
	for s.at < len(s.text) {
		switch c := s.text[s.at]; {
		case c == '"':
			if _, ok := s.string(); !ok {
				return false
			}
			continue
		case c == '{' || c == '[':
			depth++
		case (c == '}' || c == ']') && depth > 0:
			depth--
		case c == '}' || c == ']' || c == ',' && depth == 0:
			return true
		}
		s.at++
	}
	return false
}

// isPatternLogfmtLine reports whether Loki reads a line as logfmt for its patterns: the whole line is key=value
// pairs separated by one space, a key being word characters and a value either a run of non-space characters
// not starting with a quote, or a quoted string of at least one character.
func isPatternLogfmtLine(line string) bool {
	pos := 0
	for {
		keyStart := pos
		for pos < len(line) && isPatternWordByte(line[pos]) {
			pos++
		}
		if pos == keyStart || pos >= len(line) || line[pos] != '=' {
			return false
		}
		pos++
		if pos >= len(line) {
			return false
		}
		if line[pos] == '"' {
			end := strings.IndexByte(line[pos+1:], '"')
			if end < 1 {
				return false
			}
			pos += end + 2
		} else {
			valueStart := pos
			for pos < len(line) && line[pos] != ' ' {
				pos++
			}
			if pos == valueStart {
				return false
			}
		}
		if pos == len(line) {
			return true
		}
		if line[pos] != ' ' || pos+1 >= len(line) {
			return false
		}
		pos++
	}
}

func isPatternWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// joinPatternTokens writes a cluster's tokens as its pattern, by the format of the lines it was mined from: a
// JSON line's message between variables, a logfmt line's keys and values as logfmt, any other line as the
// tokenizer joins it.
func (m *patternMiner) joinPatternTokens(tokens []string, spacesAfter []int, format patternFormat) string {
	switch format {
	case patternFormatJSON:
		return deduplicatePlaceholders(patternVarPlaceholder+m.tokenizer.Join(tokens, spacesAfter)+patternVarPlaceholder, patternVarPlaceholder)
	case patternFormatLogfmt:
		var b strings.Builder
		for i := 0; i+1 < len(tokens); i += 2 {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(tokens[i])
			b.WriteByte('=')
			value := tokens[i+1]
			if isPatternPlaceholder(value) {
				value = patternVarPlaceholder
			}
			b.WriteString(patternLogfmtValue(value))
		}
		return deduplicatePlaceholders(b.String(), patternVarPlaceholder)
	}
	return m.tokenizer.Join(tokens, spacesAfter)
}

// patternLogfmtValue is a value as logfmt writes it: quoted when it holds a space, a quote, an equals sign or a
// character a bare value cannot.
func patternLogfmtValue(value string) string {
	for _, r := range value {
		if r <= ' ' || r == '=' || r == '"' || r == utf8.RuneError || !unicode.IsPrint(r) {
			return strconv.Quote(value)
		}
	}
	return value
}
