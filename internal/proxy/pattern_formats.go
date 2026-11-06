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

// patternJSONMessage is a JSON line's first top-level message field that holds a string.
func patternJSONMessage(line string) (string, bool) {
	parser := patternJSONParsers.Get()
	defer patternJSONParsers.Put(parser)
	v, err := parser.Parse(line)
	if err != nil || v.Type() != fj.TypeObject {
		return "", false
	}
	for _, field := range patternJSONMessageFields {
		if x := v.Get(field); x != nil && x.Type() == fj.TypeString {
			return string(x.GetStringBytes()), true
		}
	}
	return "", false
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

// appendLogfmtStructureSignature is appendPatternStructureSignature for a logfmt line's tokens, its keys and values
// in turn: a value is a variable, as any value after '=' is on another line, unless it has a placeholder kind.
func appendLogfmtStructureSignature(dst []byte, tokens, placeholders []string) []byte {
	for i, token := range tokens {
		if i > 0 {
			dst = append(dst, '\x1e')
		}
		part := token
		if i%2 == 1 {
			part = patternVarPlaceholder
			if kind := patternPlaceholderKindAt(tokens, placeholders, i); kind != "" {
				part = kind
			}
		}
		dst = append(dst, part...)
	}
	return dst
}

// patternFamilySimilarity is the share of fields a cluster shares with the first of a group to join it (a field
// sharing with one its pattern matches), as Loki's drain joins a line to a cluster of lines as similar.
const patternFamilySimilarity = 0.3

// patternMaxFamilyGroups bounds the groups of a family a cluster is compared with: a cluster like none of them is
// left as it was mined, so wildcarding stays linear in the clusters however varied the lines.
const patternMaxFamilyGroups = 64

// patternWildcardPasses is how many times at most wildcardVaryingFields runs over the clusters.
const patternWildcardPasses = 3

// wildcardPatterns runs wildcardVaryingFields over the clusters' distinct patterns until they stop changing, at most
// patternWildcardPasses times: lines that shared too little to group before their values were variables may group
// once they are. Clusters that read the same are wildcarded as one.
func wildcardPatterns(clusters []*patternBucket) {
	for pass := 0; pass < patternWildcardPasses; pass++ {
		at := make(map[[2]string]int, len(clusters))
		var distinct []*patternBucket
		of := make([]int, len(clusters))
		for i, c := range clusters {
			k := [2]string{string(rune(c.format)), c.pattern}
			j, seen := at[k]
			if !seen {
				j = len(distinct)
				at[k] = j
				distinct = append(distinct, &patternBucket{pattern: c.pattern, format: c.format})
			}
			of[i] = j
		}
		if !wildcardVaryingFields(distinct) {
			return
		}
		for i, c := range clusters {
			c.pattern = distinct[of[i]].pattern
		}
	}
}

// wildcardVaryingFields makes the clusters' patterns templates rather than lines, as Loki's patterns are, so a kind
// of line is one pattern rather than one per path, user agent or number. A field is a run of text between spaces, a
// quoted string being one field. Clusters of one format with as many fields and the same first word (unless it holds
// a number; for a key=value field its key) form a group when at least patternFamilySimilarity of their fields are
// the same as the group's first, whatever their level: a field whose value differs within the group becomes a
// variable, keeping the punctuation its values share at either end (a key=, a bracket, a quote), and a value that
// never varies (a method, a component, a status that is always 200) stays, as in Loki. A pattern is changed only if
// it still matches what it was mined as. Clusters that then read the same are merged by the caller. changed reports
// whether a pattern changed.
func wildcardVaryingFields(clusters []*patternBucket) (changed bool) {
	type group struct {
		first   []string
		members [][]string
		at      []*patternBucket
	}
	families := map[string][]*group{}
	var key strings.Builder
	for _, c := range clusters {
		fields := patternFields(c.pattern)
		key.Reset()
		key.WriteByte(byte(c.format))
		key.WriteString(strconv.Itoa(len(fields)))
		first := fields[0]
		if eq := strings.IndexByte(first, '='); eq > 0 && isPatternKey(first[:eq]) {
			// A key=value field: the key is what the lines share.
			first = first[:eq+1]
		}
		if !strings.ContainsAny(first, "0123456789<") {
			key.WriteByte(0)
			key.WriteString(first)
		}
		k := key.String()
		var joined *group
		for _, g := range families[k] {
			need, same := patternFamilySimilarity*float64(len(fields)), 0
			for i, field := range fields {
				if samePatternField(field, g.first[i]) {
					if same++; float64(same) >= need {
						break
					}
				}
			}
			if float64(same) >= need {
				joined = g
				break
			}
		}
		if joined == nil {
			if len(families[k]) >= patternMaxFamilyGroups {
				continue
			}
			joined = &group{first: fields}
			families[k] = append(families[k], joined)
		}
		joined.members = append(joined.members, fields)
		joined.at = append(joined.at, c)
	}
	values := make([]string, 0, 16)
	for _, family := range families {
		for _, g := range family {
			if len(g.members) == 1 {
				continue
			}
			for i := range g.first {
				values = values[:0]
				for _, fields := range g.members {
					values = append(values, fields[i])
				}
				if wildcard, ok := wildcardPatternValues(values); ok {
					for _, fields := range g.members {
						fields[i] = wildcard
					}
				}
			}
			wildcarded := make([]string, len(g.at))
			for j, c := range g.at {
				wildcarded[j] = deduplicatePlaceholders(strings.Join(g.members[j], " "), patternVarPlaceholder)
				if !patternMatchesText(wildcarded[j], c.pattern) {
					// A literal the variables leave would be found earlier in a value than where it is: the
					// pattern would not match the lines, so the group keeps the patterns as mined.
					wildcarded = nil
					break
				}
			}
			for j, pattern := range wildcarded {
				changed = changed || g.at[j].pattern != pattern
				g.at[j].pattern = pattern
			}
		}
	}
	return changed
}

// samePatternField reports whether two fields of patterns are the same for grouping: equal, or one a pattern the
// other matches.
func samePatternField(a, b string) bool {
	if a == b {
		return true
	}
	return strings.Contains(b, patternVarPlaceholder) && patternMatchesText(b, a) ||
		strings.Contains(a, patternVarPlaceholder) && patternMatchesText(a, b)
}

func isPatternKey(key string) bool {
	for i := 0; i < len(key); i++ {
		if c := key[i]; !isPatternWordByte(c) && c != '.' && c != '-' {
			return false
		}
	}
	return true
}

// patternFields splits a pattern at its spaces, except within a quoted string.
func patternFields(text string) []string {
	fields := make([]string, 0, strings.Count(text, " ")+1)
	start, quoted := 0, false
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '\\':
			i++
		case '"':
			quoted = !quoted
		case ' ':
			if !quoted {
				fields = append(fields, text[start:i])
				start = i + 1
			}
		}
	}
	return append(fields, text[start:])
}

// wildcardPatternValues is what a field reads as across a group whose values are values: the value when they are
// all the same; for quoted strings of as many words, each word wildcarded in turn; otherwise a variable between the
// punctuation all values start and end with. ok is false when they differ and a variable would match none of a
// value's text, so the field is left as it is.
func wildcardPatternValues(values []string) (string, bool) {
	first := values[0]
	same := true
	for _, v := range values[1:] {
		same = same && v == first
	}
	if same {
		return first, false
	}
	if words := quotedPatternWords(first); words > 1 {
		parts := make([][]string, len(values))
		ok := true
		for j, v := range values {
			if quotedPatternWords(v) != words {
				ok = false
				break
			}
			parts[j] = strings.Split(v[1:len(v)-1], " ")
		}
		if ok {
			out := make([]string, words)
			column := make([]string, len(values))
			for w := range out {
				for j := range parts {
					column[j] = parts[j][w]
				}
				if wildcard, changed := wildcardPatternValues(column); changed {
					out[w] = wildcard
				} else if allPatternValuesEqual(column) {
					out[w] = column[0]
				} else {
					ok = false
					break
				}
			}
			if ok {
				return `"` + strings.Join(out, " ") + `"`, true
			}
		}
	}
	prefix, suffix := len(first), len(first)
	for _, v := range values[1:] {
		prefix = min(prefix, commonPrefixLen(first, v))
		suffix = min(suffix, commonSuffixLen(first, v))
	}
	for prefix > 0 && isPatternWordByte(first[prefix-1]) {
		prefix--
	}
	for suffix > 0 && isPatternWordByte(first[len(first)-suffix]) {
		suffix--
	}
	for _, v := range values {
		if len(v) <= prefix+suffix {
			// The variable would be empty for this value: drop the shared ends, then give up.
			prefix, suffix = 0, 0
			if v == "" {
				return "", false
			}
		}
	}
	return first[:prefix] + patternVarPlaceholder + first[len(first)-suffix:], true
}

func allPatternValuesEqual(values []string) bool {
	for _, v := range values[1:] {
		if v != values[0] {
			return false
		}
	}
	return true
}

// quotedPatternWords is how many words a quoted string holds between its quotes, 0 for any other field.
func quotedPatternWords(field string) int {
	if len(field) < 2 || field[0] != '"' || field[len(field)-1] != '"' {
		return 0
	}
	return strings.Count(field[1:len(field)-1], " ") + 1
}

func commonPrefixLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func commonSuffixLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[len(a)-1-i] == b[len(b)-1-i] {
		i++
	}
	return i
}

// patternMatchesText reports whether a pattern matches a text as Loki's pattern line filter (|>) does, the filter
// Logs Drilldown adds when a pattern is included: each literal is found at its first occurrence after the text
// before it (a leading literal anywhere), every variable holds at least one character, and a pattern that ends on a
// literal ends the text there.
func patternMatchesText(pattern, text string) bool {
	pos, variable := 0, false
	for pattern != "" {
		if strings.HasPrefix(pattern, patternVarPlaceholder) {
			variable = true
			pattern = pattern[len(patternVarPlaceholder):]
			continue
		}
		next := strings.Index(pattern, patternVarPlaceholder)
		if next < 0 {
			next = len(pattern)
		}
		literal := pattern[:next]
		pattern = pattern[next:]
		i := strings.Index(text[pos:], literal)
		if i < 0 || variable && i == 0 {
			return false
		}
		pos += i + len(literal)
		variable = false
	}
	if variable {
		return pos < len(text)
	}
	return pos == len(text)
}
