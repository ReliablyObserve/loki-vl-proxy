package logql

import (
	"unicode"
	"unicode/utf8"
)

// MayHaveNonFilterStage is a screen on query text, with no allocation: false means the query certainly has no
// parser or formatting stage (`| json`, `| logfmt`, `| regexp`, `| pattern`, `| unpack`, `| line_format`,
// `| label_format`, `| decolorize`, `| drop`, `| keep`, `| unwrap`, `| distinct`), so its pipeline is line filters
// and label filters only; true means it may have one (a stage keyword in a comment or a string the screen could
// not tell apart is also true). The proxy asks it before it plans a log query to serve from the lowering, which
// covers nothing but filters, so a query with a parser pays one scan of its text and no plan.
//
// A label named like a stage keyword and compared (`| json="x"`) is a label filter, not a stage.
//
// It also answers true for a label filter on detected_level or service_name after the stream selector (Grafana's
// level and service filters): the proxy derives both labels itself, so the lowering leaves those queries to the
// string translator, and a query with one is not planned either.
func MayHaveNonFilterStage(query string) bool {
	afterSelector := false
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '}':
			afterSelector = true
		case '"':
			// A quoted string ends at its first quote not escaped by a backslash.
			for i++; i < len(query) && query[i] != '"'; i++ {
				if query[i] == '\\' {
					i++
				}
			}
		case '`':
			for i++; i < len(query) && query[i] != '`'; i++ {
			}
		case '|':
			if barOpensStage(query, i+1) {
				return true
			}
		case '#':
			// A comment runs to the end of its line, as the scanner reads it: a quote in it opens no string.
			for i < len(query) && query[i] != '\n' {
				i++
			}
		default:
			if !isStageWordByte(c) {
				continue
			}
			// A word: read it whole, so a name that merely ends like a derived label is not one.
			j := i
			for j < len(query) && isStageWordByte(query[j]) {
				j++
			}
			if afterSelector && (query[i:j] == "detected_level" || query[i:j] == "service_name") {
				return true
			}
			i = j - 1
		}
	}
	return false
}

// barOpensStage reports whether the text after a `|` (from j) names a parser or formatting stage.
func barOpensStage(query string, j int) bool {
	for j < len(query) && isStageSpace(query[j]) {
		j++
	}
	if j < len(query) && (query[j] >= utf8.RuneSelf || query[j] == '#' || query[j] == '\v' || query[j] == '\f') {
		j = skipStageSpace(query, j)
	}
	k := j
	for k < len(query) && isStageWordByte(query[k]) {
		k++
	}
	if k == j || !isNonFilterStageWord(query[j:k]) {
		return false
	}
	m := k
	for m < len(query) && isStageSpace(query[m]) {
		m++
	}
	// `| drop = "x"` and the like compare a label that has a stage's name.
	return m >= len(query) || query[m] != '=' && query[m] != '!' && query[m] != '<' && query[m] != '>'
}

func isStageSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// skipStageSpace skips what the scanner skips between `|` and a stage keyword: any Unicode white space (a vertical
// tab or a no-break space too) and comments.
func skipStageSpace(query string, j int) int {
	for j < len(query) {
		switch c := query[j]; {
		case isStageSpace(c) || c == '\v' || c == '\f':
			j++
			continue
		case c == '#':
			for j < len(query) && query[j] != '\n' {
				j++
			}
			continue
		case c < utf8.RuneSelf:
			return j
		}
		r, size := utf8.DecodeRuneInString(query[j:])
		if !unicode.IsSpace(r) {
			return j
		}
		j += size
	}
	return j
}

func isStageWordByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// isNonFilterStageWord reports a stage keyword in any letter case: the scanner folds a word Loki reads as a
// keyword to lower case, so the parser reads `| JSON` as `| json`.
func isNonFilterStageWord(w string) bool {
	var lower [len("label_format")]byte
	if len(w) > len(lower) {
		return false
	}
	for i := 0; i < len(w); i++ {
		c := w[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		lower[i] = c
	}
	switch string(lower[:len(w)]) {
	case "json", "logfmt", "regexp", "pattern", "unpack", "line_format", "label_format", "decolorize", "drop", "keep", "unwrap", "distinct":
		return true
	}
	return false
}
