package proxy

import (
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

const keywordLabelResponseVersion = "@keyword-label-names-v1:"

// keywordLabelResponseKeyVersion fences the replies cached before the LogQL parser read a label name as Loki does
// (internal/logql, semantics/loki-keyword-label-names), in every cache tier that keys on the response key (the compat
// edge, the inner query_range key with its history windows and settled steps, and the log window fingerprint):
//
//   - a label written Ip, IP, Label_replace or any other spelling that is not all lower case, with no parenthesis
//     after it, was read in lower case, so it filtered, dropped or grouped by the field ip or label_replace;
//   - a query that names a label after one of the 15 words of Loki's lexer (of, bool, offset, unwrap, json, regexp,
//     logfmt, unpack, pattern, label_format, line_format, decolorize, drop, keep, variants) was answered, and is
//     Loki's 400 now. The compat edge looks its cache up before the handler validates the query, so an old 200
//     would be served.
//
// A query is scanned once, outside quoted text, without allocating. The ip and label_replace spellings decide at the
// word; a keyword word that does not start a stage or follow a range (`| json`, `] offset`) is only a candidate,
// and the parser decides: the fence applies when the query is rejected.
func keywordLabelResponseKeyVersion(query string) string {
	candidate := false
	commentEnd := 0 // quote characters inside a comment open no quoted text; its words are read like any other
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case (c == '"' || c == '`') && i >= commentEnd:
			i = skipQuoted(query, i) + 1
		case c == '#' && i >= commentEnd:
			commentEnd = skipComment(query, i)
			i++
		case isQueryWordStart(query, i):
			j := queryWordEnd(query, i)
			w := query[i:j]
			switch {
			case (eqFoldWord(w, "ip") && w != "ip" || eqFoldWord(w, "label_replace") && w != "label_replace") && !followedByParen(query, j):
				return keywordLabelResponseVersion
			case isLexerKeywordWord(w) && !startsStageOrFollowsRange(query, i):
				candidate = true
			}
			i = j
		default:
			i++
		}
	}
	if candidate && logqlpkg.ValidateLogQL(query) != "" {
		return keywordLabelResponseVersion
	}
	return ""
}

// skipQuoted is the index of the byte that closes the quoted text opened at query[i] (the last byte when unclosed).
func skipQuoted(query string, i int) int {
	q := query[i]
	for j := i + 1; j < len(query); j++ {
		switch {
		case query[j] == '\\' && q == '"':
			j++
		case query[j] == q:
			return j
		}
	}
	return len(query) - 1
}

// followedByParen reports a ( after optional blanks from query[i].
func followedByParen(query string, i int) bool {
	for i < len(query) && (query[i] == ' ' || query[i] == '\t' || query[i] == '\n' || query[i] == '\r') {
		i++
	}
	return i < len(query) && query[i] == '('
}

// isLexerKeywordWord reports one of the 15 words, in any case.
func isLexerKeywordWord(w string) bool {
	if !isASCIIWord(w) {
		// The one non-ASCII letters that lower case to ASCII ones (U+212A to k, U+0130 to i) can stand in a word.
		for _, k := range lexerKeywordWords {
			if eqFoldWord(w, k) {
				return true
			}
		}
		return false
	}
	var words []string
	switch len(w) {
	case 2:
		words = []string{"of"}
	case 4:
		words = []string{"bool", "json", "drop", "keep"}
	case 6:
		words = []string{"offset", "unwrap", "regexp", "logfmt", "unpack"}
	case 7:
		words = []string{"pattern"}
	case 8:
		words = []string{"variants"}
	case 10:
		words = []string{"decolorize"}
	case 11:
		words = []string{"line_format"}
	case 12:
		words = []string{"label_format"}
	}
	for _, k := range words {
		if strings.EqualFold(w, k) {
			return true
		}
	}
	return false
}

var lexerKeywordWords = [...]string{"of", "bool", "json", "drop", "keep", "offset", "unwrap", "regexp", "logfmt", "unpack", "pattern", "variants", "decolorize", "line_format", "label_format"}

// startsStageOrFollowsRange reports a word right after a pipe (a stage) or a closing bracket (`[5m] offset 1h`): where
// these words are keywords and not label names.
func startsStageOrFollowsRange(query string, i int) bool {
	for i--; i >= 0; i-- {
		switch query[i] {
		case ' ', '\t', '\n', '\r':
		case '|', ']':
			return true
		default:
			return false
		}
	}
	return false
}
