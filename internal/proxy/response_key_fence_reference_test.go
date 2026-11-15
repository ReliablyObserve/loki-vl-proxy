package proxy

import (
	"regexp"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The functions below are the fence screens as they were before they read every spelling the parser reads
// (lower case keywords, one space or no blank between tokens, ASCII names, no comments). They are frozen here: a
// query these screens fired for must still fire, so no response key a binary of the older line wrote becomes
// unreachable. They are pure text screens; the new ones differ only by firing on more spellings.

var (
	refKeepStageRE          = regexp.MustCompile(`\|\s*keep\s+`)
	refIPStageRE            = regexp.MustCompile(`(?:[A-Za-z0-9_]\s*!?=|[|!]=)\s*ip\s*\(`)
	refLabelFilterStageRE   = regexp.MustCompile(`\|\s*[A-Za-z_][A-Za-z0-9_.]*\s*(?:=~|!~|!=|==|>=|<=|=|>|<)`)
	refLogfmtPresenceFilter = regexp.MustCompile(`!=\s*""`)
)

func refKeepMetadata(query string) bool { return refKeepStageRE.MatchString(query) }

func refKeepUnwrap(query string) bool {
	return strings.Contains(query, "unwrap") && refKeepStageRE.MatchString(query)
}

func refIP(query string) bool {
	return strings.Contains(query, "ip") && refIPStageRE.MatchString(query)
}

func refRepeatedLabelFilter(query string) bool {
	return len(refLabelFilterStageRE.FindAllStringIndex(query, 2)) == 2
}

func refOrderedJSONCandidate(query string) bool {
	return strings.Contains(query, "json") && (translator.HasNestedVectorKeywords(query) || strings.Contains(query, "topk") || strings.Contains(query, "bottomk"))
}

func refLogfmtPresenceText(query string) bool {
	return strings.Contains(query, "logfmt") && refLogfmtPresenceFilter.MatchString(query)
}

func refBareJSONText(query string) bool { return strings.Contains(query, "json") }

func refReservedCase(query string) bool {
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '"' || c == '`':
			i = closingQuote(query, i) + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i + 1
			for j < len(query) && (query[j] == '_' || query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z' || query[j] >= '0' && query[j] <= '9') {
				j++
			}
			if logsql.ReservedFieldNameOtherCase(query[i:j]) && refComparisonFollows(query, j) {
				return true
			}
			i = j
		default:
			i++
		}
	}
	return false
}

func refReservedLabel(query string) bool {
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '"' || c == '`':
			i = skipQuoted(query, i) + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i + 1
			for j < len(query) && (query[j] == '_' || query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z' || query[j] >= '0' && query[j] <= '9') {
				j++
			}
			w := query[i:j]
			if (strings.EqualFold(w, "_time") || strings.EqualFold(w, "_stream") || strings.EqualFold(w, "_stream_id") || w == "_msg") && refComparisonFollows(query, j) {
				return true
			}
			i = j
		default:
			i++
		}
	}
	return false
}

func refComparisonFollows(query string, i int) bool {
	for i < len(query) && (query[i] == ' ' || query[i] == '\t' || query[i] == '\n' || query[i] == '\r') {
		i++
	}
	return i < len(query) && (query[i] == '=' || query[i] == '!' || query[i] == '>' || query[i] == '<')
}

// refKeywordLabel is the keyword label screen without the comment, non-ASCII word and non-ASCII keyword readings.
func refKeywordLabel(query string) bool {
	candidate := false
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '"' || c == '`':
			i = skipQuoted(query, i) + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i + 1
			for j < len(query) && (query[j] == '_' || query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z' || query[j] >= '0' && query[j] <= '9') {
				j++
			}
			w := query[i:j]
			switch {
			case (strings.EqualFold(w, "ip") && w != "ip" || strings.EqualFold(w, "label_replace") && w != "label_replace") && !followedByParen(query, j):
				return true
			case isLexerKeywordWord(w) && !startsStageOrFollowsRange(query, i):
				candidate = true
			}
			i = j
		default:
			i++
		}
	}
	return candidate && logqlpkg.ValidateLogQL(query) != ""
}

// refStreamLevelSeries is the stream level series screen as it was before it read the Kelvin sign (U+212A) as the k
// of topk and bottomk.
func refStreamLevelSeries(query string) bool {
	if strings.IndexByte(query, '(') < 0 {
		return false
	}
	for _, c := range [...]byte{'k', 'K'} {
		for s, i := query, strings.IndexByte(query, c); i >= 0; i = strings.IndexByte(s, c) {
			if i >= 3 && strings.EqualFold(s[i-3:i+1], "topk") || i >= 6 && strings.EqualFold(s[i-6:i+1], "bottomk") {
				return true
			}
			s = s[i+1:]
		}
	}
	for _, c := range [...]byte{'w', 'W'} {
		for s, i := query, strings.IndexByte(query, c); i >= 0; i = strings.IndexByte(s, c) {
			if i+7 <= len(s) && strings.EqualFold(s[i:i+7], "without") || i >= 2 && i+4 <= len(s) && strings.EqualFold(s[i-2:i+4], "unwrap") {
				return true
			}
			s = s[i+1:]
		}
	}
	for s, i := query, strings.IndexByte(query, '('); i >= 0; i = strings.IndexByte(s, '(') {
		name := trimRightBlanks(s[:i])
		if hasSuffixFold(name, "sort") || hasSuffixFold(name, "sort_desc") || hasSuffixFold(name, "label_replace") {
			return true
		}
		s = s[i+1:]
	}
	return false
}

// refDynamicIdentityScreen is the text screen in front of the dynamic unwrap identity plan as it was before it read
// the words in any spelling (the query is the one dynamicQuery wrote: keywords with an ASCII upper case letter are
// lower cased, a letter that lower cases to an ASCII one is kept).
func refDynamicIdentityScreen(query string) bool {
	if !strings.Contains(query, "unwrap") || !strings.Contains(query, "json") && !strings.Contains(query, "logfmt") {
		return false
	}
	for _, fn := range dynamicIdentityFuncs {
		if strings.Contains(query, fn) {
			return true
		}
	}
	return false
}
