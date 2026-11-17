package proxy

import "unicode/utf8"

const unwrapBlankSpellingResponseVersion = "@unwrap-blank-spelling-v1:"

// unwrapBlankSpellingResponseKeyVersion fences the answers cached before an unwrap written with other blanks than one
// space on each side of `| unwrap` (a tab, a line feed, a carriage return, a run of blanks, a comment) was served from
// the query plan with the spaced spelling's text (semantics/translator-reads-only-space-between-tokens): the string
// translator, which looks for `| unwrap ` alone, failed such a query (Loki's 500, never cached) or read no unwrap at
// all and answered another query (`rate({app="a"} |<TAB>unwrap n [5m])` counted the lines). A text screen of the words
// outside quoted strings and comments: a query whose every unwrap is written `| unwrap ` keeps its key, and one that
// spells an unwrap otherwise costs one more cache miss after the upgrade. Every query and query_range request builds
// it; a query without the letters of unwrap in any case is turned away by containsFold.
func unwrapBlankSpellingResponseKeyVersion(query string) string {
	if unwrapSpelledOtherwise(query) {
		return unwrapBlankSpellingResponseVersion
	}
	return ""
}

// unwrapSpelledOtherwise reports a word unwrap, in any case, outside quoted strings and comments, that is not written
// with `| ` right before it and a space right after it.
func unwrapSpelledOtherwise(query string) bool {
	if !containsFold(query, "unwrap") {
		return false
	}
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '"', '`':
			i = skipQuoted(query, i)
		case '#':
			i = skipComment(query, i) - 1
		case 'u', 'U':
			end, ok := matchFoldAt(query[i:], "unwrap")
			if !ok || i > 0 && fenceNameByteBefore(query, i) || nameContinues(query, i+end) {
				continue
			}
			if i < 2 || query[i-2] != '|' || query[i-1] != ' ' || i+end >= len(query) || query[i+end] != ' ' {
				return true
			}
			i += end - 1
		}
	}
	return false
}

// nameContinues reports a name character (a letter, a digit, an underscore or a dot) at query[i].
func nameContinues(query string, i int) bool {
	if i >= len(query) {
		return false
	}
	r, _ := utf8.DecodeRuneInString(query[i:])
	return r == '.' || isIdentRune(r)
}
