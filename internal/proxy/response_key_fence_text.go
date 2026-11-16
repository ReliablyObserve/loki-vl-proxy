package proxy

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The response key fences decide from the query text, before any parse, whether a key carries a version constant.
// The LogQL parser reads more spellings than the lower case, single-space form clients usually send, and a screen
// that misses one serves an answer cached before a behaviour change. The helpers below read the spellings the
// parser reads, without allocating:
//
//   - a keyword or a function name in any case (`| KEEP`, `TOPK(`, `Sum by`), and a letter that lower cases to an
//     ASCII one (U+212A Kelvin sign reads as k, U+0130 as i): the scanner lower cases a word with strings.ToLower
//     before it looks the word up, and so do the matchers here (matchFoldAt);
//   - white space between tokens that is a space, tab, line feed or carriage return, and a `#` comment to the end
//     of its line (skipLexerBlanks); every other character Go calls white space is no blank to the lexer;
//   - a name made of any Unicode letters and digits (scanIdent), not only ASCII ones.

// isLexerBlank reports a byte the LogQL lexer skips between tokens.
func isLexerBlank(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// skipLexerBlanks returns the index of the first byte at or after i that is neither a blank the lexer skips nor part
// of a comment. A comment runs from # to the end of the line.
func skipLexerBlanks(s string, i int) int {
	for i < len(s) {
		switch c := s[i]; {
		case isLexerBlank(c) || c == '\f':
			// \f is no blank to the lexer; the older screens read it as one (\s), and so does this.
			i++
		case c == '#':
			end := strings.IndexByte(s[i:], '\n')
			if end < 0 {
				return len(s)
			}
			i += end + 1
		default:
			return i
		}
	}
	return i
}

// matchFoldAt reports whether s starts with lower (ASCII lower case) once its letters are lower cased the way
// strings.ToLower does, and the index just after the match.
func matchFoldAt(s, lower string) (end int, ok bool) {
	i := 0
	for j := 0; j < len(lower); j++ {
		if i >= len(s) {
			return 0, false
		}
		var r rune
		size := 1
		if c := s[i]; c < utf8.RuneSelf {
			r = rune(c)
			if c >= 'A' && c <= 'Z' {
				r += 'a' - 'A'
			}
		} else {
			r, size = utf8.DecodeRuneInString(s[i:])
			r = unicode.ToLower(r)
		}
		if r != rune(lower[j]) {
			return 0, false
		}
		i += size
	}
	return i, true
}

// foldAnchors are the first bytes a lower case ASCII letter can start with in a query: the letter in both cases,
// and for k and i the first byte of the one non-ASCII letter that lower cases to it.
func foldAnchors(c byte) (up byte, wide byte) {
	switch c {
	case 'k':
		return c &^ 0x20, 0xE2 // U+212A
	case 'i':
		return c &^ 0x20, 0xC4 // U+0130
	}
	return c &^ 0x20, 0
}

// indexFoldFrom is the index of the first match of lower in s at or after from, -1 when there is none. lower is ASCII
// lower case and starts with a letter. The scan jumps from one candidate to the next with strings.IndexByte, which
// is as fast on a clean query as strings.Contains.
func indexFoldFrom(s string, from int, lower string) int {
	up, wide := foldAnchors(lower[0])
	type candidate struct {
		pos int
		c   byte
	}
	cands := [3]candidate{{-1, lower[0]}, {-1, up}, {-1, wide}}
	for i := range cands {
		if p := &cands[i]; p.c != 0 {
			p.pos = indexByteFrom(s, from, p.c)
		}
	}
	for {
		var best *candidate
		for i := range cands {
			if p := &cands[i]; p.pos >= 0 && (best == nil || p.pos < best.pos) {
				best = p
			}
		}
		if best == nil {
			return -1
		}
		at := best.pos
		if _, ok := matchFoldAt(s[at:], lower); ok {
			return at
		}
		best.pos = indexByteFrom(s, at+1, best.c)
	}
}

func indexByteFrom(s string, from int, c byte) int {
	if from >= len(s) {
		return -1
	}
	if i := strings.IndexByte(s[from:], c); i >= 0 {
		return from + i
	}
	return -1
}

// containsFold reports whether s holds lower (ASCII lower case) in any spelling the lexer reads as it.
func containsFold(s, lower string) bool { return indexFoldFrom(s, 0, lower) >= 0 }

// isIdentRune reports a rune of a name: a letter, a digit or an underscore, as the lexer reads one.
func isIdentRune(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) }

// identEnd is the index just after the name that starts at s[i] (a letter, digit or underscore): the lexer's name,
// with the dot it keeps inside one.
func identEnd(s string, i int) int {
	for i < len(s) {
		if c := s[i]; c < utf8.RuneSelf {
			if c == '_' || c == '.' || c >= '0' && c <= '9' || c|0x20 >= 'a' && c|0x20 <= 'z' {
				i++
				continue
			}
			return i
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if !isIdentRune(r) {
			return i
		}
		i += size
	}
	return i
}

// pipeWordFold reports a `|` followed by blanks or comments and then lower (in any spelling), where after is the
// byte class that must follow it: followBlank asks for a blank or a comment after the word (a stage with arguments),
// otherwise anything may follow. It scans every `|` of the query, in quotes too: a quoted text that looks like a stage fences
// the key, as it always did.
func pipeWordFold(query, lower string, followBlank bool) bool {
	for i := 0; ; i++ {
		p := indexByteFrom(query, i, '|')
		if p < 0 {
			return false
		}
		i = p
		j := skipLexerBlanks(query, p+1)
		end, ok := matchFoldAt(query[j:], lower)
		if !ok {
			continue
		}
		end += j
		if !followBlank {
			return true
		}
		// A blank after the word: the lexer's, a comment, or any other character Go calls white space (a keep
		// list reads that one as a name no stream has).
		if end < len(query) {
			if r, _ := utf8.DecodeRuneInString(query[end:]); r == '#' || unicode.IsSpace(r) {
				return true
			}
		}
	}
}

// keepStageIn reports a keep stage with arguments: a `|`, then keep, then a blank or a comment.
func keepStageIn(query string) bool { return pipeWordFold(query, "keep", true) }

// isReBlank is the white space the screens read before an ip( parenthesis and after a comparison: [ \t\n\f\r].
func isReBlank(c byte) bool { return isLexerBlank(c) || c == '\f' }

// ipFilterIn reports an ip( function: `label = ip(`, `label != ip(`, `|= ip(` or `!= ip(`, with ip in any case and
// blanks or comments between the operator and the word and blanks between the word and its parenthesis. A query
// without the word ip in any case is turned away before the scan.
func ipFilterIn(query string, other bool) bool {
	if !containsWord(query, "ip", other) {
		return false
	}
	for i := 0; ; i++ {
		p := indexByteFrom(query, i, '=')
		if p < 0 {
			return false
		}
		i = p
		j := skipLexerBlanks(query, p+1)
		end, ok := matchFoldAt(query[j:], "ip")
		if !ok {
			continue
		}
		j += end
		for j < len(query) && isReBlank(query[j]) {
			j++
		}
		if j < len(query) && query[j] == '(' && ipOperatorEndingAt(query, p) {
			return true
		}
	}
}

// ipOperatorEndingAt reports that the `=` at query[p] ends the operator of an ip filter: `|=` or `!=` of a line
// filter or a label filter, or `=` after a label name and blanks or a comment.
func ipOperatorEndingAt(query string, p int) bool {
	if p > 0 && (query[p-1] == '|' || query[p-1] == '!') {
		return true
	}
	for p > 0 && isReBlank(query[p-1]) {
		p--
		if query[p] == '\n' {
			// A line break may end a comment between the name and the operator: read it as a name before.
			return true
		}
	}
	if p == 0 {
		return false
	}
	r, _ := utf8.DecodeLastRuneInString(query[:p])
	return r == '.' || isIdentRune(r)
}

// repeatedLabelFilterStages reports two or more `| label op value` stages, a superset of the label filter stages:
// a quoted text that looks like one counts too. A stage opens with a `|`, blanks or comments, then a name and a
// comparison operator, or a parenthesis (a grouped filter).
func repeatedLabelFilterStages(query string) bool {
	stages := 0
	for i := 0; ; i++ {
		p := indexByteFrom(query, i, '|')
		if p < 0 {
			return false
		}
		i = p
		if labelFilterStageAt(query, p+1) {
			if stages++; stages == 2 {
				return true
			}
		}
	}
}

func labelFilterStageAt(query string, i int) bool {
	j := skipLexerBlanks(query, i)
	if j >= len(query) {
		return false
	}
	if query[j] == '(' {
		return true
	}
	if r, _ := utf8.DecodeRuneInString(query[j:]); r != '_' && !unicode.IsLetter(r) {
		return false
	}
	k := skipLexerBlanks(query, identEnd(query, j))
	if k >= len(query) {
		return false
	}
	switch query[k] {
	case '=', '<', '>':
		return true
	case '!':
		return k+1 < len(query) && (query[k+1] == '=' || query[k+1] == '~')
	}
	return false
}

// emptyStringComparisonIn reports `!=` followed by blanks or comments and an empty string literal, in either quote.
func emptyStringComparisonIn(query string) bool {
	for i := 0; ; i++ {
		p := strings.Index(query[min(i, len(query)):], "!=")
		if p < 0 {
			return false
		}
		i += p
		j := skipLexerBlanks(query, i+2)
		if j+1 < len(query) && (query[j] == '"' && query[j+1] == '"' || query[j] == '`' && query[j+1] == '`') {
			return true
		}
	}
}

// nestedVectorKeywordsFold is translator.HasNestedVectorKeywords with every word in any case: two or more of sum,
// avg, max, min and count, each at the start of a name and followed by blanks and then a parenthesis, by or
// without. It may say yes to a query that is no nest, never no to one.
func nestedVectorKeywordsFold(query string) bool {
	found := 0
	for i := 0; i < len(query); {
		c := query[i]
		if c >= utf8.RuneSelf {
			_, size := utf8.DecodeRuneInString(query[i:])
			i += size
			continue
		}
		if c|0x20 != 's' && c|0x20 != 'a' && c|0x20 != 'm' && c|0x20 != 'c' || i > 0 && fenceNameByteBefore(query, i) {
			i++
			continue
		}
		for _, kw := range [...]string{"sum", "avg", "max", "min", "count"} {
			end, ok := matchFoldAt(query[i:], kw)
			if !ok {
				continue
			}
			j := i + end
			for j < len(query) && isLexerBlank(query[j]) {
				j++
			}
			if j < len(query) && (query[j] == '(' || hasFoldPrefix(query[j:], "by") || hasFoldPrefix(query[j:], "without")) {
				if found++; found == 2 {
					return true
				}
			}
			break
		}
		i++
	}
	return false
}

func hasFoldPrefix(s, lower string) bool {
	_, ok := matchFoldAt(s, lower)
	return ok
}

// fenceNameByteBefore reports that the rune before query[i] continues a name.
func fenceNameByteBefore(query string, i int) bool {
	r, _ := utf8.DecodeLastRuneInString(query[:i])
	return r == '.' || isIdentRune(r)
}

// skipComment returns the index after the comment that starts at query[i] (a `#` outside quoted text): its line.
func skipComment(query string, i int) int {
	end := strings.IndexByte(query[i:], '\n')
	if end < 0 {
		return len(query)
	}
	return i + end + 1
}

// isQueryWordStart reports a name that starts at query[i]: a letter or an underscore, ASCII or not.
func isQueryWordStart(query string, i int) bool {
	c := query[i]
	if c < utf8.RuneSelf {
		return c == '_' || c|0x20 >= 'a' && c|0x20 <= 'z'
	}
	r, _ := utf8.DecodeRuneInString(query[i:])
	return unicode.IsLetter(r)
}

// queryWordEnd is the index after the run of letters, digits and underscores that starts at query[i], without the
// dot a name may hold (the screens read each side of a dotted name as a word), and whether the run is all ASCII.
func queryWordEnd(query string, i int) (end int, ascii bool) {
	ascii = true
	for i < len(query) {
		if c := query[i]; c < utf8.RuneSelf {
			if c == '_' || c >= '0' && c <= '9' || c|0x20 >= 'a' && c|0x20 <= 'z' {
				i++
				continue
			}
			return i, ascii
		}
		r, size := utf8.DecodeRuneInString(query[i:])
		if !isIdentRune(r) {
			return i, ascii
		}
		ascii = false
		i += size
	}
	return i, ascii
}

func isASCIIWord(w string) bool {
	for i := 0; i < len(w); i++ {
		if w[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// eqFoldWord reports that w lower cases to lower (ASCII lower case) the way the lexer lower cases a word.
func eqFoldWord(w, lower string) bool {
	end, ok := matchFoldAt(w, lower)
	return ok && end == len(w)
}

// afterNumberUnit is the name the word query[i:j] holds. A word that follows digits directly is the unit of a number
// or a duration (`0y`, `5m`), and a name starts at its first underscore: the lexer reads `0y_time` as `0y` and
// `_time`.
func afterNumberUnit(query string, i, j int) string {
	w := query[i:j]
	if i > 0 && query[i-1] >= '0' && query[i-1] <= '9' {
		if k := strings.IndexByte(w, '_'); k > 0 {
			return w[k:]
		}
	}
	return w
}

// trailingASCIIName is the ASCII name at the end of w, after its last non-ASCII letter: the word an ASCII-only
// reading of the query ends with (the older screens split a name at each non-ASCII byte).
func trailingASCIIName(w string) string {
	k := len(w)
	for k > 0 && w[k-1] < utf8.RuneSelf {
		k--
	}
	for k < len(w) && w[k] >= '0' && w[k] <= '9' {
		k++
	}
	return w[k:]
}

// wordFences is what one scan of a query's words finds, for the three fences that read them.
type wordFences struct {
	reservedCase     bool // a name _time, _stream or _stream_id in another case, followed by a comparison
	reservedLabel    bool // _time, _stream, _stream_id in any case or _msg, followed by a comparison
	keywordFire      bool // a label spelled Ip, IP or Label_replace, no parenthesis after it
	keywordCandidate bool // a keyword word where no keyword stands: the parser decides
}

// scanFenceWords reads the words of query once, outside quoted text, for the reserved field case and label fences
// and the keyword label fence, without allocating. A comment runs to the end of its line: its quote characters open
// no quoted text, and its words are read like any other.
func scanFenceWords(query string) (w wordFences) {
	commentEnd := 0
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case (c == '"' || c == '`') && i >= commentEnd:
			i = skipQuoted(query, i) + 1
		case c == '#' && i >= commentEnd:
			commentEnd = skipComment(query, i)
			i++
		case isQueryWordStart(query, i):
			j, ascii := queryWordEnd(query, i)
			if !w.keywordFire {
				w.readKeywordWord(query, i, j, ascii)
			}
			if (!w.reservedCase || !w.reservedLabel) && strings.IndexByte(query[i:j], '_') >= 0 && comparisonFollows(query, j) {
				w.readReservedName(afterNumberUnit(query, i, j))
			}
			i = j
		default:
			i++
		}
	}
	return w
}

// readKeywordWord reads the word query[i:j] for the keyword label fence.
func (w *wordFences) readKeywordWord(query string, i, j int, ascii bool) {
	fire, cand := keywordWordCheck(query, i, j, ascii)
	if !ascii {
		// A name with a non-ASCII letter is also read as the older screen read it: each ASCII run of it a word.
		for k := i; k < j; {
			if c := query[k]; c < utf8.RuneSelf && (c == '_' || c|0x20 >= 'a' && c|0x20 <= 'z') {
				m := k + 1
				for m < j && query[m] < utf8.RuneSelf {
					m++
				}
				f, cd := keywordWordCheck(query, k, m, true)
				fire, cand = fire || f, cand || cd
				k = m
				continue
			}
			k++
		}
	}
	w.keywordFire = fire
	w.keywordCandidate = w.keywordCandidate || cand
}

// readReservedName reads a name a comparison follows for the two reserved field fences.
func (w *wordFences) readReservedName(name string) {
	for k := 0; k < 2; k++ {
		if !w.reservedCase && logsql.ReservedFieldNameOtherCase(name) {
			w.reservedCase = true
		}
		if !w.reservedLabel && reservedLabelWord(name) {
			w.reservedLabel = true
		}
		// The older screens split a name at each non-ASCII byte: read its ASCII end too.
		if isASCIIWord(name) {
			return
		}
		name = trailingASCIIName(name)
	}
}

// spelledOtherwise reports a query that holds an ASCII upper case letter or a non-ASCII byte: the only queries whose
// words can be spelled other than in lower case ASCII. A query without either is screened with strings.Contains, as
// it always was; the others are read case-folded (containsFold).
func spelledOtherwise(query string) bool {
	for i := 0; i < len(query); i++ {
		if c := query[i]; c >= utf8.RuneSelf || c-'A' <= 'Z'-'A' {
			return true
		}
	}
	return false
}

// containsWord is containsFold for a query that spelledOtherwise reported on, strings.Contains for the rest.
func containsWord(query, lower string, other bool) bool {
	if !other {
		return strings.Contains(query, lower)
	}
	return containsFold(query, lower)
}
