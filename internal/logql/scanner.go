package logql

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokType identifies a lexical token kind.
type TokType int

const (
	TokEOF TokType = iota
	TokError

	// Punctuation
	TokLBrace   // {
	TokRBrace   // }
	TokLParen   // (
	TokRParen   // )
	TokLBracket // [
	TokRBracket // ]
	TokComma    // ,
	TokPipe     // |
	TokAt       // @

	// Label match operators (inside {})
	TokEq         // =
	TokNeq        // !=
	TokReMatch    // =~
	TokReNotMatch // !~

	// Line filter operators (pipeline context)
	TokPipeEq    // |=
	TokBangEq    // != (line filter)
	TokPipeTilde // |~
	TokBangTilde // !~  (line filter)
	TokPipeGt    // |>
	TokBangGt    // !>

	// Values
	TokIdent     // identifier
	TokString    // "…" — unquoted value stored in Val
	TokRawString // `…` — raw string (regexp param)
	TokDuration  // 5m, 30s, …
	TokNumber    // numeric literal

	// Arithmetic / comparison binary operators
	TokPlus    // +
	TokMinus   // -
	TokStar    // *
	TokSlash   // /
	TokPercent // %
	TokCaret   // ^
	TokLt      // <
	TokGt      // >
	TokLtEq    // <=
	TokGtEq    // >=
	TokEqEq    // ==

	// Structural keywords
	TokBy         // by
	TokWithout    // without
	TokOn         // on
	TokIgnoring   // ignoring
	TokGroupLeft  // group_left
	TokGroupRight // group_right
	TokAnd        // and
	TokOr         // or
	TokUnless     // unless

	// Literals outside a [range], read the way Loki's lexer reads them.
	TokDurationLit // a duration (offset 5m): Loki's DURATION
	TokBytes       // a bytes size (5KB): Loki's BYTES
)

//nolint:gocyclo // one case per token type; the switch is a direct mapping with no branching logic.
func (t TokType) String() string {
	switch t {
	case TokEOF:
		return "EOF"
	case TokError:
		return "ERROR"
	case TokLBrace:
		return "{"
	case TokRBrace:
		return "}"
	case TokLParen:
		return "("
	case TokRParen:
		return ")"
	case TokLBracket:
		return "["
	case TokRBracket:
		return "]"
	case TokComma:
		return ","
	case TokPipe:
		return "|"
	case TokAt:
		return "@"
	case TokEq:
		return "="
	case TokNeq:
		return "!="
	case TokReMatch:
		return "=~"
	case TokReNotMatch:
		return "!~"
	case TokPipeEq:
		return "|="
	case TokBangEq:
		return "!="
	case TokPipeTilde:
		return "|~"
	case TokBangTilde:
		return "!~"
	case TokPipeGt:
		return "|>"
	case TokBangGt:
		return "!>"
	case TokIdent:
		return "IDENT"
	case TokString:
		return "STRING"
	case TokRawString:
		return "RAWSTRING"
	case TokDuration:
		return "DURATION"
	case TokNumber:
		return "NUMBER"
	case TokPlus:
		return "+"
	case TokMinus:
		return "-"
	case TokStar:
		return "*"
	case TokSlash:
		return "/"
	case TokPercent:
		return "%"
	case TokCaret:
		return "^"
	case TokLt:
		return "<"
	case TokGt:
		return ">"
	case TokLtEq:
		return "<="
	case TokGtEq:
		return ">="
	case TokEqEq:
		return "=="
	case TokBy:
		return "by"
	case TokWithout:
		return "without"
	case TokOn:
		return "on"
	case TokIgnoring:
		return "ignoring"
	case TokGroupLeft:
		return "group_left"
	case TokGroupRight:
		return "group_right"
	case TokAnd:
		return "and"
	case TokOr:
		return "or"
	case TokUnless:
		return "unless"
	case TokDurationLit:
		return "DURATION"
	case TokBytes:
		return "BYTES"
	}
	return "?"
}

// Token is a scanned token with its type and string value.
type Token struct {
	Typ TokType
	Val string
}

type scanner struct {
	src        string
	pos        int
	braceDepth int32 // tracks {} nesting for != vs |= disambiguation
	start      int32 // byte offset where the token last returned by next begins
	// extra holds what few scanners need: the comment spans (StripComments) and the errors of Loki's lexer.
	extra *scanExtra
	// prevStart is where the token before the last one read starts.
	prevStart int32
	// inRange is set between a '[' and its ']': a range holds a duration that is not read as a literal.
	inRange bool
	// afterComment is set when a comment stands right before the token.
	afterComment bool
	// quiet stops the scanner from noting lexer errors: a lookahead copy or a re-read of a text already read.
	quiet bool
	// hasNUL is set when the text holds a NUL byte, which Loki's lexer rejects: the position bookkeeping for
	// it (prevStart, afterComment) is only kept then.
	hasNUL bool
}

// scanExtra is the rarely used state of a scanner.
type scanExtra struct {
	// comments collects the spans of the comments skipped, when not nil (StripComments).
	comments *[][2]int
	// err is the first error of Loki's lexer found so far (a malformed number literal, NUL, an unclosed
	// range); its message is empty while there is none.
	err lexError
}

// lexError is an error of Loki's lexer: it is reported ahead of the parse error it may cause.
type lexError struct {
	msg      string
	off      int32
	zeroLine bool
}

func (s *scanner) addLexErr(off int, msg string, zeroLine bool) {
	if s.quiet {
		return
	}
	if s.extra == nil {
		s.extra = &scanExtra{}
	}
	if s.extra.err.msg == "" {
		s.extra.err = lexError{msg, int32(off), zeroLine}
	}
}

// lexErrCount is 1 once the scanner has noted an error of Loki's lexer, 0 before.
func (s *scanner) lexErrCount() int {
	if s.extra == nil || s.extra.err.msg == "" {
		return 0
	}
	return 1
}

// dropLexErr forgets the error noted after known errors were there already.
func (s *scanner) dropLexErr(known int) {
	if known == 0 && s.extra != nil {
		s.extra.err = lexError{}
	}
}

func newScanner(src string) *scanner {
	return &scanner{src: src, hasNUL: strings.IndexByte(src, 0) >= 0}
}

func (s *scanner) peek() (rune, int) {
	if s.pos >= len(s.src) {
		return 0, 0
	}
	if c := s.src[s.pos]; c < utf8.RuneSelf {
		return rune(c), 1
	}
	r, size := utf8.DecodeRuneInString(s.src[s.pos:])
	return r, size
}

func (s *scanner) read() rune {
	r, size := s.peek()
	s.pos += size
	return r
}

// skipWS skips white space and comments. It reports whether white space stands right before the token: Loki's
// lexer takes the line break that ends a comment with the comment.
func (s *scanner) skipWS() (spaced bool) {
	if s.hasNUL {
		s.afterComment = false
	}
	for {
		r, size := s.peek()
		if size == 0 {
			return spaced
		}
		if r == '#' {
			// A comment runs to the end of its line, as in Loki's lexer. A string never
			// reaches here: the string scanners read their quotes to the end.
			start := s.pos
			for s.pos < len(s.src) && s.src[s.pos] != '\n' {
				s.pos++
			}
			if s.extra != nil && s.extra.comments != nil {
				*s.extra.comments = append(*s.extra.comments, [2]int{start, s.pos})
			}
			if s.pos < len(s.src) {
				s.pos++
			}
			if s.hasNUL {
				// A comment is a token for Loki's lexer.
				s.prevStart, s.afterComment = int32(start), true
			}
			spaced = false
			continue
		}
		if !unicode.IsSpace(r) {
			return spaced
		}
		s.pos += size
		spaced = true
	}
}

// next reads the next token and notes the errors Loki's lexer would report on the way.
func (s *scanner) next() Token {
	spaced := s.skipWS()
	if !s.hasNUL {
		return s.scanToken(spaced)
	}
	start := s.pos
	if start < len(s.src) && s.src[start] == 0 && (spaced || s.afterComment) {
		// Loki meets a NUL after white space while it skips the white space, with no valid position: it reports
		// the token before, on line 0.
		s.addLexErr(int(s.prevStart), "invalid character NUL", true)
	}
	known := s.lexErrCount()
	tok := s.scanToken(spaced)
	if s.pos < len(s.src) && s.src[s.pos] == 0 && s.lexErrCount() == known {
		// The character after a token is read before the token is handed over.
		s.addLexErr(start, "invalid character NUL", false)
	}
	s.prevStart = int32(start)
	return tok
}

//nolint:gocyclo // lexer dispatches on every character class; branching is inherent to a hand-written lexer.
func (s *scanner) scanToken(spaced bool) Token {
	s.start = int32(s.pos)
	if s.pos >= len(s.src) {
		return Token{Typ: TokEOF}
	}

	r, _ := s.peek()

	switch r {
	case '{':
		s.read()
		s.braceDepth++
		return Token{Typ: TokLBrace, Val: "{"}
	case '}':
		s.read()
		if s.braceDepth > 0 {
			s.braceDepth--
		}
		return Token{Typ: TokRBrace, Val: "}"}
	case '(':
		s.read()
		return Token{Typ: TokLParen, Val: "("}
	case ')':
		s.read()
		return Token{Typ: TokRParen, Val: ")"}
	case '[':
		s.read()
		s.inRange = true
		if strings.IndexByte(s.src[s.pos:], ']') < 0 {
			s.addLexErr(int(s.start), "missing closing ']' in duration", true)
		}
		return Token{Typ: TokLBracket, Val: "["}
	case ']':
		s.read()
		s.inRange = false
		return Token{Typ: TokRBracket, Val: "]"}
	case ',':
		s.read()
		return Token{Typ: TokComma, Val: ","}
	case '@':
		s.read()
		return Token{Typ: TokAt, Val: "@"}

	case '|':
		s.read()
		r2, _ := s.peek()
		switch r2 {
		case '=':
			s.read()
			return Token{Typ: TokPipeEq, Val: "|="}
		case '~':
			s.read()
			return Token{Typ: TokPipeTilde, Val: "|~"}
		case '>':
			s.read()
			return Token{Typ: TokPipeGt, Val: "|>"}
		}
		return Token{Typ: TokPipe, Val: "|"}

	case '!':
		s.read()
		r2, _ := s.peek()
		switch r2 {
		case '=':
			s.read()
			if s.braceDepth > 0 {
				return Token{Typ: TokNeq, Val: "!="}
			}
			return Token{Typ: TokBangEq, Val: "!="}
		case '~':
			s.read()
			if s.braceDepth > 0 {
				return Token{Typ: TokReNotMatch, Val: "!~"}
			}
			return Token{Typ: TokBangTilde, Val: "!~"}
		case '>':
			s.read()
			return Token{Typ: TokBangGt, Val: "!>"}
		}
		return Token{Typ: TokError, Val: "!"}

	case '+':
		s.read()
		return Token{Typ: TokPlus, Val: "+"}
	case '-':
		if !s.inRange {
			// Loki's lexer reads a minus sign right before a duration Go reads as one DURATION token (-5m).
			if end := scanNegativeDuration(s.src, s.pos); end > 0 {
				val := s.src[s.pos:end]
				s.pos = end
				return Token{Typ: TokDurationLit, Val: val}
			}
		}
		s.read()
		return Token{Typ: TokMinus, Val: "-"}
	case '*':
		s.read()
		return Token{Typ: TokStar, Val: "*"}
	case '/':
		s.read()
		return Token{Typ: TokSlash, Val: "/"}
	case '%':
		s.read()
		return Token{Typ: TokPercent, Val: "%"}
	case '^':
		s.read()
		return Token{Typ: TokCaret, Val: "^"}
	case '<':
		s.read()
		r2, _ := s.peek()
		if r2 == '=' {
			s.read()
			return Token{Typ: TokLtEq, Val: "<="}
		}
		return Token{Typ: TokLt, Val: "<"}
	case '>':
		s.read()
		r2, _ := s.peek()
		if r2 == '=' {
			s.read()
			return Token{Typ: TokGtEq, Val: ">="}
		}
		return Token{Typ: TokGt, Val: ">"}

	case '=':
		s.read()
		r2, _ := s.peek()
		if r2 == '~' {
			s.read()
			return Token{Typ: TokReMatch, Val: "=~"}
		}
		if r2 == '=' {
			s.read()
			return Token{Typ: TokEqEq, Val: "=="}
		}
		return Token{Typ: TokEq, Val: "="}

	case '"':
		return s.scanQuotedString()

	case '`':
		return s.scanRawString()
	}

	if unicode.IsLetter(r) || r == '_' {
		return s.scanIdent()
	}

	if unicode.IsDigit(r) && !s.inRange || r == '.' && !s.inRange && s.pos+1 < len(s.src) && s.src[s.pos+1] >= '0' && s.src[s.pos+1] <= '9' {
		return s.scanLiteral(spaced)
	}
	if unicode.IsDigit(r) {
		return s.scanNumberOrDuration()
	}

	s.read()
	return Token{Typ: TokError, Val: string(r)}
}

func (s *scanner) scanQuotedString() Token {
	start := s.pos
	s.read() // consume opening "
	for s.pos < len(s.src) {
		r := s.read()
		if r == '\\' {
			s.read() // an escaped quote cannot close the literal
			continue
		}
		if r == '"' {
			literal := s.src[start:s.pos]
			value, err := strconv.Unquote(literal)
			if err != nil || !utf8.ValidString(literal) {
				return Token{Typ: TokError, Val: "invalid quoted string"}
			}
			return Token{Typ: TokString, Val: value}
		}
	}
	return Token{Typ: TokError, Val: "unterminated quoted string"}
}

func (s *scanner) scanRawString() Token {
	s.read() // consume opening `
	start := s.pos
	for {
		r, size := s.peek()
		if size == 0 || r == '`' {
			break
		}
		s.pos += size
	}
	val := s.src[start:s.pos]
	s.read() // consume closing `
	return Token{Typ: TokRawString, Val: val}
}

// readsAsKeyword reports whether Loki's lexer reads the lower-cased word at the
// scanner position as a keyword: any of lokiKeywords, or a function name
// (lex.go functionTokens) that a call follows (lex.go isFunction), so a label
// called Rate or Count stays a label.
func (s *scanner) readsAsKeyword(lower string) bool {
	return lokiKeywords[lower] || callFollows(s.src, s.pos, lower)
}

// callFollows reports whether the lower-cased function name that ends at src[end] is a keyword token: a call
// follows it, or a `by (` or `without (` grouping.
func callFollows(src string, end int, lower string) bool {
	if !rangeAggNames[lower] && !vectorAggNames[lower] && !unwrapConversions[lower] && lower != "vector" {
		return false
	}
	return parenFollows(src, end)
}

// parenFollows reports whether a '(' follows src[end], or a `by (` or `without (` grouping.
func parenFollows(src string, end int) bool {
	i := skipSpace(src, end)
	if i < len(src) && src[i] == '(' {
		return true
	}
	for _, grouping := range [...]string{"by", "without"} {
		if len(src)-i >= len(grouping) && strings.EqualFold(src[i:i+len(grouping)], grouping) {
			j := skipSpace(src, i+len(grouping))
			return j < len(src) && src[j] == '('
		}
	}
	return false
}

// lokiAlwaysKeywords are the words Loki's lexer returns as keyword tokens wherever they stand; ip and
// label_replace are keywords only before a parenthesis (the function-like words).
var lokiAlwaysKeywords = map[string]bool{
	"by": true, "without": true, "bool": true, "offset": true, "on": true, "ignoring": true,
	"group_left": true, "group_right": true, "unless": true, "unwrap": true, "json": true,
	"regexp": true, "logfmt": true, "unpack": true, "pattern": true, "label_format": true,
	"line_format": true, "decolorize": true, "drop": true, "keep": true, "variants": true, "of": true,
}

// lokiWord is what Loki's lexer makes of a word: always a keyword (call false), or one only before a
// parenthesis (call true); name is the token name its parse errors print.
type lokiWord struct {
	name string
	call bool
}

// lokiWords has every word that is a keyword token to Loki's lexer, by lower case form.
var lokiWords = func() map[string]lokiWord {
	m := map[string]lokiWord{"ip": {"ip", true}, "label_replace": {"label_replace", true}, "vector": {"VECTOR", true},
		"bytes": {"BYTES_CONV", true}, "duration": {"DURATION_CONV", true}, "duration_seconds": {"DURATION_SECONDS_CONV", true}}
	for k := range lokiAlwaysKeywords {
		m[k] = lokiWord{k, false}
	}
	for k := range rangeAggNames {
		m[k] = lokiWord{strings.ToUpper(k), true}
	}
	for k := range vectorAggNames {
		m[k] = lokiWord{strings.ToUpper(k), true}
	}
	return m
}()

// keywordTokenName is the name Loki's parse errors print for the word whose lower case form is lower and that
// ends at src[end], when Loki's lexer reads it as a keyword token, and "" when it is a name.
func keywordTokenName(src string, end int, lower string) string {
	w, ok := lokiWords[lower]
	if !ok || w.call && !parenFollows(src, end) {
		return ""
	}
	return w.name
}

func (s *scanner) scanIdent() Token {
	start := s.pos
	for s.pos < len(s.src) {
		if c := s.src[s.pos]; c < utf8.RuneSelf {
			if c|0x20 >= 'a' && c|0x20 <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '.' {
				s.pos++
				continue
			}
			break
		}
		r, size := s.peek()
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			break
		}
		s.pos += size
	}
	val := s.src[start:s.pos]
	if lower := strings.ToLower(val); lower != val && s.readsAsKeyword(lower) {
		val = lower
	}
	switch val {
	case "by":
		return Token{Typ: TokBy, Val: val}
	case "without":
		return Token{Typ: TokWithout, Val: val}
	case "on":
		return Token{Typ: TokOn, Val: val}
	case "ignoring":
		return Token{Typ: TokIgnoring, Val: val}
	case "group_left":
		return Token{Typ: TokGroupLeft, Val: val}
	case "group_right":
		return Token{Typ: TokGroupRight, Val: val}
	case "and":
		return Token{Typ: TokAnd, Val: val}
	case "or":
		return Token{Typ: TokOr, Val: val}
	case "unless":
		return Token{Typ: TokUnless, Val: val}
	}
	return Token{Typ: TokIdent, Val: val}
}

// scanLetters advances over a run of letters.
func (s *scanner) scanLetters() {
	for {
		r, size := s.peek()
		if size == 0 || !unicode.IsLetter(r) {
			return
		}
		s.pos += size
	}
}

// isDurationUnit reports a unit a compound duration may continue after.
func isDurationUnit(u string) bool {
	switch strings.ToLower(u) {
	case "ns", "us", "µs", "ms", "s", "m", "h", "d", "w", "y":
		return true
	}
	return false
}

// scanNumberOrDuration scans an integer; if followed by time-unit letters it
// returns TokDuration, otherwise TokNumber.
func (s *scanner) scanNumberOrDuration() Token {
	start := s.pos
	for {
		r, size := s.peek()
		if size == 0 || (!unicode.IsDigit(r) && r != '.') {
			break
		}
		s.pos += size
	}
	// Check for duration unit suffix
	unitStart := s.pos
	s.scanLetters()
	hasUnit := s.pos > unitStart
	// A compound duration (1h2m20s): after a duration unit, another number and unit
	// continue the same token, as Go's time.ParseDuration reads them.
	for s.pos > unitStart && isDurationUnit(s.src[unitStart:s.pos]) {
		r, _ := s.peek()
		if !unicode.IsDigit(r) {
			break
		}
		for {
			r, size := s.peek()
			if size == 0 || (!unicode.IsDigit(r) && r != '.') {
				break
			}
			s.pos += size
		}
		unitStart = s.pos
		s.scanLetters()
	}
	val := s.src[start:s.pos]
	if hasUnit {
		return Token{Typ: TokDuration, Val: val}
	}
	return Token{Typ: TokNumber, Val: val}
}

// scanLiteral reads the number, duration or bytes literal at the scanner position as Loki's lexer does
// (scanNumberLiteral), and notes its lexer error.
func (s *scanner) scanLiteral(spaced bool) Token {
	start := s.pos
	lit := scanNumberLiteral(s.src, start)
	s.pos = lit.end
	msg := lit.err
	if s.pos < len(s.src) && s.src[s.pos] == 0 {
		msg = "invalid character NUL"
	}
	if spaced && s.src[start] == '0' && lowerByte(at(s.src, start+1)) == 'b' {
		// A 0b or 0B literal after white space is a decimal number without digits to Loki's scanner.
		msg = "decimal literal has no digits"
	}
	if msg != "" {
		s.addLexErr(start, msg, false)
	}
	val := s.src[start:s.pos]
	switch lit.name {
	case "DURATION":
		return Token{Typ: TokDurationLit, Val: val}
	case "BYTES":
		return Token{Typ: TokBytes, Val: val}
	}
	return Token{Typ: TokNumber, Val: val}
}

// IsLokiKeyword reports whether word is a word Loki's lexer reads as a keyword token (in some position), by its
// lower case form. The scanner folds such an identifier to lower case, so a typed name equal to one may have been
// written in another case.
func IsLokiKeyword(word string) bool {
	_, ok := lokiWords[word]
	return ok
}
