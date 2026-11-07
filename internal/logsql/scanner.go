// internal/logsql/scanner.go
package logsql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokType identifies a lexical token kind.
type TokType int

const (
	TokEOF   TokType = iota
	TokError         // lexical error; Val contains the message

	// Punctuation
	TokLBrace   // {
	TokRBrace   // }
	TokLBracket // [
	TokRBracket // ]
	TokLParen   // (
	TokRParen   // )
	TokComma    // ,
	TokPipe     // |
	TokStar     // *
	TokColon    // :  (bare — used for _time: and before range/in keywords)

	// Field-filter compound operators  field:OP value
	TokColonEq    // :=
	TokColonTilde // :~
	TokColonGT    // :>
	TokColonGTE   // :>=
	TokColonLT    // :<
	TokColonLTE   // :<=

	// Label-match operators inside {}
	TokEq         // =
	TokNeq        // !=
	TokReMatch    // =~
	TokReNotMatch // !~

	TokTilde // ~  (message regexp prefix)

	// Logical keywords (case-sensitive)
	TokAnd // AND
	TokOr  // OR
	TokNot // NOT

	// Values
	TokIdent     // identifier (letters, digits, _, ., -, /, [, ])
	TokString    // "…" or '…' — stored without surrounding quotes
	TokRawString // `…` — stored without surrounding backticks

	TokBang // !  (negation, when no = or ~ follows)
)

// Token is a single lexical unit.
type Token struct {
	Typ TokType
	Val string
}

// Scanner tokenises a LogsQL query string one token at a time.
type Scanner struct {
	src string
	pos int
	// spaceBefore is whether whitespace came before the token Next returned last, so a parser can tell "a -b"
	// from "a-b" and "*a" from "* a".
	spaceBefore bool
	// start and end are the byte offsets of the token Next returned last in the source.
	start, end int
}

// Raw returns the source text of the token Next returned last: a quoted string with its quotes.
func (s *Scanner) Raw() string { return s.src[s.start:s.end] }

// SpaceBefore reports whether whitespace (or a comment) separated the token Next returned last from the one before.
func (s *Scanner) SpaceBefore() bool { return s.spaceBefore }

// NewScanner returns a Scanner for the given input string.
func NewScanner(src string) *Scanner { return &Scanner{src: src} }

// Next returns the next Token, advancing the position.
// Returns TokEOF once all input is consumed.
func (s *Scanner) Next() Token {
	start := s.pos
	s.skipWS()
	s.spaceBefore = s.pos > start
	s.start = s.pos
	tok := s.next()
	s.end = s.pos
	return tok
}

func (s *Scanner) next() Token {
	if s.pos >= len(s.src) {
		return Token{Typ: TokEOF}
	}
	ch, size := utf8.DecodeRuneInString(s.src[s.pos:])

	switch ch {
	case '{':
		s.pos += size
		return Token{Typ: TokLBrace, Val: "{"}
	case '}':
		s.pos += size
		return Token{Typ: TokRBrace, Val: "}"}
	case '[':
		s.pos += size
		return Token{Typ: TokLBracket, Val: "["}
	case ']':
		s.pos += size
		return Token{Typ: TokRBracket, Val: "]"}
	case '(':
		s.pos += size
		return Token{Typ: TokLParen, Val: "("}
	case ')':
		s.pos += size
		return Token{Typ: TokRParen, Val: ")"}
	case ',':
		s.pos += size
		return Token{Typ: TokComma, Val: ","}
	case '|':
		s.pos += size
		return Token{Typ: TokPipe, Val: "|"}
	case '*':
		s.pos += size
		return Token{Typ: TokStar, Val: "*"}
	case '"', '\'':
		return s.scanQuoted()
	case '`':
		return s.scanRaw()
	case ':':
		return s.scanColon()
	case '=':
		s.pos += size
		if s.pos < len(s.src) && s.src[s.pos] == '~' {
			s.pos++
			return Token{Typ: TokReMatch, Val: "=~"}
		}
		return Token{Typ: TokEq, Val: "="}
	case '!':
		s.pos += size
		if s.pos < len(s.src) {
			switch s.src[s.pos] {
			case '=':
				s.pos++
				return Token{Typ: TokNeq, Val: "!="}
			case '~':
				s.pos++
				return Token{Typ: TokReNotMatch, Val: "!~"}
			}
		}
		return Token{Typ: TokBang, Val: "!"}
	case '~':
		s.pos += size
		return Token{Typ: TokTilde, Val: "~"}
	}

	return s.scanIdent()
}

func (s *Scanner) skipWS() {
	for s.pos < len(s.src) {
		r, size := utf8.DecodeRuneInString(s.src[s.pos:])
		if r == '#' { // a comment runs to the end of the line
			if n := strings.IndexByte(s.src[s.pos:], '\n'); n >= 0 {
				s.pos += n
			} else {
				s.pos = len(s.src)
			}
			continue
		}
		if !unicode.IsSpace(r) {
			break
		}
		s.pos += size
	}
}

// scanQuoted reads a "…" or '…' string the way VictoriaLogs' lexer does: "…" with Go's string escapes and '…' with
// Go's escapes for a single-quoted string.
func (s *Scanner) scanQuoted() Token {
	rest := s.src[s.pos:]
	if rest[0] == '\'' {
		var b []byte
		i := 1
		for !strings.HasPrefix(rest[i:], "'") {
			ch, _, tail, err := strconv.UnquoteChar(rest[i:], '\'')
			if err != nil {
				s.pos++
				return Token{Typ: TokError, Val: "unterminated string"}
			}
			b = utf8.AppendRune(b, ch)
			i = len(rest) - len(tail)
		}
		s.pos += i + 1
		return Token{Typ: TokString, Val: string(b)}
	}
	prefix, err := strconv.QuotedPrefix(rest)
	if err == nil {
		var val string
		if val, err = strconv.Unquote(prefix); err == nil {
			s.pos += len(prefix)
			return Token{Typ: TokString, Val: val}
		}
	}
	s.pos++
	return Token{Typ: TokError, Val: "unterminated string"}
}

func (s *Scanner) scanRaw() Token {
	s.pos++ // consume opening `
	start := s.pos
	for s.pos < len(s.src) && s.src[s.pos] != '`' {
		s.pos++
	}
	if s.pos >= len(s.src) {
		return Token{Typ: TokError, Val: "unterminated raw string"}
	}
	val := s.src[start:s.pos]
	s.pos++ // consume closing `
	return Token{Typ: TokRawString, Val: val}
}

func (s *Scanner) scanColon() Token {
	s.pos++ // consume :
	if s.pos >= len(s.src) {
		return Token{Typ: TokColon, Val: ":"}
	}
	switch s.src[s.pos] {
	case '=':
		s.pos++
		return Token{Typ: TokColonEq, Val: ":="}
	case '~':
		s.pos++
		return Token{Typ: TokColonTilde, Val: ":~"}
	case '>':
		s.pos++
		if s.pos < len(s.src) && s.src[s.pos] == '=' {
			s.pos++
			return Token{Typ: TokColonGTE, Val: ":>="}
		}
		return Token{Typ: TokColonGT, Val: ":>"}
	case '<':
		s.pos++
		if s.pos < len(s.src) && s.src[s.pos] == '=' {
			s.pos++
			return Token{Typ: TokColonLTE, Val: ":<="}
		}
		return Token{Typ: TokColonLT, Val: ":<"}
	}
	return Token{Typ: TokColon, Val: ":"}
}

func (s *Scanner) scanIdent() Token {
	start := s.pos
	for s.pos < len(s.src) {
		r, size := utf8.DecodeRuneInString(s.src[s.pos:])
		if !isIdentRune(r) {
			break
		}
		s.pos += size
	}
	val := s.src[start:s.pos]
	if val == "" {
		// Unrecognised character
		_, size := utf8.DecodeRuneInString(s.src[s.pos:])
		bad := s.src[s.pos : s.pos+size]
		s.pos += size
		return Token{Typ: TokError, Val: "unexpected character: " + bad}
	}
	// VictoriaLogs reads the logical keywords in any case.
	switch strings.ToUpper(val) {
	case "AND":
		return Token{Typ: TokAnd, Val: val}
	case "OR":
		return Token{Typ: TokOr, Val: val}
	case "NOT":
		return Token{Typ: TokNot, Val: val}
	}
	return Token{Typ: TokIdent, Val: val}
}

// isIdentRune returns true for characters valid inside a LogsQL identifier
// or bare value (field names, durations, numbers, paths).
func isIdentRune(r rune) bool {
	return r == '_' || r == '.' || r == '-' || r == '/' || r == '+' || r == '$' ||
		(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') || (r > unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r)))
}

// String returns a human-readable name for the token type.
func (t TokType) String() string {
	names := []string{
		"EOF", "Error",
		"LBrace", "RBrace", "LBracket", "RBracket", "LParen", "RParen",
		"Comma", "Pipe", "Star", "Colon",
		"ColonEq", "ColonTilde", "ColonGT", "ColonGTE", "ColonLT", "ColonLTE",
		"Eq", "Neq", "ReMatch", "ReNotMatch",
		"Tilde",
		"And", "Or", "Not",
		"Ident", "String", "RawString", "Bang",
	}
	if int(t) < len(names) {
		return names[t]
	}
	return fmt.Sprintf("TokType(%d)", int(t))
}

// Remaining returns the unconsumed portion of the input (from current scanner position).
// Call this when you need raw text rather than tokenized input — e.g. math expressions.
func (s *Scanner) Remaining() string {
	if s.pos >= len(s.src) {
		return ""
	}
	return s.src[s.pos:]
}

// AdvanceTo advances the scanner position until it reaches a byte equal to ch or EOF.
// If ch is 0, it advances to EOF.
func (s *Scanner) AdvanceTo(ch byte) {
	if ch == 0 {
		s.pos = len(s.src)
		return
	}
	for s.pos < len(s.src) && s.src[s.pos] != ch {
		s.pos++
	}
}

// Skip advances the scanner position by n bytes (clamped to the end of input).
// Used when the caller has already computed the byte offset to skip over.
func (s *Scanner) Skip(n int) {
	s.pos += n
	if s.pos > len(s.src) {
		s.pos = len(s.src)
	}
}
