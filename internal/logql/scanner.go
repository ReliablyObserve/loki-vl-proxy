package logql

import (
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
)

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
	braceDepth int // tracks {} nesting for != vs |= disambiguation
}

func newScanner(src string) *scanner {
	return &scanner{src: src}
}

func (s *scanner) peek() (rune, int) {
	if s.pos >= len(s.src) {
		return 0, 0
	}
	r, size := utf8.DecodeRuneInString(s.src[s.pos:])
	return r, size
}

func (s *scanner) read() rune {
	r, size := s.peek()
	s.pos += size
	return r
}

func (s *scanner) skipWS() {
	for {
		r, size := s.peek()
		if size == 0 || !unicode.IsSpace(r) {
			return
		}
		s.pos += size
	}
}

func (s *scanner) next() Token {
	s.skipWS()
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
		return Token{Typ: TokLBracket, Val: "["}
	case ']':
		s.read()
		return Token{Typ: TokRBracket, Val: "]"}
	case ',':
		s.read()
		return Token{Typ: TokComma, Val: ","}

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

	if unicode.IsDigit(r) {
		return s.scanNumberOrDuration()
	}

	s.read()
	return Token{Typ: TokError, Val: string(r)}
}

func (s *scanner) scanQuotedString() Token {
	s.read() // consume opening "
	var b strings.Builder
	for {
		r := s.read()
		if r == 0 {
			break
		}
		if r == '\\' {
			r2 := s.read()
			switch r2 {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case '"':
				b.WriteByte('"')
			case '\\':
				b.WriteByte('\\')
			default:
				b.WriteRune('\\')
				b.WriteRune(r2)
			}
			continue
		}
		if r == '"' {
			break
		}
		b.WriteRune(r)
	}
	return Token{Typ: TokString, Val: b.String()}
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

func (s *scanner) scanIdent() Token {
	start := s.pos
	for {
		r, size := s.peek()
		if size == 0 || (!unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '.') {
			break
		}
		s.pos += size
	}
	val := s.src[start:s.pos]
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
	for {
		r, size := s.peek()
		if size == 0 || !unicode.IsLetter(r) {
			break
		}
		s.pos += size
	}
	val := s.src[start:s.pos]
	if s.pos > unitStart {
		return Token{Typ: TokDuration, Val: val}
	}
	return Token{Typ: TokNumber, Val: val}
}
