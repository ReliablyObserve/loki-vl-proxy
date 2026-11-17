package logsql

import "strings"

// WithGroupKeyAfterStream returns query with key added, right after _stream, to
// the group of every top-level stats pipe that groups by _stream (unbucketed) and
// not yet by key: `| stats by (_stream, a) f` becomes
// `| stats by (_stream, key, a) f`. Every other byte of the query is kept as it
// is. ok is false, and query is returned unchanged, when the scanner cannot read
// the query or no stats pipe groups by _stream.
func WithGroupKeyAfterStream(query, key string) (string, bool) {
	s := NewScanner(query)
	var inserts []int // offsets right after a group's _stream key
	depth := 0
	// state follows `| stats by (`: 0 nothing, 1 after the pipe, 2 after stats, 3 after by.
	state := 0
	for {
		tok := s.Next()
		switch {
		case tok.Typ == TokEOF:
			return insertAfter(query, inserts, ", "+key)
		case tok.Typ == TokError && !strayCharacter(tok):
			return query, false
		case tok.Typ == TokLParen && state == 3:
			at, ok := streamGroupInsert(s, key)
			if !ok {
				return query, false
			}
			if at >= 0 {
				inserts = append(inserts, at)
			}
			state = 0
		case tok.Typ == TokLParen:
			depth++
			state = 0
		case tok.Typ == TokRParen:
			depth--
			state = 0
		case tok.Typ == TokPipe && depth == 0:
			state = 1
		case tok.Typ == TokIdent && state == 1 && tok.Val == "stats":
			state = 2
		case tok.Typ == TokIdent && state == 2 && tok.Val == "by":
			state = 3
		default:
			state = 0
		}
	}
}

// streamGroupInsert reads a stats group after its opening parenthesis up to its
// closing one and returns the offset right after its _stream key when the group
// holds an unbucketed _stream and not key, else -1. ok is false when the group
// does not close.
func streamGroupInsert(s *Scanner, key string) (at int, ok bool) {
	depth, streamEnd, hasKey, prevStream := 0, -1, false, false
	for {
		tok := s.Next()
		switch {
		case tok.Typ == TokEOF || tok.Typ == TokError && !strayCharacter(tok):
			return -1, false
		case tok.Typ == TokRParen && depth == 0:
			if streamEnd >= 0 && !hasKey {
				return streamEnd, true
			}
			return -1, true
		case tok.Typ == TokLParen:
			depth++
		case tok.Typ == TokRParen:
			depth--
		case tok.Typ == TokIdent && tok.Val == key:
			hasKey = true
		case tok.Typ == TokIdent && tok.Val == "_stream":
			prevStream, streamEnd = true, s.end
			continue
		case (tok.Typ == TokColon || tok.Typ == TokColonEq) && prevStream:
			streamEnd = -1 // a bucketed or odd key, not the stream itself
		}
		prevStream = false
	}
}

// strayCharacter reports a scanner error for one character the scanner does not
// read outside quoted text (the % of a math expression): the scanner has stepped
// over it, and it is no token the edit looks for. Any other error (an unclosed
// quote) leaves the query unread.
func strayCharacter(tok Token) bool {
	return strings.HasPrefix(tok.Val, "unexpected character: ")
}

// insertAfter returns query with text inserted at each offset of at (ascending),
// and whether there was any.
func insertAfter(query string, at []int, text string) (string, bool) {
	if len(at) == 0 {
		return query, false
	}
	out := make([]byte, 0, len(query)+len(at)*len(text))
	last := 0
	for _, i := range at {
		out = append(out, query[last:i]...)
		out = append(out, text...)
		last = i
	}
	return string(append(out, query[last:]...)), true
}

// WithGroupKeyInEveryStats returns query with key added to the group of every
// top-level stats pipe: `| stats by (a) f` becomes `| stats by (a, key) f` and
// `| stats f` becomes `| stats by (key) f`, so a field set ahead of the first
// stats pipe reaches the last one's answer. Every other byte is kept. ok is false,
// and query is returned unchanged, when the scanner cannot read the query, a
// group already names key, a fields pipe (which could drop key) precedes a stats
// pipe, or the query holds no stats pipe.
func WithGroupKeyInEveryStats(query, key string) (string, bool) {
	s := NewScanner(query)
	var at []int
	var text []string
	depth, state, fields := 0, 0, false
	for {
		tok := s.Next()
		switch {
		case tok.Typ == TokEOF:
			return insertTexts(query, at, text)
		case tok.Typ == TokError && !strayCharacter(tok):
			return query, false
		case tok.Typ == TokPipe && depth == 0:
			state = 1
			continue
		case tok.Typ == TokLParen:
			depth++
		case tok.Typ == TokRParen:
			depth--
		}
		if state != 1 || tok.Typ != TokIdent {
			state = 0
			continue
		}
		state = 0
		switch tok.Val {
		case "fields":
			fields = true
		case "stats":
			if fields {
				return query, false
			}
			afterStats := s.end
			if next := s.Next(); next.Typ == TokIdent && next.Val == "by" {
				if open := s.Next(); open.Typ != TokLParen {
					return query, false
				}
				end, ok := groupEnd(s, key)
				if !ok {
					return query, false
				}
				at, text = append(at, end), append(text, ", "+key)
			} else {
				at, text = append(at, afterStats), append(text, " by ("+key+")")
				if next.Typ == TokLParen {
					depth++
				}
			}
		}
	}
}

// groupEnd reads a stats group after its opening parenthesis and returns the
// offset of its closing one; ok is false when the group does not close or names key.
func groupEnd(s *Scanner, key string) (int, bool) {
	depth := 0
	for {
		tok := s.Next()
		switch {
		case tok.Typ == TokEOF || tok.Typ == TokError && !strayCharacter(tok):
			return 0, false
		case tok.Typ == TokIdent && tok.Val == key:
			return 0, false
		case tok.Typ == TokLParen:
			depth++
		case tok.Typ == TokRParen && depth == 0:
			return s.start, true
		case tok.Typ == TokRParen:
			depth--
		}
	}
}

// insertTexts returns query with text[i] inserted at offset at[i] (ascending),
// and whether there was any.
func insertTexts(query string, at []int, text []string) (string, bool) {
	if len(at) == 0 {
		return query, false
	}
	var b strings.Builder
	last := 0
	for i, o := range at {
		b.WriteString(query[last:o])
		b.WriteString(text[i])
		last = o
	}
	b.WriteString(query[last:])
	return b.String(), true
}
